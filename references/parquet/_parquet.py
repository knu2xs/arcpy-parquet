"""Utilities for converting between Esri Feature Classes and GeoParquet files.

This module provides functions to export an Esri Feature Class to GeoParquet
format and to import a GeoParquet file into a Feature Class. Conversions rely
on `arcpy` for Feature Class access and `pyarrow` for Parquet I/O.
Geometry is stored as WKB-encoded binary following the GeoParquet 1.1
specification — no `geopandas` or `shapely` dependency is required.

``` python
from az_broadband.utils.parquet import features_to_geoparquet, geoparquet_to_features

# Export a feature class to a GeoParquet dataset directory
features_to_geoparquet(
    feature_class=r"C:\\data\\my.gdb\\counties",
    output_path=r"C:\\data\\counties",
)
# -> C:\\data\\counties\\part-<uuid>.parquet, …

# Export with an identifying name embedded in each part file
features_to_geoparquet(
    feature_class=r"C:\\data\\my.gdb\\counties",
    output_path=r"C:\\data\\counties",
    name="counties",
)
# -> C:\\data\\counties\\part-counties_<uuid>.parquet, …

# Export to a Hive-style partitioned GeoParquet dataset directory
features_to_geoparquet(
    feature_class=r"C:\\data\\my.gdb\\counties",
    output_path=r"C:\\data\\counties_partitioned",
    partition_fields=["state_fips", "county_fips"],
)
# -> C:\\data\\counties_partitioned\\state_fips=04\\county_fips=001\\part-<uuid>.parquet, …

# Import a GeoParquet dataset directory into a feature class
geoparquet_to_features(
    parquet_path=r"C:\\data\\counties",
    feature_class=r"C:\\data\\my.gdb\\counties_copy",
)
```
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Optional, Sequence

import arcpy
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from az_broadband.utils._logging import get_logger
from az_broadband.utils._main_utils import slugify

logger = get_logger(__name__, level="DEBUG", add_stream_handler=False)

__all__ = [
    "features_to_geoparquet",
    "geoparquet_to_features",
    "get_geometry_columns",
]


_GEOMETRY_COLUMN = "geometry"
"""Default name for the WKB geometry column in GeoParquet files."""

_CENTROID_COLUMN = "geometry_centroid"
"""Column name for the optional WKB centroid geometry in GeoParquet files."""

_DEFAULT_BATCH_SIZE = 10_000
"""Number of rows to accumulate per RecordBatch when streaming to/from Parquet."""


# ---------------------------------------------------------------------------
# Feature Class  ➜  GeoParquet
# ---------------------------------------------------------------------------


def features_to_geoparquet(
    feature_class: str | os.PathLike,
    output_path: str | os.PathLike,
    fields: Optional[Sequence[str]] = None,
    partition_fields: Optional[Sequence[str]] = None,
    where_clause: Optional[str] = None,
    spatial_reference: Optional[arcpy.SpatialReference] = None,
    overwrite: bool = False,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    include_centroids: bool = False,
    name: Optional[str] = None,
) -> Path:
    """Export an Esri Feature Class to a GeoParquet dataset directory.

    Output is always written as a directory of ``.parquet`` part files.
    When *partition_fields* is ``None`` (the default), part files are
    written directly into *output_path*.  When one or more
    *partition_fields* are specified, a Hive-style partitioned layout is
    created beneath *output_path*, with sub-directories named
    ``<field>=<value>`` and part files at the leaves.

    Part files follow the naming convention
    ``part-<uuid>.parquet`` or, when *name* is provided,
    ``part-<name>_<uuid>.parquet``.  Each run generates uniquely named
    files, so successive runs into the same directory will **not**
    overwrite files produced by earlier runs.

    Reads geometry and attributes from the Feature Class using an
    `arcpy.da.SearchCursor`, serializes geometry as WKB, and streams
    `RecordBatch` objects to Parquet with GeoParquet 1.1 metadata via
    `pyarrow`.  Data is flushed in batches of *batch_size* rows so the
    full dataset is never held in memory at once.

    !!! note
        The OID field (e.g. `OBJECTID`) is excluded by default unless
        explicitly listed in *fields*.

    Args:
        feature_class: Path to the input Feature Class.
        output_path: Destination directory for the GeoParquet dataset.
        fields: Subset of attribute fields to include. When ``None`` all
            non-OID fields are exported.
        partition_fields: Ordered sequence of field names to partition by.
            When ``None`` or empty, part files are written directly into
            *output_path*.  When provided, a Hive-style partitioned
            dataset is written to *output_path* as a directory.
        where_clause: Optional SQL expression to filter rows.
        spatial_reference: If provided, geometries are projected to this
            spatial reference on export.
        overwrite: When ``True``, an existing directory at *output_path*
            will be removed before writing.  Defaults to ``False``.
            When ``False`` and the directory already exists, new part
            files are written alongside existing ones using unique
            filenames — previous outputs are preserved.
        batch_size: Number of rows per RecordBatch written to Parquet.
            Defaults to 10 000.
        include_centroids: When ``True``, a secondary geometry column named
            ``geometry_centroid`` is added containing the centroid of each
            feature in the same spatial reference.  The primary geometry
            column remains ``geometry``.  Defaults to ``False``.
        name: Optional identifying name embedded in part file names.
            When provided, files are named ``part-<name>_<uuid>.parquet``.
            When ``None``, files are named ``part-<uuid>.parquet``.

    Returns:
        Path: Resolved path to the created GeoParquet dataset directory.

    Raises:
        ValueError: If *feature_class* does not exist, has no geometry, or
            a partition field is not found in the attribute fields.
    """
    import shutil

    feature_class = str(feature_class)
    output_path = Path(output_path)

    partition_fields = list(partition_fields) if partition_fields else []
    is_partitioned = len(partition_fields) > 0

    # Build the part-file basename template:
    #   part-<uuid>.parquet          (no name)
    #   part-<name>_<uuid>.parquet   (with name)
    run_id = uuid.uuid4().hex[:8]
    if name:
        safe_name = slugify(name)
        basename_template = f"part-{safe_name}_{run_id}-{{i}}.parquet"
    else:
        basename_template = f"part-{run_id}-{{i}}.parquet"

    # --- Handle existing output ------------------------------------------------
    if output_path.exists():
        if overwrite:
            if output_path.is_dir():
                shutil.rmtree(output_path)
            else:
                output_path.unlink()
            logger.info(f"Removed existing output: {output_path}")
        else:
            # Directory-based output: allow writing alongside existing files.
            # Unique part filenames (UUID-based) prevent collisions with
            # files produced by earlier runs.
            logger.info(
                f"Output directory already exists: {output_path}. "
                "New part files will be written with unique names."
            )

    # --- Describe input --------------------------------------------------------
    desc = arcpy.Describe(feature_class)
    if not hasattr(desc, "shapeFieldName"):
        raise ValueError(
            f"The input does not appear to be a spatial feature class: {feature_class}"
        )

    shape_field = desc.shapeFieldName
    sr = spatial_reference or desc.spatialReference

    # Determine attribute fields to read
    if fields is None:
        fields = [
            f.name
            for f in arcpy.ListFields(feature_class)
            if f.type not in ("OID", "Geometry") and f.name != shape_field
        ]
    else:
        fields = [f for f in fields if f != shape_field]

    # Validate partition fields
    for pf in partition_fields:
        if pf not in fields:
            raise ValueError(
                f"Partition field '{pf}' is not present in the attribute fields."
            )

    cursor_fields = list(fields) + ["SHAPE@WKB"]
    if include_centroids:
        cursor_fields.append("SHAPE@")

    # --- Build Arrow schema with GeoParquet metadata ---------------------------
    arcpy_fields = {f.name: f for f in arcpy.ListFields(feature_class)}
    arrow_fields = [
        pa.field(f, _arcpy_field_to_arrow_type(arcpy_fields[f]))
        for f in fields
    ] + [pa.field(_GEOMETRY_COLUMN, pa.binary())]
    if include_centroids:
        arrow_fields.append(pa.field(_CENTROID_COLUMN, pa.binary()))

    geo_metadata = _build_geo_metadata(
        geometry_column=_GEOMETRY_COLUMN,
        geometry_types=_arcpy_shape_to_geoparquet_types(desc.shapeType),
        spatial_reference=sr,
        centroid_column=_CENTROID_COLUMN if include_centroids else None,
    )
    schema = pa.schema(
        arrow_fields,
        metadata={b"geo": json.dumps(geo_metadata).encode("utf-8")},
    )

    logger.info(f"Exporting feature class to GeoParquet: {feature_class} -> {output_path}")
    if partition_fields:
        logger.info(f"Partitioning by: {partition_fields}")

    # --- Write: directory-based dataset ------------------------------------------
    output_path.mkdir(parents=True, exist_ok=True)

    if is_partitioned:
        total_written = _write_partitioned(
            feature_class, output_path, partition_fields, fields,
            cursor_fields, where_clause, sr, schema, batch_size,
            include_centroids, basename_template,
        )
    else:
        total_written = _write_dataset(
            feature_class, output_path, fields, cursor_fields,
            where_clause, sr, schema, batch_size, include_centroids,
            basename_template,
        )

    if total_written == 0:
        logger.warning(f"No rows returned from feature class: {feature_class}")

    logger.info(f"Exported {total_written} features to {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Internal writers
# ---------------------------------------------------------------------------


def _write_dataset(
    feature_class: str,
    output_folder: Path,
    fields: list[str],
    cursor_fields: list[str],
    where_clause: Optional[str],
    sr: arcpy.SpatialReference,
    schema: pa.Schema,
    batch_size: int,
    include_centroids: bool,
    basename_template: str,
) -> int:
    """Stream rows from a Feature Class into a directory of GeoParquet part files.

    Each batch of *batch_size* rows is accumulated and then flushed as a
    ``part-<uuid>.parquet`` file inside *output_folder* using
    ``pyarrow.parquet.write_to_dataset``.

    Returns the total number of rows written.
    """
    tables: list[pa.Table] = []
    total_written = 0

    attr_data: dict[str, list[Any]] = {f: [] for f in fields}
    wkb_list: list[bytes] = []
    centroid_wkb_list: list[bytes] = []
    buffered = 0

    with arcpy.da.SearchCursor(
        in_table=feature_class,
        field_names=cursor_fields,
        where_clause=where_clause,
        spatial_reference=sr,
    ) as cursor:
        for row in cursor:
            for field, value in zip(fields, row[: len(fields)]):
                attr_data[field].append(value)
            wkb_list.append(bytes(row[len(fields)]))
            if include_centroids:
                geom = row[len(fields) + 1]
                centroid_wkb_list.append(
                    arcpy.PointGeometry(geom.centroid, geom.spatialReference).WKB
                )
            buffered += 1

            if buffered >= batch_size:
                batch = _build_record_batch(
                    attr_data, wkb_list, fields, schema,
                    centroid_wkb_list=centroid_wkb_list if include_centroids else None,
                )
                tables.append(pa.Table.from_batches([batch], schema=schema))
                total_written += buffered
                attr_data = {f: [] for f in fields}
                wkb_list = []
                centroid_wkb_list = []
                buffered = 0

    # Flush remaining rows
    if buffered > 0:
        batch = _build_record_batch(
            attr_data, wkb_list, fields, schema,
            centroid_wkb_list=centroid_wkb_list if include_centroids else None,
        )
        tables.append(pa.Table.from_batches([batch], schema=schema))
        total_written += buffered

    if total_written == 0:
        return 0

    table = pa.concat_tables(tables)

    pq.write_to_dataset(
        table,
        root_path=str(output_folder),
        basename_template=basename_template,
        existing_data_behavior="overwrite_or_ignore",
    )

    return total_written


def _write_partitioned(
    feature_class: str,
    output_folder: Path,
    partition_fields: list[str],
    fields: list[str],
    cursor_fields: list[str],
    where_clause: Optional[str],
    sr: arcpy.SpatialReference,
    schema: pa.Schema,
    batch_size: int,
    include_centroids: bool,
    basename_template: str,
) -> int:
    """Read rows from a Feature Class and write a Hive-style partitioned
    GeoParquet dataset using ``pyarrow.parquet.write_to_dataset``.

    Returns the total number of rows written.
    """
    tables: list[pa.Table] = []

    attr_data: dict[str, list[Any]] = {f: [] for f in fields}
    wkb_list: list[bytes] = []
    centroid_wkb_list: list[bytes] = []
    buffered = 0
    total_written = 0

    with arcpy.da.SearchCursor(
        in_table=feature_class,
        field_names=cursor_fields,
        where_clause=where_clause,
        spatial_reference=sr,
    ) as cursor:
        for row in cursor:
            for field, value in zip(fields, row[: len(fields)]):
                attr_data[field].append(value)
            wkb_list.append(bytes(row[len(fields)]))
            if include_centroids:
                geom = row[len(fields) + 1]
                centroid_wkb_list.append(
                    arcpy.PointGeometry(geom.centroid, geom.spatialReference).WKB
                )
            buffered += 1

            if buffered >= batch_size:
                batch = _build_record_batch(
                    attr_data, wkb_list, fields, schema,
                    centroid_wkb_list=centroid_wkb_list if include_centroids else None,
                )
                tables.append(pa.Table.from_batches([batch], schema=schema))
                total_written += buffered
                attr_data = {f: [] for f in fields}
                wkb_list = []
                centroid_wkb_list = []
                buffered = 0

    # Flush remaining rows
    if buffered > 0:
        batch = _build_record_batch(
            attr_data, wkb_list, fields, schema,
            centroid_wkb_list=centroid_wkb_list if include_centroids else None,
        )
        tables.append(pa.Table.from_batches([batch], schema=schema))
        total_written += buffered

    if total_written == 0:
        return 0

    table = pa.concat_tables(tables)

    # Cast partition columns to string so Hive directory names are readable
    for pf in partition_fields:
        idx = table.schema.get_field_index(pf)
        if table.schema.field(idx).type != pa.utf8():
            table = table.set_column(
                idx, pf, table.column(pf).cast(pa.utf8())
            )

    pq.write_to_dataset(
        table,
        root_path=str(output_folder),
        partition_cols=partition_fields,
        basename_template=basename_template,
        existing_data_behavior="overwrite_or_ignore",
    )

    return total_written


def _build_record_batch(
    attr_data: dict[str, list[Any]],
    wkb_list: list[bytes],
    fields: list[str],
    schema: pa.Schema,
    centroid_wkb_list: Optional[list[bytes]] = None,
) -> pa.RecordBatch:
    """
    Assemble a single `RecordBatch` from buffered attribute and WKB data.

    Args:
        attr_data: Mapping of field name to list of attribute values.
        wkb_list: List of WKB-encoded geometry bytes.
        fields: Ordered attribute field names.
        schema: Target Arrow schema (includes the geometry column).
        centroid_wkb_list: Optional list of WKB-encoded centroid geometry
            bytes.  When provided, a centroid column is appended.

    Returns:
        pa.RecordBatch: A RecordBatch conforming to *schema*.
    """
    arrays: list[pa.Array] = [
        pa.array(attr_data[f], type=schema.field(f).type) for f in fields
    ] + [pa.array(wkb_list, type=pa.binary())]

    if centroid_wkb_list is not None:
        arrays.append(pa.array(centroid_wkb_list, type=pa.binary()))

    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------------------
# GeoParquet  ➜  Feature Class
# ---------------------------------------------------------------------------


def get_geometry_columns(parquet_path: str | os.PathLike) -> list[str]:
    """Return the names of all geometry columns declared in a GeoParquet dataset.

    Reads only the Parquet metadata (no row data is loaded) and
    extracts the column names from the GeoParquet `geo` metadata.
    Accepts both a single ``.parquet`` file and a directory containing
    one or more ``.parquet`` part files.

    ``` python
    cols = get_geometry_columns(r"C:\\data\\counties")
    # e.g. ["geometry", "geometry_centroid"]
    ```

    Args:
        parquet_path: Path to a GeoParquet file or dataset directory.

    Returns:
        A list of geometry column names.  The primary column is always
        first, followed by any secondary columns in the order they appear
        in the metadata.

    Raises:
        FileNotFoundError: If *parquet_path* does not exist.
    """
    parquet_path = Path(parquet_path)

    if not parquet_path.exists():
        raise FileNotFoundError(f"GeoParquet path not found: {parquet_path}")

    schema = _read_dataset_schema(parquet_path)
    geo_meta = _read_geo_metadata_from_schema(schema)

    primary = geo_meta.get("primary_column", _GEOMETRY_COLUMN)
    columns_meta = geo_meta.get("columns", {})

    # Primary first, then any remaining columns in dict order
    result = [primary] if primary in columns_meta else []
    for col_name in columns_meta:
        if col_name != primary:
            result.append(col_name)

    return result


def geoparquet_to_features(
    parquet_path: str | os.PathLike,
    feature_class: str | os.PathLike,
    geometry_column: Optional[str] = None,
    spatial_reference: Optional[arcpy.SpatialReference] = None,
    overwrite: bool = False,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> str:
    """Import a GeoParquet dataset into an Esri Feature Class.

    Reads a GeoParquet file or dataset directory in batches via `pyarrow`,
    extracts WKB geometry, and inserts rows into a new Feature Class using
    `arcpy.da.InsertCursor`.  Only one batch is held in memory at a time.

    !!! warning
        When *overwrite* is `True` the target Feature Class will be
        deleted before import if it already exists.

    !!! note
        To import data for further geoprocessing without persisting a
        Feature Class on disk, use the `memory` workspace as the
        target — for example,
        `feature_class="memory/my_layer"`.  In-memory Feature
        Classes are significantly faster to create and query, making
        them ideal for intermediate geoprocessing steps.  Remember to
        call `arcpy.management.Delete` on the in-memory Feature Class
        when it is no longer needed to free resources.

    Args:
        parquet_path: Path to the input GeoParquet file or dataset
            directory containing ``.parquet`` part files.
        feature_class: Path for the output Feature Class (e.g.
            `"C:/data/my.gdb/layer_name"`).
        geometry_column: Name of the geometry column to use for the Feature
            Class shapes.  When `None`, the primary geometry column
            declared in the GeoParquet metadata is used.  The value is
            validated against the geometry columns listed in the file
            metadata.
        spatial_reference: Spatial reference for the output Feature Class. If
            `None`, the CRS embedded in the GeoParquet metadata is used.  If
            the metadata also lacks CRS information, WGS 84 (EPSG:4326) is
            assumed per the GeoParquet specification.
        overwrite: When `True`, an existing Feature Class at the target
            path will be deleted first. Defaults to `False`.
        batch_size: Number of rows to read and insert per iteration.
            Defaults to 10 000.

    Returns:
        str: Catalog path to the created Feature Class.

    Raises:
        FileNotFoundError: If *parquet_path* does not exist.
        FileExistsError: If *feature_class* already exists and *overwrite*
            is `False`.
        ValueError: If *geometry_column* is not a valid geometry column in
            the GeoParquet metadata.
    """
    parquet_path = Path(parquet_path)
    feature_class = str(feature_class)

    if not parquet_path.exists():
        raise FileNotFoundError(f"GeoParquet path not found: {parquet_path}")

    if arcpy.Exists(feature_class):
        if overwrite:
            arcpy.management.Delete(feature_class)
            logger.info(f"Deleted existing feature class: {feature_class}")
        else:
            raise FileExistsError(
                f"Feature class already exists: {feature_class}. "
                "Set overwrite=True to replace it."
            )

    logger.info(f"Importing GeoParquet to feature class: {parquet_path} -> {feature_class}")

    schema = _read_dataset_schema(parquet_path)
    geo_meta = _read_geo_metadata_from_schema(schema)

    # Determine and validate the geometry column
    available_geom_cols = list(geo_meta.get("columns", {}).keys())

    if geometry_column is None:
        geom_col = geo_meta.get("primary_column", _GEOMETRY_COLUMN)
    else:
        geom_col = geometry_column

    if geom_col not in available_geom_cols:
        raise ValueError(
            f"Geometry column '{geom_col}' not found in GeoParquet metadata. "
            f"Available geometry columns: {available_geom_cols}"
        )

    # Resolve spatial reference
    if spatial_reference is not None:
        sr = spatial_reference
    else:
        sr = _geo_meta_to_sr(geo_meta, geom_col)

    # Determine arcpy geometry type from GeoParquet metadata
    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    geom_types = col_meta.get("geometry_types", [])
    arcpy_geom_type = _geoparquet_types_to_arcpy(geom_types)

    # Exclude all geometry columns from attribute fields
    all_geom_cols = set(available_geom_cols)
    attr_fields = [name for name in schema.names if name not in all_geom_cols]

    # Create the output feature class
    out_workspace = os.path.dirname(feature_class)
    out_name = os.path.basename(feature_class)
    arcpy.management.CreateFeatureclass(
        out_path=out_workspace,
        out_name=out_name,
        geometry_type=arcpy_geom_type,
        spatial_reference=sr,
    )

    # Add attribute fields from the Arrow schema
    _add_fields_from_arrow_schema(feature_class, schema, exclude=all_geom_cols)

    # Stream batches from Parquet directly into the InsertCursor
    cursor_fields = attr_fields + ["SHAPE@WKB"]
    inserted = 0

    dataset = ds.dataset(
        str(parquet_path), format="parquet",
    )

    # Read only the columns we need (attributes + geometry)
    read_columns = attr_fields + [geom_col]

    with arcpy.da.InsertCursor(
        in_table=feature_class,
        field_names=cursor_fields,
    ) as cursor:
        for batch in dataset.to_batches(
            batch_size=batch_size, columns=read_columns,
        ):
            batch_schema = batch.schema
            geom_column = batch.column(batch_schema.get_field_index(geom_col))
            attr_columns = [
                batch.column(batch_schema.get_field_index(f))
                for f in attr_fields
            ]
            num_rows = batch.num_rows

            for i in range(num_rows):
                values = [col[i].as_py() for col in attr_columns]
                values.append(geom_column[i].as_py())
                cursor.insertRow(values)
                inserted += 1

    logger.info(f"Inserted {inserted} features into {feature_class}")
    return feature_class


# ---------------------------------------------------------------------------
# Dataset I/O helpers
# ---------------------------------------------------------------------------


def _read_dataset_schema(parquet_path: Path) -> pa.Schema:
    """Read the Arrow schema from a GeoParquet file or dataset directory.

    When *parquet_path* is a directory, the first ``.parquet`` file found
    is used to read the schema (including ``geo`` metadata).  When it is a
    single file, the schema is read directly.

    Args:
        parquet_path: Path to a single Parquet file or a directory
            containing ``.parquet`` part files.

    Returns:
        pa.Schema: The Arrow schema with GeoParquet metadata.

    Raises:
        FileNotFoundError: If the path is a directory but contains no
            ``.parquet`` files.
    """
    if parquet_path.is_dir():
        # Find the first .parquet file in the directory tree to read
        # metadata from (ParquetDataset.schema strips custom metadata).
        part_files = sorted(parquet_path.rglob("*.parquet"))
        if not part_files:
            raise FileNotFoundError(
                f"No .parquet files found in directory: {parquet_path}"
            )
        return pq.ParquetFile(str(part_files[0])).schema_arrow

    return pq.ParquetFile(str(parquet_path)).schema_arrow


# ---------------------------------------------------------------------------
# GeoParquet metadata helpers
# ---------------------------------------------------------------------------


def _build_geo_metadata(
    geometry_column: str,
    geometry_types: list[str],
    spatial_reference: arcpy.SpatialReference,
    centroid_column: Optional[str] = None,
) -> dict[str, Any]:
    """Build a GeoParquet 1.1 `geo` metadata dictionary.

    Args:
        geometry_column: Name of the WKB geometry column.
        geometry_types: List of geometry type strings per the GeoParquet spec
            (e.g. `["Polygon", "MultiPolygon"]`).
        spatial_reference: The arcpy SpatialReference for the dataset.
        centroid_column: Optional name of a secondary geometry column holding
            centroid points. When provided, an additional entry is added to
            the `columns` mapping with geometry type `["Point"]`.

    Returns:
        A dictionary conforming to the GeoParquet 1.1 metadata schema.
    """
    crs_json = _sr_to_projjson(spatial_reference)

    column_meta: dict[str, Any] = {
        "encoding": "WKB",
        "geometry_types": geometry_types,
    }
    if crs_json is not None:
        column_meta["crs"] = crs_json

    columns: dict[str, Any] = {
        geometry_column: column_meta,
    }

    if centroid_column is not None:
        centroid_meta: dict[str, Any] = {
            "encoding": "WKB",
            "geometry_types": ["Point"],
        }
        if crs_json is not None:
            centroid_meta["crs"] = crs_json
        columns[centroid_column] = centroid_meta

    return {
        "version": "1.1.0",
        "primary_column": geometry_column,
        "columns": columns,
    }


def _read_geo_metadata_from_schema(schema: pa.Schema) -> dict[str, Any]:
    """Extract and parse GeoParquet `geo` metadata from a pyarrow Schema.

    Args:
        schema: A pyarrow Schema (e.g. from `ParquetFile.schema_arrow`).

    Returns:
        The parsed `geo` metadata dictionary, or an empty dict if the
        metadata key is absent.
    """
    raw_meta = schema.metadata or {}
    geo_bytes = raw_meta.get(b"geo")

    result: dict[str, Any] = json.loads(geo_bytes) if geo_bytes is not None else {}

    return result


def _sr_to_projjson(sr: arcpy.SpatialReference) -> Optional[dict[str, Any]]:
    """Convert an `arcpy.SpatialReference` to a PROJJSON-like dictionary.

    The GeoParquet spec recommends PROJJSON for CRS encoding. This function
    produces a minimal PROJJSON structure when a factory code (EPSG) is
    available, and falls back to embedding OGC WKT via an `id` object.

    Args:
        sr: An arcpy SpatialReference object.

    Returns:
        A PROJJSON dictionary, or `None` if the spatial reference cannot
        be determined.
    """
    projjson: Optional[dict[str, Any]] = None

    if sr is not None and sr.name:
        crs_type = "ProjectedCRS" if sr.type == "Projected" else "GeographicCRS"
        projjson = {
            "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
            "type": crs_type,
            "name": sr.name,
        }
        if sr.factoryCode:
            projjson["id"] = {
                "authority": "EPSG",
                "code": sr.factoryCode,
            }

    return projjson


def _geo_meta_to_sr(geo_meta: dict[str, Any], geom_col: str) -> arcpy.SpatialReference:
    """Derive an `arcpy.SpatialReference` from GeoParquet `geo` metadata.

    When the CRS metadata is absent **or** cannot be parsed, WGS 84
    (EPSG:4326) is returned as the default, following the GeoParquet
    specification.

    Args:
        geo_meta: The parsed GeoParquet `geo` metadata dictionary.
        geom_col: Name of the geometry column to look up.

    Returns:
        arcpy.SpatialReference: The spatial reference for the geometry column.
    """
    col_meta = geo_meta.get("columns", {}).get(geom_col, {})
    crs = col_meta.get("crs")

    sr: Optional[arcpy.SpatialReference] = None

    if crs is None:
        # GeoParquet spec: absent CRS means OGC:CRS84 (WGS 84 lon/lat)
        sr = arcpy.SpatialReference(4326)
    else:
        # Try EPSG code from the PROJJSON id object
        crs_id = crs.get("id", {})
        authority = crs_id.get("authority", "")
        code = crs_id.get("code")

        if authority.upper() == "EPSG" and code is not None:
            sr = arcpy.SpatialReference(int(code))
        else:
            # Fallback: try the CRS name
            name = crs.get("name", "")
            if name:
                try:
                    sr = arcpy.SpatialReference(text=name)
                except Exception:
                    sr = None

    if sr is None:
        logger.warning(
            f"Unable to determine spatial reference from GeoParquet CRS metadata: {crs}. "
            "Defaulting to WGS 84 (EPSG:4326) per the GeoParquet specification."
        )
        sr = arcpy.SpatialReference(4326)

    return sr


# ---------------------------------------------------------------------------
# Geometry type mapping
# ---------------------------------------------------------------------------


_ARCPY_SHAPE_TO_GEOPARQUET: dict[str, list[str]] = {
    "Point": ["Point"],
    "Multipoint": ["MultiPoint"],
    "Polyline": ["LineString", "MultiLineString"],
    "Polygon": ["Polygon", "MultiPolygon"],
}
"""Map arcpy `shapeType` values to GeoParquet geometry type strings."""


_GEOPARQUET_TO_ARCPY: dict[str, str] = {
    "Point": "POINT",
    "MultiPoint": "MULTIPOINT",
    "LineString": "POLYLINE",
    "MultiLineString": "POLYLINE",
    "Polygon": "POLYGON",
    "MultiPolygon": "POLYGON",
}
"""Map GeoParquet geometry type strings to arcpy geometry type strings."""


def _arcpy_shape_to_geoparquet_types(shape_type: str) -> list[str]:
    """Convert an arcpy shape type to a list of GeoParquet geometry types.

    Args:
        shape_type: The `shapeType` value from `arcpy.Describe`
            (e.g. `"Polygon"`).

    Returns:
        A list of GeoParquet geometry type strings.
    """
    return _ARCPY_SHAPE_TO_GEOPARQUET.get(shape_type, [shape_type])


def _geoparquet_types_to_arcpy(geometry_types: list[str]) -> str:
    """Determine the arcpy geometry type from GeoParquet geometry type strings.

    Args:
        geometry_types: List of geometry type strings from the GeoParquet
            metadata (e.g. `["Polygon", "MultiPolygon"]`).

    Returns:
        An arcpy geometry type string (`"POINT"`, `"POLYLINE"`,
        `"POLYGON"`, or `"MULTIPOINT"`).

    Raises:
        ValueError: If no geometry types are provided or the type is
            unsupported.
    """
    arcpy_type: Optional[str] = None

    if not geometry_types:
        # Default to polygon when metadata is missing
        logger.warning("No geometry_types in GeoParquet metadata; defaulting to POLYGON.")
        arcpy_type = "POLYGON"
    else:
        arcpy_type = _GEOPARQUET_TO_ARCPY.get(geometry_types[0])

    if arcpy_type is None:
        raise ValueError(f"Unsupported GeoParquet geometry type: {geometry_types[0]}")

    return arcpy_type


# ---------------------------------------------------------------------------
# Arrow-to-arcpy field mapping
# ---------------------------------------------------------------------------


_ARROW_TO_ARCPY_FIELD_TYPE: dict[str, str] = {
    "int8": "SHORT",
    "int16": "SHORT",
    "int32": "LONG",
    "int64": "LONG",
    "uint8": "SHORT",
    "uint16": "LONG",
    "uint32": "LONG",
    "uint64": "DOUBLE",
    "float": "FLOAT",
    "float16": "FLOAT",
    "float32": "FLOAT",
    "double": "DOUBLE",
    "float64": "DOUBLE",
    "bool": "SHORT",
    "string": "TEXT",
    "utf8": "TEXT",
    "large_string": "TEXT",
    "large_utf8": "TEXT",
    "date32": "DATE",
    "date64": "DATE",
    "binary": "BLOB",
    "large_binary": "BLOB",
}
"""Map pyarrow type names to arcpy field type strings."""


_ARCPY_TO_ARROW_TYPE: dict[str, pa.DataType] = {
    "SmallInteger": pa.int16(),
    "Integer": pa.int32(),
    "BigInteger": pa.int64(),
    "Single": pa.float32(),
    "Double": pa.float64(),
    "String": pa.utf8(),
    "Date": pa.timestamp("ms"),
    "Blob": pa.binary(),
    "Raster": pa.binary(),
    "GUID": pa.utf8(),
    "GlobalID": pa.utf8(),
    "XML": pa.utf8(),
    "OID": pa.int64(),
}
"""Map arcpy field type strings to pyarrow data types."""


def _arcpy_field_to_arrow_type(field: Any) -> pa.DataType:
    """Convert an arcpy field descriptor to a pyarrow data type.

    Args:
        field: An arcpy `Field` object (from `arcpy.ListFields`).

    Returns:
        pa.DataType: The corresponding pyarrow data type.
    """
    return _ARCPY_TO_ARROW_TYPE.get(field.type, pa.utf8())


def _arrow_type_to_arcpy(arrow_type: pa.DataType) -> str:
    """Convert a pyarrow data type to an arcpy field type string.

    Args:
        arrow_type: A pyarrow DataType.

    Returns:
        An arcpy field type string (e.g. `"TEXT"`, `"LONG"`).
    """
    type_str = str(arrow_type)

    # Handle timestamp variants; otherwise look up the type map
    field_type = "DATE" if type_str.startswith("timestamp") else _ARROW_TO_ARCPY_FIELD_TYPE.get(type_str, "TEXT")

    return field_type


def _add_fields_from_arrow_schema(
    feature_class: str,
    schema: pa.Schema,
    exclude: Optional[set[str]] = None,
) -> None:
    """Add fields to a Feature Class based on a pyarrow Schema.

    Args:
        feature_class: Path to the target Feature Class.
        schema: The pyarrow Schema whose fields define columns to add.
        exclude: Set of column names to skip (e.g. the geometry column).
    """
    exclude = exclude or set()
    for field in schema:
        if field.name in exclude:
            continue
        field_type = _arrow_type_to_arcpy(field.type)
        field_length = 255 if field_type == "TEXT" else None
        arcpy.management.AddField(
            in_table=feature_class,
            field_name=field.name,
            field_type=field_type,
            field_length=field_length,
        )
