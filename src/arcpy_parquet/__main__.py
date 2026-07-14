__all__ = [
    "create_schema_file",
    "features_to_parquet",
    "parquet_to_features",
]

import importlib.util
import importlib
import json
from datetime import timedelta
from pathlib import Path
import time
from typing import List, Optional, Tuple, Union, Literal
import uuid

import arcpy
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from .utils import get_logger, pyarrow_utils
from .utils._pyarrow import (
    get_geoparquet_header,
    get_parquet_max_string_lengths,
)
from .utils._h3 import h3_value_from_geometry

# set up logging
logger = get_logger(level="DEBUG", logger_name="arcpy_parquet.main")


def create_schema_file(
    input_dataset: Optional[Union[Path, str]] = None,
    output_schema_file: Optional[Union[Path, str]] = None,
    template_feature_class_path: Optional[Union[Path, str]] = None,
) -> Path:
    """Create a CSV schema file suitable for parquet_to_features imports.

    This utility inspects either ArcGIS-native tabular data (feature class/table)
    or a Parquet dataset and writes a schema CSV that can be supplied to
    parquet_to_features through its schema_file parameter.

    For ArcGIS-native data sources, field metadata is taken directly from
    arcpy.ListFields. For Parquet inputs, the function derives ArcPy-compatible
    names and types from the Arrow schema and infers text lengths using
    get_parquet_max_string_lengths.

    !!! note
        The generated schema file is intended to control field creation during
        import. It does not transform source data itself.

    !!! warning
        OID, Geometry, and Raster fields are intentionally excluded from
        schema generation because they are managed separately by ArcGIS.

    ``` python
    schema_csv = create_schema_file(
        input_dataset=r"data/sample/main_fgdb_sample/parquet",
        output_schema_file=r"data/interim/schema/main_fgdb_sample.csv",
    )
    ```

    Args:
        input_dataset: Path to a source dataset. Supported inputs include an
            ArcGIS table/feature class path or a Parquet dataset directory/file.
        output_schema_file: Destination CSV path to write. Parent directories
            are created automatically when missing.
        template_feature_class_path: Backward-compatible alias for
            input_dataset. Used when older callers provide a template feature
            class path with the legacy parameter name.

    Returns:
        Path: Path to the written schema CSV.

    Raises:
        ValueError: If output_schema_file is missing.
        ValueError: If neither input_dataset nor template_feature_class_path
            is provided.
        ValueError: If the resolved input path does not exist.
        ValueError: If no writable fields are found when reading an ArcGIS
            dataset.
    """
    if output_schema_file is None:
        raise ValueError("output_schema_file is required.")

    if input_dataset is None:
        input_dataset = template_feature_class_path

    if input_dataset is None:
        raise ValueError(
            "You must provide input_dataset (or template_feature_class_path)."
        )

    dataset_path = Path(input_dataset)
    schema_path = Path(output_schema_file)
    schema_path.parent.mkdir(parents=True, exist_ok=True)

    # Build schema directly from ArcPy fields when the input is a geodatabase table/feature class.
    if arcpy.Exists(str(dataset_path)):
        try:
            field_rows = []
            for fld in arcpy.ListFields(str(dataset_path)):
                if fld.type in ("OID", "Geometry", "Raster"):
                    continue

                field_type = (
                    "TEXT" if fld.type.lower() == "string" else fld.type.upper()
                )
                field_rows.append(
                    {
                        "field_name": fld.name,
                        "field_type": field_type,
                        "field_length": fld.length if field_type == "TEXT" else None,
                        "field_alias": fld.aliasName,
                    }
                )

            if not field_rows:
                raise ValueError(
                    f"No writable fields found in input dataset: {dataset_path}"
                )

            pd.DataFrame(field_rows).to_csv(schema_path, encoding="utf-8", index=False)
            return schema_path
        except RuntimeError:
            # ArcPy can report Exists=True for paths that are not ArcPy-readable tables.
            # In that case, continue and treat the input as parquet.
            pass

    # Otherwise treat input as parquet and infer a schema CSV suitable for parquet_to_features.
    if not dataset_path.exists():
        raise ValueError(f"Cannot locate input_dataset at {dataset_path}.")

    try:
        parquet_dataset = pq.ParquetDataset(dataset_path)
    except pa.ArrowTypeError as err:
        logger.warning(
            "Encountered ArrowTypeError while loading ParquetDataset with Hive partitioning: %s. "
            "Falling back to partitioning=None.",
            err,
        )
        parquet_dataset = pq.ParquetDataset(dataset_path, partitioning=None)

    schema_dict = pyarrow_utils.get_schema_dict(parquet_dataset.schema)
    max_lengths = get_parquet_max_string_lengths(parquet_dataset)

    rows = []
    for src_name, props in schema_dict.items():
        pa_type = props.get("type", "string")
        field_type = pyarrow_utils.import_dtype_dict.get(pa_type, "TEXT")
        row = {
            "field_name": props.get("arcpy_name", src_name),
            "field_type": field_type,
            "field_alias": src_name,
            "field_length": None,
        }
        if field_type == "TEXT":
            row["field_length"] = int(max_lengths.get(src_name, 512) or 512)
        rows.append(row)

    pd.DataFrame(rows).to_csv(schema_path, encoding="utf-8", index=False)
    return schema_path


def features_to_parquet(
    input_features: Union[Path, str],
    output_parquet: Union[Path, str],
    partition_columns: Optional[Union[list[str], str]] = None,
    include_geometry: Optional[bool] = True,
    geometry_format: Optional[
        Literal["GEOPARQUET", "H3", "XY", "GEOJSON", "JSON"]
    ] = "GEOPARQUET",
    h3_resolution: Optional[int] = 9,
    batch_size: Optional[int] = 300000,
) -> Path:
    """Export ArcGIS features/tables into a partition-capable Parquet dataset.

    The function reads source rows in batches with an ArcPy search cursor,
    maps ArcGIS field types into Arrow types, and writes compressed Parquet
    parts (optionally partitioned). Geometry output is configurable and can be
    emitted as GeoParquet geometry, XY columns, GeoJSON, or H3 indices.

    !!! note
        WKB is not accepted as a top-level geometry_format option. Use
        GEOPARQUET for standards-compliant GeoParquet output, which encodes
        geometry as WKB internally and writes GeoParquet metadata.

    !!! note
        When geometry_format is H3, coordinate order is handled as
        latitude/longitude for H3 operations even though ArcGIS geometry is
        represented as X/Y.

    !!! warning
        Very small batch_size values can increase I/O overhead substantially.
        Extremely large values can increase memory pressure.

    ``` python
    dataset_dir = features_to_parquet(
        input_features=r"data/sample/sample.gdb/wa_h3_09",
        output_parquet=r"data/interim/wa_h3_09_parquet",
        geometry_format="GEOPARQUET",
        partition_columns=["h3_06"],
        batch_size=300000,
    )
    ```

    Args:
        input_features: Path to an ArcGIS feature class or table.
        output_parquet: Directory where output Parquet data is written. The
            directory is created when it does not already exist.
        partition_columns: Optional partition column name(s). When provided,
            output is written with pq.write_to_dataset using these columns.
        include_geometry: If True, geometry output fields are included according
            to geometry_format. If False, geometry is omitted entirely.
        geometry_format: Geometry representation in the output. Supported
            values are GEOPARQUET, H3, XY, and GEOJSON. JSON is normalized to
            GEOJSON by the format normalizer.
        h3_resolution: H3 resolution used when geometry_format is H3.
        batch_size: Number of records buffered before writing each part file.

    Returns:
        Path: Output Parquet dataset directory path.

    Raises:
        ValueError: If input_features does not exist.
        ValueError: If geometry_format is invalid.
        ValueError: If partition_columns contains fields not present in the
            source dataset.
        EnvironmentError: If H3 export is requested but h3-py is unavailable or
            incompatible with required 4.x APIs.
    """
    output_parquet = Path(output_parquet)

    geometry_format = pyarrow_utils.normalize_export_geometry_format(geometry_format)

    # downgrade path so arcpy can use strings
    fc_pth = str(input_features)

    # ensure source exists
    if not arcpy.Exists(fc_pth):
        raise ValueError(f"The input path {input_features} does not appear to exist.")

    # describe the data to access properties for validation
    desc = arcpy.da.Describe(fc_pth)

    h3_module = None
    input_shape_type = (desc.get("shapeType") or "").upper()

    if include_geometry and geometry_format == "H3":
        if importlib.util.find_spec("h3") is None:
            raise EnvironmentError(
                "geometry_format='H3' requires the h3-py package, but it is not installed in "
                "the current environment. Install h3-py to enable H3 export."
            )
        h3_module = importlib.import_module("h3")

        if not hasattr(h3_module, "latlng_to_cell"):
            raise EnvironmentError(
                "geometry_format='H3' requires h3-py 4.x. The installed package does not "
                "expose latlng_to_cell."
            )

    # make sure the full output path exists where the data will be saved
    if not output_parquet.exists():
        output_parquet.mkdir(parents=True)

    # fields to be excluded
    exclude_fld_typ = ["Raster"]
    exclude_fld_lst = []

    if desc["hasOID"]:
        exclude_fld_lst.append(desc["OIDFieldName"])

    for shp_fld_key in ["lengthFieldName", "areaFieldName"]:
        shp_fld = desc.get(shp_fld_key)
        if shp_fld is not None:
            exclude_fld_lst.append(shp_fld)

    # get a list of input fields to use with the search cursor
    sc_col_lst = [
        f.name
        for f in desc["fields"]
        if f.name not in exclude_fld_lst and f.type not in exclude_fld_typ
    ]

    # if partitioning, ensure the columns are in the input data
    if partition_columns is None:
        partition_columns = []

    if isinstance(partition_columns, str):
        partition_columns = [partition_columns]

    # get a list of any potentially missing partition columns
    missing_prt_cols = [p for p in partition_columns if p not in sc_col_lst]

    # if any are missing, raise an error
    if len(missing_prt_cols) > 0:
        raise ValueError(
            f"The following partition columns do not appear to be in the input data: {', '.join(missing_prt_cols)}"
        )

    # iterate fields to create output schema, excluding geometry since handled explicitly later
    pa_fld_typ_xcld = exclude_fld_typ + ["Geometry"]
    pa_fld_lst = [
        f
        for f in desc["fields"]
        if f.name not in exclude_fld_lst and f.type not in pa_fld_typ_xcld
    ]
    pa_typ_lst = [
        (f.name, pyarrow_utils.export_dtype_dict.get(f.type, pa.string()))
        for f in pa_fld_lst
    ]
    pq_schema = pa.schema(pa_typ_lst)

    # if the input has geometry (is a feature class)
    if desc.get("shapeFieldName") is not None and include_geometry:
        # get the name of the geometry column
        geom_nm = desc.get("shapeFieldName")

        # since the geometry must be specially formatted, it needs to first be removed from the list
        sc_col_lst.remove(geom_nm)

        # if just outputting centroid (point coordinates)
        if geometry_format == "XY":
            # add X and Y columns to search cursor list and the output schema
            for prt, out_col in [("X", "x_lon"), ("Y", "y_lat")]:
                sc_col_lst.append(f"{geom_nm}@{prt}")
                pq_schema = pq_schema.append(pa.field(out_col, pa.float64()))

        # if working with any other output type, just use the specific output format
        else:
            if geometry_format == "GEOPARQUET":
                geom_token = "WKB"
                geom_column_name = "geometry"
                geom_arrow_type = pa.binary()
            elif geometry_format == "GEOJSON":
                geom_token = "JSON"
                geom_column_name = "geojson"
                geom_arrow_type = pa.string()
            else:  # H3
                geom_token = "@"
                geom_column_name = "h3_index"
                geom_arrow_type = pa.string()

            sc_col_lst.append(f"{geom_nm}@{geom_token}")
            pq_schema = pq_schema.append(pa.field(geom_column_name, geom_arrow_type))

            # get the spatial reference and geometry type from the input features
            desc = arcpy.da.Describe(str(input_features))
            in_sr = desc.get("spatialReference")
            geom_typ = desc.get("shapeType")

    # if the geometry is not desired in the output, remove it from the search cursor column list
    if "shapeFieldName" in desc.keys() and not include_geometry:
        sc_col_lst.remove(desc["shapeFieldName"])

    # get values from the data to track progress
    max_range = int(arcpy.management.GetCount(str(input_features))[0])
    rep_range = max(1, max_range // 100)

    # report progress
    features_tense = "feature" if max_range == 1 else "features"
    arcpy.AddMessage(f"Starting export of {max_range:,} {features_tense}.")
    arcpy.SetProgressor("step", "Exporting...", 0, max_range, rep_range)

    # turn off auto cancelling since handling in loop
    arcpy.env.autoCancelling = False

    # create a template dictionary for data export
    pa_dict = {col: [] for col in pq_schema.names}

    # prepend geometry onto search cursor fields if outputting geometry
    if (
        include_geometry
        and geometry_format == "GEOPARQUET"
        and desc.get("shapeFieldName") is not None
    ):
        sc_col_lst = [f"{desc.get('shapeFieldName')}@"] + sc_col_lst

    # create a search cursor to work through the data
    with arcpy.da.SearchCursor(str(input_features), sc_col_lst) as search_cur:
        # variable for batch numbering
        prt_num = 0

        # initialize variables for extent tracking
        x_min, y_min, x_max, y_max = None, None, None, None

        # begin to iterate through the features
        for idx, row in enumerate(search_cur):
            # pull the geometry object out of the row, and get the extent if including geometry
            if include_geometry and geometry_format == "GEOPARQUET":
                geom = row[0]
                if geom is not None and isinstance(geom, arcpy.Geometry):
                    ext = geom.extent
                    if x_min is None or ext.XMin < x_min:
                        x_min = ext.XMin
                    if y_min is None or ext.YMin < y_min:
                        y_min = ext.YMin
                    if x_max is None or ext.XMax > x_max:
                        x_max = ext.XMax
                    if y_max is None or ext.YMax > y_max:
                        y_max = ext.YMax
                else:
                    logger.warning(f"Feature at index {idx} has no geometry.")

                # remove the geometry from the row so the rest of the columns line up with the schema
                row = row[1:]

            # add each row column partition_column_list to the respective key in the dictionary
            for col, val in zip(pq_schema.names, row):
                if include_geometry and geometry_format == "H3" and col == "h3_index":
                    val = h3_value_from_geometry(
                        geom=val,
                        input_shape_type=input_shape_type,
                        h3_module=h3_module,
                        h3_resolution=h3_resolution,
                    )
                pa_dict[col].append(val)

            # if at a percent interval
            if idx % rep_range == 0:
                # report progress
                arcpy.SetProgressorPosition(idx)

                # check if cancelled, and if so, break out
                if arcpy.env.isCancelled:
                    break

            # if at a batch size (part) interval or end of dataset
            if (idx + 1) % batch_size == 0 or (idx + 1) == max_range:
                # if writing geoparquet, add the metadata to the schema
                if include_geometry and geometry_format == "GEOPARQUET":
                    # get the geoparquet header
                    geoparquet_col = "geometry"
                    gpqt_dict = get_geoparquet_header(
                        geometry_type=geom_typ,
                        encoding="WKB",
                        spatial_reference=in_sr,
                        bounding_box=(
                            [x_min, y_min, x_max, y_max]
                            if None not in (x_min, y_min, x_max, y_max)
                            else None
                        ),
                        column_name=geoparquet_col,
                    )

                    # add the spatial reference to the metadata
                    pqt_meta = {
                        b"geo": str.encode(json.dumps(gpqt_dict), encoding="utf-8")
                    }

                    # add the metadata to the schema
                    pq_schema = pq_schema.with_metadata(pqt_meta)

                # create a PyArrow table object instance from the accumulated dictionary
                pa_tbl = pa.Table.from_pydict(pa_dict, pq_schema)

                # write out the part, optionally partitioned
                if len(partition_columns) > 0:
                    pq.write_to_dataset(
                        table=pa_tbl,
                        root_path=output_parquet,
                        partition_cols=partition_columns,
                        compression="snappy",
                    )

                else:
                    # create a name and path string for the part file
                    part_nm = f"part-{uuid.uuid4().hex}-{prt_num:05d}.snappy.parquet"
                    part_pth = output_parquet / part_nm

                    pq.write_table(
                        table=pa_tbl,
                        where=str(part_pth),
                        flavor="spark_session",
                        compression="snappy",
                    )

                # increment the naming index
                prt_num += 1

                # reset the loading dictionary
                pa_dict = {col: [] for col in pq_schema.names}

                # Note: Do NOT reset extent variables - we want the bbox to reflect the entire dataset

    # reset the progress indicator
    arcpy.ResetProgressor()

    return output_parquet


def h3_index_to_geometry(h3_index: str, geometry_type: Optional[str] = "polygon"):
    """
    Create an ``arcpy.Geometry`` object from a hexadecimal H3 index.

    Args:
        h3_index: Single hexadecimal H3 index.
        geometry_type: Type of ``arcpy.Geometry`` desired, either ``polygon`` for ``arcpy.Polygon`` or ``point``
            for ``arcpy.Point`` delineating the centroid.

    Returns:
        ``arcpy.Geometry``, either ``arcpy.Polygon`` delineating the area or ``arcpy.Point`` of the centroid, depending
        on desired output defined in the input parameters.
    """
    # late import to accommodate potential for not having h3 installed in environment
    if importlib.util.find_spec("h3") is None:
        raise EnvironmentError(
            "The h3-py package does not appear to be available in the current Python environment. "
            "Creating H3 geometries from H3 indices requires the h3-py package to be installed in "
            "the current environment."
        )
    h3 = importlib.import_module("h3")
    if not (hasattr(h3, "cell_to_latlng") and hasattr(h3, "cell_to_boundary")):
        raise EnvironmentError(
            "h3_index_to_geometry requires h3-py 4.x APIs (cell_to_latlng and cell_to_boundary)."
        )

    # all coordinates are in WGS84
    sr = arcpy.SpatialReference(4326)

    # ensure geometry type is lowercase for comparisons
    geometry_type = geometry_type.lower()

    # since inner H3 errors are strange, catch at higher level
    try:
        if geometry_type == "point":
            # get the coordinates
            y, x = h3.cell_to_latlng(h3_index)

            # create a point geometry
            geom = arcpy.PointGeometry(arcpy.Point(x, y), spatial_reference=sr)

        elif geometry_type == "poly" or geometry_type == "polygon":
            # get the tuple of tuples with the bounding coordinates for the h3 index polygon boundary
            h3_boundary = h3.cell_to_boundary(h3_index)

            # switch the coordinate order for esri geometry
            h3_boundary = reversed(h3_boundary)

            # since the coordinates are y, x pairs, reverse to x, y to create Points and load into an Array
            pt_arr = arcpy.Array([arcpy.Point(x, y) for y, x in h3_boundary])

            # add the first point to the end to close the polygon
            pt_arr.append(pt_arr[0])

            # use the array to create the polygon geometry
            geom = arcpy.Polygon(pt_arr, spatial_reference=sr)

        else:
            raise ValueError(
                f'geometry_type must be one of ["point", "polygon"]. You provided "{geometry_type}"'
            )

    except:
        raise logger.warning(f'Cannot create geometry for H3 index "{h3_index}"')

    return geom


def parquet_to_features(
    parquet_path: Path,
    output_feature_class: Path,
    schema_file: Optional[Path] = None,
    geometry_format: Optional[
        Literal["GEOPARQUET", "COORDINATES", "H3"]
    ] = "GEOPARQUET",
    parquet_partitions: Optional[str] = None,
    geometry_column: Optional[Union[List[str], str]] = None,
    spatial_reference: Optional[Union[arcpy.SpatialReference, str, int]] = 4326,
    sample_count: Optional[int] = None,
    build_spatial_index: Optional[bool] = True,
    compact: Optional[bool] = True,
) -> Path:
    """Convert Parquet or GeoParquet datasets into an ArcGIS feature class.

    This function reads from a Parquet dataset (or specific partition),
    builds a destination feature class schema, then inserts rows in batches
    using an ArcPy insert cursor. It supports three geometry interpretation
    modes:

    - GEOPARQUET: Reads geometry from GeoParquet metadata + primary geometry
      column.
    - COORDINATES: Creates point geometry from user-specified X/Y columns.
    - H3: Creates polygon geometry from an H3 index column.

    !!! note
        Complex Arrow types (arrays/structs) are stringified for import.

    !!! note
        For GeoParquet with multiple geometry columns, only the metadata-defined
        primary column is used for geometry creation.

    !!! warning
        If a schema_file is supplied with mismatched field names or insufficient
        text lengths, field creation may still succeed but can truncate values
        depending on geodatabase rules.

    ``` python
    out_fc = parquet_to_features(
        parquet_path=r"data/sample/geoparquet_example",
        output_feature_class=r"data/interim/interim.gdb/geoparquet_example",
        geometry_format="GEOPARQUET",
        build_spatial_index=True,
    )
    ```

    Args:
        parquet_path: Path to a Parquet dataset directory or individual Parquet
            part file.
        output_feature_class: Destination feature class path (typically in a
            file geodatabase).
        schema_file: Optional CSV schema definition used to control field
            aliases, lengths, and types during field creation.
        geometry_format: Geometry interpretation mode: GEOPARQUET,
            COORDINATES, or H3.
        parquet_partitions: Optional hive-style partition selector, for example
            year=2023/month=01.
        geometry_column: Geometry source column definition. Ignored for
            GEOPARQUET. Required for COORDINATES (x/y pair) and H3 (single
            column name).
        spatial_reference: Target spatial reference. For GEOPARQUET, metadata
            SR is preferred when present.
        sample_count: Optional row limit for partial imports.
        build_spatial_index: If True, build a spatial index on the output
            feature class after load.
        compact: If True, compact the destination geodatabase after load.

    Returns:
        Path: Output feature class path.

    Raises:
        ValueError: If geometry_format is unsupported.
        ValueError: If parquet_partitions is invalid or not present.
        ValueError: If required geometry columns are missing or incompatible
            with the selected geometry_format.
        ValueError: If GeoParquet metadata is required but missing/inconsistent
            when geometry_format is GEOPARQUET.
    """
    # if used as a tool in pro, let user know what is going on...kind of
    arcpy.SetProgressorLabel("Warming up the Flux Capacitor...")

    # ensure geometry format is uppercase for comparisons
    geometry_format = geometry_format.upper()

    # ensure goemtry format is valid
    if geometry_format not in ("GEOPARQUET", "COORDINATES", "H3"):
        raise ValueError(
            f'geometry_format must be one of ["GEOPARQUET", "COORDINATES", "H3"]. You provided "{geometry_format}".'
        )

    # ensure coordinates are provided if using COORDINATES geometry type
    if geometry_format == "COORDINATES" and not isinstance(
        geometry_column, (tuple, list)
    ):
        raise ValueError(
            "If using COORDINATES as the geometry type, you must provide an iterable (list or tuple) "
            "of the x and y column names for the coordinates."
        )

    # make sure paths...are paths
    parquet_path = (
        parquet_path if isinstance(parquet_path, Path) else Path(parquet_path)
    )
    output_feature_class = (
        output_feature_class
        if isinstance(output_feature_class, Path)
        else Path(output_feature_class)
    )

    def _open_parquet_dataset(
        dataset_path: Path, filters: Optional[list[tuple]] = None
    ) -> pq.ParquetDataset:
        """Open ParquetDataset with Hive partitioning first, then safe fallback."""
        try:
            return pq.ParquetDataset(dataset_path, filters=filters)
        except pa.ArrowTypeError as err:
            logger.warning(
                "Encountered ArrowTypeError while loading ParquetDataset with Hive partitioning: %s. "
                "Falling back to partitioning=None.",
                err,
            )
            return pq.ParquetDataset(dataset_path, filters=filters, partitioning=None)

    # create a PyArrow Dataset to read from
    dataset = _open_parquet_dataset(parquet_path)
    # dataset = ds.dataset(parquet_path, format="parquet")

    # get the list of available partitions in the parquet data
    available_partitions = pyarrow_utils.get_partition_dicts(dataset)

    # if parquet partitions were provided, but not available in the data, raise an error
    if parquet_partitions is not None and len(available_partitions) == 0:
        raise ValueError(
            "The provided parquet_partitions do not appear to be available in the input data. "
            "The input data does not appear to be partitioned."
        )

    # if parquet partitions were provided, ensure they are valid
    elif parquet_partitions is not None:
        # split partition string into a list (using Path enables both forward and back slashes)
        partition_parts = (prt.split("=") for prt in Path(parquet_partitions).parts)

        # create a dictionary of the partition parts
        partition_dict = {k: pyarrow_utils.format_value(v) for k, v in partition_parts}

        if partition_dict not in available_partitions:
            raise ValueError(
                f"The provided parquet_partitions do not appear to be available in the input data. "
                f"Available partitions include: "
                f"{', '.join(pyarrow_utils.partition_path_from_dict(p) for p in available_partitions)}"
            )

        # create a filter expression from the provided partition dictionary
        fltr = pyarrow_utils.get_partition_expression(partition_dict)

        # prefer opening the specific partition path directly to avoid partition inference issues
        partition_path = parquet_path / Path(parquet_partitions)
        if partition_path.exists():
            dataset = _open_parquet_dataset(partition_path)
        else:
            # fallback to dataset filtering if partition subpath is not directly addressable
            dataset = _open_parquet_dataset(parquet_path, filters=fltr)

    # get a dictionary for handling the schema properties
    schema_dict = pyarrow_utils.get_schema_dict(dataset.schema)

    # check the schema for any columns with complex data types
    complex_cols = pyarrow_utils.get_complex_columns(dataset)

    # introspect geometry columns from the geoparquet metadata
    primary_geom_col, all_geom_cols = (
        pyarrow_utils.introspect_geoparquet_geometry_columns(dataset.schema)
    )

    # if any complex columns, create a table with just those columns to interrogate
    if len(complex_cols) > 0:
        # initialize dictionary to store max lengths
        complex_max_lengths = {col: 0 for col in complex_cols}

        # initialize the reader
        reader = dataset.read(columns=complex_cols)

        # iterate over string columns
        for col in complex_cols:
            column = reader.column(col)

            # iterate chunks in the column
            for chunk in column.chunks:
                max_len = max(
                    (
                        len(str(val))
                        for val in pa.array(
                            [str(x.as_py()) for x in chunk], type=pa.string()
                        )
                        if val is not None
                    ),
                    default=0,
                )
                complex_max_lengths[col] = max(complex_max_lengths[col], max_len)

        # update the schema dictionary with the max lengths and data type (string) for complex columns
        for complex_col in complex_cols:
            old_val = schema_dict.get(complex_col)
            new_val = {
                "arcpy_name": old_val.get("arcpy_name"),
                "arcpy_type": "string",
                "type": "string",
                "length": complex_max_lengths.get(complex_col),
            }
            schema_dict[complex_col] = new_val

    # extract the spatial reference from the parquet metadata if using geoparquet
    if geometry_format == "GEOPARQUET":
        # validate that this is actually geoparquet data
        if primary_geom_col is None:
            raise ValueError(
                "The dataset does not appear to be formatted as GeoParquet. No geometry metadata was found. "
                "If you want to import from coordinate columns or H3, please specify the geometry_format parameter."
            )

        # validate that the primary geometry column exists in the schema
        if primary_geom_col not in dataset.schema.names:
            raise ValueError(
                f"The primary geometry column '{primary_geom_col}' from the GeoParquet metadata does not "
                f"exist in the dataset schema. Available columns: {', '.join(dataset.schema.names)}"
            )

        # log if there are multiple geometry columns
        if len(all_geom_cols) > 1:
            logger.info(
                f"Multiple geometry columns detected in GeoParquet metadata: {', '.join(all_geom_cols)}. "
                f"Using primary column '{primary_geom_col}'. Other geometry columns will be ignored."
            )

        # get the geometry information from the parquet metadata if available
        geo_dict = pyarrow_utils.get_geoparquet_metadata(dataset.schema)

        # get the geometry type for the output feature class and the cursor geometry token
        geom_typ = pyarrow_utils.get_geometry_type(geo_dict)

        # get the primary geometry column name
        geometry_column = primary_geom_col
        pa_cursor_geom = [geometry_column]

        # set the cursor geometry token
        arcpy_cursor_geom = "SHAPE@WKB"

        # get the spatial reference from the metadata if available
        spatial_reference = pyarrow_utils.get_spatial_reference(geo_dict)

        # remove any geometry columns from the schema dictionary so they don't get added as fields
        for geom_col in all_geom_cols:
            if geom_col in schema_dict:
                schema_dict.pop(geom_col)

    # set the geometry type and cursor geometry token for H3
    elif geometry_format == "H3":
        geom_typ = "POLYGON"
        pa_cursor_geom = [geometry_column]
        arcpy_cursor_geom = "SHAPE@WKB"

        # validate the H3 column is provided
        if geometry_column is None:
            raise ValueError(
                "When using H3 as the geometry format, you must provide the geometry_column parameter "
                "with the name of the H3 index column."
            )

        # ensure the geometry column is in the input data
        if geometry_column not in dataset.schema.names:
            raise ValueError(
                f"The H3 geometry_column '{geometry_column}' does not appear to be in the input parquet columns. "
                f"Available columns: {', '.join(dataset.schema.names)}"
            )

        # warn if this appears to be geoparquet data but user is using H3
        if primary_geom_col is not None:
            logger.warning(
                f"The dataset appears to be GeoParquet with geometry column '{primary_geom_col}', "
                f"but you are using H3 format with column '{geometry_column}'. Make sure this is intentional."
            )

    # if working with coordinates, set the geometry type to points, set the cursor geometry token and validate
    elif geometry_format == "COORDINATES":
        geom_typ = "POINT"
        pa_cursor_geom = list(geometry_column)
        arcpy_cursor_geom = "SHAPE@XY"

        if not isinstance(geometry_column, (tuple, list)) or len(geometry_column) != 2:
            raise ValueError(
                "If using COORDINATES as the geometry type, you must provide an iterable (list or tuple) "
                "of the x and y column names for the coordinates."
            )

        # ensure coordinate columns are in input data
        if (
            geometry_column[0] not in dataset.schema.names
            or geometry_column[1] not in dataset.schema.names
        ):
            raise ValueError(
                f"The geometry_column names provided for the coordinate columns do not appear to be in "
                f"the input parquet columns. Available columns: {', '.join(dataset.schema.names)}"
            )

        # ensure the coordinate columns are numeric types
        for coord_col in geometry_column:
            col_type = str(next(c.type for c in dataset.schema if c.name == coord_col))
            if col_type not in (
                "int8",
                "int16",
                "int32",
                "int64",
                "float",
                "double",
                "decimal",
            ):
                raise ValueError(
                    f"The geometry_column '{coord_col}' does not appear to be a numeric type. "
                    "Coordinate columns must be one of the following types: "
                    "int8, int16, int32, int64, float, double, or decimal."
                )

        # warn if this appears to be geoparquet data but user is using coordinates
        if primary_geom_col is not None:
            logger.warning(
                f"The dataset appears to be GeoParquet with geometry column '{primary_geom_col}', "
                f"but you are using COORDINATES format with columns {geometry_column}. Make sure this is intentional."
            )

    # if spatial reference is a WKID create a spatial reference object
    if isinstance(spatial_reference, str) and spatial_reference.isnumeric():
        spatial_reference = int(spatial_reference)

    if isinstance(spatial_reference, int):
        spatial_reference = arcpy.SpatialReference(spatial_reference)

    # hydrate the property dictionary to use when adding fields
    prop_dict_lst = []

    # if a schema file is not provided read from the input data, introspect string lengths
    if schema_file is None:
        # get the maximum string lengths from the parquet data for use in field creation
        max_len_dict = get_parquet_max_string_lengths(dataset)

        # iterate the schema dictionary to create field property dictionaries for building the feature class
        for nm, prop in schema_dict.items():
            # get the field type from the mapping dictionary, defaulting to text if not found
            fld_typ = pyarrow_utils.import_dtype_dict.get(prop["type"], "TEXT")

            # create the property dictionary for adding a field
            prop_dict = dict(
                field_name=prop.get("arcpy_name"),
                field_type=fld_typ,
                field_alias=nm,
                field_is_nullable="NULLABLE",
            )

            # if the field type is text, add length to the property dictionary
            if fld_typ == "TEXT":
                # get the introspected length if available, defaulting to 512 if not found
                fld_len = max_len_dict.get(nm, 512)

                # create the property dictionary for adding a text field
                prop_dict["field_length"] = int(fld_len)

            # append the property dictionary to the list
            prop_dict_lst.append(prop_dict)

    # if a schema file is provided, use it to build the field properties
    else:
        # if a directory for the schema is provided, get the enclosed csv file
        if schema_file.is_dir() and schema_file.stem == "schema":
            # try to get the csv file
            csv_lst = list(schema_file.glob("*.csv"))
            if len(csv_lst) > 0:
                schema_file = csv_lst[0]

        # read the csv into a Pandas DataFrame
        schema_df = pd.read_csv(
            schema_file,
            usecols=["field_name", "field_alias", "field_length", "field_type"],
        )

        # swap out the string types for text so add field works
        schema_df.loc[
            (schema_df["field_type"].str.lower() == "string")
            | (schema_df["field_type"].str.lower() == "str"),
            "field_type",
        ] = "TEXT"

        # get a list of string fields without a length defined
        schema_string_cols = [
            col.name
            for col in dataset.schema
            if str(col.type) in ("string", "utf8", "long_string", "long_utf8")
        ]
        missing_len_fld_lst = [
            col
            for col in schema_string_cols
            if col not in list(schema_df["field_name"])
        ]

        if len(missing_len_fld_lst) > 0:
            logger.warning(
                f"The following string fields do not have a length defined in the schema file and will default to "
                f"512 characters: {', '.join(missing_len_fld_lst)}"
            )

        # create a dictionary of properties read from the csv file
        csv_dict = {
            r.field_name: {"length": r.field_length, "alias": r.field_alias}
            for r in schema_df.itertuples()
        }

        # iterate the schema dictionary to create field property dictionaries for building the feature class
        for nm, prop in schema_dict.items():
            # get the field type from the mapping dictionary, defaulting to text if not found
            fld_typ = pyarrow_utils.import_dtype_dict.get(prop["type"], "TEXT")

            # create the property dictionary for adding a field
            prop_dict = dict(
                field_name=prop.get("arcpy_name"),
                field_type=fld_typ,
                field_alias=csv_dict.get(nm, {}).get("alias", nm),
                field_is_nullable="NULLABLE",
            )

            # if the field type is text, add length to the property dictionary
            if fld_typ == "TEXT":
                # get the length from the csv if available, defaulting to 512 if not found
                fld_len = csv_dict.get(nm, {}).get("length", 512)

                # create the property dictionary for adding a text field
                prop_dict["field_length"] = int(fld_len)

            # append the property dictionary to the list
            prop_dict_lst.append(prop_dict)

    # ensure the geometry format is valid
    geom_typ = geom_typ.upper()
    if geom_typ not in pyarrow_utils.geom_dict.keys():
        raise ValueError(
            f'geometry_format must be one of {", ".join(pyarrow_utils.geom_dict.keys())}.'
        )

    # get the geometry properties for creating the feature class
    geom_properties = pyarrow_utils.geom_dict.get(geom_typ)

    # create the new feature class
    arcpy.management.CreateFeatureclass(
        out_path=str(output_feature_class.parent),
        out_name=output_feature_class.name,
        spatial_reference=spatial_reference,
        **geom_properties,
    )

    logger.info(f"Created feature class at {str(output_feature_class)}")

    # add the fields to the feature class
    for prop_dict in prop_dict_lst:
        arcpy.management.AddField(in_table=str(output_feature_class), **prop_dict)

        # for logging progress
        log_dict = dict()
        log_dict["in_table"] = str(output_feature_class)
        log_dict = {**log_dict, **prop_dict}

        # log progress
        logger.debug(f"Field added to Feature Class {log_dict}")

    # interrogate the ACTUAL column names - depending on database, names can get truncated, but alias will be correct
    fc_fld_dict = {
        c.aliasName: c.name
        for c in arcpy.ListFields(str(output_feature_class))
        if c.aliasName in schema_dict.keys()
    }

    # create the list of feature class columns for the insert cursor and for row lookup from parquet from pydict object
    pa_col_lst = list(fc_fld_dict.keys()) + pa_cursor_geom
    arcpy_col_lst = list(fc_fld_dict.values()) + [arcpy_cursor_geom]

    # this prevents pyarrow from getting hung up
    arcpy.env.autoCancelling = False

    # set up so progress is communicated to user
    arcpy.SetProgressorLabel("Importing rows...")

    # variable to track completed count
    added_cnt = 0

    # variable to track fail count
    fail_cnt = 0

    # create a cursor for inserting rows
    with arcpy.da.InsertCursor(
        str(output_feature_class), arcpy_col_lst
    ) as insert_cursor:
        # flag for if at sample count and need to break out of loop
        at_sample_count = False

        # variable to track start time
        start_time = time.time()

        # get the total number of rows in the dataset using just one column
        ds_row_cnt = dataset.read(columns=pa_col_lst[:1]).num_rows

        logger.info(f"Starting to import {ds_row_cnt:,} rows from parquet data.")

        # initialize the tqdm progress bar
        tqdm_progressor = tqdm(total=ds_row_cnt, unit="rows", desc="Importing rows")

        # create a table to batch through the data
        pa_tbl = dataset.read(columns=pa_col_lst)

        # iterate scanner batches
        for batch in pa_tbl.to_batches(max_chunksize=30000):
            # handle any complex data types, converting to strings, in the batch table
            batch = pyarrow_utils.stringify_complex_columns(batch, complex_cols)

            # pull the parquet data into a dict
            pqt_pydict = batch.to_pydict()

            # transpose the dictionary of lists into a list of dictionaries
            dict_lst = [
                dict(zip(pqt_pydict.keys(), values))
                for values in zip(*pqt_pydict.values())
            ]

            # for every row index in the number of rows
            for pqt_idx, row_pydict in enumerate(dict_lst):
                # instantiate the row variable so error messages can be formatted.
                row = None

                # try to add the row
                try:
                    # populate the row dictionary with values from the partition dict to match the insert cursor columns
                    row_dict = {k: row_pydict.get(k) for k in pa_col_lst}

                    # create a row by plucking out the values from the parquet pydict in the correct order
                    row = list(row_dict.values())

                    # if the geometry is being generated from coordinate columns, create the coordinate tuple
                    if geometry_format == "COORDINATES":
                        geom = (
                            row_pydict[geometry_column[0]],
                            row_pydict[geometry_column[1]],
                        )

                        # add the geometry to the row
                        row = row + [geom]

                    # if geometry created from H3 index, create the geometry
                    elif geometry_format == "H3":
                        geom = h3_index_to_geometry(row_pydict[geometry_column])

                        # add the geometry to the row
                        row = row + [geom]

                    # insert the row
                    insert_cursor.insertRow(row)

                    # update the completed count
                    added_cnt += 1

                    # update the tqdm progressor
                    tqdm_progressor.update()

                # if cannot add the row
                except Exception as e:
                    # handle case of having issues prior to even getting the row
                    if row is None:
                        logger.error(
                            f"Could create row object for parquet row index {pqt_idx}.\npydict: {pqt_pydict}\n"
                            f"row_dict: {row_dict}\n\nMessage: {e}"
                        )
                        raise

                    else:
                        # update the fail count
                        fail_cnt += 1

                        # make sure the reason is tracked
                        logger.warning(
                            f"Could not import row.\n\nContents:{row}\n\nMessage: {e}"
                        )

                # check of at sample count
                if added_cnt == sample_count:
                    at_sample_count = True
                    break

                # provide status updates every 1000 features, and provide an exit if cancelled
                if added_cnt % 1000 == 0:
                    if arcpy.env.isCancelled:
                        break

                    # find the elapsed time
                    elapsed_time = time.time() - start_time

                    # calculate the rate per hour
                    rate = round(added_cnt / elapsed_time * 3600)

                    # get the remaining count
                    remaining_cnt = ds_row_cnt - added_cnt

                    # calculate the per record time
                    per_record_time = elapsed_time / added_cnt if added_cnt > 0 else 0

                    # estimate the remaining time
                    est_remain_time = timedelta(
                        seconds=round(remaining_cnt * per_record_time)
                    )

                    # format remaining time as a string, even if more than a day
                    if est_remain_time.days > 0:
                        remain_str = f"{est_remain_time.days} days, {str(timedelta(seconds=est_remain_time.seconds))}"
                    else:
                        remain_str = str(timedelta(seconds=est_remain_time.seconds))

                    # build the message
                    msg = (
                        f"Imported {added_cnt:,} rows at a rate of {rate:,} per hour. Estimated time "
                        f"remaining: {remain_str}."
                    )

                    # set the progressor label
                    arcpy.SetProgressorLabel(msg)

                # provide messages every 10,000 features
                # if added_cnt % 10000 == 0:
                #     logger.info(msg)

            # ensure next batch is not run if cancelled or only running a sample
            if arcpy.env.isCancelled or at_sample_count:
                break

    # declare success, and track failure if necessary
    success_msg = f"Successfully imported {added_cnt:,} rows."
    arcpy.SetProgressorLabel(success_msg)
    tqdm_progressor.close()
    arcpy.ResetProgressor()
    logger.info(success_msg)

    if fail_cnt > 0:
        fail_msg = f"Failure count: {fail_cnt:,}"
        logger.warning(fail_msg)

    # build spatial index if requested
    if build_spatial_index:
        arcpy.SetProgressorLabel("Building spatial index.")
        arcpy.management.AddSpatialIndex(str(output_feature_class))
        logger.info("Completed building spatial index.")

    # if compacting, do it
    if compact:
        arcpy.SetProgressorLabel("Compacting data.")
        arcpy.management.Compact(str(output_feature_class.parent))
        logger.info("Successfully compacted data.")

    return output_feature_class
