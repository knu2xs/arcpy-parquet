import json
from pathlib import Path
from typing import Optional, Union, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from osgeo import osr

from arcpy_parquet.utils import get_logger

# set up logging
logger = get_logger(logger_name="arcpy_parquet.utils.pyarrow_utils", level="DEBUG")

# mapping data types for going from pyarrow to a feature class
import_dtype_dict = {
    "int8": "INTEGER",
    "int16": "INTEGER",
    "int32": "INTEGER",
    "int64": "LONG",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "times": "DATE",
    "decim": "DOUBLE",
    "decimal": "DOUBLE",
    "string": "TEXT",
    "long_string": "TEXT",
    "utf8": "TEXT",
    "long_utf8": "TEXT",
    "date": "DATE",
    "date32[day]": "DATE",
}

# dictionary for handling input geometry types
geom_dict = {
    "COORDINATES": {"geometry_type": "POINT", "has_m": "DISABLED", "has_z": "DISABLED"},
    "H3": {"geometry_type": "POLYGON", "has_m": "DISABLED", "has_z": "DISABLED"},
    "POINT": {"geometry_type": "POINT", "has_m": "DISABLED", "has_z": "DISABLED"},
    "POINT M": {"geometry_type": "POINT", "has_m": "ENABLED", "has_z": "DISABLED"},
    "POINT Z": {"geometry_type": "POINT", "has_m": "DISABLED", "has_z": "ENABLED"},
    "POLYLINE": {"geometry_type": "POLYLINE", "has_m": "DISABLED", "has_z": "DISABLED"},
    "POLYLINE M": {
        "geometry_type": "POLYLINE",
        "has_m": "ENABLED",
        "has_z": "DISABLED",
    },
    "POLYLINE Z": {
        "geometry_type": "POLYLINE",
        "has_m": "DISABLED",
        "has_z": "ENABLED",
    },
    "POLYGON": {"geometry_type": "POLYGON", "has_m": "DISABLED", "has_z": "DISABLED"},
    "POLYGON M": {"geometry_type": "POLYGON", "has_m": "ENABLED", "has_z": "DISABLED"},
    "POLYGON Z": {"geometry_type": "POLYGON", "has_m": "DISABLED", "has_z": "ENABLED"},
    "MULTIPOINT": {
        "geometry_type": "MULTIPOINT",
        "has_m": "DISABLED",
        "has_z": "DISABLED",
    },
    "MULTIPOINT M": {
        "geometry_type": "MULTIPOINT",
        "has_m": "ENABLED",
        "has_z": "DISABLED",
    },
    "MULTIPOINT Z": {
        "geometry_type": "MULTIPOINT",
        "has_m": "DISABLED",
        "has_z": "ENABLED",
    },
}

# template for the geoparquet metadata
geoparquet_metadata_template = {
    "version": "1.0.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": {
                "type": "GeographicCRS",
                "name": "GCS WGS 1984",
                "bbox": {
                    "east_longitude": 180.0,
                    "west_longitude": -180.0,
                    "south_latitude": -90.0,
                    "north_latitude": 90.0,
                },
                "datum": {
                    "type": "GeodeticReferenceFrame",
                    "name": "D WGS 1984",
                    "ellipsoid": {
                        "name": "WGS 1984",
                        "semi_major_axis": 6378137.0,
                        "inverse_flattening": 298.257223563,
                    },
                    "prime_meridian": {"name": "Greenwich", "longitude": 0.0},
                    "id": {"authority": "EPSG", "code": 6326},
                },
                "coordinate_system": {
                    "subtype": "ellipsoidal",
                    "axis": [
                        {
                            "name": "Latitude",
                            "abbreviation": "lat",
                            "direction": "north",
                            "unit": "degree",
                        },
                        {
                            "name": "Longitude",
                            "abbreviation": "lon",
                            "direction": "east",
                            "unit": "degree",
                        },
                    ],
                },
                "area": "World (by country)",
                "id": {"authority": "EPSG", "code": 4326},
            },
            "bbox": [
                -86.30001068115234,
                25.772332637530994,
                -80.19155100671779,
                32.6189679949766,
            ],
        }
    },
}


def get_complex_columns(
    table_or_dataset: Union[pa.Table, pq.ParquetDataset]
) -> list[str]:
    """
    Get a list of complex (nested) columns in a PyArrow Table.

    !!! note
        Simple scalar columns are not included.

    Args:
        table_or_dataset: Input PyArrow Table.

    Returns:
        List of complex column names.
    """
    complex_cols = [
        fld.name for fld in table_or_dataset.schema if pa.types.is_nested(fld.type)
    ]

    logger.warning(f"Complex columns detected in dataset: {complex_cols}")

    return complex_cols


def stringify_complex_columns(
    table: pa.Table, complex_columns: Optional[list[str]] = None
) -> pa.Table:
    """
    Convert all complex (nested) columns in a PyArrow Table to strings.
    Simple scalar columns remain unchanged.

    Args:
        table: Input PyArrow Table.
        complex_columns: Optional list of complex column names to convert. If None, all complex columns will be
            identified and converted.

    Returns:
        New table with complex columns converted to strings.
    """
    # get a list of any complex columns if not provided
    if complex_columns is None:
        complex_cols = get_complex_columns(table)

    # if complex columns provided, ensure they are valid
    else:
        complex_cols = [
            col
            for col in complex_columns
            if col in table.schema.names and pa.types.is_nested(table[col].type)
        ]

    # if complex columns
    if len(complex_cols) > 0:
        # list to hold new columns
        new_columns = []

        # iterate the column names
        for col_name in table.schema.names:
            # get the column object
            col = table[col_name]

            # if a complex column
            if col_name in complex_cols:
                # convert complex column to string
                arr = pa.array(
                    [str(x.as_py()).encode("utf-8") for x in col], type=pa.string()
                )

            # if not a complex column, keep original
            else:
                arr = col

            # add to new columns list
            new_columns.append(arr)

        # assemble the new table with the same column names
        table = pa.table(new_columns, names=table.schema.names)

        # logger.warning(f"Complex columns converted to strings: {table}")

    return table


def get_schema_dict(schema: pa.Schema) -> dict:
    """
    Get a dictionary of column names, output data types and aliases from a PyArrow Schema.

    Args:
        schema: Input PyArrow Schema.

    Returns:
        Dictionary with column names as keys and a dictionary with 'type', 'arcpy_type' and 'arcpy_name' as values.
    """
    # create a dictionary to hold the column info
    column_dict = {}

    # iterate the schema columns
    for col in schema:
        # prefix the column name with "c" if the column name starts with a number
        arcpy_name = (
            f"c{col.name}"
            if col.name[0].isdigit() or col.name.startswith("delta")
            else col.name
        )

        # get the output data type
        arcpy_typ = str(col.type)

        column_dict[col.name] = {
            "type": str(col.type),
            "arcpy_type": arcpy_typ,
            "arcpy_name": arcpy_name,
        }

    return column_dict


def get_partition_dicts(dataset: Union[str, Path, pq.ParquetDataset], all_combinations: bool = True) -> list[dict[str, int]]:
    """
    Get a list of partition dictionaries from a PyArrow ParquetDataset.

    Args:
        dataset: Input PyArrow ParquetDataset.
        all_combinations: If True, return all combination levels of partition keys. If False, return only
            full partition keys.

    Returns:
        List of dictionaries with partition keys and values.
    """
    # ensure the dataset is a parquet dataset
    if isinstance(dataset, (Path, str)):
        dataset = pq.ParquetDataset(dataset)

    # get the fragments from the dataset
    fragments = dataset.fragments

    partition_set = set()

    # add all the partition expressions as strings to a set to get unique values
    for fragment in fragments:
        expr = fragment.partition_expression
        expr_str = str(expr)
        partition_set.add(expr_str)

    partition_lst = []

    # helper function to clean up strings
    clean_str = lambda s: s.strip().lstrip('(').rstrip(')').strip() if isinstance(s, str) else s

    # parse these out into the discrete parts
    for expr_str in partition_set:
        parts = clean_str(expr_str).split("and")
        partition = {}
        for part in parts:
            key, _, value = part.partition("==")
            key = clean_str(key)
            value = value if value is None else clean_str(value)
            # if the value is a string, remove any extra quotes
            if isinstance(value, str):
                value = value.strip().strip("'").strip('"')
            # add to the partition dictionary
            partition[key] = value
        partition_lst.append(partition)

    # if all combinations, generate all combinations of the partition dictionaries
    if all_combinations:
        partition_lst = generate_combinations(partition_lst)

    return partition_lst


def get_partition_strings(dataset: Union[str, Path, pq.ParquetDataset], all_combinations: bool = True) -> list[str]:
    """
    Get a list of partition path strings from a PyArrow ParquetDataset.

    Args:
        dataset: Input PyArrow ParquetDataset.
        all_combinations: If True, return all combination levels of partition keys. If False, return only
            full partition keys.
    Returns:
        List of partition path strings.
    """
    partition_dicts = get_partition_dicts(dataset, all_combinations=all_combinations)
    partition_strings = [
        partition_path_from_dict(partition_dict) for partition_dict in partition_dicts
    ]
    return partition_strings


    if isinstance(dataset, pq.ParquetDataset):
        partition_dicts = get_partition_dicts(dataset, all_combinations=all_combinations)
        partition_strings = [
            partition_path_from_dict(partition_dict) for partition_dict in partition_dicts
        ]
    else:
        # ensure dataset is a path
        dataset = Path(dataset)

        # get parent and iteratively all parent with subsequent children combinations
        if all_combinations:

            # list to hold the partition strings
            partition_strings = []

            # get all the parent directories and their children
            parent_parts = [p.parent for p in dataset.rglob("*") if p.is_dir()]

            # iterate the parents and their children to get all combinations
            for parent in parent_parts:

                # iterate the children of each parent
                for child in parent.rglob("*"):

                    # if a directory, add to the list
                    if child.is_dir():

                        # get the relative path from the dataset root
                        rel_path = str(child.relative_to(dataset))

                        # if not already in the list, add it
                        if rel_path not in partition_strings:
                            partition_strings.append(rel_path)

         # get all the partition directories, without combinations
        else:
            partition_strings = [
                str(p.relative_to(dataset)) for p in dataset.rglob("*") if p.is_dir()
            ]

    return partition_strings


def generate_combinations(dict_list: list[dict]) -> list[dict]:
    """
    Generate all combinations of dictionaries from a list of dictionaries.

    Args:
        dict_list: List of dictionaries with the same keys.

    Returns:
        List of dictionaries with all combinations of keys and values.
    """

    # Extract all keys from the first dictionary (assuming consistent keys)
    keys = list(dict_list[0].keys())

    # Create combinations of keys from depth 1 to full depth
    key_combinations = []
    for i in range(1, len(keys) + 1):
        key_combinations.extend([keys[:i]])

    # Use a set to avoid duplicate dictionaries
    result_set = set()
    result = []

    for key_combo in key_combinations:
        for d in dict_list:
            new_dict = {k: d[k] for k in key_combo}
            # Convert to tuple of items to make it hashable
            dict_tuple = tuple(new_dict.items())
            if dict_tuple not in result_set:
                result_set.add(dict_tuple)
                result.append(new_dict)

    return result


def partition_path_from_dict(partition_dict: dict) -> str:
    """
    Create a partition path string from a dictionary of partition keys and values.

    Args:
        partition_dict: Dictionary with partition keys and values.

    Returns:
        Partition path string.
    """
    parts = [f"{key}={value}" for key, value in partition_dict.items()]
    partition_path = str(Path("/".join(parts)))
    return partition_path


def format_value(value) -> Union[int, str, float]:
    """
    Format a value for use in a partition dictionary.

    Args:
        value: Input value.

    Returns:
        Formatted value in correct data type.
    """
    if isinstance(value, str):
        if value.isnumeric() and "." in value and len(value.split(".")) == 2:
            value = float(value)
        elif value.isnumeric():
            value = int(value)
    return value


def partition_dict_from_path(partition_path: str) -> dict:
    """
    Create a partition dictionary from a partition path string.

    Args:
        partition_path: Partition path string.

    Returns:
        Dictionary with partition keys and values.
    """
    parts = [prt.split("=") for prt in Path(partition_path).parts]
    partition_dict = {key: format_value(val) for key, val in parts}
    return partition_dict


def get_partition_expression(
    partition_dict: dict,
) -> Union[list[tuple], list[list[tuple]]]:
    """
    Create a partition expression string from a dictionary of partition keys and values.

    Args:
        partition_dict: Dictionary with partition keys and values.
    Returns:
        Partition expression list for filtering a Parquet dataset.
    """
    expr = [(key, "=", value) for key, value in partition_dict.items()]
    return expr


def get_geoparquet_metadata(schema: pa.Schema) -> dict:
    """
    Get the GeoParquet metadata from a PyArrow Schema.

    Args:
        schema: Input PyArrow Schema.
    Returns:
        Dictionary with GeoParquet metadata. Returns empty dictionary if not found.
    """
    # get the geometry information from the parquet metadata if available
    geo_str = schema.metadata.get(b"geo")

    # if no geometry metadata, raise an error
    if geo_str is None:
        raise ValueError(
            "The dataset does not appear to be formatted as Geoparquet. No geometry metadata was found."
        )

    # get the geometry metadata as a dictionary
    geo_dict = json.loads(geo_str.decode("utf-8"))

    return geo_dict


def get_primary_geometry_column_metadata(geo_metadata: dict) -> dict:
    """
    Get the primary geometry column from a GeoParquet schema.

    Args:
        geo_metadata: Input GeoParquet metadata from the schema.

    Returns:
        Primary geometry column metadata.
    """
    # get the geometry column
    geometry_column = geo_metadata.get("primary_column")

    # get the primary geometry column metadata
    col_meta = geo_metadata.get("columns", {}).get(geometry_column)
    if col_meta is None:
        raise ValueError(
            "The input parquet data does not appear to be formatted as Geoparquet. "
            "No geometry column metadata was found."
        )

    return col_meta


def get_geometry_type(geo_metadata: dict) -> str:
    """
    Get the geometry type from a GeoParquet schema.

    Args:
        geo_metadata: Input GeoParquet metadata from the schema.

    Returns:
        Geometry type as a string.
    """
    # get the primary geometry column metadata
    col_meta = get_primary_geometry_column_metadata(geo_metadata)

    # extract the geometry types from the metadata
    geometry_types = [typ.lower() for typ in col_meta.get("geometry_types")]

    # determine the geometry type to use
    if "multipoint" in geometry_types:
        geometry_type = "MULTIPOINT"
    elif "point" in geometry_types:
        geometry_type = "POINT"
    elif "multilinestring" in geometry_types or "linestring" in geometry_types:
        geometry_type = "POLYLINE"
    elif "multipolygon" in geometry_types or "polygon" in geometry_types:
        geometry_type = "POLYGON"
    elif len(geometry_types) > 0:
        raise ValueError(
            f"Geometry type not supported {geometry_types}. Can only handle Point, MultiPoint, "
            f"LineString, MultiLineString, Polygon or MultiPolygon geometry types."
        )
    else:
        raise ValueError("No geometry types were found for the primary_column.")

    return geometry_type


def get_spatial_reference(geo_metadata: dict) -> int:
    """
    Get the EPSG code from a GeoParquet schema.

    Args:
        geo_metadata: Input GeoParquet metadata from the schema.
    Returns:
        EPSG code (WKID) as an integer. Returns 0 if not found.
    """
    # default to 0 (unknown)
    wkid = 0

    # get the primary geometry column metadata
    col_meta = get_primary_geometry_column_metadata(geo_metadata)

    # extract the spatial reference from the metadata if available
    if col_meta.get("crs") is not None:
        # get the spatial reference id information
        crs_id = col_meta.get("crs", {}).get("id")

        # if spatial reference is defined, try to use it
        if crs_id is not None:
            # if the crs is an OGC code
            if crs_id.get("authority") == "OGC" and crs_id.get("code") == "CRS84":
                wkid = 4326

            # if the crs is an EPSG code
            elif crs_id.get("authority") == "EPSG" and crs_id.get("code") is not None:
                wkid = int(crs_id.get("code"))

            # if the crs is any other OGC code, cannot look up in arcpy
            else:
                raise ValueError(
                    f"The defined coordinate system {crs_id}, cannot be looked up. Please provide "
                    f"the spatial reference to use using the spatial_reference parameter."
                )

                # TODO: potentially add support for looking up other OGC codes using pyproj if package is available

        # if no spatial reference defined, default to wgs84
        else:
            wkid = 4326
            logger.warning(
                "The coordinate reference system ID found in the metadata is not defined. Defaulting to "
                "EPSG:4326 (WGS84)."
            )

    return wkid


def get_geometry_columns(geo_metadata: dict) -> list[str]:
    """
    Get all geometry column names from a GeoParquet schema.

    Args:
        geo_metadata: Input GeoParquet metadata from the schema.

    Returns:
        List of geometry column names.
    """
    geom_cols = [col for col, meta in geo_metadata.get("columns", {}).items()]
    return geom_cols


def introspect_geoparquet_geometry_columns(
    schema: pa.Schema,
) -> tuple[Optional[str], list[str]]:
    """
    Introspect geometry columns from a GeoParquet schema.

    !!! note
        This function will attempt to extract the geometry metadata from the schema. If no
        geometry metadata is found, it will return `(None, [])`. This allows for graceful
        handling of both GeoParquet and regular Parquet files.

    Args:
        schema: Input PyArrow Schema.

    Returns:
        Tuple containing:
            - Primary geometry column name (None if not GeoParquet)
            - List of all geometry column names (empty list if not GeoParquet)
    """
    # try to get the geometry information from the parquet metadata if available
    geo_str = schema.metadata.get(b"geo")

    # if no geometry metadata, return None for primary column and empty list for all columns
    if geo_str is None:
        return None, []

    # get the geometry metadata as a dictionary
    geo_dict = json.loads(geo_str.decode("utf-8"))

    # get the primary geometry column name
    primary_column = geo_dict.get("primary_column")

    # get all geometry column names
    geometry_columns = get_geometry_columns(geo_dict)

    return primary_column, geometry_columns


def validate_parquet_path(parquet_path: Union[str, Path]) -> Path:
    """Validate the input Parquet path exists and is a directory."""
    # if a string, make into a path
    if isinstance(parquet_path, str):
        parquet_path = Path(parquet_path)

    # ensure will not encounter unexpected results based on incompatible input parameter or parameter combinations
    if not parquet_path.exists():
        raise ValueError(
            f"Cannot locate the input path {parquet_path}. Please double check to ensure the path is "
            f"correct and reachable."
        )

    elif parquet_path.is_dir():
        # get all the parquet part files to start with
        pqt_prts = [prt for prt in parquet_path.rglob("*.parquet")]

        # now, filter based on parts being part of string - enables to specify nested partition
        if isinstance(parquet_partitions, list):
            for partition in parquet_partitions:
                pqt_prts = [prt for prt in pqt_prts if partition in str(prt)]

        # if a list is not provided, throw a fit
        elif parquet_partitions is not None:
            raise ValueError("parquet_partitions must be a list")

        # ensure we have part files still left to work with
        assert len(pqt_prts) > 0, (
            "The provided directory and partitions do not appear to contain any parquet "
            "part files."
        )
    elif (
        parquet_path.is_file()
        and parquet_partitions is not None
        or len(parquet_partitions) > 0
    ):
        raise ValueError(
            "If providing a parquet part file, you cannot specify a parquet partition."
        )

    else:
        raise ValueError(
            "parquet_path must be either a directory for parquet data or a specific part file."
        )

    return parquet_path


def ensure_parquet_dataset(
    parquet_dataset: Union[str, Path, pq.ParquetDataset]
) -> pq.ParquetDataset:
    """Ensure the input is a ParquetDataset object."""
    # don't do anything if already a ParquetDataset
    if not isinstance(parquet_dataset, pq.ParquetDataset):
        # if a string, make into a path
        if isinstance(parquet_dataset, str):
            parquet_dataset = Path(parquet_dataset)

        # ensure the path exists
        if not parquet_dataset.exists():
            raise FileNotFoundError(
                f"Cannot resolve Parquet dataset path: {parquet_dataset}"
            )

        # create a ParquetDataset object
        parquet_dataset = pq.ParquetDataset(parquet_dataset)

    return parquet_dataset


def get_parquet_max_string_lengths(
    parquet_dataset: Union[str, Path, pq.ParquetDataset],
    string_columns: Optional[list[str]] = None,
) -> dict[str, int]:
    """
    For a Parquet dataset, get the maximum string lengths for all string columns.

    Args:
        parquet_dataset: Path to Parquet dataset.
    """
    logger.info("Introspectively determining maximum string lengths.")
    dataset = ensure_parquet_dataset(parquet_dataset)

    # identify string columns
    string_columns = [
        field.name for field in dataset.schema if pa.types.is_string(field.type)
    ]

    # initialize dictionary to store max lengths
    max_lengths = {col: 0 for col in string_columns}

    # initialize the reader
    reader = dataset.read()

    # iterate over string columns
    for col in string_columns:
        column = reader.column(col)

        # iterate chunks in the column
        for chunk in column.chunks:
            max_len = max(
                (len(str(val)) for val in chunk if val is not None), default=0
            )
            max_lengths[col] = max(max_lengths[col], max_len)

    return max_lengths


def get_geoparquet_bbox(
    parquet_dataset: Union[str, Path, pq.ParquetDataset]
) -> list[int]:
    """For a Geoparquet dataset, get the full maximum bounding box."""
    dataset = ensure_parquet_dataset(parquet_dataset)

    # get the explicitly added metadata for all the files
    meta_lst = [pq.read_metadata(fl).metadata for fl in dataset.files]

    # get the geography information - the metadata making the parquet dataset Geoparquet
    geo_binary_lst = [meta.get(b"geo") for meta in meta_lst]

    # convert the binary string into a list of dictionaries
    geo_lst = [json.loads(geo) for geo in geo_binary_lst]

    # get the geography definitions without the bounding boxes, and convert back to strings so can be compared in a set
    geo_set = set(
        json.dumps(
            {
                nm: {k: v for k, v in col_dict.items() if k != "bbox"}
                for nm, col_dict in geo.get("columns").items()
            }
        )
        for geo in geo_lst
    )

    # ensure only one geography is present
    if len(geo_set) > 1:
        raise ValueError(
            "More than one spatial reference detected. Cannot convert data."
        )

    # get the bounding box for all the files, the entire parquet dataset
    coords_lst = list(
        zip(*[geo.get("columns").get("geometry").get("bbox") for geo in geo_lst])
    )

    min_coords = [min(coords) for coords in coords_lst[:2]]
    max_coords = [max(coords) for coords in coords_lst[2:]]

    # create the bounding box list of coordinates
    bbox = min_coords + max_coords

    return bbox


def get_spatial_reference_projjson(
    spatial_reference: Union[int, dict, "arcpy.SpatialReference"]
) -> dict:
    """
    Get the PROJJSON representation of a Spatial Reference.

    !!! note:

        Spatial reference can be submitted as either an `arcpy.SpatialReference` object, dictionary with the
        well known identifier (WKID) or the integer well known identifier. For instance, for WGS84, this can
        be one of the following:

        * `arcpy.SpatialReference(4326)`
        * `{'wkid': 4326}`
        * `4326`

    Args:
        spatial_reference: The spatial reference to get the PROJJSON for.
    """
    # late import arcpy to avoid dependency if not needed
    import arcpy

    # message if cannot figure out spatial reference
    err_msg = (
        "Cannot determine the spatial reference from the input, please provide either an arcpy.SpatialReference or"
        "the well known identifier integer for the spatial reference."
    )

    # try to convert to string representation of a dict
    if isinstance(spatial_reference, str):
        try:
            # try to load the spatial reference string to a dictionary
            spatial_reference = json.loads(spatial_reference)

        except ValueError:
            raise ValueError(err_msg)

    # if a dictionary, try to get the wkid out of it
    if isinstance(spatial_reference, dict):
        spatial_reference = int(spatial_reference.get("wkid"))

        if spatial_reference is None:
            raise ValueError(err_msg)

    # if the spatial reference is a string representing a number, convert to an integer
    if isinstance(spatial_reference, str) and spatial_reference.isnumeric():
        spatial_reference = int(spatial_reference)

    # create an ArcPy SpatialReference object from the wkid
    if not isinstance(spatial_reference, arcpy.SpatialReference):
        spatial_reference = arcpy.SpatialReference(spatial_reference)

    # convert the spatial reference to the well known text representation
    wkt2_str = spatial_reference.exportToString("WKT2")

    # silence future warning, and ensure any issues encountered bubble up
    osr.UseExceptions()

    # use OSGeo to convert the spatial reference to PROJJSON from WKT2
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt2_str)

    prjson = json.loads(srs.ExportToPROJJSON())

    return prjson


def get_geoparquet_header(
    geometry_type: Literal["Point", "LineString", "Polygon"],
    encoding: Literal["WKB"] = "WKB",
    spatial_reference: Union[int, dict, "arcpy.SpatialReference"] = 4326,
    bounding_box: Optional[list[float]] = None,
    column_name: str = "geometry",
) -> dict:
    # account for linestring alias
    if geometry_type == "Line" or geometry_type == "Polyline":
        geometry_type = "LineString"

    # get the encoded spatial reference
    sr_projjson = get_spatial_reference_projjson(spatial_reference)

    # create the dictionary for the column
    col_dict = {
        "encoding": encoding,
        "geometry_types": [geometry_type],
        "crs": sr_projjson,
    }

    # if a bounding box is provided, ensure it is valid and add it to the column dictionary
    if bounding_box is not None:
        if not isinstance(bounding_box, list) or len(bounding_box) != 4:
            raise ValueError(
                "The bounding_box must be a list of four values: [minX, minY, maxX, maxY]"
            )
        if not all(isinstance(coord, (int, float)) for coord in bounding_box):
            raise ValueError("All values in the bounding_box must be numeric.")
        if bounding_box[0] > bounding_box[2]:
            raise ValueError(
                "The minX value must be less than the maxX value in the bounding_box."
            )
        if bounding_box[1] > bounding_box[3]:
            raise ValueError(
                "The minY value must be less than the maxY value in the bounding_box."
            )
        col_dict["bbox"] = bounding_box

    gpqt_header = {
        "version": "1.0.0",
        "primary_column": column_name,
        "columns": {column_name: col_dict},
    }

    return gpqt_header