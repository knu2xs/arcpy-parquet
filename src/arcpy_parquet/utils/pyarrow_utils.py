import json
from pathlib import Path
from typing import Optional, Union

import pyarrow as pa
from pyarrow import parquet as pq

from .logging_utils import get_logger

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
    if isinstance(dataset, pq.ParquetDataset):
        partition_dicts = get_partition_dicts(dataset, all_combinations=all_combinations)
        partition_strings = [
            partition_path_from_dict(partition_dict) for partition_dict in partition_dicts
        ]
    else:
        dataset = Path(dataset)
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
