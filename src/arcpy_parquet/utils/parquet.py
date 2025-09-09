from pathlib import Path
from functools import cached_property
import json
from typing import Union

import pyarrow.parquet as pq
import pyarrow.dataset as ds


_template = {
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

# get max length of a column from the metadata
get_col_max_len = lambda col: len(col.statistics.max) if isinstance(col.statistics.max, str) else None

# get maximum lengths for each row group in the table metadata
get_row_group_max_lengths = lambda rg: [get_col_max_len(rg.column(idx)) for idx in range(rg.num_columns)]


def get_string_columns(dataset: Union[Path, Dataset]) -> list[str]:
    """Get list of string column names for a Parquet dataset"""
    # create arrow dataset object if a path
    if isinstance(dataset, Path):
        dataset = ds.dataset(dataset, format='parquet')

    if not isinstance(dataset, Dataset):
        raise ValueError('dataset must be a PyArrow Dataset or path to a Parquet dataset')

    # get the string columns
    str_col_lst = [col.name for col in dataset.schema if "string" in str(col.type)]

    return str_col_lst


def get_file_max_len(pqt_file: Union[str, Path]) -> dict[str, int]:
    """Get a list of maximum string lengths for a file by reading the metadata statistics"""
    # get the table metadata
    meta = pq.read_metadata(pqt_file)

    # get a list of maximum lengths for every row group in the metadata
    max_len_lst_lst = [get_row_group_max_lengths(meta.row_group(idx)) for idx in range(meta.num_row_groups)]

    # zip the values into sets for each row
    max_len_zipped_lst = [set(val for val in vals if val is not None) for vals in zip(*max_len_lst_lst)]

    # get the maximum lengths in a single list of values
    max_len_lst = [max(val) if len(val) > 0 else None for val in max_len_zipped_lst]

    # create a dictionary of maximum lengths
    max_len_dict = {nm: max_len for nm, max_len in zip(meta.schema.names, max_len_lst)}

    return max_len_dict


def get_parquet_max_string_lengths(parquet_dataset: Union[str, Path]) -> dict[str, int]:
    """
    For a Parquet datset, get the maximum string lengths for all string columns.

    Args:
        parquet_dataset: Path to Parquet dataset.
    """
    # create a parquet dataset to work with
    dataset = ds.dataset(parquet_dataset, format='parquet')

    # get maximum lengths for each column from the metadata for each file in the dataset
    max_len_lst_lst = [list(get_file_max_len(fl).values()) for fl in dataset.files]

    # zip the values into sets for each row
    max_len_zipped_lst = [set(val for val in vals if val is not None) for vals in zip(*max_len_lst_lst)]

    # get the maximum lengths in a single list of values
    max_len_lst = [max(val) if len(val) > 0 else None for val in max_len_zipped_lst]

    # create a dictionary of maximum lengths
    max_len_dict = {nm: max_len for nm, max_len in zip(dataset.schema.names, max_len_lst)}

    return max_len_dict


def get_geoparquet_bbox(parquet_dataset: Union[str, Path]) -> list[int]:
    """For a Geoparquet dataset, get the full maximum bounding box."""
    dataset = ds.dataset(parquet_dataset, format='parquet')

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

    # convert the geography definition back to a dictionary
    uniq_geo = [json.loads(geo) for geo in geo_set][0]

    # get the bounding box for all the files, the entire parquet dataset
    coords_lst = list(
        zip(*[geo.get("columns").get("geometry").get("bbox") for geo in geo_lst])
    )

    min_coords = [min(coords) for coords in coords_lst[:2]]
    max_coords = [max(coords) for coords in coords_lst[2:]]

    # create the bounding box list of coordinates
    bbox = min_coords + max_coords

    return bbox
