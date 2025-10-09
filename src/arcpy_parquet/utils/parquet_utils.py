from pathlib import Path
import json
from osgeo import osr
from typing import Union, Literal, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from arcpy_parquet.utils import get_logger

# set up logging
logger = get_logger(logger_name="arcpy_parquet.utils.parquet", level="DEBUG")

# template for the geoparquet metadata
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
        "primary_column": "wkb",
        "columns": {"wkb": col_dict},
    }

    return gpqt_header
