"""
This is a stubbed out test file designed to be used with PyTest, but can 
easily be modified to support any testing framework.
"""

from pathlib import Path
import sys

import arcpy
import pandas as pd
import pyarrow.parquet as pq
import pytest


# get paths to useful resources - notably where the src directory is
self_pth = Path(__file__)
dir_test = self_pth.parent
dir_prj = dir_test.parent

dir_src = dir_prj / "src"

dir_data = dir_prj / "data"
dir_smpl = dir_data / "sample"
gdb_smpl = dir_smpl / "sample.gdb"

# insert the src directory into the path and import the project package
sys.path.insert(0, str(dir_src))
import arcpy_parquet

# set up logging
logger = arcpy_parquet.utils.get_logger(logger_name=Path(__file__).stem)


def get_parquet_columns(parquet_dataset: Path) -> list[str]:
    """Get list of column names for a Parquet dataset"""
    # create arrow dataset object if a path
    dataset = pq.ParquetDataset(parquet_dataset)
    return dataset.schema.names


def validate_columns(
    parquet_dataset: Path,
    expected_columns: list[str],
    exclude_columns: list[str] = None,
) -> None:
    """Validate that a Parquet dataset has the expected columns"""
    cols = get_parquet_columns(parquet_dataset)

    for col in expected_columns:
        assert col in cols

    if exclude_columns is not None:
        for col in exclude_columns:
            assert col not in cols


def validate_partitioning(
    parquet_dataset: Path, expected_partitions: list[str]
) -> None:
    """Validate that a Parquet dataset has the expected partitioning"""
    dataset = pq.ParquetDataset(parquet_dataset)
    parts = dataset.partitioning.schema.names

    if isinstance(expected_partitions, str):
        expected_partitions = [expected_partitions]

    for part in expected_partitions:
        assert part in parts


# data paths
features_poly = gdb_smpl / "wa_h3_09"
features_pt = gdb_smpl / "wa_h3_09_centroids"


@pytest.mark.parametrize(
    "input_features,output_format,expected_columns,exclude_columns,partition_columns",
    [
        (features_poly, "WKB", ["h3_index", "h3_06"], ["SHAPE"], None),
        (features_poly, "WKB", ["h3_index", "h3_06"], ["SHAPE"], ["h3_06"]),
        (
            features_poly,
            "XY",
            ["h3_index", "h3_06", "geometry_X", "geometry_Y"],
            ["SHAPE"],
            None,
        ),
        (features_pt, "WKB", ["h3_index", "h3_06"], ["SHAPE"], None),
        (features_pt, "WKB", ["h3_index", "h3_06"], ["SHAPE"], ["h3_06"]),
        (
            features_pt,
            "XY",
            ["h3_index", "h3_06", "geometry_X", "geometry_Y"],
            ["SHAPE"],
            None,
        ),
    ],
    ids=[
        "polygon WKB no partition",
        "polygon WKB with partition",
        "polygon XY no partition",
        "point WKB no partition",
        "point WKB with partition",
        "point XY no partition",
    ],
)
def test_feature_class_to_parquet(
    input_features,
    output_format,
    expected_columns,
    exclude_columns,
    partition_columns,
    tmp_pqt,
):
    res = arcpy_parquet.feature_class_to_parquet(
        input_table=input_features,
        output_parquet=tmp_pqt,
        partition_columns=partition_columns,
        include_geometry=True,
        geometry_format=output_format,
        batch_size=300000,
    )
    assert res.exists()
    validate_columns(
        res, expected_columns=expected_columns, exclude_columns=exclude_columns
    )
    if partition_columns is not None:
        validate_partitioning(res, expected_partitions=partition_columns)

# parquet data with coordinates
coord_pqt = dir_smpl / "main_fgdb_sample/parquet"
coord_schm = dir_smpl / "main_fgdb_sample/schema"
coord_cols = ["longitude", "latitude"]


def test_parquet_to_feature_class_coordinates(tmp_gdb):
    out_fc = tmp_gdb / "main_fgdb_sample"
    logger.info(f'output features path: {out_fc}')

    in_tbl = pq.ParquetDataset(coord_pqt).read()
    in_cnt = in_tbl.num_rows
    logger.info(f'Input row count: {in_cnt:,}')

    res = arcpy_parquet.parquet_to_feature_class(
        parquet_path=coord_pqt,
        output_feature_class=out_fc,
        geometry_column=coord_cols,
        geometry_format="COORDINATES",
        spatial_reference=4326,
    )

    assert arcpy.Exists(str(res))
    assert arcpy.Describe(str(res)).shapeType == "Point"

    out_cnt = int(arcpy.management.GetCount(str(res)).getOutput(0))
    assert in_cnt == out_cnt


def test_parquet_to_feature_class_coordinates_schema(tmp_gdb):
    out_fc = tmp_gdb / "main_fgdb_sample_scheama"
    logger.info(f'Output features path: {out_fc}')

    in_tbl = pq.ParquetDataset(coord_pqt).read()
    in_cnt = in_tbl.num_rows
    logger.info(f'Input row count: {in_cnt:,}')

    res = arcpy_parquet.parquet_to_feature_class(
        parquet_path=coord_pqt,
        output_feature_class=out_fc,
        schema_file=coord_schm,
        geometry_column=coord_cols,
        geometry_format="COORDINATES",
        spatial_reference=4326,
    )

    assert arcpy.Exists(str(res))
    assert arcpy.Describe(str(res)).shapeType == "Point"

    out_cnt = int(arcpy.management.GetCount(str(res)).getOutput(0))
    assert in_cnt == out_cnt


def test_parquet_to_feature_class_coordinates_no_schema(tmp_gdb):
    out_fc = tmp_gdb / "main_fgdb_sample"
    logger.info(f'Output features path: {out_fc}')

    in_tbl = pq.ParquetDataset(coord_pqt).read()
    in_cnt = in_tbl.num_rows
    logger.info(f'Input row count: {in_cnt:,}')

    res = arcpy_parquet.parquet_to_feature_class(
        parquet_path=coord_pqt,
        output_feature_class=out_fc,
        geometry_column=coord_cols,
        geometry_format="COORDINATES",
        spatial_reference=4326,
    )

    assert arcpy.Exists(str(res))
    assert arcpy.Describe(str(res)).shapeType == "Point"

    out_cnt = int(arcpy.management.GetCount(str(res)).getOutput(0))
    assert in_cnt == out_cnt


def test_parquet_to_feature_class_geoparquet(tmp_gdb):
    assert False, "Not implemented yet"


def test_parquet_to_feature_class_geoparquet_schema(tmp_gdb):
    assert False, "Not implemented yet"


def test_parquet_to_feature_class_h3(tmp_gdb):
    assert False, "Not implemented yet"


def test_parquet_to_feature_class_example_geoparquet(tmp_gdb):
    """Test converting a sample GeoParquet dataset to a feature class"""
    out_fc = tmp_gdb / "geoparquet_example"
    in_pqt = dir_smpl / "geoparquet_example"

    in_tbl = pq.ParquetDataset(in_pqt).read()
    in_cnt = in_tbl.num_rows
    logger.info(f'Input row count: {in_cnt:,}')

    res = arcpy_parquet.parquet_to_feature_class(
        parquet_path=in_pqt,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
        spatial_reference=4326,
    )

    assert arcpy.Exists(str(res))
    assert arcpy.Describe(str(res)).shapeType == "Polygon"

    out_cnt = int(arcpy.management.GetCount(str(res)).getOutput(0))
    assert in_cnt == out_cnt


def test_get_parquet_max_string_lengths(tmp_gdb):
    """Test getting max string lengths from a Parquet dataset"""
    res = arcpy_parquet.utils.parquet.get_parquet_max_string_lengths(coord_pqt)
    assert isinstance(res, dict)
    assert all(isinstance(k, str) for k in res.keys())
    assert all(v is None or isinstance(v, int) for v in res.values())
    assert all(v is None or v > 0 for v in res.values())
    assert set(res.keys()) == set(coord_cols)


def test_create_schema_file_parquet(tmp_dir):
    """Test creating a schema file from a Parquet dataset"""
    schema_pth = tmp_dir / "schema.csv"
    res = arcpy_parquet.create_schema_file(
        input_dataset=coord_pqt, output_schema_file=schema_pth
    )
    assert res.exists()
    assert res.stat().st_size > 0

    df = pd.read_csv(schema_pth)
    assert(len(df.index) > 0)


def test_get_partition_dicts():
    """Test getting partition dictionaries from a Parquet dataset"""
    pqt_pth = dir_smpl / "main_fgdb_sample/parquet"
    res = arcpy_parquet.utils.pyarrow_utils.get_partition_dicts(pqt_pth)
    assert isinstance(res, list)
    assert len(res) > 0
    assert all(isinstance(d, dict) for d in res)
    assert all(all(isinstance(k, str) for k in d.keys()) for d in res)
    assert all(all(isinstance(v, (str, int, float, type(None))) for v in d.values()) for d in res)


def test_get_partition_strings():
    """Test getting partition paths from a Parquet dataset"""
    pqt_pth = dir_smpl / "main_fgdb_sample/parquet"
    res = arcpy_parquet.utils.pyarrow_utils.get_partition_strings(pqt_pth)
    assert isinstance(res, list)
    assert len(res) > 0
    assert all(isinstance((pqt_pth / p), Path) for p in res)
    assert all((pqt_pth / p).exists() for p in res)
    assert all((pqt_pth / p).is_dir() for p in res)