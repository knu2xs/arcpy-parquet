"""
Comprehensive test suite for parquet_to_feature_class function.

This module contains tests for:
- GeoParquet format with single and multiple geometry columns
- COORDINATES format with XY coordinate columns
- H3 format with H3 index columns
- Schema validation and error handling
- Partitioned datasets
- Sample count functionality
- Spatial reference handling
- Complex data types
"""

from pathlib import Path
import sys
import tempfile
import importlib.util

import arcpy
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# get paths to useful resources
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
from arcpy_parquet.utils import pyarrow_utils

# set up logging
logger = arcpy_parquet.utils.get_logger(logger_name=Path(__file__).stem, level="DEBUG")

arcpy.env.overwriteOutput = True

# check if optional dependencies are available
HAS_SHAPELY = importlib.util.find_spec("shapely") is not None
HAS_H3 = importlib.util.find_spec("h3") is not None


# ========================================================================================
# Helper Functions
# ========================================================================================


def create_test_geoparquet(
    output_path: Path,
    geometry_type: str = "Point",
    num_rows: int = 100,
    spatial_reference: int = 4326,
    include_multiple_geometries: bool = False,
) -> Path:
    """
    Create a test GeoParquet file.

    Args:
        output_path: Where to save the GeoParquet file.
        geometry_type: Type of geometry (Point, LineString, Polygon).
        num_rows: Number of rows to create.
        spatial_reference: EPSG code for spatial reference.
        include_multiple_geometries: If True, include a secondary geometry column.

    Returns:
        Path to created GeoParquet file.
    """
    import json
    from shapely import wkb
    from shapely.geometry import Point, LineString, Polygon

    # create sample data
    data = {
        "id": list(range(num_rows)),
        "name": [f"Feature_{i}" for i in range(num_rows)],
        "value": [i * 10.5 for i in range(num_rows)],
    }

    # create geometries based on type
    if geometry_type == "Point":
        geoms = [Point(i * 0.1, i * 0.1).wkb for i in range(num_rows)]
        if include_multiple_geometries:
            geoms2 = [Point(i * 0.2, i * 0.2).wkb for i in range(num_rows)]
    elif geometry_type == "LineString":
        geoms = [
            LineString([(i * 0.1, i * 0.1), (i * 0.1 + 1, i * 0.1 + 1)]).wkb
            for i in range(num_rows)
        ]
        if include_multiple_geometries:
            geoms2 = [
                LineString([(i * 0.2, i * 0.2), (i * 0.2 + 1, i * 0.2 + 1)]).wkb
                for i in range(num_rows)
            ]
    elif geometry_type == "Polygon":
        geoms = [
            Polygon(
                [
                    (i * 0.1, i * 0.1),
                    (i * 0.1 + 1, i * 0.1),
                    (i * 0.1 + 1, i * 0.1 + 1),
                    (i * 0.1, i * 0.1 + 1),
                    (i * 0.1, i * 0.1),
                ]
            ).wkb
            for i in range(num_rows)
        ]
        if include_multiple_geometries:
            geoms2 = [
                Polygon(
                    [
                        (i * 0.2, i * 0.2),
                        (i * 0.2 + 1, i * 0.2),
                        (i * 0.2 + 1, i * 0.2 + 1),
                        (i * 0.2, i * 0.2 + 1),
                        (i * 0.2, i * 0.2),
                    ]
                ).wkb
                for i in range(num_rows)
            ]
    else:
        raise ValueError(f"Unsupported geometry type: {geometry_type}")

    data["geometry"] = geoms
    if include_multiple_geometries:
        data["geometry2"] = geoms2

    # create PyArrow table
    table = pa.Table.from_pydict(data)

    # create GeoParquet metadata
    geo_metadata = pyarrow_utils.get_geoparquet_header(
        geometry_type=geometry_type,
        encoding="WKB",
        spatial_reference=spatial_reference,
        column_name="geometry",
    )

    # add secondary geometry column metadata if needed
    if include_multiple_geometries:
        geo_metadata["columns"]["geometry2"] = {
            "encoding": "WKB",
            "geometry_types": [geometry_type],
            "crs": geo_metadata["columns"]["geometry"]["crs"],
        }

    # add metadata to schema
    schema = table.schema.with_metadata(
        {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    )
    table = table.cast(schema)

    # write to parquet
    output_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path / "part-0.parquet")

    return output_path


def create_test_coordinates_parquet(
    output_path: Path,
    num_rows: int = 100,
    x_col: str = "longitude",
    y_col: str = "latitude",
) -> tuple[Path, list[str]]:
    """
    Create a test Parquet file with coordinate columns.

    Args:
        output_path: Where to save the Parquet file.
        num_rows: Number of rows to create.
        x_col: Name of the X coordinate column.
        y_col: Name of the Y coordinate column.

    Returns:
        Tuple of (Path to created Parquet file, list of coordinate column names).
    """
    data = {
        "id": list(range(num_rows)),
        "name": [f"Feature_{i}" for i in range(num_rows)],
        x_col: [i * 0.1 - 122.0 for i in range(num_rows)],
        y_col: [i * 0.1 + 47.0 for i in range(num_rows)],
        "value": [i * 10.5 for i in range(num_rows)],
    }

    table = pa.Table.from_pydict(data)
    output_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path / "part-0.parquet")

    return output_path, [x_col, y_col]


def create_test_h3_parquet(
    output_path: Path, num_rows: int = 100, h3_col: str = "h3_index"
) -> tuple[Path, str]:
    """
    Create a test Parquet file with H3 index column.

    Args:
        output_path: Where to save the Parquet file.
        num_rows: Number of rows to create.
        h3_col: Name of the H3 index column.

    Returns:
        Tuple of (Path to created Parquet file, H3 column name).
    """
    # use valid H3 indices at resolution 9 for Seattle area
    h3_indices = [
        "89283082813ffff",
        "89283082817ffff",
        "8928308281bffff",
        "8928308281fffff",
        "89283082823ffff",
    ]

    data = {
        "id": list(range(num_rows)),
        "name": [f"Feature_{i}" for i in range(num_rows)],
        h3_col: [h3_indices[i % len(h3_indices)] for i in range(num_rows)],
        "value": [i * 10.5 for i in range(num_rows)],
    }

    table = pa.Table.from_pydict(data)
    output_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path / "part-0.parquet")

    return output_path, h3_col


def validate_feature_class(
    fc_path: Path,
    expected_shape_type: str,
    expected_count: int,
    expected_sr_wkid: int = None,
    expected_fields: list[str] = None,
) -> None:
    """
    Validate a feature class has expected properties.

    Args:
        fc_path: Path to feature class.
        expected_shape_type: Expected shape type (Point, Polyline, Polygon).
        expected_count: Expected feature count.
        expected_sr_wkid: Expected spatial reference WKID.
        expected_fields: Expected field names.
    """
    assert arcpy.Exists(str(fc_path)), f"Feature class does not exist: {fc_path}"

    desc = arcpy.Describe(str(fc_path))
    assert (
        desc.shapeType == expected_shape_type
    ), f"Shape type mismatch: {desc.shapeType} != {expected_shape_type}"

    count = int(arcpy.management.GetCount(str(fc_path)).getOutput(0))
    assert (
        count == expected_count
    ), f"Feature count mismatch: {count} != {expected_count}"

    if expected_sr_wkid is not None:
        sr = arcpy.Describe(str(fc_path)).spatialReference
        assert (
            sr.factoryCode == expected_sr_wkid
        ), f"Spatial reference mismatch: {sr.factoryCode} != {expected_sr_wkid}"

    if expected_fields is not None:
        field_names = [f.name for f in arcpy.ListFields(str(fc_path))]
        for field in expected_fields:
            assert field in field_names, f"Expected field not found: {field}"


# ========================================================================================
# GeoParquet Format Tests
# ========================================================================================


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_point(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet with Point geometries."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Point", num_rows=50, spatial_reference=4326
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_geoparquet_point"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=50,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_polyline(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet with LineString geometries."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="LineString", num_rows=30, spatial_reference=4326
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_geoparquet_polyline"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Polyline",
        expected_count=30,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_polygon(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet with Polygon geometries."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Polygon", num_rows=40, spatial_reference=4326
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_geoparquet_polygon"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Polygon",
        expected_count=40,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_multiple_geometries(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet with multiple geometry columns."""
    # create test data with multiple geometry columns
    pqt_path = create_test_geoparquet(
        tmp_pqt,
        geometry_type="Point",
        num_rows=50,
        spatial_reference=4326,
        include_multiple_geometries=True,
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_geoparquet_multi_geom"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate - should use primary geometry column only
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=50,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )

    # verify geometry columns are not added as fields
    field_names = [f.name for f in arcpy.ListFields(str(result))]
    assert "geometry" not in field_names, "Primary geometry column should not be a field"
    assert (
        "geometry2" not in field_names
    ), "Secondary geometry column should not be a field"


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_different_spatial_reference(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet with different spatial references."""
    # create test data with Web Mercator
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Point", num_rows=25, spatial_reference=3857
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_geoparquet_3857"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate spatial reference
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=25,
        expected_sr_wkid=3857,
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_sample_count(tmp_gdb, tmp_pqt):
    """Test importing only a sample of records."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Point", num_rows=100, spatial_reference=4326
    )

    # convert to feature class with sample count
    out_fc = tmp_gdb / "test_geoparquet_sample"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
        sample_count=25,
    )

    # validate - should have only 25 features
    validate_feature_class(
        result, expected_shape_type="Point", expected_count=25, expected_sr_wkid=4326
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_no_spatial_index(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet without building spatial index."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Point", num_rows=20, spatial_reference=4326
    )

    # convert to feature class without spatial index
    out_fc = tmp_gdb / "test_geoparquet_no_index"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
        build_spatial_index=False,
    )

    # validate
    validate_feature_class(
        result, expected_shape_type="Point", expected_count=20, expected_sr_wkid=4326
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_no_compact(tmp_gdb, tmp_pqt):
    """Test converting GeoParquet without compacting."""
    # create test data
    pqt_path = create_test_geoparquet(
        tmp_pqt, geometry_type="Point", num_rows=20, spatial_reference=4326
    )

    # convert to feature class without compacting
    out_fc = tmp_gdb / "test_geoparquet_no_compact"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
        compact=False,
    )

    # validate
    validate_feature_class(
        result, expected_shape_type="Point", expected_count=20, expected_sr_wkid=4326
    )


# ========================================================================================
# COORDINATES Format Tests
# ========================================================================================


def test_coordinates_basic(tmp_gdb, tmp_pqt):
    """Test converting Parquet with coordinate columns."""
    # create test data
    pqt_path, coord_cols = create_test_coordinates_parquet(tmp_pqt, num_rows=50)

    # convert to feature class
    out_fc = tmp_gdb / "test_coordinates_basic"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="COORDINATES",
        geometry_column=coord_cols,
        spatial_reference=4326,
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=50,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )


def test_coordinates_custom_columns(tmp_gdb, tmp_pqt):
    """Test converting Parquet with custom coordinate column names."""
    # create test data with custom column names
    pqt_path, coord_cols = create_test_coordinates_parquet(
        tmp_pqt, num_rows=30, x_col="x_coord", y_col="y_coord"
    )

    # convert to feature class
    out_fc = tmp_gdb / "test_coordinates_custom"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="COORDINATES",
        geometry_column=coord_cols,
        spatial_reference=4326,
    )

    # validate
    validate_feature_class(
        result, expected_shape_type="Point", expected_count=30, expected_sr_wkid=4326
    )


def test_coordinates_sample_count(tmp_gdb, tmp_pqt):
    """Test importing coordinates with sample count."""
    # create test data
    pqt_path, coord_cols = create_test_coordinates_parquet(tmp_pqt, num_rows=100)

    # convert to feature class with sample count
    out_fc = tmp_gdb / "test_coordinates_sample"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="COORDINATES",
        geometry_column=coord_cols,
        spatial_reference=4326,
        sample_count=40,
    )

    # validate - should have only 40 features
    validate_feature_class(
        result, expected_shape_type="Point", expected_count=40, expected_sr_wkid=4326
    )


# ========================================================================================
# H3 Format Tests
# ========================================================================================


@pytest.mark.skipif(not HAS_H3, reason="h3 package not installed")
def test_h3_basic(tmp_gdb, tmp_pqt):
    """Test converting Parquet with H3 index column."""

    # create test data
    pqt_path, h3_col = create_test_h3_parquet(tmp_pqt, num_rows=50)

    # convert to feature class
    out_fc = tmp_gdb / "test_h3_basic"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="H3",
        geometry_column=h3_col,
        spatial_reference=4326,
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Polygon",
        expected_count=50,
        expected_sr_wkid=4326,
        expected_fields=["id", "name", "value"],
    )


@pytest.mark.skipif(not HAS_H3, reason="h3 package not installed")
def test_h3_custom_column(tmp_gdb, tmp_pqt):
    """Test converting Parquet with custom H3 column name."""

    # create test data with custom column name
    pqt_path, h3_col = create_test_h3_parquet(tmp_pqt, num_rows=30, h3_col="h3_cell")

    # convert to feature class
    out_fc = tmp_gdb / "test_h3_custom"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=pqt_path,
        output_feature_class=out_fc,
        geometry_format="H3",
        geometry_column=h3_col,
        spatial_reference=4326,
    )

    # validate
    validate_feature_class(
        result, expected_shape_type="Polygon", expected_count=30, expected_sr_wkid=4326
    )


# ========================================================================================
# Error Handling Tests
# ========================================================================================


def test_geoparquet_missing_metadata(tmp_gdb, tmp_pqt):
    """Test error when GeoParquet metadata is missing."""
    # create regular parquet without geo metadata
    pqt_path, coord_cols = create_test_coordinates_parquet(tmp_pqt, num_rows=10)

    # should raise error when trying to use GEOPARQUET format
    out_fc = tmp_gdb / "test_geoparquet_missing"
    with pytest.raises(ValueError, match="does not appear to be formatted as GeoParquet"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="GEOPARQUET",
        )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_geoparquet_invalid_geometry_column(tmp_gdb, tmp_pqt):
    """Test error when geometry column doesn't exist in schema."""
    import json

    # create parquet with invalid geometry column reference
    data = {
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
    }
    table = pa.Table.from_pydict(data)

    # add geo metadata referencing non-existent column
    geo_metadata = {
        "version": "1.0.0",
        "primary_column": "nonexistent_geom",
        "columns": {
            "nonexistent_geom": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {"id": {"authority": "EPSG", "code": 4326}},
            }
        },
    }

    schema = table.schema.with_metadata(
        {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    )
    table = table.cast(schema)

    tmp_pqt.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_pqt / "part-0.parquet")

    # should raise error about missing column
    out_fc = tmp_gdb / "test_invalid_geom_col"
    with pytest.raises(ValueError, match="does not exist in the dataset schema"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=tmp_pqt,
            output_feature_class=out_fc,
            geometry_format="GEOPARQUET",
        )


def test_coordinates_missing_column(tmp_gdb, tmp_pqt):
    """Test error when coordinate column doesn't exist."""
    # create test data
    pqt_path, coord_cols = create_test_coordinates_parquet(tmp_pqt, num_rows=10)

    # try with non-existent column
    out_fc = tmp_gdb / "test_coords_missing"
    with pytest.raises(ValueError, match="do not appear to be in the input parquet columns"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="COORDINATES",
            geometry_column=["nonexistent_x", "nonexistent_y"],
            spatial_reference=4326,
        )


def test_coordinates_invalid_type(tmp_gdb, tmp_pqt):
    """Test error when coordinate column is not numeric."""
    # create parquet with non-numeric column
    data = {
        "id": [1, 2, 3],
        "x_coord": ["not", "a", "number"],
        "y_coord": [47.0, 47.1, 47.2],
    }
    table = pa.Table.from_pydict(data)

    tmp_pqt.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_pqt / "part-0.parquet")

    # should raise error about non-numeric type
    out_fc = tmp_gdb / "test_coords_invalid_type"
    with pytest.raises(ValueError, match="does not appear to be a numeric type"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=tmp_pqt,
            output_feature_class=out_fc,
            geometry_format="COORDINATES",
            geometry_column=["x_coord", "y_coord"],
            spatial_reference=4326,
        )


def test_coordinates_wrong_format(tmp_gdb, tmp_pqt):
    """Test error when geometry_column is not a list/tuple for COORDINATES."""
    # create test data
    pqt_path, coord_cols = create_test_coordinates_parquet(tmp_pqt, num_rows=10)

    # try with single string instead of list
    out_fc = tmp_gdb / "test_coords_wrong_format"
    with pytest.raises(ValueError, match="must provide an iterable"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="COORDINATES",
            geometry_column="longitude",
            spatial_reference=4326,
        )


def test_h3_missing_column(tmp_gdb, tmp_pqt):
    """Test error when H3 column doesn't exist."""
    # create test data
    pqt_path, h3_col = create_test_h3_parquet(tmp_pqt, num_rows=10)

    # try with non-existent column
    out_fc = tmp_gdb / "test_h3_missing"
    with pytest.raises(ValueError, match="does not appear to be in the input parquet columns"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="H3",
            geometry_column="nonexistent_h3",
            spatial_reference=4326,
        )


def test_h3_no_column_provided(tmp_gdb, tmp_pqt):
    """Test error when H3 column is not provided."""
    # create test data
    pqt_path, h3_col = create_test_h3_parquet(tmp_pqt, num_rows=10)

    # try without providing column name
    out_fc = tmp_gdb / "test_h3_no_column"
    with pytest.raises(ValueError, match="you must provide the geometry_column parameter"):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="H3",
            geometry_column=None,
            spatial_reference=4326,
        )


def test_invalid_geometry_format(tmp_gdb, tmp_pqt):
    """Test error when invalid geometry format is provided."""
    # create test data
    pqt_path = create_test_geoparquet(tmp_pqt, geometry_type="Point", num_rows=10)

    # try with invalid format
    out_fc = tmp_gdb / "test_invalid_format"
    with pytest.raises(ValueError, match='geometry_format must be one of'):
        arcpy_parquet.parquet_to_feature_class(
            parquet_path=pqt_path,
            output_feature_class=out_fc,
            geometry_format="INVALID_FORMAT",
        )


# ========================================================================================
# Real Data Tests (using sample data from project)
# ========================================================================================


def test_example_geoparquet_dataset(tmp_gdb):
    """Test converting the example GeoParquet dataset from sample data."""
    in_pqt = dir_smpl / "geoparquet_example"

    # skip if sample data doesn't exist
    if not in_pqt.exists():
        pytest.skip("Sample GeoParquet data not available")

    in_tbl = pq.ParquetDataset(in_pqt).read()
    in_cnt = in_tbl.num_rows

    out_fc = tmp_gdb / "example_geoparquet"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=in_pqt,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    validate_feature_class(
        result, expected_shape_type="Polygon", expected_count=in_cnt
    )


def test_coordinates_with_schema(tmp_gdb):
    """Test converting coordinates with schema file."""
    coord_pqt = dir_smpl / "main_fgdb_sample/parquet"
    coord_schm = dir_smpl / "main_fgdb_sample/schema"

    # skip if sample data doesn't exist
    if not coord_pqt.exists() or not coord_schm.exists():
        pytest.skip("Sample coordinate data not available")

    in_tbl = pq.ParquetDataset(coord_pqt).read()
    in_cnt = in_tbl.num_rows

    out_fc = tmp_gdb / "coords_with_schema"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=coord_pqt,
        output_feature_class=out_fc,
        schema_file=coord_schm,
        geometry_format="COORDINATES",
        geometry_column=["longitude", "latitude"],
        spatial_reference=4326,
    )

    validate_feature_class(result, expected_shape_type="Point", expected_count=in_cnt)


# ========================================================================================
# Schema and Complex Type Tests
# ========================================================================================


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_complex_data_types(tmp_gdb, tmp_pqt):
    """Test handling of complex data types (arrays, structs)."""
    # create parquet with complex types
    data = {
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "tags": [["tag1", "tag2"], ["tag3"], ["tag4", "tag5", "tag6"]],
        "metadata": [
            {"key": "value1"},
            {"key": "value2"},
            {"key": "value3"},
        ],
    }

    # create GeoParquet with complex types
    from shapely.geometry import Point

    data["geometry"] = [Point(i, i).wkb for i in range(3)]

    table = pa.Table.from_pydict(data)

    # add geo metadata
    import json

    geo_metadata = pyarrow_utils.get_geoparquet_header(
        geometry_type="Point", encoding="WKB", spatial_reference=4326
    )
    schema = table.schema.with_metadata(
        {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    )
    table = table.cast(schema)

    tmp_pqt.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_pqt / "part-0.parquet")

    # convert to feature class
    out_fc = tmp_gdb / "test_complex_types"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=tmp_pqt,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate - complex types should be converted to strings
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=3,
        expected_fields=["id", "name", "tags", "metadata"],
    )

    # verify complex fields are text type
    field_dict = {f.name: f.type for f in arcpy.ListFields(str(result))}
    assert field_dict["tags"] == "String", "tags field should be String type"
    assert field_dict["metadata"] == "String", "metadata field should be String type"


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_long_string_fields(tmp_gdb, tmp_pqt):
    """Test handling of long string fields."""
    # create parquet with long strings
    long_string = "X" * 1000
    data = {
        "id": [1, 2, 3],
        "short_text": ["A", "B", "C"],
        "long_text": [long_string, long_string[:500], long_string[:750]],
    }

    # create GeoParquet
    from shapely.geometry import Point

    data["geometry"] = [Point(i, i).wkb for i in range(3)]

    table = pa.Table.from_pydict(data)

    # add geo metadata
    import json

    geo_metadata = pyarrow_utils.get_geoparquet_header(
        geometry_type="Point", encoding="WKB", spatial_reference=4326
    )
    schema = table.schema.with_metadata(
        {b"geo": json.dumps(geo_metadata).encode("utf-8")}
    )
    table = table.cast(schema)

    tmp_pqt.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_pqt / "part-0.parquet")

    # convert to feature class
    out_fc = tmp_gdb / "test_long_strings"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=tmp_pqt,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=3,
        expected_fields=["id", "short_text", "long_text"],
    )

    # verify long_text field has sufficient length
    field_dict = {f.name: (f.type, f.length) for f in arcpy.ListFields(str(result))}
    assert field_dict["long_text"][0] == "String"
    assert (
        field_dict["long_text"][1] >= 1000
    ), "long_text field should have length >= 1000"


# ========================================================================================
# Integration Tests
# ========================================================================================


def test_roundtrip_geoparquet(tmp_gdb, tmp_pqt):
    """Test round-trip conversion: FC -> GeoParquet -> FC."""
    # use sample feature class if available
    features_poly = gdb_smpl / "wa_h3_09"

    if not arcpy.Exists(str(features_poly)):
        pytest.skip("Sample feature class not available")

    # export to parquet
    tmp_pqt1 = tmp_pqt / "export"
    arcpy_parquet.feature_class_to_parquet(
        input_table=features_poly,
        output_parquet=tmp_pqt1,
        include_geometry=True,
        geometry_format="WKB",
    )

    # import back to feature class
    out_fc = tmp_gdb / "roundtrip_test"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=tmp_pqt1,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate
    original_count = int(arcpy.management.GetCount(str(features_poly)).getOutput(0))
    validate_feature_class(
        result,
        expected_shape_type="Polygon",
        expected_count=original_count,
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_partitioned_dataset(tmp_gdb, tmp_pqt):
    """Test converting a partitioned Parquet dataset."""
    # create partitioned dataset
    import json
    from shapely.geometry import Point

    # create data for multiple partitions
    for year in [2023, 2024]:
        for month in [1, 2]:
            data = {
                "id": list(range(10)),
                "year": [year] * 10,
                "month": [month] * 10,
                "value": [i * 10.5 for i in range(10)],
                "geometry": [Point(i, i).wkb for i in range(10)],
            }

            table = pa.Table.from_pydict(data)

            # add geo metadata
            geo_metadata = pyarrow_utils.get_geoparquet_header(
                geometry_type="Point", encoding="WKB", spatial_reference=4326
            )
            schema = table.schema.with_metadata(
                {b"geo": json.dumps(geo_metadata).encode("utf-8")}
            )
            table = table.cast(schema)

            # write to partitioned location
            partition_dir = tmp_pqt / f"year={year}" / f"month={month}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, partition_dir / "part-0.parquet")

    # convert entire dataset
    out_fc = tmp_gdb / "test_partitioned"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=tmp_pqt,
        output_feature_class=out_fc,
        geometry_format="GEOPARQUET",
    )

    # validate - should have all records from all partitions
    validate_feature_class(
        result,
        expected_shape_type="Point",
        expected_count=40,  # 10 records * 2 years * 2 months
    )


@pytest.mark.skipif(not HAS_SHAPELY, reason="shapely package not installed")
def test_specific_partition(tmp_gdb, tmp_pqt):
    """Test converting a specific partition from a Parquet dataset."""
    # create partitioned dataset
    import json
    from shapely.geometry import Point

    for year in [2023, 2024]:
        data = {
            "id": list(range(10)),
            "year": [year] * 10,
            "value": [i * 10.5 for i in range(10)],
            "geometry": [Point(i, i).wkb for i in range(10)],
        }

        table = pa.Table.from_pydict(data)

        # add geo metadata
        geo_metadata = pyarrow_utils.get_geoparquet_header(
            geometry_type="Point", encoding="WKB", spatial_reference=4326
        )
        schema = table.schema.with_metadata(
            {b"geo": json.dumps(geo_metadata).encode("utf-8")}
        )
        table = table.cast(schema)

        partition_dir = tmp_pqt / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, partition_dir / "part-0.parquet")

    # convert only 2024 partition
    out_fc = tmp_gdb / "test_partition_2024"
    result = arcpy_parquet.parquet_to_feature_class(
        parquet_path=tmp_pqt,
        output_feature_class=out_fc,
        parquet_partitions="year=2024",
        geometry_format="GEOPARQUET",
    )

    # validate - should have only 2024 records
    validate_feature_class(result, expected_shape_type="Point", expected_count=10)
