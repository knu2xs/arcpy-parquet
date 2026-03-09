"""
Tests to validate that feature_class_to_parquet creates valid GeoParquet files.

This test suite verifies:
1. GeoParquet metadata is correctly formatted according to the specification
2. Bounding box reflects the entire dataset, not just the last batch
3. Geometry column name in metadata matches the actual column name
4. Multiple batches maintain consistent global extent
"""

import json
from pathlib import Path
import sys
import tempfile

import arcpy
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

# set up logging
logger = arcpy_parquet.utils.get_logger(logger_name=Path(__file__).stem)


@pytest.fixture
def tmp_gdb():
    """Create a temporary geodatabase for testing"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        gdb_name = "test.gdb"
        gdb_path = tmp_path / gdb_name
        arcpy.management.CreateFileGDB(str(tmp_path), gdb_name)
        yield gdb_path


@pytest.fixture
def sample_fc():
    """Get the sample feature class path"""
    fc_path = gdb_smpl / "main_fgdb_sample"
    if not arcpy.Exists(str(fc_path)):
        pytest.skip("Sample feature class not available")
    return fc_path


def get_geoparquet_metadata(parquet_path: Path) -> dict:
    """
    Extract and parse GeoParquet metadata from a Parquet file.
    
    Args:
        parquet_path: Path to Parquet file or directory
        
    Returns:
        dict: Parsed GeoParquet metadata
    """
    # Read the parquet dataset
    dataset = pq.ParquetDataset(parquet_path)
    
    # Get the first file's metadata
    if hasattr(dataset, 'fragments'):
        # Get first fragment
        fragment = list(dataset.fragments)[0]
        metadata = fragment.metadata
    else:
        # Single file
        parquet_file = pq.ParquetFile(parquet_path)
        metadata = parquet_file.schema_arrow.metadata
    
    # Check if geo metadata exists
    if metadata and b'geo' in metadata:
        geo_json = metadata[b'geo'].decode('utf-8')
        return json.loads(geo_json)
    
    return None


def get_feature_class_extent(fc_path: Path) -> tuple:
    """
    Get the extent of a feature class.
    
    Args:
        fc_path: Path to feature class
        
    Returns:
        tuple: (xmin, ymin, xmax, ymax)
    """
    desc = arcpy.Describe(str(fc_path))
    ext = desc.extent
    return (ext.XMin, ext.YMin, ext.XMax, ext.YMax)


def test_geoparquet_metadata_exists(sample_fc, tmp_gdb):
    """Test that GeoParquet metadata is created"""
    output_dir = tmp_gdb.parent / "parquet_output"
    output_dir.mkdir(exist_ok=True)
    
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=100,  # Small batch to test multiple files
    )
    
    # Get metadata
    geo_metadata = get_geoparquet_metadata(output_dir)
    
    # Verify metadata exists
    assert geo_metadata is not None, "GeoParquet metadata not found"
    
    # Verify required fields
    assert "version" in geo_metadata
    assert "primary_column" in geo_metadata
    assert "columns" in geo_metadata
    
    logger.info(f"GeoParquet metadata: {json.dumps(geo_metadata, indent=2)}")


def test_geometry_column_name_matches(sample_fc, tmp_gdb):
    """Test that the geometry column name in metadata matches the actual column"""
    output_dir = tmp_gdb.parent / "parquet_column_test"
    output_dir.mkdir(exist_ok=True)
    
    # Export with WKB format
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=100,
    )
    
    # Get metadata and schema
    geo_metadata = get_geoparquet_metadata(output_dir)
    dataset = pq.ParquetDataset(output_dir)
    schema_columns = dataset.schema.names
    
    # Get the primary column name from metadata
    primary_column = geo_metadata["primary_column"]
    
    # Verify the primary column exists in the schema
    assert primary_column in schema_columns, \
        f"Primary column '{primary_column}' not found in schema columns: {schema_columns}"
    
    # Verify it matches the expected name (lowercase WKB -> 'wkb')
    assert primary_column == "wkb", \
        f"Expected primary column to be 'wkb', got '{primary_column}'"
    
    # Verify columns metadata key matches
    assert primary_column in geo_metadata["columns"], \
        f"Primary column '{primary_column}' not in columns metadata"
    
    logger.info(f"Primary column '{primary_column}' correctly matches schema")


def test_bbox_reflects_entire_dataset(sample_fc, tmp_gdb):
    """Test that bounding box reflects entire dataset, not just last batch"""
    output_dir = tmp_gdb.parent / "parquet_bbox_test"
    output_dir.mkdir(exist_ok=True)
    
    # Get the actual extent of the feature class
    fc_extent = get_feature_class_extent(sample_fc)
    logger.info(f"Feature class extent: {fc_extent}")
    
    # Export with small batch size to ensure multiple batches
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=50,  # Very small to ensure multiple batches
    )
    
    # Get metadata
    geo_metadata = get_geoparquet_metadata(output_dir)
    primary_column = geo_metadata["primary_column"]
    
    # Get bbox from metadata
    assert "bbox" in geo_metadata["columns"][primary_column], \
        "Bounding box not found in GeoParquet metadata"
    
    bbox = geo_metadata["columns"][primary_column]["bbox"]
    logger.info(f"GeoParquet bbox: {bbox}")
    
    # Verify bbox has 4 values
    assert len(bbox) == 4, f"Expected 4 values in bbox, got {len(bbox)}"
    
    # Verify bbox approximately matches feature class extent
    # Allow for small floating point differences
    tolerance = 0.001
    
    assert abs(bbox[0] - fc_extent[0]) < tolerance, \
        f"BBox xmin {bbox[0]} doesn't match FC extent {fc_extent[0]}"
    assert abs(bbox[1] - fc_extent[1]) < tolerance, \
        f"BBox ymin {bbox[1]} doesn't match FC extent {fc_extent[1]}"
    assert abs(bbox[2] - fc_extent[2]) < tolerance, \
        f"BBox xmax {bbox[2]} doesn't match FC extent {fc_extent[2]}"
    assert abs(bbox[3] - fc_extent[3]) < tolerance, \
        f"BBox ymax {bbox[3]} doesn't match FC extent {fc_extent[3]}"
    
    logger.info("Bounding box correctly reflects entire dataset")


def test_multiple_batches_same_bbox(sample_fc, tmp_gdb):
    """Test that all batch files have the same global bbox"""
    output_dir = tmp_gdb.parent / "parquet_multi_batch"
    output_dir.mkdir(exist_ok=True)
    
    # Export with small batch size
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=30,  # Ensure multiple files
    )
    
    # Get all parquet files
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 1, \
        f"Expected multiple parquet files, got {len(parquet_files)}"
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    # Get bbox from each file
    bboxes = []
    for pf in parquet_files:
        parquet_file = pq.ParquetFile(pf)
        metadata = parquet_file.schema_arrow.metadata
        
        if metadata and b'geo' in metadata:
            geo_json = metadata[b'geo'].decode('utf-8')
            geo_metadata = json.loads(geo_json)
            primary_column = geo_metadata["primary_column"]
            
            if "bbox" in geo_metadata["columns"][primary_column]:
                bbox = geo_metadata["columns"][primary_column]["bbox"]
                bboxes.append((pf.name, bbox))
                logger.info(f"{pf.name}: bbox = {bbox}")
    
    # Verify all bboxes are the same
    assert len(bboxes) > 1, "Expected multiple files with bbox metadata"
    
    first_bbox = bboxes[0][1]
    for filename, bbox in bboxes[1:]:
        assert bbox == first_bbox, \
            f"File {filename} has different bbox {bbox} vs {first_bbox}"
    
    logger.info("All batch files have identical global bbox")


def test_geoparquet_version_compliance(sample_fc, tmp_gdb):
    """Test that GeoParquet metadata follows version 1.0.0 specification"""
    output_dir = tmp_gdb.parent / "parquet_version_test"
    output_dir.mkdir(exist_ok=True)
    
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=100,
    )
    
    geo_metadata = get_geoparquet_metadata(output_dir)
    
    # Verify version
    assert geo_metadata["version"] == "1.0.0", \
        f"Expected GeoParquet version 1.0.0, got {geo_metadata['version']}"
    
    # Verify primary_column structure
    primary_column = geo_metadata["primary_column"]
    assert isinstance(primary_column, str), "primary_column must be a string"
    
    # Verify columns structure
    assert isinstance(geo_metadata["columns"], dict), "columns must be a dict"
    assert primary_column in geo_metadata["columns"], \
        "primary_column must be in columns dict"
    
    # Verify column metadata structure
    col_metadata = geo_metadata["columns"][primary_column]
    assert "encoding" in col_metadata, "Column metadata must have encoding"
    assert "geometry_types" in col_metadata, "Column metadata must have geometry_types"
    assert "crs" in col_metadata, "Column metadata must have crs"
    
    # Verify geometry_types is a list
    assert isinstance(col_metadata["geometry_types"], list), \
        "geometry_types must be a list"
    
    logger.info("GeoParquet metadata complies with v1.0.0 specification")


def test_xy_format_no_geoparquet_metadata(sample_fc, tmp_gdb):
    """Test that XY format doesn't create GeoParquet metadata"""
    output_dir = tmp_gdb.parent / "parquet_xy_test"
    output_dir.mkdir(exist_ok=True)
    
    # Export with XY format (centroid coordinates)
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="XY",
        batch_size=100,
    )
    
    # Get metadata
    geo_metadata = get_geoparquet_metadata(output_dir)
    
    # XY format should NOT have GeoParquet metadata
    assert geo_metadata is None, \
        "XY format should not create GeoParquet metadata"
    
    # Verify X and Y columns exist instead
    dataset = pq.ParquetDataset(output_dir)
    schema_columns = dataset.schema.names
    
    assert "geometry_X" in schema_columns, "Expected geometry_X column for XY format"
    assert "geometry_Y" in schema_columns, "Expected geometry_Y column for XY format"
    
    logger.info("XY format correctly omits GeoParquet metadata")


def test_encoding_in_metadata(sample_fc, tmp_gdb):
    """Test that the encoding field in metadata matches the geometry format"""
    output_dir = tmp_gdb.parent / "parquet_encoding_test"
    output_dir.mkdir(exist_ok=True)
    
    # Export with WKB format
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=100,
    )
    
    geo_metadata = get_geoparquet_metadata(output_dir)
    primary_column = geo_metadata["primary_column"]
    col_metadata = geo_metadata["columns"][primary_column]
    
    # Verify encoding matches
    assert col_metadata["encoding"] == "WKB", \
        f"Expected encoding 'WKB', got '{col_metadata['encoding']}'"
    
    logger.info("Encoding correctly set in metadata")


def test_geometry_type_in_metadata(sample_fc, tmp_gdb):
    """Test that the geometry type is correctly identified in metadata"""
    output_dir = tmp_gdb.parent / "parquet_geomtype_test"
    output_dir.mkdir(exist_ok=True)
    
    # Get the geometry type from the feature class
    desc = arcpy.Describe(str(sample_fc))
    fc_geom_type = desc.shapeType
    logger.info(f"Feature class geometry type: {fc_geom_type}")
    
    arcpy_parquet.feature_class_to_parquet(
        input_table=sample_fc,
        output_parquet=output_dir,
        include_geometry=True,
        geometry_format="WKB",
        batch_size=100,
    )
    
    geo_metadata = get_geoparquet_metadata(output_dir)
    primary_column = geo_metadata["primary_column"]
    geometry_types = geo_metadata["columns"][primary_column]["geometry_types"]
    
    # Verify geometry_types is a list with at least one entry
    assert len(geometry_types) > 0, "geometry_types should not be empty"
    
    # Map ArcGIS geometry types to GeoParquet types
    type_mapping = {
        "Point": "Point",
        "Polyline": "LineString",
        "Polygon": "Polygon",
    }
    
    expected_type = type_mapping.get(fc_geom_type)
    if expected_type:
        assert expected_type in geometry_types, \
            f"Expected {expected_type} in geometry_types, got {geometry_types}"
    
    logger.info(f"Geometry type correctly identified: {geometry_types}")


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "-s"])
