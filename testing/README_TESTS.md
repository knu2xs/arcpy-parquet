# Parquet to Feature Class Test Suite

## Overview

This test suite provides comprehensive testing for the `parquet_to_feature_class` function, covering all supported geometry formats, error handling, and edge cases.

## Test Organization

### GeoParquet Format Tests
- `test_geoparquet_point`: Basic point geometry conversion
- `test_geoparquet_polyline`: LineString geometry conversion
- `test_geoparquet_polygon`: Polygon geometry conversion
- `test_geoparquet_multiple_geometries`: Handling multiple geometry columns (uses only primary)
- `test_geoparquet_different_spatial_reference`: Various spatial references (WGS84, Web Mercator)
- `test_geoparquet_sample_count`: Partial data import using sample_count
- `test_geoparquet_no_spatial_index`: Skip spatial index building
- `test_geoparquet_no_compact`: Skip geodatabase compacting

### COORDINATES Format Tests
- `test_coordinates_basic`: Basic coordinate column conversion
- `test_coordinates_custom_columns`: Custom X/Y column names
- `test_coordinates_sample_count`: Partial import with coordinates

### H3 Format Tests
- `test_h3_basic`: Basic H3 index conversion
- `test_h3_custom_column`: Custom H3 column names

### Error Handling Tests
- `test_geoparquet_missing_metadata`: Error when GeoParquet metadata is absent
- `test_geoparquet_invalid_geometry_column`: Error when geometry column doesn't exist
- `test_coordinates_missing_column`: Error when coordinate columns are missing
- `test_coordinates_invalid_type`: Error when coordinate columns are not numeric
- `test_coordinates_wrong_format`: Error when geometry_column is not a list/tuple
- `test_h3_missing_column`: Error when H3 column doesn't exist
- `test_h3_no_column_provided`: Error when H3 column parameter is not provided
- `test_invalid_geometry_format`: Error when invalid geometry format specified

### Real Data Tests
- `test_example_geoparquet_dataset`: Uses sample GeoParquet from project
- `test_coordinates_with_schema`: Uses sample coordinate data with schema file

### Schema and Complex Type Tests
- `test_complex_data_types`: Arrays and structs converted to strings
- `test_long_string_fields`: Proper handling of long string fields

### Integration Tests
- `test_roundtrip_geoparquet`: Feature Class → GeoParquet → Feature Class
- `test_partitioned_dataset`: Converting entire partitioned dataset
- `test_specific_partition`: Converting specific partition only

## Dependencies

### Required
- `arcpy`: ArcGIS Python API
- `pandas`: Data manipulation
- `pyarrow`: Parquet file handling
- `pytest`: Test framework

### Optional
- `shapely`: Required for creating test GeoParquet files (most tests)
  - Tests requiring shapely will be skipped if not installed
- `h3`: Required for H3 format tests
  - H3 tests will be skipped if not installed

## Running Tests

### Run all tests
```bash
pytest testing/test_parquet_to_feature_class.py -v
```

### Run specific test category
```bash
# GeoParquet tests only
pytest testing/test_parquet_to_feature_class.py -v -k "geoparquet"

# COORDINATES tests only
pytest testing/test_parquet_to_feature_class.py -v -k "coordinates"

# H3 tests only
pytest testing/test_parquet_to_feature_class.py -v -k "h3"

# Error handling tests only
pytest testing/test_parquet_to_feature_class.py -v -k "error or missing or invalid"
```

### Run specific test
```bash
pytest testing/test_parquet_to_feature_class.py::test_geoparquet_point -v
```

### Show test coverage
```bash
pytest testing/test_parquet_to_feature_class.py --cov=arcpy_parquet --cov-report=html
```

## Test Data

Tests use a combination of:
1. **Dynamically created test data**: Helper functions create temporary Parquet files for testing
2. **Sample data**: Located in `data/sample/` directory (some tests skip if not available)

### Helper Functions
- `create_test_geoparquet()`: Creates GeoParquet files with various geometry types
- `create_test_coordinates_parquet()`: Creates Parquet with coordinate columns
- `create_test_h3_parquet()`: Creates Parquet with H3 indices
- `validate_feature_class()`: Validates output feature class properties

## Test Fixtures

Defined in `conftest.py`:
- `tmp_dir`: Session-scoped temporary directory
- `tmp_pqt`: Function-scoped temporary Parquet directory
- `tmp_gdb`: Session-scoped temporary file geodatabase

## Expected Behavior

### GeoParquet Format
- Primary geometry column is used for features
- All geometry columns (including secondary) are excluded from attribute fields
- Spatial reference is read from GeoParquet metadata
- Multiple geometry types handled appropriately

### COORDINATES Format
- Two numeric columns required (X, Y)
- Creates Point features
- Validates column existence and numeric types
- Warns if used on GeoParquet data

### H3 Format
- Single H3 index column required
- Creates Polygon features from H3 cells
- Validates column existence
- Warns if used on GeoParquet data

## Known Limitations

1. Complex data types (arrays, structs) are converted to strings
2. Only WKB-encoded geometries supported for GeoParquet
3. H3 format requires the `h3` package to be installed

## Troubleshooting

### Tests skip with "shapely package not installed"
Install shapely: `pip install shapely`

### Tests skip with "h3 package not installed"
Install h3: `pip install h3`

### "Sample data not available" messages
Some tests require sample data in `data/sample/`. These tests will skip if the data is not present.

## Contributing

When adding new tests:
1. Follow the naming convention: `test_<format>_<feature>`
2. Use appropriate fixtures (`tmp_gdb`, `tmp_pqt`)
3. Add skipif decorators for optional dependencies
4. Document expected behavior in docstring
5. Use helper functions for validation
