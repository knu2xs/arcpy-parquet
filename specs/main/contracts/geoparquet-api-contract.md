# Contract: GeoParquet API and Toolbox Mapping

## Scope

Defines contract expectations for the efficient GeoParquet integration across:
- Python package public API in `arcpy_parquet`
- ArcGIS toolbox parameter mapping in `arcgis/ArcPy-Parquet-Tools.pyt`

## Public Python API Contract

### Function: `features_to_geoparquet`

- Purpose: Export feature class data to GeoParquet dataset/file output.
- Required inputs:
  - `feature_class`: Existing feature class path.
  - `output_path`: Destination path.
- Optional inputs:
  - `fields`, `partition_fields`, `where_clause`, `spatial_reference`,
    `overwrite`, `batch_size`, `include_centroids`, `name`.
- Required behavior:
  - Writes valid Parquet output with GeoParquet metadata.
  - Uses WKB geometry encoding.
  - Supports partitioned output when `partition_fields` are provided.
- Error contract:
  - Raises explicit exceptions for invalid source, invalid partition fields,
    and conflicting output state when overwrite is disabled.

### Function: `geoparquet_to_features`

- Purpose: Import GeoParquet dataset/file into feature class.
- Required inputs:
  - `parquet_path`: Existing Parquet dataset/file path.
  - `feature_class`: Output feature class path.
- Optional inputs:
  - `geometry_column`, `spatial_reference`, `overwrite`, `batch_size`.
- Required behavior:
  - Reads primary geometry by default when `geometry_column` is not provided.
  - Validates geometry column when explicitly specified.
  - Produces feature class with expected attribute and geometry data.
- Error contract:
  - Raises explicit exceptions for missing input path, invalid geometry column,
    and disallowed overwrite behavior.

### Function: `get_geometry_columns`

- Purpose: Return geometry column names declared in GeoParquet metadata.
- Required behavior:
  - Returns primary and additional geometry columns in deterministic order.
  - Fails with clear error if input path does not exist.

## Backward Compatibility Contract

- Legacy public functions remain callable during migration window.
- Legacy calls emit deprecation warnings with explicit migration targets:
  - `feature_class_to_parquet` -> `features_to_geoparquet`
  - `parquet_to_feature_class` -> `geoparquet_to_features`

## ArcGIS Toolbox Mapping Contract

### Tool: FeatureClassToParquet

- Maps to `features_to_geoparquet`.
- Required mapping updates:
  - `include_centroids` (bool)
  - `name` (string, optional)
  - `batch_size` (int, default 10000)

### Tool: GeoparquetToFeatureClass

- Maps to `geoparquet_to_features`.
- Required mapping updates:
  - `geometry_column` (string, optional)
  - `batch_size` (int, default 10000)

## Validation and Test Contract

- Tests MUST cover:
  - Export and import happy paths.
  - Partitioned export behavior.
  - Geometry metadata validation.
  - Compatibility warning behavior.
  - Error handling for invalid geometry column and missing paths.
