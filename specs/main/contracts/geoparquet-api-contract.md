# Contract: GeoParquet API and ArcGIS Toolbox Mapping

## Scope

This contract defines external behavior for:
- Public Python APIs exposed by `arcpy_parquet`.
- ArcGIS toolbox parameter mapping in `arcgis/ArcPy-Parquet-Tools.pyt`.

## Public Python API Contract

### Function: `features_to_geoparquet`

- Purpose: Export feature class data to a GeoParquet file or dataset.
- Required inputs:
  - `feature_class`
  - `output_path`
- Optional inputs:
  - `fields`, `partition_fields`, `where_clause`, `spatial_reference`, `overwrite`, `batch_size`, `include_centroids`, `name`
- Required behavior:
  - Writes Parquet output with GeoParquet 1.1-compliant metadata.
  - Encodes geometry via `GEOPARQUET` output (WKB internally per GeoParquet).
  - Supports optional partitioned output when partition fields are provided.
- Error behavior:
  - Raises clear exceptions for invalid source path, invalid partition fields, and non-overwritable output conflicts.

### Function: `geoparquet_to_features`

- Purpose: Import GeoParquet file/dataset into a feature class.
- Required inputs:
  - `parquet_path`
  - `feature_class`
- Optional inputs:
  - `geometry_column`, `spatial_reference`, `overwrite`, `batch_size`
- Required behavior:
  - Uses GeoParquet primary geometry when `geometry_column` is omitted.
  - Validates explicit geometry-column selection when provided.
  - Produces feature class with expected geometry and attribute parity.
- Error behavior:
  - Raises clear exceptions for missing input, invalid geometry column, and overwrite conflicts.

### Function: `get_geometry_columns`

- Purpose: Return declared geometry column names from GeoParquet metadata.
- Required behavior:
  - Returns deterministic ordered names including primary geometry.
  - Fails with explicit error for missing/invalid input path.

## Backward Compatibility Contract

- Legacy conversion entry points remain callable during migration window.
- Legacy calls emit deprecation warnings with migration targets:
  - `feature_class_to_parquet` -> `features_to_geoparquet`
  - `parquet_to_feature_class` -> `geoparquet_to_features`

## ArcGIS Toolbox Mapping Contract

### Tool: `FeatureClassToParquet`

- Must map to `features_to_geoparquet`.
- Includes support for:
  - `include_centroids` (boolean)
  - `name` (optional string)
  - `batch_size` (integer default)

### Tool: `GeoparquetToFeatureClass`

- Must map to `geoparquet_to_features`.
- Includes support for:
  - `geometry_column` (optional string)
  - `batch_size` (integer default)

## Validation Contract

Automated tests in `testing/` must cover:
- Export happy path, import happy path, and roundtrip integrity.
- GeoParquet metadata validity and geometry-column behavior.
- Partitioned export behavior.
- Deprecation warning behavior for legacy APIs.
- Error handling for invalid geometry column, invalid paths, and overwrite constraints.
