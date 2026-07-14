# ArcPy Parquet Python Toolbox (`ArcPy-Parquet-Tools.pyt`)

This is the documentation to be included if planning on packaging the `ArcPy-Parquet-Tools.pyt`
toolbox using the `make pyt_pkg` command invoking the `./scripts/make_pyt_archive.pyt`. This script
packages the `ArcPy-Parquet-Tools.pyt`, supporting Python package `./src/arcpy_parquet` and any 
dependencies listed in `pyproject.toml`. This enables you to distribute the Python toolbox for 
non-technical users with any custom libraries you are utilizing.

## Tool Mapping Notes

Tool execution is aligned with the GeoParquet API surface in `arcpy_parquet`:

- `FeatureClassToParquet` maps to `features_to_geoparquet`
- `GeoparquetToFeatureClass` maps to `geoparquet_to_features`

Expected parameter coverage for current and upcoming toolbox updates:

- export path: `batch_size`, `include_centroids`, optional `name`
- import path: `geometry_column`, `batch_size`

When packaging the toolbox, keep the `src/arcpy_parquet` package version synchronized with the toolbox XML docs so users can rely on matching parameter semantics.