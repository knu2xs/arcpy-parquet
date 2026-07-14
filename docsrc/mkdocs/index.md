---
title: Home
---
# ArcPy Parquet 0.3.0 Documentation

ArcPy Parquet provides conversion between ArcGIS Pro feature classes and Parquet datasets,
with GeoParquet as the default spatial data standard. While ArcGIS Pro can read Parquet
directly, built-in support is limited for many partitioned and sharded layouts. ArcPy
Parquet is designed for large, distributed-style Parquet datasets and practical ArcGIS
workflows.

ArcPy Parquet is comprised of an installable Python package located in `./src` along with an ArcGIS Python Toolbox, 
which can be used in ArcGIS Pro.

## Start Here

Most users are looking for the ArcGIS Pro toolbox workflow:

1. **ArcGIS Pro users**: start with [ArcGIS Pro Toolbox](toolbox.md).
2. **Python developers**: go to [ArcPy Parquet Python API](api.md).

If you download the entire ArcPy Parquet repository to your local machine, and keep all the downloaded assets 
together in the same location, you do not need to customize your Python configuration at all. The toolbox knows 
how to find the Python package relative to itself, and will include all functionality.

## API Surface

Use the following public functions:

See the full API reference: [ArcPy Parquet Python API](api.md).

- [`features_to_parquet`](api.md#arcpy_parquet.features_to_parquet)
- [`parquet_to_features`](api.md#arcpy_parquet.parquet_to_features)
- [`create_schema_file`](api.md#arcpy_parquet.create_schema_file)

These functions define the package conversion interface.

## Geometry Export Notes

- `GEOPARQUET` is the default export geometry format.
- `WKB` is not a supported top-level export format value.
- `XY` exports coordinates to columns `x_lon` and `y_lat`.
- `H3` exports H3 indices using h3-py helper-driven geometry conversion.
