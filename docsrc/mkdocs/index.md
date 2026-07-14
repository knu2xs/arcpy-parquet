---
title: Home
---
# ArcPy Parquet 0.3.0 Documentation

ArcPy Parquet provides conversion between ArcGIS Pro feature classes and Parquet datasets,
with GeoParquet as the default spatial data standard. While ArcGIS Pro can read Parquet
directly, built-in support is limited for many partitioned and sharded layouts. Reading 
and importing requires multiple steps, and exporting partitioneda and sharded data is 
not possible.

ArcPy Parquet addresses these issues. It is designed for working with large, distributed, 
hive-style Parquet datasets.

ArcPy Parquet is comprised of an installable Python package located in `./src` along with 
an ArcGIS Python Toolbox, which can be used in ArcGIS Pro.

1. **ArcGIS Pro users**: start with [ArcGIS Pro Toolbox](toolbox.md).
2. **Python developers**: go to [ArcPy Parquet Python API](api.md).

If you download the entire ArcPy Parquet repository to your local machine, and keep all the 
downloaded assets together in the same location, you do not need to customize your Python 
configuration at all. The toolbox knows how to find the Python package relative to itself, 
and will include all functionality.

## API Surface

If you are more interested in using this functionality as part of a Python workflow, you can 
use the `arcpy_parquet` package directly. The package is included in the `src/arcpy_parquet/`
directory. 

In the root of the repository, there is a `pyproject.toml` file that defines the 
package and its dependencies. This means you can install the package directly after downloading
or simply by using pip install directly from the git repository 
(`pip install git+https://github.com/knu2xs/arcpy-parquet.git`).

- [`features_to_parquet`](api.md#arcpy_parquet.features_to_parquet)
- [`parquet_to_features`](api.md#arcpy_parquet.parquet_to_features)
