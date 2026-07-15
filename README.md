# ArcPy Parquet

Conversion utilities for conversion to and from Parquet data using ArcPy.

## ArcPy Parquet Python Toolbox

The ArcGIS Pro Python toolbox is provided as `arcgis/ArcPy-Parquet-Tools.pyt`.
It includes the following geoprocessing tools:

- `FeatureClassToParquet`: exports ArcGIS feature classes/tables to Parquet datasets (GeoParquet by default).
- `GeoparquetToFeatureClass`: imports Parquet/GeoParquet datasets into ArcGIS feature classes.
- `CreateSchemaFile`: creates CSV schema templates to standardize field definitions for conversion workflows.

These tools use the same core implementation as the Python package, so toolbox behavior aligns with the
`features_to_parquet`, `parquet_to_features`, and `create_schema_file` functions in the `arcpy_parquet` package.

## Python API

The primary API surface for new work is:

- `features_to_parquet`
- `parquet_to_features`
- `create_schema_file`

```python
from arcpy_parquet import features_to_parquet, parquet_to_features

dataset_dir = features_to_parquet(
  input_features=r"data/sample/sample.gdb/sample_layer",
  output_parquet=r"data/interim/roundtrip_dataset",
  geometry_format="GEOPARQUET",
  batch_size=50000,
)

parquet_to_features(
  parquet_path=dataset_dir,
  output_feature_class=r"data/interim/interim.gdb/roundtrip_layer",
  geometry_format="GEOPARQUET",
)
```

### Geometry Format Notes

- `GEOPARQUET` is the default geometry format for export and writes GeoParquet metadata.
- `XY` writes two columns named `x_lon` and `y_lat`.
- `H3` writes H3 indices: a single index string for points, and JSON-encoded intersecting indices for lines/polygons.

<p><small>Project based on the <a target="_blank" href="https://github.com/knu2xs/cookiecutter-geoai">cookiecutter 
GeoAI project template</a>. This template, in turn, is simply an extension and light modification of the 
<a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project 
template</a>. #cookiecutterdatascience</small></p>

