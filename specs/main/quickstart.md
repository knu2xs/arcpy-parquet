# Quickstart: Efficient GeoParquet Integration

## Prerequisites

- ArcGIS-compatible Python environment activated.
- Dependencies installed from project environment (`pyarrow`, `pytest`, etc.).
- Sample/test geospatial data available in repository test fixtures.

## 1. Validate Package Imports

```powershell
python -c "from arcpy_parquet import features_to_geoparquet, geoparquet_to_features, get_geometry_columns; print('ok')"
```

Expected result: `ok`

## 2. Run Core Tests

```powershell
pytest testing/test_arcpy_parquet.py -q
pytest testing/test_parquet_to_feature_class.py -q
pytest testing/test_geoparquet_validity.py -q
```

Expected result: all relevant tests pass for conversion behavior and metadata validity.

## 3. Run Roundtrip Sanity Flow

```python
from arcpy_parquet import features_to_geoparquet, geoparquet_to_features

dataset = features_to_geoparquet(
    feature_class=r"data/sample/sample.gdb/sample_layer",
    output_path=r"data/interim/roundtrip_dataset",
    batch_size=10000,
    overwrite=True,
)

geoparquet_to_features(
    parquet_path=dataset,
    feature_class=r"data/interim/interim.gdb/roundtrip_layer",
    overwrite=True,
)
```

Expected result: output dataset is created and import succeeds with matching geometry/attributes.

## 4. Validate ArcGIS Toolbox Integration

- Open `arcgis/arcpy-parquet.aprx`.
- Run updated toolbox tools from `arcgis/ArcPy-Parquet-Tools.pyt`.
- Confirm parameter mappings for `batch_size`, `geometry_column`, `include_centroids`, and `name`.

## 5. Check Compatibility Warnings

Run legacy conversion entry points and confirm deprecation warnings indicate migration targets.

## 6. Documentation Follow-through

- Update `README.md` and `arcgis/README.md` migration sections.
- Rebuild docs if changed:

```powershell
make docs
```
