# Quickstart: Efficient GeoParquet Integration Validation

## Prerequisites

- ArcGIS-compatible Python environment activated (project `env` or equivalent).
- Project dependencies installed (`make env` or equivalent setup).
- Access to representative test data in repository fixtures.

## 1. Validate Public API Availability

```powershell
python -c "from arcpy_parquet import features_to_geoparquet, geoparquet_to_features, get_geometry_columns; print('ok')"
```

Expected outcome: `ok`.

## 2. Execute Core Test Coverage

```powershell
python -m pytest testing/test_arcpy_parquet.py -q
python -m pytest testing/test_parquet_to_feature_class.py -q
python -m pytest testing/test_geoparquet_validity.py -q
python -m pytest testing/test_geoparquet_api.py -q
python -m pytest testing/test_compat_deprecations.py -q
```

Expected outcome: tests pass for conversion behavior, metadata handling, and compatibility/error paths.

## 3. Run Export/Import Roundtrip Scenario

```python
from arcpy_parquet import features_to_geoparquet, geoparquet_to_features

out_dataset = features_to_geoparquet(
    feature_class=r"data/sample/sample.gdb/sample_layer",
    output_path=r"data/interim/roundtrip_dataset",
    batch_size=10000,
    overwrite=True,
)

geoparquet_to_features(
    parquet_path=out_dataset,
    feature_class=r"data/interim/interim.gdb/roundtrip_layer",
    overwrite=True,
)
```

Expected outcome: GeoParquet output is created and imported feature class matches expected geometry/attribute structure.

## 4. Validate Toolbox Parameter Mapping

- Open `arcgis/arcpy-parquet.aprx`.
- Run tools from `arcgis/ArcPy-Parquet-Tools.pyt`.
- Confirm parameter mapping coverage for `batch_size`, `geometry_column`, `include_centroids`, and `name`.

Expected outcome: toolbox execution maps parameters to new API behavior without regression.

## 5. Confirm Legacy Compatibility Warnings

- Run legacy entry points (`feature_class_to_parquet`, `parquet_to_feature_class`).
- Verify deprecation warnings include migration targets.

Expected outcome: behavior remains functional while warning users to migrate.

## 6. Documentation and Workflow Validation

- Update migration details in `README.md` and `arcgis/README.md` if changed.
- Run docs build when documentation changes are present:

```powershell
make docs
```

Expected outcome: documentation remains reproducible and consistent with implemented behavior.
