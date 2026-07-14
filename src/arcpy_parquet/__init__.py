__title__ = "arcpy-parquet"
__version__ = "0.2.0.dev0"
__author__ = "Joel McCune (https://github.com/knu2xs)"
__license__ = "Apache 2.0"
__copyright__ = "Copyright 2023 by Joel McCune (https://github.com/knu2xs)"

# add specific imports below if you want to organize your code into modules, which is mostly what I do
from . import utils


def _missing_dependency_factory(func_name: str, err: Exception):
    def _raise_missing_dependency(*args, **kwargs):
        raise ImportError(
            f"{func_name} is unavailable because optional parquet dependencies "
            f"failed to import: {err}"
        ) from err

    return _raise_missing_dependency


try:
    from .__main__ import (
        feature_class_to_parquet,
        parquet_to_feature_class,
        create_schema_file,
    )
except ImportError as _main_import_error:
    feature_class_to_parquet = _missing_dependency_factory(
        "feature_class_to_parquet", _main_import_error
    )
    parquet_to_feature_class = _missing_dependency_factory(
        "parquet_to_feature_class", _main_import_error
    )
    create_schema_file = _missing_dependency_factory(
        "create_schema_file", _main_import_error
    )

try:
    from .geoparquet import (
        features_to_geoparquet,
        geoparquet_to_features,
        get_geometry_columns,
    )
except ImportError as _geo_import_error:
    features_to_geoparquet = _missing_dependency_factory(
        "features_to_geoparquet", _geo_import_error
    )
    geoparquet_to_features = _missing_dependency_factory(
        "geoparquet_to_features", _geo_import_error
    )
    get_geometry_columns = _missing_dependency_factory(
        "get_geometry_columns", _geo_import_error
    )

__all__ = [
    "create_schema_file",
    "feature_class_to_parquet",
    "features_to_geoparquet",
    "geoparquet_to_features",
    "get_geometry_columns",
    "parquet_to_feature_class",
    "utils",
]
