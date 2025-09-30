__title__ = "arcpy-parquet"
__version__ = "0.2.0.dev0"
__author__ = "Joel McCune (https://github.com/knu2xs)"
__license__ = "Apache 2.0"
__copyright__ = "Copyright 2023 by Joel McCune (https://github.com/knu2xs)"

# add specific imports below if you want to organize your code into modules, which is mostly what I do
from . import utils
from .__main__ import (
    feature_class_to_parquet,
    parquet_to_feature_class,
    create_schema_file,
)

__all__ = [
    "create_schema_file",
    "feature_class_to_parquet",
    "parquet_to_feature_class",
    "utils",
]
