__title__ = "arcpy-parquet"
__version__ = "0.3.0.dev0"
__author__ = "Joel McCune (https://github.com/knu2xs)"
__license__ = "Apache 2.0"
__copyright__ = "Copyright 2023 by Joel McCune (https://github.com/knu2xs)"

# add specific imports below if you want to organize your code into modules, which is mostly what I do
from . import utils

from .__main__ import (
    create_schema_file,
    features_to_parquet,
    parquet_to_features,
)

__all__ = [
    "create_schema_file",
    "features_to_parquet",
    "parquet_to_features",
    "utils",
]
