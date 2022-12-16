__title__ = 'arcpy-parquet'
__version__ = '0.0.0'
__author__ = 'Joel McCune'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2022 by Joel McCune'

from .main import feature_class_to_parquet, parquet_to_feature_class, create_schema_file

__all__ = ['feature_class_to_parquet', 'parquet_to_feature_class', 'create_schema_file']
