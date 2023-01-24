__title__ = 'arcpy-parquet'
__version__ = '0.1.1-dev0'
__author__ = 'Joel McCune'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2022 by Joel McCune'

"""
Resources making data_dir import and export easier, and in some cases, *possible*, in an
environment with ArcGIS.

.. note::

    This module requires ``arcpy`` and ``pyarrow``. Both are included with the default
    installation of ArcGIS Pro or the ArcGIS Enterprise Application Server component of
    ArcGIS Enterprise.

"""

from .main import feature_class_to_parquet, parquet_to_feature_class, create_schema_file

__all__ = ['feature_class_to_parquet', 'parquet_to_feature_class', 'create_schema_file']
