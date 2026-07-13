"""Parquet conversion utilities for Esri Feature Classes.

This sub-package exposes helpers to convert between Esri Feature Classes
and the GeoParquet format.
"""

from ._parquet import (
    features_to_geoparquet,
    geoparquet_to_features,
    get_geometry_columns,
)

__all__ = [
    "features_to_geoparquet",
    "geoparquet_to_features",
    "get_geometry_columns",
]
