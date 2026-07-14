"""Compatibility tests for deprecation behavior of legacy entry points."""

from __future__ import annotations

import warnings

import pytest

pytest.importorskip("arcpy")

from arcpy_parquet._compat import (
    warn_feature_class_to_parquet_deprecated,
    warn_parquet_to_feature_class_deprecated,
)


def test_feature_class_deprecation_warning() -> None:
    """Legacy export warning should be emitted as DeprecationWarning."""
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        warn_feature_class_to_parquet_deprecated()
    assert any(item.category is DeprecationWarning for item in captured)


def test_parquet_to_feature_class_deprecation_warning() -> None:
    """Legacy import warning should be emitted as DeprecationWarning."""
    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        warn_parquet_to_feature_class_deprecated()
    assert any(item.category is DeprecationWarning for item in captured)
