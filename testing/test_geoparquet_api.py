"""API-level tests for GeoParquet conversion entry points."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("arcpy")


def test_public_parquet_exports_are_callable() -> None:
    """Validate primary API exports are importable and callable objects."""
    from arcpy_parquet import (
        features_to_parquet,
        parquet_to_features,
    )

    assert callable(features_to_parquet)
    assert callable(parquet_to_features)


@pytest.mark.skip(reason="Requires ArcPy dataset fixtures for integration execution.")
def test_batch_size_validation_placeholder(tmp_path: Path) -> None:
    """Placeholder for batch-size validation behavior tests."""
    _ = tmp_path


@pytest.mark.skip(reason="Requires ArcPy dataset fixtures for quickstart smoke execution.")
def test_quickstart_smoke_placeholder(tmp_path: Path) -> None:
    """Placeholder for documented quickstart smoke tests."""
    _ = tmp_path
