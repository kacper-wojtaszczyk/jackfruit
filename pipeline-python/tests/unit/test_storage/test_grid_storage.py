"""
Tests for the generic grid storage module (just the data class).
"""
from datetime import datetime
from uuid import uuid7

import numpy as np
import pytest

from pipeline_python.storage.grid_store import GridData


def _make_grid(**overrides) -> GridData:
    """Build a valid GridData with 2D arrays of shape (3, 6). Override any field."""
    defaults = dict(
        variable="temp",
        unit="°C",
        timestamp=datetime(2026, 1, 1, 2, 0, 0),
        lats=np.ones((3, 6), dtype=np.float32),
        lons=np.ones((3, 6), dtype=np.float32),
        values=np.ones((3, 6), dtype=np.float32) * 15.0,
        catalog_id=uuid7(),
    )
    return GridData(**{**defaults, **overrides})


class TestGridData:
    def test_row_count(self):
        grid = _make_grid()
        assert grid.row_count == 18

    def test_accepts_single_element_grid(self):
        grid = _make_grid(
            lats=np.ones((1, 1), dtype=np.float32),
            lons=np.ones((1, 1), dtype=np.float32),
            values=np.ones((1, 1), dtype=np.float32),
        )
        assert grid.row_count == 1

    def test_rejects_1d_values(self):
        with pytest.raises(ValueError, match="values must be 2-dimensional"):
            _make_grid(values=np.ones(18, dtype=np.float32))

    def test_rejects_3d_values(self):
        with pytest.raises(ValueError, match="values must be 2-dimensional"):
            _make_grid(values=np.ones((3, 6, 1), dtype=np.float32))

    def test_rejects_shape_mismatch_lats(self):
        with pytest.raises(ValueError, match="All arrays must have the same shape"):
            _make_grid(lats=np.ones((2, 6), dtype=np.float32))

    def test_rejects_shape_mismatch_lons(self):
        with pytest.raises(ValueError, match="All arrays must have the same shape"):
            _make_grid(lons=np.ones((3, 4), dtype=np.float32))

    def test_rejects_1d_lats_with_2d_values(self):
        with pytest.raises(ValueError, match="All arrays must have the same shape"):
            _make_grid(lats=np.ones(18, dtype=np.float32))
