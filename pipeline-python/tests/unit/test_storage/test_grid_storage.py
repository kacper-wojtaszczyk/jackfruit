"""
Tests for the generic grid storage module (just the data class).
"""
from datetime import datetime
from uuid import uuid7

import numpy as np
import pytest

from pipeline_python.storage.grid_store import GridData, GridStore


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


class TestGridStoreCompact:
    """Tests for the GridStore.compact() no-op default."""

    def test_compact_is_not_abstract(self):
        """compact() should have a default no-op — subclasses that only implement
        insert_grid() should instantiate and call compact() without error."""

        class MinimalStore(GridStore):
            def insert_grid(self, grid: GridData) -> int:
                return 0

        store = MinimalStore()
        # Should not raise — the no-op default does nothing
        store.compact()

    def test_compact_default_returns_none(self):
        """The default compact() should return None."""

        class MinimalStore(GridStore):
            def insert_grid(self, grid: GridData) -> int:
                return 0

        assert MinimalStore().compact() is None
