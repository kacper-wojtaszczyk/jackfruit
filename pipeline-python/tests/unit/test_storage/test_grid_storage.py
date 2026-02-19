"""
Tests for the generic grid storage module (just the data class).
"""
from datetime import datetime
from uuid import uuid4

import numpy as np

from pipeline_python.storage.grid_store import GridData


class TestGridData:
    def test_row_count(self):
        n = 18
        grid = GridData(
            variable="temp",
            unit="°C",
            timestamp=datetime(2026, 1, 1, 2, 0, 0),
            lats=np.linspace(30.05, 30.25, n, dtype=np.float32),
            lons=np.linspace(335.05, 335.55, n, dtype=np.float32),
            values=np.ones(n, dtype=np.float32) * 15.0,
            catalog_id=uuid4(),
        )

        assert grid.row_count == n

    def test_validates_lats_must_be_1d(self):
        with np.testing.assert_raises(ValueError):
            GridData(
                variable="temp",
                unit="°C",
                timestamp=datetime(2026, 1, 1, 2, 0, 0),
                lats=np.ones((3, 6), dtype=np.float32),
                lons=np.ones(18, dtype=np.float32),
                values=np.ones(18, dtype=np.float32),
                catalog_id=uuid4(),
            )

    def test_validates_values_must_be_1d(self):
        with np.testing.assert_raises(ValueError):
            GridData(
                variable="temp",
                unit="°C",
                timestamp=datetime(2026, 1, 1, 2, 0, 0),
                lats=np.ones(18, dtype=np.float32),
                lons=np.ones(18, dtype=np.float32),
                values=np.ones((3, 6), dtype=np.float32),
                catalog_id=uuid4(),
            )

    def test_validates_all_arrays_same_size(self):
        with np.testing.assert_raises(ValueError):
            GridData(
                variable="temp",
                unit="°C",
                timestamp=datetime(2026, 1, 1, 2, 0, 0),
                lats=np.ones(3, dtype=np.float32),
                lons=np.ones(6, dtype=np.float32),
                values=np.ones(18, dtype=np.float32),
                catalog_id=uuid4(),
            )
