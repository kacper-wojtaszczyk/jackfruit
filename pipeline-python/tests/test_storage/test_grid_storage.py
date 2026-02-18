"""
Tests for the generic grid storage module (just the data class i guess)
"""
import os
from datetime import datetime
from uuid import uuid7

import numpy as np

from pipeline_python.storage.clickhouse import ClickHouseGridStore
from pipeline_python.storage.grid_store import GridData


class TestGridData:
    def test_row_count(self):
        grid = GridData(
            variable="temp",
            unit="°C",
            timestamp=datetime(2026, 1, 1, 2, 0, 0),
            lats=np.array([30.05, 30.15, 30.25]),
            lons=np.array([335.05, 335.15, 335.25, 335.35, 335.45, 335.55]),
            values=np.array([
                [15.1, 15.3, 15.1, 14.88, 14.5, 14.0],
                [15.3, 15.6, 15.7, 15.0, 14.8, 14.8],
                [15.7, 16.0, 16.2, 15.5, 15.0, 14.9],
            ]),
            catalog_id=uuid7()
        )

        assert grid.row_count == 18

class TestClickHouse:
    def test_insert_clickhouse_row(self):
        grid = GridData(
            variable="temp",
            unit="°C",
            timestamp=datetime(2026, 1, 1, 2, 0, 0),
            lats=np.array([30.05, 30.15, 30.25]),
            lons=np.array([335.05, 335.15, 335.25, 335.35, 335.45, 335.55]),
            values=np.array([
                [15.1, 15.3, 15.1, 14.88, 14.5, 14.0],
                [15.3, 15.6, 15.7, 15.0, 14.8, 14.8],
                [15.7, 16.0, 16.2, 15.5, 15.0, 14.9],
            ]),
            catalog_id=uuid7()
        )

        ch = ClickHouseGridStore(
            host="localhost",
            username="jackfruit",
            password="jackfruit",
            database="jackfruit",
        )

        ch.insert_grid(grid)