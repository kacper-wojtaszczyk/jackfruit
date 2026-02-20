from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import dagster as dg
from abc import abstractmethod

import numpy as np


@dataclass(frozen=True)
class GridData:
    """
    Extracted grid data ready for ClickHouse insertion.

    All arrays are 2D of the same shape (M, K) — one element per grid point.
    grib2io returns lats, lons, and data() as 2D arrays; GridData preserves this.
    Flattening to 1D happens in the storage layer (_to_columnar in clickhouse.py).
    Values are stored in µg/m³ (converted from CAMS kg m**-3 during extraction).

    Fields align with CH jackfruit.grid_data columns:
    (variable, timestamp, lat, lon, value, unit, catalog_id)
    """

    variable: str
    unit: str
    timestamp: datetime
    lats: np.ndarray
    lons: np.ndarray
    values: np.ndarray
    catalog_id: UUID

    def __post_init__(self) -> None:
        if self.values.ndim != 2:
            raise ValueError(f"values must be 2-dimensional, got shape {self.lats.shape}")
        if not (self.lats.shape == self.lons.shape == self.values.shape):
            raise ValueError(
                f"All arrays must have the same shape, "
                f"got lats={self.lats.shape}, lons={self.lons.shape}, values={self.values.shape}"
            )

    @property
    def row_count(self) -> int:
        """Total number of grid points."""
        return self.values.size


class GridStore(dg.ConfigurableResource):
    @abstractmethod
    def insert_grid(self, grid: GridData) -> int:
        """
        Insert a single grid into storage.

        Args:
            grid: Extracted grid data

        Returns:
            Number of rows inserted
        """
        ...