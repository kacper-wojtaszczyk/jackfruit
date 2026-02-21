from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import dagster as dg
from abc import abstractmethod, ABC

import numpy as np


@dataclass(frozen=True)
class GridData:
    """
    Extracted grid data ready for ClickHouse insertion.

    All arrays are 2D of the same shape (M, K) — one element per grid point.
    Flattening to 1D happens in the storage layer.
    Units are source-dependent; conversion (if any) happens during extraction.

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
            raise ValueError(f"values must be 2-dimensional, got shape {self.values.shape}")
        if not (self.lats.shape == self.lons.shape == self.values.shape):
            raise ValueError(
                f"All arrays must have the same shape, "
                f"got lats={self.lats.shape}, lons={self.lons.shape}, values={self.values.shape}"
            )

    @property
    def row_count(self) -> int:
        """Total number of grid points."""
        return self.values.size


class GridStore(dg.ConfigurableResource, ABC):
    """
    Abstract base class for grid data storage backends.

    Subclasses must implement insert_grid(). Extends ConfigurableResource so Dagster
    manages lifecycle (config injection, teardown).
    """

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
