from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import dagster as dg
from abc import abstractmethod

import numpy as np


@dataclass(frozen=True)
class GridData:
    """Data class representing a collection of geographic points and associated values"""

    variable: str
    unit: str
    timestamp: datetime
    lats: np.ndarray
    lons: np.ndarray
    values: np.ndarray
    catalog_id: UUID

    def __post_init__(self) -> None:
        if self.lats.ndim != 1:
            raise ValueError(f"lats must be 1-dimensional, got shape {self.lats.shape}")
        if self.lons.ndim != 1:
            raise ValueError(f"lons must be 1-dimensional, got shape {self.lons.shape}")
        if self.values.ndim != 1:
            raise ValueError(f"values must be 1-dimensional, got shape {self.values.shape}")
        if self.lons.size != self.lats.size != self.values.size:
            raise ValueError(f"values, lats and lons must have the same size, got ({self.values.size}, {self.lats.size}, {self.lons.size})")

    @property
    def row_count(self) -> int:
        """Returns the number of rows (points) in the grid data."""
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