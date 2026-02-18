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

    @property
    def row_count(self) -> int:
        """Returns the number of rows (points) in the grid data."""
        return self.lats.size * self.lons.size

class GridStore(dg.ConfigurableResource):
    @abstractmethod
    def insert_grid(self, grid_data: GridData) -> int:
        ...