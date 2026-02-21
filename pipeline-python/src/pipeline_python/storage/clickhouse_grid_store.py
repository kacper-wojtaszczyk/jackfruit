import clickhouse_connect
import numpy as np
from clickhouse_connect.driver import Client
from dagster import InitResourceContext
from pydantic import PrivateAttr

from pipeline_python.storage.grid_store import GridStore, GridData


class ClickHouseGridStore(GridStore):
    """
    ClickHouse implementation of GridStore.

    Uses clickhouse-connect for column-oriented batch inserts. Connection is created
    lazily on first insert and closed by Dagster via teardown_after_execution.

    Attributes:
        host: ClickHouse server hostname
        port: ClickHouse HTTP port
        username: ClickHouse username
        password: ClickHouse password
        database: Target database name
    """

    host: str
    port: int
    username: str
    password: str
    database: str
    _client: Client | None = PrivateAttr(default=None)

    def _get_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                username=self.username,
                password=self.password,
                database=self.database,
                port=self.port,
            )
        return self._client

    def insert_grid(self, grid: GridData) -> int:
        """
        Insert a single grid into ClickHouse as column-oriented data.

        Flattens 2D lat/lon/value arrays to 1D and inserts into the grid_data table.

        Args:
            grid: Extracted grid data with 2D arrays

        Returns:
            Number of rows written
        """
        return self._get_client().insert(
            table="grid_data",
            column_names=["variable", "timestamp", "lat", "lon", "value", "unit", "catalog_id"],
            column_oriented=True,
            data=[
                np.full(grid.row_count, grid.variable, dtype=object),
                np.full(grid.row_count, grid.timestamp, dtype=object),
                grid.lats.ravel().astype(np.float32),
                grid.lons.ravel().astype(np.float32),
                grid.values.ravel().astype(np.float32),
                np.full(grid.row_count, grid.unit, dtype=object),
                np.full(grid.row_count, grid.catalog_id, dtype=object),
            ],
        ).written_rows

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Close the ClickHouse client. Called by Dagster at end of each asset execution."""
        if self._client is not None:
            self._client.close()
            self._client = None
