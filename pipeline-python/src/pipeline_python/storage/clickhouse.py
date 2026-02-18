import clickhouse_connect
import numpy as np
from clickhouse_connect.driver import Client
from pydantic import PrivateAttr

from pipeline_python.storage.grid_store import GridStore, GridData


class ClickHouseGridStore(GridStore):
    host: str
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
            )
        return self._client

    @staticmethod
    def _to_columnar(grid: GridData) -> dict[str, np.ndarray] | None:
        lon_grid, lat_grid = np.meshgrid(grid.lons, grid.lats)

        lat_flat = lat_grid.ravel()
        lon_flat = lon_grid.ravel()
        val_flat = grid.values.ravel()

        lat_filtered = lat_flat.astype(np.float64)
        lon_filtered = lon_flat.astype(np.float64)
        val_filtered = val_flat.astype(np.float32)
        n = grid.row_count

        return {
            "variable": np.full(n, grid.variable, dtype=object),
            "timestamp": np.full(n, grid.timestamp, dtype=object),
            "lat": lat_filtered,
            "lon": lon_filtered,
            "value": val_filtered,
            "unit": np.full(n, grid.unit, dtype=object),
            "catalog_id": np.full(n, grid.catalog_id, dtype=object),
        }


    def insert_grid(self, grid_data: GridData) -> int:
        data = self._to_columnar(grid_data)
        return self._get_client().insert(
            table="grid_data",
            column_names=["variable", "timestamp", "lat", "lon", "value", "unit", "catalog_id"],
            column_oriented=True,
            data=[
                data["variable"].tolist(),
                data["timestamp"].tolist(),
                data["lat"].tolist(),
                data["lon"].tolist(),
                data["value"].tolist(),
                data["unit"].tolist(),
                data["catalog_id"].tolist(),
            ],
        ).written_rows
