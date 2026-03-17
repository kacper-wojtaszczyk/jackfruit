"""
Unit tests for transform assets (CAMS + ECMWF).

Transform assets can't use dg.materialize() because they read upstream metadata
via instance.get_event_records(). Instead we use direct invocation with
dg.build_asset_context() + instance.report_runless_asset_event().
"""
import shutil
import uuid
from pathlib import Path

import dagster as dg
import numpy as np
import pytest

from pipeline_python.defs.assets import (
    transform_cams_data,
    transform_ecmwf_data,
    _clip_to_europe,
    _EUROPE_LAT_MIN,
    _EUROPE_LAT_MAX,
    _EUROPE_LON_MIN,
    _EUROPE_LON_MAX,
)
from pipeline_python.defs.models import CuratedDataRecord
from pipeline_python.storage.grid_store import GridStore, GridData


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CAMS_FIXTURE = Path(__file__).parent.parent / "fixtures" / "019c7f73-419f-727c-8e56-95880501e36b.grib"
ECMWF_FIXTURE = Path(__file__).parent.parent / "fixtures" / "019cf6d7-02a0-745b-ac05-e1201d8f8a72.grib"

CAMS_RUN_ID = "019c7f73-419f-727c-8e56-95880501e36b"
CAMS_SOURCE = "ads"
CAMS_DATASET = "cams-europe-air-quality-forecast"
CAMS_PARTITION = "2026-02-21"

ECMWF_RUN_ID = "019cf6d7-02a0-745b-ac05-e1201d8f8a72"
ECMWF_SOURCE = "ecmwf"
ECMWF_DATASET = "ifs-weather-forecast"
ECMWF_PARTITION = "2026-03-16"


# ---------------------------------------------------------------------------
# Module-level state for mock tracking
# ---------------------------------------------------------------------------

_grid_inserts: list[GridData] = []
_curated_inserts: list[CuratedDataRecord] = []
_download_calls: list[dict] = []


# ---------------------------------------------------------------------------
# Mock resources
# ---------------------------------------------------------------------------


class MockGridStore(GridStore):
    """Records GridData inserts and returns row_count."""

    def insert_grid(self, grid: GridData) -> int:
        _grid_inserts.append(grid)
        return grid.row_count


class MockObjectStore(dg.ConfigurableResource):
    """Copies the fixture file on download_raw()."""

    fixture_path: str
    should_fail: bool = False

    def download_raw(self, key: str, local_path: Path) -> None:
        _download_calls.append({"key": key, "local_path": local_path})
        if self.should_fail:
            raise FileNotFoundError(f"Mock download failure: {key}")
        shutil.copy2(self.fixture_path, local_path)


class MockCatalogResource(dg.ConfigurableResource):
    """Records CuratedDataRecord inserts."""

    should_fail: bool = False

    def insert_curated_data(self, record: CuratedDataRecord) -> None:
        _curated_inserts.append(record)
        if self.should_fail:
            raise Exception("Mock catalog failure")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_mock_state():
    """Reset all module-level mock state before each test."""
    global _grid_inserts, _curated_inserts, _download_calls
    _grid_inserts = []
    _curated_inserts = []
    _download_calls = []


def _report_cams_upstream(instance: dg.DagsterInstance) -> None:
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="ingest_cams_data",
            metadata={"run_id": CAMS_RUN_ID, "dataset": CAMS_DATASET, "source": CAMS_SOURCE},
            partition=CAMS_PARTITION,
        )
    )


def _report_ecmwf_upstream(instance: dg.DagsterInstance) -> None:
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="ingest_ecmwf_data",
            metadata={"run_id": ECMWF_RUN_ID, "dataset": ECMWF_DATASET, "source": ECMWF_SOURCE},
            partition=ECMWF_PARTITION,
        )
    )


# ---------------------------------------------------------------------------
# Tests: transform_cams_data
# ---------------------------------------------------------------------------


class TestTransformCamsDataUnit:
    """Unit tests for transform_cams_data using mocked resources."""

    def _run(self, object_store=None, catalog=None, grid_store=None):
        instance = dg.DagsterInstance.ephemeral()
        _report_cams_upstream(instance)
        context = dg.build_asset_context(instance=instance, partition_key=CAMS_PARTITION)
        return transform_cams_data(
            context,
            object_store or MockObjectStore(fixture_path=str(CAMS_FIXTURE)),
            catalog or MockCatalogResource(),
            grid_store or MockGridStore(),
        )

    def test_converts_kg_m3_to_ug_m3(self):
        """CAMS raw unit is kg m-3; transform should convert to micrograms."""
        self._run()
        for grid in _grid_inserts:
            assert grid.unit == "µg/m³"

    def test_records_curated_lineage_per_message(self):
        """8 curated inserts expected (2 variables x 4 timestamps)."""
        self._run()
        assert len(_curated_inserts) == 8

    def test_variables_processed_is_dynamic(self):
        """Metadata variables_processed should reflect actual GRIB content."""
        result = self._run()
        assert set(result.metadata["variables_processed"]) == {"pm2p5", "pm10"}

    def test_fails_without_upstream(self):
        """Should raise dg.Failure when no upstream materialization exists."""
        instance = dg.DagsterInstance.ephemeral()
        context = dg.build_asset_context(instance=instance, partition_key=CAMS_PARTITION)
        with pytest.raises(dg.Failure):
            transform_cams_data(
                context,
                MockObjectStore(fixture_path=str(CAMS_FIXTURE)),
                MockCatalogResource(),
                MockGridStore(),
            )

    def test_fails_on_download_error(self):
        """Should raise dg.Failure with 'Failed to download' on download error."""
        with pytest.raises(dg.Failure, match="Failed to download"):
            self._run(object_store=MockObjectStore(fixture_path=str(CAMS_FIXTURE), should_fail=True))


# ---------------------------------------------------------------------------
# Tests: transform_ecmwf_data
# ---------------------------------------------------------------------------


class TestTransformEcmwfDataUnit:
    """Unit tests for transform_ecmwf_data using mocked resources."""

    def _run(self, object_store=None, catalog=None, grid_store=None):
        instance = dg.DagsterInstance.ephemeral()
        _report_ecmwf_upstream(instance)
        context = dg.build_asset_context(instance=instance, partition_key=ECMWF_PARTITION)
        return transform_ecmwf_data(
            context,
            object_store or MockObjectStore(fixture_path=str(ECMWF_FIXTURE)),
            catalog or MockCatalogResource(),
            grid_store or MockGridStore(),
        )

    def test_inserts_temperature_dewpoint_and_humidity(self):
        """Grid inserts should include temperature, dewpoint, and humidity."""
        self._run()
        variables = {g.variable for g in _grid_inserts}
        assert variables == {"temperature", "dewpoint", "humidity"}

    def test_temperature_converted_to_celsius(self):
        """Temperature values should be in Celsius, not Kelvin."""
        self._run()
        for grid in _grid_inserts:
            if grid.variable == "temperature":
                assert grid.unit == "°C"
                # Celsius range: roughly -65 to +55 for Earth
                assert grid.values.min() > -80
                assert grid.values.max() < 60

    def test_dewpoint_converted_to_celsius(self):
        """Dewpoint values should be in Celsius, not Kelvin."""
        self._run()
        for grid in _grid_inserts:
            if grid.variable == "dewpoint":
                assert grid.unit == "°C"
                assert grid.values.min() > -80
                assert grid.values.max() < 60

    def test_humidity_computed_as_percentage(self):
        """Humidity values should be percentage (0-~105)."""
        self._run()
        for grid in _grid_inserts:
            if grid.variable == "humidity":
                assert grid.unit == "%"
                assert grid.values.min() >= 0
                assert grid.values.max() <= 105

    def test_grid_clipped_to_europe(self):
        """All lat/lon values should be within the European bounding box, shape 169x281."""
        self._run()
        for grid in _grid_inserts:
            assert grid.values.shape == (169, 281)
            assert grid.lats.min() >= _EUROPE_LAT_MIN
            assert grid.lats.max() <= _EUROPE_LAT_MAX
            assert grid.lons.min() >= _EUROPE_LON_MIN
            assert grid.lons.max() <= _EUROPE_LON_MAX

    def test_records_curated_lineage(self):
        """6 curated inserts expected (3 variables x 2 timestamps)."""
        self._run()
        assert len(_curated_inserts) == 6

    def test_inserted_rows_metadata_matches_actual(self):
        """Metadata inserted_rows should match sum of grid row counts."""
        result = self._run()
        expected = sum(g.row_count for g in _grid_inserts)
        assert result.metadata["inserted_rows"] == expected

    def test_fails_without_upstream(self):
        """Should raise dg.Failure when no upstream materialization exists."""
        instance = dg.DagsterInstance.ephemeral()
        context = dg.build_asset_context(instance=instance, partition_key=ECMWF_PARTITION)
        with pytest.raises(dg.Failure):
            transform_ecmwf_data(
                context,
                MockObjectStore(fixture_path=str(ECMWF_FIXTURE)),
                MockCatalogResource(),
                MockGridStore(),
            )

    def test_fails_on_download_error(self):
        """Should raise dg.Failure with 'Failed to download' on download error."""
        with pytest.raises(dg.Failure, match="Failed to download"):
            self._run(object_store=MockObjectStore(fixture_path=str(ECMWF_FIXTURE), should_fail=True))


# ---------------------------------------------------------------------------
# Tests: _clip_to_europe
# ---------------------------------------------------------------------------


class TestClipToEurope:
    """Unit tests for the _clip_to_europe helper function."""

    @pytest.fixture()
    def global_grid(self):
        """Create a synthetic 0.25° global grid matching ECMWF dimensions."""
        lat_1d = np.arange(90, -90.25, -0.25)   # 721 points: 90, 89.75, ..., -90
        lon_1d = np.arange(-180, 180, 0.25)      # 1440 points: -180, -179.75, ..., 179.75
        lons, lats = np.meshgrid(lon_1d, lat_1d)
        values = lats * 100 + lons  # deterministic values based on coordinates
        return values, lats, lons

    def test_clip_output_shape(self, global_grid):
        """Clipped grid should be 169x281."""
        values, lats, lons = global_grid
        clipped_v, clipped_la, clipped_lo = _clip_to_europe(values, lats, lons)
        assert clipped_v.shape == (169, 281)
        assert clipped_la.shape == (169, 281)
        assert clipped_lo.shape == (169, 281)

    def test_clip_lat_bounds(self, global_grid):
        """All latitudes should be in [30, 72]."""
        values, lats, lons = global_grid
        _, clipped_la, _ = _clip_to_europe(values, lats, lons)
        assert clipped_la.min() == pytest.approx(30.0)
        assert clipped_la.max() == pytest.approx(72.0)

    def test_clip_lon_bounds(self, global_grid):
        """All longitudes should be in [-25, 45]."""
        values, lats, lons = global_grid
        _, _, clipped_lo = _clip_to_europe(values, lats, lons)
        assert clipped_lo.min() == pytest.approx(-25.0)
        assert clipped_lo.max() == pytest.approx(45.0)

    def test_clip_preserves_values(self, global_grid):
        """Values at known coordinates should survive clipping."""
        values, lats, lons = global_grid
        clipped_v, clipped_la, clipped_lo = _clip_to_europe(values, lats, lons)
        # Check a specific point: lat=50.0, lon=10.0 → value = 50*100 + 10 = 5010
        # Find it in the clipped arrays
        mask = (clipped_la == 50.0) & (clipped_lo == 10.0)
        assert mask.sum() == 1
        assert clipped_v[mask][0] == pytest.approx(5010.0)

    def test_clip_works_with_coarser_grid(self):
        """Should infer shape for any regular resolution, not just 0.25°."""
        lat_1d = np.arange(90, -90.5, -0.5)    # 0.5° grid: 361 points
        lon_1d = np.arange(-180, 180, 0.5)      # 720 points
        lons, lats = np.meshgrid(lon_1d, lat_1d)
        values = np.ones_like(lats)
        clipped_v, clipped_la, clipped_lo = _clip_to_europe(values, lats, lons)
        # 0.5° over [30,72] × [-25,45]: 85 lats × 141 lons
        assert clipped_v.shape == (85, 141)

    def test_clip_handles_irregular_points(self):
        """Station-like data with arbitrary coordinates returns (N, 1) shape."""
        lats = np.array([50.1, 48.3, 35.0, 10.0, 60.5])
        lons = np.array([5.2, 12.7, -10.0, 20.0, 40.0])
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # Treat as 2D (N, 1) input — same as a column of points
        lats_2d = lats.reshape(-1, 1)
        lons_2d = lons.reshape(-1, 1)
        values_2d = values.reshape(-1, 1)
        clipped_v, clipped_la, clipped_lo = _clip_to_europe(values_2d, lats_2d, lons_2d)
        # Point at lat=10 is outside Europe (< 30), 4 remaining
        assert clipped_v.shape == (4, 1)
        assert clipped_la.min() >= _EUROPE_LAT_MIN


# ---------------------------------------------------------------------------
# Tests: Magnus formula
# ---------------------------------------------------------------------------


class TestMagnusFormula:
    """Standalone test for the Magnus relative humidity formula."""

    def test_known_values(self):
        """T=20°C, Td=15°C should give RH ≈ 73%."""
        t_k = np.array([293.15])   # 20°C in Kelvin
        td_k = np.array([288.15])  # 15°C in Kelvin
        t_c = t_k - 273.15
        td_c = td_k - 273.15
        rh = 100 * np.exp(17.625 * td_c / (243.04 + td_c)) \
                 / np.exp(17.625 * t_c / (243.04 + t_c))
        assert rh[0] == pytest.approx(73.0, abs=1.0)

    def test_equal_temp_and_dewpoint_gives_100_percent(self):
        """When T == Td, air is saturated — RH must be 100%."""
        t_c = np.array([15.0])
        td_c = np.array([15.0])
        rh = 100 * np.exp(17.625 * td_c / (243.04 + td_c)) \
                 / np.exp(17.625 * t_c / (243.04 + t_c))
        assert rh[0] == pytest.approx(100.0, abs=0.01)

    def test_vectorised_over_array(self):
        """Formula must work element-wise on arrays (same as in the transform)."""
        t_c = np.array([20.0, 30.0, 0.0])
        td_c = np.array([15.0, 20.0, -5.0])
        rh = 100 * np.exp(17.625 * td_c / (243.04 + td_c)) \
                 / np.exp(17.625 * t_c / (243.04 + t_c))
        assert rh.shape == (3,)
        assert all(0 < v < 100 for v in rh)
