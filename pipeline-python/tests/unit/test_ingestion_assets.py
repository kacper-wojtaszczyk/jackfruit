"""
Tests for ingestion assets.

Dagster testing patterns used:
- `materialize()` for integration-style tests with mocked resources
- Mock resources to avoid network/API/S3 dependencies
- Module-level state for call tracking (ConfigurableResource is frozen)
"""
import re
import uuid
from datetime import date

import dagster as dg
import pytest

from pipeline_python.defs.assets import (
    CamsForecastConfig,
    _AIR_QUALITY_FORECAST,
    _WEATHER_FORECAST,
    ingest_cams_data,
    ingest_ecmwf_data,
)
from pipeline_python.defs.models import RawFileRecord


# ---------------------------------------------------------------------------
# Module-level state for mock tracking (ConfigurableResource is frozen)
# ---------------------------------------------------------------------------

_mock_cds_calls: list[dict] = []
_mock_ecmwf_calls: list[dict] = []
_mock_uploads: list[dict] = []
_catalog_raw_inserts: list[RawFileRecord] = []


# ---------------------------------------------------------------------------
# Mock resources
# ---------------------------------------------------------------------------


class MockEcmwfClient(dg.ConfigurableResource):
    """Mock ECMWF Open Data client that creates an empty file instead of downloading."""

    should_fail: bool = False

    def retrieve_forecast(self, forecast_date, variables, target):
        _mock_ecmwf_calls.append({
            "forecast_date": forecast_date,
            "variables": variables,
        })
        if self.should_fail:
            raise Exception("Mock ECMWF API failure")
        target.touch()


class MockCdsClient(dg.ConfigurableResource):
    """Mock CDS API client that creates an empty file instead of downloading."""

    should_fail: bool = False

    def retrieve_forecast(self, forecast_date, variables, target, max_leadtime_hours=48):
        _mock_cds_calls.append({
            "forecast_date": forecast_date,
            "variables": variables,
            "max_leadtime_hours": max_leadtime_hours,
        })
        if self.should_fail:
            raise Exception("Mock CDS API failure")
        target.touch()


class MockObjectStore(dg.ConfigurableResource):
    """Mock object store that records upload calls."""

    should_fail: bool = False

    def upload_raw(self, key: str, local_path) -> None:
        _mock_uploads.append({
            "key": key,
            "local_path": local_path,
        })
        if self.should_fail:
            raise IOError("Mock upload failure")


class MockCatalogResource(dg.ConfigurableResource):
    """Mock catalog resource that records insert calls."""

    should_fail: bool = False

    def insert_raw_file(self, raw_file: RawFileRecord) -> None:
        _catalog_raw_inserts.append(raw_file)
        if self.should_fail:
            raise Exception("Mock catalog failure")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_mock_state():
    """Reset all module-level mock state before each test."""
    global _mock_cds_calls, _mock_ecmwf_calls, _mock_uploads, _catalog_raw_inserts
    _mock_cds_calls = []
    _mock_ecmwf_calls = []
    _mock_uploads = []
    _catalog_raw_inserts = []


def _make_resources(*, cds_client=None, object_store=None, catalog=None):
    """Build resource dict with defaults for all three mocks."""
    return {
        "cds_client": cds_client or MockCdsClient(),
        "object_store": object_store or MockObjectStore(),
        "catalog": catalog or MockCatalogResource(),
    }


def _make_ecmwf_resources(*, ecmwf_client=None, object_store=None, catalog=None):
    """Build resource dict with defaults for ECMWF asset mocks."""
    return {
        "ecmwf_client": ecmwf_client or MockEcmwfClient(),
        "object_store": object_store or MockObjectStore(),
        "catalog": catalog or MockCatalogResource(),
    }


# ---------------------------------------------------------------------------
# Tests: IngestCamsDataAsset
# ---------------------------------------------------------------------------


class TestIngestCamsDataAsset:
    """Tests for the ingest_cams_data asset."""

    def test_calls_cds_with_domain_args(self):
        """Core happy path: CDS called with correct args, upload key + catalog record are right."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(),
            partition_key="2026-01-15",
        )

        assert result.success

        # CDS client called with correct domain args
        assert len(_mock_cds_calls) == 1
        call = _mock_cds_calls[0]
        assert call["forecast_date"] == date(2026, 1, 15)
        assert call["variables"] == ["pm2p5", "pm10"]
        assert call["max_leadtime_hours"] == 48

        # Upload key matches expected pattern
        assert len(_mock_uploads) == 1
        key = _mock_uploads[0]["key"]
        assert re.match(
            r"ads/cams-europe-air-quality-forecast/2026-01-15/[0-9a-f-]+\.grib$",
            key,
        )

        # Catalog insert with correct fields
        assert len(_catalog_raw_inserts) == 1
        record = _catalog_raw_inserts[0]
        assert record.source == "ads"
        assert record.dataset == _AIR_QUALITY_FORECAST

    def test_forwards_horizon_from_config(self):
        """Config horizon_hours plumbed through to CDS client."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(),
            partition_key="2026-01-15",
            run_config={
                "ops": {
                    "ingest_cams_data": {
                        "config": {"horizon_hours": 24}
                    }
                }
            },
        )

        assert result.success
        assert _mock_cds_calls[0]["max_leadtime_hours"] == 24

    def test_metadata_contains_keys_for_transform(self):
        """Contract: transform_cams_data reads run_id, dataset, date, source from upstream metadata."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(),
            partition_key="2026-01-15",
        )

        events = result.get_asset_materialization_events()
        assert len(events) == 1
        metadata = events[0].step_materialization_data.materialization.metadata

        for key in ("run_id", "dataset", "date", "source"):
            assert key in metadata, f"Missing metadata key: {key}"

        assert metadata["dataset"].value == _AIR_QUALITY_FORECAST
        uuid.UUID(metadata["run_id"].value)  # raises if not a valid UUID

    def test_generates_unique_run_ids(self):
        """Each materialization must produce a distinct run_id (idempotency safety)."""
        dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(),
            partition_key="2026-01-15",
        )
        dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(),
            partition_key="2026-01-16",
        )

        assert len(_mock_uploads) == 2
        id1 = _mock_uploads[0]["key"].split("/")[-1].replace(".grib", "")
        id2 = _mock_uploads[1]["key"].split("/")[-1].replace(".grib", "")
        assert id1 != id2

    def test_fails_on_catalog_failure(self):
        """Catalog insert is fatal — asset must fail (licensing requires lineage)."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(catalog=MockCatalogResource(should_fail=True)),
            partition_key="2026-01-15",
            raise_on_error=False,
        )

        assert not result.success

    def test_propagates_cds_failure(self):
        """CDS API failure is fatal — asset must fail."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources=_make_resources(cds_client=MockCdsClient(should_fail=True)),
            partition_key="2026-01-15",
            raise_on_error=False,
        )

        assert not result.success


# ---------------------------------------------------------------------------
# Tests: CamsForecastConfig
# ---------------------------------------------------------------------------


class TestCamsForecastConfig:
    """Tests for CamsForecastConfig defaults and overrides."""

    def test_default_horizon(self):
        assert CamsForecastConfig().horizon_hours == 48

    def test_custom_horizon(self):
        assert CamsForecastConfig(horizon_hours=24).horizon_hours == 24


# ---------------------------------------------------------------------------
# Tests: _AIR_QUALITY_FORECAST constant
# ---------------------------------------------------------------------------


class TestAirQualityForecastConstant:
    """Guard against accidental rename breaking S3 key paths."""

    def test_dataset_constant(self):
        assert _AIR_QUALITY_FORECAST == "cams-europe-air-quality-forecast"


# ---------------------------------------------------------------------------
# Tests: IngestEcmwfDataAsset
# ---------------------------------------------------------------------------


class TestIngestEcmwfDataAsset:
    """Tests for the ingest_ecmwf_data asset."""

    def test_calls_ecmwf_with_domain_args(self):
        """ECMWF client called with correct date and variable names."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-15",
        )

        assert result.success
        assert len(_mock_ecmwf_calls) == 1
        call = _mock_ecmwf_calls[0]
        assert call["forecast_date"] == date(2026, 1, 15)
        assert call["variables"] == ["temperature", "dewpoint"]

    def test_uploads_to_correct_s3_path(self):
        """Upload key must match the expected pattern for the future transform asset."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-15",
        )

        assert result.success
        assert len(_mock_uploads) == 1
        key = _mock_uploads[0]["key"]
        assert re.match(
            r"ecmwf/ifs-weather-forecast/2026-01-15/[0-9a-f-]+\.grib$",
            key,
        )

    def test_records_catalog_entry(self):
        """Catalog insert must record correct source and dataset for lineage."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-15",
        )

        assert result.success
        assert len(_catalog_raw_inserts) == 1
        record = _catalog_raw_inserts[0]
        assert record.source == "ecmwf"
        assert record.dataset == _WEATHER_FORECAST

    def test_metadata_contains_keys_for_transform(self):
        """Contract: future transform_ecmwf_data will read these keys from upstream metadata."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-15",
        )

        events = result.get_asset_materialization_events()
        assert len(events) == 1
        metadata = events[0].step_materialization_data.materialization.metadata

        for key in ("run_id", "source", "dataset", "date", "s3_key"):
            assert key in metadata, f"Missing metadata key: {key}"

        uuid.UUID(metadata["run_id"].value)  # raises if not a valid UUID

    def test_generates_unique_run_ids(self):
        """Each materialization must produce a distinct run_id (idempotency safety)."""
        dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-15",
        )
        dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(),
            partition_key="2026-01-16",
        )

        assert len(_mock_uploads) == 2
        id1 = _mock_uploads[0]["key"].split("/")[-1].replace(".grib", "")
        id2 = _mock_uploads[1]["key"].split("/")[-1].replace(".grib", "")
        assert id1 != id2

    def test_fails_on_catalog_failure(self):
        """Catalog insert is fatal — asset must fail (licensing requires lineage)."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(catalog=MockCatalogResource(should_fail=True)),
            partition_key="2026-01-15",
            raise_on_error=False,
        )

        assert not result.success

    def test_propagates_ecmwf_client_failure(self):
        """ECMWF client failure is fatal — asset must fail."""
        result = dg.materialize(
            assets=[ingest_ecmwf_data],
            resources=_make_ecmwf_resources(ecmwf_client=MockEcmwfClient(should_fail=True)),
            partition_key="2026-01-15",
            raise_on_error=False,
        )

        assert not result.success


# ---------------------------------------------------------------------------
# Tests: _WEATHER_FORECAST constant
# ---------------------------------------------------------------------------


class TestWeatherForecastConstant:
    """Guard against accidental rename breaking S3 key paths."""

    def test_dataset_constant(self):
        assert _WEATHER_FORECAST == "ifs-weather-forecast"
