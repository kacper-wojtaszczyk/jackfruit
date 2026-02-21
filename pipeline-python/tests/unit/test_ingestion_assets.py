"""
Tests for ingestion assets.

Dagster testing patterns used:
- `materialize()` for integration-style tests with mocked resources
- Mock resources to avoid Docker/network dependencies
- pytest fixtures for common setup
"""
import uuid

import dagster as dg
import pytest

from pipeline_python.defs.assets import IngestionConfig, ingest_cams_data
from pipeline_python.defs.resources import DockerIngestionClient
from pipeline_python.defs.models import RawFileRecord



# Global state for tracking mock calls (used during tests)
_mock_calls = []
_catalog_raw_inserts = []


class MockCatalogResource(dg.ConfigurableResource):
    """
    Mock catalog resource for testing.

    Tracks insert calls for verification in tests.
    """

    should_fail: bool = False

    def insert_raw_file(self, raw_file: RawFileRecord) -> None:
        """Record the insert call."""
        _catalog_raw_inserts.append(raw_file)
        if self.should_fail:
            raise Exception("Mock catalog failure")


class StatefulMockIngestionClient(dg.ConfigurableResource):
    """
    Stateful mock ingestion client for testing with call tracking.

    Since ConfigurableResource is frozen (immutable), we use module-level state
    to track calls and reset it per test.
    """

    should_fail: bool = False

    def run_ingestion(
        self,
        context: dg.AssetExecutionContext,
        *,
        dataset: str,
        date: str,
        run_id: str,
    ) -> dg.MaterializeResult:
        """Record the call and return a mock result."""
        call_info = {
            "dataset": dataset,
            "date": date,
            "run_id": run_id,
        }
        _mock_calls.append(call_info)

        if self.should_fail:
            raise dg.Failure(
                description="Mock ingestion failure",
                metadata={"error": "simulated"},
            )

        return dg.MaterializeResult(
            metadata={
                "run_id": run_id,
                "dataset": dataset,
                "date": date,
                "mock": True,
            }
        )


@pytest.fixture
def mock_ingestion_client():
    """Fixture providing a fresh mock client for each test."""
    global _mock_calls, _catalog_raw_inserts
    _mock_calls = []
    _catalog_raw_inserts = []
    return StatefulMockIngestionClient()


@pytest.fixture
def mock_catalog():
    """Fixture providing a fresh mock catalog for each test."""
    global _catalog_raw_inserts
    _catalog_raw_inserts = []
    return MockCatalogResource()


class TestIngestCamsDataAsset:
    """Tests for the ingest_cams_data asset."""

    def test_asset_executes_with_partition(self, mock_ingestion_client, mock_catalog):
        """Asset should execute successfully with a partition key."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": mock_catalog},
            partition_key="2026-01-15",
        )

        assert result.success
        assert len(_mock_calls) == 1

        call = _mock_calls[0]
        assert call["dataset"] == "cams-europe-air-quality-forecasts-forecast"
        # Date should match the partition key
        assert call["date"] == "2026-01-15"
        # run_id should be a valid UUID
        uuid.UUID(call["run_id"])  # Raises if invalid

        # Verify catalog insert was called
        assert len(_catalog_raw_inserts) == 1
        raw_record = _catalog_raw_inserts[0]
        assert raw_record.source == "ads"
        assert raw_record.dataset == "cams-europe-air-quality-forecasts-forecast"
        assert str(raw_record.date) == "2026-01-15"
        assert raw_record.s3_key == f"ads/cams-europe-air-quality-forecasts-forecast/2026-01-15/{call['run_id']}.grib"

    def test_asset_uses_custom_dataset(self, mock_ingestion_client, mock_catalog):
        """Asset should use the dataset from config when provided."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": mock_catalog},
            partition_key="2026-01-15",
            run_config={
                "ops": {
                    "ingest_cams_data": {
                        "config": {
                            "dataset": "cams-europe-air-quality-forecasts-analysis",
                        }
                    }
                }
            },
        )

        assert result.success
        call = _mock_calls[0]
        assert call["date"] == "2026-01-15"
        assert call["dataset"] == "cams-europe-air-quality-forecasts-analysis"

        # Verify catalog uses custom dataset
        assert len(_catalog_raw_inserts) == 1
        assert _catalog_raw_inserts[0].dataset == "cams-europe-air-quality-forecasts-analysis"

    def test_asset_generates_unique_run_ids(self, mock_ingestion_client, mock_catalog):
        """Each asset execution should generate a unique run_id."""
        global _mock_calls, _catalog_raw_inserts
        _mock_calls = []
        _catalog_raw_inserts = []

        # Execute twice with same partition
        dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": mock_catalog},
            partition_key="2026-01-15",
        )
        dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": mock_catalog},
            partition_key="2026-01-16",
        )

        assert len(_mock_calls) == 2
        run_id_1 = _mock_calls[0]["run_id"]
        run_id_2 = _mock_calls[1]["run_id"]
        assert run_id_1 != run_id_2

    def test_asset_returns_metadata(self, mock_ingestion_client, mock_catalog):
        """Asset should return MaterializeResult with expected metadata."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": mock_catalog},
            partition_key="2026-01-01",
            run_config={
                "ops": {
                    "ingest_cams_data": {
                        "config": {
                            "dataset": "test-dataset",
                        }
                    }
                }
            },
        )

        assert result.success

        # Get materialization events
        materializations = result.get_asset_materialization_events()
        assert len(materializations) == 1

    def test_asset_propagates_client_failure(self, mock_catalog):
        """Asset should propagate failures from the ingestion client."""
        mock_client = StatefulMockIngestionClient(should_fail=True)

        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_client, "catalog": mock_catalog},
            partition_key="2026-01-15",
            raise_on_error=False,
        )

        assert not result.success

    def test_asset_continues_on_catalog_failure(self, mock_ingestion_client):
        """Asset should succeed even if catalog insert fails (log warning only)."""
        failing_catalog = MockCatalogResource(should_fail=True)

        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client, "catalog": failing_catalog},
            partition_key="2026-01-15",
        )

        # Asset should still succeed - catalog failure is non-fatal
        assert result.success
        assert len(_mock_calls) == 1


class TestDockerIngestionClientUnit:
    """Unit tests for DockerIngestionClient (without Docker)."""

    def test_get_forwarded_env_collects_vars(self, monkeypatch):
        """Should collect environment variables from os.environ."""
        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "testkey")
        monkeypatch.setenv("ADS_API_KEY", "secret123")

        client = DockerIngestionClient()
        env = client._get_forwarded_env()

        assert env["MINIO_ENDPOINT"] == "localhost:9000"
        assert env["MINIO_ACCESS_KEY"] == "testkey"
        assert env["ADS_API_KEY"] == "secret123"

    def test_get_forwarded_env_skips_missing(self, monkeypatch):
        """Should skip environment variables that aren't set."""
        # Clear all ingestion env vars
        for var in ["ADS_BASE_URL", "ADS_API_KEY", "MINIO_ENDPOINT",
                    "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_RAW_BUCKET", "MINIO_USE_SSL"]:
            monkeypatch.delenv(var, raising=False)

        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")

        client = DockerIngestionClient()
        env = client._get_forwarded_env()

        assert env == {"MINIO_ENDPOINT": "localhost:9000"}

    def test_client_config_defaults(self):
        """Should have sensible default configuration."""
        client = DockerIngestionClient()

        assert client.image == "jackfruit-ingestion:latest"
        assert client.network == "jackfruit_jackfruit"

    def test_client_config_override(self):
        """Should accept custom configuration."""
        client = DockerIngestionClient(
            image="my-image:v2",
            network="custom_network",
        )

        assert client.image == "my-image:v2"
        assert client.network == "custom_network"


class TestIngestionConfig:
    """Tests for IngestionConfig schema."""

    def test_default_values(self):
        """Config should have expected defaults."""
        config = IngestionConfig()

        assert config.date is None
        assert config.dataset == "cams-europe-air-quality-forecasts-forecast"

    def test_custom_values(self):
        """Config should accept custom values."""
        config = IngestionConfig(
            date="2025-12-25",
            dataset="custom-dataset",
        )

        assert config.date == "2025-12-25"
        assert config.dataset == "custom-dataset"

