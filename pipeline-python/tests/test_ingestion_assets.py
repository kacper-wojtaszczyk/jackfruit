"""
Tests for ingestion assets.

Dagster testing patterns used:
- `materialize()` for integration-style tests with mocked resources
- Mock resources to avoid Docker/network dependencies
- pytest fixtures for common setup
"""
import uuid
from datetime import datetime

import dagster as dg
import pytest

from pipeline_python.defs.assets import IngestionConfig, ingest_cams_data
from pipeline_python.defs.resources import DockerIngestionClient



# Global state for tracking mock calls (used during tests)
_mock_calls = []


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
    global _mock_calls
    _mock_calls = []
    return StatefulMockIngestionClient()


class TestIngestCamsDataAsset:
    """Tests for the ingest_cams_data asset."""

    def test_asset_executes_with_default_config(self, mock_ingestion_client):
        """Asset should execute successfully with default configuration."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client},
        )

        assert result.success
        assert len(_mock_calls) == 1

        call = _mock_calls[0]
        assert call["dataset"] == "cams-europe-air-quality-forecasts-forecast"
        # Date should be today if not specified
        assert call["date"] == datetime.now().strftime("%Y-%m-%d")
        # run_id should be a valid UUID
        uuid.UUID(call["run_id"])  # Raises if invalid

    def test_asset_uses_provided_date(self, mock_ingestion_client):
        """Asset should use the date from config when provided."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client},
            run_config={
                "ops": {
                    "ingest_cams_data": {
                        "config": {
                            "date": "2025-06-15",
                            "dataset": "cams-europe-air-quality-forecasts-analysis",
                        }
                    }
                }
            },
        )

        assert result.success
        call = _mock_calls[0]
        assert call["date"] == "2025-06-15"
        assert call["dataset"] == "cams-europe-air-quality-forecasts-analysis"

    def test_asset_generates_unique_run_ids(self, mock_ingestion_client):
        """Each asset execution should generate a unique run_id."""
        # Execute twice
        dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client},
        )
        dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client},
        )

        assert len(_mock_calls) == 2
        run_id_1 = _mock_calls[0]["run_id"]
        run_id_2 = _mock_calls[1]["run_id"]
        assert run_id_1 != run_id_2

    def test_asset_returns_metadata(self, mock_ingestion_client):
        """Asset should return MaterializeResult with expected metadata."""
        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_ingestion_client},
            run_config={
                "ops": {
                    "ingest_cams_data": {
                        "config": {
                            "date": "2025-01-01",
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

    def test_asset_propagates_client_failure(self):
        """Asset should propagate failures from the ingestion client."""
        mock_client = StatefulMockIngestionClient(should_fail=True)

        result = dg.materialize(
            assets=[ingest_cams_data],
            resources={"ingestion_client": mock_client},
            raise_on_error=False,
        )

        assert not result.success


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
                    "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET", "MINIO_USE_SSL"]:
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

