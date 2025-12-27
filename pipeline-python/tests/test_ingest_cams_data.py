"""Tests for the ingest_cams_data Dagster asset."""

import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dagster._core.definitions.decorators.op_decorator import DecoratedOpFunction

from pipeline_python.defs.assets import IngestionConfig, ingest_cams_data


# Helper to access the original compute function behind the asset
def _invoke_asset_fn(context: dg.AssetExecutionContext, config: IngestionConfig):
    # dagster stores the python function on the op.compute_fn.decorated_fn
    compute_fn = ingest_cams_data.op.compute_fn
    assert isinstance(compute_fn, DecoratedOpFunction)
    return compute_fn.decorated_fn(context, config)


class TestIngestionConfig:
    """Tests for the IngestionConfig class."""

    def test_default_config(self):
        """Test default configuration values."""
        config = IngestionConfig()
        assert config.date is None
        assert config.dataset == "cams-europe-air-quality-forecasts-analysis"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = IngestionConfig(
            date="2025-12-27",
            dataset="cams-europe-air-quality-forecasts-forecast"
        )
        assert config.date == "2025-12-27"
        assert config.dataset == "cams-europe-air-quality-forecasts-forecast"


class TestIngestCamsDataAsset:
    """Tests for the ingest_cams_data asset."""

    @pytest.fixture
    def mock_context(self):
        """Create a Dagster asset execution context for testing."""
        return dg.build_asset_context()

    @pytest.fixture
    def mock_subprocess_success(self, mocker):
        """Mock subprocess.run to return successful execution."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Ingestion completed successfully"
        mock_result.stderr = ""
        return mocker.patch("pipeline_python.defs.assets.subprocess.run", return_value=mock_result)

    @pytest.fixture
    def mock_subprocess_failure(self, mocker):
        """Mock subprocess.run to return failed execution."""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error: Failed to fetch data"
        return mocker.patch("pipeline_python.defs.assets.subprocess.run", return_value=mock_result)

    def test_asset_success_with_defaults(self, mock_context, mock_subprocess_success):
        """Test successful asset execution with default config."""
        config = IngestionConfig()

        result = _invoke_asset_fn(mock_context, config)

        # Verify subprocess was called correctly
        assert mock_subprocess_success.call_count == 1
        call_args = mock_subprocess_success.call_args

        # Check command structure
        cmd = call_args[0][0]
        assert cmd[0:2] == ["docker", "compose"]
        assert "run" in cmd
        assert "--rm" in cmd
        assert "--env-file" in cmd
        assert ".env" in cmd
        assert "ingestion" in cmd
        assert "--date" in cmd
        assert "--dataset" in cmd
        assert "--run-id" in cmd

        # Verify dataset is in command
        dataset_idx = cmd.index("--dataset")
        assert cmd[dataset_idx + 1] == "cams-europe-air-quality-forecasts-analysis"

        # Verify date format (should be today's date)
        date_idx = cmd.index("--date")
        date_value = cmd[date_idx + 1]
        assert len(date_value) == 10  # YYYY-MM-DD format
        assert date_value == datetime.now().strftime("%Y-%m-%d")

        # Verify run-id is UUIDv7
        run_id_idx = cmd.index("--run-id")
        run_id_value = cmd[run_id_idx + 1]
        parsed_uuid = uuid.UUID(run_id_value)
        assert parsed_uuid.version == 7

        # Verify cwd was set to project root
        assert "cwd" in call_args[1]

        # Verify result
        assert isinstance(result, dg.MaterializeResult)
        assert "run_id" in result.metadata
        assert "dataset" in result.metadata
        assert "date" in result.metadata
        assert "exit_code" in result.metadata
        assert result.metadata["exit_code"] == 0
        assert result.metadata["dataset"] == "cams-europe-air-quality-forecasts-analysis"

        # Verify context has log (can't easily test log calls with real context)

    def test_asset_success_with_custom_config(self, mock_context, mock_subprocess_success):
        """Test successful asset execution with custom config."""
        config = IngestionConfig(
            date="2025-12-25",
            dataset="cams-europe-air-quality-forecasts-forecast"
        )

        result = _invoke_asset_fn(mock_context, config)

        # Verify custom config was used
        call_args = mock_subprocess_success.call_args
        cmd = call_args[0][0]

        date_idx = cmd.index("--date")
        assert cmd[date_idx + 1] == "2025-12-25"

        dataset_idx = cmd.index("--dataset")
        assert cmd[dataset_idx + 1] == "cams-europe-air-quality-forecasts-forecast"

        # Verify result metadata
        assert result.metadata["date"] == "2025-12-25"
        assert result.metadata["dataset"] == "cams-europe-air-quality-forecasts-forecast"

    def test_asset_failure_with_non_zero_exit(self, mock_context, mock_subprocess_failure):
        """Test asset handles container failure correctly."""
        config = IngestionConfig()

        with pytest.raises(dg.Failure) as exc_info:
            _invoke_asset_fn(mock_context, config)

        failure = exc_info.value
        assert "exit code 1" in failure.description
        assert failure.metadata["exit_code"].value == 1
        assert "run_id" in failure.metadata
        assert "dataset" in failure.metadata
        assert "date" in failure.metadata
        assert "stderr" in failure.metadata
        assert "Failed to fetch data" in failure.metadata["stderr"].text

    def test_uuid7_generation_is_unique(self, mock_context, mock_subprocess_success):
        """Test that each invocation generates a unique UUIDv7."""
        config = IngestionConfig()

        # Run asset twice
        result1 = _invoke_asset_fn(mock_context, config)
        result2 = _invoke_asset_fn(mock_context, config)

        run_id_1 = result1.metadata["run_id"]
        run_id_2 = result2.metadata["run_id"]

        # Verify both are valid UUIDv7s
        uuid1 = uuid.UUID(run_id_1)
        uuid2 = uuid.UUID(run_id_2)
        assert uuid1.version == 7
        assert uuid2.version == 7

        # Verify they are different
        assert run_id_1 != run_id_2

    def test_project_root_calculation(self, mock_context, mock_subprocess_success):
        """Test that project root path is calculated correctly."""
        config = IngestionConfig()

        _invoke_asset_fn(mock_context, config)

        call_args = mock_subprocess_success.call_args
        cwd = call_args[1]["cwd"]

        # Should be 5 levels up from assets.py
        assert isinstance(cwd, Path)
        assert cwd.name == "jackfruit" or "GolandProjects" in str(cwd.parent)

    def test_subprocess_capture_configuration(self, mock_context, mock_subprocess_success):
        """Test that subprocess is configured to capture output."""
        config = IngestionConfig()

        _invoke_asset_fn(mock_context, config)

        call_args = mock_subprocess_success.call_args
        kwargs = call_args[1]

        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert "cwd" in kwargs

    def test_logs_stdout_when_present(self, mock_context, mock_subprocess_success):
        """Test that stdout handling works when present."""
        config = IngestionConfig()
        mock_subprocess_success.return_value.stdout = "Test output line 1\nTest output line 2"

        # Should not raise
        result = _invoke_asset_fn(mock_context, config)
        assert result is not None

    def test_logs_stderr_as_warning(self, mock_context, mock_subprocess_success):
        """Test that stderr handling works when present (but still succeeds)."""
        config = IngestionConfig()
        mock_subprocess_success.return_value.stderr = "Warning: something happened"
        mock_subprocess_success.return_value.returncode = 0  # Still success

        # Should not raise
        result = _invoke_asset_fn(mock_context, config)
        assert result is not None
        assert result.metadata["exit_code"] == 0


    def test_stderr_truncated_in_failure_metadata(self, mock_context, mock_subprocess_failure):
        """Test that stderr is truncated to last 1000 chars in failure metadata."""
        config = IngestionConfig()
        long_stderr = "x" * 2000  # 2000 character error message
        mock_subprocess_failure.return_value.stderr = long_stderr

        with pytest.raises(dg.Failure) as exc_info:
            _invoke_asset_fn(mock_context, config)

        failure = exc_info.value
        stderr_text = failure.metadata["stderr"].text
        assert len(stderr_text) == 1000
        assert stderr_text == long_stderr[-1000:]


class TestAssetIntegration:
    """Integration tests for the asset within Dagster framework."""

    def test_asset_can_be_loaded_in_definitions(self):
        """Test that the asset can be loaded by Dagster definitions."""
        from pipeline_python.definitions import defs

        d = defs()
        asset_specs = list(d.resolve_all_asset_specs())

        # Find our asset
        ingest_asset = next(
            (spec for spec in asset_specs if spec.key.path[-1] == "ingest_cams_data"),
            None
        )

        assert ingest_asset is not None
        assert "docker compose" in ingest_asset.description

    def test_asset_has_correct_metadata(self):
        """Test that the asset has expected metadata structure."""
        from pipeline_python.definitions import defs

        d = defs()
        asset_specs = list(d.resolve_all_asset_specs())

        ingest_asset = next(
            (spec for spec in asset_specs if spec.key.path[-1] == "ingest_cams_data"),
            None
        )

        assert ingest_asset is not None
        assert ingest_asset.group_name == "default"
        assert not ingest_asset.skippable

