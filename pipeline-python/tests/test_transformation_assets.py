"""
Tests for transformation assets.

Tests the transform_cams_data asset with mocked resources and data.
"""
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import dagster as dg
import grib2io
import pytest

from pipeline_python.defs.assets import CAMS_CONSTITUENT_CODES, transform_cams_data
from pipeline_python.defs.resources import ObjectStorageResource


class MockObjectStorageResource(dg.ConfigurableResource):
    """
    Mock storage resource for testing.
    
    Stores uploaded files in memory for verification.
    """
    
    uploaded_files: list = []
    raw_file_path: str = ""
    
    def download_raw(self, key: str, local_path: Path) -> None:
        """Mock download - copy from test fixture if available."""
        if self.raw_file_path and Path(self.raw_file_path).exists():
            import shutil
            shutil.copy(self.raw_file_path, local_path)
        else:
            # Create an empty file if no fixture provided
            local_path.touch()
    
    def upload_curated(self, local_path: Path, key: str) -> None:
        """Mock upload - track uploaded files."""
        self.uploaded_files.append({
            "key": key,
            "path": str(local_path),
            "exists": local_path.exists(),
        })


@pytest.fixture
def mock_storage():
    """Provide a mock storage resource."""
    return MockObjectStorageResource()


@pytest.fixture
def mock_instance_with_metadata():
    """
    Create a mock Dagster instance with materialization metadata.
    
    Returns a mock that simulates the upstream ingest_cams_data materialization.
    """
    mock_instance = Mock()
    
    # Create mock materialization event
    mock_event = Mock()
    mock_event.asset_materialization.metadata = {
        "run_id": Mock(value="test-run-id-123"),
        "dataset": Mock(value="cams-europe-air-quality-forecasts-forecast"),
    }
    
    mock_instance.get_latest_materialization_event.return_value = mock_event
    
    return mock_instance


class TestTransformCamsDataAsset:
    """Tests for the transform_cams_data asset."""
    
    def test_asset_fails_without_upstream_materialization(self, mock_storage):
        """Asset should fail if upstream ingestion hasn't run."""
        # Create a mock instance that returns None for materialization
        mock_instance = Mock()
        mock_instance.get_latest_materialization_event.return_value = None
        
        # We need to test this at the asset function level since materialize()
        # doesn't give us fine-grained control over instance
        with pytest.raises(dg.Failure, match="No materialization found"):
            # Create a mock context
            context = Mock(spec=dg.AssetExecutionContext)
            context.partition_key = "2025-01-15"
            context.instance = mock_instance
            context.log = Mock()
            
            # Call the asset function directly
            transform_cams_data(context, mock_storage)
    
    def test_asset_reads_upstream_metadata(self, mock_storage, mock_instance_with_metadata):
        """Asset should read run_id and dataset from upstream materialization."""
        context = Mock(spec=dg.AssetExecutionContext)
        context.partition_key = "2025-01-15"
        context.instance = mock_instance_with_metadata
        context.log = Mock()
        
        # Mock grib2io.open to avoid needing a real GRIB file
        with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
            mock_grib_file = MagicMock()
            mock_grib_file.__enter__.return_value = mock_grib_file
            mock_grib_file.__exit__.return_value = None
            mock_grib_file.messages = 0
            mock_grib_file.__iter__.return_value = iter([])
            mock_grib_open.return_value = mock_grib_file
            
            result = transform_cams_data(context, mock_storage)
        
        # Verify it attempted to download with the correct key
        context.log.info.assert_any_call(
            "Processing raw file: ads/cams-europe-air-quality-forecasts-forecast/2025-01-15/test-run-id-123.grib"
        )
        
        assert result.metadata["run_id"] == "test-run-id-123"
        assert result.metadata["date"] == "2025-01-15"
    
    def test_asset_skips_non_cams_constituents(self, mock_storage, mock_instance_with_metadata):
        """Asset should skip constituent codes not in CAMS_CONSTITUENT_CODES."""
        context = Mock(spec=dg.AssetExecutionContext)
        context.partition_key = "2025-01-15"
        context.instance = mock_instance_with_metadata
        context.log = Mock()
        
        # Create mock GRIB messages with different constituent codes
        mock_msg_pm25 = Mock()
        mock_msg_pm25.atmosphericChemicalConstituentType.value = 40009  # PM2.5 (in CAMS_CONSTITUENT_CODES)
        mock_msg_pm25.validDate.year = 2025
        mock_msg_pm25.validDate.month = 1
        mock_msg_pm25.validDate.day = 15
        mock_msg_pm25.validDate.hour = 12
        
        mock_msg_unknown = Mock()
        mock_msg_unknown.atmosphericChemicalConstituentType.value = 99999  # Unknown (not in CAMS_CONSTITUENT_CODES)
        
        with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
            mock_grib_file = MagicMock()
            mock_grib_file.__enter__.return_value = mock_grib_file
            mock_grib_file.__exit__.return_value = None
            mock_grib_file.messages = 2
            mock_grib_file.__iter__.return_value = iter([mock_msg_unknown, mock_msg_pm25])
            mock_grib_open.return_value = mock_grib_file
            
            # Mock the grib2io.open for writing output files
            with patch("pipeline_python.defs.assets.grib2io.open", side_effect=[
                mock_grib_open.return_value,  # First call for reading
                MagicMock()  # Second call for writing
            ]):
                result = transform_cams_data(context, mock_storage)
        
        # Should have skipped the unknown constituent
        context.log.debug.assert_called_with("Skipping constituent code: 99999")
        
        # Should have processed only PM2.5
        assert result.metadata["files_written"] == 1
    
    def test_asset_creates_correct_curated_paths(self, mock_storage, mock_instance_with_metadata):
        """Asset should create curated paths following the expected pattern."""
        context = Mock(spec=dg.AssetExecutionContext)
        context.partition_key = "2025-01-15"
        context.instance = mock_instance_with_metadata
        context.log = Mock()
        
        # Create mock PM10 message
        mock_msg = Mock()
        mock_msg.atmosphericChemicalConstituentType.value = 40008  # PM10
        mock_msg.validDate.year = 2025
        mock_msg.validDate.month = 1
        mock_msg.validDate.day = 15
        mock_msg.validDate.hour = 14
        
        with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
            # Mock reading the input file
            mock_read_file = MagicMock()
            mock_read_file.__enter__.return_value = mock_read_file
            mock_read_file.__exit__.return_value = None
            mock_read_file.messages = 1
            mock_read_file.__iter__.return_value = iter([mock_msg])
            
            # Mock writing the output file
            mock_write_file = MagicMock()
            mock_write_file.__enter__.return_value = mock_write_file
            mock_write_file.__exit__.return_value = None
            
            mock_grib_open.side_effect = [mock_read_file, mock_write_file]
            
            result = transform_cams_data(context, mock_storage)
        
        # Verify the curated path
        expected_key = "curated/cams/europe-air-quality/pm10/2025/01/15/14/data.grib2"
        assert len(mock_storage.uploaded_files) == 1
        assert mock_storage.uploaded_files[0]["key"] == expected_key
        
        # Verify metadata
        assert result.metadata["files_written"] == 1
        assert "pm10" in result.metadata["variables_processed"]
    
    def test_asset_handles_multiple_variables_and_timestamps(self, mock_storage, mock_instance_with_metadata):
        """Asset should process multiple variables and timestamps correctly."""
        context = Mock(spec=dg.AssetExecutionContext)
        context.partition_key = "2025-01-15"
        context.instance = mock_instance_with_metadata
        context.log = Mock()
        
        # Create multiple mock messages
        messages = []
        
        # PM2.5 at hour 12
        msg1 = Mock()
        msg1.atmosphericChemicalConstituentType.value = 40009  # PM2.5
        msg1.validDate.year = 2025
        msg1.validDate.month = 1
        msg1.validDate.day = 15
        msg1.validDate.hour = 12
        messages.append(msg1)
        
        # PM2.5 at hour 13
        msg2 = Mock()
        msg2.atmosphericChemicalConstituentType.value = 40009  # PM2.5
        msg2.validDate.year = 2025
        msg2.validDate.month = 1
        msg2.validDate.day = 15
        msg2.validDate.hour = 13
        messages.append(msg2)
        
        # PM10 at hour 12
        msg3 = Mock()
        msg3.atmosphericChemicalConstituentType.value = 40008  # PM10
        msg3.validDate.year = 2025
        msg3.validDate.month = 1
        msg3.validDate.day = 15
        msg3.validDate.hour = 12
        messages.append(msg3)
        
        with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
            # Mock reading
            mock_read_file = MagicMock()
            mock_read_file.__enter__.return_value = mock_read_file
            mock_read_file.__exit__.return_value = None
            mock_read_file.messages = 3
            mock_read_file.__iter__.return_value = iter(messages)
            
            # Mock writing (3 times)
            mock_write_files = [MagicMock() for _ in range(3)]
            for mock_write in mock_write_files:
                mock_write.__enter__.return_value = mock_write
                mock_write.__exit__.return_value = None
            
            mock_grib_open.side_effect = [mock_read_file] + mock_write_files
            
            result = transform_cams_data(context, mock_storage)
        
        # Should have created 3 files
        assert result.metadata["files_written"] == 3
        assert len(mock_storage.uploaded_files) == 3
        
        # Should have 2 variables
        assert set(result.metadata["variables_processed"]) == {"pm2p5", "pm10"}
        
        # Verify unique paths
        keys = [f["key"] for f in mock_storage.uploaded_files]
        assert len(keys) == len(set(keys))  # All unique


class TestCamsConstituentCodes:
    """Tests for the CAMS_CONSTITUENT_CODES constant."""
    
    def test_contains_expected_codes(self):
        """CAMS_CONSTITUENT_CODES should contain the ECMWF local codes."""
        assert 40008 in CAMS_CONSTITUENT_CODES  # PM10
        assert 40009 in CAMS_CONSTITUENT_CODES  # PM2.5
    
    def test_is_a_set(self):
        """CAMS_CONSTITUENT_CODES should be a set for O(1) lookup."""
        assert isinstance(CAMS_CONSTITUENT_CODES, set)
