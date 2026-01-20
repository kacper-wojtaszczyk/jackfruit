"""
Tests for transformation assets.

Tests the transform_cams_data asset with mocked resources and data.
Focuses on unit testing the helper functions which contain the core logic.
"""
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import dagster as dg
import pytest

from pipeline_python.defs.assets import (
    CAMS_CONSTITUENT_CODES,
    _extract_message_metadata,
    _write_curated_grib,
)


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
def mock_context():
    """
    Create a mock Dagster context for testing helper functions.
    """
    context = Mock()
    context.log = Mock()
    return context


class TestExtractMessageMetadata:
    """Tests for the _extract_message_metadata helper function."""

    def test_extracts_pm25_metadata(self, mock_context):
        """Should extract metadata for PM2.5 messages."""
        mock_msg = Mock()
        mock_msg.atmosphericChemicalConstituentType.value = 40009  # PM2.5
        mock_msg.validDate.year = 2025
        mock_msg.validDate.month = 1
        mock_msg.validDate.day = 15
        mock_msg.validDate.hour = 12

        result = _extract_message_metadata(mock_msg, mock_context)

        assert result is not None
        assert result["constituent_code"] == 40009
        assert result["var_name"] == "pm2p5"
        assert result["year"] == 2025
        assert result["month"] == 1
        assert result["day"] == 15
        assert result["hour"] == 12

    def test_extracts_pm10_metadata(self, mock_context):
        """Should extract metadata for PM10 messages."""
        mock_msg = Mock()
        mock_msg.atmosphericChemicalConstituentType.value = 40008  # PM10
        mock_msg.validDate.year = 2025
        mock_msg.validDate.month = 3
        mock_msg.validDate.day = 20
        mock_msg.validDate.hour = 6

        result = _extract_message_metadata(mock_msg, mock_context)

        assert result is not None
        assert result["constituent_code"] == 40008
        assert result["var_name"] == "pm10"
        assert result["year"] == 2025
        assert result["month"] == 3
        assert result["day"] == 20
        assert result["hour"] == 6

    def test_skips_unknown_constituent_codes(self, mock_context):
        """Should return None for constituent codes not in CAMS_CONSTITUENT_CODES."""
        mock_msg = Mock()
        mock_msg.atmosphericChemicalConstituentType.value = 99999  # Unknown

        result = _extract_message_metadata(mock_msg, mock_context)

        assert result is None
        mock_context.log.debug.assert_called_once_with("Skipping constituent code: 99999")

    def test_handles_missing_constituent_type(self, mock_context):
        """Should return None if atmosphericChemicalConstituentType is missing."""
        mock_msg = Mock()
        del mock_msg.atmosphericChemicalConstituentType  # Remove the attribute

        result = _extract_message_metadata(mock_msg, mock_context)

        assert result is None
        mock_context.log.warning.assert_called_once()

    def test_handles_invalid_valid_date(self, mock_context):
        """Should return None if validDate extraction fails."""
        mock_msg = Mock()
        mock_msg.atmosphericChemicalConstituentType.value = 40009
        mock_msg.validDate = Mock()
        # Make year access raise an exception
        type(mock_msg.validDate).year = PropertyMock(side_effect=ValueError("Invalid date"))

        result = _extract_message_metadata(mock_msg, mock_context)

        assert result is None
        mock_context.log.warning.assert_called_once()


class TestWriteCuratedGrib:
    """Tests for the _write_curated_grib helper function."""

    def test_constructs_correct_curated_key(self, mock_storage, mock_context):
        """Should construct the correct curated S3 key."""
        mock_msg = Mock()
        metadata = {
            "constituent_code": 40009,
            "var_name": "pm2p5",
            "year": 2025,
            "month": 1,
            "day": 15,
            "hour": 12,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
                mock_out_file = MagicMock()
                mock_out_file.__enter__.return_value = mock_out_file
                mock_out_file.__exit__.return_value = None
                mock_grib_open.return_value = mock_out_file

                result = _write_curated_grib(mock_msg, metadata, tmpdir_path, mock_storage, mock_context)

        expected_key = "curated/cams/europe-air-quality/pm2p5/2025/01/15/12/data.grib2"
        assert result == expected_key

        # Verify storage was called
        assert len(mock_storage.uploaded_files) == 1
        assert mock_storage.uploaded_files[0]["key"] == expected_key

    def test_returns_none_on_write_failure(self, mock_storage, mock_context):
        """Should return None if GRIB write fails."""
        mock_msg = Mock()
        metadata = {
            "constituent_code": 40009,
            "var_name": "pm2p5",
            "year": 2025,
            "month": 1,
            "day": 15,
            "hour": 12,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
                mock_grib_open.side_effect = Exception("GRIB write error")

                result = _write_curated_grib(mock_msg, metadata, tmpdir_path, mock_storage, mock_context)

        assert result is None
        mock_context.log.error.assert_called_once()

    def test_returns_none_on_upload_failure(self, mock_context):
        """Should return None if S3 upload fails."""
        mock_msg = Mock()
        metadata = {
            "constituent_code": 40008,
            "var_name": "pm10",
            "year": 2025,
            "month": 3,
            "day": 20,
            "hour": 6,
        }

        # Create a mock storage that fails on upload
        mock_storage = Mock()
        mock_storage.upload_curated.side_effect = Exception("Upload failed")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            with patch("pipeline_python.defs.assets.grib2io.open") as mock_grib_open:
                mock_out_file = MagicMock()
                mock_out_file.__enter__.return_value = mock_out_file
                mock_out_file.__exit__.return_value = None
                mock_grib_open.return_value = mock_out_file

                result = _write_curated_grib(mock_msg, metadata, tmpdir_path, mock_storage, mock_context)

        assert result is None
        mock_context.log.error.assert_called_once()


class TestCamsConstituentCodes:
    """Tests for the CAMS_CONSTITUENT_CODES constant."""
    
    def test_contains_expected_codes(self):
        """CAMS_CONSTITUENT_CODES should contain the ECMWF local codes."""
        assert 40008 in CAMS_CONSTITUENT_CODES  # PM10
        assert 40009 in CAMS_CONSTITUENT_CODES  # PM2.5
    
    def test_is_a_set(self):
        """CAMS_CONSTITUENT_CODES should be a set for O(1) lookup."""
        assert isinstance(CAMS_CONSTITUENT_CODES, set)
