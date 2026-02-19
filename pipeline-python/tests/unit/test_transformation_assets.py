"""
Tests for transformation assets.

Tests the transform_cams_data asset with mocked resources and data.
Focuses on unit testing the helper functions which contain the core logic.
"""
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest

from pipeline_python.defs.assets import (
    _extract_message_metadata,
)
from pipeline_python.defs.models import RawFileRecord, CuratedDataRecord


class MockObjectStorageResource:
    """
    Mock storage resource for testing.

    Stores uploaded files in memory for verification.
    """

    def __init__(self):
        self.uploaded_files = []
        self.raw_file_path = ""

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


class MockCatalogResource:
    """
    Mock catalog resource for testing.

    Tracks insert calls using instance state instead of module globals.
    """

    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.inserted_curated_files = []

    def insert_raw_file(self, raw_file: RawFileRecord) -> None:
        """Raise if configured to fail; otherwise no-op."""
        if self.should_fail:
            raise Exception("Mock catalog failure")

    def insert_curated_file(self, curated_file: CuratedDataRecord) -> None:
        """Record the insert call in instance list."""
        if self.should_fail:
            raise Exception("Mock catalog failure")
        self.inserted_curated_files.append(curated_file)


@pytest.fixture
def mock_storage():
    """Provide a mock storage resource."""
    return MockObjectStorageResource()


@pytest.fixture
def mock_catalog():
    """Provide a mock catalog resource with clean state."""
    return MockCatalogResource()


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
        """Should return None for constituent codes not in configured codes."""
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


