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
