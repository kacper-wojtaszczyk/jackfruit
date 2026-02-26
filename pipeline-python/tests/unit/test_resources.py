"""
Tests for storage resources.

Tests the ObjectStorageResource for S3/MinIO interactions.
"""
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import uuid
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from pipeline_python.defs.resources import ObjectStorageResource, PostgresCatalogResource
from pipeline_python.defs.models import CuratedDataRecord, RawFileRecord


@pytest.fixture
def mock_s3_client():
    """Provide a mock boto3 S3 client."""
    return Mock()


@pytest.fixture
def storage_resource():
    """Provide an ObjectStorageResource for testing."""
    return ObjectStorageResource(
        endpoint_url="http://localhost:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        raw_bucket="test-raw",
        use_ssl=False,
    )


@pytest.fixture
def psycopg_mocks():
    """Provide psycopg connection and cursor mocks that support context managers."""
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value = cursor
    conn.commit = MagicMock()  # Add commit method
    return {"connect": MagicMock(return_value=conn), "conn": conn, "cursor": cursor}


class TestObjectStorageResourceDownloadRaw:
    """Tests for download_raw method."""

    def test_downloads_file_successfully(self, storage_resource, mock_s3_client):
        """Should download file from raw bucket."""
        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "downloaded.grib"

                storage_resource.download_raw("ads/dataset/2025-01-01/file.grib", local_path)

                mock_s3_client.download_file.assert_called_once_with(
                    "test-raw",
                    "ads/dataset/2025-01-01/file.grib",
                    str(local_path),
                )

    def test_creates_parent_directories(self, storage_resource, mock_s3_client):
        """Should create parent directories if they don't exist."""
        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "nested" / "dir" / "downloaded.grib"

                storage_resource.download_raw("ads/dataset/2025-01-01/file.grib", local_path)

                assert local_path.parent.exists()
                assert local_path.parent.is_dir()

    def test_raises_error_for_empty_key(self, storage_resource):
        """Should raise ValueError for empty key."""
        with pytest.raises(ValueError, match="cannot be empty"):
            storage_resource.download_raw("", Path("/tmp/file.grib"))

        with pytest.raises(ValueError, match="cannot be empty"):
            storage_resource.download_raw("   ", Path("/tmp/file.grib"))

    def test_raises_error_for_missing_file(self, storage_resource, mock_s3_client):
        """Should raise FileNotFoundError for missing S3 file."""
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        mock_s3_client.download_file.side_effect = ClientError(error_response, "download_file")

        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "file.grib"

                with pytest.raises(FileNotFoundError, match="test-raw"):
                    storage_resource.download_raw("missing/file.grib", local_path)



class TestObjectStorageResourceConfig:
    """Tests for ObjectStorageResource configuration."""

    def test_create_object_storage(self):
        """Should accept custom configuration values."""
        resource = ObjectStorageResource(
            endpoint_url="https://s3.amazonaws.com",
            access_key="custom-key",
            secret_key="custom-secret",
            raw_bucket="my-raw-bucket",
            use_ssl=True,
        )

        assert resource.endpoint_url == "https://s3.amazonaws.com"
        assert resource.access_key == "custom-key"
        assert resource.secret_key == "custom-secret"
        assert resource.raw_bucket == "my-raw-bucket"
        assert resource.use_ssl is True


class TestPostgresCatalogResource:
    """Tests for PostgresCatalogResource insert helpers."""

    def test_insert_raw_file_uses_dataclass(self, psycopg_mocks):
        """Should insert raw file using typed dataclass fields."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db")
        raw = RawFileRecord(
            id=uuid.uuid4(),
            source="ads",
            dataset="cams-europe-air-quality-forecasts-forecast",
            date=date(2025, 1, 2),
            s3_key="ads/cams/2025-01-02/run.grib",
        )

        with patch("pipeline_python.defs.resources.psycopg.connect", psycopg_mocks["connect"]):
            resource.insert_raw_file(raw)

        psycopg_mocks["connect"].assert_called_once_with("postgresql://localhost:5432/db")
        psycopg_mocks["cursor"].execute.assert_called_once()
        args, kwargs = psycopg_mocks["cursor"].execute.call_args
        assert "INSERT INTO catalog.raw_files" in args[0]
        assert args[1] == (
            str(raw.id),
            "ads",
            "cams-europe-air-quality-forecasts-forecast",
            date(2025, 1, 2),
            "ads/cams/2025-01-02/run.grib",
        )

    def test_insert_curated_data_uses_dataclass(self, psycopg_mocks):
        """Should insert curated file using typed dataclass fields."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db")
        curated = CuratedDataRecord(
            id=uuid.uuid4(),
            raw_file_id=uuid.uuid4(),
            variable="pm2p5",
            unit="µg/m³",
            timestamp=datetime(2025, 1, 2, 12, 0, 0),
        )

        with patch("pipeline_python.defs.resources.psycopg.connect", psycopg_mocks["connect"]):
            resource.insert_curated_data(curated)

        psycopg_mocks["connect"].assert_called_once_with("postgresql://localhost:5432/db")
        psycopg_mocks["cursor"].execute.assert_called_once()
        args, kwargs = psycopg_mocks["cursor"].execute.call_args
        assert "INSERT INTO catalog.curated_data" in args[0]
        assert args[1] == (
            str(curated.id),
            str(curated.raw_file_id),
            "pm2p5",
            "µg/m³",
            datetime(2025, 1, 2, 12, 0, 0),
        )

    def test_teardown_closes_connection(self, psycopg_mocks):
        """teardown_after_execution should close the connection."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db")
        raw = RawFileRecord(
            id=uuid.uuid4(),
            source="ads",
            dataset="test-dataset",
            date=date(2025, 1, 2),
            s3_key="ads/test/2025-01-02/run1.grib",
        )

        with patch("pipeline_python.defs.resources.psycopg.connect", psycopg_mocks["connect"]):
            resource.insert_raw_file(raw)
            resource.teardown_after_execution(None)

        # Connection should be created once and closed by teardown
        psycopg_mocks["connect"].assert_called_once_with("postgresql://localhost:5432/db")
        psycopg_mocks["conn"].close.assert_called_once()

    def test_reuses_connection_across_calls(self, psycopg_mocks):
        """Multiple inserts within one execution should reuse the same connection."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db")
        raw1 = RawFileRecord(
            id=uuid.uuid4(),
            source="ads",
            dataset="test-dataset",
            date=date(2025, 1, 2),
            s3_key="ads/test/2025-01-02/run1.grib",
        )
        raw2 = RawFileRecord(
            id=uuid.uuid4(),
            source="ads",
            dataset="test-dataset",
            date=date(2025, 1, 3),
            s3_key="ads/test/2025-01-03/run2.grib",
        )

        with patch("pipeline_python.defs.resources.psycopg.connect", psycopg_mocks["connect"]):
            resource.insert_raw_file(raw1)
            resource.insert_raw_file(raw2)

        # psycopg.connect should only be called once despite two inserts
        psycopg_mocks["connect"].assert_called_once_with("postgresql://localhost:5432/db")


class TestPostgresDsnFromEnv:
    """Tests for _postgres_dsn_from_env function."""

    def test_constructs_dsn_with_all_env_vars(self, monkeypatch):
        """Should construct correct DSN with all environment variables."""
        from pipeline_python.defs.resources import _postgres_dsn_from_env

        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTGRES_HOST", "testhost")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "testdb")

        dsn = _postgres_dsn_from_env()

        assert dsn == "postgresql://testuser:testpass@testhost:5433/testdb"

    def test_raises_key_error_when_user_missing(self, monkeypatch):
        """Should raise KeyError when POSTGRES_USER is missing."""
        from pipeline_python.defs.resources import _postgres_dsn_from_env

        monkeypatch.delenv("POSTGRES_USER", raising=False)
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")

        with pytest.raises(KeyError, match="POSTGRES_USER"):
            _postgres_dsn_from_env()

    def test_raises_key_error_when_password_missing(self, monkeypatch):
        """Should raise KeyError when POSTGRES_PASSWORD is missing."""
        from pipeline_python.defs.resources import _postgres_dsn_from_env

        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

        with pytest.raises(KeyError, match="POSTGRES_PASSWORD"):
            _postgres_dsn_from_env()
