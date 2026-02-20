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
        curated_bucket="test-curated",
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
        """Should raise ClientError for missing S3 file."""
        # Simulate S3 NoSuchKey error
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        mock_s3_client.download_file.side_effect = ClientError(error_response, "download_file")

        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "file.grib"

                with pytest.raises(ClientError, match="NoSuchKey"):
                    storage_resource.download_raw("missing/file.grib", local_path)


class TestObjectStorageResourceUploadCurated:
    """Tests for upload_curated method."""

    def test_uploads_file_successfully(self, storage_resource, mock_s3_client):
        """Should upload file to curated bucket."""
        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            with tempfile.NamedTemporaryFile(delete=False) as f:
                local_path = Path(f.name)

            try:
                storage_resource.upload_curated(
                    local_path,
                    "pm2p5/cams/2025/01/15/12/data.grib2"
                )

                mock_s3_client.upload_file.assert_called_once_with(
                    str(local_path),
                    "test-curated",
                    "pm2p5/cams/2025/01/15/12/data.grib2",
                )
            finally:
                local_path.unlink()

    def test_raises_error_for_empty_key(self, storage_resource):
        """Should raise ValueError for empty key."""
        with tempfile.NamedTemporaryFile() as f:
            local_path = Path(f.name)

            with pytest.raises(ValueError, match="cannot be empty"):
                storage_resource.upload_curated(local_path, "")

            with pytest.raises(ValueError, match="cannot be empty"):
                storage_resource.upload_curated(local_path, "   ")

    def test_raises_error_for_nonexistent_file(self, storage_resource):
        """Should raise ValueError if local file doesn't exist."""
        with pytest.raises(ValueError, match="does not exist"):
            storage_resource.upload_curated(
                Path("/nonexistent/file.grib"),
                "pm2p5/cams/2025/01/15/12/data.grib2"
            )

    def test_raises_error_for_directory(self, storage_resource):
        """Should raise ValueError if path is a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="not a file"):
                storage_resource.upload_curated(
                    Path(tmpdir),
                    "pm2p5/cams/2025/01/15/12/data.grib2"
                )


class TestObjectStorageResourceKeyExists:
    """Tests for key_exists method."""

    def test_returns_true_for_existing_key(self, storage_resource, mock_s3_client):
        """Should return True if key exists."""
        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            result = storage_resource.key_exists("test-raw", "ads/dataset/file.grib")

            assert result is True
            mock_s3_client.head_object.assert_called_once_with(
                Bucket="test-raw",
                Key="ads/dataset/file.grib"
            )

    def test_returns_false_for_missing_key(self, storage_resource, mock_s3_client):
        """Should return False if key doesn't exist."""
        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )

        with patch("pipeline_python.defs.resources.boto3.client", return_value=mock_s3_client):
            result = storage_resource.key_exists("test-raw", "missing/file.grib")

            assert result is False


class TestObjectStorageResourceConfig:
    """Tests for ObjectStorageResource configuration."""

    def test_has_default_values(self):
        """Should have sensible default values."""
        resource = ObjectStorageResource(
            endpoint_url="http://localhost:9000",
            access_key="key",
            secret_key="secret",
        )

        assert resource.raw_bucket == "jackfruit-raw"
        assert resource.curated_bucket == "jackfruit-curated"
        assert resource.use_ssl is False

    def test_accepts_custom_values(self):
        """Should accept custom configuration values."""
        resource = ObjectStorageResource(
            endpoint_url="https://s3.amazonaws.com",
            access_key="custom-key",
            secret_key="custom-secret",
            raw_bucket="my-raw-bucket",
            curated_bucket="my-curated-bucket",
            use_ssl=True,
        )

        assert resource.endpoint_url == "https://s3.amazonaws.com"
        assert resource.access_key == "custom-key"
        assert resource.secret_key == "custom-secret"
        assert resource.raw_bucket == "my-raw-bucket"
        assert resource.curated_bucket == "my-curated-bucket"
        assert resource.use_ssl is True


class TestPostgresCatalogResource:
    """Tests for PostgresCatalogResource insert helpers."""

    def test_insert_raw_file_uses_dataclass(self, psycopg_mocks):
        """Should insert raw file using typed dataclass fields."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db", schema="catalog")
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

    def test_insert_curated_file_uses_dataclass(self, psycopg_mocks):
        """Should insert curated file using typed dataclass fields."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db", schema="catalog")
        curated = CuratedDataRecord(
            id=uuid.uuid4(),
            raw_file_id=uuid.uuid4(),
            variable="pm2p5",
            unit="kg m**-3",
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
            "kg m**-3",
            datetime(2025, 1, 2, 12, 0, 0),
        )

    def test_context_manager_reuses_connection(self, psycopg_mocks):
        """Should reuse same connection when used as context manager."""
        resource = PostgresCatalogResource(dsn="postgresql://localhost:5432/db", schema="catalog")
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
            with resource:
                resource.insert_raw_file(raw1)
                resource.insert_raw_file(raw2)

        # Connection should be created once and reused
        psycopg_mocks["connect"].assert_called_once_with("postgresql://localhost:5432/db")
        # Close should be called on exit
        psycopg_mocks["conn"].close.assert_called_once()


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
