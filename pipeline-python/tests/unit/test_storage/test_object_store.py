"""
Tests for ObjectStore (storage/object_store.py).
"""
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from pipeline_python.storage.object_store import ObjectStore


@pytest.fixture
def mock_s3_client():
    """Provide a mock boto3 S3 client."""
    return Mock()


@pytest.fixture
def storage_resource():
    """Provide an ObjectStore for testing."""
    return ObjectStore(
        endpoint_url="http://localhost:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        raw_bucket="test-raw",
        use_ssl=False,
    )


class TestObjectStoreDownloadRaw:
    """Tests for download_raw method."""

    def test_downloads_file_successfully(self, storage_resource, mock_s3_client):
        """Should download file from raw bucket."""
        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
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
        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
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

        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "file.grib"

                with pytest.raises(FileNotFoundError, match="test-raw"):
                    storage_resource.download_raw("missing/file.grib", local_path)

    def test_raises_error_for_404_code(self, storage_resource, mock_s3_client):
        """Should raise FileNotFoundError for HTTP 404 error code (alternative to NoSuchKey)."""
        error_response = {"Error": {"Code": "404", "Message": "Not found"}}
        mock_s3_client.download_file.side_effect = ClientError(error_response, "download_file")

        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "file.grib"

                with pytest.raises(FileNotFoundError, match="test-raw"):
                    storage_resource.download_raw("missing/file.grib", local_path)

    def test_reraises_unexpected_client_error(self, storage_resource, mock_s3_client):
        """Should re-raise ClientError as-is for non-404 errors (e.g. access denied)."""
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}
        mock_s3_client.download_file.side_effect = ClientError(error_response, "download_file")

        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "file.grib"

                with pytest.raises(ClientError):
                    storage_resource.download_raw("secret/file.grib", local_path)


class TestObjectStoreUploadRaw:
    """Tests for upload_raw method."""

    def test_uploads_file_successfully(self, storage_resource, mock_s3_client):
        """Should upload local file to raw bucket with correct args."""
        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
            with tempfile.NamedTemporaryFile(suffix=".grib") as tmpfile:
                local_path = Path(tmpfile.name)

                storage_resource.upload_raw("ads/dataset/2025-01-01/file.grib", local_path)

                mock_s3_client.upload_file.assert_called_once_with(
                    str(local_path),
                    "test-raw",
                    "ads/dataset/2025-01-01/file.grib",
                )

    def test_raises_error_for_empty_key(self, storage_resource):
        """Should raise ValueError for empty or whitespace-only key."""
        with pytest.raises(ValueError, match="cannot be empty"):
            storage_resource.upload_raw("", Path("/tmp/file.grib"))

        with pytest.raises(ValueError, match="cannot be empty"):
            storage_resource.upload_raw("   ", Path("/tmp/file.grib"))

    def test_raises_ioerror_on_client_error(self, storage_resource, mock_s3_client):
        """Should wrap ClientError as IOError on upload failure."""
        error_response = {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}
        mock_s3_client.upload_file.side_effect = ClientError(error_response, "upload_file")

        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client):
            with pytest.raises(IOError, match="test-raw"):
                storage_resource.upload_raw("ads/dataset/file.grib", Path("/tmp/file.grib"))


class TestObjectStoreConfig:
    """Tests for ObjectStore configuration."""

    def test_create_object_storage(self):
        """Should accept custom configuration values."""
        resource = ObjectStore(
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

    def test_client_passes_credentials_to_boto3(self, storage_resource, mock_s3_client):
        """Should forward endpoint, credentials, and ssl flag to boto3.client."""
        with patch("pipeline_python.storage.object_store.boto3.client", return_value=mock_s3_client) as mock_boto3:
            storage_resource.download_raw("some/key.grib", Path("/tmp/file.grib"))

            mock_boto3.assert_called_once_with(
                "s3",
                endpoint_url="http://localhost:9000",
                aws_access_key_id="test-access-key",
                aws_secret_access_key="test-secret-key",
                use_ssl=False,
            )
