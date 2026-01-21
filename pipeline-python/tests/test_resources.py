"""
Tests for storage resources.

Tests the ObjectStorageResource for S3/MinIO interactions.
"""
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from pipeline_python.defs.resources import ObjectStorageResource


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
