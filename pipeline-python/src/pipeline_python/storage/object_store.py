from pathlib import Path

import boto3
import dagster as dg
from botocore.exceptions import ClientError


class ObjectStore(dg.ConfigurableResource):
    """
    S3/MinIO client for the raw data bucket.

    Provides explicit download/upload methods optimized for large files.
    Uses boto3 with local temp files (grib2io requires local file access).

    Attributes:
        endpoint_url: S3/MinIO endpoint URL (e.g., 'http://minio:9000')
        access_key: S3/MinIO access key (sourced from environment variable)
        secret_key: S3/MinIO secret key (sourced from environment variable)
        raw_bucket: Name of the raw data bucket (default: 'jackfruit-raw')
        use_ssl: Whether to use SSL for connections (default: False)

    Example usage in an asset:
        @dg.asset
        def my_asset(storage: ObjectStorageResource):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "raw.grib"
                storage.download_raw("ads/cams/.../file.grib", local_path)
                # ... process ...
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    raw_bucket: str
    use_ssl: bool

    def _get_client(self):
        """Create boto3 S3 client configured for MinIO/S3."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=self.use_ssl,
        )

    def download_raw(self, key: str, local_path: Path) -> None:
        """
        Download a file from the raw bucket to local disk.

        Args:
            key: S3 key (e.g., "ads/cams-europe.../2025-03-11/{run_id}.grib")
            local_path: Local file path to write to

        Raises:
            ValueError: If key is empty or contains invalid characters
            FileNotFoundError: If the S3 download fails (e.g., file not found, permission denied)
        """
        if not key or not key.strip():
            raise ValueError("S3 key cannot be empty")

        client = self._get_client()
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            client.download_file(self.raw_bucket, key, str(local_path))
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code in {"404", "NoSuchKey"}:
                raise FileNotFoundError(f"Object not found in bucket '{self.raw_bucket}': {key}") from e
            raise

    def upload_raw(self, key: str, local_path: Path) -> None:
        """
        Upload a local file to the raw bucket.

        Args:
            key: S3 key (e.g., "ads/cams-europe.../2025-03-11/{run_id}.grib")
            local_path: Local file path to upload

        Raises:
            ValueError: If key is empty
            IOError: If the upload fails
        """
        if not key or not key.strip():
            raise ValueError("S3 key cannot be empty")

        client = self._get_client()

        try:
            client.upload_file(str(local_path), self.raw_bucket, key)
        except ClientError as e:
            raise IOError(f"Failed to upload to bucket '{self.raw_bucket}': {key}") from e
