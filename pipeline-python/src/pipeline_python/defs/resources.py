"""
Dagster resources for ingestion execution and object storage.

This module provides:
1. DockerIngestionClient: Run Go ingestion container via Dagster Pipes
2. ObjectStorageResource: S3/MinIO client for raw and curated buckets

INGESTION EXECUTION:
The Go ingestion container uses Dagster Pipes in "external process" mode — it
launches the container, waits for exit, and captures logs. No bidirectional comms.

DOCKER SIBLING PATTERN:
When Dagster runs inside a container with Docker socket mounted, PipesDockerClient
spawns containers as *siblings* on the host Docker daemon (not nested). We must
explicitly configure:
- Network: attach to 'jackfruit' network so container can reach MinIO
- Env vars: pass through from Dagster container's environment

OBJECT STORAGE:
ObjectStorageResource provides download/upload methods optimized for large GRIB
files. Uses boto3 with local temp files (grib2io requires local file access).

DEPRECATION NOTICE:
The Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import os
from pathlib import Path

import boto3
from dagster_docker import PipesDockerClient

import dagster as dg


# Environment variables to forward to the ingestion container
_INGESTION_ENV_VARS = [
    "ADS_BASE_URL",
    "ADS_API_KEY",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_BUCKET",
    "MINIO_USE_SSL",
]


class DockerIngestionClient(dg.ConfigurableResource):
    """
    Run ingestion via local Docker using dagster-docker PipesDockerClient.

    Spawns the ingestion container as a sibling on the host Docker daemon.
    Requires Docker socket mounted at /var/run/docker.sock.

    Attributes:
        image: Docker image name (e.g., 'jackfruit-ingestion:latest')
        network: Docker network to attach to (e.g., 'jackfruit_jackfruit')
    """

    image: str = "jackfruit-ingestion:latest"
    network: str = "jackfruit_jackfruit"  # docker-compose prefixes with project name

    def _get_forwarded_env(self) -> dict[str, str]:
        """Collect environment variables to forward to the ingestion container."""
        env = {}
        for var in _INGESTION_ENV_VARS:
            value = os.environ.get(var)
            if value is not None:
                env[var] = value
        return env

    def run_ingestion(
        self,
        context: dg.AssetExecutionContext,
        *,
        dataset: str,
        date: str,
        run_id: str,
    ) -> dg.MaterializeResult:
        """
        Run ingestion container as Docker sibling.

        Uses PipesDockerClient to spawn a sibling container on the host Docker daemon.
        The container is attached to the configured network so it can reach MinIO.
        Environment variables are forwarded from the Dagster container.

        Args:
            context: Dagster asset execution context
            dataset: Dataset identifier (e.g., 'cams-europe-air-quality-forecasts')
            date: Date to ingest (YYYY-MM-DD format)
            run_id: Unique run identifier for idempotency

        Returns:
            MaterializeResult with run metadata
        """
        client = PipesDockerClient()

        # Build command arguments for the ingestion CLI
        command = [
            f"--dataset={dataset}",
            f"--date={date}",
            f"--run-id={run_id}",
        ]

        # Forward env vars and set network for sibling container
        container_kwargs = {
            "network": self.network,
        }

        context.log.info(
            f"Running ingestion container: image={self.image}, "
            f"network={self.network}, dataset={dataset}, date={date}"
        )

        # Run the container and wait for completion
        # PipesDockerClient handles: create -> start -> wait -> stop
        # Logs are streamed to Dagster's log output
        client.run(
            context=context,
            image=self.image,
            command=command,
            env=self._get_forwarded_env(),
            container_kwargs=container_kwargs,
        )

        return dg.MaterializeResult(
            metadata={
                "run_id": run_id,
                "dataset": dataset,
                "date": date,
                "image": self.image,
                "network": self.network,
            }
        )


# -----------------------------------------------------------------------------
# Resource Definitions - Wired up for use by assets
# -----------------------------------------------------------------------------

@dg.definitions
def ingestion_resources():
    """
    Register the ingestion_client resource.

    Uses DockerIngestionClient for local development.
    """
    return dg.Definitions(
        resources={
            "ingestion_client": DockerIngestionClient(
                image="jackfruit-ingestion:latest",
                network="jackfruit_jackfruit",
            ),
        }
    )

# -----------------------------------------------------------------------------
# Storage resources
# -----------------------------------------------------------------------------

class ObjectStorageResource(dg.ConfigurableResource):
    """
    S3/MinIO client for raw and curated buckets.

    Provides explicit download/upload methods optimized for large files.
    Uses boto3 with local temp files (grib2io requires local file access).

    **SECURITY**: Credentials must ALWAYS be sourced from secure stores (environment
    variables, secrets manager, etc.) and NEVER hardcoded. The `access_key` and
    `secret_key` fields use `EnvVar` to prevent credential exposure in logs and UI.

    Attributes:
        endpoint_url: S3/MinIO endpoint URL (e.g., 'http://minio:9000')
        access_key: S3/MinIO access key (sourced from environment variable)
        secret_key: S3/MinIO secret key (sourced from environment variable)
        raw_bucket: Name of the raw data bucket (default: 'jackfruit-raw')
        curated_bucket: Name of the curated data bucket (default: 'jackfruit-curated')
        use_ssl: Whether to use SSL for connections (default: False)

    Example usage in an asset:
        @dg.asset
        def my_asset(storage: ObjectStorageResource):
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / "raw.grib"
                storage.download_raw("ads/cams/.../file.grib", local_path)
                # ... process ...
                storage.upload_curated(output_path, "curated/cams/.../data.grib2")
    """

    endpoint_url: str
    access_key: dg.EnvVar
    secret_key: dg.EnvVar
    raw_bucket: str = "jackfruit-raw"
    curated_bucket: str = "jackfruit-curated"
    use_ssl: bool = False

    def _get_client(self):
        """Create boto3 S3 client configured for MinIO/S3."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key.get_value(),
            aws_secret_access_key=self.secret_key.get_value(),
            use_ssl=self.use_ssl,
        )

    def download_raw(self, key: str, local_path: Path) -> None:
        """
        Download a file from the raw bucket to local disk.

        Args:
            key: S3 key (e.g., "ads/cams-europe.../2025-03-11/{run_id}.grib")
            local_path: Local file path to write to
        """
        client = self._get_client()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(self.raw_bucket, key, str(local_path))

    def upload_curated(self, local_path: Path, key: str) -> None:
        """
        Upload a local file to the curated bucket.

        Uses multipart upload for large files automatically.

        Args:
            local_path: Local file path to upload
            key: S3 key (e.g., "curated/cams/europe-air-quality/pm2p5/2025/03/11/14/data.grib2")
        """
        client = self._get_client()
        client.upload_file(str(local_path), self.curated_bucket, key)

    def key_exists(self, bucket: str, key: str) -> bool:
        """
        Check if a key exists in the specified bucket.

        Args:
            bucket: Bucket name to check
            key: S3 key to check for existence

        Returns:
            True if key exists, False otherwise
        """
        client = self._get_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.ClientError:
            return False


# Resource factory for Dagster definitions
@dg.definitions
def storage_resources():
    """
    Register the storage resource.

    Reads configuration from environment variables. The following environment
    variables are REQUIRED:
    - MINIO_ACCESS_KEY: S3/MinIO access key (required, no default)
    - MINIO_SECRET_KEY: S3/MinIO secret key (required, no default)

    Optional environment variables with defaults:
    - MINIO_ENDPOINT_URL: S3/MinIO endpoint URL (default: 'http://minio:9000')
    - MINIO_RAW_BUCKET: Raw data bucket name (default: 'jackfruit-raw')
    - MINIO_CURATED_BUCKET: Curated data bucket name (default: 'jackfruit-curated')
    """
    return dg.Definitions(
        resources={
            "storage": ObjectStorageResource(
                endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
                access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
                secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
                raw_bucket=os.environ.get("MINIO_RAW_BUCKET", "jackfruit-raw"),
                curated_bucket=os.environ.get("MINIO_CURATED_BUCKET", "jackfruit-curated"),
            ),
        }
    )
