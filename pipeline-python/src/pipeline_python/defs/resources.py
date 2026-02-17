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
from botocore.exceptions import ClientError
from dagster_docker import PipesDockerClient
import psycopg
from pydantic import PrivateAttr

import dagster as dg
from .models import CuratedDataRecord, RawFileRecord


# Environment variables to forward to the ingestion container
_INGESTION_ENV_VARS = [
    "ADS_BASE_URL",
    "ADS_API_KEY",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_RAW_BUCKET",
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
                storage.upload_curated(output_path, "pm2p5/cams/.../data.grib2")
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    raw_bucket: str = "jackfruit-raw"
    curated_bucket: str = "jackfruit-curated"
    use_ssl: bool = False

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
            ClientError: If the S3 download fails (e.g., file not found, permission denied)
        """
        if not key or not key.strip():
            raise ValueError("S3 key cannot be empty")
        
        client = self._get_client()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            client.download_file(self.raw_bucket, key, str(local_path))
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404" or error_code == "NoSuchKey":
                # Add more context to the error message
                e.response["Error"]["Message"] = f"Object not found in bucket '{self.raw_bucket}': {key}"
            raise

    def upload_curated(self, local_path: Path, key: str) -> None:
        """
        Upload a local file to the curated bucket.

        Uses multipart upload for large files automatically.

        Args:
            local_path: Local file path to upload
            key: S3 key (e.g., "pm2p5/cams/2025/03/11/14/data.grib2")

        Raises:
            ValueError: If key is empty or local_path doesn't exist
            ClientError: If the S3 upload fails
        """
        if not key or not key.strip():
            raise ValueError("S3 key cannot be empty")
        
        if not local_path.exists():
            raise ValueError(f"Local file does not exist: {local_path}")
        
        if not local_path.is_file():
            raise ValueError(f"Path is not a file: {local_path}")
        
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
        except ClientError:
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

# -----------------------------------------------------------------------------
# Catalog resources
# -----------------------------------------------------------------------------

def _postgres_dsn_from_env() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db_name = os.environ.get("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


class PostgresCatalogResource(dg.ConfigurableResource):
    """
    Resource for interacting with the Postgres metadata catalog.

    This resource manages connections to the catalog database and provides methods
    for inserting raw and curated file metadata. Supports context manager protocol
    for efficient connection reuse across multiple operations.

    **Security Note**: The DSN contains database credentials. Always source these
    from environment variables or secure credential stores, never hardcode them.

    **Upsert Behavior**:
    - `insert_raw_file`: Uses ON CONFLICT DO NOTHING (idempotent)
    - `insert_curated_file`: Uses ON CONFLICT DO UPDATE (latest metadata wins)

    **Usage**:
        # Single insert (creates new connection)
        catalog.insert_raw_file(record)

        # Multiple inserts (reuses connection)
        with catalog:
            for record in records:
                catalog.insert_curated_file(record)

    Attributes:
        dsn: Postgres DSN (e.g., postgresql://user:password@host:port/dbname)
    """
    dsn: str
    _connection: psycopg.Connection | None = PrivateAttr(default=None)

    def get_connection(self) -> psycopg.Connection:
        """
        Get or create a database connection.

        Returns existing connection if available, otherwise creates a new one.
        When using the context manager, the connection is reused across operations.

        Returns:
            Active Postgres connection
        """
        if self._connection is None:
            self._connection = psycopg.connect(self.dsn)
        return self._connection

    def close(self) -> None:
        """Close the database connection if open."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Enter context manager - prepares connection for reuse."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - closes connection."""
        self.close()
        return False

    def insert_raw_file(self, raw_file: RawFileRecord) -> None:
        """
        Insert a raw file record into the database.

        Uses ON CONFLICT DO NOTHING for idempotency - repeated inserts with the
        same ID are silently ignored.

        Args:
            raw_file: Raw file record to insert
        """
        query = """
        INSERT INTO catalog.raw_files (id, source, dataset, date, s3_key, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO NOTHING;
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (
                str(raw_file.id),
                raw_file.source,
                raw_file.dataset,
                raw_file.date,
                raw_file.s3_key,
            ))
        conn.commit()

    def insert_curated_data(self, curated_file: CuratedDataRecord) -> None:
        """
        Insert a curated file record into the database.

        Uses ON CONFLICT DO UPDATE - if a file with the same id already exists,
        its metadata is updated with the new values. This allows reprocessing without
        manual cleanup.

        Args:
            curated_file: Curated file record to insert or update
        """
        query = """
        INSERT INTO catalog.curated_data (id, raw_file_id, variable, unit, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            raw_file_id = EXCLUDED.raw_file_id,
            variable = EXCLUDED.variable,
            unit = EXCLUDED.unit,
            timestamp = EXCLUDED.timestamp;
        """
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (
                str(curated_file.id),
                str(curated_file.raw_file_id),
                curated_file.variable,
                curated_file.unit,
                curated_file.timestamp,
            ))
        conn.commit()


@dg.definitions
def catalog_resources():
    return dg.Definitions(
        resources={
            "catalog": PostgresCatalogResource(
                dsn=_postgres_dsn_from_env()
            )
        }
    )

