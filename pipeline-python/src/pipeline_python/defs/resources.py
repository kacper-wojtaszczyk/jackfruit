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
from pipeline_python.storage.clickhouse_grid_store import ClickHouseGridStore


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
# Storage resources
# -----------------------------------------------------------------------------

class ObjectStorageResource(dg.ConfigurableResource):
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
            if error_code in {"404", "NoSuchKey"}:
                raise FileNotFoundError(f"Object not found in bucket '{self.raw_bucket}': {key}") from e
            raise



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
    for inserting raw and curated file metadata. The connection is opened lazily
    on first use and closed by Dagster at the end of each asset execution via
    teardown_after_execution.

    **Security Note**: The DSN contains database credentials. Always source these
    from environment variables or secure credential stores, never hardcode them.

    **Upsert Behavior**:
    - `insert_raw_file`: Uses ON CONFLICT DO NOTHING (idempotent)
    - `insert_curated_data`: Uses ON CONFLICT DO UPDATE (latest metadata wins)

    Attributes:
        dsn: Postgres DSN (e.g., postgresql://user:password@host:port/dbname)
        schema: Postgres schema name (e.g., "catalog" in prod, "test_catalog" in tests)
    """
    dsn: str
    schema: str
    _connection: psycopg.Connection | None = PrivateAttr(default=None)

    def _get_connection(self) -> psycopg.Connection:
        """Get or create a database connection. Reused across calls within one execution."""
        if self._connection is None:
            self._connection = psycopg.connect(self.dsn)
        return self._connection

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        """Close the database connection. Called by Dagster at end of each asset execution."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def insert_raw_file(self, raw_file: RawFileRecord) -> None:
        """
        Insert a raw file record into the database.

        Uses ON CONFLICT DO NOTHING for idempotency - repeated inserts with the
        same ID are silently ignored.

        Args:
            raw_file: Raw file record to insert
        """
        query = f"""
        INSERT INTO {self.schema}.raw_files (id, source, dataset, date, s3_key, created_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO NOTHING;
        """
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (
                str(raw_file.id),
                raw_file.source,
                raw_file.dataset,
                raw_file.date,
                raw_file.s3_key,
            ))
        conn.commit()

    def insert_curated_data(self, curated_data: CuratedDataRecord) -> None:
        """
        Insert a curated file record into the database.

        Uses ON CONFLICT DO UPDATE - if a file with the same id already exists,
        its metadata is updated with the new values. This allows reprocessing without
        manual cleanup.

        Args:
            curated_data: Curated file record to insert or update
        """
        query = f"""
        INSERT INTO {self.schema}.curated_data (id, raw_file_id, variable, unit, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            raw_file_id = EXCLUDED.raw_file_id,
            variable = EXCLUDED.variable,
            unit = EXCLUDED.unit,
            timestamp = EXCLUDED.timestamp;
        """
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(query, (
                str(curated_data.id),
                str(curated_data.raw_file_id),
                curated_data.variable,
                curated_data.unit,
                curated_data.timestamp,
            ))
        conn.commit()


# -----------------------------------------------------------------------------
# Resource Definitions - Wired up for use by assets
# -----------------------------------------------------------------------------

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "ingestion_client": DockerIngestionClient(
                image="jackfruit-ingestion:latest",
                network="jackfruit_jackfruit",
            ),
            "storage": ObjectStorageResource(
                endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
                access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
                secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
                raw_bucket=os.environ.get("MINIO_RAW_BUCKET", "jackfruit-raw"),
                use_ssl=os.environ.get("MINIO_USE_SSL", "false").lower() in {"true", "1", "yes", "on"},
            ),
            "catalog": PostgresCatalogResource(
                dsn=_postgres_dsn_from_env(),
                schema=os.environ.get("POSTGRES_SCHEMA", "catalog"),
            ),
            "grid_store": ClickHouseGridStore(
                host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
                username=dg.EnvVar("CLICKHOUSE_USER"),
                password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
                database=os.environ.get("CLICKHOUSE_DB", "jackfruit"),
            ),
        }
    )

