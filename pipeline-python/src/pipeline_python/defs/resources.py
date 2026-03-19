"""
Dagster resources for pipeline execution.

Resources assembled here:
- CdsClient: Copernicus ADS API client for GRIB data retrieval
- PostgresCatalogResource: Postgres metadata catalog (raw files, curated data lineage)
- ObjectStore / ClickHouseGridStore: storage resources wired from pipeline_python.storage.*
"""
import os

import psycopg
from pydantic import PrivateAttr

import dagster as dg
from .models import CuratedDataRecord, RawFileRecord
from pipeline_python.storage import ObjectStore
from pipeline_python.storage.clickhouse_grid_store import ClickHouseGridStore
from pipeline_python.ingestion import CdsClient, EcmwfClient


# -----------------------------------------------------------------------------
# Catalog resources
# -----------------------------------------------------------------------------

def _postgres_dsn_from_env() -> str:
    user = os.environ["CATALOG_PG_USER"]
    password = os.environ["CATALOG_PG_PASSWORD"]
    host = os.environ.get("CATALOG_PG_HOST", "postgres")
    port = os.environ.get("CATALOG_PG_PORT", "5432")
    db_name = os.environ.get("CATALOG_PG_DB", "jackfruit")
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
    """
    dsn: str
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
        query = """
        INSERT INTO catalog.raw_files (id, source, dataset, date, s3_key, created_at)
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
        query = """
        INSERT INTO catalog.curated_data (id, raw_file_id, variable, unit, timestamp)
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
            "cds_client": CdsClient(
                url=os.environ.get("ADS_BASE_URL", "https://ads.atmosphere.copernicus.eu/api"),
                api_key=dg.EnvVar("ADS_API_KEY"),
            ),
            "ecmwf_client": EcmwfClient(
                source=os.environ.get("ECMWF_SOURCE", "ecmwf"),
            ),
            "object_store": ObjectStore(
                endpoint_url=os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
                access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
                secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
                raw_bucket=os.environ.get("MINIO_RAW_BUCKET", "jackfruit-raw"),
                use_ssl=os.environ.get("MINIO_USE_SSL", "false").lower() in {"true", "1", "yes", "on"},
            ),
            "catalog": PostgresCatalogResource(
                dsn=_postgres_dsn_from_env(),
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

