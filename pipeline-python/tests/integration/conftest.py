"""
Integration test infrastructure fixtures.

Loads .env.test before any imports so all resources pick up test config.
Auto-marks tests in this directory as 'integration'

DDL in session fixtures mirrors production migrations — keep in sync:
  - ClickHouse: migrations/clickhouse/init.sql
  - Postgres: migrations/postgres/init.sql
"""
import os

import boto3
import clickhouse_connect
import psycopg
import pytest
from dotenv import load_dotenv
from pathlib import Path

# Load test env vars BEFORE any resource imports
load_dotenv(Path(__file__).parent.parent / ".env.test", override=True)

from pipeline_python.defs.resources import ObjectStorageResource, PostgresCatalogResource
from pipeline_python.storage.clickhouse import ClickHouseGridStore

def pytest_collection_modifyitems(config, items):
    """Auto-mark all tests in this directory as integration."""
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


# ---------------------------------------------------------------------------
# Session-scoped infrastructure fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def ch_client():
    """Create test database and table in ClickHouse.
    DDL mirrors migrations/clickhouse/init.sql — keep in sync."""
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )
    client.command("CREATE DATABASE IF NOT EXISTS jackfruit_test")
    client.command("""
        CREATE TABLE IF NOT EXISTS jackfruit_test.grid_data (
            variable     LowCardinality(String),
            timestamp    DateTime,
            lat          Float32,
            lon          Float32,
            value        Float32,
            unit         LowCardinality(String),
            catalog_id   UUID,
            inserted_at  DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(inserted_at)
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (variable, timestamp, lat, lon)
    """)
    yield client
    client.close()


@pytest.fixture(scope="session")
def pg_connection():
    """Create test schema and tables in Postgres.
    DDL mirrors migrations/postgres/init.sql — keep in sync."""
    dsn = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    conn = psycopg.connect(dsn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS test_catalog")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_catalog.raw_files (
            id UUID PRIMARY KEY,
            source TEXT NOT NULL,
            dataset TEXT NOT NULL,
            date DATE NOT NULL,
            s3_key TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_catalog.curated_data (
            id UUID PRIMARY KEY,
            raw_file_id UUID NOT NULL REFERENCES test_catalog.raw_files(id),
            variable TEXT NOT NULL,
            unit TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3_client():
    """Create test bucket in MinIO."""
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    bucket = os.environ["MINIO_RAW_BUCKET"]
    try:
        client.create_bucket(Bucket=bucket)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    yield client


# ---------------------------------------------------------------------------
# Per-test cleanup fixtures (autouse)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_ch(ch_client):
    """Truncate CH test table before each test."""
    ch_client.command("TRUNCATE TABLE jackfruit_test.grid_data")
    yield


@pytest.fixture(autouse=True)
def clean_pg(pg_connection):
    """Truncate Postgres test tables before each test."""
    pg_connection.execute("TRUNCATE test_catalog.curated_data, test_catalog.raw_files CASCADE")
    pg_connection.commit()
    yield


@pytest.fixture(autouse=True)
def clean_minio(s3_client):
    """Empty MinIO test bucket before each test."""
    bucket = os.environ["MINIO_RAW_BUCKET"]
    resp = s3_client.list_objects_v2(Bucket=bucket)
    for obj in resp.get("Contents", []):
        s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
    yield


# ---------------------------------------------------------------------------
# Resource fixtures (env-driven, same pattern as defs/resources.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def grid_store():
    return ClickHouseGridStore(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DB"],
    )


@pytest.fixture
def storage():
    return ObjectStorageResource(
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        raw_bucket=os.environ["MINIO_RAW_BUCKET"],
    )


@pytest.fixture
def catalog():
    dsn = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return PostgresCatalogResource(
        dsn=dsn,
        schema=os.environ["POSTGRES_SCHEMA"],
    )
