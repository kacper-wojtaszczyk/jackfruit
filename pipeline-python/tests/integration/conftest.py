"""
Integration test infrastructure fixtures.

Loads .env.test before any imports so all resources pick up test config.
Auto-marks tests in this directory as 'integration'

Uses production databases/schemas (created by docker-compose init scripts).
Tables are truncated before each test — no separate test namespaces.
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
from pipeline_python.storage.clickhouse_grid_store import ClickHouseGridStore

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
    """Session-scoped ClickHouse connection for cleanup."""
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        database=os.environ["CLICKHOUSE_DB"],
    )
    yield client
    client.close()


@pytest.fixture(scope="session")
def pg_connection():
    """Session-scoped Postgres connection for cleanup."""
    dsn = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    conn = psycopg.connect(dsn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3_client():
    """Session-scoped MinIO/S3 client for cleanup."""
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        use_ssl=os.environ.get("MINIO_USE_SSL", "false").lower() in {"true", "1", "yes", "on"},
    )
    yield client


# ---------------------------------------------------------------------------
# Per-test cleanup fixtures (autouse)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_ch(ch_client):
    """Truncate CH grid_data before each test."""
    ch_client.command("TRUNCATE TABLE grid_data")
    yield


@pytest.fixture(autouse=True)
def clean_pg(pg_connection):
    """Truncate Postgres catalog tables before each test."""
    pg_connection.execute("TRUNCATE catalog.curated_data, catalog.raw_files CASCADE")
    pg_connection.commit()
    yield


@pytest.fixture(autouse=True)
def clean_minio(s3_client):
    """Empty MinIO raw bucket before each test."""
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
        port=int(os.environ["CLICKHOUSE_PORT"]),
    )


@pytest.fixture
def storage():
    return ObjectStorageResource(
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        raw_bucket=os.environ["MINIO_RAW_BUCKET"],
        use_ssl=os.environ.get("MINIO_USE_SSL", "false").lower() in {"true", "1", "yes", "on"},
    )


@pytest.fixture
def catalog():
    dsn = (
        f"postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
    )
    return PostgresCatalogResource(
        dsn=dsn,
    )
