"""
End-to-end integration test for CAMS data transformation.

Tests the full chain: raw GRIB in MinIO → download → extract grids → insert to ClickHouse.
This test anchors "same inputs → same outputs" regardless of implementation changes.

Requires Docker infrastructure (MinIO, ClickHouse, Postgres).
Run with: uv run pytest -m integration
"""
import datetime
import os
import uuid
from pathlib import Path

import dagster as dg
import pytest

from pipeline_python.defs import RawFileRecord
from pipeline_python.defs.assets import transform_cams_data

FIXTURE_GRIB = Path(__file__).parent.parent / "fixtures" / "019c7f73-419f-727c-8e56-95880501e36b.grib"
RUN_ID = "019c7f73-419f-727c-8e56-95880501e36b"
DATASET = "cams-europe-air-quality-forecasts-forecast"
PARTITION = "2026-02-21"
RAW_KEY = f"ads/{DATASET}/{PARTITION}/{RUN_ID}.grib"


def _report_upstream(instance: dg.DagsterInstance) -> None:
    """Register a fake ingest_cams_data materialization so transform can find upstream metadata."""
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="ingest_cams_data",
            metadata={"run_id": RUN_ID, "dataset": DATASET},
            partition=PARTITION,
        )
    )


def _make_context(instance: dg.DagsterInstance) -> dg.AssetExecutionContext:
    return dg.build_asset_context(instance=instance, partition_key=PARTITION)


def _arrange_raw_file(s3_client, catalog) -> None:
    """Upload fixture GRIB to MinIO and record it in the catalog."""
    s3_client.upload_file(str(FIXTURE_GRIB), os.environ["MINIO_RAW_BUCKET"], RAW_KEY)
    catalog.insert_raw_file(RawFileRecord(
        id=uuid.UUID(RUN_ID),
        source="ads",
        dataset=DATASET,
        date=datetime.date.fromisoformat(PARTITION),
        s3_key=RAW_KEY,
    ))

# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_transform_inserts_grid_data_to_clickhouse(s3_client, ch_client, storage, grid_store, catalog):
    """Raw GRIB in MinIO → download → extract → insert → rows in ClickHouse."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_cams_data(_make_context(instance), storage, catalog, grid_store)

    result = ch_client.query(
        "SELECT variable, count() FROM jackfruit_test.grid_data FINAL GROUP BY variable"
    )
    rows = {r[0]: r[1] for r in result.result_rows}
    assert len(rows) == 2
    assert "pm2p5" in rows
    assert "pm10" in rows
    assert rows["pm2p5"] == 1176000  # 420x700 grid × 4 timestamps in the grib file
    assert rows["pm10"] == 1176000


# ---------------------------------------------------------------------------
# Unhappy paths
# ---------------------------------------------------------------------------

def test_transform_fails_without_upstream_materialization(storage, catalog, grid_store):
    """Transform should raise dg.Failure when no upstream materialization event exists."""
    instance = dg.DagsterInstance.ephemeral()
    context = _make_context(instance)

    with pytest.raises(dg.Failure):
        transform_cams_data(context, storage, catalog, grid_store)


def test_transform_fails_on_missing_raw_file(storage, catalog, grid_store):
    """Transform should raise dg.Failure when the GRIB is not in MinIO."""
    catalog.insert_raw_file(RawFileRecord(
        id=uuid.UUID(RUN_ID),
        source="ads",
        dataset=DATASET,
        date=datetime.date.fromisoformat(PARTITION),
        s3_key=RAW_KEY,
    ))
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    context = _make_context(instance)

    with pytest.raises(dg.Failure, match="Failed to download"):
        transform_cams_data(context, storage, catalog, grid_store)


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_transform_is_idempotent(s3_client, ch_client, storage, grid_store, catalog):
    """Running transform twice should produce the same CH row count as once (FINAL deduplication)."""
    _arrange_raw_file(s3_client, catalog)

    def run_transform():
        instance = dg.DagsterInstance.ephemeral()
        _report_upstream(instance)
        transform_cams_data(_make_context(instance), storage, catalog, grid_store)

    run_transform()
    run_transform()

    result = ch_client.query(
        "SELECT variable, count() FROM jackfruit_test.grid_data FINAL GROUP BY variable"
    )
    rows = {r[0]: r[1] for r in result.result_rows}
    assert rows["pm2p5"] == 1176000
    assert rows["pm10"] == 1176000


# ---------------------------------------------------------------------------
# Catalog / lineage
# ---------------------------------------------------------------------------

def test_curated_lineage_recorded_in_postgres(s3_client, storage, grid_store, catalog, pg_connection):
    """Transform should insert one curated_data row per GRIB message into Postgres."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_cams_data(_make_context(instance), storage, catalog, grid_store)

    rows = pg_connection.execute(
        "SELECT variable FROM test_catalog.curated_data ORDER BY variable"
    ).fetchall()
    variables = [r[0] for r in rows]
    assert len(variables) == 8  # 2 variables × 4 timestamps
    assert variables.count("pm10") == 4
    assert variables.count("pm2p5") == 4


def test_catalog_id_links_ch_and_pg(s3_client, ch_client, storage, grid_store, catalog, pg_connection):
    """Every catalog_id written to ClickHouse must have a matching row in Postgres."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_cams_data(_make_context(instance), storage, catalog, grid_store)

    ch_ids = {
        str(r[0])
        for r in ch_client.query("SELECT DISTINCT catalog_id FROM jackfruit_test.grid_data").result_rows
    }
    pg_ids = {
        r[0]
        for r in pg_connection.execute("SELECT id::text FROM test_catalog.curated_data").fetchall()
    }

    assert ch_ids == pg_ids
    assert len(ch_ids) == 8  # 2 variables × 4 timestamps


# ---------------------------------------------------------------------------
# Metadata accuracy
# ---------------------------------------------------------------------------

def test_transform_metadata_accuracy(s3_client, ch_client, storage, grid_store, catalog):
    """MaterializeResult metadata should match what was actually written to ClickHouse."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    result = transform_cams_data(_make_context(instance), storage, catalog, grid_store)

    assert result.metadata["run_id"] == RUN_ID
    assert result.metadata["date"] == PARTITION
    assert set(result.metadata["variables_processed"]) == {"pm2p5", "pm10"}
    assert result.metadata["inserted_rows"] == 1176000 * 2

    ch_total = ch_client.query("SELECT count() FROM jackfruit_test.grid_data").result_rows[0][0]
    assert ch_total == result.metadata["inserted_rows"]
