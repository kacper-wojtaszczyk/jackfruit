"""
End-to-end integration test for ECMWF data transformation.

Tests the full chain: raw GRIB in MinIO -> download -> clip to Europe ->
convert K->C + compute RH -> insert to ClickHouse.

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
from pipeline_python.defs.assets import transform_ecmwf_data

FIXTURE_GRIB = Path(__file__).parent.parent / "fixtures" / "019cf6d7-02a0-745b-ac05-e1201d8f8a72.grib"
RUN_ID = "019cf6d7-02a0-745b-ac05-e1201d8f8a72"
SOURCE = "ecmwf"
DATASET = "ifs-weather-forecast"
PARTITION = "2026-03-16"
RAW_KEY = f"{SOURCE}/{DATASET}/{PARTITION}/{RUN_ID}.grib"

# After Europe clipping: 169 lats x 281 lons = 47,489 points per message
# 2 timestamps in fixture -> 94,978 per variable
POINTS_PER_MSG = 169 * 281  # 47,489
TIMESTAMPS = 2
VARS = 2
ROWS_PER_VAR = TIMESTAMPS * POINTS_PER_MSG  # 94,978
TOTAL_ROWS = VARS * ROWS_PER_VAR  # 189,956
CURATED_COUNT = VARS * TIMESTAMPS  # 4


def _report_upstream(instance: dg.DagsterInstance) -> None:
    """Register a fake ingest_ecmwf_data materialization so transform can find upstream metadata."""
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="ingest_ecmwf_data",
            metadata={"run_id": RUN_ID, "dataset": DATASET, "source": SOURCE},
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
        source=SOURCE,
        dataset=DATASET,
        date=datetime.date.fromisoformat(PARTITION),
        s3_key=RAW_KEY,
    ))


def test_transform_inserts_grid_data_to_clickhouse(s3_client, ch_client, object_store, grid_store, catalog):
    """Raw GRIB in MinIO -> download -> extract -> clip -> insert -> rows in ClickHouse."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    result = ch_client.query(
        "SELECT variable, count() FROM grid_data FINAL GROUP BY variable"
    )
    rows = {r[0]: r[1] for r in result.result_rows}
    assert len(rows) == 2
    assert "temperature" in rows
    assert "humidity" in rows
    assert rows["temperature"] == ROWS_PER_VAR
    assert rows["humidity"] == ROWS_PER_VAR


def test_transform_fails_without_upstream_materialization(object_store, catalog, grid_store):
    """Transform should raise dg.Failure when no upstream materialization event exists."""
    instance = dg.DagsterInstance.ephemeral()
    context = _make_context(instance)

    with pytest.raises(dg.Failure):
        transform_ecmwf_data(context, object_store, catalog, grid_store)


def test_transform_fails_on_missing_raw_file(object_store, catalog, grid_store):
    """Transform should raise dg.Failure when the GRIB is not in MinIO."""
    catalog.insert_raw_file(RawFileRecord(
        id=uuid.UUID(RUN_ID),
        source=SOURCE,
        dataset=DATASET,
        date=datetime.date.fromisoformat(PARTITION),
        s3_key=RAW_KEY,
    ))
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    context = _make_context(instance)

    with pytest.raises(dg.Failure, match="Failed to download"):
        transform_ecmwf_data(context, object_store, catalog, grid_store)


def test_transform_is_idempotent(s3_client, ch_client, object_store, grid_store, catalog):
    """Running transform twice should produce the same CH row count as once (FINAL deduplication)."""
    _arrange_raw_file(s3_client, catalog)

    def run_transform():
        instance = dg.DagsterInstance.ephemeral()
        _report_upstream(instance)
        transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    run_transform()
    run_transform()

    result = ch_client.query(
        "SELECT variable, count() FROM grid_data FINAL GROUP BY variable"
    )
    rows = {r[0]: r[1] for r in result.result_rows}
    assert rows["temperature"] == ROWS_PER_VAR
    assert rows["humidity"] == ROWS_PER_VAR


def test_curated_lineage_recorded_in_postgres(s3_client, object_store, grid_store, catalog, pg_connection):
    """Transform should insert one curated_data row per (variable, timestamp) into Postgres."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    rows = pg_connection.execute(
        "SELECT variable FROM catalog.curated_data ORDER BY variable"
    ).fetchall()
    variables = [r[0] for r in rows]
    assert len(variables) == CURATED_COUNT
    assert variables.count("temperature") == TIMESTAMPS
    assert variables.count("humidity") == TIMESTAMPS


def test_catalog_id_links_ch_and_pg(s3_client, ch_client, object_store, grid_store, catalog, pg_connection):
    """Every catalog_id written to ClickHouse must have a matching row in Postgres."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    ch_ids = {
        str(r[0])
        for r in ch_client.query("SELECT DISTINCT catalog_id FROM grid_data").result_rows
    }
    pg_ids = {
        r[0]
        for r in pg_connection.execute("SELECT id::text FROM catalog.curated_data").fetchall()
    }

    assert ch_ids == pg_ids
    assert len(ch_ids) == CURATED_COUNT


def test_transform_metadata_accuracy(s3_client, ch_client, object_store, grid_store, catalog):
    """MaterializeResult metadata should match what was actually written to ClickHouse."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    result = transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    assert result.metadata["run_id"] == RUN_ID
    assert result.metadata["date"] == PARTITION
    assert set(result.metadata["variables_processed"]) == {"temperature", "humidity"}
    assert result.metadata["inserted_rows"] == TOTAL_ROWS

    ch_total = ch_client.query("SELECT count() FROM grid_data").result_rows[0][0]
    assert ch_total == result.metadata["inserted_rows"]


def test_temperature_is_celsius(s3_client, ch_client, object_store, grid_store, catalog):
    """Temperature values should be in Celsius range, not Kelvin."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    result = ch_client.query(
        "SELECT min(value), max(value) FROM grid_data FINAL WHERE variable = 'temperature'"
    )
    min_val, max_val = result.result_rows[0]
    # Celsius: roughly -65 to +55; Kelvin would be > 150
    assert min_val > -80
    assert max_val < 60


def test_humidity_is_percentage(s3_client, ch_client, object_store, grid_store, catalog):
    """Humidity values should be 0-~105%."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    result = ch_client.query(
        "SELECT min(value), max(value) FROM grid_data FINAL WHERE variable = 'humidity'"
    )
    min_val, max_val = result.result_rows[0]
    assert min_val >= 0
    assert max_val <= 105


def test_grid_clipped_to_europe(s3_client, ch_client, object_store, grid_store, catalog):
    """All lat/lon values in ClickHouse should be within the European bounding box."""
    _arrange_raw_file(s3_client, catalog)
    instance = dg.DagsterInstance.ephemeral()
    _report_upstream(instance)
    transform_ecmwf_data(_make_context(instance), object_store, catalog, grid_store)

    result = ch_client.query(
        "SELECT min(lat), max(lat), min(lon), max(lon) FROM grid_data FINAL"
    )
    min_lat, max_lat, min_lon, max_lon = result.result_rows[0]
    assert min_lat == pytest.approx(30.0, abs=0.3)
    assert max_lat == pytest.approx(72.0, abs=0.3)
    assert min_lon == pytest.approx(-25.0, abs=0.3)
    assert max_lon == pytest.approx(45.0, abs=0.3)
