"""
End-to-end integration test for CAMS data transformation.

Tests the full chain: raw GRIB in MinIO → download → extract grids → insert to ClickHouse.
This test anchors "same inputs → same outputs" regardless of implementation changes.

Requires Docker infrastructure (MinIO, ClickHouse, Postgres).
Run with: uv run pytest -m integration
"""
import os
from pathlib import Path

import dagster as dg
from pipeline_python.defs.assets import transform_cams_data

FIXTURE_GRIB = Path(__file__).parent.parent / "fixtures" / "019c2817-c7a9-745a-8d4d-508b9983ae65.grib"
RUN_ID = "019c2817-c7a9-745a-8d4d-508b9983ae65"
DATASET = "cams-europe-air-quality-forecasts-forecast"
PARTITION = "2026-02-04"
RAW_KEY = f"ads/{DATASET}/{PARTITION}/{RUN_ID}.grib"


def test_transform_inserts_grid_data_to_clickhouse(s3_client, ch_client, storage, grid_store, catalog):
    """Raw GRIB in MinIO → download → extract → insert → rows in ClickHouse."""
    # Arrange: upload fixture GRIB to test bucket
    s3_client.upload_file(str(FIXTURE_GRIB), os.environ["MINIO_RAW_BUCKET"], RAW_KEY)

    # Arrange: fake upstream materialization so transform_cams_data can read
    # run_id and dataset from context.instance.get_latest_materialization_event()
    instance = dg.DagsterInstance.ephemeral()
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="ingest_cams_data",
            metadata={"run_id": RUN_ID, "dataset": DATASET},
            partition=PARTITION,
        )
    )

    context = dg.build_asset_context(
        instance=instance,
        partition_key=PARTITION,
    )

    # Act: run the actual asset with real test resources
    transform_cams_data(context, storage, catalog, grid_store)

    # Assert: expected variables are present
    result = ch_client.query(
        "SELECT variable, count() FROM jackfruit_test.grid_data FINAL GROUP BY variable"
    )
    rows = {r[0]: r[1] for r in result.result_rows}
    assert "pm2p5" in rows
    assert "pm10" in rows
