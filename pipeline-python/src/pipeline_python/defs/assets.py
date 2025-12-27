import uuid
from datetime import datetime

import dagster as dg


class IngestionConfig(dg.Config):
    """Configuration for the ingestion asset."""
    date: str | None = None
    dataset: str = "cams-europe-air-quality-forecasts-analysis"


@dg.asset
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig
) -> dg.MaterializeResult:
    """
    PLACEHOLDER Run the ingestion service via docker compose to fetch and store raw data.

    This asset will spawn the Go ingestion CLI container, passing date, dataset,
    and a generated UUIDv7 run-id. Data will be written to the jackfruit-raw bucket.
    """
    # Generate UUIDv7 run-id
    run_id = str(uuid.uuid7())

    # Use today's date if not provided
    date = config.date if config.date else datetime.now().strftime("%Y-%m-%d")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "dataset": config.dataset,
            "date": date,
            "exit_code": 0
        }
    )
