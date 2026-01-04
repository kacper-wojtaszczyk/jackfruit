"""
Ingestion assets for fetching and storing raw environmental data.

These assets orchestrate the Go ingestion container via Docker.

DEPRECATION NOTICE:
This Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import uuid
from datetime import datetime

import dagster as dg

from pipeline_python.defs.resources import DockerIngestionClient


class IngestionConfig(dg.Config):
    """Configuration for the ingestion asset."""

    date: str | None = None
    dataset: str = "cams-europe-air-quality-forecasts-forecast"  # CAMS Europe Air Quality forecast


@dg.asset
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
    ingestion_client: DockerIngestionClient,  # Resource looked up by param name
) -> dg.MaterializeResult:
    """
    Run the ingestion service to fetch and store raw CAMS data.

    This asset uses the DockerIngestionClient to run the Go ingestion CLI container.

    Data will be written to the jackfruit-raw bucket following the pattern:
        {source}/{dataset}/{YYYY-MM-DD}/{run_id}.grib

    Args:
        context: Dagster execution context
        config: Ingestion configuration (date, dataset)
        ingestion_client: Docker container client resource

    Returns:
        MaterializeResult with run metadata
    """
    # Generate UUIDv7 run-id for traceability
    run_id = str(uuid.uuid7())

    # Use today's date if not provided
    date = config.date if config.date else datetime.now().strftime("%Y-%m-%d")

    context.log.info(f"Starting ingestion: dataset={config.dataset}, date={date}, run_id={run_id}")

    # Delegate to the container client
    return ingestion_client.run_ingestion(
        context,
        dataset=config.dataset,
        date=date,
        run_id=run_id,
    )
