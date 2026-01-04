"""
Ingestion assets for fetching and storing raw environmental data.

These assets orchestrate the Go ingestion container via Docker (local) or ECS (AWS).
"""
import uuid
from datetime import datetime

import dagster as dg

from pipeline_python.defs.resources import DockerIngestionClient


class IngestionConfig(dg.Config):
    """Configuration for the ingestion asset."""

    date: str | None = None
    dataset: str = "cams-europe-air-quality-forecasts-forecast"


@dg.asset
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
    ingestion_client: DockerIngestionClient,  # Resource looked up by param name
) -> dg.MaterializeResult:
    """
    Run the ingestion service to fetch and store raw CAMS data.

    This asset uses the injected IngestionContainerClient to run the Go ingestion
    CLI container. The client implementation determines whether this runs via
    Docker (local dev) or ECS (AWS prod).

    Data will be written to the jackfruit-raw bucket following the pattern:
        {source}/{dataset}/{variable}/ingest_date=YYYY-MM-DD/{filename}

    Args:
        context: Dagster execution context
        config: Ingestion configuration (date, dataset)
        ingestion_client: Container client resource (Docker or ECS)

    Returns:
        MaterializeResult with run metadata
    """
    # Generate UUIDv7 run-id for traceability
    run_id = str(uuid.uuid7())

    # Use today's date if not provided
    date = config.date if config.date else datetime.now().strftime("%Y-%m-%d")

    context.log.info(f"Starting ingestion: dataset={config.dataset}, date={date}, run_id={run_id}")

    # Delegate to the container client (Docker or ECS)
    # TODO: Implement the client methods - they should return MaterializeResult
    return ingestion_client.run_ingestion(
        context,
        dataset=config.dataset,
        date=date,
        run_id=run_id,
    )
