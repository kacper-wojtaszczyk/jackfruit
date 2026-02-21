"""
Ingestion assets for fetching and storing raw environmental data.

These assets orchestrate the Go ingestion container via Docker.

DEPRECATION NOTICE:
This Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import tempfile
import uuid
from datetime import date
from pathlib import Path
from uuid import UUID

import dagster as dg

from pipeline_python.grib2 import grib2io, get_shortname

from pipeline_python.defs.partitions import daily_partitions
from pipeline_python.defs.resources import DockerIngestionClient, ObjectStorageResource, PostgresCatalogResource
from pipeline_python.defs.models import RawFileRecord, CuratedDataRecord
from pipeline_python.storage import GridStore
from pipeline_python.storage.grid_store import GridData


class IngestionConfig(dg.Config):
    """Configuration for the ingestion asset."""

    date: str | None = None
    dataset: str = "cams-europe-air-quality-forecasts-forecast"  # CAMS Europe Air Quality forecast


@dg.asset(
    partitions_def=daily_partitions,
    kinds={"go", "docker"},
)
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
    ingestion_client: DockerIngestionClient,
    catalog: PostgresCatalogResource,
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
        catalog: Postgres catalog resource for metadata tracking

    Returns:
        MaterializeResult with run metadata
    """
    run_id = str(uuid.uuid7())

    partition_date = context.partition_key

    context.log.info(f"Starting ingestion: dataset={config.dataset}, date={partition_date}, run_id={run_id}")

    result = ingestion_client.run_ingestion(
        context,
        dataset=config.dataset,
        date=partition_date,
        run_id=run_id,
    )

    # Record raw file in catalog
    s3_key = f"ads/{config.dataset}/{partition_date}/{run_id}.grib"
    raw_record = RawFileRecord(
        id=uuid.UUID(run_id),
        source="ads",
        dataset=config.dataset,
        date=date.fromisoformat(partition_date),
        s3_key=s3_key,
    )
    try:
        catalog.insert_raw_file(raw_record)
        context.log.info(f"Recorded raw file in catalog: {s3_key}")
    except Exception as e:
        context.log.warning(f"Failed to record raw file in catalog: {e}")

    return result

@dg.asset(
    partitions_def=daily_partitions,
    deps=[ingest_cams_data],
    kinds={"python"},
)
def transform_cams_data(
    context: dg.AssetExecutionContext,
    storage: ObjectStorageResource,
    catalog: PostgresCatalogResource,
    grid_store: GridStore,
) -> dg.MaterializeResult:
    """
    Transform raw CAMS GRIB data into curated grid rows in ClickHouse.

    Reads upstream ingestion metadata to locate the raw file, downloads it from MinIO,
    extracts grid data per GRIB message, inserts rows into ClickHouse, and records
    lineage in the Postgres catalog.

    Args:
        context: Dagster execution context (provides partition key and upstream metadata)
        storage: S3/MinIO client for downloading raw files
        catalog: Postgres catalog for lineage recording
        grid_store: Grid storage backend (ClickHouse in production)

    Returns:
        MaterializeResult with run_id, date, variables_processed, and inserted_rows
    """
    partition_date = context.partition_key
    upstream_key = ingest_cams_data.key
    materialization_event = context.instance.get_latest_materialization_event(
        upstream_key
    )
    if materialization_event is None:
        raise dg.Failure(
            f"No materialization found for upstream asset {upstream_key} partition {context.partition_key}. "
            f"Please ensure the ingestion asset has been materialized for this partition before running transformation."
        )

    ingest_metadata = materialization_event.asset_materialization.metadata
    context.log.info(f"Upstream metadata: {ingest_metadata}")
    run_id = ingest_metadata["run_id"].value
    dataset = ingest_metadata["dataset"].value
    raw_key = f"ads/{dataset}/{partition_date}/{run_id}.grib"
    context.log.info(f"Processing {raw_key}")
    curated_keys: list[UUID] = []
    variables_processed: list[str] = []
    rows_inserted: int = 0
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)
        tmp_raw_path = tmp_dir / "raw.grib"
        try:
            storage.download_raw(raw_key, tmp_raw_path)
        except Exception as e:
            raise dg.Failure(f"Failed to download {raw_key}: {e}")
        with grib2io.open(str(tmp_raw_path)) as grib_file:
            for message in grib_file:
                catalog_id = uuid.uuid7()
                constituent_code = message.atmosphericChemicalConstituentType.value
                values = message.data
                unit = message.units
                if unit == "kg m-3":
                    values = values * 1e9
                    unit = "µg/m³"
                variable_name = get_shortname(constituent_code)
                timestamp = message.refDate + message.leadTime
                rows_inserted += grid_store.insert_grid(GridData(
                    variable=variable_name,
                    unit=unit,
                    timestamp=timestamp,
                    lats=message.lats,
                    lons=message.lons,
                    values=values,
                    catalog_id=catalog_id,
                ))
                catalog.insert_curated_data(CuratedDataRecord(
                    id=catalog_id,
                    raw_file_id=uuid.UUID(run_id),
                    variable=variable_name,
                    unit=unit,
                    timestamp=timestamp,
                ))
                curated_keys.append(catalog_id)
                variables_processed.append(variable_name)

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "date": partition_date,
            "curated_keys": [str(key) for key in curated_keys],
            "variables_processed": list(set(variables_processed)),
            "inserted_rows": rows_inserted,
        }
    )
