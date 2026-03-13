"""
Dagster assets for CAMS environmental data ingestion and transformation.

Ingestion fetches raw GRIB data from the Copernicus ADS API via cdsapi
and stores it in MinIO. Transformation decodes the GRIB, extracts grids,
and writes curated rows to ClickHouse.
"""
import tempfile
import uuid
from datetime import date
from pathlib import Path
from uuid import UUID

import dagster as dg

from pipeline_python.defs.partitions import daily_partitions
from pipeline_python.defs.resources import PostgresCatalogResource
from pipeline_python.ingestion import CdsClient
from pipeline_python.storage import ObjectStore, GridStore
from pipeline_python.storage.grid_store import GridData
from pipeline_python.defs.models import RawFileRecord, CuratedDataRecord
from pipeline_python.grib2 import CamsReader

_AIR_QUALITY_FORECAST = "cams-europe-air-quality-forecast"


class CamsForecastConfig(dg.Config):
    """Configuration for the ingestion asset."""
    horizon_hours: int = 48

@dg.asset(
    partitions_def=daily_partitions,
    kinds={"python", "ingest"},
)
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: CamsForecastConfig,
    cds_client: CdsClient,
    object_store: ObjectStore,
    catalog: PostgresCatalogResource,
) -> dg.MaterializeResult:
    """
    Fetch raw CAMS forecast data from Copernicus ADS and store in MinIO.

    Downloads a GRIB file via cdsapi, uploads it to the raw bucket at
    ads/{dataset}/{YYYY-MM-DD}/{run_id}.grib, and records lineage in Postgres.

    Args:
        context: Dagster execution context
        config: Forecast-specific configuration (horizon_hours)
        cds_client: API client to retrieve the data from Copernicus
        object_store: S3 object storage client
        catalog: Postgres catalog resource for metadata tracking

    Returns:
        MaterializeResult with run metadata
    """
    source: str = "ads"
    run_id = str(uuid.uuid7())
    partition_date = date.fromisoformat(context.partition_key)

    context.log.info(
        f"Starting ingestion: source={source}, dataset={_AIR_QUALITY_FORECAST}, date={partition_date}, run_id={run_id}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "cams.grib"
        cds_client.retrieve_forecast(
            forecast_date=partition_date,
            variables=["pm2p5", "pm10"],
            target=tmp_path,
            max_leadtime_hours=config.horizon_hours,
        )
        context.log.info(f"Downloaded CAMS data ({tmp_path.stat().st_size} bytes)")
        s3_key = f"{source}/{_AIR_QUALITY_FORECAST}/{partition_date}/{run_id}.grib"
        object_store.upload_raw(s3_key, tmp_path)
        context.log.info(f"Uploaded to {s3_key}")

    raw_record = RawFileRecord(
        id=uuid.UUID(run_id),
        source=source,
        dataset=_AIR_QUALITY_FORECAST,
        date=partition_date,
        s3_key=s3_key,
    )
    try:
        catalog.insert_raw_file(raw_record)
        context.log.info(f"Recorded raw file in catalog: {s3_key}")
    except Exception as e:
        context.log.warning(f"Failed to record raw file in catalog: {e}")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "source": source,
            "dataset": _AIR_QUALITY_FORECAST,
            "date": partition_date.isoformat(),
        }
    )

@dg.asset(
    partitions_def=daily_partitions,
    deps=[ingest_cams_data],
    kinds={"python", "transform"},
)
def transform_cams_data(
    context: dg.AssetExecutionContext,
    object_store: ObjectStore,
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
        object_store: S3/MinIO client for downloading raw files
        catalog: Postgres catalog for lineage recording
        grid_store: Grid storage backend (ClickHouse in production)

    Returns:
        MaterializeResult with run_id, date, variables_processed, and inserted_rows
    """
    partition_date = context.partition_key
    upstream_key = ingest_cams_data.key
    records = context.instance.get_event_records(
        event_records_filter=dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=upstream_key,
            asset_partitions=[partition_date],
        ),
        limit=1,
    )
    if not records:
        raise dg.Failure(
            f"No materialization found for upstream asset {upstream_key} partition {context.partition_key}. "
            f"Please ensure the ingestion asset has been materialized for this partition before running transformation."
        )

    ingest_metadata = records[0].asset_materialization.metadata
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
            object_store.download_raw(raw_key, tmp_raw_path)
        except Exception as e:
            raise dg.Failure(f"Failed to download {raw_key}: {e}")
        reader = CamsReader()
        with reader.open(str(tmp_raw_path)) as messages:
            for message in messages:
                catalog_id = uuid.uuid7()
                values = message.values
                unit = message.unit
                if unit == "kg m-3":
                    values = values * 1e9
                    unit = "µg/m³"
                rows_inserted += grid_store.insert_grid(GridData(
                    variable=message.variable_name,
                    unit=unit,
                    timestamp=message.timestamp,
                    lats=message.lats,
                    lons=message.lons,
                    values=values,
                    catalog_id=catalog_id,
                ))
                catalog.insert_curated_data(CuratedDataRecord(
                    id=catalog_id,
                    raw_file_id=uuid.UUID(run_id),
                    variable=message.variable_name,
                    unit=unit,
                    timestamp=message.timestamp,
                ))
                curated_keys.append(catalog_id)
                variables_processed.append(message.variable_name)

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "date": partition_date,
            "curated_keys": [str(key) for key in curated_keys],
            "variables_processed": list(set(variables_processed)),
            "inserted_rows": rows_inserted,
        }
    )
