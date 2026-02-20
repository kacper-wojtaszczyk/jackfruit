"""
Ingestion assets for fetching and storing raw environmental data.

These assets orchestrate the Go ingestion container via Docker.

DEPRECATION NOTICE:
This Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dagster as dg

from pipeline_python.grib2 import grib2io, get_shortname

from pipeline_python.defs.partitions import daily_partitions
from pipeline_python.defs.resources import DockerIngestionClient, ObjectStorageResource, PostgresCatalogResource
from pipeline_python.defs.models import RawFileRecord, CuratedDataRecord
from pipeline_python.storage import GridStore


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

# GRIB2 Table 4.230 constituent type codes we want to process
# Note: ECMWF/CAMS uses local codes (40xxx) that differ from WMO standard codes (62xxx)
# Reference: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml
_CAMS_CONSTITUENT_CODES = {
    40008,  # PM10 (ECMWF local) -> "pm10"
    40009,  # PM2.5 (ECMWF local) -> "pm2p5"
    # Add more as needed (check ECMWF local codes for CAMS data):
    # WMO codes if you're using non-ECMWF data:
    # 62100,  # PM2.5 (WMO)
    # 62101,  # PM10 (WMO)
}


def _extract_message_metadata(msg: Any, context: dg.AssetExecutionContext) -> dict | None:
    """
    Extract metadata from a GRIB message.
    
    Args:
        msg: GRIB message object (grib2io message type)
        context: Dagster execution context for logging
    
    Returns:
        Dictionary with constituent_code, var_name, year, month, day, hour
        or None if the message should be skipped
    """
    # Get constituent type code from PDT 4.40
    try:
        constituent_code = msg.atmosphericChemicalConstituentType.value
    except AttributeError:
        context.log.warning(
            f"Message does not have atmosphericChemicalConstituentType attribute. "
            f"This may not be a PDT 4.40 message. Skipping."
        )
        return None

    if constituent_code not in _CAMS_CONSTITUENT_CODES:
        context.log.debug(f"Skipping constituent code: {constituent_code}")
        return None

    # Get variable name from constituent code
    var_name = get_shortname(constituent_code)

    # Use validDate (refDate + leadTime) for the actual forecast timestamp
    try:
        valid_date = msg.validDate  # datetime object
        year, month, day, hour = valid_date.year, valid_date.month, valid_date.day, valid_date.hour
    except Exception as e:
        context.log.warning(
            f"Failed to extract validDate from message for constituent {constituent_code}: {e}. Skipping."
        )
        return None
    
    return {
        "constituent_code": constituent_code,
        "var_name": var_name,
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
    }



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
    raw_file_id = uuid.UUID(run_id)
    raw_key = f"ads/{dataset}/{partition_date}/{run_id}.grib"
    context.log.info(f"Processing {raw_key}")
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)
        tmp_raw_path = tmp_dir / "raw.grib"
        try:
            storage.download_raw(raw_key, tmp_raw_path)
        except Exception as e:
            raise dg.Failure(f"Failed to download {raw_key}: {e}")
        with grib2io.open(tmp_raw_path) as grib_file:
            for message in grib_file:
                constituent_code = message.atmosphericChemicalConstituentType.value



    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "date": partition_date,
            "curated_keys": curated_keys,
            "variables_processed": list(set(k.split("/")[0] for k in curated_keys)) if curated_keys else [],
            "files_written": len(curated_keys),
            "total_messages": num_messages,
        }
    )
