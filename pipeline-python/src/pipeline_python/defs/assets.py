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
from pipeline_python.defs.models import RawFileRecord, CuratedFileRecord


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


def _write_curated_grib(
    msg: Any,
    metadata: dict,
    tmpdir: Path,
    storage: ObjectStorageResource,
    context: dg.AssetExecutionContext,
    catalog: PostgresCatalogResource,
    raw_file_id: uuid.UUID,
) -> str | None:
    """
    Write a single GRIB message to a curated file and upload to storage.
    
    Args:
        msg: GRIB message to write (grib2io message type)
        metadata: Message metadata from _extract_message_metadata
        tmpdir: Temporary directory for writing files
        storage: Storage resource for uploading
        context: Dagster execution context for logging
        catalog: Catalog resource for metadata tracking
        raw_file_id: Raw file UUID for lineage tracking

    Returns:
        Curated key if successful, None otherwise
    """
    var_name = metadata["var_name"]
    year = metadata["year"]
    month = metadata["month"]
    day = metadata["day"]
    hour = metadata["hour"]
    constituent_code = metadata["constituent_code"]
    
    context.log.info(
        f"Processing {var_name} (code={constituent_code}) at {year}-{month:02d}-{day:02d} {hour:02d}:00"
    )

    # Construct curated key (no prefix - bucket is already jackfruit-curated)
    curated_key = (
        f"{var_name}/cams/"
        f"{year:04d}/{month:02d}/{day:02d}/{hour:02d}/data.grib2"
    )

    # Write single message to new GRIB2 file
    out_path = tmpdir / f"{var_name}_{year}{month:02d}{day:02d}_{hour:02d}.grib2"
    try:
        with grib2io.open(str(out_path), mode="w") as out_file:
            # Copy the message and write it
            out_file.write(msg)
    except Exception as e:
        context.log.error(
            f"Failed to write GRIB message for {var_name} at hour {hour}: {e}. Skipping."
        )
        return None

    # Upload to curated bucket
    context.log.info(f"Uploading {curated_key}")
    try:
        storage.upload_curated(out_path, curated_key)
    except Exception as e:
        context.log.error(
            f"Failed to upload {curated_key} to storage: {e}. Skipping."
        )
        return None

    # Record curated file in catalog
    curated_record = CuratedFileRecord(
        id=uuid.uuid7(),
        raw_file_id=raw_file_id,
        variable=var_name,
        source="cams",
        timestamp=datetime(year, month, day, hour),
        s3_key=curated_key,
    )
    try:
        catalog.insert_curated_file(curated_record)
        context.log.debug(f"Recorded curated file in catalog: {curated_key}")
    except Exception as e:
        context.log.warning(f"Failed to record curated file in catalog: {e}")

    return curated_key


@dg.asset(
    partitions_def=daily_partitions,
    deps=[ingest_cams_data],
    kinds={"python"},
)
def transform_cams_data(
    context: dg.AssetExecutionContext,
    storage: ObjectStorageResource,
    catalog: PostgresCatalogResource,
) -> dg.MaterializeResult:
    """
    Transform raw CAMS data into curated single-variable, single-timestamp files.

    Reads multi-variable GRIB from raw bucket, splits by variable and hour,
    writes individual GRIB2 files to curated bucket.

    Output path pattern (in jackfruit-curated bucket):
        {variable}/cams/{year}/{month}/{day}/{hour}/data.grib2

    Args:
        context: Dagster execution context
        storage: S3/MinIO storage resource
        catalog: Postgres catalog resource for metadata tracking

    Returns:
        MaterializeResult with list of curated keys written. If no valid GRIB
        messages are found, returns successfully with an empty curated_keys list
        and emits a warning.
    
    Raises:
        dg.Failure: If upstream materialization not found, raw file download fails,
                    or GRIB file is invalid
    """

    partition_date = context.partition_key

    # Get metadata from upstream asset's latest materialization for this partition
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
    context.log.info(f"Processing raw file: {raw_key}")

    curated_keys = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Download raw file
        raw_path = tmpdir / "raw.grib"
        context.log.info(f"Downloading {raw_key} to {raw_path}")
        try:
            storage.download_raw(raw_key, raw_path)
        except Exception as e:
            raise dg.Failure(
                f"Failed to download raw file {raw_key} from storage: {e}. "
                f"Ensure the file exists and storage credentials are correct."
            )
        
        # Verify file was downloaded
        if not raw_path.exists():
            raise dg.Failure(f"Downloaded file {raw_path} does not exist after download")
        
        file_size = raw_path.stat().st_size
        if file_size == 0:
            raise dg.Failure(f"Downloaded file {raw_path} is empty (0 bytes)")
        
        context.log.info(f"Downloaded {file_size / 1024:.1f} KB")

        # Open with grib2io
        context.log.info("Opening GRIB file with grib2io")
        num_messages = 0  # Initialize outside the context to avoid NameError
        try:
            grib_file = grib2io.open(str(raw_path))
        except Exception as e:
            raise dg.Failure(
                f"Failed to open GRIB file {raw_path}: {e}. "
                f"The file may be corrupted or not a valid GRIB2 file."
            )
        
        with grib_file:
            num_messages = grib_file.messages
            context.log.info(f"Found {num_messages} messages")
            
            if num_messages == 0:
                context.log.warning(f"GRIB file {raw_key} contains no messages")
                # Return early with empty result
                return dg.MaterializeResult(
                    metadata={
                        "run_id": run_id,
                        "date": partition_date,
                        "curated_keys": [],
                        "variables_processed": [],
                        "files_written": 0,
                        "warning": "No messages in GRIB file",
                    }
                )

            # Use catalog context manager to reuse connection across all inserts
            with catalog:
                # Group messages by variable and time
                for msg in grib_file:
                    # Extract message metadata
                    metadata = _extract_message_metadata(msg, context)
                    if metadata is None:
                        continue

                    # Write and upload curated file
                    curated_key = _write_curated_grib(
                        msg, metadata, tmpdir, storage, context,
                        catalog=catalog, raw_file_id=raw_file_id,
                    )
                    if curated_key is not None:
                        curated_keys.append(curated_key)

    context.log.info(f"Wrote {len(curated_keys)} curated files")
    
    # Warn if no files were written despite having messages
    if len(curated_keys) == 0 and num_messages > 0:
        context.log.warning(
            f"No curated files were written despite {num_messages} messages in the GRIB file. "
            f"This may indicate that no messages matched the configured CAMS constituent codes: {_CAMS_CONSTITUENT_CODES}"
        )

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
