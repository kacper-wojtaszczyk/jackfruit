"""
Ingestion assets for fetching and storing raw environmental data.

These assets orchestrate the Go ingestion container via Docker.

DEPRECATION NOTICE:
This Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import tempfile
import uuid
from pathlib import Path

import dagster as dg

from pipeline_python.grib2io_patch import get_constituent_name  # noqa: F401  # Patch + helper
import grib2io

from pipeline_python.defs.partitions import daily_partitions
from pipeline_python.defs.resources import DockerIngestionClient, ObjectStorageResource


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
    date = context.partition_key

    context.log.info(f"Starting ingestion: dataset={config.dataset}, date={date}, run_id={run_id}")

    # Delegate to the container client
    result = ingestion_client.run_ingestion(
        context,
        dataset=config.dataset,
        date=date,
        run_id=run_id,
    )

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "dataset": config.dataset,
            "date": date,
        }
    )

# GRIB2 Table 4.230 constituent type codes we want to process
# Note: ECMWF/CAMS uses local codes (40xxx) that differ from WMO standard codes (62xxx)
# Reference: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml
CAMS_CONSTITUENT_CODES = {
    40008,  # PM10 (ECMWF local) -> "pm10"
    40009,  # PM2.5 (ECMWF local) -> "pm2p5"
    # Add more as needed (check ECMWF local codes for CAMS data):
    # WMO codes if you're using non-ECMWF data:
    # 62100,  # PM2.5 (WMO)
    # 62101,  # PM10 (WMO)
}

@dg.asset(
    partitions_def=daily_partitions,
    deps=[ingest_cams_data],
    kinds={"python"},
)
def transform_cams_data(
    context: dg.AssetExecutionContext,
    storage: ObjectStorageResource
) -> dg.MaterializeResult:
    """
    Transform raw CAMS data into curated single-variable, single-timestamp files.

    Reads multi-variable GRIB from raw bucket, splits by variable and hour,
    writes individual GRIB2 files to curated bucket.

    Output path pattern:
        curated/cams/europe-air-quality/{variable}/{year}/{month}/{day}/{hour}/data.grib2

    Args:
        context: Dagster execution context
        storage: S3/MinIO storage resource

    Returns:
        MaterializeResult with list of curated keys written
    """

    date = context.partition_key

    # Get metadata from upstream asset's latest materialization for this partition
    upstream_key = ingest_cams_data.key
    materialization_event = context.instance.get_latest_materialization_event(
        upstream_key
    )
    if materialization_event is None:
        raise dg.Failure(f"No materialization found for upstream asset {upstream_key} partition {context.partition_key}")

    ingest_metadata = materialization_event.asset_materialization.metadata
    context.log.info(f"Upstream metadata: {ingest_metadata}")
    run_id = ingest_metadata["run_id"].value
    dataset = ingest_metadata["dataset"].value

    raw_key = f"ads/{dataset}/{date}/{run_id}.grib"
    context.log.info(f"Processing raw file: {raw_key}")

    curated_keys = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Download raw file
        raw_path = tmpdir / "raw.grib"
        context.log.info(f"Downloading {raw_key} to {raw_path}")
        storage.download_raw(raw_key, raw_path)

        # Open with grib2io
        context.log.info("Opening GRIB file with grib2io")
        with grib2io.open(str(raw_path)) as grib_file:
            context.log.info(f"Found {grib_file.messages} messages")

            # Group messages by variable and time
            for msg in grib_file:
                # Get constituent type code from PDT 4.40
                constituent_code = msg.atmosphericChemicalConstituentType.value

                if constituent_code not in CAMS_CONSTITUENT_CODES:
                    context.log.debug(f"Skipping constituent code: {constituent_code}")
                    continue

                # Get variable name from constituent code
                var_name = get_constituent_name(constituent_code)

                # Use validDate (refDate + leadTime) for the actual forecast timestamp
                valid_date = msg.validDate  # datetime object
                year, month, day, hour = valid_date.year, valid_date.month, valid_date.day, valid_date.hour

                context.log.info(f"Processing {var_name} (code={constituent_code}) at {year}-{month:02d}-{day:02d} {hour:02d}:00")

                # Construct curated key
                curated_key = (
                    f"curated/cams/europe-air-quality/{var_name}/"
                    f"{year:04d}/{month:02d}/{day:02d}/{hour:02d}/data.grib2"
                )

                # Write single message to new GRIB2 file
                out_path = tmpdir / f"{var_name}_{hour:02d}.grib2"
                with grib2io.open(str(out_path), mode="w") as out_file:
                    # Copy the message and write it
                    out_file.write(msg)

                # Upload to curated bucket
                context.log.info(f"Uploading {curated_key}")
                storage.upload_curated(out_path, curated_key)
                curated_keys.append(curated_key)

    context.log.info(f"Wrote {len(curated_keys)} curated files")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "date": date,
            "curated_keys": curated_keys,
            "variables_processed": list(set(k.split("/")[3] for k in curated_keys)),
            "files_written": len(curated_keys),
        }
    )