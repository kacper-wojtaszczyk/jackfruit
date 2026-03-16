"""
Dagster assets for environmental data ingestion and transformation.

Ingestion fetches raw GRIB data from external APIs (Copernicus ADS via cdsapi,
ECMWF Open Data) and stores it in MinIO. Transformation decodes the GRIB,
extracts grids, and writes curated rows to ClickHouse.
"""
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

import dagster as dg
import numpy as np

from pipeline_python.defs.partitions import daily_partitions
from pipeline_python.defs.resources import PostgresCatalogResource
from pipeline_python.ingestion import CdsClient, EcmwfClient
from pipeline_python.storage import ObjectStore, GridStore
from pipeline_python.storage.grid_store import GridData
from pipeline_python.defs.models import RawFileRecord, CuratedDataRecord
from pipeline_python.grib2 import CamsReader, EcmwfReader, GribMessage

_ADS_SOURCE = "ads"
_ECMWF_SOURCE = "ecmwf"

_AIR_QUALITY_FORECAST = "cams-europe-air-quality-forecast"
_WEATHER_FORECAST = "ifs-weather-forecast"

# European bounding box — matches CAMS Europe domain
_EUROPE_LAT_MIN, _EUROPE_LAT_MAX = 30.0, 72.0
_EUROPE_LON_MIN, _EUROPE_LON_MAX = -25.0, 45.0


def _clip_to_europe(
    values: np.ndarray, lats: np.ndarray, lons: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Clip 2D grids to the European bounding box.

    ECMWF Open Data returns global 0.25° grids (721x1440). This clips to
    Europe (lat [30, 72], lon [-25, 45]) and reshapes to 2D (169x281) for
    GridData compatibility.
    """
    mask = (
        (lats >= _EUROPE_LAT_MIN) & (lats <= _EUROPE_LAT_MAX)
        & (lons >= _EUROPE_LON_MIN) & (lons <= _EUROPE_LON_MAX)
    )
    n_lats = int((_EUROPE_LAT_MAX - _EUROPE_LAT_MIN) / 0.25) + 1  # 169
    n_lons = int((_EUROPE_LON_MAX - _EUROPE_LON_MIN) / 0.25) + 1  # 281
    return (
        values[mask].reshape(n_lats, n_lons),
        lats[mask].reshape(n_lats, n_lons),
        lons[mask].reshape(n_lats, n_lons),
    )


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
    run_id = str(uuid.uuid7())
    partition_date = date.fromisoformat(context.partition_key)

    context.log.info(f"Starting ingestion: source={_ADS_SOURCE}, dataset={_AIR_QUALITY_FORECAST}, date={partition_date}, run_id={run_id}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "cams.grib"
        cds_client.retrieve_forecast(
            forecast_date=partition_date,
            variables=["pm2p5", "pm10"],
            target=tmp_path,
            max_leadtime_hours=config.horizon_hours,
        )
        context.log.info(f"Downloaded CAMS data ({tmp_path.stat().st_size} bytes)")
        s3_key = f"{_ADS_SOURCE}/{_AIR_QUALITY_FORECAST}/{partition_date}/{run_id}.grib"
        object_store.upload_raw(s3_key, tmp_path)
        context.log.info(f"Uploaded to {s3_key}")

    raw_record = RawFileRecord(
        id=uuid.UUID(run_id),
        source=_ADS_SOURCE,
        dataset=_AIR_QUALITY_FORECAST,
        date=partition_date,
        s3_key=s3_key,
    )
    catalog.insert_raw_file(raw_record)
    context.log.info(f"Recorded raw file in catalog: {s3_key}")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "source": _ADS_SOURCE,
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
    source = ingest_metadata["source"].value
    raw_key = f"{source}/{dataset}/{partition_date}/{run_id}.grib"
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


@dg.asset(
    partitions_def=daily_partitions,
    kinds={"python", "ingest"},
)
def ingest_ecmwf_data(
    context: dg.AssetExecutionContext,
    ecmwf_client: EcmwfClient,
    object_store: ObjectStore,
    catalog: PostgresCatalogResource,
) -> dg.MaterializeResult:
    """
    Download 2t + 2d from ECMWF Open Data and store in MinIO.

    Single retrieve call — both surface variables in one GRIB file.
    S3 key pattern: ecmwf/{dataset}/{YYYY-MM-DD}/{run_id}.grib
    """
    run_id = str(uuid.uuid7())
    partition_date = date.fromisoformat(context.partition_key)
    context.log.info(f"Starting ingestion: source={_ECMWF_SOURCE}, dataset={_WEATHER_FORECAST}, date={partition_date}, run_id={run_id}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "ecmwf.grib"
        ecmwf_client.retrieve_forecast(
            forecast_date=partition_date,
            variables=["temperature", "dewpoint"],
            target=tmp_path,
            max_leadtime_hours=48,
        )
        context.log.info(f"Downloaded ECMWF data ({tmp_path.stat().st_size} bytes)")
        s3_key = f"{_ECMWF_SOURCE}/{_WEATHER_FORECAST}/{partition_date}/{run_id}.grib"
        object_store.upload_raw(s3_key, tmp_path)
        context.log.info(f"Uploaded to {s3_key}")

    raw_record = RawFileRecord(
        id=uuid.UUID(run_id),
        source=_ECMWF_SOURCE,
        dataset=_WEATHER_FORECAST,
        date=partition_date,
        s3_key=s3_key,
    )
    catalog.insert_raw_file(raw_record)
    context.log.info(f"Recorded raw file in catalog: {s3_key}")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "source": _ECMWF_SOURCE,
            "dataset": _WEATHER_FORECAST,
            "date": partition_date.isoformat(),
            "s3_key": s3_key,
        }
    )


@dg.asset(
    partitions_def=daily_partitions,
    deps=[ingest_ecmwf_data],
    kinds={"python", "transform"},
)
def transform_ecmwf_data(
    context: dg.AssetExecutionContext,
    object_store: ObjectStore,
    catalog: PostgresCatalogResource,
    grid_store: GridStore,
) -> dg.MaterializeResult:
    """
    Transform raw ECMWF GRIB data into curated grid rows in ClickHouse.

    Reads upstream ingestion metadata to locate the raw file, downloads it from MinIO,
    groups messages by timestamp, converts temperature from Kelvin to Celsius, computes
    relative humidity from dewpoint via the Magnus formula, clips to the European bounding
    box, inserts rows into ClickHouse, and records lineage in the Postgres catalog.

    Args:
        context: Dagster execution context (provides partition key and upstream metadata)
        object_store: S3/MinIO client for downloading raw files
        catalog: Postgres catalog for lineage recording
        grid_store: Grid storage backend (ClickHouse in production)

    Returns:
        MaterializeResult with run_id, date, variables_processed, and inserted_rows
    """
    partition_date = context.partition_key
    upstream_key = ingest_ecmwf_data.key
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
            f"No materialization for {upstream_key} partition {partition_date}"
        )

    ingest_metadata = records[0].asset_materialization.metadata
    run_id = ingest_metadata["run_id"].value
    dataset = ingest_metadata["dataset"].value
    source = ingest_metadata["source"].value
    raw_key = f"{source}/{dataset}/{partition_date}/{run_id}.grib"

    curated_keys: list[uuid.UUID] = []
    variables_processed: list[str] = []
    rows_inserted = 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / "raw.grib"
        try:
            object_store.download_raw(raw_key, tmp_path)
        except Exception as e:
            raise dg.Failure(f"Failed to download {raw_key}: {e}")

        reader = EcmwfReader()
        groups: dict[datetime, dict[str, GribMessage]] = {}

        with reader.open(tmp_path) as messages:
            for msg in messages:
                ts = msg.timestamp
                if ts not in groups:
                    groups[ts] = {}
                groups[ts][msg.variable_name] = msg

            for ts, vars in groups.items():
                if "temperature" not in vars or "dewpoint" not in vars:
                    context.log.warning(
                        f"Skipping {ts}: missing variable(s), got {list(vars)}"
                    )
                    continue

                t_msg = vars["temperature"]
                d_msg = vars["dewpoint"]

                t_vals, t_lats, t_lons = _clip_to_europe(t_msg.values, t_msg.lats, t_msg.lons)
                d_vals, _, _ = _clip_to_europe(d_msg.values, d_msg.lats, d_msg.lons)

                t_c = t_vals - 273.15
                d_c = d_vals - 273.15
                rh = 100 * np.exp(17.625 * d_c / (243.04 + d_c)) \
                         / np.exp(17.625 * t_c / (243.04 + t_c))

                catalog_id_t = uuid.uuid7()
                rows_inserted += grid_store.insert_grid(GridData(
                    variable="temperature", unit="°C", timestamp=ts,
                    lats=t_lats, lons=t_lons, values=t_c,
                    catalog_id=catalog_id_t,
                ))
                catalog.insert_curated_data(CuratedDataRecord(
                    id=catalog_id_t, raw_file_id=uuid.UUID(run_id),
                    variable="temperature", unit="°C", timestamp=ts,
                ))
                curated_keys.append(catalog_id_t)
                variables_processed.append("temperature")

                catalog_id_h = uuid.uuid7()
                rows_inserted += grid_store.insert_grid(GridData(
                    variable="humidity", unit="%", timestamp=ts,
                    lats=t_lats, lons=t_lons, values=rh,
                    catalog_id=catalog_id_h,
                ))
                catalog.insert_curated_data(CuratedDataRecord(
                    id=catalog_id_h, raw_file_id=uuid.UUID(run_id),
                    variable="humidity", unit="%", timestamp=ts,
                ))
                curated_keys.append(catalog_id_h)
                variables_processed.append("humidity")

    return dg.MaterializeResult(metadata={
        "run_id": run_id,
        "date": partition_date,
        "curated_keys": [str(k) for k in curated_keys],
        "variables_processed": list(set(variables_processed)),
        "inserted_rows": rows_inserted,
    })
