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
    """Clip spatial data to the European bounding box.

    Works with any grid resolution (ECMWF 0.25°, CAMS 0.1°, etc.) or
    irregular point data (e.g. station measurements). For regular grids,
    infers the 2D output shape from unique coordinates. For irregular
    data, returns arrays shaped (N, 1) to satisfy GridData's 2D requirement.
    """
    mask = (
        (lats >= _EUROPE_LAT_MIN) & (lats <= _EUROPE_LAT_MAX)
        & (lons >= _EUROPE_LON_MIN) & (lons <= _EUROPE_LON_MAX)
    )
    flat_v = values[mask]
    flat_la = lats[mask]
    flat_lo = lons[mask]

    # Infer 2D shape from unique coordinates (resolution-independent).
    # Round to 6 decimals (~0.11 m) to avoid floating-point splitting.
    n_lats = len(np.unique(np.round(flat_la, decimals=6)))
    n_lons = len(np.unique(np.round(flat_lo, decimals=6)))

    if n_lats * n_lons == len(flat_v):
        return (
            flat_v.reshape(n_lats, n_lons),
            flat_la.reshape(n_lats, n_lons),
            flat_lo.reshape(n_lats, n_lons),
        )
    # Irregular points — column vector for GridData compatibility
    return (
        flat_v.reshape(-1, 1),
        flat_la.reshape(-1, 1),
        flat_lo.reshape(-1, 1),
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
            "variables_processed": sorted(set(variables_processed)),
            "inserted_rows": rows_inserted,
        }
    )


class EcmwfForecastConfig(dg.Config):
    """Configuration for the ECMWF ingestion asset."""
    horizon_hours: int = 48


@dg.asset(
    partitions_def=daily_partitions,
    kinds={"python", "ingest"},
)
def ingest_ecmwf_data(
    context: dg.AssetExecutionContext,
    config: EcmwfForecastConfig,
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
            max_leadtime_hours=config.horizon_hours,
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

            for ts, variables in groups.items():
                if "temperature" not in variables or "dewpoint" not in variables:
                    context.log.warning(
                        f"Skipping {ts}: missing variable(s), got {list(variables)}"
                    )
                    continue

                t_msg = variables["temperature"]
                d_msg = variables["dewpoint"]

                t_vals, t_lats, t_lons = _clip_to_europe(t_msg.values, t_msg.lats, t_msg.lons)
                d_vals, d_lats, d_lons = _clip_to_europe(d_msg.values, d_msg.lats, d_msg.lons)

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

                catalog_id_d = uuid.uuid7()
                rows_inserted += grid_store.insert_grid(GridData(
                    variable="dewpoint", unit="°C", timestamp=ts,
                    lats=d_lats, lons=d_lons, values=d_c,
                    catalog_id=catalog_id_d,
                ))
                catalog.insert_curated_data(CuratedDataRecord(
                    id=catalog_id_d, raw_file_id=uuid.UUID(run_id),
                    variable="dewpoint", unit="°C", timestamp=ts,
                ))
                curated_keys.append(catalog_id_d)
                variables_processed.append("dewpoint")

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
        "variables_processed": sorted(set(variables_processed)),
        "inserted_rows": rows_inserted,
    })


def make_optimize_asset(
    source: str,
    upstream: dg.AssetsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"optimize_{source}_data",
        partitions_def=daily_partitions,
        deps=[upstream],
        kinds={"python", "optimize"},
    )
    def _asset(
        context: dg.AssetExecutionContext,
        grid_store: GridStore,
    ) -> dg.MaterializeResult:
        context.log.info(f"Running post-{source.upper()} compaction")
        grid_store.compact()
        context.log.info("Compaction complete")
        return dg.MaterializeResult(metadata={"table": "grid_data"})

    return _asset


optimize_cams_data  = make_optimize_asset("cams",  transform_cams_data)
optimize_ecmwf_data = make_optimize_asset("ecmwf", transform_ecmwf_data)
