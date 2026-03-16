# Layer 2 — Transformation

Read raw data, normalize schemas, compute quality flags, and write to ClickHouse.

## Status

| Component                                | Status              |
|------------------------------------------|---------------------|
| Dagster project setup                    | ✅ Done              |
| Dagster asset: Python-native ingestion   | ✅ Done              |
| CAMS transformation asset                | ✅ Done              |
| ECMWF transformation asset               | ✅ Done              |
| European clipping (`_clip_to_europe`)    | ✅ Done              |
| Magnus formula for relative humidity     | ✅ Done              |
| Curated storage                          | ✅ Done (ClickHouse) |
| Error handling                           | ✅ Fail-fast         |
| CAMS daily schedule (08:00 UTC)          | ✅ Done              |
| ECMWF daily schedule (09:30 UTC)         | ✅ Done              |
| Metadata storage                         | ✅ Postgres catalog  |

## Responsibilities

- Schema mapping per source → unified schema
- Unit normalization
- Timestamp standardization (all UTC)
- Geo-coordinate handling and validation
- Null/missing value flagging
- Provenance tagging (source, ingestion time)
- Writing curated grid data to ClickHouse
- Recording lineage in Postgres (which raw files → which ClickHouse data)

## Does NOT Do

- Fetching from external APIs (that's ingestion)
- Serving queries to clients (that's serving layer)
- Domain-specific scoring (that's client logic)
- Mutating raw data

## Technology

**Language:** Python  
**Orchestration:** Dagster

**Why Python:** Rich data ecosystem (pandas, polars, pyarrow, xarray)  
**Why Dagster:** Asset-centric model, built-in partitioning, lineage tracking, observability UI

## Ingestion from Dagster

Two parallel ingestion pipelines run independently. See [Layer 1 docs](layer-1-ingestion.md) and [ADR 003](ADR/003-python-native-ingestion.md) for the decision record.

**CAMS flow** (08:00 UTC daily):
1. Dagster asset generates UUIDv7 for `run_id`
2. `CdsClient.retrieve_forecast()` downloads GRIB from Copernicus ADS (async job pattern) to a temp file
3. `ObjectStore.upload_raw()` uploads to MinIO at `ads/cams-europe-air-quality-forecast/{date}/{run_id}.grib`
4. `PostgresCatalogResource.insert_raw_file()` records lineage in Postgres
5. On success, downstream `transform_cams_data` materializes

**ECMWF flow** (09:30 UTC daily):
1. Dagster asset generates UUIDv7 for `run_id`
2. `EcmwfClient.retrieve_forecast()` downloads IFS GRIB (direct download, no polling, no API key) to a temp file
3. `ObjectStore.upload_raw()` uploads to MinIO at `ecmwf/ifs-weather-forecast/{date}/{run_id}.grib`
4. `PostgresCatalogResource.insert_raw_file()` records lineage in Postgres
5. On success, downstream `transform_ecmwf_data` materializes

## Storage

**Input:** `jackfruit-raw` bucket (for API-sourced data)  
**Output:** ClickHouse `jackfruit.grid_data` table — columns: `(variable, timestamp, lat, lon, value, unit, catalog_id, inserted_at)`

ETL reads raw (or public S3 directly for large datasets), writes to ClickHouse. Never mutates raw.

**Raw path pattern:** `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

**Raw file selection:** Explicit lineage via `run_id` from ingestion asset metadata.
- Ingestion asset outputs `run_id` in `MaterializeResult.metadata`
- Transform asset reads upstream metadata to construct exact raw key
- No S3 LIST operations needed — direct GET by constructed key
- Dataset name is deterministic per transform asset (e.g., `cams-europe-air-quality-forecast`)

**Processing:**
- Multi-variable GRIB files are read with `pygrib` (ecCodes-backed library) via source-specific adapters
- `CamsReader`/`CamsMessage` — maps constituent codes (40008→PM10, 40009→PM2.5) from CAMS GRIB
- `EcmwfReader`/`EcmwfMessage` — maps `shortName` (2t→temperature, 2d→dewpoint) from IFS GRIB
- ECMWF transform: clips global 0.25° grid to Europe (30–72°N, -25–45°E) via `_clip_to_europe()`, converts K→°C, computes relative humidity from temperature and dewpoint via the Magnus formula
- Grid data extracted to numpy arrays and batch-inserted into ClickHouse

### Catalog Integration

All transformations are recorded in the Postgres metadata catalog (`catalog` schema):

**Ingestion** writes to `catalog.raw_files`:
- Record includes `run_id`, source, dataset, date, S3 key
- Uses `ON CONFLICT DO NOTHING` for idempotency (re-runs don't duplicate)

**Transformation** writes to `catalog.curated_data`:
- Record includes variable, source, timestamp, and `raw_file_id` for lineage
- Uses `ON CONFLICT DO UPDATE` for reprocessing (latest metadata wins)
- Linked to ClickHouse rows via `catalog_id` column (each CH row references a `curated_data` record)

**Connection reuse:** Transform assets use resources with lazy connections (Dagster calls teardown_after_execution) to reuse database connections across multiple inserts (reduces connection overhead).

**Error handling:** Catalog writes are fatal — both ingestion and transformation fail-fast on catalog insert failure. Lineage is required for licensing compliance.
## Curated Data Output (ClickHouse)

**Storage:** ClickHouse

**Why ClickHouse:**
- Column-oriented — efficient for point queries
- Excellent compression on repetitive coordinate data
- SQL interface — simple for both Python writes and Go reads
- No CGO dependencies in serving layer (unlike eccodes for GRIB)

**Output pattern:**
- One row per grid point per timestamp per variable
- Batch inserts for efficiency (~120K points per grid)

**Example insert (columnar format):**
```python
# insert_grid() flattens 2D arrays to 1D and inserts column-oriented — no Python loops
grid_store.insert_grid(grid)

# Under the hood (ClickHouseGridStore.insert_grid):
client.insert(
    table="grid_data",
    column_names=["variable", "timestamp", "lat", "lon", "value", "unit", "catalog_id"],
    column_oriented=True,
    data=[
        np.full(grid.row_count, grid.variable, dtype=object),
        np.full(grid.row_count, grid.timestamp, dtype=object),
        grid.lats.ravel().astype(np.float32),
        grid.lons.ravel().astype(np.float32),
        grid.values.ravel().astype(np.float32),
        np.full(grid.row_count, grid.unit, dtype=object),
        np.full(grid.row_count, grid.catalog_id, dtype=object),
    ],
)
```

**Schema:** `inserted_at` is auto-filled by `DEFAULT now64(3)`.

### Design Principles

- **Batch inserts** — insert entire grid at once, not row-by-row
- **Idempotent** — re-running transformation overwrites same data
- **Single source of truth** — ClickHouse holds the queryable grid data

### Why Not GRIB2 Files?

Previous approach stored curated data as GRIB2 files in S3. Problems:
- Go serving layer needed eccodes (CGO) for parsing
- Coordinate-to-value lookup required complex grid math
- File-per-timestamp created many small files

ClickHouse solves all three: SQL queries, no CGO, no file management.

See [ADR 001](ADR/001-grid-data-storage.md) for the full decision record.

### Grid Storage Abstraction

The transformation code depends on the `GridStore` abstract base class, not ClickHouse directly. The resource is injected by Dagster:

```python
from pipeline_python.storage import GridStore

# Asset receives GridStore — doesn't know it's ClickHouse
@dg.asset(...)
def transform_cams_data(
    context, object_store, catalog,
    grid_store: GridStore,  # Abstract type
):
    for grid in _extract_grids_from_grib(raw_path):
        grid_store.insert_grid(grid)
```

This abstraction enables:
- Unit testing with a stub `GridStore` subclass (no ClickHouse needed)
- Future storage backend swaps without changing pipeline logic
- Dagster resource lifecycle management (lazy connection, teardown)

See [ADR 001](ADR/001-grid-data-storage.md) for the storage decision record.

## Multi-Resolution Sources

Different sources have different temporal resolutions. Each source has a defined granularity.

| Source           | Granularity | Resolution | Grid    |
|------------------|-------------|------------|---------|
| CAMS             | Hourly      | 1 hour     | 0.1°    |
| ECMWF Open Data  | 3-hourly    | 3 hours    | 0.25°   |
| ERA5             | Hourly      | 1 hour     | 0.25°   |
| GloFAS           | Daily       | 1 day      | 0.1°    |
| CGLS (NDVI)      | Weekly      | ISO week   | varies  |

**Granularity handling:** Serving layer snaps query timestamps to the correct resolution before querying ClickHouse.

## Metadata

### Postgres Catalog (Implemented)

All transformations are recorded in the Postgres metadata catalog (`catalog` schema).

**Schema:**
- `catalog.raw_files` — tracks ingested files with run_id, source, dataset, date, S3 key
- `catalog.curated_data` — tracks processed data with variable, timestamp, lineage via `raw_file_id` FK

**Upsert behavior:**
- `raw_files`: `ON CONFLICT DO NOTHING` (idempotent, re-runs don't duplicate)
- `curated_data`: `ON CONFLICT DO UPDATE` (reprocessing updates metadata)

**Connection reuse:**
Transform assets use resources with lazy connections (Dagster calls teardown_after_execution) to reuse database connections across multiple inserts.

### Dagster Metadata

Lineage and processing metadata also stored in `MaterializeResult.metadata`:

**Ingestion asset:**
```python
return dg.MaterializeResult(
    metadata={
        "run_id": run_id,
        "source": _ADS_SOURCE,
        "dataset": _AIR_QUALITY_FORECAST,
        "date": partition_date.isoformat(),
    }
)
```

**Transformation asset:**
```python
return dg.MaterializeResult(
    metadata={
        "run_id": run_id,
        "date": partition_date,
        "curated_keys": [str(key) for key in curated_keys],
        "variables_processed": list(set(variables_processed)),
        "inserted_rows": rows_inserted,
    }
)
```


## Error Handling

**Strategy: Fail-fast, no partial updates.**

If any variable fails to extract or insert:
- Entire asset fails
- No partial ClickHouse inserts committed
- Retry processes all variables again

**Rationale:**
- Simpler to reason about — asset either succeeds completely or fails completely
- Avoids inconsistent state in ClickHouse
- Can refine later if specific failure modes warrant partial success

## Schedules

### CAMS — `cams_daily_schedule`

**Cron:** `0 8 * * *` (08:00 UTC every day)

1. **Trigger Time:** 08:00 UTC — CAMS forecast data is typically available ~6 hours after midnight UTC
2. **Partition:** Today's date
3. **Execution:** `ingest_cams_data` → `transform_cams_data`
4. **Observability:** Tagged `source=schedule`, `pipeline=cams`, `scheduled_date=YYYY-MM-DD`

### ECMWF — `ecmwf_daily_schedule`

**Cron:** `30 9 * * *` (09:30 UTC every day)

1. **Trigger Time:** 09:30 UTC — ECMWF IFS 00Z forecasts are typically available by ~09:00 UTC
2. **Partition:** Today's date
3. **Execution:** `ingest_ecmwf_data` → `transform_ecmwf_data`
4. **Observability:** Tagged `source=schedule`, `pipeline=ecmwf`, `scheduled_date=YYYY-MM-DD`

### Manual Backfills

To materialize data for specific dates (e.g., historical backfill):

```bash
# Via Dagster UI
# 1. Navigate to Assets → transform_cams_data (or transform_ecmwf_data)
# 2. Select date range (e.g., 2026-01-01 to 2026-03-15)
# 3. Click "Materialize selected"
```

## Processing Libraries

| Purpose           | Library                                                                                           |
|-------------------|---------------------------------------------------------------------------------------------------|
| GRIB2 reading     | [`pygrib`](https://github.com/jswhit/pygrib) — Pythonic ecCodes wrapper                          |
| ClickHouse client | [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) — Official Python driver |

**Why pygrib:**
- ecCodes backend handles all ECMWF products (CAMS, GloFAS, ERA5) natively — no patches or manual lookup tables
- `grb.data()` returns `(values_2d, lats_2d, lons_2d)` — matches the `GribMessage` protocol interface cleanly
- PDT 4.40 and ECMWF constituent codes (40008 PM10, 40009 PM2.5) are in the ecCodes definitions database
- Bundled wheel — no system library dependencies
- `assets.py` is decoupled from pygrib via the `GribMessage`/`GribReader` Protocol in `grib2/reader.py`; see [ADR 003](ADR/003-python-native-ingestion.md)for the full decision record

**Why clickhouse-connect:**
- Official ClickHouse Python driver
- Efficient batch inserts with numpy/pandas support
- Connection pooling built-in

## Idempotency

Transformation jobs must be idempotent:
- Re-running produces identical output in ClickHouse
- Can delete ClickHouse data, re-run ETL, get same results

**Implementation:** Uses ClickHouse's `ReplacingMergeTree` with `FINAL` keyword for deduplication on read.

---

## Future Refactoring Ideas

- [ ] **Unified schema design** — common structure across datasets
- [ ] **Derived metrics** — compute here vs push to serving layer?
- [ ] **Schema evolution strategy** — how to handle breaking changes
- [ ] **Backfill strategy** — bulk historical data processing
- [ ] **ClickHouse materialized views** — pre-aggregate common query patterns
