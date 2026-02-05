# Layer 2 — Transformation

Read raw data, normalize schemas, compute quality flags, and write to ClickHouse.

## Status

| Component | Status |
|-----------|--------|
| Dagster project setup | ✅ Done |
| Dagster asset invoking Go ingestion | ✅ Done |
| CAMS transformation asset | 🔄 Needs migration to ClickHouse |
| Curated storage | 🔄 Migrating: GRIB2 files → ClickHouse |
| Error handling | ✅ Fail-fast |
| Daily schedule (08:00 UTC) | ✅ Done |
| Metadata storage | ✅ Postgres catalog |

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

## Invoking Go Ingestion from Dagster

> ⚠️ **Deprecation Notice:** The Go ingestion layer will be replaced with native Python ingestion using `cdsapi`. See [Layer 1 docs](layer-1-ingestion.md) for details. The current implementation works and unblocks development.

The Go ingestion layer is containerized and invoked by Dagster using `PipesDockerClient`.

**Current implementation:** Uses `dagster-docker` to spawn the ingestion container as a sibling on the host Docker daemon.

**Pattern:**
1. Dagster asset generates UUIDv7 for `run_id`
2. `PipesDockerClient` spawns ingestion container with network and env vars configured
3. Container runs, logs stream to Dagster
4. On success (exit 0), downstream transformation assets can materialize

**Key configuration (Docker sibling pattern):**
- Network: `jackfruit_jackfruit` (so container can reach MinIO)
- Env vars: Forwarded from Dagster container (`ADS_*`, `MINIO_*`)

**Exit code handling:**
- `0`: Success → materialize downstream assets
- `1`: Config error → fail immediately
- `2`: Application error → retry with backoff (future)

## Storage

**Input:** `jackfruit-raw` bucket (for API-sourced data)  
**Output:** ClickHouse `grid_data` table (schema TBD)

ETL reads raw (or public S3 directly for large datasets), writes to ClickHouse. Never mutates raw.

**Raw path pattern:** `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

**Raw file selection:** Explicit lineage via `run_id` from ingestion asset metadata.
- Ingestion asset outputs `run_id` in `MaterializeResult.metadata`
- Transform asset reads upstream metadata to construct exact raw key
- No S3 LIST operations needed — direct GET by constructed key
- Dataset name is deterministic per transform asset (e.g., `cams-europe-air-quality-forecasts-analysis`)

**Processing:**
- Multi-variable GRIB files are read with `grib2io` (NOAA's GRIB2 library)
- Grid data extracted to numpy arrays
- Each variable/timestamp batch-inserted into ClickHouse

### Catalog Integration

All transformations are recorded in the Postgres metadata catalog (`catalog` schema):

**Ingestion** writes to `catalog.raw_files`:
- Record includes `run_id`, source, dataset, date, S3 key
- Uses `ON CONFLICT DO NOTHING` for idempotency (re-runs don't duplicate)

**Transformation** writes to `catalog.curated_data`:
- Record includes variable, source, timestamp, and `raw_file_id` for lineage
- Uses `ON CONFLICT DO UPDATE` for reprocessing (latest metadata wins)
- Relationship to ClickHouse rows: TBD (see task 06)

**Connection reuse:** Transform assets use resources as context managers to reuse database connections across multiple inserts (reduces connection overhead).

**Error handling:** Catalog writes are non-fatal. If a catalog insert fails, the asset logs a warning but continues successfully. ClickHouse is the source of truth for grid data; the catalog is a derived lineage index.

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

**Example insert (conceptual):**
```python
client.insert('grid_data',
    data=[(var, source, ts, lat, lon, val) for ...],
    column_names=['variable', 'source', 'timestamp', 'lat', 'lon', 'value']
)
```

**Schema:** TBD — you design it. See task 01 for guidelines on ClickHouse schema design for grid data.

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

The transformation code depends on a `GridStore` Protocol, not ClickHouse directly:

```python
from pipeline_python.storage import GridStore
from pipeline_python.storage.clickhouse import ClickHouseGridStore

# Asset code uses the protocol
store: GridStore = ClickHouseGridStore(client)
store.insert_grids(grids)
```

This abstraction enables:
- Unit testing with `InMemoryGridStore` (no ClickHouse needed)
- Future storage backend swaps without changing pipeline logic

See [Task 04](guides/tasks/04-transform-insert-clickhouse.md) for implementation details.

## Multi-Resolution Sources

Different sources have different temporal resolutions. Each source has a defined granularity.

| Source | Granularity | Resolution |
|--------|-------------|------------|
| CAMS | Hourly | 1 hour |
| ERA5 | Hourly | 1 hour |
| GloFAS | Daily | 1 day |
| CGLS (NDVI) | Weekly | ISO week |

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
Transform assets use resources as context managers to reuse database connections across multiple inserts.

### Dagster Metadata

Lineage and processing metadata also stored in `MaterializeResult.metadata`:

```python
return dg.MaterializeResult(
    metadata={
        "run_id": run_id,
        "raw_key": raw_key,
        "variables_processed": variables,
        "rows_inserted": row_count,
        "processing_version": "1.0.0",
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

## Daily Schedule

**Name:** `cams_daily_schedule`  
**Cron:** `0 8 * * *` (08:00 UTC every day)  
**Timezone:** UTC

The schedule automatically triggers ingestion and transformation for the previous day's data each morning:

1. **Trigger Time:** 08:00 UTC (after CAMS data is typically available ~6 hours after midnight UTC)
2. **Partition:** Today's date (e.g., if scheduled run is 2025-03-12 08:00, it processes 2025-03-11)
3. **Execution:**
   - Dagster creates a `RunRequest` for the specific partition
   - `ingest_cams_data` asset executes first (fetches from CDS API → raw bucket)
   - On success, `transform_cams_data` executes (transforms raw → curated)
4. **Observability:** Each run is tagged with `source=schedule`, `pipeline=cams`, `scheduled_date=YYYY-MM-DD`

### Manual Backfills

To materialize data for specific dates (e.g., historical backfill):

```bash
# Via Dagster UI
# 1. Navigate to Assets → transform_cams_data
# 2. Select date range (e.g., 2025-01-01 to 2025-03-11)
# 3. Click "Materialize selected"

# Via Dagster CLI (future)
# dagster asset materialize --asset-selection 'transform_cams_data' \
#   --start-partition '2025-01-01' --end-partition '2025-03-11'
```

## Processing Libraries

| Purpose | Library |
|---------|---------|
| GRIB2 reading | [`grib2io`](https://github.com/NOAA-MDL/grib2io) — NOAA's GRIB2 read/write library |
| ClickHouse client | [`clickhouse-connect`](https://github.com/ClickHouse/clickhouse-connect) — Official Python driver |

**Why grib2io:**
- Full GRIB2 read support
- Native Python API with clean context managers
- Python 3.10-3.14 support
- Uses NCEP g2c library backend
- Maintained by NOAA MDL — production-quality library

**Why clickhouse-connect:**
- Official ClickHouse Python driver
- Efficient batch inserts with numpy/pandas support
- Connection pooling built-in

## Idempotency

Transformation jobs must be idempotent:
- Re-running produces identical output in ClickHouse
- Can delete ClickHouse data, re-run ETL, get same results

**Implementation:** Use ClickHouse's `ReplacingMergeTree` or explicit delete-before-insert pattern. TBD in schema design.

---

## Future Refactoring Ideas

- [ ] **Unified schema design** — common structure across datasets
- [ ] **Derived metrics** — compute here vs push to serving layer?
- [ ] **Schema evolution strategy** — how to handle breaking changes
- [ ] **Backfill strategy** — bulk historical data processing
- [ ] **ClickHouse materialized views** — pre-aggregate common query patterns
