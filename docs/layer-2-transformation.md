# Layer 2 — Transformation

Read raw data, normalize schemas, compute quality flags, and write to curated storage.

## Status

| Component | Status |
|-----------|--------|
| Dagster project setup | ✅ Done |
| Dagster asset invoking Go ingestion | ✅ Done |
| CAMS transformation asset | ✅ Done |
| Curated partitioning scheme | ✅ Done |
| Curated file format | ✅ GRIB2 |
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
- Spatial + temporal chunking for query efficiency
- Writing curated data to `jackfruit-curated` bucket
- Recording lineage (which raw files → which curated files)

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
**Output:** `jackfruit-curated` bucket

ETL reads raw (or public S3 directly for large datasets), writes curated. Never mutates raw.

**Raw path pattern:** `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

**Raw file selection:** Explicit lineage via `run_id` from ingestion asset metadata.
- Ingestion asset outputs `run_id` in `MaterializeResult.metadata`
- Transform asset reads upstream metadata to construct exact raw key
- No S3 LIST operations needed — direct GET by constructed key
- Dataset name is deterministic per transform asset (e.g., `cams-europe-air-quality-forecasts-analysis`)

**Processing:**
- Multi-variable GRIB files are read with `grib2io` (NOAA's GRIB2 library)
- Each variable/timestamp is split into separate curated files
- Single message per output file for serving layer simplicity

### Catalog Integration

All file writes are recorded in the Postgres metadata catalog (`catalog` schema):

**Ingestion** writes to `catalog.raw_files`:
- Record includes `run_id`, source, dataset, date, S3 key
- Uses `ON CONFLICT DO NOTHING` for idempotency (re-runs don't duplicate)

**Transformation** writes to `catalog.curated_files`:
- Record includes variable, source, timestamp, S3 key, and `raw_file_id` for lineage
- Uses `ON CONFLICT DO UPDATE` for reprocessing (latest metadata wins)

**Connection reuse:** Transform assets use the catalog resource as a context manager to reuse database connections across multiple curated file inserts (reduces connection overhead).

**Error handling:** Catalog writes are non-fatal. If a catalog insert fails, the asset logs a warning but continues successfully. S3 is the source of truth; the catalog is a derived index.

## Curated File Format

**Format:** GRIB2

**Why GRIB2:**
- Native format for gridded meteorological data — no conversion overhead
- Coordinate-aware (lat/lon/time built-in)
- Go serving layer can read via `eccodes` bindings
- Compact and well-supported by ECMWF tools

**Alternatives considered:**
- Zarr: Cloud-native but poor Go support
- Parquet: Columnar, loses grid structure
- NetCDF: Good but less cloud-optimized than GRIB2

## Curated Partitioning

**One file per variable per timestamp.** Path structure ends where granularity ends.

### Key Structure (Plain Paths)

```
{variable}/{source}/{year}/{month}/{day}/{hour}/data.grib2   # hourly
{variable}/{source}/{year}/{month}/{day}/data.grib2          # daily
{variable}/{source}/{year}/W{week}/data.grib2                # weekly (ISO week)
```

### Examples by Source

| Source | Granularity | Example Path |
|--------|-------------|--------------|
| CAMS | Hourly | `pm2p5/cams/2025/03/11/14/data.grib2` |
| ERA5 | Hourly | `temperature/era5/2025/03/11/00/data.grib2` |
| GloFAS | Daily | `discharge/glofas/2025/03/11/data.grib2` |
| CGLS | Weekly | `ndvi/cgls/2025/W11/data.grib2` |

### Design Principles

- **Plain paths** — no `key=value` Hive-style, simpler key construction in Go
- **Single timestamp per file** — serving layer fetches exactly one file per query
- **Time-first partitioning** — most selective dimension for queries
- **Granularity in path depth** — hourly paths deeper than daily, weekly uses ISO week
- **Filename is always `data.grib2`** — time components are directories

### Why Single-Timestamp Files?

Optimized for the serving layer query pattern:

1. Client requests: "PM2.5 for Berlin at 2025-03-11T14:37:00Z"
2. Server snaps to nearest hour: `14:00`
3. Server constructs key: `pm2p5/cams/2025/03/11/14/data.grib2`
4. Server fetches single file via direct S3 GET (no listing)
5. Server extracts grid cell, returns value

**Trade-offs accepted:**
- More files (CAMS: ~88K files/year) — S3 handles this fine
- Small file overhead (~5%) — acceptable for query simplicity
- Write amplification in ETL (raw→many curated) — ETL runs once, serving queries many times

### Serving Layer Key Construction (Go)

```go
// Hourly
key := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/data.grib2",
    variable, source, year, month, day, hour)

// Daily
key := fmt.Sprintf("%s/%s/%04d/%02d/%02d/data.grib2",
    variable, source, year, month, day)

// Weekly (ISO week)
year, week := timestamp.ISOWeek()
key := fmt.Sprintf("%s/%s/%04d/W%02d/data.grib2",
    variable, source, year, week)
```

### Scope

- **MVP:** Europe only (no spatial tiling needed)
- **Future:** Add spatial partitioning if file sizes grow or global coverage added

## Multi-Resolution Sources

Different sources have different temporal resolutions. Each source has a defined granularity.

| Source | Granularity | Resolution |
|--------|-------------|------------|
| CAMS | Hourly | 1 hour |
| ERA5 | Hourly | 1 hour |
| GloFAS | Daily | 1 day |
| CGLS (NDVI) | Weekly | ISO week |

**Granularity config:** Serving layer maintains a simple lookup (source → granularity) to snap query timestamps to the correct resolution.

## Metadata

GRIB2 files are self-describing (coordinates, units, CRS embedded). 

### Current: Dagster Metadata

Lineage and processing metadata stored in `MaterializeResult.metadata`:

```python
return dg.MaterializeResult(
    metadata={
        "run_id": run_id,
        "raw_key": raw_key,
        "curated_keys": curated_keys,  # list of output paths
        "variables_processed": variables,
        "processing_version": "1.0.0",
    }
)
```

### Future: Postgres Catalog

Migrate to Postgres for queryable metadata:
- Which raw files → which curated files (lineage)
- Processing version per curated file
- Temporal/spatial bounds index for serving layer queries

**Migration path:** Keep Dagster metadata structure consistent so it can be written to Postgres when ready.

## Error Handling

**Strategy: Fail-fast, no partial updates.**

If any variable fails to extract or write:
- Entire asset fails
- No partial curated files written
- Retry processes all variables again

**Rationale:**
- Simpler to reason about — asset either succeeds completely or fails completely
- Avoids inconsistent state in curated bucket
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

| Format | Library |
|--------|---------|
| GRIB2 | [`grib2io`](https://github.com/NOAA-MDL/grib2io) — NOAA's GRIB2 read/write library |

**Why grib2io:**
- Full GRIB2 read AND write support
- Native Python API with clean context managers
- Python 3.10-3.14 support
- Uses NCEP g2c library backend
- Maintained by NOAA MDL — production-quality library

## Idempotency

Transformation jobs must be idempotent:
- Re-running produces identical output
- Can delete curated, re-run ETL, get same results

---

## Future Refactoring Ideas

- [ ] **Unified schema design** — common structure across datasets
- [ ] **Derived metrics** — compute here vs push to serving layer?
- [ ] **Schema evolution strategy** — how to handle breaking changes
- [ ] **Backfill strategy** — bulk historical data processing
- [ ] **Spatial chunking** — add when needed for global coverage or large files
- [ ] **Postgres metadata catalog** — migrate from Dagster metadata
