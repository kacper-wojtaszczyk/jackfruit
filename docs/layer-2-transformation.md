# Layer 2 — Transformation

Read raw data, normalize schemas, compute quality flags, and write to curated storage.

## Status

| Component | Status |
|-----------|--------|
| Dagster project setup | ✅ Done |
| Dagster asset invoking Go ingestion | ✅ Done |
| CAMS transformation asset | ⏳ Next |
| Curated partitioning scheme | ⏳ Planned |
| Curated file format | GRIB2 (decided) |

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
- ETL can list by date prefix and then by run_id
- Dataset name includes variants (e.g., `cams-europe-air-quality-forecasts-analysis`)
- Multi-variable GRIB files are introspected with `xarray` + `cfgrib`
- Each variable is split into separate curated files

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

**Partition by event time, one file per variable.**

```
curated/
  source=cams/
    dataset=europe-air-quality/
      variable=pm2p5/
        year=2025/
          month=03/
            day=11/
              data.grib2
```

**Principles:**
- Time-first partitioning (most selective for queries)
- One variable per file (simplifies serving layer aggregation)
- Europe-only for MVP (no spatial tiling needed yet)
- Each file = manageable size (tens of MB for Europe)

**Deferred:**
- Spatial tiling — not needed for Europe-only single-metric files
- Can add later if file sizes grow or global coverage is added

## Multi-Resolution Sources

Different sources have different temporal resolutions. Don't force alignment too early.

| Source Type | Resolution |
|-------------|------------|
| ERA5, CAMS | Hourly |
| Satellite imagery | Daily/weekly |
| River discharge | Daily |

Queries can snap to nearest timestamp, interpolate, or aggregate.

## Metadata

GRIB2 files are self-describing (coordinates, units, CRS embedded). Additional metadata TBD:

- Processing version
- Lineage (source raw file checksums)
- Ingest timestamp

## Processing Libraries

| Format | Library |
|--------|---------|
| GRIB | `xarray` + `cfgrib` (primary) |
| NetCDF | `xarray` + `netCDF4` (if needed) |

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
- [ ] **Spatial chunking granularity** — lat/lon grid? Named regions? H3?
- [ ] **Metadata format** — JSON sidecar? Postgres+PostGIS catalog?
- [ ] **Dagster partitions** — daily partitions aligned with raw storage
