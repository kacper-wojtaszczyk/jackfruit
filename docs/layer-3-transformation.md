# Layer 3 — Transformation (ETL)

Read raw data, normalize schemas, compute quality flags, and write to curated storage.

## Status

| Component | Status |
|-----------|--------|
| Dagster project setup | 🚧 In progress |
| Dagster invoking Go ingestion | 🚧 In progress |
| CAMS transformation asset | ⏳ Planned |
| Curated partitioning scheme | ⏳ Planned |

## Responsibilities

- Schema mapping per source → unified schema
- Unit normalization
- Timestamp standardization (all UTC)
- Geo-coordinate handling and validation
- Null/missing value flagging
- Provenance tagging (source, ingestion time)
- Spatial + temporal chunking for query efficiency
- Writing metadata alongside data chunks

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

## Storage

**Input:** `jackfruit-raw` bucket  
**Output:** `jackfruit-curated` bucket

ETL reads raw, writes curated. Never mutates raw.

**Raw path pattern:** `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`
- ETL can list by date prefix and then by run_id
- Dataset name includes variants (e.g., `cams-europe-air-quality-forecasts-analysis`)
- Multi-variable NetCDF files are introspected with `xarray` (`ds.data_vars`)
- Each variable is split into separate curated partitions

## Curated Partitioning

**Partition by event time first, then space.**

```
curated/
  dataset=weather/
    temporal_resolution=hourly/
      year=2025/
        month=03/
          day=11/
            hour=12/
              spatial_chunk=<TBD>/
                data.parquet
                metadata.json
```

**Principles:**
- Time-first (most selective for queries)
- Spatial chunking comes after time
- Each chunk = manageable query unit (MBs, not GBs)

## Multi-Resolution Sources

Different sources have different temporal resolutions. Don't force alignment too early.

| Source Type | Resolution |
|-------------|------------|
| ERA5, CAMS | Hourly |
| Satellite imagery | Daily/weekly |
| River discharge | Daily |

Queries can snap to nearest timestamp, interpolate, or aggregate.

## Metadata

Each curated chunk should have metadata (format TBD):

- Source dataset
- Temporal bounds (start, end)
- Spatial bounds (bbox or region)
- Units
- CRS
- Processing version
- Checksum of input files

## Processing Libraries

| Format | Library |
|--------|---------|
| NetCDF | `xarray` + `netCDF4` |
| GRIB | `xarray` + `cfgrib` |
| GeoTIFF | `rasterio` |
| Parquet | `pyarrow` |

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
