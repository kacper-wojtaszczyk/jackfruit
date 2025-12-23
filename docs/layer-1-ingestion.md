# Layer 1 — Ingestion

Fetch raw data from external environmental APIs and store unchanged in the raw bucket.

## Responsibilities

- API authentication and key management
- Rate limiting and retry logic
- Scheduling fetch jobs (via Dagster subprocess)
- Writing raw responses to `jackfruit-raw` bucket
- Logging for observability

## Does NOT Do

- Schema normalization or unit conversion
- Quality checks beyond "did the request succeed?"
- Derived metrics
- Parsing or transformation — just fetch and dump

## Technology

**Language:** Go

**Why Go:**
- Single static binary — trivial deployment
- Native concurrency (goroutines) for parallel fetching
- Explicit error handling
- Full control over HTTP without SDK abstractions

## Storage Contract

**Output bucket:** `jackfruit-raw`

**Object key pattern:**
```
{source}/{dataset}/{YYYY-MM-DD}.{ext}
```

**Example:**
```
ads/cams-europe-air-quality-forecasts-analysis/2025-03-12.nc
ads/cams-europe-air-quality-forecasts-forecast/2025-03-12.nc
ads/glofas-river-discharge/2025-03-12.nc
```

**Rules:**
- Re-ingestion overwrites (idempotent by date)
- Detect format from content, use correct extension (`.nc`, `.grib2`)
- Date in path is **ingest date** (when we fetched), not event date
- Static filename: `{date}.{ext}` — path contains all metadata
- Multi-variable files stored as-is (ETL splits them later)
- Dataset name includes request variants (e.g., CAMS analysis vs forecast are separate datasets)

## Data Strategy

### Size-Based Ingestion Rule

| Dataset Size | Strategy |
|--------------|----------|
| Small/medium | Go ingestion → MinIO |
| Large (multi-GB) | ETL reads directly from public S3 |

### Geographic Scope

- **POC:** Europe only (smaller downloads, faster iteration)
- **Future:** Global coverage

## Data Sources

### Go Ingestion Targets (no public S3)

| Source | Data Types | API | Status |
|--------|------------|-----|--------|
| **Copernicus CAMS** | PM2.5, PM10, O3, NO2, AQI | CDS API | 🚧 In progress |
| **Copernicus GloFAS** | River discharge, floods | CDS API | ⏳ Next |
| **Copernicus CGLS** | NDVI, LAI, land cover | CDS API | ⏳ Planned |

### Public S3 (ETL reads directly)

| Source | Bucket | Data Types |
|--------|--------|------------|
| ERA5 | `s3://era5-pds` | Weather reanalysis |
| NOAA GFS | `s3://noaa-gfs-bdp-pds` | Forecasts |
| MODIS | `s3://modis-pds` | Vegetation imagery |
| Sentinel-5P | `s3://meeo-s5p` | Atmospheric chemistry |

## CDS API Pattern

The Copernicus Data Store API uses an async job pattern:

1. **Submit** request → get job ID
2. **Poll** status until complete
3. **Download** result file
4. **Store** to MinIO

This pattern applies to CAMS, GloFAS, and CGLS.

## Open Questions

- [ ] Adapter interface design
- [ ] CLI structure (subcommands, flags)
- [ ] Retry policy details
- [ ] Rate limiting strategy

