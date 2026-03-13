# Layer 1 — Ingestion

Fetch raw data from external environmental APIs and store unchanged in the raw bucket.

## Status

| Component | Status |
|-----------|--------|
| CDS API client (`cdsapi` via `CdsClient`) | ✅ Done |
| MinIO storage integration (`ObjectStore`) | ✅ Done |
| CAMS Europe Air Quality datasets | ✅ Done |
| Dagster orchestration (native Python asset) | ✅ Done |
| GloFAS dataset | ⏳ Planned |
| Retry/rate limiting | ⏳ Planned |

## History

Ingestion was originally implemented as a Go CLI (`ingestion-go/`) invoked from Dagster via `PipesDockerClient`. This was replaced by native Python ingestion using `cdsapi` — see [ADR 003](ADR/003-python-native-ingestion.md) for the full decision record. The Go code and `dagster-docker` dependency have been deleted.

## Responsibilities

- API authentication and key management
- Rate limiting and retry logic
- Scheduling fetch jobs (via Dagster)
- Writing raw responses to `jackfruit-raw` bucket
- Logging for observability

## Does NOT Do

- Schema normalization or unit conversion
- Quality checks beyond "did the request succeed?"
- Derived metrics
- Parsing or transformation — just fetch and dump

## Technology

**Language:** Python (with `cdsapi` library)
**Orchestration:** Native Dagster asset (`ingest_cams_data`)

**Key components:**
- `CdsClient` (`ingestion/cds_client.py`) — `ConfigurableResource` wrapping `cdsapi`. Handles CDS async job pattern (submit → poll → download)
- `ObjectStore` (`storage/object_store.py`) — boto3-based S3/MinIO client. `upload_raw()` puts the downloaded GRIB into MinIO

**Why Python:**
- Same language as transformation layer — single stack for the entire pipeline
- `cdsapi` handles the CDS async job pattern natively (no hand-rolled polling)
- No container orchestration overhead (runs in-process in the Dagster worker)

## Storage Contract

**Output bucket:** `jackfruit-raw`

**Object key pattern:**
```
{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}
```

**Example:**
```
ads/cams-europe-air-quality-forecast/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib
```

**Rules:**
- Multiple ingests on same date have distinct run_ids (no overwrites)
- Extension currently hardcoded to `.grib` (detection TBD)
- Date in path is **ingest date** (when we fetched), not event date
- Run ID is UUIDv7 passed from orchestration (Dagster)
- Multi-variable files stored as-is (ETL splits them later)
- Dataset name includes request variants (e.g., CAMS analysis vs forecast are separate datasets)

## Data Strategy

### Size-Based Ingestion Rule

| Dataset Size | Strategy |
|--------------|----------|
| Small/medium | Python ingestion → MinIO |
| Large (multi-GB) | ETL reads directly from public S3 |

### Geographic Scope

- **POC:** Europe only (smaller downloads, faster iteration)
- **Future:** Global coverage

## Data Sources

### Ingestion Targets (no public S3)

| Source | Data Types | API | Status |
|--------|------------|-----|--------|
| **Copernicus CAMS** | PM2.5, PM10, O3, NO2, AQI | CDS API | ✅ Done |
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

---

## Future Refactoring Ideas

High-level improvements to revisit after MVP:

### CDS Client
- [ ] **Extension detection** — derive `.nc` vs `.grib` from Content-Type or asset type
- [ ] **Retry with backoff** — exponential backoff with jitter for transient CDS API failures
- [ ] **Rate limiting** — token bucket or leaky bucket per API
- [ ] **Richer error types** — distinguish retryable vs permanent failures

### Architecture
- [ ] **Adapter registry** — map dataset → adapter dynamically
- [ ] **Progress reporting** — structured events for Dagster to parse

### Testing
- [ ] **Integration test with real MinIO** — docker-compose test harness
- [ ] **Golden file tests** — snapshot CDS request payloads
