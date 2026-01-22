# Layer 1 — Ingestion

Fetch raw data from external environmental APIs and store unchanged in the raw bucket.

## Status

| Component | Status |
|-----------|--------|
| CDS API client (async job pattern) | ✅ Done (Go) |
| CLI with flags (`--date`, `--dataset`, `--run-id`) | ✅ Done (Go) |
| MinIO storage integration | ✅ Done (Go) |
| Structured logging (slog/JSON) | ✅ Done (Go) |
| CAMS Europe Air Quality datasets | ✅ Done (Go) |
| Containerization (Docker) | ✅ Done (Go) |
| Dagster orchestration (Docker client) | ✅ Done |
| Python-native ingestion rewrite | ⏳ Planned |
| GloFAS dataset | ⏳ Planned |
| Retry/rate limiting | ⏳ Planned |

## ⚠️ Migration Notice: Go → Python

**Current state:** Go CLI invoked via Docker from Dagster (`PipesDockerClient`).

**Planned:** Rewrite ingestion as native Python Dagster assets using `cdsapi`.

**Why migrate:**
- Container orchestration adds complexity (Docker sibling pattern, env var forwarding, network config)
- Original parallelism rationale is moot — CDS returns single aggregated files, no benefit from goroutines
- Unified Python stack simplifies the pipeline (ingestion + transformation in one language)
- Go's strengths are better utilized in the serving layer (concurrent client requests)

**Timeline:** Migrate when convenient. Go implementation is functional and unblocks development.

**What stays:** Go serving layer (Layer 5) — genuine benefits from multi-threaded execution there.

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

### Current: Go (deprecated, to be replaced)

**Language:** Go

**Why Go was chosen:**
- Single static binary — trivial deployment
- Native concurrency (goroutines) for parallel fetching
- Explicit error handling
- Full control over HTTP without SDK abstractions

**Why Go is being replaced:**
- Parallelism benefit didn't materialize (CDS API returns single files)
- Container-in-container orchestration from Dagster is complex
- Python `cdsapi` library handles async job pattern natively

### Future: Python

**Language:** Python (with `cdsapi` library)
**Orchestration:** Native Dagster assets

**Benefits:**
- Same language as transformation layer
- No container orchestration overhead
- `cdsapi` handles CDS async job pattern (submit → poll → download)
- Simpler configuration (no env var forwarding between containers)

## Storage Contract

**Output bucket:** `jackfruit-raw`

**Object key pattern:**
```
{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}
```

**Example:**
```
ads/cams-europe-air-quality-forecasts-forecast/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib
```

**Rules:**
- Multiple ingests on same date have distinct run_ids (no overwrites)
- Extension currently hardcoded to `.grib` (detection TBD)
- Date in path is **ingest date** (when we fetched), not event date
- Run ID is UUIDv7 passed from orchestration (Dagster)
- Multi-variable files stored as-is (ETL splits them later)
- Dataset name includes request variants (e.g., CAMS analysis vs forecast are separate datasets)

## CLI Usage

```bash
./ingestion \
  --dataset=cams-europe-air-quality-forecasts-analysis \
  --date=2025-03-12 \
  --run-id=01890c24-905b-7122-b170-b60814e6ee06
```

**Environment variables required:**
- `ADS_BASE_URL` — CDS API base URL
- `ADS_API_KEY` — CDS API key
- `MINIO_ENDPOINT` — MinIO endpoint (e.g., `localhost:9000`)
- `MINIO_ACCESS_KEY` — MinIO access key
- `MINIO_SECRET_KEY` — MinIO secret key
- `MINIO_RAW_BUCKET` — Target bucket name
- `MINIO_USE_SSL` — `true` or `false`

## Docker Usage

The ingestion app is containerized and can be invoked via `docker-compose` or standalone Docker.

### Via docker-compose (recommended)

```bash
# Build the image
docker compose build ingestion

# Run a single ingestion job
docker compose run --rm ingestion \
  --dataset=cams-europe-air-quality-forecasts-analysis \
  --date=2025-03-12 \
  --run-id=01890c24-905b-7122-b170-b60814e6ee06
```

The `ingestion` service uses the `ingestion` profile, so it won't start automatically with `docker-compose up`. It's designed to be invoked on-demand (e.g., by Dagster).

**How it works:**
- `depends_on` ensures MinIO is healthy before ingestion runs
- Environment variables are read from `.env` file or shell
- Logs stream to stdout (JSON format)
- Exit codes: `0` = success, `1` = config error, `2` = application error

### Via standalone Docker

```bash
# Build
docker build -t jackfruit-ingestion:latest ./ingestion-go

# Run (must have MinIO running and network accessible)
docker run --rm \
  --network jackfruit_jackfruit \
  -e ADS_BASE_URL=$ADS_BASE_URL \
  -e ADS_API_KEY=$ADS_API_KEY \
  -e MINIO_ENDPOINT=minio:9000 \
  -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY \
  -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY \
  -e MINIO_RAW_BUCKET=jackfruit-raw \
  jackfruit-ingestion:latest \
  --dataset=cams-europe-air-quality-forecasts-forecast \
  --date=2025-03-12 \
  --run-id=01890c24-905b-7122-b170-b60814e6ee06
```

### Exit Codes

Dagster (or any orchestrator) can use exit codes to decide retry strategy:

| Code | Meaning | Retry? |
|------|---------|--------|
| `0` | Success | N/A |
| `1` | Config error (missing env vars, invalid flags) | No — fix config first |
| `2` | Application error (network, API, storage) | Maybe — depends on error type |

Future: split code `2` into retryable (transient network) vs non-retryable (invalid dataset) errors.

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
- [ ] **Options pattern for client config** — timeouts, poll intervals as functional options
- [ ] **Extension detection** — derive `.nc` vs `.grib` from Content-Type or asset type
- [ ] **Retry middleware** — exponential backoff with jitter for transient failures
- [ ] **Rate limiting** — token bucket or leaky bucket per API
- [ ] **Richer error types** — distinguish retryable vs permanent failures

### CLI / Main
- [ ] **Subcommands** — `ingestion fetch`, `ingestion list-datasets`, etc.
- [ ] **Config file support** — YAML/TOML as alternative to env vars
- [ ] **Dry-run mode** — validate request without executing

### Architecture
- [ ] **Adapter registry** — map dataset → adapter dynamically
- [ ] **Parallel fetching** — goroutines for multiple datasets/dates
- [ ] **Progress reporting** — structured events for Dagster to parse

### Testing
- [ ] **Integration test with real MinIO** — docker-compose test harness
- [ ] **Golden file tests** — snapshot CDS request payloads
