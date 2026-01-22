# Infrastructure

Pluggable backend services. Local dev uses Docker containers; production uses managed cloud services.

## Components

| Component | Local (Dev) | Production | Purpose |
|-----------|-------------|------------|---------|
| Object Storage | MinIO | S3 | Raw + curated data files |
| Metadata DB | Postgres | RDS Postgres | Dataset catalog, tile index, run history |
| Orchestration | Dagster (local) | Dagster Cloud / ECS | Pipeline scheduling and monitoring |

All components expose standard APIs — application code doesn't change between environments.

## Object Storage

**Buckets:**

| Bucket | Purpose |
|--------|---------|
| `jackfruit-raw` | Immutable, append-only, source-faithful |
| `jackfruit-curated` | Processed, query-optimized |

**Why separate buckets:**
- Clear separation of concerns
- Different lifecycle rules (raw kept longer, curated may be pruned)
- Safety — transformation bugs can't corrupt raw data

**Object key pattern (raw):**
```
{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}
```

**Example:**
```
ads/cams-europe-air-quality-forecasts-analysis/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib
```

### What "Raw" Means

Raw = as close to source as possible:
- No semantic transformation
- No aggregation or resampling
- No merging across sources

Think of raw as **evidence**, not data.

## Metadata Database

**Tech:** Postgres

**Why Postgres:**
- Simple, battle-tested
- Good enough for catalog/index queries
- Native Dagster support
- Not overkill (ClickHouse deferred until analytics needs arise)

**Schema:** `catalog`

The metadata database tracks all files in object storage and their lineage relationships. Schema is initialized via `migrations/init.sql` mounted into the Postgres container.

### Tables

**`catalog.raw_files`** — Raw files ingested from external sources

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID (PK) | Run ID from ingestion (UUIDv7) |
| `source` | TEXT | Data source (e.g., 'ads') |
| `dataset` | TEXT | Dataset identifier (e.g., 'cams-europe-air-quality-forecasts-forecast') |
| `date` | DATE | Partition date |
| `s3_key` | TEXT (UNIQUE) | Full S3 key in `jackfruit-raw` bucket |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |

**`catalog.curated_files`** — Processed files derived from raw files

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID (PK) | Generated UUIDv7 |
| `raw_file_id` | UUID (FK) | References `catalog.raw_files(id)` for lineage |
| `variable` | TEXT | Variable name (e.g., 'pm2p5', 'pm10') |
| `source` | TEXT | Data source (e.g., 'cams') |
| `timestamp` | TIMESTAMPTZ | Valid time of data |
| `s3_key` | TEXT (UNIQUE) | Full S3 key in `jackfruit-curated` bucket |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |

**Indexes:**
- `idx_curated_files_lookup` on `(variable, timestamp)` — serving layer queries
- `idx_curated_files_raw` on `(raw_file_id)` — lineage queries

### Access Patterns

| Operation | Query Pattern |
|-----------|---------------|
| Ingestion writes | Insert into `raw_files` after S3 upload |
| Transformation writes | Insert into `curated_files` after S3 upload, linked via `raw_file_id` |
| Serving reads | Query `curated_files` by variable + timestamp range to find S3 keys |
| Lineage queries | Join `curated_files` → `raw_files` to trace provenance |

**What it stores:**

| Table (conceptual) | Purpose |
|--------------------|---------|
| `datasets` | Source definitions, schemas, refresh schedules (TBD) |
| `ingestion_runs` | Run history, status, checksums (TBD — Dagster tracks this) |
| `tiles` | Spatial/temporal index of curated chunks (TBD — may not need if GRIB covers this) |
| `lineage` | Which raw files → which curated files (✅ via `raw_file_id` FK) |

## Orchestration

**Tech:** Dagster

**Responsibilities:**
- Schedule ingestion and transformation jobs
- Track asset dependencies and lineage
- Provide observability UI
- Generate run IDs (UUIDv7)

**Job execution model:**

| Layer | Execution | Rationale |
|-------|-----------|-----------|
| Ingestion (Go) | Sibling container via `docker compose run` | Isolated binary, different language |
| Transformation (Python) | In Dagster container | Same runtime, simpler, faster iteration |

Dagster container mounts host Docker socket to spawn ingestion containers as siblings (not nested).

**Future:** If transformation jobs need resource isolation or conflicting dependencies, they can be containerized like ingestion.

**Local:** Dagster runs in container via `docker-compose up`  
**Production:** Dagster Cloud or self-hosted on ECS

Dagster is infrastructure, not a processing layer — it orchestrates Layers 1 and 2.

## Local Development

```bash
# Start infrastructure (MinIO + Dagster container)
docker-compose up -d

# Dagster UI
open http://localhost:3099
```

**docker-compose.yml provides:**
- MinIO (API: 9099, Console: 9098)
- Dagster (port 3099) with host Docker socket mounted to run ingestion containers
- Network for service communication

## Environment Configuration

All services configured via environment variables (`.env` file):

```bash
# Object Storage
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_RAW_BUCKET=jackfruit-raw

# Metadata DB (TBD)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=jackfruit
POSTGRES_PASSWORD=...
POSTGRES_DB=jackfruit

# External APIs
ADS_BASE_URL=https://ads.atmosphere.copernicus.eu/api
ADS_API_KEY=...
```

## Production Deployment

**TBD.** Likely:
- Terraform for AWS resources (S3, RDS, ECS)
- Same env var interface, different values
- Dagster Cloud or self-hosted Dagster on ECS

## Open Questions

- [x] Postgres schema design — ✅ Done (`catalog.raw_files`, `catalog.curated_files`)
- [ ] Tile indexing strategy (PostGIS for spatial?) — Deferred, GRIB self-describes coordinates
- [ ] Production deployment approach
- [ ] Backup/restore strategy

