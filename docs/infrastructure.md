# Infrastructure

Pluggable backend services. Local dev uses Docker containers; production uses managed cloud services.

## Components

| Component       | Local (Dev)     | Production          | Purpose                               |
|-----------------|-----------------|---------------------|---------------------------------------|
| Object Storage  | MinIO           | S3                  | Raw data files (immutable)            |
| Metadata DB     | Postgres        | RDS Postgres        | Dataset catalog, lineage, run history |
| Grid Data Store | ClickHouse      | ClickHouse Cloud    | Curated grid data (query-optimized)   |
| Orchestration   | Dagster (local) | Dagster Cloud / ECS | Pipeline scheduling and monitoring    |

All components expose standard APIs — application code doesn't change between environments.

**Storage split:**
- **Postgres** — metadata, lineage tracking, small relational data
- **ClickHouse** — bulk grid data (lat/lon/value), optimized for point queries and time-series

## Object Storage

**Bucket:**

| Bucket          | Purpose                                 |
|-----------------|-----------------------------------------|
| `jackfruit-raw` | Immutable, append-only, source-faithful |

Raw data is stored in object storage. Curated (processed) data is stored in ClickHouse for query efficiency.

**Why this split:**
- Raw files are large, append-only, rarely read — S3/MinIO is ideal
- Curated data needs point queries by coordinates — ClickHouse excels here
- Clear separation: S3 = evidence/archive, ClickHouse = queryable data

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
- Handles relational data (lineage, run history) well

**Role:** Tracks file ingestion and lineage. Does NOT store grid data (that's ClickHouse).

**Schema:** `catalog`

The metadata database tracks all raw files ingested and their transformation lineage. Schema is initialized via `migrations/postgres/init.sql` mounted into the Postgres container.

### Tables

**`catalog.raw_files`** — Raw files ingested from external sources

| Column       | Type          | Description                                                             |
|--------------|---------------|-------------------------------------------------------------------------|
| `id`         | UUID (PK)     | Run ID from ingestion (UUIDv7)                                          |
| `source`     | TEXT          | Data source (e.g., 'ads')                                               |
| `dataset`    | TEXT          | Dataset identifier (e.g., 'cams-europe-air-quality-forecasts-forecast') |
| `date`       | DATE          | Partition date                                                          |
| `s3_key`     | TEXT (UNIQUE) | Full S3 key in `jackfruit-raw` bucket                                   |
| `created_at` | TIMESTAMPTZ   | Record creation timestamp                                               |

### Access Patterns

| Operation        | Query Pattern                                                             |
|------------------|---------------------------------------------------------------------------|
| Ingestion writes | Insert into `raw_files` after S3 upload                                   |
| Serving reads    | Query ClickHouse directly for grid values; Postgres for lineage if needed |
| Lineage queries  | Join CH grid data → `raw_files` over `raw_file_id` to trace provenance    |

**What it stores:**

| Table                  | Purpose                                              |
|------------------------|------------------------------------------------------|
| `catalog.raw_files`    | Raw file metadata, S3 keys, ingestion history        |
| `datasets`             | Source definitions, schemas, refresh schedules (TBD) |

## Grid Data Store (ClickHouse)

**Tech:** ClickHouse

**Why ClickHouse:**
- Column-oriented — only reads columns needed for query
- Excellent compression (10-20x on repetitive lat/lon data)
- Sub-10ms point queries on billions of rows
- Native SQL — simple Go integration
- No CGO dependencies (unlike eccodes for GRIB)

**Role:** Stores curated grid data (environmental values at lat/lon/time). Serves point queries from the API.

**What it stores:**
- Grid data: variable, source, timestamp, lat, lon, value
- One row per grid point per timestamp per variable
- `raw_file_id` reference to postgres `catalog.raw_files` for lineage tracking

**Schema:** TBD — see task 01 (ClickHouse setup) for design guidelines. You design the actual schema.

**Typical query:**
```sql
SELECT value
FROM grid_data
WHERE variable = 'pm2p5'
  AND timestamp = '2025-03-11 14:00:00'
ORDER BY greatCircleDistance(lon, lat, 13.40, 52.52)
LIMIT 1
```

**Local:** ClickHouse runs in container via `docker-compose up`
**Production:** ClickHouse Cloud or self-hosted

## Orchestration

**Tech:** Dagster

**Responsibilities:**
- Schedule ingestion and transformation jobs
- Track asset dependencies and lineage
- Provide observability UI
- Generate run IDs (UUIDv7)

**Job execution model:**

| Layer                   | Execution                                  | Rationale                               |
|-------------------------|--------------------------------------------|-----------------------------------------|
| Ingestion (Go)          | Sibling container via `docker compose run` | Isolated binary, different language     |
| Transformation (Python) | In Dagster container                       | Same runtime, simpler, faster iteration |

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
# Dagster environment
ENV=dev

# External APIs
ADS_BASE_URL=https://ads.atmosphere.copernicus.eu/api/retrieve/v1
ADS_API_KEY=...

# Object Storage (raw files only)
MINIO_ENDPOINT=minio:9000
MINIO_ENDPOINT_URL=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_RAW_BUCKET=jackfruit-raw
MINIO_USE_SSL=false

# Metadata DB (Postgres)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=jackfruit
POSTGRES_PASSWORD=...
POSTGRES_DB=jackfruit

# Grid Data Store (ClickHouse)
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=jackfruit
```

## Production Deployment

**TBD.** Likely:
- Terraform for AWS resources (S3, RDS, ECS)
- Same env var interface, different values
- Dagster Cloud or self-hosted Dagster on ECS

## Open Questions

- [x] Postgres schema design — ✅ Done (`catalog.raw_files`)
- [x] Lineage tracking — ✅ Done (via `raw_file_id` FK)
- [x] Curated data storage — ✅ Decided: ClickHouse (not S3 + GRIB2)
- [ ] ClickHouse schema design — see task 01
- [ ] Production deployment approach
- [ ] Backup/restore strategy
