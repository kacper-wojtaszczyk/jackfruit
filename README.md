# Jackfruit

Environmental data platform. Ingests, transforms, and serves weather, air quality, hydrology, and vegetation data.

## Status

**Early development:**

- [x] Architecture defined (infrastructure + 3 processing layers)
- [x] Storage strategy decided (MinIO raw bucket + ClickHouse for curated)
- [x] Go ingestion CLI (CAMS adapter working)
- [x] Dagster orchestration setup
- [x] Ingestion asset (runs Go CLI via docker compose)
- [x] CAMS transformation asset — needs migration to ClickHouse
- [x] Metadata DB (Postgres)
- [x] ClickHouse setup
- [x] Transform to ClickHouse
- [ ] Serving API — planned

## Quick Start

```bash
# Copy and configure secrets (first time)
cp .env.example .env
# Edit .env with your API keys and credentials (ask kacper)

# Start infrastructure (MinIO, Postgres, ClickHouse, Dagster)
docker-compose up -d

# MinIO console: http://localhost:9098 (minioadmin / minioadmin)
# Create bucket (first time): jackfruit-raw (or let it auto-create)
# Dagster UI: http://localhost:3099
```

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                          INFRASTRUCTURE                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │   MinIO    │  │  Postgres  │  │ ClickHouse │  │  Dagster   │       │
│  │ (raw data) │  │ (metadata) │  │(grid data) │  │            │       │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘       │
└───────────────────────────────────────────────────────────────────────┘
        │                │                │                │
        ▼                ▼                ▼                ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ L1: Ingest   │ ──▶ │ L2: Transform│ ──▶ │ L3: Serving  │
│    (Go)      │     │   (Python)   │     │    (Go)      │
└──────────────┘     └──────────────┘     └──────────────┘
```

| Component             | Tech             | Status              |
|-----------------------|------------------|---------------------|
| **Infrastructure**    |                  |                     |
| Object Storage        | MinIO / S3       | ✅ Active (raw only) |
| Metadata DB           | Postgres         | ✅ Active            |
| Grid Data Store       | ClickHouse       | ✅ Active            |
| Orchestration         | Dagster          | ✅ Active            |
| **Processing Layers** |                  |                     |
| L1: Ingestion         | Go CLI           | ✅ Active (CAMS)     |
| L2: Transformation    | Python + Dagster | ✅ Active (CAMS)     |
| L3: Serving           | Go               | ⏳ Planned           |

See `docs/` for details. Key decisions are documented in `docs/ADR/`.

## Project Structure

```
jackfruit/
├── ingestion-go/       # Go CLI — fetch external data → raw bucket
├── pipeline-python/    # Dagster orchestration + ETL assets
├── docs/               # Architecture docs
└── docker-compose.yml  # MinIO, Postgres, Dagster
```

## Data Sources (Current Targets)

| Source            | Type        | Status                  |
|-------------------|-------------|-------------------------|
| Copernicus CAMS   | Air quality | ✅ Implemented ingestion |
| Copernicus GloFAS | Hydrology   | ⏳ Next                  |
| ERA5 (public S3)  | Weather     | ⏳ ETL target            |

## License

TBD

