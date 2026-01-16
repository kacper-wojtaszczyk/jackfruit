# Jackfruit

Environmental data platform. Ingests, transforms, and serves weather, air quality, hydrology, and vegetation data.

## Status

**Early development:**

- [x] Architecture defined (infrastructure + 3 processing layers)
- [x] Storage strategy decided (MinIO raw/curated buckets)
- [x] Go ingestion CLI (CAMS adapter working)
- [x] Dagster orchestration setup
- [x] Ingestion asset (runs Go CLI via docker compose)
- [ ] CAMS transformation asset — [in progress](docs/impl-cams-transformation.md)
- [ ] Metadata DB (Postgres) — not started
- [ ] Serving API — not started

## Quick Start

```bash
# Copy and configure secrets (first time)
cp .env.example .env
# Edit .env with your API keys and credentials (ask kacper)

# Start MinIO and Dagster
docker-compose up -d

# MinIO console: http://localhost:9098 (minioadmin / minioadmin)
# Create buckets (first time): jackfruit-raw, jackfruit-curated (or not, they will auto-create)
# Dagster UI: http://localhost:3099
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      INFRASTRUCTURE                         │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│   │ Object Store │  │  Metadata DB │  │   Dagster    │      │
│   │ (MinIO / S3) │  │  (Postgres)  │  │              │      │
│   └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
        │                    │                   │
        ▼                    ▼                   ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ L1: Ingest   │ ──▶ │ L2: Transform│ ──▶ │ L3: Serving  │
│    (Go)      │     │   (Python)   │     │    (Go)      │
└──────────────┘     └──────────────┘     └──────────────┘
```

| Component | Tech | Status |
|-----------|------|--------|
| **Infrastructure** |||
| Object Storage | MinIO / S3 | ✅ Active |
| Metadata DB | Postgres | ⏳ Planned |
| Orchestration | Dagster | ✅ Active |
| **Processing Layers** |||
| L1: Ingestion | Go CLI | ✅ Active (CAMS) |
| L2: Transformation | Python + Dagster | 🚧 In progress |
| L3: Serving | Go | ⏳ Planned |

See `docs/` for details.

## Project Structure

```
jackfruit/
├── ingestion-go/       # Go CLI — fetch external data → raw bucket
├── pipeline-python/    # Dagster orchestration + ETL assets
├── docs/               # Architecture docs
└── docker-compose.yml  # MinIO
```

## Data Sources (Current Targets)

| Source | Type | Status                  |
|--------|------|-------------------------|
| Copernicus CAMS | Air quality | ✅ Implemented ingestion |
| Copernicus GloFAS | Hydrology | ⏳ Next                  |
| ERA5 (public S3) | Weather | ⏳ ETL target            |

## License

TBD

