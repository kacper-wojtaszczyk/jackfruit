# Jackfruit

Environmental data platform. Ingests, transforms, and serves gridded weather and air quality data from public meteorological datasets.

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
│  (Python)    │     │   (Python)   │     │    (Go)      │
└──────────────┘     └──────────────┘     └──────────────┘
```

| Component             | Tech                                  |
|-----------------------|---------------------------------------|
| **Infrastructure**    |                                       |
| Object Storage        | MinIO / S3                            |
| Metadata DB           | Postgres                              |
| Grid Data Store       | ClickHouse                            |
| Orchestration         | Dagster                               |
| **Processing Layers** |                                       |
| L1: Ingestion         | Python + cdsapi + ecmwf-opendata      |
| L2: Transformation    | Python + Dagster                      |
| L3: Serving           | Go                                    |

## Data Sources

| Source            | Type                              |
|-------------------|-----------------------------------|
| Copernicus CAMS   | Air quality (PM2.5, PM10)         |
| ECMWF Open Data   | Weather (temperature, humidity)  |

## Project Structure

```
jackfruit/
├── pipeline-python/    # Dagster orchestration — ingestion + ETL assets
│   └── migrations/     # Postgres + ClickHouse schema
├── serving-go/         # Go HTTP API serving grid data
├── docs/               # Architecture docs + ADRs
└── docker-compose.yml  # Local development stack
```

## Quick Start

```bash
# Copy and configure secrets
cp .env.example .env

# Start local development stack
docker-compose up -d

# Dagster UI: http://localhost:3099
# MinIO console: http://localhost:9098 (minioadmin / minioadmin)
```

## Documentation

See `docs/` for layer-by-layer architecture docs and `docs/ADR/` for architectural decision records.

## Related Repos

Part of the [Climacterium](https://github.com/kacper-wojtaszczyk?tab=repositories) ecosystem:

| Repo | Description |
|------|-------------|
| [buttprint-api](https://github.com/kacper-wojtaszczyk/buttprint-api) | Atmospheric scoring API + SVG butt generation (Go) |
| [buttprint-fe](https://github.com/kacper-wojtaszczyk/buttprint-fe) | Display layer (SvelteKit) |
| [climacterium-infra](https://github.com/kacper-wojtaszczyk/climacterium-infra) | Terraform + Kubernetes deployment (Scaleway) |
