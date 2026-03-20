# Jackfruit

Environmental data platform. Ingests, transforms, and serves weather, air quality, hydrology, and vegetation data.

## Status

**Early development:**

- [x] Architecture defined (infrastructure + 3 processing layers)
- [x] Storage strategy decided (MinIO raw bucket + ClickHouse for curated)
- [x] Dagster orchestration setup
- [x] Metadata DB (Postgres)
- [x] ClickHouse setup
- [x] Python-native ingestion — CAMS (cdsapi) + ECMWF Open Data (ecmwf-opendata)
- [x] Transformation — CAMS (PM2.5, PM10) + ECMWF (temperature, humidity)
- [x] Serving API (Go, `GET /v1/environmental`)
- [x] Serving API - Lineage

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
│  (Python)    │     │   (Python)   │     │    (Go)      │
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
| L1: Ingestion         | Python + cdsapi + ecmwf-opendata | ✅ Active (CAMS + ECMWF) |
| L2: Transformation    | Python + Dagster | ✅ Active (CAMS + ECMWF) |
| L3: Serving           | Go               | ✅ Active             |

See `docs/` for details. Key decisions are documented in `docs/ADR/`.

## Project Structure

```
jackfruit/
├── pipeline-python/    # Dagster orchestration — ingestion + ETL assets
├── serving-go/         # Go HTTP api serving the data from CH
├── docs/               # Architecture docs
└── docker-compose.yml  # MinIO, Postgres, Dagster
```

## Data Sources (Current Targets)

| Source            | Type                        | Status                   |
|-------------------|-----------------------------|--------------------------|
| Copernicus CAMS   | Air quality (PM2.5, PM10)   | ✅ Implemented            |
| ECMWF Open Data   | Weather (temperature, humidity) | ✅ Implemented        |

## License

TBD

