# Jackfruit

Environmental data platform. Ingests, transforms, and serves weather, air quality, hydrology, and vegetation data.

## Status

**Early development.** Core pipeline taking shape:

- [x] Architecture defined (5 layers)
- [x] Storage strategy decided (MinIO raw/curated buckets)
- [x] Go ingestion CLI (CAMS adapter working)
- [x] Dagster orchestration setup
- [x] Ingestion asset (runs Go CLI via docker compose)
- [ ] Python ETL assets — not started
- [ ] Serving API — not started

## Quick Start

```bash
# Copy and configure secrets (first time)
cp .env.example .env
# Edit .env with your API keys and credentials

# Start MinIO
docker-compose up -d

# MinIO console: http://localhost:9001 (minioadmin / minioadmin)
# Create buckets (first time): jackfruit-raw, jackfruit-curated

# Start Dagster (orchestration UI)
cd pipeline-python
uv sync
dg dev
# Dagster UI: http://localhost:3000
```

## Architecture

```
External APIs → [Ingestion/Go] → jackfruit-raw (MinIO)
                                      ↓
                         [ETL/Python + Dagster orchestration]
                                      ↓
                              jackfruit-curated (MinIO)
                                      ↓
                              [Serving/Go + DuckDB] → Clients
```

| Layer | Tech | Status |
|-------|------|--------|
| Ingestion | Go CLI | ✅ Active (CAMS) |
| Raw Storage | MinIO/S3 | ✅ Active |
| Orchestration | Dagster | ✅ Active |
| ETL | Python + Dagster | 🚧 In progress |
| Warehouse | ClickHouse | ⏸️ Deferred |
| Serving | Go + DuckDB | ⏳ Planned |

See `docs/` for layer details.

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

