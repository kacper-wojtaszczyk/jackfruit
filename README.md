# Jackfruit

Environmental data platform. Ingests, transforms, and serves weather, air quality, hydrology, and vegetation data.

## Status

**Early development.** Core pipeline taking shape:

- [x] Architecture defined (5 layers)
- [x] Storage strategy decided (MinIO raw/curated buckets)
- [ ] Go ingestion — in progress (CAMS adapter)
- [ ] Python ETL — not started
- [ ] Serving API — not started

## Quick Start

```bash
# Start local infrastructure
docker-compose up -d  # MinIO, (future: ClickHouse, Dagster)

# MinIO console
open http://localhost:9001  # minioadmin / minioadmin

# Create buckets (first time only)
# - jackfruit-raw
# - jackfruit-curated
```

## Architecture

```
External APIs → [Ingestion/Go] → jackfruit-raw (MinIO)
                                      ↓
                              [ETL/Python+Dagster]
                                      ↓
                              jackfruit-curated (MinIO)
                                      ↓
                              [Serving/Go] → Clients
```

| Layer | Tech | Status |
|-------|------|--------|
| Ingestion | Go | 🚧 In progress |
| Raw Storage | MinIO/S3 | ✅ Ready |
| ETL | Python + Dagster | ⏳ Planned |
| Warehouse | ClickHouse | ⏸️ On-hold |
| Serving | Go + DuckDB | ⏳ Planned |

See `docs/` for layer details.

## Project Structure

```
jackfruit/
├── ingestion-go/      # Go — fetch external data → raw bucket
├── etl-python/       # Python + Dagster — ETL
├── serving-go/        # Go — API for clients
├── infra/          # MinIO, ClickHouse config
└── docs/           # Architecture docs
```

## Data Sources (Current Targets)

| Source | Type | Status |
|--------|------|--------|
| Copernicus CAMS | Air quality | 🚧 In progress |
| Copernicus GloFAS | Hydrology | ⏳ Next |
| ERA5 (public S3) | Weather | ⏳ ETL target |

## License

TBD

