# Ingestion (Go)

Go CLI for fetching raw environmental data from external APIs and storing in MinIO.

## Documentation

See [docs/layer-1-ingestion.md](../docs/layer-1-ingestion.md) for:
- Architecture and responsibilities
- CLI usage and flags
- Docker usage
- CDS API pattern
- Data sources

## Quick Start

```bash
# Build
make build

# Run (requires MinIO and env vars configured)
./bin/ingestion \
  --dataset=cams-europe-air-quality-forecasts-analysis \
  --date=2025-03-12 \
  --run-id=01890c24-905b-7122-b170-b60814e6ee06
```

## Migration Notice

This Go CLI will be replaced with native Python ingestion using `cdsapi`. See the [layer-1 docs](../docs/layer-1-ingestion.md#migration-notice-go--python) for rationale.
