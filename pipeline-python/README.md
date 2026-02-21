# pipeline_python

Dagster-based data pipeline for ingestion orchestration and transformation.

**Output:** Grid data is written to ClickHouse (replacing previous GRIB2 file output to S3).

## Grid Storage Abstraction

Transformation code uses a `GridStore` ABC (`src/pipeline_python/storage/grid_store.py`):
- `ClickHouseGridStore` — production implementation

This abstraction enables future storage backend swaps. See [ADR 001](../docs/ADR/001-grid-data-storage.md) for the storage decision.

## Getting started

### Installing dependencies

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Install dependencies:

```bash
uv sync
```

### Running Dagster

Start the Dagster UI web server:

```bash
# Local development (standalone, without Docker)
uv run dg dev
```

Open http://localhost:3000 in your browser to see the project.

> **Note:** When running via `docker-compose up`, Dagster runs on **http://localhost:3099** instead (port is mapped in docker-compose.yml).

## Schedules

The pipeline includes a daily schedule that automatically materializes CAMS data:

**`cams_daily_schedule`** — Runs at **08:00 UTC** every day
- Materializes `ingest_cams_data` → `transform_cams_data` for today's partition
- CAMS data is typically available ~6 hours after midnight UTC
- Each run is tagged with the date being processed

### Manual Backfill

To process a specific date range (e.g., historical data):

1. In Dagster UI, navigate to **Assets** → `transform_cams_data`
2. Select the partition(s) you want to materialize
3. Click **Materialize selected**

Dagster will execute `ingest_cams_data` first, then `transform_cams_data` in dependency order.

### GRIB Sanity Check Script

Validate that `grib2io` can read a GRIB file (useful for debugging CAMS data issues):

```bash
# Local
uv run scripts/grib_sanity_check.py path/to/file.grib

# Inside Docker
docker compose run dagster uv run scripts/grib_sanity_check.py data/file.grib
```

The script includes a monkey-patch for PDT 4.40 (Atmospheric Chemical Constituents) which `grib2io` 2.6.0 doesn't support natively. CAMS air quality data uses this template.

> **Note:** GRIB files are read for transformation, but output goes to ClickHouse (not GRIB2 files).

## Testing

### Structure

```
tests/
  unit/          # no external deps
  integration/   # requires Docker infra
  fixtures/      # real GRIB file for integration tests
  .env.test      # test-specific env vars (localhost endpoints, isolated namespaces)
```

### Running

```bash
uv run pytest                  # everything
uv run pytest -m unit          # unit only
uv run pytest -m integration   # integration only
uv run pytest -m integration -s  # integration with stdout (good for debugger)
```

### Integration test infra

Needs the docker-compose stack running (`docker-compose up -d`). Tests use isolated namespaces so they don't touch production data:
- MinIO bucket: `jackfruit-raw-test`
- Postgres schema: `test_catalog`
- ClickHouse DB: `jackfruit_test`

Session-scoped fixtures in `tests/integration/conftest.py` create these on first run. Per-test autouse fixtures truncate them before each test. Tests are auto-skipped if ClickHouse isn't reachable on `localhost:8123`.

The `.env.test` file is loaded by conftest before anything else — no env vars needed in the run config (useful for GoLand debug runs).

### PyCharm debugger

`Run > Edit Configurations > + > pytest`, set:
- Script path: `tests/integration/test_transform_cams.py`
- Additional args: `-m integration -s`
- Working dir: `pipeline-python/`

Set breakpoints anywhere in test or implementation code, run with the bug icon.

## Learn more

To learn more about Dagster:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
