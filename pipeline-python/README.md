# pipeline_python

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
uv run dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Schedules

The pipeline includes a daily schedule that automatically materializes CAMS data:

**`cams_daily_schedule`** — Runs at **08:00 UTC** every day
- Materializes `ingest_cams_data` → `transform_cams_data` for yesterday's partition
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

## Learn more

To learn more about Dagster:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
