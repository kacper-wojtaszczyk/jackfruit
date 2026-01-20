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
