import subprocess
import uuid
from datetime import datetime
from pathlib import Path

import dagster as dg


class IngestionConfig(dg.Config):
    """Configuration for the ingestion asset."""
    date: str | None = None
    dataset: str = "cams-europe-air-quality-forecasts-analysis"


@dg.asset
def ingest_cams_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig
) -> dg.MaterializeResult:
    """
    Run the ingestion service via docker compose to fetch and store raw data.

    This asset spawns the Go ingestion CLI container, passing date, dataset,
    and a generated UUIDv7 run-id. Data is written to the jackfruit-raw bucket.
    """
    # Generate UUIDv7 run-id
    run_id = str(uuid.uuid7())

    # Use today's date if not provided
    date = config.date if config.date else datetime.now().strftime("%Y-%m-%d")

    context.log.info(
        f"Starting ingestion: dataset={config.dataset}, date={date}, run_id={run_id}"
    )

    # Determine project root (3 levels up from this file)
    project_root = Path(__file__).parent.parent.parent.parent.parent

    # Build docker compose command
    cmd = [
        "docker", "compose", "run", "--rm",
        "--env-from-file", ".env",
        "ingestion",
        "--date", date,
        "--dataset", config.dataset,
        "--run-id", run_id
    ]

    context.log.info(f"Running command: {' '.join(cmd)}")
    context.log.info(f"Working directory: {project_root}")

    # Execute the command
    result = subprocess.run(
        cmd,
        cwd=project_root,
        capture_output=True,
        text=True
    )

    # Log stdout and stderr
    if result.stdout:
        context.log.info(f"Container stdout:\n{result.stdout}")
    if result.stderr:
        context.log.warning(f"Container stderr:\n{result.stderr}")

    # Check exit code
    if result.returncode != 0:
        raise dg.Failure(
            description=f"Ingestion container failed with exit code {result.returncode}",
            metadata={
                "exit_code": result.returncode,
                "run_id": run_id,
                "dataset": config.dataset,
                "date": date,
                "stderr": result.stderr[-1000:] if result.stderr else ""  # Last 1000 chars
            }
        )

    context.log.info(f"Ingestion completed successfully: run_id={run_id}")

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "dataset": config.dataset,
            "date": date,
            "exit_code": result.returncode
        }
    )
