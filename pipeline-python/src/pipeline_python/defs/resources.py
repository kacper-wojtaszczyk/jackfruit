"""
Container execution resources for local (Docker) and cloud (AWS ECS) environments.

This module provides a unified interface for running the Go ingestion container
across different environments. Uses Dagster Pipes for container orchestration.

NOTE: The Go ingestion container does NOT implement Pipes protocol internally.
We use PipesDockerClient in "external process" mode — it just launches the
container, waits for exit, and captures logs. No bidirectional comms.

DOCKER SIBLING PATTERN:
When Dagster runs inside a container with Docker socket mounted, PipesDockerClient
spawns containers as *siblings* on the host Docker daemon (not nested). We must
explicitly configure:
- Network: attach to 'jackfruit' network so container can reach MinIO
- Env vars: pass through from Dagster container's environment

DEPRECATION NOTICE:
This Go-based ingestion will be replaced with Python-native ingestion using cdsapi.
See docs/layer-1-ingestion.md for details.
"""
import os

from dagster_docker import PipesDockerClient

import dagster as dg


# Environment variables to forward to the ingestion container
_INGESTION_ENV_VARS = [
    "ADS_BASE_URL",
    "ADS_API_KEY",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_BUCKET",
    "MINIO_USE_SSL",
]


class DockerIngestionClient(dg.ConfigurableResource):
    """
    Run ingestion via local Docker using dagster-docker PipesDockerClient.

    Spawns the ingestion container as a sibling on the host Docker daemon.
    Requires docker socket mounted at /var/run/docker.sock.

    Config:
        image: Docker image name (e.g., 'jackfruit-ingestion:latest')
        network: Docker network to attach to (e.g., 'jackfruit_jackfruit')
    """

    image: str = "jackfruit-ingestion:latest"
    network: str = "jackfruit_jackfruit"  # docker-compose prefixes with project name

    def _get_forwarded_env(self) -> dict[str, str]:
        """Collect environment variables to forward to the ingestion container."""
        env = {}
        for var in _INGESTION_ENV_VARS:
            value = os.environ.get(var)
            if value is not None:
                env[var] = value
        return env

    def run_ingestion(
        self,
        context: dg.AssetExecutionContext,
        *,
        dataset: str,
        date: str,
        run_id: str,
    ) -> dg.MaterializeResult:
        """
        Run ingestion container as Docker sibling.

        Uses PipesDockerClient to spawn a sibling container on the host Docker daemon.
        The container is attached to the configured network so it can reach MinIO.
        Environment variables are forwarded from the Dagster container.
        """
        client = PipesDockerClient()

        # Build command arguments for the ingestion CLI
        command = [
            f"--dataset={dataset}",
            f"--date={date}",
            f"--run-id={run_id}",
        ]

        # Forward env vars and set network for sibling container
        container_kwargs = {
            "network": self.network,
        }

        context.log.info(
            f"Running ingestion container: image={self.image}, "
            f"network={self.network}, dataset={dataset}, date={date}"
        )

        # Run the container and wait for completion
        # PipesDockerClient handles: create -> start -> wait -> stop
        # Logs are streamed to Dagster's log output
        client.run(
            context=context,
            image=self.image,
            command=command,
            env=self._get_forwarded_env(),
            container_kwargs=container_kwargs,
        )

        return dg.MaterializeResult(
            metadata={
                "run_id": run_id,
                "dataset": dataset,
                "date": date,
                "image": self.image,
                "network": self.network,
            }
        )


# -----------------------------------------------------------------------------
# Resource Definitions - Wired up for use by assets
# -----------------------------------------------------------------------------

@dg.definitions
def ingestion_resources():
    """
    Register the ingestion_client resource.

    Uses DockerIngestionClient for local development.
    """
    return dg.Definitions(
        resources={
            "ingestion_client": DockerIngestionClient(
                image="jackfruit-ingestion:latest",
                network="jackfruit_jackfruit",
            ),
        }
    )
