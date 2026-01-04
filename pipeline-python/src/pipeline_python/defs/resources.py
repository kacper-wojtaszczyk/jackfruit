"""
Container execution resources for local (Docker) and cloud (AWS ECS) environments.

This module provides a unified interface for running the Go ingestion container
across different environments. Uses Dagster Pipes for container orchestration.

NOTE: The Go ingestion container does NOT implement Pipes protocol internally.
We use PipesDockerClient/PipesECSClient in "external process" mode — they just
launch the container, wait for exit, and capture logs. No bidirectional comms.

DOCKER SIBLING PATTERN:
When Dagster runs inside a container with Docker socket mounted, PipesDockerClient
spawns containers as *siblings* on the host Docker daemon (not nested). We must
explicitly configure:
- Network: attach to 'jackfruit' network so container can reach MinIO
- Env vars: pass through from Dagster container's environment
"""
import os
from typing import Protocol

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


class IngestionContainerClient(Protocol):
    """
    Protocol for running the ingestion container.

    Implementations:
    - DockerIngestionClient: Local dev via Docker sibling containers
    - ECSIngestionClient: AWS prod via ECS tasks
    """

    def run_ingestion(
        self,
        context: dg.AssetExecutionContext,
        *,
        dataset: str,
        date: str,
        run_id: str,
    ) -> dg.MaterializeResult:
        """Run the ingestion container with the given parameters."""
        ...


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


class ECSIngestionClient(dg.ConfigurableResource):
    """
    Run ingestion via AWS ECS using dagster-aws PipesECSClient.

    Launches an ECS task with the ingestion container. Requires:
    - ECS task definition with ingestion container image
    - AWS credentials configured (via IAM role or env vars)
    - CloudWatch logs configured for task output

    Config:
        task_definition: ECS task definition name or ARN
        cluster: ECS cluster name (optional)
        container_name: Name of container in task definition to override
        subnets: List of subnet IDs for awsvpc networking
        security_groups: List of security group IDs
    """

    task_definition: str
    container_name: str = "ingestion"
    cluster: str = ""
    subnets: list[str] = []
    security_groups: list[str] = []

    def run_ingestion(
        self,
        context: dg.AssetExecutionContext,
        *,
        dataset: str,
        date: str,
        run_id: str,
    ) -> dg.MaterializeResult:
        """
        Run ingestion as ECS task.

        TODO: Implement using PipesECSClient

        Steps:
        1. Import PipesECSClient from dagster_aws.pipes
        2. Create client instance
        3. Build run_task_params:
           - taskDefinition: self.task_definition
           - cluster: self.cluster (if set)
           - networkConfiguration: subnets + security groups (if awsvpc)
           - overrides.containerOverrides: command args
        4. Call client.run(context=context, run_task_params=run_task_params)
        5. Return MaterializeResult with metadata

        Example run_task_params:
        {
            "taskDefinition": "jackfruit-ingestion",
            "cluster": "jackfruit-cluster",
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-xxx"],
                    "securityGroups": ["sg-xxx"],
                    "assignPublicIp": "ENABLED",
                }
            },
            "overrides": {
                "containerOverrides": [{
                    "name": "ingestion",
                    "command": ["--dataset=...", "--date=...", "--run-id=..."],
                }]
            },
        }

        Env vars should be in task definition or use Secrets Manager references.
        """
        raise NotImplementedError("TODO: Implement ECSIngestionClient.run_ingestion")


# -----------------------------------------------------------------------------
# Resource Definitions - Wired up for use by assets
# -----------------------------------------------------------------------------

@dg.definitions
def ingestion_resources():
    """
    Register the ingestion_client resource.

    Uses DockerIngestionClient for local development.
    For AWS production, swap to ECSIngestionClient.
    """
    return dg.Definitions(
        resources={
            "ingestion_client": DockerIngestionClient(
                image="jackfruit-ingestion:latest",
                network="jackfruit_jackfruit",
            ),
        }
    )


