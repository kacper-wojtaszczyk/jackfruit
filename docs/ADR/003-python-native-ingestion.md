# ADR 003: Python-Native CDS API Ingestion

## Status

Accepted

## Context

The `ingest_cams_data` Dagster asset used `DockerIngestionClient` (from `dagster-docker`) to spawn the Go ingestion binary (`ingestion-go/`) as a sibling container on the host Docker daemon. This required:

1. The host Docker socket (`/var/run/docker.sock`) mounted into the Dagster container
2. The `DOCKER_NETWORK` environment variable so the spawned container could reach MinIO
3. `docker.io` installed in the Dagster Docker image
4. A separately built `jackfruit-ingestion:latest` Docker image containing the Go binary

For local development this worked. For Kubernetes deployment on Scaleway Kapsule, it is problematic: mounting the node's Docker socket into a pod is a security risk (effectively grants root on the node), and Kapsule's mutualized control plane does not guarantee a Docker runtime on nodes — containerd is the default. The alternative — Docker-in-Docker with `--privileged` — adds operational complexity and is discouraged on managed clusters.

The Go binary's core job was implementing the CDS API async flow: submit a job, poll for completion, download the result. The `cdsapi` Python library provides exactly this in two lines. With no remaining justification for a separate Go process in the pipeline layer, we have an opportunity to simplify.

### Options Considered

**Option A: Keep Go + Docker-in-Docker**

Retain the Go binary and run DinD inside the Dagster pod.

- ✅ No code changes to the ingestion logic
- ❌ Requires `--privileged` SecurityContext or a DinD sidecar
- ❌ Adds ~200MB to the Dagster pod (Docker daemon) and startup latency
- ❌ Managed K8s clusters increasingly move away from Docker runtime — this is swimming against the current

**Option B: Go ingestion as a Kubernetes Job**

Replace the Docker sibling with a Dagster-managed K8s Job that runs the Go binary as a separate pod (using `dagster-k8s` `PipesK8sClient`).

- ✅ No Docker socket needed — native K8s pattern
- ⚠️ Requires ServiceAccount, Role, and RoleBinding for Dagster to create Jobs
- ⚠️ Maintains two languages (Go + Python) in the pipeline layer
- ⚠️ The Go async polling loop reimplements what `cdsapi` already provides
- ⚠️ Still needs a separately built and pushed Go container image

**Option C: Python-native `cdsapi`**

Replace the Go binary entirely. Call the CDS API directly from the Dagster asset using the `cdsapi` Python library.

- ✅ No Docker socket, no sidecar, no separate container
- ✅ Single language stack for the entire pipeline layer
- ✅ `cdsapi` handles submit/poll/download internally — replaces ~300 lines of Go
- ✅ `dagster-docker` dependency removed
- ⚠️ Python is slower than Go for CPU-bound work (irrelevant here — CDS API latency dominates)

## Decision

Use **Python-native `cdsapi`** (Option C).

The Go ingestion binary's value was encapsulating the CDS API async flow. The `cdsapi` Python library provides this identically. With no remaining justification for Go in the pipeline layer, and with the K8s deployment target making Docker socket access unacceptable, the simplest path is to eliminate the indirection entirely.

The new flow: `CdsClient` (a `ConfigurableResource` wrapping `cdsapi`) retrieves the GRIB file to a local temp path, then `ObjectStore.upload_raw()` puts it in MinIO. The asset itself orchestrates both calls — no container spawning involved.

`ingestion-go/` is deleted entirely.

## Consequences

### Positive

- **Docker socket dependency eliminated** — Dagster pod needs no privileged access on Kapsule
- **`ingestion-go/` deleted** — removes ~500 lines of Go, a multi-stage Dockerfile, and a `go.work` module entry
- **`dagster-docker` removed** from `pyproject.toml` — one fewer transitive dependency tree
- **`docker.io` removed** from Dagster Dockerfile — smaller image, faster builds
- **Single language stack** — the entire pipeline layer is Python, simplifying debugging and onboarding
- **Simpler `docker-compose.yml`** — no Docker socket mount, no `DOCKER_HOST`, no `DOCKER_NETWORK`

### Negative

- Python is slower than Go for CPU-bound work. This is acceptable: the CDS API queue time (5–30 minutes) dominates total execution time. Local computation is negligible.

## References

- `CdsClient`: `src/pipeline_python/ingestion/cds_client.py`
- Replaced Go code: `ingestion-go/` (deleted)
- K8s architecture: `docs/buttprint-infrastructure.md` (Kapsule deployment plan)
- ADR 001: Grid Data Storage — `GridStore` abstraction pattern
- ADR 002: GRIB Library — similar "simplify by switching to the right tool" decision
