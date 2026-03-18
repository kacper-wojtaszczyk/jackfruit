FROM python:3.14-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /usr/local/bin/uv

# Keep the project environment outside /app so the bind mount doesn't hide it
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Dependency files first for better cache
COPY pyproject.toml uv.lock ./

# Install runtime deps only (not the project itself — source isn't copied yet)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY src/ ./src/

# Install the project package now that source is available
RUN uv sync --frozen --no-dev

# Dagster home for run storage and schedule state
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

# Run as non-root
RUN useradd --system --create-home --home-dir /home/dagster dagster && \
    chown -R dagster:dagster /opt/dagster /home/dagster
USER dagster

# Run Dagster daemon (K8s deployment may override this command)
CMD ["dagster-daemon", "run"]
