FROM python:3.14-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install docker CLI so Dagster can spawn sibling containers via host Docker socket
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Dependency files first for better cache
COPY pyproject.toml uv.lock ./

# Install runtime deps only
RUN uv sync --frozen --no-dev

# Copy source
COPY src/ ./src/

EXPOSE 3000

# Run Dagster webserver
CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]

