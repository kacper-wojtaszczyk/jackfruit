FROM python:3.14-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Keep the project environment outside /app so the bind mount doesn't hide it
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Dependency files first for better cache
COPY pyproject.toml uv.lock ./

# Install runtime deps only
RUN uv sync --frozen --no-dev

# Copy source
COPY src/ ./src/

EXPOSE 3000

# Run Dagster webserver
CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
