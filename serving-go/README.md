# serving-go

Go HTTP service for querying environmental data from the Jackfruit platform.

## Status

| Component | Status |
|-----------|--------|
| Health endpoint (`/health`) | ✅ Done |
| Grid retriever (ClickHouse-backed) | ✅ Done |
| Environmental endpoint (`/v1/environmental`) | ✅ Done |
| Lineage retriever (Postgres-backed) | ✅ Done |

## Running

```bash
# Start infrastructure
docker-compose up -d clickhouse postgres

# Run server (default port 8080)
go run ./cmd/serving

# Build binary
go build -o bin/serving ./cmd/serving
```

## Testing

```bash
make test-short      # Unit tests only (no infra needed)
make test            # All tests including integration (requires ClickHouse)
```

## API Reference

Full design rationale, response shape, and SQL query: [docs/layer-3-serving.md](../docs/layer-3-serving.md)

### `GET /health`

Returns `204 No Content`. Liveness check for container orchestration.

### `GET /v1/environmental`

```
GET /v1/environmental?lat=52.52&lon=13.40&timestamp=2025-03-12T14:55:00Z&variables=pm2p5,pm10
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | Yes | Latitude (−90 to 90) |
| `lon` | float | Yes | Longitude (−180 to 180) |
| `timestamp` | ISO 8601 UTC | Yes | Requested timestamp |
| `variables` | string | Yes | Comma-separated variable names |

Errors return `{"error": "..."}` with HTTP status codes: 400 (invalid/missing params), 404 (variable not found), 500 (internal error).

Fails the entire request if any requested variable is not found — no partial responses.
