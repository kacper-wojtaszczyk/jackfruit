# serving-go

Go HTTP service for querying environmental data from the Jackfruit platform.

## Status

| Component | Status |
|-----------|--------|
| Health endpoint (`/health`) | ✅ Done |
| ClickHouse client + GridStore | ✅ Done |
| Environmental endpoint (`/v1/environmental`) | ⏳ Next |
| Postgres catalog integration | ⏳ Planned |

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
GET /v1/environmental?lat=52.52&lon=13.40&time=2025-03-12T14:55:00Z&vars=pm2p5,pm10
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | Yes | Latitude (−90 to 90) |
| `lon` | float | Yes | Longitude (−180 to 180) |
| `time` | ISO 8601 UTC | Yes | Requested timestamp |
| `vars` | string | Yes | Comma-separated variable names |

**Error codes:**

| Code | HTTP | Description |
|------|------|-------------|
| `INVALID_REQUEST` | 400 | Missing or invalid parameters |
| `VARIABLE_NOT_FOUND` | 404 | No data for a variable within time tolerance |
| `INTERNAL_ERROR` | 500 | ClickHouse or Postgres failure |

Fails the entire request if any requested variable is not found — no partial responses.
