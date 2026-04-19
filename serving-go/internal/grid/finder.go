package grid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

// spatialBoxDeg must be >= the half-diagonal of the coarsest grid cell so the
// nearest grid point is always inside the bounding box — otherwise a query
// exactly between four grid points would miss all of them and yield a false
// ErrSpatialMiss. Coarsest current grid is ECMWF at 0.25° → half-diagonal
// 0.125 * sqrt(2) ≈ 0.177°. 0.3° gives ~70% headroom; widen or make
// per-source if a coarser data source is added.
const spatialBoxDeg = 0.3

type Finder struct {
	conn driver.Conn
}

func NewFinder(conn driver.Conn) *Finder {
	return &Finder{conn: conn}
}

func (c *Finder) GetSample(
	ctx context.Context,
	variable string,
	timestamp time.Time,
	lat float32,
	lon float32,
) (*domain.GridSample, error) {
	// FINAL is intentionally omitted: max(timestamp) is invariant to
	// ReplacingMergeTree duplicates because they share the ORDER BY key
	// (variable, timestamp, lat, lon), so duplicates agree on timestamp.
	const resolveTimestampQuery = `
        SELECT maxOrNull(timestamp)
        FROM grid_data
        WHERE variable = @variable AND timestamp <= @timestamp
    `
	var resolvedTS *time.Time
	err := c.conn.QueryRow(
		ctx,
		resolveTimestampQuery,
		clickhouse.Named("variable", variable),
		clickhouse.Named("timestamp", timestamp),
	).Scan(&resolvedTS)
	if err != nil {
		return nil, fmt.Errorf("resolve timestamp: %w", err)
	}
	if resolvedTS == nil {
		return nil, &domain.ErrTemporalMiss{Variable: variable}
	}

	// FINAL required here: value and catalog_id differ between unmerged
	// duplicates, so we need the replaced row.
	const spatialLookupQuery = `
        SELECT value, unit, lat, lon, catalog_id, timestamp
        FROM grid_data FINAL
        WHERE variable = @variable
          AND timestamp = @timestamp
          AND lat BETWEEN @lat - @tol AND @lat + @tol
          AND lon BETWEEN @lon - @tol AND @lon + @tol
        ORDER BY (lat - @lat) * (lat - @lat) + (lon - @lon) * (lon - @lon)
        LIMIT 1
    `
	var result domain.GridSample
	err = c.conn.QueryRow(
		ctx,
		spatialLookupQuery,
		clickhouse.Named("variable", variable),
		clickhouse.Named("timestamp", *resolvedTS),
		clickhouse.Named("lat", lat),
		clickhouse.Named("lon", lon),
		clickhouse.Named("tol", float32(spatialBoxDeg)),
	).Scan(&result.Value, &result.Unit, &result.Lat, &result.Lon, &result.CatalogID, &result.Timestamp)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, &domain.ErrSpatialMiss{Variable: variable}
	}
	if err != nil {
		return nil, fmt.Errorf("spatial lookup: %w", err)
	}

	return &result, nil
}
