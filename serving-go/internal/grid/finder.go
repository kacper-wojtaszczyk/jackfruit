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
	const resolveTimestampQuery = `
        SELECT maxOrNull(timestamp)
        FROM grid_data FINAL
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
		return nil, domain.ErrTemporalMiss
	}

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
		return nil, domain.ErrSpatialMiss
	}
	if err != nil {
		return nil, fmt.Errorf("spatial lookup: %w", err)
	}

	return &result, nil
}
