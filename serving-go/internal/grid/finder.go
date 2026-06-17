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

const boxTolerance = float32(0.5)

const lookbackWindow = 24 * time.Hour

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
	var result domain.GridSample
	err := c.conn.QueryRow(
		ctx,
		`
        SELECT value, unit, lat, lon, catalog_id, timestamp
        FROM grid_data FINAL
        WHERE variable = @variable
          AND timestamp = (
            SELECT max(timestamp) FROM grid_data FINAL
            WHERE variable = @variable
              AND timestamp <= @timestamp
              AND timestamp >= @tsFloor
          )
          AND lat BETWEEN @lat - @tol AND @lat + @tol
          AND lon BETWEEN @lon - @tol AND @lon + @tol
        ORDER BY (lat - @lat) * (lat - @lat) + (lon - @lon) * (lon - @lon)
        LIMIT 1
        `,
		clickhouse.Named("variable", variable),
		clickhouse.Named("timestamp", timestamp),
		clickhouse.Named("tsFloor", timestamp.Add(-lookbackWindow)),
		clickhouse.Named("lat", lat),
		clickhouse.Named("lon", lon),
		clickhouse.Named("tol", boxTolerance),
	).Scan(&result.Value, &result.Unit, &result.Lat, &result.Lon, &result.CatalogID, &result.Timestamp)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrGridSampleNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("query clickhouse: %w", err)
	}

	return &result, nil
}
