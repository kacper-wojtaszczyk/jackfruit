package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

type Client struct {
	conn driver.Conn
}

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

func NewClient(cfg Config, logger *slog.Logger) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Logger: logger,
		Settings: clickhouse.Settings{
			"max_execution_time": 15,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return &Client{conn: conn}, nil
}

func (c *Client) GetValue(
	ctx context.Context,
	variable string,
	timestamp time.Time,
	lat float32,
	lon float32,
) (*domain.GridValue, error) {
	var result domain.GridValue

	err := c.conn.QueryRow(
		ctx,
		`
		SELECT value, unit, lat, lon, catalog_id, timestamp
        FROM grid_data FINAL
        WHERE variable = @variable
          AND timestamp = (
            SELECT max(timestamp) FROM grid_data FINAL
            WHERE variable = @variable AND timestamp <= @timestamp
          )
        ORDER BY (lat - @lat) * (lat - @lat) + (lon - @lon) * (lon - @lon)
        LIMIT 1
        `,
		clickhouse.Named("variable", variable),
		clickhouse.Named("timestamp", timestamp),
		clickhouse.Named("lat", lat),
		clickhouse.Named("lon", lon),
	).Scan(&result.Value, &result.Unit, &result.Lat, &result.Lon, &result.CatalogID, &result.Timestamp)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrGridValueNotFound
	}

	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
