package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	clickhouseraw "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/clickhouse"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/config"
)

func NewClient(t *testing.T) *clickhouse.Client {
	t.Helper()

	cfg := config.Load()
	client, err := clickhouse.NewClient(clickhouse.Config{
		Host:     cfg.ClickHouseHost,
		Port:     cfg.ClickHousePort,
		User:     cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
		Database: cfg.ClickHouseDatabase,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}

func NewRawConn(t *testing.T) chdriver.Conn {
	t.Helper()

	cfg := config.Load()
	conn, err := clickhouseraw.Open(&clickhouseraw.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouseraw.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// InsertGridRow inserts a single row into grid_data and registers cleanup.
// It generates and returns the catalog_id so callers can assert on it.
func InsertGridRow(t *testing.T, conn chdriver.Conn, variable string, value float32, unit string,
	timestamp time.Time, lat, lon float32) uuid.UUID {
	t.Helper()

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Exec(t.Context(), `
		INSERT INTO grid_data (variable, timestamp, lat, lon, value, unit, catalog_id)
		VALUES (@variable, @timestamp, @lat, @lon, @value, @unit, @catalog_id)
	`,
		clickhouseraw.Named("variable", variable),
		clickhouseraw.Named("timestamp", timestamp),
		clickhouseraw.Named("lat", lat),
		clickhouseraw.Named("lon", lon),
		clickhouseraw.Named("value", value),
		clickhouseraw.Named("unit", unit),
		clickhouseraw.Named("catalog_id", catalogID),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = conn.Exec(context.Background(),
			"ALTER TABLE grid_data DELETE WHERE catalog_id = @catalog_id",
			clickhouseraw.Named("catalog_id", catalogID),
		)
	})

	return catalogID
}
