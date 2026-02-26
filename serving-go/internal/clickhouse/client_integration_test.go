package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	clickhouseraw "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/clickhouse"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/config"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

func newClient(t *testing.T) *clickhouse.Client {
	t.Helper()

	cfg := config.Load()
	client, err := clickhouse.NewClient(clickhouse.Config{
		Host:     cfg.ClickHouseHost,
		Port:     cfg.ClickHousePort,
		User:     cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
		Database: cfg.ClickHouseDatabase,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}

func newRawConn(t *testing.T) chdriver.Conn {
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

func TestGetValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test, requires ClickHouse")
	}

	ctx := t.Context()
	rawConn := newRawConn(t)

	variable := "pm2p5"
	value := float32(3.05)
	unit := "µg/m³"
	// Truncate to seconds: ClickHouse DateTime has second precision.
	timestamp := time.Now().UTC().Truncate(time.Second)
	lat := float32(55.05)
	lon := float32(106.15)
	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	err = rawConn.Exec(ctx, `
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
		_ = rawConn.Exec(context.Background(),
			"ALTER TABLE grid_data DELETE WHERE catalog_id = @catalog_id",
			clickhouseraw.Named("catalog_id", catalogID),
		)
	})

	client := newClient(t)

	gridValue, err := client.GetValue(ctx, variable, timestamp.Add(30*time.Minute), lat+0.435, lon+0.195)
	if err != nil {
		t.Fatalf("error getting value: %v", err)
	}

	if gridValue.Value != value {
		t.Errorf("expected value %v, got %v", value, gridValue.Value)
	}
	if gridValue.Unit != unit {
		t.Errorf("expected unit %v, got %v", unit, gridValue.Unit)
	}
	if gridValue.Lat != lat {
		t.Errorf("expected latitude %v, got %v", lat, gridValue.Lat)
	}
	if gridValue.Lon != lon {
		t.Errorf("expected longitude %v, got %v", lon, gridValue.Lon)
	}
	if !gridValue.Timestamp.Equal(timestamp) {
		t.Errorf("expected timestamp %v, got %v", timestamp, gridValue.Timestamp)
	}
	if gridValue.CatalogID != catalogID {
		t.Errorf("expected catalogID %v, got %v", catalogID, gridValue.CatalogID)
	}
}

func TestGetValueNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test, requires ClickHouse")
	}

	_, err := newClient(t).GetValue(t.Context(), "nonexistent_variable", time.Now(), 0, 0)
	if !errors.Is(err, domain.ErrGridValueNotFound) {
		t.Errorf("expected ErrGridValueNotFound, got %v", err)
	}
}
