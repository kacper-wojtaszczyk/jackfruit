package grid_test

import (
	"errors"
	"testing"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/grid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/testutil"
)

func TestGetSample(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test, requires ClickHouse")
	}

	ctx := t.Context()
	rawConn := testutil.NewRawConn(t)

	variable := "pm2p5"
	value := float32(3.05)
	unit := "µg/m³"
	// Truncate to seconds: ClickHouse DateTime has second precision.
	timestamp := time.Now().UTC().Truncate(time.Second)
	lat := float32(55.05)
	lon := float32(106.15)

	catalogID := testutil.InsertGridRow(t, rawConn, variable, value, unit, timestamp, lat, lon)

	client := grid.NewFinder(rawConn)

	gridValue, err := client.GetSample(ctx, variable, timestamp.Add(30*time.Minute), lat+0.435, lon+0.195)
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

func TestGetSampleNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test, requires ClickHouse")
	}

	_, err := grid.NewFinder(testutil.NewRawConn(t)).GetSample(t.Context(), "nonexistent_variable", time.Now(), 0, 0)
	if !errors.Is(err, domain.ErrGridSampleNotFound) {
		t.Errorf("expected ErrGridSampleNotFound, got %v", err)
	}
}
