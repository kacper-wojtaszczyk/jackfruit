package domain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

type mockGridStore struct {
	values map[string]*GridValue
}

func (m *mockGridStore) GetValue(
	ctx context.Context,
	variable string,
	timestamp time.Time,
	lat float32,
	lon float32,
) (*GridValue, error) {
	if len(m.values) == 0 {
		return nil, ErrGridValueNotFound
	}
	value := m.values[variable]

	return value, nil
}

func TestService_GetVariables(t *testing.T) {
	catalogID, _ := uuid.NewV7()
	value := float32(1.0)
	unit := "µg/m³"
	lat := float32(75.05)
	lon := float32(106.25)
	timestamp := time.Now()
	service := NewService(&mockGridStore{
		values: map[string]*GridValue{
			"pm2p5": {Value: value, Unit: unit, Lat: lat, Lon: lon, Timestamp: timestamp, CatalogID: catalogID},
		},
	})

	variables, err := service.GetVariables(t.Context(), timestamp, lat, lon, []string{"pm2p5"})
	if err != nil {
		t.Errorf("GetVariables returned error: %v", err)
	}
	if len(variables) != 1 {
		t.Errorf("GetVariables returned wrong number of variables: %v", variables)
	}
	variable := variables[0]
	if variable.Unit != unit {
		t.Errorf("GetVariables returned wrong unit: %v", variable.Unit)
	}
	if variable.Lat != lat {
		t.Errorf("GetVariables returned wrong latitude: %v", variable.Lat)
	}
	if variable.Lon != lon {
		t.Errorf("GetVariables returned wrong longitude: %v", variable.Lon)
	}
	if variable.Timestamp != timestamp {
		t.Errorf("GetVariables returned wrong timestamp: %v", variable.Timestamp)
	}
	if variable.CatalogID != catalogID {
		t.Errorf("GetVariables returned wrong catalogID: %v", variable.CatalogID)
	}
	if variable.Value != value {
		t.Errorf("GetVariables returned wrong value: %v", variable.Value)
	}
}
