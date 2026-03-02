package domain

import (
	"context"
	"errors"
	"strings"
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
	value := m.values[variable]
	if value == nil {
		return nil, ErrGridValueNotFound
	}

	return value, nil
}

func TestService_GetVariables(t *testing.T) {
	variableNames := [2]string{"pm2p5", "pm10"}
	values := [2]float32{1.0, 0.001}
	units := [2]string{"µg/m³", "ng/m³"}
	lats := [2]float32{75.05, 75.1}
	lons := [2]float32{106.25, 106.2}
	timestamps := [2]time.Time{
		time.Date(2026, 2, 27, 4, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 27, 5, 0, 0, 0, time.UTC),
	}
	catalogIDs := [2]uuid.UUID{uuid.New(), uuid.New()}
	service := NewService(&mockGridStore{
		values: map[string]*GridValue{
			variableNames[0]: {Value: values[0], Unit: units[0], Lat: lats[0], Lon: lons[0], Timestamp: timestamps[0], CatalogID: catalogIDs[0]},
			variableNames[1]: {Value: values[1], Unit: units[1], Lat: lats[1], Lon: lons[1], Timestamp: timestamps[1], CatalogID: catalogIDs[1]},
		},
	})

	variables, err := service.GetVariables(t.Context(), time.Date(2026, 2, 27, 6, 0, 0, 0, time.UTC), 75.08, 106.22, variableNames[:])
	if err != nil {
		t.Errorf("GetVariables returned error: %v", err)
	}
	if len(variables) != 2 {
		t.Errorf("GetVariables returned wrong number of variables: %v", variables)
	}
	for i := range 2 {
		variable := variables[i]
		if variable.Unit != units[i] {
			t.Errorf("GetVariables returned wrong unit: %v", variable.Unit)
		}
		if variable.ActualLat != lats[i] {
			t.Errorf("GetVariables returned wrong latitude: %v", variable.ActualLat)
		}
		if variable.ActualLon != lons[i] {
			t.Errorf("GetVariables returned wrong longitude: %v", variable.ActualLon)
		}
		if variable.RefTimestamp != timestamps[i] {
			t.Errorf("GetVariables returned wrong timestamp: %v", variable.RefTimestamp)
		}
		if variable.CatalogID != catalogIDs[i] {
			t.Errorf("GetVariables returned wrong catalogID: %v", variable.CatalogID)
		}
		if variable.Value != values[i] {
			t.Errorf("GetVariables returned wrong value: %v", variable.Value)
		}
	}
}

func TestService_GetVariables_NotFound(t *testing.T) {
	existingVariable := "pm10"

	variableName := "pm2p5"
	timestamp := time.Date(2026, 2, 27, 4, 0, 0, 0, time.UTC)
	lat := float32(75.08)
	lon := float32(106.29)

	service := NewService(&mockGridStore{values: map[string]*GridValue{
		existingVariable: {Value: 1.0, Unit: "µg/m³", Lat: 75.05, Lon: 106.25, Timestamp: time.Date(2026, 2, 27, 4, 0, 0, 0, time.UTC), CatalogID: uuid.New()},
	}})

	_, err := service.GetVariables(t.Context(), timestamp, lat, lon, []string{existingVariable, variableName})
	targetError, ok := errors.AsType[*ErrVariableNotFound](err)
	if !ok {
		t.Errorf("GetVariables returned wrong error: %v", err)
	}
	if targetError.Variable != variableName {
		t.Errorf("GetVariables returned wrong variable: %v", targetError.Variable)
	}
	if !strings.Contains(err.Error(), "pm2p5") {
		t.Errorf("ErrVariableNotFound message should contain variable pm2p5, actual message: %q", err.Error())
	}
}
