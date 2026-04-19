package domain

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type mockGridRetriever struct {
	samples map[string]*GridSample
	errs    map[string]error
}

func (m *mockGridRetriever) GetSample(
	ctx context.Context,
	variable string,
	timestamp time.Time,
	lat float32,
	lon float32,
) (*GridSample, error) {
	if err, ok := m.errs[variable]; ok {
		return nil, err
	}
	sample := m.samples[variable]
	if sample == nil {
		return nil, &ErrTemporalMiss{Variable: variable}
	}

	return sample, nil
}

type mockLineageRetriever struct {
	lineages map[uuid.UUID]*Lineage
	err      error
	calls    atomic.Int64
}

func (m *mockLineageRetriever) GetLineage(ctx context.Context, catalogID uuid.UUID) (*Lineage, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	l := m.lineages[catalogID]
	if l == nil {
		return nil, ErrLineageNotFound
	}
	return l, nil
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
	catalogID0, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	catalogID1, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	rawFileID0, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	rawFileID1, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	catalogIDs := [2]uuid.UUID{catalogID0, catalogID1}
	rawFileIDs := [2]uuid.UUID{rawFileID0, rawFileID1}

	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			variableNames[0]: {Value: values[0], Unit: units[0], Lat: lats[0], Lon: lons[0], Timestamp: timestamps[0], CatalogID: catalogIDs[0]},
			variableNames[1]: {Value: values[1], Unit: units[1], Lat: lats[1], Lon: lons[1], Timestamp: timestamps[1], CatalogID: catalogIDs[1]},
		},
	}, &mockLineageRetriever{
		lineages: map[uuid.UUID]*Lineage{
			catalogIDs[0]: {Source: "ads", Dataset: "cams-europe-air-quality-forecast", RawFileID: rawFileIDs[0]},
			catalogIDs[1]: {Source: "ads", Dataset: "cams-europe-air-quality-forecast", RawFileID: rawFileIDs[1]},
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
		if variable.Lineage.Source != "ads" {
			t.Errorf("GetVariables returned wrong lineage source: %v", variable.Lineage.Source)
		}
		if variable.Lineage.Dataset != "cams-europe-air-quality-forecast" {
			t.Errorf("GetVariables returned wrong lineage dataset: %v", variable.Lineage.Dataset)
		}
		if variable.Lineage.RawFileID != rawFileIDs[i] {
			t.Errorf("GetVariables returned wrong lineage raw_file_id: %v", variable.Lineage.RawFileID)
		}
	}
}

func TestService_GetVariables_TemporalMiss(t *testing.T) {
	lineageMock := &mockLineageRetriever{lineages: map[uuid.UUID]*Lineage{}}
	service := NewService(&mockGridRetriever{
		errs: map[string]error{"pm2p5": &ErrTemporalMiss{Variable: "pm2p5"}},
	}, lineageMock)

	_, err := service.GetVariables(t.Context(), time.Date(2026, 2, 27, 4, 0, 0, 0, time.UTC), 75.08, 106.29, []string{"pm2p5"})
	miss, ok := errors.AsType[*ErrTemporalMiss](err)
	if !ok {
		t.Fatalf("expected *ErrTemporalMiss, got: %v", err)
	}
	if miss.Variable != "pm2p5" {
		t.Errorf("expected Variable=pm2p5, got %q", miss.Variable)
	}
	if !strings.Contains(err.Error(), "pm2p5") {
		t.Errorf("expected error message to mention variable name, got: %q", err.Error())
	}
	if got := lineageMock.calls.Load(); got != 0 {
		t.Errorf("lineage must not be fetched on temporal miss, got %d calls", got)
	}
}

func TestService_GetVariables_SpatialMiss(t *testing.T) {
	lineageMock := &mockLineageRetriever{lineages: map[uuid.UUID]*Lineage{}}
	service := NewService(&mockGridRetriever{
		errs: map[string]error{"pm2p5": &ErrSpatialMiss{Variable: "pm2p5"}},
	}, lineageMock)

	_, err := service.GetVariables(t.Context(), time.Date(2026, 2, 27, 4, 0, 0, 0, time.UTC), 75.08, 106.29, []string{"pm2p5"})
	miss, ok := errors.AsType[*ErrSpatialMiss](err)
	if !ok {
		t.Fatalf("expected *ErrSpatialMiss, got: %v", err)
	}
	if miss.Variable != "pm2p5" {
		t.Errorf("expected Variable=pm2p5, got %q", miss.Variable)
	}
	if got := lineageMock.calls.Load(); got != 0 {
		t.Errorf("lineage must not be fetched on spatial miss, got %d calls", got)
	}
}

func TestService_GetVariables_DataTooStale(t *testing.T) {
	requestedTS := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	availableTS := requestedTS.Add(-(maxTemporalGap + time.Minute))

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	lineageMock := &mockLineageRetriever{lineages: map[uuid.UUID]*Lineage{}}
	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			"pm2p5": {Value: 12.5, Unit: "µg/m³", Lat: 52.5, Lon: 13.4, Timestamp: availableTS, CatalogID: catalogID},
		},
	}, lineageMock)

	_, err = service.GetVariables(t.Context(), requestedTS, 52.5, 13.4, []string{"pm2p5"})
	stale, ok := errors.AsType[*ErrDataTooStale](err)
	if !ok {
		t.Fatalf("expected *ErrDataTooStale, got: %v", err)
	}
	if stale.Variable != "pm2p5" {
		t.Errorf("expected Variable=pm2p5, got %q", stale.Variable)
	}
	if !stale.RequestedAt.Equal(requestedTS) {
		t.Errorf("expected RequestedAt=%v, got %v", requestedTS, stale.RequestedAt)
	}
	if !stale.AvailableAt.Equal(availableTS) {
		t.Errorf("expected AvailableAt=%v, got %v", availableTS, stale.AvailableAt)
	}
	if stale.Gap != maxTemporalGap+time.Minute {
		t.Errorf("expected Gap=%v, got %v", maxTemporalGap+time.Minute, stale.Gap)
	}
	if got := lineageMock.calls.Load(); got != 0 {
		t.Errorf("lineage must not be fetched when data is stale, got %d calls", got)
	}
}

func TestService_GetVariables_DataAtTemporalBoundary(t *testing.T) {
	requestedTS := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	availableTS := requestedTS.Add(-maxTemporalGap)

	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	rawFileID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			"pm2p5": {Value: 12.5, Unit: "µg/m³", Lat: 52.5, Lon: 13.4, Timestamp: availableTS, CatalogID: catalogID},
		},
	}, &mockLineageRetriever{
		lineages: map[uuid.UUID]*Lineage{
			catalogID: {Source: "ads", Dataset: "cams-europe-air-quality-forecast", RawFileID: rawFileID},
		},
	})

	_, err = service.GetVariables(t.Context(), requestedTS, 52.5, 13.4, []string{"pm2p5"})
	if err != nil {
		t.Errorf("expected no error at boundary (gap == maxTemporalGap), got: %v", err)
	}
}

func TestService_GetVariables_WithLineage(t *testing.T) {
	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	rawFileID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			"temperature": {Value: 22.5, Unit: "°C", Lat: 52.5, Lon: 13.4, Timestamp: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC), CatalogID: catalogID},
		},
	}, &mockLineageRetriever{
		lineages: map[uuid.UUID]*Lineage{
			catalogID: {Source: "ecmwf", Dataset: "ifs-weather-forecast", RawFileID: rawFileID},
		},
	})

	variables, err := service.GetVariables(t.Context(), time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC), 52.5, 13.4, []string{"temperature"})
	if err != nil {
		t.Fatalf("GetVariables returned error: %v", err)
	}
	if len(variables) != 1 {
		t.Fatalf("expected 1 variable, got %d", len(variables))
	}

	v := variables[0]
	if v.Lineage.Source != "ecmwf" {
		t.Errorf("expected lineage source %q, got %q", "ecmwf", v.Lineage.Source)
	}
	if v.Lineage.Dataset != "ifs-weather-forecast" {
		t.Errorf("expected lineage dataset %q, got %q", "ifs-weather-forecast", v.Lineage.Dataset)
	}
	if v.Lineage.RawFileID != rawFileID {
		t.Errorf("expected lineage raw_file_id %v, got %v", rawFileID, v.Lineage.RawFileID)
	}
}

func TestService_GetVariables_LineageNotFound(t *testing.T) {
	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}

	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			"pm2p5": {Value: 12.5, Unit: "µg/m³", Lat: 52.5, Lon: 13.4, Timestamp: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC), CatalogID: catalogID},
		},
	}, &mockLineageRetriever{lineages: map[uuid.UUID]*Lineage{}})

	_, err = service.GetVariables(t.Context(), time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC), 52.5, 13.4, []string{"pm2p5"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrLineageNotFound) {
		t.Errorf("expected error wrapping ErrLineageNotFound, got: %v", err)
	}
}

func TestService_GetVariables_LineageFails(t *testing.T) {
	catalogID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	pgErr := errors.New("postgres down")

	service := NewService(&mockGridRetriever{
		samples: map[string]*GridSample{
			"pm2p5": {Value: 12.5, Unit: "µg/m³", Lat: 52.5, Lon: 13.4, Timestamp: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC), CatalogID: catalogID},
		},
	}, &mockLineageRetriever{err: pgErr})

	_, err = service.GetVariables(t.Context(), time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC), 52.5, 13.4, []string{"pm2p5"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrLineageNotFound) {
		t.Error("error should not be ErrLineageNotFound for a generic DB failure")
	}
	if !errors.Is(err, pgErr) {
		t.Errorf("expected error wrapping original postgres error, got: %v", err)
	}
}
