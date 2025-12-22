package model

import (
	"fmt"

	"github.com/google/uuid"
)

// Dataset represents a known dataset identifier.
type Dataset string

const (
	CAMSEuropeAirQualityForecastsAnalysis Dataset = "cams-europe-air-quality-forecasts-analysis"
	CAMSEuropeAirQualityForecastsForecast Dataset = "cams-europe-air-quality-forecasts-forecast"
)

// RunID represents a UUIDv7 run identifier from orchestration (Dagster).
type RunID string

// Validate checks that the RunID is a valid UUIDv7.
func (r RunID) Validate() error {
	if r == "" {
		return fmt.Errorf("run-id cannot be empty")
	}
	id, err := uuid.Parse(string(r))
	if err != nil {
		return fmt.Errorf("run-id must be a valid UUID: %w", err)
	}
	if id.Version() != uuid.Version(7) {
		return fmt.Errorf("run-id must be a UUIDv7, got v%d", id.Version())
	}
	return nil
}

// String returns the run ID as a string.
func (r RunID) String() string {
	return string(r)
}
