package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ErrTemporalMiss struct {
	Variable string
}

func (e *ErrTemporalMiss) Error() string {
	return fmt.Sprintf("variable %q: no data available at or before requested timestamp", e.Variable)
}

type ErrSpatialMiss struct {
	Variable string
}

func (e *ErrSpatialMiss) Error() string {
	return fmt.Sprintf("variable %q: requested coordinates are outside data coverage", e.Variable)
}

type GridSample struct {
	Value     float32
	Unit      string
	Lat       float32
	Lon       float32
	Timestamp time.Time
	CatalogID uuid.UUID
}

type GridRetriever interface {
	GetSample(ctx context.Context, variable string, timestamp time.Time, lat float32, lon float32) (*GridSample, error)
}
