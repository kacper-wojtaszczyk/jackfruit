package domain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrGridSampleNotFound = errors.New("grid sample not found")

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
