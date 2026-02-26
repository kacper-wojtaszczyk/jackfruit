package domain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrGridValueNotFound = errors.New("grid value not found")

type GridValue struct {
	Value     float32
	Unit      string
	Lat       float32
	Lon       float32
	Timestamp time.Time
	CatalogID uuid.UUID
}

type GridStore interface {
	GetValue(ctx context.Context, variable string, timestamp time.Time, lat float32, lon float32) (*GridValue, error)
}
