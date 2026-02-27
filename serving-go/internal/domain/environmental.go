package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ErrVariableNotFound struct {
	Variable  string
	Timestamp time.Time
	Lat       float32
	Lon       float32
}

func (e *ErrVariableNotFound) Error() string {
	return fmt.Sprintf("variable %q not found at time: %v lat: %v lon: %v", e.Variable, e.Timestamp, e.Lat, e.Lon)
}

type VariableResult struct {
	Name      string
	Value     string
	Unit      string
	Timestamp time.Time
	Lat       float32
	Lon       float32
	CatalogID uuid.UUID
}

type Service struct {
	store GridStore
}

func NewService(store GridStore) *Service {
	return &Service{store: store}
}

func (s *Service) GetVariables(
	ctx context.Context,
	lat, lon float32,
	ts time.Time,
	vars []string,
) ([]VariableResult, error) {
	results := make([]VariableResult, len(vars))

	return results, nil
}
