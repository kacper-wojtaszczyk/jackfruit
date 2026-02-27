package domain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type ErrVariableNotFound struct {
	Variable  string
	Timestamp time.Time
	Lat       float32
	Lon       float32
}

func (e *ErrVariableNotFound) Error() string {
	return fmt.Sprintf(
		"variable %q not found at time: %v lat: %v lon: %v",
		e.Variable,
		e.Timestamp,
		e.Lat,
		e.Lon,
	)
}

type VariableResult struct {
	Name      string
	Value     float32
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
	ts time.Time,
	lat, lon float32,
	vars []string,
) ([]VariableResult, error) {
	results := make([]VariableResult, len(vars))
	g, ctx := errgroup.WithContext(ctx)

	for i, variable := range vars {
		g.Go(func() error {
			result, err := s.getVariable(ctx, variable, ts, lat, lon)
			if err != nil {
				return err
			}
			results[i] = *result

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *Service) getVariable(
	ctx context.Context,
	variable string,
	ts time.Time,
	lat, lon float32,
) (*VariableResult, error) {
	gridValue, err := s.store.GetValue(ctx, variable, ts, lat, lon)
	if errors.Is(err, ErrGridValueNotFound) {
		return nil, &ErrVariableNotFound{Variable: variable, Timestamp: ts, Lat: lat, Lon: lon}
	}
	if err != nil {
		return nil, fmt.Errorf("getting variable %q: %w", variable, err)
	}

	return &VariableResult{
		Name:      variable,
		Value:     gridValue.Value,
		Unit:      gridValue.Unit,
		Timestamp: gridValue.Timestamp,
		Lat:       gridValue.Lat,
		Lon:       gridValue.Lon,
		CatalogID: gridValue.CatalogID,
	}, nil
}
