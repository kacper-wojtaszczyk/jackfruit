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
	Variable string
}

func (e *ErrVariableNotFound) Error() string {
	return fmt.Sprintf("variable %q not found", e.Variable)
}

type VariableResult struct {
	Name         string
	Value        float32
	Unit         string
	RefTimestamp time.Time
	ActualLat    float32
	ActualLon    float32
	CatalogID    uuid.UUID
	Lineage      Lineage
}

type Service struct {
	grid    GridRetriever
	lineage LineageRetriever
}

func NewService(grid GridRetriever, lineage LineageRetriever) *Service {
	return &Service{grid: grid, lineage: lineage}
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
	gridSample, err := s.grid.GetSample(ctx, variable, ts, lat, lon)
	if errors.Is(err, ErrGridSampleNotFound) {
		return nil, &ErrVariableNotFound{Variable: variable}
	}
	if err != nil {
		return nil, fmt.Errorf("getting variable %q: %w", variable, err)
	}

	lineage, err := s.lineage.GetLineage(ctx, gridSample.CatalogID)
	if err != nil {
		return nil, fmt.Errorf("lineage for variable %q (catalog_id %s): %w", variable, gridSample.CatalogID, err)
	}

	return &VariableResult{
		Name:         variable,
		Value:        gridSample.Value,
		Unit:         gridSample.Unit,
		RefTimestamp: gridSample.Timestamp,
		ActualLat:    gridSample.Lat,
		ActualLon:    gridSample.Lon,
		CatalogID:    gridSample.CatalogID,
		Lineage:      *lineage,
	}, nil
}
