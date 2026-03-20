package domain

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

type Lineage struct {
	Source    string
	Dataset   string
	RawFileID uuid.UUID
}

var ErrLineageNotFound = errors.New("lineage not found")

type LineageRetriever interface {
	GetLineage(ctx context.Context, catalogID uuid.UUID) (*Lineage, error)
}
