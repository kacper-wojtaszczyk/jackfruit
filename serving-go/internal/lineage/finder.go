package lineage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

type Finder struct {
	db *sql.DB
}

func NewFinder(db *sql.DB) *Finder {
	return &Finder{db: db}
}

func (f *Finder) GetLineage(ctx context.Context, catalogID uuid.UUID) (*domain.Lineage, error) {
	const query = `
        SELECT rf.source, rf.dataset, cd.raw_file_id
        FROM catalog.curated_data cd
        JOIN catalog.raw_files rf ON rf.id = cd.raw_file_id
        WHERE cd.id = $1
    `
	var lineage domain.Lineage
	err := f.db.QueryRowContext(ctx, query, catalogID).Scan(
		&lineage.Source,
		&lineage.Dataset,
		&lineage.RawFileID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrLineageNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lineage query for %s: %w", catalogID, err)
	}

	return &lineage, nil
}
