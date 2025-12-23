package ingestion

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/storage"
)

// FetchRequest contains input parameters for fetching a dataset.
type FetchRequest struct {
	Dataset model.Dataset
	Date    time.Time
}

// FetchResult wraps the fetched stream and metadata derived during fetch.
type FetchResult struct {
	Body      io.ReadCloser
	Source    string
	Extension string
}

// Fetcher retrieves raw data for a given request.
type Fetcher interface {
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
}

// ObjectStorage writes data streams to object storage.
type ObjectStorage interface {
	Put(ctx context.Context, key string, data io.Reader) error
}

// Service orchestrates ingestion steps: fetch then store.
type Service struct {
	fetcher       Fetcher
	objectStorage ObjectStorage
	bucket        string
}

func NewService(fetcher Fetcher, objectStorage ObjectStorage, bucket string) *Service {
	return &Service{fetcher: fetcher, objectStorage: objectStorage, bucket: bucket}
}

func (s *Service) Ingest(ctx context.Context, req FetchRequest, runID model.RunID) error {
	if err := runID.Validate(); err != nil {
		return err
	}

	result, err := s.fetcher.Fetch(ctx, req)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	defer result.Body.Close()

	// Build object key
	key := storage.ObjectKey{
		Source:    result.Source,
		Dataset:   req.Dataset,
		Date:      req.Date.Format("2006-01-02"),
		RunID:     runID,
		Extension: result.Extension,
	}

	slog.DebugContext(ctx, "ingestion started", "dataset", req.Dataset, "date", req.Date.Format("2006-01-02"), "run_id", runID, "key", key.Key())

	// Store data
	if err := s.objectStorage.Put(ctx, key.Key(), result.Body); err != nil {
		return fmt.Errorf("store: %w", err)
	}

	slog.InfoContext(ctx, "ingestion complete", "key", key.Key(), "run_id", runID)
	return nil
}
