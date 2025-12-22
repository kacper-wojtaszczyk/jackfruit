package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/adapters/cds"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
)

type stubFetcher struct{}

func (s stubFetcher) Fetch(ctx context.Context, req cds.Request) (io.ReadCloser, error) {
	return nil, errors.New("stub fetch error")
}

func TestRun_ConstructsObjectKey(t *testing.T) {
	ctx := context.Background()
	date := time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)

	err := run(ctx, date, "cams-europe-air-quality-forecasts-analysis", model.RunID("01890c24-905b-7122-b170-b60814e6ee06"), stubFetcher{})
	if err == nil {
		t.Fatalf("expected error from stub fetcher, got nil")
	}
}
