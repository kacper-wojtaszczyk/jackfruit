package ingestion

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
)

type stubFetcher struct {
	source    string
	extension string
	data      string
	err       error
}

func (s stubFetcher) Fetch(ctx context.Context, req FetchRequest) (FetchResult, error) {
	if s.err != nil {
		return FetchResult{}, s.err
	}
	return FetchResult{
		Body:      io.NopCloser(strings.NewReader(s.data)),
		Source:    s.source,
		Extension: s.extension,
	}, nil
}

type stubStorage struct {
	bucket string
	key    string
	data   string
	err    error
}

func (s *stubStorage) Put(ctx context.Context, key string, data io.Reader) error {
	if s.err != nil {
		return s.err
	}
	s.bucket = "jackfruit-raw"
	s.key = key
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	s.data = string(b)
	return nil
}

func TestService_Ingest_Success(t *testing.T) {
	fetcher := stubFetcher{source: "ads", extension: "grib", data: "hello"}
	storage := &stubStorage{}
	svc := NewService(fetcher, storage)

	req := FetchRequest{Dataset: model.CAMSEuropeAirQualityForecastsAnalysis, Date: time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)}
	runID := model.RunID("01890c24-905b-7122-b170-b60814e6ee06")

	if err := svc.Ingest(context.Background(), req, runID); err != nil {
		t.Fatalf("Ingest() error = %v", err)
	}

	if storage.bucket != "jackfruit-raw" {
		t.Fatalf("expected bucket jackfruit-raw, got %s", storage.bucket)
	}

	expectedKey := "ads/cams-europe-air-quality-forecasts-analysis/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib"
	if storage.key != expectedKey {
		t.Fatalf("expected key %s, got %s", expectedKey, storage.key)
	}

	if storage.data != "hello" {
		t.Fatalf("expected data 'hello', got '%s'", storage.data)
	}
}

func TestService_Ingest_FetchError(t *testing.T) {
	fetcher := stubFetcher{source: "ads", extension: "nc", err: errors.New("fetch failed")}
	storage := &stubStorage{}
	svc := NewService(fetcher, storage)

	req := FetchRequest{Dataset: model.CAMSEuropeAirQualityForecastsAnalysis, Date: time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)}
	runID := model.RunID("01890c24-905b-7122-b170-b60814e6ee06")

	err := svc.Ingest(context.Background(), req, runID)
	if err == nil || !strings.Contains(err.Error(), "fetch failed") {
		t.Fatalf("expected fetch error, got %v", err)
	}
}

func TestService_Ingest_StoreError(t *testing.T) {
	fetcher := stubFetcher{source: "ads", extension: "nc", data: "hello"}
	storage := &stubStorage{err: errors.New("store failed")}
	svc := NewService(fetcher, storage)

	req := FetchRequest{Dataset: model.CAMSEuropeAirQualityForecastsAnalysis, Date: time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)}
	runID := model.RunID("01890c24-905b-7122-b170-b60814e6ee06")

	err := svc.Ingest(context.Background(), req, runID)
	if err == nil || !strings.Contains(err.Error(), "store failed") {
		t.Fatalf("expected store error, got %v", err)
	}
}

func TestService_Ingest_InvalidRunID(t *testing.T) {
	fetcher := stubFetcher{source: "ads", extension: "nc", data: "hello"}
	storage := &stubStorage{}
	svc := NewService(fetcher, storage)

	req := FetchRequest{Dataset: model.CAMSEuropeAirQualityForecastsAnalysis, Date: time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC)}
	runID := model.RunID("not-a-uuid")

	if err := svc.Ingest(context.Background(), req, runID); err == nil {
		t.Fatalf("expected validation error for runID")
	}
}
