package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/adapters/cds"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/config"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/storage"
)

type dataFetcher interface {
	Fetch(ctx context.Context, req cds.Request) (io.ReadCloser, error)
}

func main() {
	// Configure the global logger
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Parse CLI flags
	dateStr := flag.String("date", time.Now().Format("2006-01-02"), "Date for data request (YYYY-MM-DD)")
	datasetStr := flag.String("dataset", "cams-europe-air-quality-forecasts-analysis", "Dataset name")
	runID := flag.String("run-id", "", "Run identifier (ULID from orchestration)")
	flag.Parse()

	// Parse and validate flags
	datasetName := model.Dataset(*datasetStr)
	date, err := time.Parse("2006-01-02", *dateStr)
	if err != nil {
		slog.Error("invalid date format", "date", *dateStr, "error", err)
		fmt.Fprintf(os.Stderr, "Usage: date must be in YYYY-MM-DD format\n")
		os.Exit(1)
	}
	if *runID == "" {
		slog.Error("run-id is required")
		fmt.Fprintf(os.Stderr, "Usage: run-id must be provided (UUIDv7)\n")
		os.Exit(1)
	}

	// Ensure run-id parses as UUIDv7 early
	if err := model.RunID(*runID).Validate(); err != nil {
		slog.Error("invalid run-id", "error", err)
		fmt.Fprintf(os.Stderr, "Usage: run-id must be a UUIDv7\n")
		os.Exit(1)
	}

	// Ensure environment variables are loaded
	err = godotenv.Load()
	if err != nil {
		slog.Error("failed to load env vars", "error", err)
		os.Exit(1)
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Create a cancellable context (for graceful shutdown)
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Run the application
	client := cds.NewClient(cfg.ADSBaseURL, cfg.ADSAPIKey)
	if err := run(ctx, date, datasetName, model.RunID(*runID), client); err != nil {
		slog.Error("application error", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
}

func run(ctx context.Context, date time.Time, datasetName model.Dataset, runID model.RunID, fetcher dataFetcher) error {
	slog.DebugContext(ctx, "running application", "date", date.Format("2006-01-02"), "dataset", datasetName, "run_id", runID)

	if runID == "" {
		return fmt.Errorf("run-id cannot be empty")
	}

	if err := runID.Validate(); err != nil {
		return err
	}

	key := storage.ObjectKey{
		Source:    "ads",
		Dataset:   datasetName,
		Date:      date.Format("2006-01-02"),
		RunID:     runID,
		Extension: "nc", // TODO: detect extension after download/unzip
	}

	slog.DebugContext(ctx, "object key constructed", "key", key.Key())

	data, err := fetcher.Fetch(ctx, &cds.CAMSRequest{Date: date, Dataset: datasetName})

	if err != nil {
		return fmt.Errorf("fetch from CDS: %w", err)
	}
	defer data.Close()

	// TODO: Stream to MinIO (next tutorial)
	// For now, just discard the data to verify it works
	n, err := io.Copy(io.Discard, data)
	if err != nil {
		return fmt.Errorf("read data: %w", err)
	}

	slog.Info("ingestion complete", "bytes", n, "run_id", runID)

	return nil
}
