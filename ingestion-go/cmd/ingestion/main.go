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
)

func main() {
	// Configure the global logger
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Parse CLI flags
	dateStr := flag.String("date", time.Now().Format("2006-01-02"), "Date for data request (YYYY-MM-DD)")
	datasetStr := flag.String("dataset", "cams-europe-air-quality-forecasts-analysis", "Dataset name")
	flag.Parse()

	// Parse and validate flags
	dataset := cds.Dataset(*datasetStr)
	date, err := time.Parse("2006-01-02", *dateStr)
	if err != nil {
		slog.Error("invalid date format", "date", *dateStr, "error", err)
		fmt.Fprintf(os.Stderr, "Usage: date must be in YYYY-MM-DD format\n")
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
	if err := run(ctx, cfg, date, dataset); err != nil {
		slog.Error("application error", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
}

func run(ctx context.Context, cfg *config.Config, date time.Time, dataset cds.Dataset) error {
	slog.DebugContext(ctx, "running application", "config", cfg, "date", date.Format("2006-01-02"), "dataset", dataset)

	client := cds.NewClient(cfg.ADSBaseURL, cfg.ADSAPIKey)

	slog.DebugContext(ctx, "client created", "client", client)

	data, err := client.Fetch(ctx, &cds.CAMSRequest{Date: date, Dataset: dataset})

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

	slog.Info("ingestion complete", "bytes", n)

	return nil
}
