package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/adapters/cds"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/config"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/exitcode"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/ingestion"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/model"
	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/storage"
)

func main() {
	// Configure the global logger
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Parse CLI flags
	dateStr := flag.String("date", time.Now().Format("2006-01-02"), "Date for data request (YYYY-MM-DD)")
	datasetStr := flag.String("dataset", "cams-europe-air-quality-forecasts-analysis", "Dataset name")
	runID := flag.String("run-id", "", "Run identifier (UUIDv7 from orchestration)")
	flag.Parse()

	// Parse and validate flags
	datasetName := model.Dataset(*datasetStr)
	if err := datasetName.Validate(); err != nil {
		slog.Error("invalid dataset", "error", err)
		fmt.Fprintf(os.Stderr, "Usage: %v\n", err)
		os.Exit(exitcode.ConfigError)
	}
	date, err := time.Parse("2006-01-02", *dateStr)
	if err != nil {
		slog.Error("invalid date format", "date", *dateStr, "error", err)
		fmt.Fprintf(os.Stderr, "Usage: date must be in YYYY-MM-DD format\n")
		os.Exit(exitcode.ConfigError)
	}
	if *runID == "" {
		slog.Error("run-id is required")
		fmt.Fprintf(os.Stderr, "Usage: run-id must be provided (UUIDv7)\n")
		os.Exit(exitcode.ConfigError)
	}

	// Ensure run-id parses as UUIDv7 early
	if err := model.RunID(*runID).Validate(); err != nil {
		slog.Error("invalid run-id", "error", err)
		fmt.Fprintf(os.Stderr, "Usage: run-id must be a UUIDv7\n")
		os.Exit(exitcode.ConfigError)
	}

	// Ensure environment variables are loaded
	err = godotenv.Load()
	if err != nil {
		slog.Warn("failed to load env vars", "error", err)
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(exitcode.ConfigError)
	}

	// Create a cancellable context (for graceful shutdown)
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := cds.NewClient(cfg.ADSBaseURL, cfg.ADSAPIKey)

	// Initialize MinIO client
	minioCfg := storage.MinIOConfig{
		Endpoint:  cfg.MinIOEndpoint,
		AccessKey: cfg.MinIOAccessKey,
		SecretKey: cfg.MinIOSecretKey,
		Bucket:    cfg.MinIOBucket,
		UseSSL:    cfg.MinIOUseSSL,
	}
	minioClient, err := storage.NewMinIOClient(ctx, minioCfg)
	if err != nil {
		slog.Error("failed to initialize minio client", "error", err)
		os.Exit(exitcode.ConfigError)
	}

	svc := ingestion.NewService(client, minioClient)

	req := ingestion.FetchRequest{Dataset: datasetName, Date: date}

	if err := svc.Ingest(ctx, req, model.RunID(*runID)); err != nil {
		slog.Error("application error", "error", err)
		os.Exit(exitcode.ApplicationError)
	}

	slog.Info("shutdown complete")
}
