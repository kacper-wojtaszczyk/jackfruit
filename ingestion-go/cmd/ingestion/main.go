package main

import (
	"context"
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

	// Ensure environment variables are loaded
	err := godotenv.Load()
	if err != nil {
		slog.Error("failed to load anv vars", "error", err)
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
	if err := run(ctx, cfg); err != nil {
		slog.Error("application error", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
}

func run(ctx context.Context, cfg *config.Config) error {
	slog.DebugContext(ctx, "running application", "config", cfg)

	client := cds.NewClient(cfg.CDSBaseURL, cfg.CDSAPIKey)

	slog.DebugContext(ctx, "client created", "client", client)

	data, err := client.Fetch(ctx, &cds.CAMSForecastRequest{Date: time.Now()})

	if err != nil {
		return fmt.Errorf("fetch from CDS: %w", err)
	}
	defer data.Close()

	// Create output file
	file, err := os.Create("zip.zip")
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	// Copy stream directly to file
	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	slog.DebugContext(ctx, "fetched data and saved to file")

	return nil
}
