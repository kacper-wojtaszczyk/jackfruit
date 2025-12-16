package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
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

	return nil
}
