package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/api"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/clickhouse"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/config"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

type app struct {
	cfg    *config.Config
	logger *slog.Logger
	ch     *clickhouse.Client
	server *http.Server
}

func newApp() (*app, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
	cfg := config.Load()

	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Host:     cfg.ClickHouseHost,
		Port:     cfg.ClickHousePort,
		User:     cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
		Database: cfg.ClickHouseDatabase,
	}, logger.With("component", "clickhouse"))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	var store domain.GridStore = chClient
	service := domain.NewService(store)

	mux := http.NewServeMux()
	api.NewHandler(service).RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &app{cfg: cfg, logger: logger, ch: chClient, server: server}, nil
}

func (a *app) run() {
	go func() {
		a.logger.Info("starting server", "port", a.cfg.Port)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	a.logger.Info("shutting down server")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.shutdown(ctx)
}

func (a *app) shutdown(ctx context.Context) {
	if err := a.server.Shutdown(ctx); err != nil {
		a.logger.Error("server shutdown error", "error", err)
	}
	if err := a.ch.Close(); err != nil {
		a.logger.Error("clickhouse close error", "error", err)
	}
	a.logger.Info("server stopped")
}

func main() {
	a, err := newApp()
	if err != nil {
		slog.Error("failed to start the app", "error", err)
		os.Exit(1)
	}
	a.run()
}
