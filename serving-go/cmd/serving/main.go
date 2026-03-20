package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/api"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/config"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/grid"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/lineage"
)

type app struct {
	cfg     *config.Config
	logger  *slog.Logger
	server  *http.Server
	closers []io.Closer
}

func newApp() (*app, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
	cfg := config.Load()

	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.ClickHouseHost, cfg.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Logger: logger.With("component", "clickhouse"),
		Settings: clickhouse.Settings{
			"max_execution_time": 15,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	chPingCtx, chPingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer chPingCancel()
	if err := chConn.Ping(chPingCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("ping clickhouse: timeout after 5s: %w", err)
		}
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	chFinder := grid.NewFinder(chConn)

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s",
		cfg.PostgresHost, cfg.PostgresPort, cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresDB,
	)
	pgDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := pgDB.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	lineageFinder := lineage.NewFinder(pgDB)

	service := domain.NewService(chFinder, lineageFinder)

	mux := http.NewServeMux()
	api.NewHandler(service, logger.With("component", "api")).RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return &app{cfg: cfg, logger: logger, server: server, closers: []io.Closer{pgDB, chConn}}, nil
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
	for _, closer := range a.closers {
		if err := closer.Close(); err != nil {
			a.logger.Error("close error", "error", err)
		}
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
