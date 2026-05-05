package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/api"
	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

type mockVariableProvider struct {
	err error
	// When true, GetVariables blocks until the context is done and
	// returns ctx.Err(). Used by the cancel/deadline tests below.
	blockUntilCtxDone bool
}

func (m *mockVariableProvider) GetVariables(ctx context.Context, _ time.Time, _ float32, _ float32, _ []string) ([]domain.VariableResult, error) {
	if m.blockUntilCtxDone {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, m.err
}

func TestHandleEnvironmental_InternalError(t *testing.T) {
	mock := &mockVariableProvider{err: errors.New("database connection reset by peer")}
	logger := slog.New(slog.DiscardHandler)

	mux := http.NewServeMux()
	api.NewHandler(mock, logger).RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/v1/environmental?lat=52.5&lon=13.4&timestamp=2025-03-11T00:00:00Z&variables=pm2p5", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}

	body := w.Body.String()
	if strings.Contains(body, "database connection reset") {
		t.Errorf("response body must not leak internal error details, got: %s", body)
	}
	if !strings.Contains(body, "internal server error") {
		t.Errorf("response body must contain 'internal server error', got: %s", body)
	}
}

// When the client disconnects mid-flight, the handler must not write
// anything — the connection is gone. Asserting an empty body is the
// only verifiable contract here (httptest.ResponseRecorder defaults
// Code to 200 even when no WriteHeader is ever called).
func TestHandleEnvironmental_ClientCanceled_NoBody(t *testing.T) {
	mock := &mockVariableProvider{blockUntilCtxDone: true}
	logger := slog.New(slog.DiscardHandler)

	mux := http.NewServeMux()
	api.NewHandler(mock, logger).RegisterRoutes(mux)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the mock returns context.Canceled immediately

	req := httptest.NewRequest("GET", "/v1/environmental?lat=52.5&lon=13.4&timestamp=2025-03-11T00:00:00Z&variables=pm2p5", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Body.Len() != 0 {
		t.Errorf("expected empty body for cancelled client, got: %q", w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "" {
		t.Errorf("expected no Content-Type for cancelled client, got: %q", got)
	}
}

// When the upstream takes too long (deadline exceeded), the handler
// must convert that into a 504, not a 500 or a silent drop.
func TestHandleEnvironmental_DeadlineExceeded_Returns504(t *testing.T) {
	mock := &mockVariableProvider{blockUntilCtxDone: true}
	logger := slog.New(slog.DiscardHandler)

	mux := http.NewServeMux()
	api.NewHandler(mock, logger).RegisterRoutes(mux)

	// Parent deadline already in the past. The handler's inner
	// context.WithTimeout(parent, 18s) inherits this earlier deadline,
	// so ctx.Err() resolves to context.DeadlineExceeded immediately.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	req := httptest.NewRequest("GET", "/v1/environmental?lat=52.5&lon=13.4&timestamp=2025-03-11T00:00:00Z&variables=pm2p5", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Errorf("expected status 504, got %d (body=%q)", w.Code, w.Body.String())
	}

	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("decode body: %v (body=%q)", err, w.Body.String())
	}
	if errResp.Error != "query timed out" {
		t.Errorf("error message = %q, want %q", errResp.Error, "query timed out")
	}
}
