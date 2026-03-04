package api_test

import (
	"context"
	"errors"
	"io"
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
}

func (m *mockVariableProvider) GetVariables(_ context.Context, _ time.Time, _ float32, _ float32, _ []string) ([]domain.VariableResult, error) {
	return nil, m.err
}

func TestHandleEnvironmental_InternalError(t *testing.T) {
	mock := &mockVariableProvider{err: errors.New("database connection reset by peer")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

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
