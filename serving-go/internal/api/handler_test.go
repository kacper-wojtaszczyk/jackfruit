package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/api"
)

func TestHealthHandler(t *testing.T) {
	mux := http.NewServeMux()
	api.NewHandler().RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}
	if body := w.Body.String(); body != "" {
		t.Errorf("expected empty body, got %q", body)
	}
}
