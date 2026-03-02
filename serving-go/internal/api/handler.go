package api

import (
	"context"
	"net/http"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

// Handler holds shared dependencies for all HTTP handlers.
// Fields will be added here as the domain service and config are wired in.
type Handler struct {
	variableProvider VariableProvider
}

type VariableProvider interface {
	GetVariables(ctx context.Context, ts time.Time, lat float32, lon float32, vars []string) ([]domain.VariableResult, error)
}

// NewHandler creates a new Handler.
func NewHandler(variableProvider VariableProvider) *Handler {
	return &Handler{variableProvider: variableProvider}
}

// RegisterRoutes attaches all routes to the provided mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
}

// handleHealth returns 204 No Content for liveness checks.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
