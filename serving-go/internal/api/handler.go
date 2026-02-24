package api

import "net/http"

// Handler holds shared dependencies for all HTTP handlers.
// Fields will be added here as the domain service and config are wired in.
type Handler struct{}

// NewHandler creates a new Handler.
func NewHandler() *Handler {
	return &Handler{}
}

// RegisterRoutes attaches all routes to the provided mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
}

// handleHealth returns 204 No Content for liveness checks.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
