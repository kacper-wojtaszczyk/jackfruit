package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/serving-go/internal/domain"
)

type Handler struct {
	variableProvider variableProvider
	logger           *slog.Logger
}

type variableProvider interface {
	GetVariables(ctx context.Context, ts time.Time, lat float32, lon float32, vars []string) ([]domain.VariableResult, error)
}

func NewHandler(variableProvider variableProvider, logger *slog.Logger) *Handler {
	return &Handler{variableProvider: variableProvider, logger: logger}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("GET /v1/environmental", h.handleEnvironmental)
}

func (h *Handler) handleEnvironmental(w http.ResponseWriter, r *http.Request) {
	envReq, err := ParseEnvironmentalRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	varResults, err := h.variableProvider.GetVariables(
		r.Context(),
		envReq.Timestamp,
		envReq.Lat,
		envReq.Lon,
		envReq.Variables,
	)
	if err != nil {
		if notFound, ok := errors.AsType[*domain.ErrVariableNotFound](err); ok {
			writeError(w, http.StatusNotFound, notFound.Error())
		} else {
			h.logger.Error("variableProvider.GetVariables failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
		return
	}

	response := EnvironmentalResponse{
		Lat:                envReq.Lat,
		Lon:                envReq.Lon,
		RequestedTimestamp: envReq.Timestamp,
		Variables:          make([]VariableResponse, len(varResults)),
	}
	for i, varResult := range varResults {
		response.Variables[i] = VariableResponse{
			Name:         varResult.Name,
			Value:        float64(varResult.Value),
			Unit:         varResult.Unit,
			RefTimestamp: varResult.RefTimestamp,
			ActualLat:    varResult.ActualLat,
			ActualLon:    varResult.ActualLon,
		}
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
