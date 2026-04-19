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

	ctx, cancel := context.WithTimeout(r.Context(), 18*time.Second)
	defer cancel()

	varResults, err := h.variableProvider.GetVariables(
		ctx,
		envReq.Timestamp,
		envReq.Lat,
		envReq.Lon,
		envReq.Variables,
	)
	if err != nil {
		if stale, ok := errors.AsType[*domain.ErrDataTooStale](err); ok {
			writeError(w, http.StatusNotFound, stale.Error())
		} else if errors.Is(err, domain.ErrTemporalMiss) {
			writeError(w, http.StatusNotFound, "no data available at or before requested timestamp")
		} else if errors.Is(err, domain.ErrSpatialMiss) {
			writeError(w, http.StatusNotFound, "requested coordinates are outside data coverage")
		} else if ctx.Err() != nil {
			h.logger.Error("variableProvider.GetVariables timed out", "error", err)
			writeError(w, http.StatusGatewayTimeout, "query timed out")
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
			Lineage: LineageResponse{
				Source:    varResult.Lineage.Source,
				Dataset:   varResult.Lineage.Dataset,
				RawFileID: varResult.Lineage.RawFileID,
			},
		}
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
