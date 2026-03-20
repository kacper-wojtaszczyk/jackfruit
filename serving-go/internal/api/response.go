package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type EnvironmentalResponse struct {
	Lat                float32            `json:"lat"`
	Lon                float32            `json:"lon"`
	RequestedTimestamp time.Time          `json:"requested_timestamp"`
	Variables          []VariableResponse `json:"variables"`
}

type VariableResponse struct {
	Name         string          `json:"name"`
	Value        float64         `json:"value"`
	Unit         string          `json:"unit"`
	RefTimestamp time.Time       `json:"ref_timestamp"`
	ActualLat    float32         `json:"actual_lat"`
	ActualLon    float32         `json:"actual_lon"`
	Lineage      LineageResponse `json:"lineage"`
}

type LineageResponse struct {
	Source    string    `json:"source"`
	Dataset   string    `json:"dataset"`
	RawFileID uuid.UUID `json:"raw_file_id"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}
