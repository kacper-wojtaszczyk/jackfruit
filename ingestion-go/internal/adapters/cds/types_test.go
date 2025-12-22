package cds

import (
	"testing"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/dataset"
)

// Unit tests for CAMSRequest payload generation

func TestCAMSRequest_Payload_Analysis(t *testing.T) {
	req := &CAMSRequest{
		Date:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Dataset: dataset.CAMSEuropeAirQualityForecastsAnalysis,
	}

	payload := req.Payload()

	// Assert payload is not nil
	if payload == nil {
		t.Fatal("expected payload, got nil")
	}

	// Type assert to camsRequest to check internals
	camsReq, ok := payload.(camsRequest)
	if !ok {
		t.Fatalf("expected payload type camsRequest, got %T", payload)
	}

	// Analysis uses multiple time values and single leadtime_hour "0"
	expectedTimes := []string{"00:00", "04:00", "08:00", "12:00", "16:00", "20:00"}
	if len(camsReq.Inputs.Time) != len(expectedTimes) {
		t.Errorf("expected %d time values, got %d", len(expectedTimes), len(camsReq.Inputs.Time))
	}

	expectedLeadtime := []string{"0"}
	if len(camsReq.Inputs.LeadTimeHour) != 1 || camsReq.Inputs.LeadTimeHour[0] != "0" {
		t.Errorf("expected leadtime_hour %v, got %v", expectedLeadtime, camsReq.Inputs.LeadTimeHour)
	}

	// Check type field is "analysis"
	if len(camsReq.Inputs.Type) != 1 || camsReq.Inputs.Type[0] != "analysis" {
		t.Errorf("expected type 'analysis', got %v", camsReq.Inputs.Type)
	}

	// Check date format
	expectedDate := "2024-01-15/2024-01-15"
	if len(camsReq.Inputs.Date) != 1 || camsReq.Inputs.Date[0] != expectedDate {
		t.Errorf("expected date %s, got %v", expectedDate, camsReq.Inputs.Date)
	}
}

func TestCAMSRequest_Payload_Forecast(t *testing.T) {
	req := &CAMSRequest{
		Date:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Dataset: dataset.CAMSEuropeAirQualityForecastsForecast,
	}

	payload := req.Payload()

	// Assert payload is not nil
	if payload == nil {
		t.Fatal("expected payload, got nil")
	}

	// Type assert to camsRequest to check internals
	camsReq, ok := payload.(camsRequest)
	if !ok {
		t.Fatalf("expected payload type camsRequest, got %T", payload)
	}

	// Check forecast-specific fields
	// Forecast uses single time "00:00" and multiple leadtime_hour values
	expectedTime := []string{"00:00"}
	if len(camsReq.Inputs.Time) != 1 || camsReq.Inputs.Time[0] != "00:00" {
		t.Errorf("expected time %v, got %v", expectedTime, camsReq.Inputs.Time)
	}

	expectedLeadtimes := []string{"0", "4", "8", "12", "16", "20", "24", "28", "32", "36", "40", "44", "48"}
	if len(camsReq.Inputs.LeadTimeHour) != len(expectedLeadtimes) {
		t.Errorf("expected %d leadtime hours, got %d", len(expectedLeadtimes), len(camsReq.Inputs.LeadTimeHour))
	}

	// Check type field (analysis_forecast) is "forecast"
	if len(camsReq.Inputs.Type) != 1 || camsReq.Inputs.Type[0] != "forecast" {
		t.Errorf("expected type 'forecast', got %v", camsReq.Inputs.Type)
	}

	// Check date format
	expectedDate := "2024-01-15/2024-01-15"
	if len(camsReq.Inputs.Date) != 1 || camsReq.Inputs.Date[0] != expectedDate {
		t.Errorf("expected date %s, got %v", expectedDate, camsReq.Inputs.Date)
	}
}

func TestCAMSRequest_Payload_InvalidDataset(t *testing.T) {
	req := &CAMSRequest{
		Date:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Dataset: dataset.Dataset("invalid"),
	}

	payload := req.Payload()

	// Assert payload is nil for invalid Dataset
	if payload != nil {
		t.Errorf("expected nil payload for invalid Dataset, got %v", payload)
	}
}

func TestCAMSRequest_APIDataset(t *testing.T) {
	req := &CAMSRequest{
		Date:    time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		Dataset: dataset.CAMSEuropeAirQualityForecastsAnalysis,
	}

	apiDataset := req.APIDataset()
	expected := "cams-europe-air-quality-forecasts"

	if apiDataset != expected {
		t.Errorf("expected API dataset %s, got %s", expected, apiDataset)
	}
}
