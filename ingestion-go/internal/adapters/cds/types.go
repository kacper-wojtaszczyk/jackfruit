package cds

import (
	"time"
)

// Dataset identifies a dataset internally.
type Dataset string

const (
	DatasetCAMSAnalysis Dataset = "cams-europe-air-quality-forecasts-analysis"
	DatasetCAMSForecast Dataset = "cams-europe-air-quality-forecasts-forecast"
	// Add more as needed (e.g., DatasetGloFAS)
)

// CAMSRequest represents a CAMS data request.
type CAMSRequest struct {
	Date    time.Time
	Dataset Dataset
}

// APIDataset returns the CDS API dataset name for the request.
func (r *CAMSRequest) APIDataset() string {
	return "cams-europe-air-quality-forecasts"
}

func (r *CAMSRequest) Payload() any {
	switch r.Dataset {
	case DatasetCAMSAnalysis:
		return camsRequest{
			Inputs: camsInputs{
				[]string{"particulate_matter_2.5um", "particulate_matter_10um"},
				[]string{"ensemble"},
				[]string{"0"},
				[]string{r.Date.Format("2006-01-02/2006-01-02")},
				[]string{"analysis"},
				[]string{"00:00", "04:00", "08:00", "12:00", "16:00", "20:00"},
				[]string{"0"},
				"netcdf_zip",
			},
		}
	case DatasetCAMSForecast:
		return camsRequest{
			Inputs: camsInputs{
				[]string{"particulate_matter_2.5um", "particulate_matter_10um"},
				[]string{"ensemble"},
				[]string{"0"},
				[]string{r.Date.Format("2006-01-02/2006-01-02")},
				[]string{"forecast"},
				[]string{"00:00"},
				[]string{"0", "4", "8", "12", "16", "20", "24", "28", "32", "36", "40", "44", "48"},
				"netcdf_zip",
			},
		}
	default:
		return nil
	}
}

// jobState represents the state of a CDS job (internal).
type jobState string

const (
	jobStateAccepted   jobState = "accepted"
	jobStateRunning    jobState = "running"
	jobStateSuccessful jobState = "successful"
	jobStateFailed     jobState = "failed"
	jobStateRejected   jobState = "rejected"
	jobStateDismissed  jobState = "dismissed"
)

// jobResponse is the raw API response for job submission.
type jobResponse struct {
	Asset  asset    `json:"asset"`
	JobID  string   `json:"jobID"`
	Status jobState `json:"status"`
}

// resultResponse is the raw API response for job submission.
type resultResponse struct {
	Asset asset `json:"asset"`
}

type asset struct {
	Value value `json:"value"`
}
type value struct {
	Type string `json:"type"`
	Href string `json:"href"`
}

type camsInputs struct {
	Variable     []string `json:"variable"`
	Model        []string `json:"model"`
	Level        []string `json:"level"`
	Date         []string `json:"date"`
	Type         []string `json:"type"`
	Time         []string `json:"time"`
	LeadTimeHour []string `json:"leadtime_hour"`
	DataFormat   string   `json:"data_format"`
}
type camsRequest struct {
	Inputs camsInputs `json:"inputs"`
}
