package cds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/kacper-wojtaszczyk/jackfruit/ingestion-go/internal/ingestion"
)

// Request represents a CDS data request.
// This is the ONLY public type consumers need.
type Request interface {
	APIDataset() string // For CDS API endpoint (base dataset name)
	Payload() any
}

// Client interacts with the CDS API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client

	// Polling configuration (internal)
	pollInterval time.Duration
	pollTimeout  time.Duration
}

// NewClient creates a new CDS API client.
func NewClient(baseURL, apiKey string) *Client {
	c := &Client{
		baseURL:      baseURL,
		apiKey:       apiKey,
		pollInterval: 10 * time.Second, // sensible default
		pollTimeout:  30 * time.Minute, // sensible default
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	return c
}

// Fetch requests data from CDS and returns the file content with metadata.
// It handles the full async flow: submit → poll → download.
func (c *Client) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	cdsReq := &CAMSRequest{Date: req.Date, Dataset: req.Dataset}

	body, err := c.fetchInternal(ctx, cdsReq)
	if err != nil {
		return ingestion.FetchResult{}, err
	}

	return ingestion.FetchResult{
		Body:      body,
		Source:    "ads",
		Extension: "grib", // TODO: detect from asset type
	}, nil
}

// fetchInternal preserves existing behavior for tests that use Request directly.
func (c *Client) fetchInternal(ctx context.Context, req Request) (io.ReadCloser, error) {
	slog.InfoContext(ctx, "submitting execute request", "dataset", req.APIDataset())
	job, err := c.apiPostExecute(ctx, req)
	if err != nil {
		return nil, c.toClientError(err, "failed to submit execute request")
	}

	slog.InfoContext(ctx, "execute request submitted", "job_id", job.JobID, "status", job.Status)

	completedJob, err := c.waitForCompletion(ctx, job.JobID)
	if err != nil {
		return nil, c.toClientError(err, "failed to wait for job completion")
	}

	slog.InfoContext(ctx, "job completed", "job_id", completedJob.JobID, "status", completedJob.Status)

	resultResp, err := c.apiGetResults(ctx, completedJob.JobID)
	if err != nil {
		return nil, c.toClientError(err, "failed to get job results")
	}

	slog.InfoContext(ctx, "downloading result asset", "asset_url", resultResp.Asset.Value.Href, "asset_type", resultResp.Asset.Value.Type)

	assetBody, err := c.apiDownloadAsset(ctx, resultResp.Asset.Value.Href)
	if err != nil {
		return nil, c.toClientError(err, "failed to download asset")
	}

	return assetBody, nil
}

func (c *Client) doRequest(ctx context.Context, method string, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("PRIVATE-TOKEN", c.apiKey)

	// Set optional headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return c.httpClient.Do(req)
}

func (c *Client) apiPostExecute(ctx context.Context, req Request) (*jobResponse, error) {
	body, err := json.Marshal(req.Payload())
	if err != nil {
		return nil, err
	}
	slog.DebugContext(ctx, "gonna send it now", "dataset", req.APIDataset())
	response, err := c.doRequest(
		ctx,
		"POST",
		fmt.Sprintf("/processes/%s/execution", req.APIDataset()),
		bytes.NewBuffer(body),
		map[string]string{
			"Content-Type": "application/json",
			"Accept":       "application/json",
		},
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 201 {
		return nil, &apiError{StatusCode: response.StatusCode, Message: "execute request failed"}
	}

	slog.InfoContext(ctx, "CDS request submitted", "response_length", response.ContentLength)
	var job jobResponse
	err = json.NewDecoder(response.Body).Decode(&job)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (c *Client) apiGetJob(ctx context.Context, jobID string) (*jobResponse, error) {
	response, err := c.doRequest(
		ctx,
		"GET",
		fmt.Sprintf("/jobs/%s", jobID),
		nil,
		map[string]string{
			"Accept": "application/json",
		},
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, &apiError{StatusCode: response.StatusCode, Message: "failed to get job status"}
	}

	var job jobResponse
	err = json.NewDecoder(response.Body).Decode(&job)
	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (c *Client) apiGetResults(ctx context.Context, jobID string) (*resultResponse, error) {
	response, err := c.doRequest(
		ctx,
		"GET",
		fmt.Sprintf("/jobs/%s/results", jobID),
		nil,
		map[string]string{
			"Accept": "application/json",
		},
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, &apiError{StatusCode: response.StatusCode, Message: "failed to get job results"}
	}

	var resultResp resultResponse
	err = json.NewDecoder(response.Body).Decode(&resultResp)
	if err != nil {
		return nil, err
	}

	return &resultResp, nil
}

func (c *Client) apiDownloadAsset(ctx context.Context, assetURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", assetURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		resp.Body.Close() // Cleanup on error
		return nil, &apiError{StatusCode: resp.StatusCode, Message: "failed to download asset"}
	}

	// Caller must close this body
	return resp.Body, nil
}

// waitForCompletion polls until the job completes or fails.
func (c *Client) waitForCompletion(ctx context.Context, requestID string) (*jobResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.pollTimeout)
	defer cancel()

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			// continue polling
		}

		job, err := c.apiGetJob(ctx, requestID)
		if err != nil {
			return nil, err
		}

		switch job.Status {
		case jobStateSuccessful:
			return job, nil
		case jobStateFailed, jobStateRejected:
			return nil, fmt.Errorf("job %s failed with status: %s", requestID, job.Status)
		default:
			slog.InfoContext(ctx, "job not completed yet", "job_id", job.JobID, "status", job.Status)
		}
	}
}

// toClientError wraps an internal error into a ClientError for external consumers.
func (c *Client) toClientError(err error, context string) error {
	if err == nil {
		return nil
	}
	return &ClientError{
		Message: fmt.Sprintf("%s: %v", context, err),
	}
}
