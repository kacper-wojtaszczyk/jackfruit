package cds

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient_Fetch(t *testing.T) {
	// Track which endpoints were called
	var submitCalled, statusCalled, resultsCalled, downloadCalled bool

	// Create a mock server that simulates the CDS API
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/processes/"):
			// Submit endpoint
			submitCalled = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "accepted"}`))

		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/") && strings.Contains(r.URL.Path, "/results"):
			// Results endpoint — return asset info
			resultsCalled = true
			w.Header().Set("Content-Type", "application/json")
			// Build download URL from the request host
			downloadURL := fmt.Sprintf("http://%s/download/test-123", r.Host)
			response := fmt.Sprintf(`{
				"asset": {
					"value": {
						"type": "application/x-netcdf",
						"href": "%s"
					}
				}
			}`, downloadURL)
			_, _ = w.Write([]byte(response))

		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Status endpoint — return completed immediately
			statusCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"jobID": "test-123",
				"status": "successful"
			}`))

		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/download/"):
			// Download endpoint
			downloadCalled = true
			_, _ = w.Write([]byte("fake netcdf data"))

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// Create client pointing to mock server
	client := NewClient(server.URL, "test-key")
	// Speed up polling for tests
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 1 * time.Second

	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	// 1. Call client.Fetch(ctx, req)
	body, err := client.Fetch(ctx, req)

	// 2. Assert no error
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	defer body.Close()

	// 3. Read the response body and verify content
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	expectedContent := "fake netcdf data"
	if string(data) != expectedContent {
		t.Errorf("expected body content %q, got %q", expectedContent, string(data))
	}

	// 4. Assert all endpoints were called
	if !submitCalled {
		t.Error("submit endpoint was not called")
	}
	if !statusCalled {
		t.Error("status endpoint was not called")
	}
	if !resultsCalled {
		t.Error("results endpoint was not called")
	}
	if !downloadCalled {
		t.Error("download endpoint was not called")
	}
}

func TestClient_Fetch_JobFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "fail-123", "status": "accepted"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Return failed state
			_, _ = w.Write([]byte(`{"jobID": "fail-123", "status": "failed"}`))
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 1 * time.Second

	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	// 1. Call Fetch
	_, err := client.Fetch(ctx, req)

	// 2. Assert error is not nil
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// 3. Check error message contains "failed"
	if !strings.Contains(err.Error(), "failed") {
		t.Errorf("expected error message to contain 'failed', got: %v", err)
	}
}

func TestClient_Fetch_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "slow-123", "status": "accepted"}`))
		case r.Method == "GET":
			// Always return running — never completes
			_, _ = w.Write([]byte(`{"jobID": "slow-123", "status": "running"}`))
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 100 * time.Millisecond // Very short timeout for test

	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	// 1. Call Fetch
	_, err := client.Fetch(ctx, req)

	// 2. Assert error is context.DeadlineExceeded
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err.Error())
	}
}

func TestClient_Fetch_JobRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "reject-123", "status": "accepted"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Return rejected state
			_, _ = w.Write([]byte(`{"jobID": "reject-123", "status": "rejected"}`))
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 1 * time.Second

	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	// 1. Call Fetch
	_, err := client.Fetch(ctx, req)

	// 2. Assert error is not nil
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// 3. Check error message contains "rejected"
	if !strings.Contains(err.Error(), "rejected") {
		t.Errorf("expected error message to contain 'rejected', got: %v", err)
	}
}

func TestClient_Fetch_SubmitNon201(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			// Return 500 error on submit
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	_, err := client.Fetch(ctx, req)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should get apiError with status code
	if !strings.Contains(err.Error(), "execute request failed") {
		t.Errorf("expected error message to contain 'execute request failed', got: %v", err)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error message to contain status code 500, got: %v", err)
	}
}

func TestClient_Fetch_StatusCheckNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "accepted"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Return 503 error on status check
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 100 * time.Millisecond
	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	_, err := client.Fetch(ctx, req)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should get error with unexpected status code
	if !strings.Contains(err.Error(), "unexpected status code") {
		t.Errorf("expected error message to contain 'unexpected status code', got: %v", err)
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("expected error message to contain status code 503, got: %v", err)
	}
}

func TestClient_Fetch_ResultsNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "accepted"}`))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/results"):
			// Return 404 error on results fetch
			w.WriteHeader(http.StatusNotFound)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Status check returns successful
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "successful"}`))
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 1 * time.Second
	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	_, err := client.Fetch(ctx, req)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should get error with unexpected status code when fetching results
	if !strings.Contains(err.Error(), "unexpected status code") {
		t.Errorf("expected error message to contain 'unexpected status code', got: %v", err)
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error message to contain status code 404, got: %v", err)
	}
}

func TestClient_Fetch_AssetDownloadNon200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "accepted"}`))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/results"):
			// Return asset info with download URL
			downloadURL := fmt.Sprintf("http://%s/download/test-123", r.Host)
			response := fmt.Sprintf(`{
				"asset": {
					"value": {
						"type": "application/x-netcdf",
						"href": "%s"
					}
				}
			}`, downloadURL)
			_, _ = w.Write([]byte(response))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/download/"):
			// Return 403 error on asset download
			w.WriteHeader(http.StatusForbidden)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/jobs/"):
			// Status check returns successful
			_, _ = w.Write([]byte(`{"jobID": "test-123", "status": "successful"}`))
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	client.pollInterval = 10 * time.Millisecond
	client.pollTimeout = 1 * time.Second
	ctx := context.Background()
	req := &CAMSRequest{
		Date:             time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		AnalysisForecast: AnalysisForecastAnalysis,
	}

	_, err := client.Fetch(ctx, req)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should get error with unexpected status code when downloading asset
	if !strings.Contains(err.Error(), "unexpected status code") {
		t.Errorf("expected error message to contain 'unexpected status code', got: %v", err)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected error message to contain status code 403, got: %v", err)
	}
}
