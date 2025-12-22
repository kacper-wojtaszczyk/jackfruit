package storage

import (
	"context"
	"testing"
)

func TestNewMinIOClient_InvalidEndpoint(t *testing.T) {
	// Test with an invalid endpoint to trigger initialization error
	cfg := MinIOConfig{
		Endpoint:  "invalid-endpoint:port:scheme", // Invalid format
		AccessKey: "minio",
		SecretKey: "minio123",
		Bucket:    "test-bucket",
		UseSSL:    false,
	}

	_, err := NewMinIOClient(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error with invalid endpoint, got nil")
	}
}

func TestNewMinIOClient_ConnectionRefused(t *testing.T) {
	// Test connection failure (assuming no MinIO at localhost:12345)
	cfg := MinIOConfig{
		Endpoint:  "localhost:12345",
		AccessKey: "minio",
		SecretKey: "minio123",
		Bucket:    "test-bucket",
		UseSSL:    false,
	}

	// Note: minio.New() doesn't connect immediately, but BucketExists does.
	_, err := NewMinIOClient(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error connecting to non-existent minio, got nil")
	}
}

func TestMinIOClient_Put_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This test requires a running MinIO instance.
	// It attempts to connect to localhost:9000 by default.
	// If connection fails, we skip.

	cfg := MinIOConfig{
		Endpoint:  "localhost:9000",
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		Bucket:    "test-bucket",
		UseSSL:    false,
	}

	client, err := NewMinIOClient(context.Background(), cfg)
	if err != nil {
		t.Skipf("skipping integration test: failed to connect to minio: %v", err)
	}

	// If we got here, we have a connection.
	// Try to Put something.
	// Note: This might fail if bucket creation failed in NewMinIOClient but we didn't catch it (unlikely)
	// or if we don't have permissions.

	// For now, just a placeholder to show intent.
	// In a real CI, we'd spin up MinIO service.
	_ = client
}
