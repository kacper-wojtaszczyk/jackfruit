package storage

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
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

func loadMinIOConfigFromEnv(t *testing.T) MinIOConfig {
	t.Helper()
	godotenv.Load("../../.env.test")

	endpoint := os.Getenv("MINIO_ENDPOINT")
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	useSSL := os.Getenv("MINIO_USE_SSL") == "true"

	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Fatalf("MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set for integration tests")
	}

	return MinIOConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    useSSL,
	}
}

func TestMinIOClient_Put_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := loadMinIOConfigFromEnv(t)
	cfg.Bucket = "test-bucket-" + time.Now().Format("20060102-150405")

	ctx := context.Background()
	client, err := NewMinIOClient(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to initialize minio client: %v", err)
	}

	key := "integration/hello.txt"
	content := "hello minio"

	if err := client.Put(ctx, key, strings.NewReader(content)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	obj, err := client.client.GetObject(ctx, cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	if string(data) != content {
		t.Fatalf("unexpected content: got %q, want %q", string(data), content)
	}
}
