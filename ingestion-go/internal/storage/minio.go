package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOClient implements ObjectStorage using MinIO.
type MinIOClient struct {
	client     *minio.Client
	bucketName string
}

// MinIOConfig holds MinIO connection settings.
type MinIOConfig struct {
	Endpoint  string // e.g., "localhost:9000"
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

// NewMinIOClient creates a new MinIO storage client.
func NewMinIOClient(ctx context.Context, cfg MinIOConfig) (*MinIOClient, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return &MinIOClient{
		client:     client,
		bucketName: cfg.Bucket,
	}, nil
}

// Put stores an object in MinIO.
func (m *MinIOClient) Put(ctx context.Context, key string, reader io.Reader) error {
	// Upload the file with PutObject
	_, err := m.client.PutObject(ctx, m.bucketName, key, reader, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream", // Default, can be improved later
	})
	if err != nil {
		return fmt.Errorf("failed to upload to minio: %w", err)
	}

	return nil
}
