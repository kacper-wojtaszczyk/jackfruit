package config

import (
	"os"
	"testing"
)

func TestLoad_RequiredVarsMissing(t *testing.T) {
	os.Unsetenv("CDS_API_KEY")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if y, ok := err.(*ErrMissingRequiredEnvVar); !ok {
		t.Fatalf("expected ErrMissingRequiredEnvVar, got %s", y)
	}
}

func TestLoad_ValidConfig(t *testing.T) {
	csdKey := "test-key"
	os.Setenv("CDS_API_KEY", csdKey)
	defer os.Unsetenv("CDS_API_KEY")
	csdUrl := "test-url"
	os.Setenv("CDS_BASE_URL", csdUrl)
	defer os.Unsetenv("CDS_BASE_URL")
	minioUrl := "test-url"
	os.Setenv("MINIO_URL", minioUrl)
	defer os.Unsetenv("MINIO_URL")

	config, err := Load()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if config.CDSAPIKey != csdKey {
		t.Fatal()
	}
	if config.CDSBaseURL != csdUrl {
		t.Fatal()
	}
	if config.MinIOURL != minioUrl {
		t.Fatal()
	}
}
