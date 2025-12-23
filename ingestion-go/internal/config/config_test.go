package config

import (
	"fmt"
	"os"
	"testing"
)

var configVars = []string{"ADS_API_KEY", "ADS_BASE_URL", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"}

func TestLoad_RequiredVarsMissing(t *testing.T) {

	for _, configVar := range configVars {
		os.Setenv(configVar, "test-value")
	}
	for _, configVar := range configVars {
		t.Run(configVar, func(t *testing.T) {
			os.Unsetenv(configVar)
			defer os.Setenv(configVar, "test-value")
			_, err := Load()
			if err == nil {
				t.Fatal("expected error")
			}
			if y, ok := err.(*ErrMissingRequiredEnvVar); !ok {
				t.Fatalf("expected ErrMissingRequiredEnvVar, got %s", y)
			}
			var varName string
			c, _ := fmt.Sscanf(
				err.Error(),
				"required environment variable %q is not set",
				&varName,
			)
			if c != 1 || varName != configVar {
				t.Fatalf("expected ErrMissingRequiredEnvVar to be set to %q, got %q", configVar, varName)
			}
		})
	}
}

func TestLoad_ValidConfig(t *testing.T) {
	testValue := "test-value"
	for _, configVar := range configVars {
		os.Setenv(configVar, testValue)
	}

	config, err := Load()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if config.ADSAPIKey != testValue {
		t.Fatal()
	}
	if config.ADSBaseURL != testValue {
		t.Fatal()
	}
	if config.MinIOEndpoint != testValue {
		t.Fatal()
	}
	if config.MinIOAccessKey != testValue {
		t.Fatal()
	}
	if config.MinIOSecretKey != testValue {
		t.Fatal()
	}
	if config.MinIOBucket != testValue {
		t.Fatal()
	}
	if config.MinIOUseSSL {
		t.Fatal("expected MinIOUseSSL to be false by default")
	}
}

func TestLoad_SSL(t *testing.T) {
	testValue := "test-value"
	for _, configVar := range configVars {
		os.Setenv(configVar, testValue)
	}
	os.Setenv("MINIO_USE_SSL", "true")
	defer os.Unsetenv("MINIO_USE_SSL")

	config, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !config.MinIOUseSSL {
		t.Fatal("expected MinIOUseSSL to be true")
	}
}
