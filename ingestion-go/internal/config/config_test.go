package config

import (
	"fmt"
	"os"
	"testing"
)

var configVars = []string{"CDS_API_KEY", "CDS_ADS_BASE_URL", "MINIO_URL"}

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
	if config.CDSAPIKey != testValue {
		t.Fatal()
	}
	if config.CDSBaseURL != testValue {
		t.Fatal()
	}
	if config.MinIOURL != testValue {
		t.Fatal()
	}
}
