package config

import (
	"fmt"
	"os"
)

// Config holds application configuration.
// Add fields as needed throughout the project.
type Config struct {
	ADSAPIKey      string
	ADSBaseURL     string
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool
}

type ErrMissingRequiredEnvVar struct {
	Name string
}

func (e *ErrMissingRequiredEnvVar) Error() string {
	return fmt.Sprintf("required environment variable %q is not set", e.Name)
}

// Load reads configuration from environment variables.
// Returns an error if required variables are missing.
func Load() (*Config, error) {
	config := Config{}
	config.ADSBaseURL = os.Getenv("ADS_BASE_URL")
	if config.ADSBaseURL == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "ADS_BASE_URL"}
	}
	config.ADSAPIKey = os.Getenv("ADS_API_KEY")
	if config.ADSAPIKey == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "ADS_API_KEY"}
	}

	config.MinIOEndpoint = os.Getenv("MINIO_ENDPOINT")
	if config.MinIOEndpoint == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "MINIO_ENDPOINT"}
	}
	config.MinIOAccessKey = os.Getenv("MINIO_ACCESS_KEY")
	if config.MinIOAccessKey == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "MINIO_ACCESS_KEY"}
	}
	config.MinIOSecretKey = os.Getenv("MINIO_SECRET_KEY")
	if config.MinIOSecretKey == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "MINIO_SECRET_KEY"}
	}
	config.MinIOBucket = os.Getenv("MINIO_BUCKET")
	if config.MinIOBucket == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "MINIO_BUCKET"}
	}

	useSSLStr := os.Getenv("MINIO_USE_SSL")
	if useSSLStr == "true" {
		config.MinIOUseSSL = true
	}

	return &config, nil
}
