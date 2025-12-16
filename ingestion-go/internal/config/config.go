package config

import (
	"fmt"
	"os"
)

// Config holds application configuration.
// Add fields as needed throughout the project.
type Config struct {
	CDSAPIKey  string
	CDSBaseURL string
	MinIOURL   string
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
	config.CDSBaseURL = os.Getenv("CDS_BASE_URL")
	if config.CDSBaseURL == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "CDS_BASE_URL"}
	}
	config.CDSAPIKey = os.Getenv("CDS_API_KEY")
	if config.CDSAPIKey == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "CDS_API_KEY"}
	}
	config.MinIOURL = os.Getenv("MINIO_URL")
	if config.MinIOURL == "" {
		return nil, &ErrMissingRequiredEnvVar{Name: "MINIO_URL"}
	}

	return &config, nil
}
