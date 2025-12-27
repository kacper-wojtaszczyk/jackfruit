package exitcode

// Exit codes for the ingestion CLI.
// Dagster can use these to decide retry strategy.
const (
	// Success - job completed successfully
	Success = 0

	// ConfigError - missing or invalid configuration
	// Don't retry: fix the config first
	ConfigError = 1

	// NetworkError - transient network failure (API timeout, DNS, etc.)
	// Retry with backoff
	NetworkError = 2

	// APIError - remote API returned an error (rate limit, auth, bad request)
	// Check logs, may need manual intervention
	APIError = 3

	// StorageError - failed to write to MinIO/S3
	// Retry with backoff
	StorageError = 4

	// DataError - received invalid/unparseable data from source
	// Don't retry: investigate the data
	DataError = 5
)
