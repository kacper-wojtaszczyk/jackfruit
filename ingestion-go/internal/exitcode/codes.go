package exitcode

// Exit codes for the ingestion CLI.
// Dagster can use these to decide retry strategy.
const (
	// Success - job completed successfully
	Success = 0

	// ConfigError - missing or invalid configuration
	// Don't retry: fix the config first
	ConfigError = 1

	// ApplicationError - Runtime error during application execution
	// Don't retry: investigate the error (in the future split into more specific (potentially retryable) errors)
	ApplicationError = 2
)
