package config

import (
	"os"
)

// Config holds the application configuration.
type Config struct {
	Port               string
	ClickHouseHost     string
	ClickHousePort     string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDatabase string
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Load reads configuration from environment variables.
func Load() *Config {
	return &Config{
		Port:               getEnv("PORT", "8080"),
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "localhost"),
		ClickHousePort:     getEnv("CLICKHOUSE_NATIVE_PORT", "9097"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "jackfruit"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", "jackfruit"),
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "jackfruit"),
	}
}
