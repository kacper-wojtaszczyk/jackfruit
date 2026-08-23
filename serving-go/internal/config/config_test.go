package config

import "testing"

func TestLoadPostgresSSLMode(t *testing.T) {
	t.Run("defaults to disable", func(t *testing.T) {
		t.Setenv("POSTGRES_SSLMODE", "")
		if got := Load().PostgresSSLMode; got != "disable" {
			t.Errorf("PostgresSSLMode = %q, want %q", got, "disable")
		}
	})

	t.Run("honors override", func(t *testing.T) {
		t.Setenv("POSTGRES_SSLMODE", "require")
		if got := Load().PostgresSSLMode; got != "require" {
			t.Errorf("PostgresSSLMode = %q, want %q", got, "require")
		}
	})
}
