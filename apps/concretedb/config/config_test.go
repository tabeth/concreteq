package config

import (
	"os"
	"testing"
)

// TestNewConfigDefaults verifies that NewConfig returns a Config struct
// with the correct default values when no environment variables are set.
func TestNewConfigDefaults(t *testing.T) {
	// Ensure the environment variable is not set.
	os.Unsetenv("CONCRETEDB_PORT")

	cfg := NewConfig()

	expectedPort := 8000
	if cfg.Port != expectedPort {
		t.Errorf("expected default port %d, but got %d", expectedPort, cfg.Port)
	}
}

// TestNewConfigWithEnvVars verifies that NewConfig correctly loads configuration
// from environment variables when they are set.
func TestNewConfigWithEnvVars(t *testing.T) {
	// Set a custom port as an environment variable.
	customPort := "9001"
	os.Setenv("CONCRETEDB_PORT", customPort)
	// 'defer' ensures that os.Unsetenv is called at the end of the test,
	// cleaning up the environment for subsequent tests.
	defer os.Unsetenv("CONCRETEDB_PORT")

	cfg := NewConfig()

	expectedPort := 9001
	if cfg.Port != expectedPort {
		t.Errorf("expected port from env var to be %d, but got %d", expectedPort, cfg.Port)
	}
}

// TestNewConfigWithInvalidEnvVar verifies that the fallback is used when the
// environment variable is not a valid integer.
func TestNewConfigWithInvalidEnvVar(t *testing.T) {
	os.Setenv("CONCRETEDB_PORT", "not-a-port")
	defer os.Unsetenv("CONCRETEDB_PORT")

	cfg := NewConfig()

	expectedPort := 8000 // Fallback port
	if cfg.Port != expectedPort {
		t.Errorf("expected fallback port %d for invalid env var, but got %d", expectedPort, cfg.Port)
	}
}
