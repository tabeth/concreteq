package config

import (
	"os"
	"strconv"
)

// Config holds all configuration for the application.
type Config struct {
	Port int
	// LogLevel string // Can be, DEBUG, INFO, WARNING, ERROR
	// StoragePath string
}

// NewConfig creates and returns a new Config instance, populating it from
// environment variables or using default values.
func NewConfig() *Config {
	return &Config{
		Port: getEnvAsInt("CONCRETEDB_PORT", 8000),
	}
}

// getEnvAsInt parses an environment variable as an integer.
// If the environment variable is not set, not a valid integer, or is empty,
// it returns the provided fallback value.
func getEnvAsInt(key string, fallback int) int {
	if valueStr, exists := os.LookupEnv(key); exists {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return fallback
}
