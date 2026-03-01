package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type APIConfig struct {
	BaseURL        string      `yaml:"base_url"`
	MaxRPM         int         `yaml:"max_rpm"`         // Maximum requests per minute
	RequestTimeout int         `yaml:"request_timeout"` // Request timeout in seconds
	Retries        RetryConfig `yaml:"retries"`         // Retry configuration
}

type RetryConfig struct {
	ErrorsMaxRetries  int `yaml:"errors_max_retries"`  // Retries for 5xx and 429
	BasicRetryTimeout int `yaml:"basic_retry_timeout"` // Base retry timeout in seconds
}

type DatabaseConfig struct {
	ConnectionString string `yaml:"connection_string"`
}

type Config struct {
	OpenAPIPath string         `yaml:"openapi_path"`
	API         APIConfig      `yaml:"api"`
	Database    DatabaseConfig `yaml:"database"`
}

func Load(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
