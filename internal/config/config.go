package config

import (
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type APIConfig struct {
	Provider       string        `yaml:"provider"`
	BaseURL        string        `yaml:"base_url"`
	MaxRPM         int           `yaml:"max_rpm"`
	RequestTimeout time.Duration `yaml:"-"`
	Retries        RetryConfig   `yaml:"retries"`

	RequestTimeoutSeconds int `yaml:"request_timeout"`
}

type RetryConfig struct {
	ErrorsMaxRetries  int           `yaml:"errors_max_retries"`
	BasicRetryTimeout time.Duration `yaml:"-"`

	BasicRetryTimeoutSeconds int `yaml:"basic_retry_timeout"`
}

type DatabaseConfig struct {
	Provider         string `yaml:"provider"`
	ConnectionString string `yaml:"connection_string"`
}

type RuntimeConfig struct {
	SQLOutputPath string `yaml:"sql_output_path"`
	RunLogPath    string `yaml:"run_log_path"`
}

type Config struct {
	OpenAPIPath string         `yaml:"openapi_path"`
	API         APIConfig      `yaml:"api"`
	Database    DatabaseConfig `yaml:"database"`
	Runtime     RuntimeConfig  `yaml:"runtime"`
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
	cfg.applyDefaults()
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.API.Provider) == "" {
		c.API.Provider = "openapi_http"
	}
	if c.API.MaxRPM <= 0 {
		c.API.MaxRPM = 60
	}
	if c.API.RequestTimeoutSeconds <= 0 {
		c.API.RequestTimeoutSeconds = 30
	}
	c.API.RequestTimeout = time.Duration(c.API.RequestTimeoutSeconds) * time.Second
	if c.API.Retries.BasicRetryTimeoutSeconds <= 0 {
		c.API.Retries.BasicRetryTimeoutSeconds = 1
	}
	c.API.Retries.BasicRetryTimeout = time.Duration(c.API.Retries.BasicRetryTimeoutSeconds) * time.Second
	if c.API.Retries.ErrorsMaxRetries < 0 {
		c.API.Retries.ErrorsMaxRetries = 0
	}

	if strings.TrimSpace(c.Database.Provider) == "" {
		c.Database.Provider = "postgres"
	}

	if strings.TrimSpace(c.Runtime.SQLOutputPath) == "" {
		c.Runtime.SQLOutputPath = "res.sql"
	}
	if strings.TrimSpace(c.Runtime.RunLogPath) == "" {
		c.Runtime.RunLogPath = "runlog.log"
	}
}
