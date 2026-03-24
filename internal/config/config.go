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
	EventLogPath  string `yaml:"event_log_path"`

	FullReloadInterval        time.Duration `yaml:"-"`
	FullReloadEnabled         bool          `yaml:"-"`
	FullReloadIntervalSeconds int           `yaml:"full_reload_interval_seconds"`
}

type ControlConfig struct {
	ListenAddr   string `yaml:"listen_addr"`
	Enabled      *bool  `yaml:"enabled"`
	HistoryLimit int    `yaml:"history_limit"`
}

type Config struct {
	OpenAPIPath string         `yaml:"openapi_path"`
	API         APIConfig      `yaml:"api"`
	Database    DatabaseConfig `yaml:"database"`
	Runtime     RuntimeConfig  `yaml:"runtime"`
	Control     ControlConfig  `yaml:"control"`
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
	cfg.expandEnv()
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
	if strings.TrimSpace(c.Runtime.EventLogPath) == "" {
		c.Runtime.EventLogPath = "events.log"
	}
	if c.Runtime.FullReloadIntervalSeconds < 0 {
		c.Runtime.FullReloadIntervalSeconds = 0
	}
	c.Runtime.FullReloadEnabled = c.Runtime.FullReloadIntervalSeconds > 0
	c.Runtime.FullReloadInterval = time.Duration(c.Runtime.FullReloadIntervalSeconds) * time.Second

	if strings.TrimSpace(c.Control.ListenAddr) == "" {
		c.Control.ListenAddr = ":8080"
	}
	if c.Control.Enabled == nil {
		enabled := true
		c.Control.Enabled = &enabled
	}
	if c.Control.HistoryLimit <= 0 {
		c.Control.HistoryLimit = 20
	}
}

func (c *Config) expandEnv() {
	c.OpenAPIPath = os.ExpandEnv(c.OpenAPIPath)
	c.API.Provider = os.ExpandEnv(c.API.Provider)
	c.API.BaseURL = os.ExpandEnv(c.API.BaseURL)
	c.Database.Provider = os.ExpandEnv(c.Database.Provider)
	c.Database.ConnectionString = os.ExpandEnv(c.Database.ConnectionString)
	c.Runtime.SQLOutputPath = os.ExpandEnv(c.Runtime.SQLOutputPath)
	c.Runtime.RunLogPath = os.ExpandEnv(c.Runtime.RunLogPath)
	c.Runtime.EventLogPath = os.ExpandEnv(c.Runtime.EventLogPath)
	c.Control.ListenAddr = os.ExpandEnv(c.Control.ListenAddr)
}

func (c Config) ControlEnabled() bool {
	return c.Control.Enabled != nil && *c.Control.Enabled
}
