package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type APIConfig struct {
	BaseURL string `yaml:"base_url"`
	MaxRPM  int    `yaml:"max_rpm"` // Maximum requests per minute
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
