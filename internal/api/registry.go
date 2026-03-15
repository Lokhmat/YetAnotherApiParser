package api

import (
	"fmt"
	"sync"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
)

type Factory func(cfg config.APIConfig, logger observability.RequestLogger) (core.APIConnector, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = factory
}

func New(name string, cfg config.APIConfig, logger observability.RequestLogger) (core.APIConnector, error) {
	mu.RLock()
	factory, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown api provider %q", name)
	}
	return factory(cfg, logger)
}
