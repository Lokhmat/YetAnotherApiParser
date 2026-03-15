package db

import (
	"fmt"
	"sync"

	"api-parser/internal/config"
	"api-parser/internal/core"
)

type Factory func(cfg config.DatabaseConfig) (core.MigrationTarget, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = factory
}

func New(name string, cfg config.DatabaseConfig) (core.MigrationTarget, error) {
	mu.RLock()
	factory, ok := factories[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown db provider %q", name)
	}
	return factory(cfg)
}
