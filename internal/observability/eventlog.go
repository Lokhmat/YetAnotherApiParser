package observability

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type Event struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Kind      string         `json:"kind"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type EventLogger interface {
	LogEvent(event Event)
}

type NopEventLogger struct{}

func (NopEventLogger) LogEvent(Event) {}

type FileEventLogger struct {
	path string
	mu   sync.Mutex
}

func NewFileEventLogger(path string) *FileEventLogger {
	return &FileEventLogger{path: path}
}

func (l *FileEventLogger) LogEvent(event Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	_, _ = f.Write(append(data, '\n'))
}
