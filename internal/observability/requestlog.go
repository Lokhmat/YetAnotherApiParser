package observability

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type RequestEvent struct {
	Timestamp  time.Time
	Method     string
	URL        string
	Params     map[string]string
	StatusCode int
	Err        error
}

type RequestLogger interface {
	LogRequest(event RequestEvent)
}

type NopRequestLogger struct{}

func (NopRequestLogger) LogRequest(RequestEvent) {}

type MultiRequestLogger []RequestLogger

func (l MultiRequestLogger) LogRequest(event RequestEvent) {
	for _, logger := range l {
		if logger == nil {
			continue
		}
		logger.LogRequest(event)
	}
}

type FileRequestLogger struct {
	path string
	mu   sync.Mutex
}

func NewFileRequestLogger(path string) *FileRequestLogger {
	return &FileRequestLogger{path: path}
}

func (l *FileRequestLogger) LogRequest(event RequestEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	keys := make([]string, 0, len(event.Params))
	for k := range event.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, event.Params[k]))
	}

	line := fmt.Sprintf(
		"%s method=%s url=%s params={%s} status=%d",
		event.Timestamp.Format(time.RFC3339),
		event.Method,
		event.URL,
		strings.Join(pairs, ","),
		event.StatusCode,
	)
	if event.Err != nil {
		line += fmt.Sprintf(" error=%q", event.Err.Error())
	}
	line += "\n"
	_, _ = f.WriteString(line)
}
