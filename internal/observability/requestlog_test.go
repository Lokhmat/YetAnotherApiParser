package observability

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileRequestLoggerWritesSortedStructuredLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.log")
	logger := NewFileRequestLogger(path)

	logger.LogRequest(RequestEvent{
		Timestamp:  time.Date(2026, 3, 21, 10, 11, 12, 0, time.UTC),
		Method:     "GET",
		URL:        "https://api.example.com/users?id=1",
		Params:     map[string]string{"z": "last", "a": "first"},
		StatusCode: 200,
		Err:        errors.New("boom"),
	})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	line := string(data)
	if !strings.Contains(line, "2026-03-21T10:11:12Z method=GET url=https://api.example.com/users?id=1") {
		t.Fatalf("unexpected log line: %s", line)
	}
	if !strings.Contains(line, "params={a=first,z=last}") {
		t.Fatalf("expected sorted params, got %s", line)
	}
	if !strings.Contains(line, `status=200 error="boom"`) {
		t.Fatalf("expected quoted error text, got %s", line)
	}
}

func TestFileRequestLoggerIgnoresOpenErrors(t *testing.T) {
	logger := NewFileRequestLogger(t.TempDir())

	logger.LogRequest(RequestEvent{
		Timestamp:  time.Now(),
		Method:     "GET",
		URL:        "https://api.example.com/health",
		StatusCode: 200,
	})
}
