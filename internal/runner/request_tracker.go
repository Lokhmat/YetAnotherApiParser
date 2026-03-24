package runner

import (
	"sync"

	"api-parser/internal/observability"
)

type RequestTracker struct {
	mu                 sync.Mutex
	active             bool
	requestCount       int
	failedRequestCount int
}

func NewRequestTracker() *RequestTracker {
	return &RequestTracker{}
}

func (t *RequestTracker) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.active = true
	t.requestCount = 0
	t.failedRequestCount = 0
}

func (t *RequestTracker) Finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active = false
}

func (t *RequestTracker) Snapshot() (requestCount, failedRequestCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requestCount, t.failedRequestCount
}

func (t *RequestTracker) LogRequest(event observability.RequestEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.active {
		return
	}
	t.requestCount++
	if event.Err != nil || event.StatusCode < 200 || event.StatusCode >= 400 {
		t.failedRequestCount++
	}
}
