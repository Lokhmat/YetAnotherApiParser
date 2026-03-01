package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
)

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	mu           sync.Mutex
	tokens       float64
	lastRefill   time.Time
	maxRPM       int       // max requests per minute
	refillRate   float64   // tokens per second
	lastRequest  time.Time // track time of last request for proper rate limiting
	requestCount int       // number of requests in current second
}

var runLogMu sync.Mutex

func writeRunLog(method, requestURL string, params map[string]string, statusCode int, reqErr error) {
	runLogMu.Lock()
	defer runLogMu.Unlock()

	f, err := os.OpenFile("runlog.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	paramKeys := make([]string, 0, len(params))
	for k := range params {
		paramKeys = append(paramKeys, k)
	}
	sort.Strings(paramKeys)

	paramPairs := make([]string, 0, len(paramKeys))
	for _, k := range paramKeys {
		paramPairs = append(paramPairs, fmt.Sprintf("%s=%s", k, params[k]))
	}

	line := fmt.Sprintf(
		"%s method=%s url=%s params={%s} status=%d",
		time.Now().Format(time.RFC3339),
		method,
		requestURL,
		strings.Join(paramPairs, ","),
		statusCode,
	)
	if reqErr != nil {
		line += fmt.Sprintf(" error=%q", reqErr.Error())
	}
	line += "\n"

	_, _ = f.WriteString(line)
}

// NewRateLimiter creates a new rate limiter based on max requests per minute
func NewRateLimiter(maxRPM int) *RateLimiter {
	if maxRPM <= 0 {
		maxRPM = 60 // default to 60 requests per minute (1 per second)
	}
	return &RateLimiter{
		tokens:     float64(maxRPM),
		lastRefill: time.Now(),
		maxRPM:     maxRPM,
		refillRate: float64(maxRPM) / 60.0, // refill tokens at maxRPM per minute
	}
}

// Wait blocks until a request can be made, ensuring proper rate limiting
func (rl *RateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Refill tokens based on time elapsed
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > float64(rl.maxRPM) {
		rl.tokens = float64(rl.maxRPM)
	}
	rl.lastRefill = now

	// If no tokens available, wait until the next token is available
	if rl.tokens < 1 {
		// Calculate wait time until next token
		waitTime := time.Duration((1 - rl.tokens) / rl.refillRate * float64(time.Second))
		rl.lastRefill = now.Add(waitTime)
		rl.tokens = 0

		rl.mu.Unlock()
		time.Sleep(waitTime)
		rl.mu.Lock()
		rl.lastRefill = time.Now()
		rl.tokens = 0 // tokens are now 0 since we just used one
	}

	rl.tokens--
	rl.lastRequest = now
}

type Fetcher struct {
	client    *http.Client
	rateLimit *RateLimiter
	cfg       ClientConfig
}

type ClientConfig struct {
	RequestTimeout    time.Duration
	ErrorsMaxRetries  int
	BasicRetryTimeout time.Duration
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		RequestTimeout:    30 * time.Second,
		ErrorsMaxRetries:  0,
		BasicRetryTimeout: 1 * time.Second,
	}
}

func New(maxRPM int, cfgs ...ClientConfig) *Fetcher {
	cfg := DefaultClientConfig()
	if len(cfgs) > 0 {
		cfg = cfgs[0]
		if cfg.RequestTimeout <= 0 {
			cfg.RequestTimeout = 30 * time.Second
		}
		if cfg.BasicRetryTimeout <= 0 {
			cfg.BasicRetryTimeout = 1 * time.Second
		}
		if cfg.ErrorsMaxRetries < 0 {
			cfg.ErrorsMaxRetries = 0
		}
	}

	// Use secure defaults: standard certificate verification and system CAs.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ForceAttemptHTTP2 = true
	// Keep direct behavior from previous custom transport: do not auto-use env proxies.
	tr.Proxy = nil

	return &Fetcher{
		client: &http.Client{
			Transport: tr,
			Timeout:   cfg.RequestTimeout,
		},
		rateLimit: NewRateLimiter(maxRPM),
		cfg:       cfg,
	}
}

func (f *Fetcher) FetchData(ctx context.Context, baseURL string, path string, op *openapi3.Operation, params map[string]string) ([]byte, error) {
	// Build the URL
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}

	// Resolve path parameters in /resource/{id} style paths.
	resolvedPath := path
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		p := paramRef.Value
		if p.In != "path" {
			continue
		}

		val, ok := params[p.Name]
		if !ok {
			if p.Required {
				return nil, fmt.Errorf("missing required path parameter: %s", p.Name)
			}
			continue
		}

		resolvedPath = strings.ReplaceAll(resolvedPath, "{"+p.Name+"}", url.PathEscape(val))
	}

	// Add path - ensure proper slash handling
	if u.Path == "" {
		// If baseURL has no path, just use the path
		u.Path = strings.TrimLeft(resolvedPath, "/")
	} else {
		// Otherwise, concatenate with proper slashes
		u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(resolvedPath, "/")
	}

	// Build query parameters
	q := u.Query()
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		p := paramRef.Value

		if p.In == "query" {
			if val, ok := params[p.Name]; ok {
				q.Set(p.Name, val)
			} else if p.Required {
				return nil, fmt.Errorf("missing required query parameter: %s", p.Name)
			}
		}
	}
	u.RawQuery = q.Encode()

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Add default User-Agent header to mimic a browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

	// Add headers
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		p := paramRef.Value

		if p.In == "header" {
			if val, ok := params[p.Name]; ok {
				req.Header.Set(p.Name, val)
			} else if p.Required {
				return nil, fmt.Errorf("missing required header parameter: %s", p.Name)
			}
		}
	}

	attempts := f.cfg.ErrorsMaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		// Wait for rate limiter before each attempt
		if f.rateLimit != nil {
			f.rateLimit.Wait()
		}

		// Send request
		resp, err := f.client.Do(req)
		if err != nil {
			writeRunLog(http.MethodGet, u.String(), params, -1, err)
			return nil, fmt.Errorf("send request: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		writeRunLog(http.MethodGet, u.String(), params, resp.StatusCode, nil)
		if readErr != nil {
			return nil, fmt.Errorf("read response body: %w", readErr)
		}

		// Success
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}

		// Retry policy
		shouldRetry := false
		retryDelay := f.cfg.BasicRetryTimeout
		if resp.StatusCode == http.StatusTooManyRequests {
			shouldRetry = attempt < f.cfg.ErrorsMaxRetries
			retryDelay = f.cfg.BasicRetryTimeout * time.Duration(1<<attempt)
		} else if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			shouldRetry = attempt < f.cfg.ErrorsMaxRetries
		}

		if !shouldRetry {
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
		}

		if err := sleepWithContext(ctx, retryDelay); err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("request failed after retries")
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (f *Fetcher) FetchAndParse(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, target interface{}) error {
	data, err := f.FetchData(ctx, baseURL, path, op, params)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("unmarshal JSON: %w", err)
	}

	return nil
}
