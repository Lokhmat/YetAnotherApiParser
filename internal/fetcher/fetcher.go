package fetcher

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	maxRPM       int         // max requests per minute
	refillRate   float64     // tokens per second
	lastRequest  time.Time   // track time of last request for proper rate limiting
	requestCount int         // number of requests in current second
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
}

func New(maxRPM int) *Fetcher {
	// Create HTTP client with custom transport for testing
	// Note: In production, you should use proper SSL verification
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return &Fetcher{
		client: &http.Client{
			Transport: tr,
		},
		rateLimit: NewRateLimiter(maxRPM),
	}
}

func (f *Fetcher) FetchData(ctx context.Context, baseURL string, path string, op *openapi3.Operation, params map[string]string) ([]byte, error) {
	// Build the URL
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}

	// Add path - ensure proper slash handling
	if u.Path == "" {
		// If baseURL has no path, just use the path
		u.Path = strings.TrimLeft(path, "/")
	} else {
		// Otherwise, concatenate with proper slashes
		u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(path, "/")
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

	// Wait for rate limiter to allow the request
	if f.rateLimit != nil {
		f.rateLimit.Wait()
	}

	// Send request
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return body, nil
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
