package http

import (
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"api-parser/internal/api"
	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
)

type RateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	maxRPM     int
	refillRate float64
}

func NewRateLimiter(maxRPM int) *RateLimiter {
	if maxRPM <= 0 {
		maxRPM = 60
	}
	return &RateLimiter{
		tokens:     float64(maxRPM),
		lastRefill: time.Now(),
		maxRPM:     maxRPM,
		refillRate: float64(maxRPM) / 60.0,
	}
}

func (rl *RateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()
	rl.tokens += elapsed * rl.refillRate
	if rl.tokens > float64(rl.maxRPM) {
		rl.tokens = float64(rl.maxRPM)
	}
	rl.lastRefill = now

	if rl.tokens < 1 {
		waitTime := time.Duration((1 - rl.tokens) / rl.refillRate * float64(time.Second))
		rl.lastRefill = now.Add(waitTime)
		rl.tokens = 0

		rl.mu.Unlock()
		time.Sleep(waitTime)
		rl.mu.Lock()
		rl.lastRefill = time.Now()
		rl.tokens = 0
	}

	rl.tokens--
}

type connector struct {
	client    *nethttp.Client
	rateLimit *RateLimiter
	cfg       config.APIConfig
	logger    observability.RequestLogger
}

func init() {
	api.Register("openapi_http", func(cfg config.APIConfig, logger observability.RequestLogger) (core.APIConnector, error) {
		return newConnector(cfg, logger, nil), nil
	})
}

func newConnector(cfg config.APIConfig, logger observability.RequestLogger, client *nethttp.Client) *connector {
	if logger == nil {
		logger = observability.NopRequestLogger{}
	}
	if client == nil {
		tr := nethttp.DefaultTransport.(*nethttp.Transport).Clone()
		tr.ForceAttemptHTTP2 = true
		tr.Proxy = nil
		client = &nethttp.Client{
			Transport: tr,
			Timeout:   cfg.RequestTimeout,
		}
	}
	return &connector{
		client:    client,
		rateLimit: NewRateLimiter(cfg.MaxRPM),
		cfg:       cfg,
		logger:    logger,
	}
}

func (c *connector) Fetch(ctx context.Context, req core.FetchRequest) (core.FetchResult, error) {
	u, err := url.Parse(req.BaseURL)
	if err != nil {
		return core.FetchResult{}, fmt.Errorf("parse base URL: %w", err)
	}

	resolvedPath := req.Path
	for key, value := range req.PathParams {
		resolvedPath = strings.ReplaceAll(resolvedPath, "{"+key+"}", url.PathEscape(value))
	}

	if u.Path == "" {
		u.Path = strings.TrimLeft(resolvedPath, "/")
	} else {
		u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(resolvedPath, "/")
	}

	q := u.Query()
	for key, value := range req.QueryParams {
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()

	httpReq, err := nethttp.NewRequestWithContext(ctx, req.Method, u.String(), nil)
	if err != nil {
		return core.FetchResult{}, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	attempts := c.cfg.Retries.ErrorsMaxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		if c.rateLimit != nil {
			c.rateLimit.Wait()
		}

		resp, err := c.client.Do(httpReq)
		if err != nil {
			c.logger.LogRequest(observability.RequestEvent{
				Timestamp:  time.Now(),
				Method:     req.Method,
				URL:        u.String(),
				Params:     combineRequestParams(req),
				StatusCode: -1,
				Err:        err,
			})
			return core.FetchResult{}, fmt.Errorf("send request: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		c.logger.LogRequest(observability.RequestEvent{
			Timestamp:  time.Now(),
			Method:     req.Method,
			URL:        u.String(),
			Params:     combineRequestParams(req),
			StatusCode: resp.StatusCode,
		})
		if readErr != nil {
			return core.FetchResult{}, fmt.Errorf("read response body: %w", readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return core.FetchResult{
				Payload:    body,
				StatusCode: resp.StatusCode,
				FinalURL:   u.String(),
			}, nil
		}

		shouldRetry := false
		retryDelay := c.cfg.Retries.BasicRetryTimeout
		if resp.StatusCode == nethttp.StatusTooManyRequests {
			shouldRetry = attempt < c.cfg.Retries.ErrorsMaxRetries
			retryDelay = c.cfg.Retries.BasicRetryTimeout * time.Duration(1<<attempt)
		} else if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			shouldRetry = attempt < c.cfg.Retries.ErrorsMaxRetries
		}

		if !shouldRetry {
			return core.FetchResult{}, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
		}
		if err := sleepWithContext(ctx, retryDelay); err != nil {
			return core.FetchResult{}, err
		}
	}

	return core.FetchResult{}, fmt.Errorf("request failed after retries")
}

func combineRequestParams(req core.FetchRequest) map[string]string {
	result := make(map[string]string, len(req.PathParams)+len(req.QueryParams)+len(req.Headers))
	for k, v := range req.PathParams {
		result[k] = v
	}
	for k, v := range req.QueryParams {
		if req.SensitiveQuery[k] {
			result[k] = "***"
			continue
		}
		result[k] = v
	}
	for k, v := range req.Headers {
		if req.SensitiveHeaders[k] {
			result[k] = "***"
			continue
		}
		result[k] = v
	}
	return result
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
