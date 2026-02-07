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

	"github.com/getkin/kin-openapi/openapi3"
)

type Fetcher struct {
	client *http.Client
}

func New() *Fetcher {
	// Create HTTP client with custom transport for testing
	// Note: In production, you should use proper SSL verification
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return &Fetcher{
		client: &http.Client{
			Transport: tr,
		},
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
