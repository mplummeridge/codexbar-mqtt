package codexbar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPClient struct {
	baseURL string
	client  *http.Client
}

type HTTPResponse struct {
	Payload     json.RawMessage
	StatusCode  int
	ContentType string
	StartedAt   time.Time
	FinishedAt  time.Time
	Duration    time.Duration
}

type HTTPStatusError struct {
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("CodexBar HTTP status %d: %s", e.StatusCode, e.Body)
}

func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	return &HTTPClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

func (c *HTTPClient) Fetch(ctx context.Context, path string, query map[string]string) (HTTPResponse, error) {
	u, err := url.Parse(c.baseURL + path)
	if err != nil {
		return HTTPResponse{}, err
	}
	values := u.Query()
	for key, value := range query {
		if value != "" {
			values.Set(key, value)
		}
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return HTTPResponse{}, err
	}
	req.Header.Set("Accept", "application/json")
	started := time.Now().UTC()
	resp, err := c.client.Do(req)
	finished := time.Now().UTC()
	duration := finished.Sub(started)
	if err != nil {
		return HTTPResponse{StartedAt: started, FinishedAt: finished, Duration: duration}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024+1))
	if err != nil {
		return HTTPResponse{StatusCode: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), StartedAt: started, FinishedAt: finished, Duration: duration}, err
	}
	if len(body) > 64*1024*1024 {
		return HTTPResponse{StatusCode: resp.StatusCode, StartedAt: started, FinishedAt: finished, Duration: duration}, fmt.Errorf("CodexBar response exceeds 64 MiB")
	}
	result := HTTPResponse{
		Payload:     append(json.RawMessage(nil), body...),
		StatusCode:  resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		StartedAt:   started,
		FinishedAt:  finished,
		Duration:    duration,
	}
	if !json.Valid(body) {
		return result, fmt.Errorf("CodexBar returned invalid JSON")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		excerpt := strings.TrimSpace(string(body))
		if len(excerpt) > 1024 {
			excerpt = excerpt[:1024]
		}
		return result, &HTTPStatusError{StatusCode: resp.StatusCode, Body: excerpt}
	}
	return result, nil
}

func (c *HTTPClient) Healthy(ctx context.Context) bool {
	resp, err := c.Fetch(ctx, "/health", nil)
	return err == nil && resp.StatusCode == http.StatusOK
}
