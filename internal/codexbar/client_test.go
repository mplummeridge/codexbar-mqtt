package codexbar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientFetch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/usage" || r.URL.Query().Get("provider") != "both" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"provider":"codex"}]`))
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL, time.Second)
	resp, err := client.Fetch(context.Background(), "/usage", map[string]string{"provider": "both"})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != `[{"provider":"codex"}]` || resp.Duration < 0 || resp.StartedAt.IsZero() || resp.FinishedAt.IsZero() {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHTTPClientPreservesJSONErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad provider"}`))
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL, time.Second)
	resp, err := client.Fetch(context.Background(), "/usage", nil)
	if err == nil {
		t.Fatal("expected HTTP status error")
	}
	if string(resp.Payload) != `{"error":"bad provider"}` {
		t.Fatalf("unexpected payload %s", resp.Payload)
	}
}
