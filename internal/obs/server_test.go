package obs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestHealthzAlwaysOK(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return errors.New("down") })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/healthz")
	if code != http.StatusOK || !strings.Contains(body, `"ok"`) {
		t.Fatalf("/healthz = %d %s", code, body)
	}
}

func TestReadyzAllPass(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return nil })
	h.AddReadiness("s3", func(context.Context) error { return nil })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d %s, want 200", code, body)
	}
}

func TestReadyzReportsFailures(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return nil })
	h.AddReadiness("broker", func(context.Context) error { return errors.New("dial tcp: connection refused") })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", code)
	}
	var resp struct {
		Status   string            `json:"status"`
		Failures map[string]string `json:"failures"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, body)
	}
	want := map[string]string{"broker": "dial tcp: connection refused"}
	if resp.Status != "not_ready" || len(resp.Failures) != 1 || resp.Failures["broker"] != want["broker"] {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestReadyzTimesOutHungCheck(t *testing.T) {
	h := NewHealth()
	h.readyTimeout = 50 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	h.AddReadiness("stuck", func(context.Context) error { <-release; return nil }) // ignores ctx on purpose
	start := time.Now()
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "timed out") {
		t.Fatalf("/readyz = %d %s, want 503 timed out", code, body)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("/readyz took %v, timeout not enforced", time.Since(start))
	}
}

func TestMetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.APIRateLimited.Add(3)
	code, body := get(t, newMux(NewHealth(), reg), "/metrics")
	if code != http.StatusOK || !strings.Contains(body, "etl_api_rate_limited_total 3") {
		t.Fatalf("/metrics = %d\n%s", code, body)
	}
}

func TestServeStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, NewHealth(), prometheus.NewRegistry()) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
}

func TestServeReportsListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	err = Serve(context.Background(), ln.Addr().String(), NewHealth(), prometheus.NewRegistry())
	if err == nil {
		t.Fatal("Serve on a busy port returned nil")
	}
}
