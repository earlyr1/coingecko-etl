package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check reports whether one dependency is reachable. It must honour ctx.
type Check func(ctx context.Context) error

type namedCheck struct {
	name  string
	check Check
}

// Health holds the readiness checks served on /readyz. /healthz is always 200.
type Health struct {
	mu           sync.RWMutex
	checks       []namedCheck
	readyTimeout time.Duration
}

func NewHealth() *Health {
	return &Health{readyTimeout: 2 * time.Second}
}

// AddReadiness registers a named dependency check. Safe to call while serving.
func (h *Health) AddReadiness(name string, c Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks = append(h.checks, namedCheck{name: name, check: c})
}

type checkResult struct {
	name string
	err  error
}

// ready runs all checks concurrently under one timeout and returns name -> error
// text for each failed (or unfinished) check. Empty map means ready.
func (h *Health) ready(ctx context.Context) map[string]string {
	h.mu.RLock()
	checks := append([]namedCheck(nil), h.checks...)
	h.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, h.readyTimeout)
	defer cancel()
	results := make(chan checkResult, len(checks)) // buffered: late checks never block
	for _, c := range checks {
		go func() { results <- checkResult{name: c.name, err: c.check(ctx)} }()
	}
	failures := map[string]string{}
	pending := map[string]bool{}
	for _, c := range checks {
		pending[c.name] = true
	}
	for range checks {
		select {
		case r := <-results:
			delete(pending, r.name)
			if r.err != nil {
				failures[r.name] = r.err.Error()
			}
		case <-ctx.Done():
			for name := range pending {
				failures[name] = "readiness check timed out"
			}
			return failures
		}
	}
	return failures
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newMux(h *Health, g prometheus.Gatherer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if failures := h.ready(r.Context()); len(failures) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "failures": failures})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	return mux
}

// Serve listens on addr and serves /healthz, /readyz and /metrics until ctx is
// cancelled, then shuts down gracefully and returns nil. A listen or serve
// failure is returned immediately.
func Serve(ctx context.Context, addr string, h *Health, g prometheus.Gatherer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("obs: listen %s: %w", addr, err)
	}
	return serve(ctx, ln, h, g)
}

func serve(ctx context.Context, ln net.Listener, h *Health, g prometheus.Gatherer) error {
	srv := &http.Server{Handler: newMux(h, g), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("obs: serve: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("obs: shutdown: %w", err)
		}
		if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("obs: serve: %w", err)
		}
		return nil
	}
}
