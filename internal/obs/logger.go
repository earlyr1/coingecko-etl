// Package obs holds the observability plumbing shared by every etl service:
// structured logging, Prometheus metrics and the /healthz /readyz /metrics server.
package obs

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"coingecko-etl/internal/xtime"
)

// NewLogger returns a JSON slog logger that writes every record both to
// <logDir>/<service>/etl.log (appended, created if missing) and to stdout.
// Each record carries service=<service> and a UTC millisecond timestamp.
// The returned Closer closes the log file; call it on shutdown.
func NewLogger(service, logDir string) (*slog.Logger, io.Closer, error) {
	if service == "" || strings.ContainsAny(service, `/\`) || service == "." || service == ".." {
		return nil, nil, fmt.Errorf("obs: invalid service name %q", service)
	}
	dir := filepath.Join(logDir, service)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("obs: create log dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "etl.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("obs: open log file: %w", err)
	}
	h := slog.NewJSONHandler(io.MultiWriter(f, os.Stdout), &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// slog writes local time with nanoseconds by default; force the pipeline format.
			if len(groups) == 0 && a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
				return slog.String(slog.TimeKey, xtime.Format(a.Value.Time()))
			}
			return a
		},
	})
	return slog.New(h).With(slog.String("service", service)), f, nil
}
