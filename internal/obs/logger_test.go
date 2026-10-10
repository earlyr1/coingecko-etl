package obs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLoggerWritesJSONToServiceFile(t *testing.T) {
	dir := t.TempDir()
	log, closer, err := NewLogger("fetcher", dir)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	log.Info("api call", "endpoint", "coins_markets", "status", 200)
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "fetcher", "etl.log"))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("want 1 log line, got %d: %q", len(lines), b)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v: %s", err, lines[0])
	}
	if rec["service"] != "fetcher" || rec["msg"] != "api call" || rec["endpoint"] != "coins_markets" || rec["level"] != "INFO" {
		t.Fatalf("unexpected record: %v", rec)
	}
	ts, _ := rec["time"].(string)
	// 2026-10-10T10:07:55.000Z: 24 chars, UTC "Z", millisecond precision.
	if len(ts) != 24 || !strings.HasSuffix(ts, "Z") || ts[19] != '.' {
		t.Fatalf("time %q is not UTC millisecond RFC 3339", ts)
	}
}

func TestNewLoggerAppends(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2; i++ {
		log, closer, err := NewLogger("exporter", dir)
		if err != nil {
			t.Fatalf("NewLogger: %v", err)
		}
		log.Info("run", "i", i)
		closer.Close()
	}
	b, _ := os.ReadFile(filepath.Join(dir, "exporter", "etl.log"))
	if n := strings.Count(string(b), "\n"); n != 2 {
		t.Fatalf("want 2 lines after reopening, got %d", n)
	}
}

func TestNewLoggerRejectsBadService(t *testing.T) {
	for _, s := range []string{"", "a/b", ".."} {
		if _, _, err := NewLogger(s, t.TempDir()); err == nil {
			t.Errorf("NewLogger(%q) succeeded, want error", s)
		}
	}
}
