package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseNow(t *testing.T) {
	want := time.Date(2026, 10, 12, 14, 6, 0, 0, time.UTC)
	for _, in := range []string{"1791813960", "2026-10-12T14:06:00Z"} {
		got, err := parseNow(in)
		if err != nil || !got.Equal(want) || got.Location() != time.UTC {
			t.Errorf("parseNow(%q) = %v (%v), %v; want %v in UTC", in, got, got.Location(), err, want)
		}
	}
	got, err := parseNow("2026-10-12T17:06:00.5+03:00")
	if err != nil || !got.Equal(want.Add(500*time.Millisecond)) || got.Location() != time.UTC {
		t.Errorf("parseNow with an offset = %v, %v; want %v in UTC", got, err, want.Add(500*time.Millisecond))
	}
	for _, in := range []string{"", "abc", "1791813960.5", "-5", "+5", "2026-10-12 14:06", "99999999999999999999"} {
		if _, err := parseNow(in); err == nil || !strings.Contains(err.Error(), "--now") {
			t.Errorf("parseNow(%q) error = %v; want one mentioning --now", in, err)
		}
	}
}

// closedURL returns an http URL on a loopback port where nothing listens.
func closedURL(t *testing.T) string {
	t.Helper()
	return "http://" + closedAddr(t)
}

func TestPublishExitsWhenS3Unreachable(t *testing.T) {
	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_S3_ENDPOINT", closedURL(t))
	t.Setenv("ETL_BROKERS", closedAddr(t))
	t.Setenv("ETL_PUSHGATEWAY_URL", closedURL(t))
	t.Setenv("ETL_PG_DSN", "not a dsn") // the publish job never opens Postgres
	t.Setenv("ETL_WINDOW", "5m")

	var stderr bytes.Buffer
	if code := run([]string{"publish", "--now", "1791813960"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("exit %d, want 1 (stderr %q)", code, stderr.String())
	}
	logged, err := os.ReadFile(filepath.Join(logDir, "publisher", "etl.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"msg":"publish started"`, `"msg":"job failed"`, `"job":"etl_publisher"`,
		`"window_start":"2026-10-12T14:00:00.000Z"`, `"msg":"pushgateway push failed"`} {
		if !strings.Contains(string(logged), want) {
			t.Errorf("log lacks %s:\n%s", want, logged)
		}
	}
}
