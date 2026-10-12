package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRunDispatch(t *testing.T) {
	cases := []struct {
		args       []string
		wantCode   int
		wantStderr string
	}{
		{nil, 2, "usage: etl"},
		{[]string{"bogus"}, 2, `unknown command "bogus"`},
		{[]string{"help"}, 0, ""},
		{[]string{"publisher"}, 2, `unknown command "publisher"`},
		{[]string{"publish", "--now", "bogus"}, 2, "--now"},
		{[]string{"publish", "extra"}, 2, `unexpected argument "extra"`},
		{[]string{"maintenance"}, 2, "usage: etl"},
		{[]string{"maintenance", "bogus"}, 2, `unknown maintenance job "bogus"`},
		{[]string{"maintenance", "vacuum", "extra"}, 2, "usage: etl"},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(c.args, &stdout, &stderr); code != c.wantCode {
				t.Fatalf("exit code %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

func TestServiceRejectsBadConfig(t *testing.T) {
	t.Setenv("ETL_LOG_DIR", t.TempDir())
	commands := [][]string{{"init"}, {"fetcher"}, {"exporter"}, {"publish"}, {"transformer"},
		{"maintenance", "retention"}, {"maintenance", "vacuum"}}
	for _, bad := range []struct{ name, value string }{
		{"ETL_TOPIC_PARTITIONS", "six"},
		{"ETL_CLAIMS_RETENTION_DAYS", "3"}, // below the 168h topic retention in days + 1
	} {
		t.Run(bad.name, func(t *testing.T) {
			t.Setenv(bad.name, bad.value)
			for _, args := range commands {
				var stderr bytes.Buffer
				if code := run(args, &bytes.Buffer{}, &stderr); code != 1 {
					t.Fatalf("%v: exit %d, want 1", args, code)
				}
				if !strings.Contains(stderr.String(), bad.name) {
					t.Fatalf("%v: stderr %q does not name the bad variable", args, stderr.String())
				}
			}
		})
	}
}

// closedAddr returns a loopback address on which nothing listens.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestServiceExitsWhenPostgresUnreachable(t *testing.T) {
	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_PG_DSN", "postgres://etl:etl@"+closedAddr(t)+"/etl?sslmode=disable&connect_timeout=1")
	t.Setenv("ETL_HTTP_ADDR", closedAddr(t))
	for _, svc := range []string{"fetcher", "exporter", "transformer"} {
		if code := run([]string{svc}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
			t.Fatalf("%s: exit %d, want 1", svc, code)
		}
		logged, err := os.ReadFile(filepath.Join(logDir, svc, "etl.log"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(logged), `"msg":"service failed"`) {
			t.Fatalf("%s: log does not record the failure:\n%s", svc, logged)
		}
	}
}

func TestReadyzURL(t *testing.T) {
	cases := map[string]string{
		"":               "http://127.0.0.1:8080/readyz",
		":8080":          "http://127.0.0.1:8080/readyz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/readyz",
		"[::]:8080":      "http://127.0.0.1:8080/readyz",
		"10.0.0.5:8080":  "http://10.0.0.5:8080/readyz",
		"localhost:8081": "http://localhost:8081/readyz",
	}
	for in, want := range cases {
		got, err := readyzURL(in)
		if err != nil || got != want {
			t.Errorf("readyzURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := readyzURL("8080"); err == nil {
		t.Error(`readyzURL("8080") succeeded, want error`)
	}
}

func TestHealthcheckCommand(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	t.Setenv("ETL_HTTP_ADDR", srv.Listener.Addr().String())

	var stderr bytes.Buffer
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 0 {
		t.Fatalf("ready server: exit %d (%s), want 0", code, stderr.String())
	}
	status = http.StatusServiceUnavailable
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("not-ready server: exit %d, want 1", code)
	}
}

func TestHealthcheckUnreachable(t *testing.T) {
	t.Setenv("ETL_HTTP_ADDR", closedAddr(t))
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
}

func TestLogDirPerHost(t *testing.T) {
	t.Setenv("ETL_LOG_PER_HOST", "")
	if got := logDir("logs"); got != "logs" {
		t.Fatalf("logDir without ETL_LOG_PER_HOST = %q, want logs", got)
	}
	t.Setenv("ETL_LOG_PER_HOST", "true")
	host, err := os.Hostname()
	if err != nil {
		t.Skip("no hostname")
	}
	if got, want := logDir("logs"), filepath.Join("logs", host); got != want {
		t.Fatalf("logDir = %q, want %q", got, want)
	}
}

// pushRecorder is a fake Pushgateway that records "METHOD PATH\nBODY" per push.
type pushRecorder struct {
	mu     sync.Mutex
	pushes []string
}

func newPushRecorder(t *testing.T) (*pushRecorder, string) {
	t.Helper()
	rec := &pushRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.pushes = append(rec.pushes, r.Method+" "+r.URL.Path+"\n"+string(body))
		rec.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return rec, srv.URL
}

func (r *pushRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.pushes...)
}

func TestMaintenanceFailsWhenPostgresUnreachable(t *testing.T) {
	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_LOG_PER_HOST", "")
	t.Setenv("ETL_PG_DSN", "postgres://etl:etl@"+closedAddr(t)+"/etl?sslmode=disable&connect_timeout=1")
	rec, url := newPushRecorder(t)
	t.Setenv("ETL_PUSHGATEWAY_URL", url)

	if code := run([]string{"maintenance", "retention"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	logged, err := os.ReadFile(filepath.Join(logDir, "maintenance", "etl.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"msg":"job failed"`, `"job":"etl_retention"`} {
		if !strings.Contains(string(logged), want) {
			t.Errorf("log lacks %s:\n%s", want, logged)
		}
	}
	got := rec.all()
	if len(got) != 1 {
		t.Fatalf("got %d pushes, want 1: %q", len(got), got)
	}
	if !strings.HasPrefix(got[0], "POST /metrics/job/etl_retention\n") ||
		!strings.Contains(got[0], "\netl_job_success 0\n") ||
		strings.Contains(got[0], "etl_job_last_success_timestamp_seconds") {
		t.Fatalf("push = %q; want a POST with etl_job_success 0 and no last-success timestamp", got[0])
	}
}
