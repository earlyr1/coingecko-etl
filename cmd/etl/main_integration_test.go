package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/window"
)

// TestFetcherServesAndStopsOnSIGTERM runs `etl fetcher` in-process against a
// real Postgres and a fake CoinGecko, checks the HTTP endpoints, then sends
// SIGTERM to the test process and expects a clean exit 0.
func TestFetcherServesAndStopsOnSIGTERM(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	markets, err := os.ReadFile("../../schemas/testdata/coingecko/markets_top10_t0.json")
	if err != nil {
		t.Fatal(err)
	}
	fakeCG := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(markets)
	}))
	defer fakeCG.Close()

	addr := closedAddr(t) // a free port for the service's HTTP server
	t.Setenv("ETL_LOG_DIR", t.TempDir())
	t.Setenv("ETL_PG_DSN", dsn)
	t.Setenv("ETL_HTTP_ADDR", addr)
	t.Setenv("ETL_COINGECKO_BASE_URL", fakeCG.URL)
	t.Setenv("ETL_MIN_REQUEST_SPACING", "0s")
	t.Setenv("ETL_WATCHLIST", "bitcoin")

	exit := make(chan int, 1)
	go func() { exit <- run([]string{"fetcher"}, &bytes.Buffer{}, &bytes.Buffer{}) }()

	// /readyz turns 200 once the server is up and Postgres answers. The signal
	// handler is installed before the server starts, so after this point a
	// SIGTERM is caught by signal.NotifyContext instead of killing the test.
	waitFor(t, 30*time.Second, func() bool { return get(t, "http://"+addr+"/readyz") == http.StatusOK })
	if code := get(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d", code)
	}

	// The first markets poll happens immediately; wait until it is stored and counted.
	waitFor(t, 30*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM raw.api_calls WHERE endpoint = 'coins_markets'`).Scan(&n)
		return n > 0
	})
	resp, err := noKeepAlive.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`etl_api_requests_total{endpoint="coins_markets",status="200"}`, "go_goroutines"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("fetcher exit code %d after SIGTERM, want 0", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("fetcher did not stop within 15s of SIGTERM")
	}
}

// noKeepAlive closes every connection after its response. Pooled idle
// connections would otherwise make http.Server.Shutdown wait up to 5 s.
var noKeepAlive = &http.Client{
	Timeout:   2 * time.Second,
	Transport: &http.Transport{DisableKeepAlives: true},
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := noKeepAlive.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitFor(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", d)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestPublishCommand runs `etl publish` in-process against versitygw and
// Redpanda with a fake Pushgateway: a window without _SUCCESS fails, the same
// window succeeds once the exporter closed it, and a dead Pushgateway does not
// change the exit code.
func TestPublishCommand(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, _ := testutil.StartVersityGW(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var mu sync.Mutex
	var pushes []string
	pgw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		pushes = append(pushes, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer pgw.Close()
	pushed := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), pushes...)
	}

	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_LOG_PER_HOST", "")
	t.Setenv("ETL_S3_ENDPOINT", endpoint)
	t.Setenv("ETL_S3_ACCESS_KEY", testutil.VersityGWAccessKey)
	t.Setenv("ETL_S3_SECRET_KEY", testutil.VersityGWSecretKey)
	t.Setenv("ETL_BROKERS", strings.Join(brokers, ","))
	t.Setenv("ETL_PUSHGATEWAY_URL", pgw.URL)
	t.Setenv("ETL_WINDOW", "5m")
	readLog := func() string {
		b, err := os.ReadFile(filepath.Join(logDir, "publisher", "etl.log"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	publish := func() int {
		var stderr bytes.Buffer
		code := run([]string{"publish", "--now", "1791813960"}, &bytes.Buffer{}, &stderr)
		if stderr.Len() > 0 {
			t.Logf("stderr: %s", stderr.String())
		}
		return code
	}
	const push = "POST /metrics/job/etl_publisher"

	// 1. The exporter has not closed [14:00, 14:05) yet.
	if code := publish(); code != 1 {
		t.Fatalf("without _SUCCESS: exit %d, want 1", code)
	}
	if l := readLog(); !strings.Contains(l, `"msg":"window not ready"`) || !strings.Contains(l, "window=1400/_SUCCESS") {
		t.Fatalf("log does not report the missing _SUCCESS:\n%s", l)
	}
	if got := pushed(); len(got) != 1 || got[0] != push {
		t.Fatalf("pushes = %q, want [%s]", got, push)
	}

	// 2. The exporter closed it, empty.
	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: testutil.VersityGWAccessKey, SecretKey: testutil.VersityGWSecretKey, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	w := window.ForPublish(time.Unix(1791813960, 0), 5*time.Minute)
	body, err := window.NewSuccess(w, nil).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutIfAbsent(ctx, s3.BucketRaw, window.RawSuccessKey(w), body); err != nil {
		t.Fatal(err)
	}
	if code := publish(); code != 0 {
		t.Fatalf("with _SUCCESS: exit %d, want 0\n%s", code, readLog())
	}
	if l := readLog(); !strings.Contains(l, `"msg":"window published"`) {
		t.Fatalf("log lacks window published:\n%s", l)
	}
	if got := pushed(); len(got) != 2 || got[1] != push {
		t.Fatalf("pushes = %q, want two %s", got, push)
	}

	// 3. A dead Pushgateway is logged and does not fail the job.
	t.Setenv("ETL_PUSHGATEWAY_URL", "http://"+closedAddr(t))
	if code := publish(); code != 0 {
		t.Fatalf("with the Pushgateway down: exit %d, want 0", code)
	}
	if l := readLog(); !strings.Contains(l, `"level":"ERROR","msg":"pushgateway push failed"`) {
		t.Fatalf("log lacks the push failure:\n%s", l)
	}
}

// TestMaintenanceJobs runs `etl maintenance retention` and `etl maintenance
// vacuum` in-process against a migrated Postgres with a fake Pushgateway.
func TestMaintenanceJobs(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Two expired claims partitions (and their raw twins, which stay).
	if err := pg.EnsurePartitions(ctx, pool, time.Now().AddDate(0, 0, -30), 0); err != nil {
		t.Fatal(err)
	}

	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_LOG_PER_HOST", "")
	t.Setenv("ETL_PG_DSN", dsn)
	rec, url := newPushRecorder(t)
	t.Setenv("ETL_PUSHGATEWAY_URL", url)
	readLog := func() string {
		b, err := os.ReadFile(filepath.Join(logDir, "maintenance", "etl.log"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	maint := func(job string) int {
		var stderr bytes.Buffer
		code := run([]string{"maintenance", job}, &bytes.Buffer{}, &stderr)
		if stderr.Len() > 0 {
			t.Logf("stderr: %s", stderr.String())
		}
		return code
	}

	if code := maint("retention"); code != 0 {
		t.Fatalf("retention: exit %d, want 0\n%s", code, readLog())
	}
	if code := maint("vacuum"); code != 0 {
		t.Fatalf("vacuum: exit %d, want 0\n%s", code, readLog())
	}
	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("got %d pushes, want 2: %q", len(got), got)
	}
	for _, want := range []string{"POST /metrics/job/etl_retention\n", "\netl_job_success 1\n",
		"\netl_retention_partitions_dropped 2\n", "\netl_retention_batches_deleted 0\n",
		"\netl_job_last_success_timestamp_seconds "} {
		if !strings.Contains(got[0], want) {
			t.Errorf("retention push lacks %q:\n%s", want, got[0])
		}
	}
	for _, want := range []string{"POST /metrics/job/etl_vacuum\n", "\netl_job_success 1\n"} {
		if !strings.Contains(got[1], want) {
			t.Errorf("vacuum push lacks %q:\n%s", want, got[1])
		}
	}
	if strings.Contains(got[1], "etl_retention_") {
		t.Errorf("vacuum push carries retention gauges:\n%s", got[1])
	}
	logged := readLog()
	for _, job := range []string{"etl_retention", "etl_vacuum"} {
		if !strings.Contains(logged, `"msg":"job finished","service":"maintenance","job":"`+job+`"`) {
			t.Errorf("log lacks job finished for %s:\n%s", job, logged)
		}
	}

	// A dead Pushgateway is logged and does not fail the job.
	t.Setenv("ETL_PUSHGATEWAY_URL", "http://"+closedAddr(t))
	if code := maint("vacuum"); code != 0 {
		t.Fatalf("vacuum with the Pushgateway down: exit %d, want 0", code)
	}
	var failed []string
	for line := range strings.SplitSeq(readLog(), "\n") {
		if strings.Contains(line, `"level":"ERROR","msg":"pushgateway push failed"`) {
			failed = append(failed, line)
		}
	}
	if len(failed) != 1 || strings.Count(failed[0], `"job":"etl_vacuum"`) != 1 {
		t.Fatalf("push failure records = %q, want one carrying job=etl_vacuum once", failed)
	}
}
