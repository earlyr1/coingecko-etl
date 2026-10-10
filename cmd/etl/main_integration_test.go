package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"
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
