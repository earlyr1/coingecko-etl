package fetcher

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeCoinGecko serves recorded responses. The first /coins/markets call
// gets a 429 with Retry-After: 1, like the real API does under load.
func fakeCoinGecko(t *testing.T) *httptest.Server {
	t.Helper()
	markets := readFixture(t, "markets_top10_t0.json")
	bitcoin := readFixture(t, "coin_bitcoin_full_t0.json")
	usdc := readFixture(t, "coin_usd-coin_meta.json")
	var marketsCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/coins/markets", func(w http.ResponseWriter, r *http.Request) {
		if marketsCalls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"status":{"error_code":429,"error_message":"You've exceeded the Rate Limit."}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cf-Cache-Status", "HIT")
		w.Header().Set("Age", "12")
		_, _ = w.Write(markets)
	})
	mux.HandleFunc("GET /api/v3/coins/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch r.PathValue("id") {
		case "bitcoin":
			_, _ = w.Write(bitcoin)
		case "usd-coin":
			_, _ = w.Write(usdc)
		default:
			http.Error(w, `{"error":"coin not found"}`, http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFetcherIntegration(t *testing.T) {
	testutil.SkipIfNoDocker(t)
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

	srv := fakeCoinGecko(t)
	cfg := config.Config{
		MarketsInterval:   300 * time.Millisecond,
		DetailInterval:    400 * time.Millisecond,
		DetailOffset:      150 * time.Millisecond,
		Watchlist:         []string{"bitcoin", "usd-coin"},
		MarketsPerPage:    10,
		MinRequestSpacing: 50 * time.Millisecond,
	}
	lim := coingecko.NewLimiter(cfg.MinRequestSpacing)
	cg := coingecko.NewClient(srv.URL+"/api/v3", "", srv.Client(), lim, time.Now)
	m := obs.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Run(runCtx, cfg, cg, lim, pool, m, log) }()

	count := func(where string) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM raw.api_calls WHERE "+where).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if count("endpoint = 'coins_markets' AND outcome = 'ok'") >= 2 &&
			count("request->>'path' = '/coins/bitcoin' AND outcome = 'ok'") >= 1 &&
			count("request->>'path' = '/coins/usd-coin' AND outcome = 'ok'") >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}

	if n := count("endpoint = 'coins_markets' AND outcome = 'ok'"); n < 2 {
		t.Errorf("markets ok rows = %d, want >= 2", n)
	}
	if n := count("endpoint = 'coin_detail' AND outcome = 'ok' AND request->>'path' IN ('/coins/bitcoin','/coins/usd-coin')"); n < 2 {
		t.Errorf("detail ok rows = %d, want >= 2 (bitcoin and usd-coin)", n)
	}
	if n := count("outcome = 'http_error' AND (http->>'status')::int = 429 AND payload IS NULL AND idempotency_key IS NULL AND (http->>'retry_after_s')::int = 1"); n != 1 {
		t.Errorf("429 http_error rows = %d, want 1", n)
	}
	// Byte-exact payload: the hash of the stored json text equals payload_sha256.
	if n := count("outcome = 'ok' AND sha256(convert_to(payload::text, 'UTF8')) <> payload_sha256"); n != 0 {
		t.Errorf("%d rows whose payload bytes do not match payload_sha256", n)
	}
	// Identical markets responses share one idempotency key (raw keeps duplicates).
	if n := count("endpoint = 'coins_markets' AND outcome = 'ok' AND idempotency_key IS DISTINCT FROM (SELECT idempotency_key FROM raw.api_calls WHERE endpoint = 'coins_markets' AND outcome = 'ok' LIMIT 1)"); n != 0 {
		t.Errorf("identical markets bodies produced %d different idempotency keys", n)
	}
	if got := promtest.ToFloat64(m.APIRateLimited); got != 1 {
		t.Errorf("api_rate_limited_total = %v, want 1", got)
	}
	// The 1 s cooldown after the 429 must have skipped at least one tick.
	if got := promtest.ToFloat64(m.APIRequests.WithLabelValues("coins_markets", "skipped")) +
		promtest.ToFloat64(m.APIRequests.WithLabelValues("coin_detail", "skipped")); got < 1 {
		t.Errorf("skipped ticks = %v, want >= 1 during the cooldown", got)
	}
	if got := promtest.ToFloat64(m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "fail")); got != 0 {
		t.Errorf(`schema_validations_total{result="fail"} = %v, want 0`, got)
	}
}
