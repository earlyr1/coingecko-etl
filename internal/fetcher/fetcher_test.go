package fetcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

// readFixture returns a recorded CoinGecko response from schemas/testdata.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schemas/testdata/coingecko/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

// fakeClock runs virtual time: After(d) advances the clock by d and fires
// immediately, so run() executes hours of schedule in microseconds. When
// the next wake-up would pass end, it cancels the run instead.
type fakeClock struct {
	t      time.Time
	end    time.Time
	cancel context.CancelFunc
}

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	if c.t.Add(d).After(c.end) {
		c.cancel()
		return ch // never fires; run() returns via ctx.Done()
	}
	c.t = c.t.Add(d)
	ch <- c.t
	return ch
}

type call struct {
	at       time.Duration // offset from t0
	endpoint string
	path     string
}

type harness struct {
	r          *runner
	clk        *fakeClock
	m          *obs.Metrics
	calls      []call
	inserted   []event.RawEvent
	partitions int
	// respond builds the event for a call; default: 200 ok.
	respond func(endpoint string, req event.Request, now time.Time) event.RawEvent
	// cooldownUntil emulates the limiter: allowed is false before it.
	cooldownUntil time.Time
	// latency is added to the clock by every fetch.
	latency func(n int) time.Duration
}

func newHarness(t *testing.T, cfg config.Config, horizon time.Duration) (*harness, context.Context) {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &harness{clk: &fakeClock{t: t0, end: t0.Add(horizon), cancel: cancel}, m: obs.NewMetrics(prometheus.NewRegistry())}
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		if endpoint == event.EndpointMarkets {
			return okEvent(endpoint, req, now, []byte(`[{"id":"a","last_updated":"2026-10-10T09:59:30.000Z"},{"id":"b","last_updated":"2026-10-10T07:55:52.000Z"}]`))
		}
		return okEvent(endpoint, req, now, []byte(`{"id":"x","last_updated":"2026-10-10T09:59:30.000Z"}`))
	}
	h.r = &runner{
		cfg: cfg,
		clk: h.clk,
		fetch: func(ctx context.Context, endpoint string, req event.Request) event.RawEvent {
			now := h.clk.t
			h.calls = append(h.calls, call{at: now.Sub(t0), endpoint: endpoint, path: req.Path})
			if h.latency != nil {
				h.clk.t = h.clk.t.Add(h.latency(len(h.calls)))
			}
			return h.respond(endpoint, req, now)
		},
		allowed: func(now time.Time) bool { return !now.Before(h.cooldownUntil) },
		insert: func(ctx context.Context, ev event.RawEvent) error {
			h.inserted = append(h.inserted, ev)
			return nil
		},
		ensurePartitions: func(ctx context.Context, now time.Time) error { h.partitions++; return nil },
		reg:              reg,
		m:                h.m,
		log:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	return h, ctx
}

func okEvent(endpoint string, req event.Request, now time.Time, payload []byte) event.RawEvent {
	sha := event.SHA256Hex(payload)
	key := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	return event.RawEvent{
		SchemaVersion: event.SchemaVersion, EventID: uuid.Must(uuid.NewV7()).String(), IdempotencyKey: &key,
		Source: event.SourceCoinGecko, Endpoint: endpoint, Request: req, FetchedAt: xtime.Format(now),
		Outcome: event.OutcomeOK, HTTP: &event.HTTPMeta{Status: 200, LatencyMS: 120},
		PayloadSHA256: &sha, Payload: json.RawMessage(payload),
	}
}

func http429Event(endpoint string, req event.Request, now time.Time) event.RawEvent {
	msg := "HTTP 429: rate limited"
	ra := 60
	return event.RawEvent{
		SchemaVersion: event.SchemaVersion, EventID: uuid.Must(uuid.NewV7()).String(),
		Source: event.SourceCoinGecko, Endpoint: endpoint, Request: req, FetchedAt: xtime.Format(now),
		Outcome: event.OutcomeHTTPError, HTTP: &event.HTTPMeta{Status: 429, LatencyMS: 80, RetryAfterS: &ra},
		Error: &msg,
	}
}

func cfg(watchlist ...string) config.Config {
	return config.Config{
		MarketsInterval: 30 * time.Second, DetailInterval: 60 * time.Second, DetailOffset: 15 * time.Second,
		Watchlist: watchlist, MarketsPerPage: 100,
	}
}

func skipped(m *obs.Metrics, endpoint string) float64 {
	return promtest.ToFloat64(m.APIRequests.WithLabelValues(endpoint, "skipped"))
}

func TestScheduleAndRotation(t *testing.T) {
	h, ctx := newHarness(t, cfg("bitcoin", "ethereum", "tether"), 3*time.Minute)
	if err := h.r.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	m, d := event.EndpointMarkets, event.EndpointCoinDetail
	want := []call{
		{0, m, "/coins/markets"},
		{15 * time.Second, d, "/coins/bitcoin"},
		{30 * time.Second, m, "/coins/markets"},
		{60 * time.Second, m, "/coins/markets"},
		{75 * time.Second, d, "/coins/ethereum"},
		{90 * time.Second, m, "/coins/markets"},
		{120 * time.Second, m, "/coins/markets"},
		{135 * time.Second, d, "/coins/tether"},
		{150 * time.Second, m, "/coins/markets"},
		{180 * time.Second, m, "/coins/markets"},
	}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if len(h.inserted) != len(want) {
		t.Errorf("inserted %d rows, want %d", len(h.inserted), len(want))
	}
	if h.partitions != 1 {
		t.Errorf("EnsurePartitions called %d times in 3 min, want 1 (startup)", h.partitions)
	}
	if got := promtest.ToFloat64(h.m.APIRequests.WithLabelValues(m, "200")); got != 7 {
		t.Errorf("api_requests_total{coins_markets,200} = %v, want 7", got)
	}
	if got := promtest.ToFloat64(h.m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "pass")); got != 10 {
		t.Errorf(`schema_validations_total{result="pass"} = %v, want 10`, got)
	}
	if skipped(h.m, m)+skipped(h.m, d) != 0 {
		t.Error("ticks skipped without cooldown or slow requests")
	}
	// Source data age: fetched_at (10:03:00) - last_updated (09:59:30) = 210 s.
	if got := promtest.ToFloat64(h.m.SourceDataAge.WithLabelValues(m)); got != 210 {
		t.Errorf("source_data_age_seconds{coins_markets} = %v, want 210", got)
	}
	if got := promtest.ToFloat64(h.m.LastSuccess.WithLabelValues(m)); got != float64(t0.Add(3*time.Minute).Unix()) {
		t.Errorf("last_success = %v", got)
	}
}

func TestPartitionsHourly(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 2*time.Hour+time.Minute)
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	if h.partitions != 3 { // start, +1h, +2h
		t.Fatalf("EnsurePartitions calls = %d, want 3", h.partitions)
	}
}

func TestSkipsTicksDuringCooldown(t *testing.T) {
	h, ctx := newHarness(t, cfg("bitcoin", "ethereum"), 2*time.Minute)
	first := true
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		if first {
			first = false
			h.cooldownUntil = now.Add(60 * time.Second) // what Client.Fetch does on 429
			return http429Event(endpoint, req, now)
		}
		return okEvent(endpoint, req, now, []byte(`[]`))
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	m, d := event.EndpointMarkets, event.EndpointCoinDetail
	want := []call{
		{0, m, "/coins/markets"},                // 429 -> cooldown until 60 s
		{60 * time.Second, m, "/coins/markets"}, // 15 s detail and 30 s markets skipped
		{75 * time.Second, d, "/coins/bitcoin"}, // rotation did not advance on the skip
		{90 * time.Second, m, "/coins/markets"},
		{120 * time.Second, m, "/coins/markets"},
	}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if skipped(h.m, m) != 1 || skipped(h.m, d) != 1 {
		t.Errorf("skipped markets=%v detail=%v, want 1 and 1", skipped(h.m, m), skipped(h.m, d))
	}
	if got := promtest.ToFloat64(h.m.APIRateLimited); got != 1 {
		t.Errorf("api_rate_limited_total = %v, want 1", got)
	}
	if got := promtest.ToFloat64(h.m.APIRequests.WithLabelValues(m, "429")); got != 1 {
		t.Errorf("api_requests_total{coins_markets,429} = %v, want 1", got)
	}
	if len(h.inserted) != 5 || h.inserted[0].Outcome != event.OutcomeHTTPError {
		t.Fatalf("inserted %d rows, first outcome %q; the 429 must be stored as http_error", len(h.inserted), h.inserted[0].Outcome)
	}
}

func TestSkipsTicksThatCameDueWhileInFlight(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 2*time.Minute)
	h.latency = func(n int) time.Duration {
		if n == 1 {
			return 70 * time.Second // the first request hangs past two ticks
		}
		return 0
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	m := event.EndpointMarkets
	want := []call{{0, m, "/coins/markets"}, {90 * time.Second, m, "/coins/markets"}, {120 * time.Second, m, "/coins/markets"}}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if got := skipped(h.m, m); got != 2 {
		t.Errorf("skipped = %v, want 2 (ticks at 30 s and 60 s)", got)
	}
}

func TestInvalidEnvelopeIsNotInserted(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 0)
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		ev := okEvent(endpoint, req, now, []byte(`[]`))
		ev.EventID = "not-a-uuid" // a bug in our envelope code
		return ev
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 1 {
		t.Fatalf("calls = %d", len(h.calls))
	}
	if len(h.inserted) != 0 {
		t.Fatal("invalid envelope was inserted")
	}
	if got := promtest.ToFloat64(h.m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "fail")); got != 1 {
		t.Errorf(`schema_validations_total{result="fail"} = %v, want 1`, got)
	}
}

func TestRunRejectsZeroIntervals(t *testing.T) {
	h, ctx := newHarness(t, config.Config{}, time.Minute)
	if err := h.r.run(ctx); err == nil {
		t.Fatal("run accepted zero intervals")
	}
}

func TestSourceLastUpdated(t *testing.T) {
	cases := []struct {
		endpoint, file, want string
	}{
		// 248/250 rows share 10:07:55; the max wins over stale rows.
		{event.EndpointMarkets, "markets_usd250_t0.json", "2026-10-10T10:07:55.000Z"},
		{event.EndpointCoinDetail, "coin_bitcoin_full_t0.json", "2026-10-10T10:01:51.000Z"},
	}
	for _, tc := range cases {
		got, ok := sourceLastUpdated(tc.endpoint, readFixture(t, tc.file))
		if !ok || xtime.Format(got) != tc.want {
			t.Errorf("%s: got %v %v, want %s", tc.file, xtime.Format(got), ok, tc.want)
		}
	}
	if _, ok := sourceLastUpdated(event.EndpointMarkets, []byte(`[{"last_updated":null}]`)); ok {
		t.Error("null last_updated reported as ok")
	}
	if _, ok := sourceLastUpdated(event.EndpointCoinDetail, []byte(`{"id":"x"}`)); ok {
		t.Error("missing last_updated reported as ok")
	}
}
