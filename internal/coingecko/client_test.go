package coingecko

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
)

var fixedNow = time.Date(2026, 10, 10, 10, 2, 33, 123_000_000, time.UTC)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schemas/testdata/coingecko/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

func newTestClient(t *testing.T, h http.HandlerFunc, apiKey string) (*Client, *Limiter) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	lim := NewLimiter(0)
	c := NewClient(srv.URL+"/api/v3/", apiKey, srv.Client(), lim, func() time.Time { return fixedNow })
	return c, lim
}

// assertS1 checks the event against the real S1 schema, as the fetcher will.
func assertS1(t *testing.T, ev event.RawEvent) {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	b, err := event.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	vs, err := reg.Validate(validation.IDRawEvent, b)
	if err != nil || len(vs) > 0 {
		t.Fatalf("event violates S1: err=%v violations=%+v\n%s", err, vs, b)
	}
}

func TestMarketsRequest(t *testing.T) {
	r := MarketsRequest(100)
	if r.Path != "/coins/markets" {
		t.Fatalf("path = %q", r.Path)
	}
	want := map[string]string{
		"vs_currency": "usd", "order": "market_cap_desc", "per_page": "100", "page": "1",
		"price_change_percentage": "1h,24h,7d", "sparkline": "false", "precision": "full",
	}
	assertParams(t, r.Params, want)
}

func TestCoinDetailRequest(t *testing.T) {
	r := CoinDetailRequest("usd-coin")
	if r.Path != "/coins/usd-coin" {
		t.Fatalf("path = %q", r.Path)
	}
	want := map[string]string{
		"localization": "false", "tickers": "true", "market_data": "true",
		"community_data": "false", "developer_data": "false", "sparkline": "false",
	}
	assertParams(t, r.Params, want)
}

// assertParams checks that got holds exactly the params in want.
func assertParams(t *testing.T, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("params = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("param %s = %q, want %q", k, got[k], v)
		}
	}
}

func TestFetchOK(t *testing.T) {
	body := fixture(t, "markets_top10_t0.json")
	var gotPath, gotQuery, gotAccept, gotKey string
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		gotAccept, gotKey = r.Header.Get("Accept"), r.Header.Get("x-cg-demo-api-key")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Age", "34")
		w.Header().Set("Cf-Cache-Status", "HIT")
		w.Header().Set("Etag", `W/"c4924cc46ddaa91ee4660396b13c9c63"`)
		_, _ = w.Write(body)
	}, "")

	req := MarketsRequest(10)
	ev := c.Fetch(context.Background(), event.EndpointMarkets, req)

	if gotPath != "/api/v3/coins/markets" {
		t.Errorf("path = %q", gotPath)
	}
	wantQuery := "order=market_cap_desc&page=1&per_page=10&precision=full&price_change_percentage=1h,24h,7d&sparkline=false&vs_currency=usd"
	if gotQuery != wantQuery {
		t.Errorf("query = %q\nwant    %q", gotQuery, wantQuery)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q", gotAccept)
	}
	if gotKey != "" {
		t.Errorf("api key header sent without a key: %q", gotKey)
	}

	if ev.Outcome != event.OutcomeOK || ev.Error != nil {
		t.Fatalf("outcome = %s error = %v", ev.Outcome, ev.Error)
	}
	id, err := uuid.Parse(ev.EventID)
	if err != nil || id.Version() != 7 {
		t.Errorf("event_id %q is not a UUIDv7 (%v)", ev.EventID, err)
	}
	if ev.SchemaVersion != 1 || ev.Source != "coingecko" || ev.Endpoint != "coins_markets" {
		t.Errorf("envelope = %d %s %s", ev.SchemaVersion, ev.Source, ev.Endpoint)
	}
	if ev.FetchedAt != "2026-10-10T10:02:33.123Z" {
		t.Errorf("fetched_at = %s", ev.FetchedAt)
	}
	if !bytes.Equal(ev.Payload, body) {
		t.Errorf("payload is not the verbatim body")
	}
	sum := sha256.Sum256(body)
	wantSHA := hex.EncodeToString(sum[:])
	if ev.PayloadSHA256 == nil || *ev.PayloadSHA256 != wantSHA {
		t.Errorf("payload_sha256 = %v, want %s", ev.PayloadSHA256, wantSHA)
	}
	wantKey := event.IdempotencyKey("coingecko", "coins_markets", req.Params, wantSHA)
	if ev.IdempotencyKey == nil || *ev.IdempotencyKey != wantKey {
		t.Errorf("idempotency_key = %v, want %s", ev.IdempotencyKey, wantKey)
	}
	h := ev.HTTP
	if h == nil || h.Status != 200 {
		t.Fatalf("http = %+v", h)
	}
	if h.Date == nil || h.AgeSeconds == nil || *h.AgeSeconds != 34 ||
		h.CFCacheStatus == nil || *h.CFCacheStatus != "HIT" ||
		h.ETag == nil || *h.ETag != `W/"c4924cc46ddaa91ee4660396b13c9c63"` || h.RetryAfterS != nil {
		t.Errorf("http meta = %+v", h)
	}
	if !lim.Allowed(time.Now()) {
		t.Error("limiter not released after Fetch")
	}
	assertS1(t, ev)
}

func TestFetchSameBodySameKey(t *testing.T) {
	body := fixture(t, "coin_bitcoin_full_t0.json")
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }, "")
	req := CoinDetailRequest("bitcoin")
	a := c.Fetch(context.Background(), event.EndpointCoinDetail, req)
	b := c.Fetch(context.Background(), event.EndpointCoinDetail, req)
	if a.EventID == b.EventID {
		t.Error("two calls share an event_id")
	}
	if a.IdempotencyKey == nil || b.IdempotencyKey == nil || *a.IdempotencyKey != *b.IdempotencyKey {
		t.Error("identical responses must share the idempotency key")
	}
}

func TestFetchSendsAPIKey(t *testing.T) {
	var got string
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-cg-demo-api-key")
		_, _ = w.Write([]byte(`[]`))
	}, "CG-demo-key")
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	if got != "CG-demo-key" {
		t.Fatalf("x-cg-demo-api-key = %q", got)
	}
	if ev.Outcome != event.OutcomeOK {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
}

func TestFetch429SetsCooldown(t *testing.T) {
	body := `{"status":{"error_code":429,"error_message":"You've exceeded the Rate Limit."}}`
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(body))
	}, "")
	before := time.Now()
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))

	if ev.Outcome != event.OutcomeHTTPError {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	if ev.HTTP == nil || ev.HTTP.Status != 429 || ev.HTTP.RetryAfterS == nil || *ev.HTTP.RetryAfterS != 60 {
		t.Fatalf("http = %+v", ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "HTTP 429") || !strings.Contains(*ev.Error, "Rate Limit") {
		t.Errorf("error = %v", ev.Error)
	}
	if ev.Payload != nil || ev.PayloadSHA256 != nil || ev.IdempotencyKey != nil {
		t.Error("failed call must not carry payload, hash or idempotency key")
	}
	if lim.Allowed(before.Add(59 * time.Second)) {
		t.Error("limiter allows requests 59s after a 429 with Retry-After: 60")
	}
	if !lim.Allowed(time.Now().Add(61 * time.Second)) {
		t.Error("limiter still cooling down after Retry-After elapsed")
	}
	assertS1(t, ev)
}

func TestFetch429WithoutRetryAfterCoolsDown60s(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}, "")
	before := time.Now()
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.HTTP == nil || ev.HTTP.Status != 429 || ev.HTTP.RetryAfterS != nil {
		t.Fatalf("http = %+v", ev.HTTP)
	}
	if lim.Allowed(before.Add(59 * time.Second)) {
		t.Error("no default 60s cooldown after a 429 without Retry-After")
	}
	if !lim.Allowed(time.Now().Add(61 * time.Second)) {
		t.Error("default cooldown longer than 60s")
	}
}

func TestFetch500(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusInternalServerError)
	}, "")
	ev := c.Fetch(context.Background(), event.EndpointCoinDetail, CoinDetailRequest("bitcoin"))
	if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 500 {
		t.Fatalf("outcome = %s http = %+v", ev.Outcome, ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "HTTP 500: upstream exploded") {
		t.Errorf("error = %v", ev.Error)
	}
	if lim.Allowed(time.Now()) {
		t.Error("500 must start a backoff cooldown")
	}
	assertS1(t, ev)
}

// Spec §3.2: exponential backoff with jitter on 5xx and timeouts. The
// cooldown doubles per consecutive failure, is capped, and resets after a
// good response.
func TestFetchBackoffGrowsCapsAndResets(t *testing.T) {
	status := http.StatusBadGateway
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			http.Error(w, "bad gateway", status)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}, "")
	var got []time.Duration
	c.backoffBase, c.backoffMax = time.Millisecond, 4*time.Millisecond
	c.jitter = func(d time.Duration) time.Duration { got = append(got, d); return d }
	for range 4 {
		c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	}
	status = http.StatusOK
	if ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1)); ev.Outcome != event.OutcomeOK {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	status = http.StatusServiceUnavailable
	c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond, time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("cooldowns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cooldowns = %v, want %v", got, want)
		}
	}
	for range 1000 {
		if j := equalJitter(10 * time.Second); j < 5*time.Second || j >= 10*time.Second {
			t.Fatalf("equalJitter(10s) = %v, want [5s, 10s)", j)
		}
	}
}

func TestFetchInvalidJSONBody(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<html>maintenance\x00\xff</html>"))
	}, "")
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 200 {
		t.Fatalf("outcome = %s http = %+v", ev.Outcome, ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "invalid JSON body") {
		t.Fatalf("error = %v", ev.Error)
	}
	if strings.Contains(*ev.Error, "\x00") {
		t.Error("error text contains NUL")
	}
	if ev.Payload != nil || ev.PayloadSHA256 != nil || ev.IdempotencyKey != nil {
		t.Error("invalid body must not be stored as payload")
	}
	assertS1(t, ev)
}

// A 200 body that encoding/json accepts but strict JSON rejects (duplicate
// member names, invalid UTF-8) or a bare null must not become an ok event:
// S1 validation would reject it later and the call would vanish.
func TestFetchRejectsNonStrictJSONBody(t *testing.T) {
	for name, body := range map[string]string{
		"duplicate names": `[{"id":"bitcoin","id":"ethereum"}]`,
		"invalid utf-8":   "[{\"id\":\"bit\xffcoin\"}]",
		"null":            `null`,
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }, "")
			c.jitter = func(time.Duration) time.Duration { return 0 }
			ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
			if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 200 || ev.Payload != nil {
				t.Fatalf("outcome = %s http = %+v payload = %s", ev.Outcome, ev.HTTP, ev.Payload)
			}
			assertS1(t, ev)
		})
	}
}

func TestFetchTimeoutIsNetworkError(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}, "")
	c.timeout = 50 * time.Millisecond
	c.jitter = func(time.Duration) time.Duration { return 0 } // isolate the release check from the backoff
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	if ev.HTTP != nil {
		t.Errorf("network error must have http = nil, got %+v", ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "deadline exceeded") {
		t.Errorf("error = %v", ev.Error)
	}
	if ev.FetchedAt == "" {
		t.Error("fetched_at missing")
	}
	if !lim.Allowed(time.Now()) {
		t.Error("limiter not released after a timeout")
	}
	assertS1(t, ev)
}

func TestFetchConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := NewClient(url, "", &http.Client{}, NewLimiter(0), func() time.Time { return fixedNow })
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError || ev.HTTP != nil || ev.Error == nil {
		t.Fatalf("event = %+v", ev)
	}
}

func TestFetchCancelledWhileCoolingDown(t *testing.T) {
	calls := 0
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { calls++ }, "")
	lim.Cooldown(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ev := c.Fetch(ctx, event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError || calls != 0 {
		t.Fatalf("outcome = %s calls = %d", ev.Outcome, calls)
	}
	if ev.Error == nil || !strings.HasPrefix(*ev.Error, "rate limiter: ") {
		t.Errorf("error = %v", ev.Error)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 10, 10, 8, 44, 0, time.UTC)
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"60", 60, true},
		{" 5 ", 5, true},
		{"Sat, 10 Oct 2026 10:09:44 GMT", 60, true},
		{"Sat, 10 Oct 2026 10:00:00 GMT", 0, true},
		{"-1", 0, false},
		{"soon", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseRetryAfter(tc.in, now)
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseRetryAfter(%q) = %d,%v want %d,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
