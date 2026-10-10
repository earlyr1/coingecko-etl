// Package coingecko is the transport layer for the CoinGecko REST API. It
// turns one HTTP call into one event.RawEvent (schema S1) and never inspects
// the payload beyond "is it JSON".
package coingecko

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"

	"github.com/google/uuid"
)

const (
	defaultTimeout    = 10 * time.Second
	defaultCooldown   = 60 * time.Second // used on 429 without a usable Retry-After
	backoffBase       = 2 * time.Second  // first cooldown after a 5xx / timeout / connection error
	backoffMax        = 60 * time.Second // cap of the exponential backoff
	maxBodyBytes      = 32 << 20         // a /coins/{id} body is ~100 KB
	maxErrorBodyBytes = 512              // body excerpt kept in event.Error
	userAgent         = "coingecko-etl/1"
)

// Client calls CoinGecko through a shared Limiter.
type Client struct {
	baseURL string
	apiKey  string
	hc      *http.Client
	lim     *Limiter
	now     func() time.Time
	timeout time.Duration // per request; tests shorten it

	// Exponential backoff with jitter on transient failures (spec §3.2).
	// failures is only touched while Fetch holds the limiter's in-flight
	// slot, so it needs no lock of its own.
	failures    int
	backoffBase time.Duration
	backoffMax  time.Duration
	jitter      func(time.Duration) time.Duration // tests replace it
}

// NewClient builds a client. baseURL is e.g. "https://api.coingecko.com/api/v3".
// apiKey may be empty (keyless). now supplies fetched_at (time.Now in production).
func NewClient(baseURL, apiKey string, hc *http.Client, l *Limiter, now func() time.Time) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	if l == nil {
		l = NewLimiter(0)
	}
	if now == nil {
		now = time.Now
	}
	return &Client{
		baseURL:     strings.TrimRight(baseURL, "/"),
		apiKey:      apiKey,
		hc:          hc,
		lim:         l,
		now:         now,
		timeout:     defaultTimeout,
		backoffBase: backoffBase,
		backoffMax:  backoffMax,
		jitter:      equalJitter,
	}
}

// equalJitter returns a duration in [d/2, d): half fixed, half random, so
// processes that failed together do not retry in lockstep.
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

// backoff records one more consecutive transient failure and cools the
// limiter down for backoffBase * 2^(failures-1), capped at backoffMax, with
// jitter. Ticks that fall inside the cooldown are skipped, never queued.
func (c *Client) backoff() {
	c.failures++
	d := c.backoffMax
	if shift := c.failures - 1; shift < 16 {
		if v := c.backoffBase << shift; v > 0 && v < d {
			d = v
		}
	}
	c.lim.Cooldown(c.jitter(d))
}

// MarketsRequest is GET /coins/markets for the top perPage coins by market cap.
func MarketsRequest(perPage int) event.Request {
	return event.Request{
		Path: "/coins/markets",
		Params: map[string]string{
			"vs_currency":             "usd",
			"order":                   "market_cap_desc",
			"per_page":                strconv.Itoa(perPage),
			"page":                    "1",
			"price_change_percentage": "1h,24h,7d",
			"sparkline":               "false",
			"precision":               "full",
		},
	}
}

// CoinDetailRequest is GET /coins/{id} with market data and tickers.
func CoinDetailRequest(coinID string) event.Request {
	return event.Request{
		Path: "/coins/" + url.PathEscape(coinID),
		Params: map[string]string{
			"localization":   "false",
			"tickers":        "true",
			"market_data":    "true",
			"community_data": "false",
			"developer_data": "false",
			"sparkline":      "false",
		},
	}
}

// requestURL joins baseURL, path and the params sorted by key. Commas stay
// literal (price_change_percentage=1h,24h,7d), everything else is escaped.
func requestURL(baseURL string, req event.Request) string {
	keys := make([]string, 0, len(req.Params))
	for k := range req.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(baseURL)
	sb.WriteString(req.Path)
	for i, k := range keys {
		if i == 0 {
			sb.WriteByte('?')
		} else {
			sb.WriteByte('&')
		}
		sb.WriteString(url.QueryEscape(k))
		sb.WriteByte('=')
		sb.WriteString(strings.ReplaceAll(url.QueryEscape(req.Params[k]), "%2C", ","))
	}
	return sb.String()
}

// Fetch performs exactly one HTTP call (never retries) and returns the event.
// It never returns an error: failures are encoded in Outcome/HTTP/Error.
//   - ok:            2xx and the body is valid JSON; Payload, PayloadSHA256, IdempotencyKey set
//   - http_error:    any other response (non-2xx, or 2xx with a non-JSON body); HTTP set
//   - network_error: no complete response (DNS, connect, timeout, body read, ctx); HTTP nil
//
// Payload is the body with surrounding whitespace trimmed (the bytes inside
// are verbatim), and PayloadSHA256 is the hash of exactly those bytes.
//
// On HTTP 429 it calls Cooldown(Retry-After, or 60 s) on the limiter. On a
// 5xx, a 2xx with a non-JSON body, a timeout or a connection error it calls
// Cooldown with an exponential backoff (2 s, 4 s, ... capped at 60 s, with
// jitter). Any other non-429 response (ok, 3xx, non-429 4xx) resets the
// backoff count; a 429 (handled by its Retry-After cooldown) and a 2xx body
// over the size limit leave it unchanged. A cancelled ctx (shutdown) is not
// counted as a failure.
func (c *Client) Fetch(ctx context.Context, endpoint string, req event.Request) event.RawEvent {
	ev := event.RawEvent{
		SchemaVersion: event.SchemaVersion,
		EventID:       uuid.Must(uuid.NewV7()).String(),
		Source:        event.SourceCoinGecko,
		Endpoint:      endpoint,
		Request:       req,
	}
	if err := c.lim.acquire(ctx); err != nil {
		ev.FetchedAt = xtime.Format(c.now())
		return failed(ev, event.OutcomeNetworkError, "rate limiter: "+err.Error())
	}
	defer c.lim.release()
	ev.FetchedAt = xtime.Format(c.now())

	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL(c.baseURL, req), nil)
	if err != nil {
		return failed(ev, event.OutcomeNetworkError, "build request: "+err.Error())
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", userAgent)
	if c.apiKey != "" {
		httpReq.Header.Set("x-cg-demo-api-key", c.apiKey)
	}

	start := time.Now()
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		if parent.Err() == nil { // shutdown is not an upstream failure
			c.backoff()
		}
		return failed(ev, event.OutcomeNetworkError, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	latency := time.Since(start)
	if err != nil {
		if parent.Err() == nil {
			c.backoff()
		}
		return failed(ev, event.OutcomeNetworkError, fmt.Sprintf("read body (HTTP %d): %v", resp.StatusCode, err))
	}

	ev.HTTP = httpMeta(resp, latency, c.now())
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		cd := defaultCooldown
		if ra := ev.HTTP.RetryAfterS; ra != nil && *ra > 0 {
			cd = time.Duration(*ra) * time.Second
		}
		c.lim.Cooldown(cd)
	case resp.StatusCode >= 500:
		c.backoff()
	}

	switch {
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			c.failures = 0 // a 3xx or 4xx is an answer, not an outage
		}
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, excerpt(body)))
	case len(body) > maxBodyBytes:
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: body larger than %d bytes", resp.StatusCode, maxBodyBytes))
	}
	// Surrounding whitespace is not part of the JSON value; trimming it keeps
	// the payload on one JSONL line. The bytes inside are kept verbatim, and
	// payload_sha256 is the hash of the trimmed bytes.
	payload := bytes.TrimSpace(body)
	// Strict JSON (valid UTF-8, no duplicate member names), the same rule the
	// validator applies at every later hop. encoding/json's Valid is more
	// lenient: a body it accepts could be stored as ok and then fail S1.
	// A literal null is no snapshot either.
	if !jsontext.Value(payload).IsValid() || bytes.Equal(payload, []byte("null")) {
		// An HTML maintenance or challenge page served with 200 is transient.
		c.backoff()
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: invalid JSON body: %s", resp.StatusCode, excerpt(body)))
	}
	c.failures = 0 // a good response ends the backoff
	sha := event.SHA256Hex(payload)
	key := event.IdempotencyKey(ev.Source, ev.Endpoint, req.Params, sha)
	ev.Outcome = event.OutcomeOK
	ev.Payload = json.RawMessage(payload)
	ev.PayloadSHA256 = &sha
	ev.IdempotencyKey = &key
	return ev
}

// failed fills the outcome fields of a non-ok event: no payload, no hash, no key.
func failed(ev event.RawEvent, outcome, msg string) event.RawEvent {
	ev.Outcome = outcome
	ev.Error = &msg
	ev.Payload = nil
	ev.PayloadSHA256 = nil
	ev.IdempotencyKey = nil
	return ev
}

func httpMeta(resp *http.Response, latency time.Duration, now time.Time) *event.HTTPMeta {
	m := &event.HTTPMeta{Status: resp.StatusCode, LatencyMS: latency.Milliseconds()}
	m.Date = headerPtr(resp.Header, "Date")
	m.CFCacheStatus = headerPtr(resp.Header, "Cf-Cache-Status")
	m.ETag = headerPtr(resp.Header, "Etag")
	if v := resp.Header.Get("Age"); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			m.AgeSeconds = &n
		}
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		if n, ok := parseRetryAfter(v, now); ok {
			m.RetryAfterS = &n
		}
	}
	return m
}

// parseRetryAfter accepts both forms of RFC 9110 Retry-After: delay-seconds
// ("60") and an HTTP-date ("Sat, 10 Oct 2026 10:09:44 GMT").
func parseRetryAfter(v string, now time.Time) (int, bool) {
	v = strings.TrimSpace(v)
	if n, err := strconv.Atoi(v); err == nil {
		if n < 0 {
			return 0, false
		}
		return n, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	secs := int(math.Ceil(t.Sub(now).Seconds()))
	if secs < 0 {
		secs = 0
	}
	return secs, true
}

func headerPtr(h http.Header, name string) *string {
	v := h.Get(name)
	if v == "" {
		return nil
	}
	return &v
}

// excerpt returns at most maxErrorBodyBytes of body as valid UTF-8 without
// NUL bytes (Postgres text rejects both invalid UTF-8 and NUL).
func excerpt(body []byte) string {
	if len(body) > maxErrorBodyBytes {
		body = body[:maxErrorBodyBytes]
	}
	s := strings.ToValidUTF8(string(body), "�")
	return strings.ReplaceAll(s, "\x00", "")
}
