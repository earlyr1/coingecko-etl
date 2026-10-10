package exporter

import (
	"bytes"
	"testing"
	"time"

	"coingecko-etl/internal/event"
)

func TestObjectKeys(t *testing.T) {
	// 2026-10-11T01:30:00+03:00 is 2026-10-10T22:30:00Z: the date segment must
	// be the UTC date, so a key built from the local date fails here.
	at := time.Date(2026, 10, 11, 1, 30, 0, 0, time.FixedZone("x", 3*3600))
	if got, want := rawObjectKey(42, at), "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl"; got != want {
		t.Errorf("rawObjectKey = %q, want %q", got, want)
	}
	if got, want := dlqObjectKey(42, at), "stage=export/date=2026-10-10/export+batch+00000000000000000042.jsonl"; got != want {
		t.Errorf("dlqObjectKey = %q, want %q", got, want)
	}
}

func TestJoinLines(t *testing.T) {
	if got := joinLines(nil); len(got) != 0 {
		t.Errorf("joinLines(nil) = %q, want empty", got)
	}
	got := joinLines([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if want := []byte("{\"a\":1}\n{\"b\":2}\n"); !bytes.Equal(got, want) {
		t.Errorf("joinLines = %q, want %q", got, want)
	}
}

func TestRowToEventKeepsPayloadVerbatim(t *testing.T) {
	payload := `[{"id":"bitcoin","current_price":61000.10,"market_cap":1234567890123456789}]`
	sha := event.SHA256Hex([]byte(payload))
	idem := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointMarkets, map[string]string{"page": "1"}, sha)
	httpJSON := `{"status": 200, "latency_ms": 85, "date": null, "age_s": 12, "cf_cache_status": "HIT", "etag": null, "retry_after_s": null}`
	r := apiRow{
		Seq: 7, EventID: "0199c8a0-0000-7000-8000-000000000001", IdempotencyKey: &idem, SchemaVersion: 1,
		Source: event.SourceCoinGecko, Endpoint: event.EndpointMarkets,
		Request:   `{"path": "/coins/markets", "params": {"page": "1"}}`,
		FetchedAt: time.Date(2026, 10, 10, 10, 7, 55, 123456000, time.UTC),
		Outcome:   event.OutcomeOK, HTTP: &httpJSON, Payload: &payload, PayloadSHA256: &sha,
	}
	ev, err := rowToEvent(r)
	if err != nil {
		t.Fatalf("rowToEvent: %v", err)
	}
	if string(ev.Payload) != payload {
		t.Errorf("payload = %s, want verbatim %s", ev.Payload, payload)
	}
	if ev.FetchedAt != "2026-10-10T10:07:55.123Z" {
		t.Errorf("fetched_at = %q", ev.FetchedAt)
	}
	if ev.Request.Path != "/coins/markets" || ev.Request.Params["page"] != "1" {
		t.Errorf("request = %+v", ev.Request)
	}
	if ev.HTTP == nil || ev.HTTP.Status != 200 || ev.HTTP.AgeSeconds == nil || *ev.HTTP.AgeSeconds != 12 {
		t.Errorf("http = %+v", ev.HTTP)
	}
	if ev.SchemaVersion != 1 || ev.EventID != r.EventID || *ev.PayloadSHA256 != sha || *ev.IdempotencyKey != idem {
		t.Errorf("envelope fields not copied: %+v", ev)
	}
}
