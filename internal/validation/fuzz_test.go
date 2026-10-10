package validation_test

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"testing"
	"time"
	"unicode/utf8"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

// FuzzRawEventMarshalsToValidS1 (spec §5.3: fuzzed structs marshal to valid
// documents): every ok event the fetcher can build (any strict-JSON body
// except null, any string params, any 2xx status) marshals to a document that
// passes S1. A failure here means event.Marshal and raw_event.v1.json have
// drifted apart. The seeds run under plain go test;
// go test -fuzz=FuzzRawEventMarshalsToValidS1 ./internal/validation/ explores further.
func FuzzRawEventMarshalsToValidS1(f *testing.F) {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(true, "vs_currency", "usd", []byte(`[{"id":"bitcoin","current_price":82878.57}]`), 200, int64(12), 34)
	f.Add(false, "localization", "false", []byte(`{"id":"bitcoin","name":"é \"q\" <b>"}`), 203, int64(0), 0)
	f.Add(true, "", "", []byte(`[]`), 299, int64(999999), -1)
	f.Fuzz(func(t *testing.T, markets bool, k, v string, body []byte, status int, latency int64, age int) {
		payload := bytes.TrimSpace(body)
		// The same acceptance rule as coingecko.Client.Fetch (Task 6).
		if !jsontext.Value(payload).IsValid() || bytes.Equal(payload, []byte("null")) || !utf8.ValidString(k) || !utf8.ValidString(v) {
			t.Skip("not a body or params the fetcher would store as ok")
		}
		if status < 200 || status > 299 || latency < 0 {
			t.Skip("not an ok response")
		}
		endpoint, path := event.EndpointMarkets, "/coins/markets"
		if !markets {
			endpoint, path = event.EndpointCoinDetail, "/coins/bitcoin"
		}
		params := map[string]string{k: v}
		sha := event.SHA256Hex(payload)
		key := event.IdempotencyKey(event.SourceCoinGecko, endpoint, params, sha)
		meta := &event.HTTPMeta{Status: status, LatencyMS: latency}
		if age >= 0 {
			meta.AgeSeconds = &age
		}
		ev := event.RawEvent{
			SchemaVersion:  event.SchemaVersion,
			EventID:        "0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11",
			IdempotencyKey: &key,
			Source:         event.SourceCoinGecko,
			Endpoint:       endpoint,
			Request:        event.Request{Path: path, Params: params},
			FetchedAt:      xtime.Format(time.Date(2026, 10, 10, 10, 2, 33, 120_000_000, time.UTC)),
			Outcome:        event.OutcomeOK,
			HTTP:           meta,
			PayloadSHA256:  &sha,
			Payload:        json.RawMessage(payload),
		}
		b, err := event.Marshal(ev)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		vs, err := reg.Validate(validation.IDRawEvent, b)
		if err != nil || len(vs) > 0 {
			t.Fatalf("S1 rejects a fetcher-shaped event: err=%v violations=%+v\n%s", err, vs, b)
		}
	})
}
