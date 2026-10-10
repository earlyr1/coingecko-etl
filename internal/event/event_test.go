package event

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
)

func strp(s string) *string { return &s }
func intp(i int) *int       { return &i }

func TestSHA256Hex(t *testing.T) {
	// sha256("abc") from FIPS 180-2.
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := SHA256Hex([]byte("abc")); got != want {
		t.Fatalf("SHA256Hex = %s, want %s", got, want)
	}
}

func TestIdempotencyKeyCanonicalForm(t *testing.T) {
	payloadHash := SHA256Hex([]byte(`[{"id":"bitcoin"}]`))
	params := map[string]string{"vs_currency": "usd", "order": "market_cap_desc", "per_page": "100", "page": "1"}
	canonical := "coingecko|coins_markets|order=market_cap_desc&page=1&per_page=100&vs_currency=usd|" + payloadHash
	sum := sha256.Sum256([]byte(canonical))
	want := hex.EncodeToString(sum[:])

	if got := IdempotencyKey(SourceCoinGecko, EndpointMarkets, params, payloadHash); got != want {
		t.Fatalf("IdempotencyKey = %s, want %s", got, want)
	}
}

func TestIdempotencyKeyIgnoresMapOrder(t *testing.T) {
	h := SHA256Hex([]byte(`{}`))
	keys := []string{"vs_currency", "order", "per_page", "page", "sparkline", "price_change_percentage"}
	vals := []string{"usd", "market_cap_desc", "100", "1", "false", "1h,24h,7d"}
	first := ""
	// Build the same map with different insertion orders (rotations); Go map
	// iteration order is also randomized per run.
	for rot := 0; rot < len(keys); rot++ {
		m := map[string]string{}
		for i := range keys {
			j := (i + rot) % len(keys)
			m[keys[j]] = vals[j]
		}
		got := IdempotencyKey(SourceCoinGecko, EndpointMarkets, m, h)
		if first == "" {
			first = got
		} else if got != first {
			t.Fatalf("rotation %d: key %s != %s", rot, got, first)
		}
	}
}

func TestIdempotencyKeyDistinguishesInputs(t *testing.T) {
	p := map[string]string{"localization": "false"}
	base := IdempotencyKey(SourceCoinGecko, EndpointCoinDetail, p, SHA256Hex([]byte(`{"a":1}`)))
	others := []string{
		IdempotencyKey(SourceCoinGecko, EndpointCoinDetail, p, SHA256Hex([]byte(`{"a":2}`))),
		IdempotencyKey(SourceCoinGecko, EndpointMarkets, p, SHA256Hex([]byte(`{"a":1}`))),
		IdempotencyKey(SourceCoinGecko, EndpointCoinDetail, map[string]string{"localization": "true"}, SHA256Hex([]byte(`{"a":1}`))),
	}
	for i, o := range others {
		if o == base {
			t.Errorf("variant %d produced the same key", i)
		}
	}
	if len(base) != 64 {
		t.Errorf("key length %d, want 64 hex chars", len(base))
	}
}

// Unusual whitespace, key order and HTML-ish characters that json.Marshal would
// normally compact or escape. No line break: Marshal refuses those.
var oddPayload = []byte("[ {\"z\" : 1,\t \"a\":[ 1 , 2.50 ,1e3],  \"html\":\"<b>&</b>\", \"price\": 82800.000000001 } ]")

func sampleEvent() RawEvent {
	sum := SHA256Hex(oddPayload)
	params := map[string]string{"vs_currency": "usd", "per_page": "100", "page": "1", "order": "market_cap_desc"}
	key := IdempotencyKey(SourceCoinGecko, EndpointMarkets, params, sum)
	return RawEvent{
		SchemaVersion:  SchemaVersion,
		EventID:        "0199cd8e-6f4a-7b3c-9d2e-1f0a2b3c4d5e",
		IdempotencyKey: &key,
		Source:         SourceCoinGecko,
		Endpoint:       EndpointMarkets,
		Request:        Request{Path: "/coins/markets", Params: params},
		FetchedAt:      "2026-10-10T10:02:33.123Z",
		Outcome:        OutcomeOK,
		HTTP: &HTTPMeta{
			Status:        200,
			LatencyMS:     412,
			Date:          strp("Sat, 10 Oct 2026 10:02:33 GMT"),
			AgeSeconds:    intp(12),
			CFCacheStatus: strp("HIT"),
			ETag:          strp(`W/"c4924cc46ddaa91ee4660396b13c9c63"`),
		},
		PayloadSHA256: &sum,
		Payload:       oddPayload,
	}
}

func TestMarshalIsDeterministicAndOrdered(t *testing.T) {
	ev := sampleEvent()
	got, err := Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"schema_version":1,"event_id":"0199cd8e-6f4a-7b3c-9d2e-1f0a2b3c4d5e",` +
		`"idempotency_key":"` + *ev.IdempotencyKey + `","source":"coingecko","endpoint":"coins_markets",` +
		`"request":{"path":"/coins/markets","params":{"order":"market_cap_desc","page":"1","per_page":"100","vs_currency":"usd"}},` +
		`"fetched_at":"2026-10-10T10:02:33.123Z","outcome":"ok",` +
		`"http":{"status":200,"latency_ms":412,"date":"Sat, 10 Oct 2026 10:02:33 GMT","age_s":12,"cf_cache_status":"HIT","etag":"W/\"c4924cc46ddaa91ee4660396b13c9c63\"","retry_after_s":null},` +
		`"error":null,"payload_sha256":"` + *ev.PayloadSHA256 + `","payload":` + string(oddPayload) + `}`
	if string(got) != want {
		t.Fatalf("Marshal mismatch:\n got %s\nwant %s", got, want)
	}
	for i := 0; i < 20; i++ {
		again, _ := Marshal(sampleEvent())
		if !bytes.Equal(again, got) {
			t.Fatalf("Marshal not deterministic on run %d", i)
		}
	}
}

func TestRoundTripKeepsPayloadBytes(t *testing.T) {
	ev := sampleEvent()
	b, err := Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	back, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !bytes.Equal(back.Payload, oddPayload) {
		t.Fatalf("payload changed:\n got %q\nwant %q", back.Payload, oddPayload)
	}
	if SHA256Hex(back.Payload) != *ev.PayloadSHA256 {
		t.Fatal("payload hash no longer verifies after round trip")
	}
	if !reflect.DeepEqual(back, ev) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", back, ev)
	}
	again, err := Marshal(back)
	if err != nil || !bytes.Equal(again, b) {
		t.Fatalf("Marshal(Unmarshal(b)) != b: %v", err)
	}
}

func TestMarshalFailedCallWritesNullPayload(t *testing.T) {
	ev := RawEvent{
		SchemaVersion: SchemaVersion,
		EventID:       "0199cd8e-6f4a-7b3c-9d2e-1f0a2b3c4d5f",
		Source:        SourceCoinGecko,
		Endpoint:      EndpointCoinDetail,
		Request:       Request{Path: "/coins/bitcoin"},
		FetchedAt:     "2026-10-10T10:03:00.000Z",
		Outcome:       OutcomeNetworkError,
		Error:         strp("dial tcp: i/o timeout"),
	}
	b, err := Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"schema_version":1,"event_id":"0199cd8e-6f4a-7b3c-9d2e-1f0a2b3c4d5f","idempotency_key":null,` +
		`"source":"coingecko","endpoint":"coin_detail","request":{"path":"/coins/bitcoin","params":{}},` +
		`"fetched_at":"2026-10-10T10:03:00.000Z","outcome":"network_error","http":null,` +
		`"error":"dial tcp: i/o timeout","payload_sha256":null,"payload":null}`
	if string(b) != want {
		t.Fatalf("Marshal mismatch:\n got %s\nwant %s", b, want)
	}
	back, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.Payload != nil {
		t.Fatalf("null payload decoded as %q, want nil", back.Payload)
	}
}

func TestMarshalRejectsInvalidPayload(t *testing.T) {
	ev := sampleEvent()
	ev.Payload = []byte(`{"truncated":`)
	if _, err := Marshal(ev); err == nil {
		t.Fatal("Marshal accepted invalid JSON payload")
	}
}

// One event is one JSONL line in raw/ and one message: a payload with a line
// break would split it, so Marshal refuses it (the fetcher compacts such bodies).
func TestMarshalRejectsLineBreaksInPayload(t *testing.T) {
	for _, p := range []string{"[1,\n2]", "{\"a\":\r1}", "[1]\n"} {
		ev := sampleEvent()
		ev.Payload = []byte(p)
		if _, err := Marshal(ev); err == nil {
			t.Errorf("Marshal accepted payload %q", p)
		}
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	for _, in := range []string{``, `{`, `[]`, `{"schema_version":"one"}`} {
		if _, err := Unmarshal([]byte(in)); err == nil {
			t.Errorf("Unmarshal(%q) succeeded, want error", in)
		}
	}
}
