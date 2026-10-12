package transform

import (
	"strings"
	"testing"

	"coingecko-etl/internal/event"
)

// okEvent is rawEvent with the idempotency key the fetcher computes.
func okEvent(endpoint string, params map[string]string, payload []byte) event.RawEvent {
	ev := rawEvent(endpoint, params, payload)
	k := event.IdempotencyKey(ev.Source, endpoint, params, event.SHA256Hex(payload))
	ev.IdempotencyKey = &k
	return ev
}

// missingIDKeys returns the keys of the missing_id records of dataset.
func missingIDKeys(t *testing.T, ev event.RawEvent, dataset string) []string {
	t.Helper()
	var keys []string
	for _, r := range mustTransform(t, ev) {
		if r.Dataset == dataset && r.Reason == ReasonMissingID {
			keys = append(keys, r.Key)
		}
	}
	if len(keys) == 0 {
		t.Fatalf("no missing_id %s record", dataset)
	}
	return keys
}

const tickerTS = "2026-10-10T10:00:00+00:00"

func coinPayload(id string, tickers ...string) []byte {
	return []byte(`{"id":"` + id + `","symbol":"x","last_updated":"2026-10-10T10:01:51.000Z","tickers":[` + strings.Join(tickers, ",") + `]}`)
}

// CoinGecko stamps every coin of one pass with the same last_updated, so a key
// of index and snapshot time alone collides across coins and responses.
func TestMissingIDKeysUniqueAcrossResponses(t *testing.T) {
	detail := map[string]string{"localization": "false", "tickers": "true", "market_data": "true"}
	markets := map[string]string{"vs_currency": "usd"}

	t.Run("tickers", func(t *testing.T) {
		btc := missingIDKeys(t, okEvent(event.EndpointCoinDetail, detail, coinPayload("bitcoin", tickerJSON("", "BTC", "USDT", "100", false, tickerTS))), DatasetTicker)
		eth := missingIDKeys(t, okEvent(event.EndpointCoinDetail, detail, coinPayload("ethereum", tickerJSON("", "ETH", "USDT", "100", false, tickerTS))), DatasetTicker)
		if btc[0] == eth[0] {
			t.Fatalf("id-less tickers of two coins share key %q", btc[0])
		}
		if !strings.HasPrefix(btc[0], "bitcoin|#") || !strings.HasPrefix(eth[0], "ethereum|#") {
			t.Fatalf("ticker keys %q, %q do not start with the coin id", btc[0], eth[0])
		}
	})
	t.Run("coin_snapshot", func(t *testing.T) {
		a := missingIDKeys(t, okEvent(event.EndpointCoinDetail, detail, []byte(`{"id":"","symbol":"a","last_updated":"2026-10-10T10:01:51Z"}`)), DatasetCoinSnapshot)
		b := missingIDKeys(t, okEvent(event.EndpointCoinDetail, detail, []byte(`{"id":"","symbol":"b","last_updated":"2026-10-10T10:01:51Z"}`)), DatasetCoinSnapshot)
		if a[0] == b[0] {
			t.Fatalf("id-less coin snapshots of two responses share key %q", a[0])
		}
	})
	t.Run("market_snapshot", func(t *testing.T) {
		a := missingIDKeys(t, okEvent(event.EndpointMarkets, markets, []byte(`[{"symbol":"a","current_price":1,"last_updated":"2026-10-10T10:07:55.000Z"}]`)), DatasetMarketSnapshot)
		b := missingIDKeys(t, okEvent(event.EndpointMarkets, markets, []byte(`[{"symbol":"b","current_price":2,"last_updated":"2026-10-10T10:07:55.000Z"}]`)), DatasetMarketSnapshot)
		if a[0] == b[0] {
			t.Fatalf("id-less market rows of two responses share key %q", a[0])
		}
	})
}

// Byte-identical copies of one response (CDN repeats, replays) must produce the
// same keys, so dedup drops the copy as duplicate_response or replay.
func TestMissingIDKeysStableForCopiesOfOneResponse(t *testing.T) {
	detail := map[string]string{"localization": "false", "tickers": "true", "market_data": "true"}
	cases := []struct {
		name, endpoint, dataset string
		params                  map[string]string
		payload                 []byte
	}{
		{"market_snapshot", event.EndpointMarkets, DatasetMarketSnapshot, map[string]string{"vs_currency": "usd"},
			[]byte(`[{"symbol":"a","current_price":1,"last_updated":"2026-10-10T10:07:55.000Z"},{"id":"","symbol":"b","current_price":1,"last_updated":"2026-10-10T10:07:55.000Z"}]`)},
		{"ticker", event.EndpointCoinDetail, DatasetTicker, detail,
			coinPayload("bitcoin", tickerJSON("binance", "BTC", "USDT", "100", false, tickerTS), tickerJSON("", "BTC", "USDT", "100", false, tickerTS))},
		{"coin_snapshot", event.EndpointCoinDetail, DatasetCoinSnapshot, detail,
			[]byte(`{"id":"","symbol":"x","last_updated":"2026-10-10T10:01:51Z"}`)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first := okEvent(c.endpoint, c.params, c.payload)
			copyOf := okEvent(c.endpoint, c.params, c.payload)
			copyOf.EventID = "0199d0a5-0000-7000-8000-000000000002"
			copyOf.FetchedAt = "2026-10-10T10:08:47.456Z"
			a, b := missingIDKeys(t, first, c.dataset), missingIDKeys(t, copyOf, c.dataset)
			if strings.Join(a, ",") != strings.Join(b, ",") {
				t.Fatalf("copies of one response keyed %v and %v", a, b)
			}
			for _, k := range a {
				if !strings.Contains(k, "#"+*first.IdempotencyKey+"|") {
					t.Fatalf("key %q does not carry the idempotency key", k)
				}
			}
		})
	}
}
