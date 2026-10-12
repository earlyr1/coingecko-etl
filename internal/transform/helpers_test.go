package transform

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

// fixtureDir is the recorded CoinGecko responses copied in Task 4.
const fixtureDir = "../../schemas/testdata/coingecko"

var testRules = Rules{StaleAfter: 10 * time.Minute, TickerStaleAfter: time.Hour, TickerAnomalyPct: 2.0}

// rawEvent wraps payload bytes in an ok RawEvent the way the fetcher would.
func rawEvent(endpoint string, params map[string]string, payload []byte) event.RawEvent {
	return event.RawEvent{
		SchemaVersion: event.SchemaVersion,
		EventID:       "0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11",
		Source:        event.SourceCoinGecko,
		Endpoint:      endpoint,
		Request:       event.Request{Path: "/coins/markets", Params: params},
		FetchedAt:     "2026-10-10T10:08:17.123Z",
		Outcome:       event.OutcomeOK,
		Payload:       payload,
	}
}

// fixtureEvent loads schemas/testdata/coingecko/<name> as an ok RawEvent.
func fixtureEvent(t *testing.T, endpoint, name string) event.RawEvent {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	params := map[string]string{"vs_currency": "usd"}
	if endpoint == event.EndpointCoinDetail {
		params = map[string]string{"localization": "false", "tickers": "true", "market_data": "true"}
	}
	return rawEvent(endpoint, params, b)
}

func testLineage(offset int64) Lineage {
	return Lineage{
		EventID:     "0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11",
		RawObject:   "source=coingecko/ingest_date=2026-10-10/window=1000/part-00042.jsonl",
		Partition:   0,
		Offset:      offset,
		SchemaVer:   1,
		ProcessedAt: "2026-10-10T10:09:00.000Z",
	}
}

func loadRegistry(t *testing.T) *validation.Registry {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	return reg
}

// mustTransform runs Transform and fails the test on error.
func mustTransform(t *testing.T, ev event.RawEvent) []Record {
	t.Helper()
	recs, err := Transform(ev, testRules)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	return recs
}

// validateProcessed checks every processed record against its S2 schema.
func validateProcessed(t *testing.T, reg *validation.Registry, recs []Record) {
	t.Helper()
	for _, r := range recs {
		if r.Zone != ZoneProcessed {
			continue
		}
		row, err := RowJSON(r, testLineage(7))
		if err != nil {
			t.Fatalf("RowJSON %s %s: %v", r.Dataset, r.Key, err)
		}
		vs, err := reg.Validate(SchemaIDFor(r.Dataset), row)
		if err != nil {
			t.Fatalf("validate %s %s: %v", r.Dataset, r.Key, err)
		}
		if len(vs) > 0 {
			t.Errorf("%s %s violates schema: %+v\nrow: %s", r.Dataset, r.Key, vs, row)
		}
	}
}
