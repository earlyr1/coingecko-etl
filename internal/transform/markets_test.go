package transform

import (
	"bytes"
	"testing"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
)

func TestMarketsSample(t *testing.T) {
	ev := fixtureEvent(t, event.EndpointMarkets, "markets_usd250_t0.json")
	recs := mustTransform(t, ev)
	if len(recs) != 250 {
		t.Fatalf("records = %d, want 250", len(recs))
	}
	var processed, stale, inactive, anomaly, roi int
	staleIDs := map[string]bool{}
	anomalyIDs := map[string]bool{}
	for i, r := range recs {
		if r.Dataset != DatasetMarketSnapshot || r.Ordinal != i || r.EventDate != "2026-10-10" {
			t.Fatalf("record %d: dataset=%s ordinal=%d date=%s", i, r.Dataset, r.Ordinal, r.EventDate)
		}
		m := r.Business.(MarketSnapshot)
		if m.SnapshotAt != "2026-10-10T10:07:55.000Z" {
			t.Fatalf("snapshot_at = %s", m.SnapshotAt)
		}
		if r.Key != m.CoinID+"|2026-10-10T10:07:55.000Z" {
			t.Fatalf("key = %s", r.Key)
		}
		if r.Zone == ZoneProcessed {
			processed++
		}
		if m.IsStale {
			stale++
			staleIDs[m.CoinID] = true
		}
		if m.IsInactive {
			inactive++
		}
		if m.IsAnomaly {
			anomaly++
			anomalyIDs[m.CoinID] = true
		}
		if m.ROITimes != nil {
			roi++
			if m.ROICurrency == nil || m.ROIPct == nil {
				t.Errorf("%s: partial roi", m.CoinID)
			}
		}
	}
	if processed != 250 {
		t.Errorf("processed = %d, want 250", processed)
	}
	if stale != 2 || !staleIDs["figure-heloc"] || !staleIDs["a7a5"] {
		t.Errorf("stale = %d %v, want figure-heloc and a7a5", stale, staleIDs)
	}
	if inactive != 14 {
		t.Errorf("inactive = %d, want 14", inactive)
	}
	if anomaly != 3 || !anomalyIDs["shuffle-2"] || !anomalyIDs["sonic-3"] || !anomalyIDs["ultima"] {
		t.Errorf("anomaly = %d %v, want shuffle-2, sonic-3, ultima", anomaly, anomalyIDs)
	}
	if roi != 27 {
		t.Errorf("roi rows = %d, want 27 (223 of 250 have roi null)", roi)
	}
}

func TestMarketsRowsValidateAgainstSchema(t *testing.T) {
	reg := loadRegistry(t)
	for _, name := range []string{"markets_usd250_t0.json", "markets_top10_t0.json", "markets_top10_t1.json", "markets_top10_t2.json"} {
		t.Run(name, func(t *testing.T) {
			validateProcessed(t, reg, mustTransform(t, fixtureEvent(t, event.EndpointMarkets, name)))
		})
	}
}

func TestMarketsSchemaRejectsBadRow(t *testing.T) {
	reg := loadRegistry(t)
	recs := mustTransform(t, fixtureEvent(t, event.EndpointMarkets, "markets_top10_t0.json"))
	row, err := RowJSON(recs[0], testLineage(1))
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Replace(row, []byte(`"vs_currency":"usd"`), []byte(`"vs_currency":"usd","extra":1`), 1)
	vs, err := reg.Validate(validation.IDMarketSnapshot, bad)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) == 0 {
		t.Fatal("schema accepted an unknown field")
	}
}

const goldenBitcoinRow = `{"coin_id":"bitcoin","snapshot_at":"2026-10-10T10:07:55.000Z","symbol":"BTC","name":"Bitcoin","vs_currency":"usd","market_cap_rank":1,"price":"82776","market_cap":"1663572458130","fdv":"1663572458130","total_volume":"22365630055","high_24h":"83398","low_24h":"82229","price_change_24h":"148.1","price_change_pct_1h":0.0036,"price_change_pct_24h":0.17924,"price_change_pct_7d":-2.1678,"market_cap_change_24h":"3114590672","market_cap_change_pct_24h":0.18757,"circulating_supply":"20096053","total_supply":"20096068","max_supply":"21000000","ath":"126080","ath_change_pct":-34.3464,"ath_at":"2025-10-06T10:57:42.000Z","atl":"67.81","atl_change_pct":121972.27062,"atl_at":"2013-07-05T16:00:00.000Z","roi_times":null,"roi_currency":null,"roi_pct":null,"source_updated_at":"2026-10-10T10:07:55.000Z","is_stale":false,"is_inactive":false,"is_anomaly":false,"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl","_kafka_partition":0,"_kafka_offset":7,"_schema_version":1,"_processed_at":"2026-10-10T10:09:00.000Z"}`

func TestMarketsGoldenRow(t *testing.T) {
	recs := mustTransform(t, fixtureEvent(t, event.EndpointMarkets, "markets_usd250_t0.json"))
	row, err := RowJSON(recs[0], testLineage(7))
	if err != nil {
		t.Fatal(err)
	}
	if string(row) != goldenBitcoinRow {
		t.Fatalf("row mismatch\n got: %s\nwant: %s", row, goldenBitcoinRow)
	}
}

// roi_currency is a currency code like vs_currency and
// coin_market_by_currency.currency, so it is lower case too and joins on them.
func TestMarketsROIFlattened(t *testing.T) {
	payload := []byte(`[{"id":"ethereum","symbol":"eth","name":"Ethereum","current_price":2000,"total_volume":1,
	  "roi":{"times":42.012142715693386,"currency":"btc","percentage":4201.214271569338},
	  "last_updated":"2026-10-10T10:07:55.000Z"},
	 {"id":"a","symbol":"a","name":"A","current_price":1,"total_volume":1,
	  "roi":{"times":1.5,"currency":"USD","percentage":150},"last_updated":"2026-10-10T10:07:55.000Z"},
	 {"id":"b","symbol":"b","name":"B","current_price":1,"total_volume":1,
	  "roi":{"times":2.5,"currency":" Eth ","percentage":250},"last_updated":"2026-10-10T10:07:55.000Z"}]`)
	recs := mustTransform(t, rawEvent(event.EndpointMarkets, nil, payload))
	m := recs[0].Business.(MarketSnapshot)
	if m.ROITimes == nil || *m.ROITimes != 42.012142715693386 || *m.ROICurrency != "btc" || *m.ROIPct != 4201.214271569338 {
		t.Fatalf("roi = %v %v %v", m.ROITimes, m.ROICurrency, m.ROIPct)
	}
	if m.VsCurrency != "usd" {
		t.Fatalf("vs_currency default = %q", m.VsCurrency)
	}
	for i, want := range map[int]string{1: "usd", 2: "eth"} {
		if got := recs[i].Business.(MarketSnapshot).ROICurrency; got == nil || *got != want {
			t.Errorf("record %d roi_currency = %v, want %q", i, got, want)
		}
	}
}

// With precision=full, current_price has more digits than the rounded
// high_24h/low_24h (USYC 1.140213 against [1.14, 1.14]). is_anomaly therefore
// allows AnomalyTolerancePct percent around the range; 0 is the strict check.
func TestMarketsAnomalyTolerance(t *testing.T) {
	cases := []struct {
		name  string
		price string
		tol   float64
		want  bool
	}{
		{"precision=full price at rounded bounds, 0.5%", "1.140213033752708", 0.5, false},
		{"exactly on the widened high, 0.5%", "1.1457", 0.5, false}, // 1.14 * 1.005
		{"just above the widened high, 0.5%", "1.14570001", 0.5, true},
		{"2% above the high, 0.5%", "1.1628", 0.5, true}, // 1.14 * 1.02
		{"2% below the low, 0.5%", "1.1172", 0.5, true},
		{"precision=full price at rounded bounds, strict", "1.140213033752708", 0, true},
		{"on the bound, strict", "1.14", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`[{"id":"usyc","symbol":"usyc","name":"USYC","current_price":` + tc.price + `,"total_volume":5,
			  "high_24h":1.14,"low_24h":1.14,"last_updated":"2026-10-10T10:07:55.000Z"}]`)
			rules := testRules
			rules.AnomalyTolerancePct = tc.tol
			recs, err := Transform(rawEvent(event.EndpointMarkets, map[string]string{"vs_currency": "usd"}, payload), rules)
			if err != nil {
				t.Fatal(err)
			}
			if got := recs[0].Business.(MarketSnapshot).IsAnomaly; got != tc.want {
				t.Fatalf("is_anomaly = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTransformDeterministic(t *testing.T) {
	ev := fixtureEvent(t, event.EndpointMarkets, "markets_usd250_t0.json")
	serialize := func() []byte {
		var buf bytes.Buffer
		for _, r := range mustTransform(t, ev) {
			row, err := RowJSON(r, testLineage(3))
			if err != nil {
				t.Fatal(err)
			}
			buf.Write(row)
			buf.WriteByte('\n')
			buf.Write(r.ContentSHA256[:])
		}
		return buf.Bytes()
	}
	if a, b := serialize(), serialize(); !bytes.Equal(a, b) {
		t.Fatal("two runs produced different bytes")
	}
}

func TestContentHashIgnoresLineage(t *testing.T) {
	ev1 := fixtureEvent(t, event.EndpointMarkets, "markets_top10_t0.json")
	ev2 := ev1
	ev2.EventID = "0199d0a4-0000-7000-8000-000000000000"
	ev2.FetchedAt = "2026-10-10T10:08:47.000Z"
	r1, r2 := mustTransform(t, ev1), mustTransform(t, ev2)
	for i := range r1 {
		if r1[i].ContentSHA256 != r2[i].ContentSHA256 {
			t.Fatalf("hash of %s depends on event metadata", r1[i].Key)
		}
	}
	// The cached t0/t1 responses are byte-identical, so their records are equal content.
	t1 := mustTransform(t, fixtureEvent(t, event.EndpointMarkets, "markets_top10_t1.json"))
	for i := range r1 {
		if r1[i].Key != t1[i].Key || r1[i].ContentSHA256 != t1[i].ContentSHA256 {
			t.Fatalf("t0/t1 record %d differs: %s vs %s", i, r1[i].Key, t1[i].Key)
		}
	}
}

func TestMarketsRejects(t *testing.T) {
	payload := []byte(`[
	  {"id":"","symbol":"x","name":"X","current_price":1,"last_updated":"2026-10-10T10:07:55.000Z"},
	  {"symbol":"y","name":"Y","current_price":1,"last_updated":"2026-10-10T10:07:55.000Z"},
	  {"id":"nullprice","symbol":"np","name":"NP","current_price":null,"last_updated":"2026-10-10T10:07:55.000Z"},
	  {"id":"zeroprice","symbol":"zp","name":"ZP","current_price":0,"last_updated":"2026-10-10T10:07:55.000Z"},
	  {"id":"good","symbol":"gd","name":"","current_price":0.5,"total_volume":0,"high_24h":0.4,"low_24h":0.3,
	   "last_updated":"2026-10-10T09:00:00.000Z","image":"https://dropped"}
	]`)
	recs := mustTransform(t, rawEvent(event.EndpointMarkets, map[string]string{"vs_currency": "usd"}, payload))
	want := []struct {
		zone   Zone
		reason string
		key    string
	}{
		// rawEvent sets no idempotency key: the event id stands for the response.
		{ZoneRejected, ReasonMissingID, "#0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11|0|2026-10-10T10:07:55.000Z"},
		{ZoneRejected, ReasonMissingID, "#0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11|1|2026-10-10T10:07:55.000Z"},
		{ZoneRejected, ReasonNullPrice, "nullprice|2026-10-10T10:07:55.000Z"},
		{ZoneRejected, ReasonNonpositivePrice, "zeroprice|2026-10-10T10:07:55.000Z"},
		{ZoneProcessed, "", "good|2026-10-10T10:07:55.000Z"},
	}
	if len(recs) != len(want) {
		t.Fatalf("records = %d", len(recs))
	}
	for i, w := range want {
		if recs[i].Zone != w.zone || recs[i].Reason != w.reason || recs[i].Key != w.key {
			t.Errorf("record %d = %s/%s/%s, want %s/%s/%s", i, recs[i].Zone, recs[i].Reason, recs[i].Key, w.zone, w.reason, w.key)
		}
	}
	good := recs[4].Business.(MarketSnapshot)
	if !good.IsStale || !good.IsInactive || !good.IsAnomaly || good.Name != nil || *good.Symbol != "GD" {
		t.Errorf("good row flags/normalization wrong: %+v", good)
	}
	validateProcessed(t, loadRegistry(t), recs)
}

func TestMarketsUndecodable(t *testing.T) {
	for _, p := range []string{`{"not":"an array"}`, `[{"id":"x","last_updated":"yesterday"}]`, `[]`, `[{"id":"x","current_price":"abc","last_updated":"2026-10-10T10:07:55Z"}]`} {
		if _, err := Transform(rawEvent(event.EndpointMarkets, nil, []byte(p)), testRules); err == nil {
			t.Errorf("payload %s: expected error", p)
		}
	}
	if _, err := Transform(rawEvent("bogus", nil, []byte(`[]`)), testRules); err == nil {
		t.Error("unknown endpoint: expected error")
	}
}

func TestSnapshotAtMarkets(t *testing.T) {
	got, err := SnapshotAt(fixtureEvent(t, event.EndpointMarkets, "markets_usd250_t0.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Format("2006-01-02T15:04:05.000Z07:00") != "2026-10-10T10:07:55.000Z" {
		t.Fatalf("SnapshotAt = %v", got)
	}
}

func TestSchemaIDFor(t *testing.T) {
	if SchemaIDFor(DatasetMarketSnapshot) != validation.IDMarketSnapshot || SchemaIDFor("nope") != "" {
		t.Fatal("SchemaIDFor mapping wrong")
	}
}

// A snapshot assembled across midnight: some coins were recomputed at
// 23:59:58, the rest at 00:00:05. Every row of the message takes snapshot_at =
// max(last_updated), so the whole snapshot lands in one event_date (the later
// day) and keys stay stable; nothing is split across two dates and nothing is
// flagged stale because of the date change.
func TestMarketsSnapshotSpanningMidnight(t *testing.T) {
	payload := []byte(`[
	  {"id":"early","symbol":"e","name":"Early","current_price":1,"total_volume":5,"high_24h":2,"low_24h":0.5,"last_updated":"2026-10-10T23:59:58.000Z"},
	  {"id":"late","symbol":"l","name":"Late","current_price":1,"total_volume":5,"high_24h":2,"low_24h":0.5,"last_updated":"2026-10-11T00:00:05.000Z"}
	]`)
	recs := mustTransform(t, rawEvent(event.EndpointMarkets, map[string]string{"vs_currency": "usd"}, payload))
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	for _, r := range recs {
		ms := r.Business.(MarketSnapshot)
		if r.EventDate != "2026-10-11" || ms.SnapshotAt != "2026-10-11T00:00:05.000Z" || r.Zone != ZoneProcessed {
			t.Errorf("%s: event_date=%s snapshot_at=%s zone=%s, want 2026-10-11 / 2026-10-11T00:00:05.000Z / processed",
				r.Key, r.EventDate, ms.SnapshotAt, r.Zone)
		}
		if ms.IsStale {
			t.Errorf("%s: flagged stale across midnight (7 s behind the snapshot)", r.Key)
		}
	}
	if recs[0].Key != "early|2026-10-11T00:00:05.000Z" {
		t.Errorf("key = %s", recs[0].Key)
	}
	if got := *recs[0].Business.(MarketSnapshot).SourceUpdatedAt; got != "2026-10-10T23:59:58.000Z" {
		t.Errorf("source_updated_at = %s, want the coin's own last_updated", got)
	}
	validateProcessed(t, loadRegistry(t), recs)
}
