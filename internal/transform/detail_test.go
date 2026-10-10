package transform

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"coingecko-etl/internal/event"
)

// byDataset groups records and counts zones per dataset.
func byDataset(recs []Record) map[string][]Record {
	m := map[string][]Record{}
	for _, r := range recs {
		m[r.Dataset] = append(m[r.Dataset], r)
	}
	return m
}

func zoneCount(recs []Record, z Zone) int {
	n := 0
	for _, r := range recs {
		if r.Zone == z {
			n++
		}
	}
	return n
}

func TestDetailBitcoin(t *testing.T) {
	recs := mustTransform(t, fixtureEvent(t, event.EndpointCoinDetail, "coin_bitcoin_full_t0.json"))
	for i, r := range recs {
		if r.Ordinal != i || r.EventDate != "2026-10-10" {
			t.Fatalf("record %d: ordinal %d date %s", i, r.Ordinal, r.EventDate)
		}
	}
	ds := byDataset(recs)

	parents := ds[DatasetCoinSnapshot]
	if len(parents) != 1 || parents[0].Zone != ZoneProcessed || parents[0].Key != "bitcoin|2026-10-10T10:01:51.000Z" {
		t.Fatalf("coin_snapshot = %+v", parents)
	}
	p := parents[0].Business.(CoinSnapshot)
	if *p.Symbol != "BTC" || *p.GenesisDate != "2009-01-03" || *p.HashingAlgorithm != "SHA-256" ||
		*p.MarketCapRank != 1 || len(p.Categories) != 8 || p.CirculatingSupply != "20096053" ||
		p.MaxSupply != "21000000" || p.MaxSupplyInfinite == nil || *p.MaxSupplyInfinite || p.IsStale {
		t.Errorf("coin_snapshot business = %+v", p)
	}

	cur := ds[DatasetCoinMarketByCurrency]
	if len(cur) != 63 || zoneCount(cur, ZoneProcessed) != 63 {
		t.Fatalf("coin_market_by_currency = %d (processed %d), want 63", len(cur), zoneCount(cur, ZoneProcessed))
	}
	for i := 1; i < len(cur); i++ {
		a, b := cur[i-1].Business.(CoinMarketByCurrency), cur[i].Business.(CoinMarketByCurrency)
		if a.Currency >= b.Currency {
			t.Fatalf("currencies not sorted: %s >= %s", a.Currency, b.Currency)
		}
	}
	var usd CoinMarketByCurrency
	for _, r := range cur {
		if c := r.Business.(CoinMarketByCurrency); c.Currency == "usd" {
			usd = c
			if r.Key != "bitcoin|usd|2026-10-10T10:01:51.000Z" {
				t.Errorf("usd key = %s", r.Key)
			}
		}
	}
	if usd.Price == "" || usd.ATHAt == nil || usd.PriceChangePct1y == nil || usd.MarketCapChangePct24h == nil {
		t.Errorf("usd row incomplete: %+v", usd)
	}

	tickers := ds[DatasetTicker]
	if len(tickers) != 100 || zoneCount(tickers, ZoneProcessed) != 100 {
		t.Fatalf("tickers = %d (processed %d), want 100", len(tickers), zoneCount(tickers, ZoneProcessed))
	}
	first := tickers[0].Business.(Ticker)
	if tickers[0].Key != "bitcoin|coinup|BTC|USDT|2026-10-10T10:01:51.000Z" ||
		first.LastUSD != "82809" || first.LastBTC != "0.9995798" || *first.ExchangeName != "CoinUp.io" ||
		*first.TickerTimestamp != "2026-10-10T10:00:45.000Z" || first.TrustScore != nil ||
		*first.BaseCoinID != "bitcoin" || *first.TargetCoinID != "tether" || first.IsStale || first.IsAnomaly {
		t.Errorf("first ticker = %s %+v", tickers[0].Key, first)
	}
	for _, r := range tickers {
		if tk := r.Business.(Ticker); tk.CoinID != "bitcoin" {
			t.Errorf("ticker coin_id = %s, want parent id bitcoin", tk.CoinID)
		}
	}

	contracts := ds[DatasetCoinContract]
	if len(contracts) != 1 || contracts[0].Zone != ZoneRejected || contracts[0].Reason != ReasonEmptyPlatform {
		t.Fatalf("bitcoin contracts = %+v", contracts)
	}
	if len(recs) != 1+63+100+1 {
		t.Fatalf("total records = %d", len(recs))
	}
}

func TestDetailUSDCoinMeta(t *testing.T) {
	recs := mustTransform(t, fixtureEvent(t, event.EndpointCoinDetail, "coin_usd-coin_meta.json"))
	ds := byDataset(recs)
	if len(ds[DatasetCoinMarketByCurrency]) != 0 || len(ds[DatasetTicker]) != 0 {
		t.Fatalf("usd-coin meta has no market_data/tickers, got %d/%d", len(ds[DatasetCoinMarketByCurrency]), len(ds[DatasetTicker]))
	}
	p := ds[DatasetCoinSnapshot][0].Business.(CoinSnapshot)
	if p.GenesisDate != nil || p.HashingAlgorithm != nil || p.CirculatingSupply != "" || *p.Symbol != "USDC" ||
		*p.SourceUpdatedAt != "2026-10-10T10:07:55.000Z" {
		t.Errorf("usd-coin snapshot = %+v", p)
	}
	contracts := ds[DatasetCoinContract]
	if len(contracts) != 36 || zoneCount(contracts, ZoneProcessed) != 36 {
		t.Fatalf("usd-coin contracts = %d (processed %d), want 36", len(contracts), zoneCount(contracts, ZoneProcessed))
	}
	got := map[string]CoinContract{}
	for i, r := range contracts {
		c := r.Business.(CoinContract)
		got[c.PlatformID] = c
		if i > 0 && contracts[i-1].Business.(CoinContract).PlatformID >= c.PlatformID {
			t.Fatal("contracts not sorted by platform_id")
		}
		if r.Key != "usd-coin|"+c.PlatformID+"|2026-10-10T10:07:55.000Z" {
			t.Errorf("contract key = %s", r.Key)
		}
	}
	if c := got["ethereum"]; c.ContractAddress != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" || *c.DecimalPlace != 6 {
		t.Errorf("ethereum contract = %+v", c)
	}
	if c := got["solana"]; c.ContractAddress != "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
		t.Errorf("solana address must keep its case: %+v", c)
	}
	if c := got["algorand"]; *c.DecimalPlace != 5 {
		t.Errorf("algorand decimal_place = %v", c.DecimalPlace)
	}
}

func TestDetailRowsValidateAgainstSchemas(t *testing.T) {
	reg := loadRegistry(t)
	for _, name := range []string{"coin_bitcoin_full_t0.json", "coin_bitcoin_full_t1.json", "coin_usd-coin_meta.json"} {
		t.Run(name, func(t *testing.T) {
			validateProcessed(t, reg, mustTransform(t, fixtureEvent(t, event.EndpointCoinDetail, name)))
		})
	}
}

func TestDetailDeterministic(t *testing.T) {
	ev := fixtureEvent(t, event.EndpointCoinDetail, "coin_bitcoin_full_t0.json")
	serialize := func(ev event.RawEvent) []byte {
		var buf bytes.Buffer
		for _, r := range mustTransform(t, ev) {
			row, err := RowJSON(r, testLineage(11))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&buf, "%s %s %d %s %x\n", r.Dataset, r.Key, r.Ordinal, row, r.ContentSHA256)
		}
		return buf.Bytes()
	}
	a, b := serialize(ev), serialize(ev)
	if !bytes.Equal(a, b) {
		t.Fatal("two runs produced different bytes")
	}
	// t1 is a byte-identical cached response: same records, same hashes.
	if c := serialize(fixtureEvent(t, event.EndpointCoinDetail, "coin_bitcoin_full_t1.json")); !bytes.Equal(a, c) {
		t.Fatal("t0 and t1 differ")
	}
}

// tickerJSON builds one minimal ticker element.
func tickerJSON(exchange, base, target string, lastUSD string, upstreamAnomaly bool, lastTraded string) string {
	return fmt.Sprintf(`{"base":%q,"target":%q,"market":{"name":%q,"identifier":%q,"has_trading_incentive":false},
	  "last":1,"volume":1,"converted_last":{"usd":%s,"btc":1,"eth":1},"converted_volume":{"usd":1,"btc":1,"eth":1},
	  "trust_score":"green","bid_ask_spread_percentage":0.1,"timestamp":%q,"last_traded_at":%q,"last_fetch_at":%q,
	  "is_anomaly":%t,"is_stale":false,"coin_id":"bitcoin","target_coin_id":"tether"}`,
		base, target, exchange, exchange, lastUSD, lastTraded, lastTraded, lastTraded, upstreamAnomaly)
}

func detailPayload(tickers []string, extra string) []byte {
	return []byte(`{"id":"bitcoin","symbol":"btc","name":"Bitcoin","categories":[],"hashing_algorithm":"",
	  "genesis_date":null,"market_cap_rank":1,"platforms":{},"detail_platforms":{},
	  "last_updated":"2026-10-10T10:01:51.000Z"` + extra + `,"tickers":[` + strings.Join(tickers, ",") + `]}`)
}

func TestTickerMedianAnomaly(t *testing.T) {
	const ts = "2026-10-10T10:00:00+00:00"
	tickers := []string{
		tickerJSON("a", "btc", "usdt", "100", false, ts),                     // median of {99,100,101,101,102,110} is 101 → |100-101|/101 = 0.99%
		tickerJSON("b", "btc", "usdt", "101", false, ts),                     // 0%
		tickerJSON("c", "btc", "usdt", "99", false, ts),                      // 1.98% → not anomalous (≤ 2%)
		tickerJSON("d", "btc", "usdt", "102", true, ts),                      // upstream flag → anomalous
		tickerJSON("e", "btc", "usdt", "110", false, ts),                     // 8.9% → anomalous
		tickerJSON("f", "btc", "usdt", "null", false, ts),                    // no last_usd → only upstream flag counts
		tickerJSON("g", "btc", "usdt", "101", false, "2026-10-10T08:59:00Z"), // trade > 1 h before snapshot → stale
	}
	recs := mustTransform(t, rawEvent(event.EndpointCoinDetail, nil, detailPayload(tickers, "")))
	want := map[string][2]bool{ // exchange → {is_anomaly, is_stale}
		"a": {false, false}, "b": {false, false}, "c": {false, false}, "d": {true, false},
		"e": {true, false}, "f": {false, false}, "g": {false, true},
	}
	n := 0
	for _, r := range recs {
		if r.Dataset != DatasetTicker {
			continue
		}
		n++
		tk := r.Business.(Ticker)
		w := want[tk.ExchangeID]
		if tk.IsAnomaly != w[0] || tk.IsStale != w[1] {
			t.Errorf("ticker %s: anomaly=%v stale=%v, want %v", tk.ExchangeID, tk.IsAnomaly, tk.IsStale, w)
		}
		if tk.Base != "BTC" || tk.Target != "USDT" {
			t.Errorf("ticker %s: base/target not upper-cased: %s/%s", tk.ExchangeID, tk.Base, tk.Target)
		}
	}
	if n != 7 {
		t.Fatalf("tickers = %d, want 7", n)
	}
	validateProcessed(t, loadRegistry(t), recs)
}

func TestTickerDuplicateKeysKeepOrdinals(t *testing.T) {
	const ts = "2026-10-10T10:00:00+00:00"
	tickers := []string{
		tickerJSON("binance", "BTC", "USDT", "100", false, ts),
		tickerJSON("binance", "btc", "usdt", "100", false, ts),
		tickerJSON("binance", "0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48", "usdt", "100", false, ts),
		tickerJSON("", "BTC", "USDT", "100", false, ts),
	}
	recs := mustTransform(t, rawEvent(event.EndpointCoinDetail, nil, detailPayload(tickers, "")))
	var tk []Record
	for _, r := range recs {
		if r.Dataset == DatasetTicker {
			tk = append(tk, r)
		}
	}
	if tk[0].Key != tk[1].Key || tk[0].Ordinal == tk[1].Ordinal || tk[0].ContentSHA256 != tk[1].ContentSHA256 {
		t.Fatalf("duplicate tickers must share key and hash but keep distinct ordinals: %+v %+v", tk[0], tk[1])
	}
	if tk[2].Key != "bitcoin|binance|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48|USDT|2026-10-10T10:01:51.000Z" {
		t.Errorf("EVM base not lower-cased: %s", tk[2].Key)
	}
	if tk[3].Zone != ZoneRejected || tk[3].Reason != ReasonMissingID {
		t.Errorf("ticker without exchange id: %s/%s", tk[3].Zone, tk[3].Reason)
	}
}

func TestCurrencyRejects(t *testing.T) {
	md := `,"market_data":{
	  "current_price":{"usd":1,"eur":null,"jpy":0,"xyz":null},
	  "market_cap":{"usd":10,"eur":9,"jpy":1000,"xyz":null},
	  "total_volume":{"usd":0,"eur":1,"jpy":1,"xyz":null},
	  "high_24h":{"usd":0.9,"eur":1,"jpy":1},
	  "low_24h":{"usd":0.8,"eur":1,"jpy":1},
	  "ath_date":{"usd":"2025-10-06T10:57:42.000Z","xyz":null},
	  "total_supply":null,"max_supply":null,"circulating_supply":1.5e3,"max_supply_infinite":true,
	  "last_updated":"2026-10-10T09:00:00.000Z"}`
	recs := mustTransform(t, rawEvent(event.EndpointCoinDetail, nil, detailPayload(nil, md)))
	ds := byDataset(recs)
	got := map[string]Record{}
	for _, r := range ds[DatasetCoinMarketByCurrency] {
		got[r.Business.(CoinMarketByCurrency).Currency] = r
	}
	check := func(cur string, zone Zone, reason string) {
		t.Helper()
		if r := got[cur]; r.Zone != zone || r.Reason != reason {
			t.Errorf("%s: %s/%s, want %s/%s", cur, r.Zone, r.Reason, zone, reason)
		}
	}
	check("eur", ZoneRejected, ReasonNullPrice)
	check("jpy", ZoneRejected, ReasonNonpositivePrice)
	check("usd", ZoneProcessed, "")
	check("xyz", ZoneRejected, ReasonEmptyRecord)
	usd := got["usd"].Business.(CoinMarketByCurrency)
	if !usd.IsInactive || !usd.IsAnomaly || *usd.ATHAt != "2025-10-06T10:57:42.000Z" {
		t.Errorf("usd flags: %+v", usd)
	}
	p := ds[DatasetCoinSnapshot][0].Business.(CoinSnapshot)
	if !p.IsStale || p.CirculatingSupply != "1500" || p.Categories != nil || p.HashingAlgorithm != nil ||
		*p.SourceUpdatedAt != "2026-10-10T09:00:00.000Z" || !*p.MaxSupplyInfinite {
		t.Errorf("parent = %+v", p)
	}
	validateProcessed(t, loadRegistry(t), recs)
}

func TestDetailMissingIDAndContracts(t *testing.T) {
	recs := mustTransform(t, rawEvent(event.EndpointCoinDetail, nil,
		[]byte(`{"id":"","symbol":"x","last_updated":"2026-10-10T10:01:51Z","tickers":[{"base":"X"}]}`)))
	if len(recs) != 1 || recs[0].Dataset != DatasetCoinSnapshot || recs[0].Reason != ReasonMissingID {
		t.Fatalf("missing id: %+v", recs)
	}
	recs = mustTransform(t, rawEvent(event.EndpointCoinDetail, nil, []byte(`{"id":"tok","symbol":"tok",
	  "last_updated":"2026-10-10T10:01:51Z",
	  "platforms":{"ethereum":"0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48","solana":"","":""},
	  "detail_platforms":{"solana":{"decimal_place":9,"contract_address":""},"base":{"decimal_place":null,"contract_address":"0xAB"}}}`)))
	var reasons []string
	for _, r := range recs {
		if r.Dataset == DatasetCoinContract {
			c := r.Business.(CoinContract)
			reasons = append(reasons, c.PlatformID+"="+string(r.Zone)+":"+r.Reason+":"+c.ContractAddress)
		}
	}
	want := "=rejected:empty_platform:,base=processed::0xAB,ethereum=processed::0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,solana=rejected:empty_platform:"
	if strings.Join(reasons, ",") != want {
		t.Fatalf("contracts = %s\nwant      %s", strings.Join(reasons, ","), want)
	}
}

func TestSnapshotAtDetail(t *testing.T) {
	got, err := SnapshotAt(fixtureEvent(t, event.EndpointCoinDetail, "coin_bitcoin_full_t0.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Format("2006-01-02T15:04:05.000Z07:00") != "2026-10-10T10:01:51.000Z" {
		t.Fatalf("SnapshotAt = %v", got)
	}
}

func TestDetailUndecodable(t *testing.T) {
	for _, p := range []string{`[]`, `{"id":"x"}`, `{"id":"x","last_updated":"2026-10-10T10:01:51Z","market_data":{"current_price":{"usd":"abc"}}}`} {
		if _, err := Transform(rawEvent(event.EndpointCoinDetail, nil, []byte(p)), testRules); err == nil {
			t.Errorf("payload %s: expected error", p)
		}
	}
}

// Clocks of different systems are not synchronized (spec §6.1): an exchange
// may report a trade after CoinGecko's snapshot time. A trade "from the
// future" is kept as reported, is not stale and is not rejected.
func TestTickerTradeAfterSnapshotIsKept(t *testing.T) {
	tickers := []string{
		tickerJSON("ahead", "btc", "usdt", "101", false, "2026-10-10T10:06:51+00:00"), // 5 min after snapshot_at 10:01:51
		tickerJSON("behind", "btc", "usdt", "101", false, "2026-10-10T10:01:40+00:00"),
	}
	recs := mustTransform(t, rawEvent(event.EndpointCoinDetail, nil, detailPayload(tickers, "")))
	n := 0
	for _, r := range recs {
		if r.Dataset != DatasetTicker {
			continue
		}
		n++
		tk := r.Business.(Ticker)
		if r.Zone != ZoneProcessed || tk.IsStale || tk.IsAnomaly {
			t.Errorf("ticker %s: zone=%s stale=%v anomaly=%v, want processed/false/false", tk.ExchangeID, r.Zone, tk.IsStale, tk.IsAnomaly)
		}
		if tk.ExchangeID == "ahead" && (tk.LastTradedAt == nil || *tk.LastTradedAt != "2026-10-10T10:06:51.000Z") {
			t.Errorf("last_traded_at = %v, want the exchange's value normalized to UTC millis", tk.LastTradedAt)
		}
		if r.EventDate != "2026-10-10" || tk.SnapshotAt != "2026-10-10T10:01:51.000Z" {
			t.Errorf("ticker %s: event_date=%s snapshot_at=%s", tk.ExchangeID, r.EventDate, tk.SnapshotAt)
		}
	}
	if n != 2 {
		t.Fatalf("tickers = %d, want 2", n)
	}
	validateProcessed(t, loadRegistry(t), recs)
}
