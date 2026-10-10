package validation

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"coingecko-etl/schemas"
)

// Recorded live CoinGecko responses (2026-10-10), copied verbatim.
const samplesDir = "../../schemas/testdata/coingecko"

var recordedSamples = []struct {
	file string
	id   string
}{
	{"markets_usd250_t0.json", IDSourceMarkets},
	{"markets_top10_t0.json", IDSourceMarkets},
	{"markets_top10_t1.json", IDSourceMarkets},
	{"markets_top10_t2.json", IDSourceMarkets},
	{"coin_bitcoin_full_t0.json", IDSourceCoin},
	{"coin_bitcoin_full_t1.json", IDSourceCoin},
	{"coin_usd-coin_meta.json", IDSourceCoin},
}

func loadEmbedded(t *testing.T) *Registry {
	t.Helper()
	r, err := Load(schemas.FS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return r
}

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(samplesDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRecordedSamplesPassS0(t *testing.T) {
	r := loadEmbedded(t)
	for _, s := range recordedSamples {
		t.Run(s.file, func(t *testing.T) {
			vs, err := r.Validate(s.id, readSample(t, s.file))
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if len(vs) != 0 {
				t.Fatalf("recorded sample fails S0: %+v", vs)
			}
		})
	}
}

// mutate decodes a sample with UseNumber (so 2^53+ values keep their
// digits), applies f and re-encodes it.
func mutate(t *testing.T, sample []byte, f func(doc any)) []byte {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(sample))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	f(doc)
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func obj(v any) map[string]any { return v.(map[string]any) }
func arr(v any) []any          { return v.([]any) }

func TestMutatedSamplesFailS0(t *testing.T) {
	r := loadEmbedded(t)
	cases := []struct {
		name         string
		file, id     string
		mutate       func(doc any) // applied to the decoded sample
		raw          string        // used instead of file+mutate when set
		wantInstance string
		wantKeyword  string
	}{
		{
			name: "markets current_price as string", file: "markets_top10_t0.json", id: IDSourceMarkets,
			mutate:       func(d any) { obj(arr(d)[0])["current_price"] = "82776" },
			wantInstance: "/0/current_price", wantKeyword: "type",
		},
		{
			name: "markets missing id", file: "markets_top10_t0.json", id: IDSourceMarkets,
			mutate:       func(d any) { delete(obj(arr(d)[3]), "id") },
			wantInstance: "/3", wantKeyword: "required",
		},
		{
			name: "markets last_updated yesterday", file: "markets_top10_t0.json", id: IDSourceMarkets,
			mutate:       func(d any) { obj(arr(d)[0])["last_updated"] = "yesterday" },
			wantInstance: "/0/last_updated", wantKeyword: "format",
		},
		{
			name: "markets ath_date yesterday", file: "markets_top10_t0.json", id: IDSourceMarkets,
			mutate:       func(d any) { obj(arr(d)[1])["ath_date"] = "yesterday" },
			wantInstance: "/1/ath_date", wantKeyword: "format",
		},
		{
			name: "markets response is an object", id: IDSourceMarkets,
			raw:          `{"error":"coin not found"}`,
			wantInstance: "", wantKeyword: "type",
		},
		{
			name: "coin missing id", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { delete(obj(d), "id") },
			wantInstance: "", wantKeyword: "required",
		},
		{
			name: "coin last_updated yesterday", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { obj(d)["last_updated"] = "yesterday" },
			wantInstance: "/last_updated", wantKeyword: "format",
		},
		{
			name: "coin current_price.usd as string", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { obj(obj(obj(d)["market_data"])["current_price"])["usd"] = "82776" },
			wantInstance: "/market_data/current_price/usd", wantKeyword: "type",
		},
		{
			name: "coin ticker last_traded_at yesterday", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { obj(arr(obj(d)["tickers"])[5])["last_traded_at"] = "yesterday" },
			wantInstance: "/tickers/5/last_traded_at", wantKeyword: "format",
		},
		{
			name: "coin ticker without market", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { delete(obj(arr(obj(d)["tickers"])[0]), "market") },
			wantInstance: "/tickers/0", wantKeyword: "required",
		},
		{
			name: "coin genesis_date not a date", file: "coin_bitcoin_full_t0.json", id: IDSourceCoin,
			mutate:       func(d any) { obj(d)["genesis_date"] = "Jan 3 2009" },
			wantInstance: "/genesis_date", wantKeyword: "format",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var doc []byte
			if c.raw != "" {
				doc = []byte(c.raw)
			} else {
				doc = mutate(t, readSample(t, c.file), c.mutate)
			}
			vs, err := r.Validate(c.id, doc)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if len(vs) != 1 {
				t.Fatalf("want exactly 1 violation, got %d: %+v", len(vs), vs)
			}
			if vs[0].InstanceLocation != c.wantInstance || Keyword(vs[0]) != c.wantKeyword {
				t.Fatalf("got (%q, %s) %q, want (%q, %s)",
					vs[0].InstanceLocation, Keyword(vs[0]), vs[0].Message, c.wantInstance, c.wantKeyword)
			}
		})
	}
}

// Tolerated "absent" encodings must not fail S0: the transformer turns
// them into nulls or cleaning rejects.
func TestS0ToleratesAbsentEncodings(t *testing.T) {
	r := loadEmbedded(t)
	markets := mutate(t, readSample(t, "markets_top10_t0.json"), func(d any) {
		c := obj(arr(d)[0])
		c["current_price"] = nil // -> rejected: null_price
		c["id"] = ""             // -> rejected: missing_id
		c["roi"] = nil
		c["ath_date"] = ""
		c["market_cap_rank"] = nil
		c["brand_new_field"] = map[string]any{"x": 1} // upstream drift
	})
	if vs, err := r.Validate(IDSourceMarkets, markets); err != nil || len(vs) != 0 {
		t.Fatalf("markets: %v %+v", err, vs)
	}
	coin := mutate(t, readSample(t, "coin_bitcoin_full_t0.json"), func(d any) {
		c := obj(d)
		c["genesis_date"] = ""
		c["categories"] = []any{}
		delete(c, "community_data")
		obj(obj(c["market_data"])["current_price"])["usd"] = nil
		obj(arr(c["tickers"])[0])["last_traded_at"] = nil
	})
	if vs, err := r.Validate(IDSourceCoin, coin); err != nil || len(vs) != 0 {
		t.Fatalf("coin: %v %+v", err, vs)
	}
}
