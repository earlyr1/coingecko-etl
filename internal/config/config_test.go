package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var allKeys = []string{
	"ETL_HTTP_ADDR", "ETL_LOG_DIR", "ETL_PG_DSN", "ETL_S3_ENDPOINT", "ETL_S3_ACCESS_KEY",
	"ETL_S3_SECRET_KEY", "ETL_S3_REGION", "ETL_BROKERS", "ETL_TOPIC", "ETL_TOPIC_PARTITIONS",
	"ETL_TOPIC_RETENTION", "ETL_PIPELINE_EPOCH", "ETL_COINGECKO_BASE_URL", "COINGECKO_API_KEY",
	"ETL_MARKETS_INTERVAL", "ETL_DETAIL_INTERVAL", "ETL_DETAIL_OFFSET", "ETL_WATCHLIST",
	"ETL_MARKETS_PER_PAGE", "ETL_MIN_REQUEST_SPACING", "ETL_EXPORT_INTERVAL", "ETL_EXPORT_BATCH_MAX",
	"ETL_PUBLISH_LEASE", "ETL_PUBLISH_POLL", "ETL_TRANSFORM_MAX_RECORDS", "ETL_STALE_AFTER",
	"ETL_TICKER_STALE_AFTER", "ETL_TICKER_ANOMALY_PCT", "ETL_ANOMALY_TOLERANCE_PCT",
}

// clearEnv blanks every variable Load reads; blank means "use the default".
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	got, err := Load("fetcher")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		Service:             "fetcher",
		HTTPAddr:            ":8080",
		LogDir:              "logs",
		PostgresDSN:         "postgres://etl:etl@localhost:5432/etl?sslmode=disable",
		S3Endpoint:          "http://localhost:7070",
		S3AccessKey:         "etl",
		S3SecretKey:         "etlsecret123",
		S3Region:            "us-east-1",
		Brokers:             []string{"localhost:19092"},
		Topic:               "coingecko.raw",
		TopicPartitions:     6,
		TopicRetention:      168 * time.Hour,
		PipelineEpoch:       "00000000-0000-0000-0000-000000000001",
		CoinGeckoBaseURL:    "https://api.coingecko.com/api/v3",
		CoinGeckoAPIKey:     "",
		MarketsInterval:     30 * time.Second,
		DetailInterval:      30 * time.Second,
		DetailOffset:        15 * time.Second,
		Watchlist:           []string{"bitcoin", "ethereum", "tether", "usd-coin"},
		MarketsPerPage:      100,
		MinRequestSpacing:   15 * time.Second,
		ExportInterval:      15 * time.Second,
		ExportBatchMax:      500,
		PublishLease:        2 * time.Minute,
		PublishPoll:         5 * time.Second,
		TransformMaxRecords: 500,
		StaleAfter:          10 * time.Minute,
		TickerStaleAfter:    time.Hour,
		TickerAnomalyPct:    2.0,
		AnomalyTolerancePct: 0.5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	env := map[string]string{
		"ETL_HTTP_ADDR":             ":9090",
		"ETL_LOG_DIR":               "/var/log/etl",
		"ETL_PG_DSN":                "postgres://u:p@db:5432/x?sslmode=disable",
		"ETL_S3_ENDPOINT":           "http://versitygw:7070",
		"ETL_S3_ACCESS_KEY":         "ak",
		"ETL_S3_SECRET_KEY":         "sk",
		"ETL_S3_REGION":             "eu-central-1",
		"ETL_BROKERS":               " redpanda:9092 , other:9092 ,",
		"ETL_TOPIC":                 "t.v1",
		"ETL_TOPIC_PARTITIONS":      "12",
		"ETL_TOPIC_RETENTION":       "24h",
		"ETL_PIPELINE_EPOCH":        "0192A3B4-C5D6-7E8F-9A0B-1C2D3E4F5A6B",
		"ETL_COINGECKO_BASE_URL":    "http://stub:8000/api/v3/",
		"COINGECKO_API_KEY":         "CG-demo",
		"ETL_MARKETS_INTERVAL":      "10s",
		"ETL_DETAIL_INTERVAL":       "2m",
		"ETL_DETAIL_OFFSET":         "0s",
		"ETL_WATCHLIST":             "bitcoin",
		"ETL_MARKETS_PER_PAGE":      "250",
		"ETL_MIN_REQUEST_SPACING":   "1500ms",
		"ETL_EXPORT_INTERVAL":       "1s",
		"ETL_EXPORT_BATCH_MAX":      "10",
		"ETL_PUBLISH_LEASE":         "30s",
		"ETL_PUBLISH_POLL":          "1s",
		"ETL_TRANSFORM_MAX_RECORDS": "50",
		"ETL_STALE_AFTER":           "5m",
		"ETL_TICKER_STALE_AFTER":    "2h",
		"ETL_TICKER_ANOMALY_PCT":    "3.5",
		"ETL_ANOMALY_TOLERANCE_PCT": "1.25",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	c, err := Load("exporter")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"Service", c.Service, "exporter"},
		{"HTTPAddr", c.HTTPAddr, ":9090"},
		{"LogDir", c.LogDir, "/var/log/etl"},
		{"PostgresDSN", c.PostgresDSN, "postgres://u:p@db:5432/x?sslmode=disable"},
		{"S3Endpoint", c.S3Endpoint, "http://versitygw:7070"},
		{"S3AccessKey", c.S3AccessKey, "ak"},
		{"S3SecretKey", c.S3SecretKey, "sk"},
		{"S3Region", c.S3Region, "eu-central-1"},
		{"Brokers", c.Brokers, []string{"redpanda:9092", "other:9092"}},
		{"Topic", c.Topic, "t.v1"},
		{"TopicPartitions", c.TopicPartitions, int32(12)},
		{"TopicRetention", c.TopicRetention, 24 * time.Hour},
		{"PipelineEpoch", c.PipelineEpoch, "0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b"},
		{"CoinGeckoBaseURL", c.CoinGeckoBaseURL, "http://stub:8000/api/v3"},
		{"CoinGeckoAPIKey", c.CoinGeckoAPIKey, "CG-demo"},
		{"MarketsInterval", c.MarketsInterval, 10 * time.Second},
		{"DetailInterval", c.DetailInterval, 2 * time.Minute},
		{"DetailOffset", c.DetailOffset, time.Duration(0)},
		{"Watchlist", c.Watchlist, []string{"bitcoin"}},
		{"MarketsPerPage", c.MarketsPerPage, 250},
		{"MinRequestSpacing", c.MinRequestSpacing, 1500 * time.Millisecond},
		{"ExportInterval", c.ExportInterval, time.Second},
		{"ExportBatchMax", c.ExportBatchMax, 10},
		{"PublishLease", c.PublishLease, 30 * time.Second},
		{"PublishPoll", c.PublishPoll, time.Second},
		{"TransformMaxRecords", c.TransformMaxRecords, 50},
		{"StaleAfter", c.StaleAfter, 5 * time.Minute},
		{"TickerStaleAfter", c.TickerStaleAfter, 2 * time.Hour},
		{"TickerAnomalyPct", c.TickerAnomalyPct, 3.5},
		{"AnomalyTolerancePct", c.AnomalyTolerancePct, 1.25},
	}
	for _, ch := range checks {
		if !reflect.DeepEqual(ch.got, ch.want) {
			t.Errorf("%s = %#v, want %#v", ch.name, ch.got, ch.want)
		}
	}
}

func TestLoadInvalid(t *testing.T) {
	cases := []struct{ key, val string }{
		{"ETL_MARKETS_INTERVAL", "30"},          // no unit
		{"ETL_DETAIL_INTERVAL", "soon"},         // not a duration
		{"ETL_EXPORT_INTERVAL", "-5s"},          // negative
		{"ETL_PUBLISH_LEASE", "0s"},             // zero not allowed
		{"ETL_DETAIL_OFFSET", "-1s"},            // zero allowed, negative not
		{"ETL_TOPIC_PARTITIONS", "abc"},         // not an int
		{"ETL_TOPIC_PARTITIONS", "99999999999"}, // overflows int32
		{"ETL_EXPORT_BATCH_MAX", "0"},           // not positive
		{"ETL_TICKER_ANOMALY_PCT", "two"},       // not a float
		{"ETL_TICKER_ANOMALY_PCT", "NaN"},       // parses, but is no threshold
		{"ETL_TICKER_ANOMALY_PCT", "+Inf"},      // parses, but is no threshold
		{"ETL_ANOMALY_TOLERANCE_PCT", "-0.5"},   // negative
		{"ETL_ANOMALY_TOLERANCE_PCT", "NaN"},    // not a number
		{"ETL_ANOMALY_TOLERANCE_PCT", "Inf"},    // infinite
		{"ETL_ANOMALY_TOLERANCE_PCT", "half"},   // not a float
		{"ETL_PIPELINE_EPOCH", "not-a-uuid"},    // not a uuid
		{"ETL_WATCHLIST", " , ,"},               // no values
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.val, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(c.key, c.val)
			_, err := Load("fetcher")
			if err == nil {
				t.Fatalf("Load succeeded, want error for %s=%q", c.key, c.val)
			}
			if !strings.Contains(err.Error(), c.key) {
				t.Fatalf("error %q does not name %s", err, c.key)
			}
		})
	}
}

// ETL_ANOMALY_TOLERANCE_PCT=0 is allowed: it means the strict range check.
func TestLoadAnomalyToleranceZeroIsStrict(t *testing.T) {
	clearEnv(t)
	t.Setenv("ETL_ANOMALY_TOLERANCE_PCT", "0")
	c, err := Load("transformer")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.AnomalyTolerancePct != 0 {
		t.Fatalf("AnomalyTolerancePct = %v, want 0", c.AnomalyTolerancePct)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv("ETL_MARKETS_INTERVAL", "x")
	t.Setenv("ETL_MARKETS_PER_PAGE", "y")
	_, err := Load("fetcher")
	if err == nil {
		t.Fatal("Load succeeded, want error")
	}
	for _, key := range []string{"ETL_MARKETS_INTERVAL", "ETL_MARKETS_PER_PAGE"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not name %s", err, key)
		}
	}
}
