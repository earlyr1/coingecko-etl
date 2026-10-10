// Package config loads the runtime configuration of every etl subcommand from
// environment variables. An unset or blank variable means "use the default".
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	Service             string
	HTTPAddr            string        // ETL_HTTP_ADDR, default ":8080"
	LogDir              string        // ETL_LOG_DIR, default "logs"
	PostgresDSN         string        // ETL_PG_DSN, default "postgres://etl:etl@localhost:5432/etl?sslmode=disable"
	S3Endpoint          string        // ETL_S3_ENDPOINT, default "http://localhost:7070"
	S3AccessKey         string        // ETL_S3_ACCESS_KEY, default "etl"
	S3SecretKey         string        // ETL_S3_SECRET_KEY, default "etlsecret123"
	S3Region            string        // ETL_S3_REGION, default "us-east-1"
	Brokers             []string      // ETL_BROKERS (comma-separated), default "localhost:19092"
	Topic               string        // ETL_TOPIC, default "coingecko.raw"
	TopicPartitions     int32         // ETL_TOPIC_PARTITIONS, default 6
	TopicRetention      time.Duration // ETL_TOPIC_RETENTION, default 168h
	PipelineEpoch       string        // ETL_PIPELINE_EPOCH (uuid), default "00000000-0000-0000-0000-000000000001"
	CoinGeckoBaseURL    string        // ETL_COINGECKO_BASE_URL, default "https://api.coingecko.com/api/v3"
	CoinGeckoAPIKey     string        // COINGECKO_API_KEY, optional
	MarketsInterval     time.Duration // ETL_MARKETS_INTERVAL, default 30s
	DetailInterval      time.Duration // ETL_DETAIL_INTERVAL, default 60s
	DetailOffset        time.Duration // ETL_DETAIL_OFFSET, default 15s
	Watchlist           []string      // ETL_WATCHLIST, default "bitcoin,ethereum,tether,usd-coin"
	MarketsPerPage      int           // ETL_MARKETS_PER_PAGE, default 100
	MinRequestSpacing   time.Duration // ETL_MIN_REQUEST_SPACING, default 15s
	ExportInterval      time.Duration // ETL_EXPORT_INTERVAL, default 15s
	ExportBatchMax      int           // ETL_EXPORT_BATCH_MAX, default 500
	PublishLease        time.Duration // ETL_PUBLISH_LEASE, default 2m
	PublishPoll         time.Duration // ETL_PUBLISH_POLL, default 5s
	TransformMaxRecords int           // ETL_TRANSFORM_MAX_RECORDS, default 500
	StaleAfter          time.Duration // ETL_STALE_AFTER, default 10m
	TickerStaleAfter    time.Duration // ETL_TICKER_STALE_AFTER, default 1h
	TickerAnomalyPct    float64       // ETL_TICKER_ANOMALY_PCT, default 2.0
}

// Load reads the environment. service names the subcommand ("fetcher", ...) and is
// copied into Config.Service. All invalid variables are reported together.
func Load(service string) (Config, error) {
	var p parser
	c := Config{
		Service:             service,
		HTTPAddr:            p.str("ETL_HTTP_ADDR", ":8080"),
		LogDir:              p.str("ETL_LOG_DIR", "logs"),
		PostgresDSN:         p.str("ETL_PG_DSN", "postgres://etl:etl@localhost:5432/etl?sslmode=disable"),
		S3Endpoint:          p.str("ETL_S3_ENDPOINT", "http://localhost:7070"),
		S3AccessKey:         p.str("ETL_S3_ACCESS_KEY", "etl"),
		S3SecretKey:         p.str("ETL_S3_SECRET_KEY", "etlsecret123"),
		S3Region:            p.str("ETL_S3_REGION", "us-east-1"),
		Brokers:             p.list("ETL_BROKERS", "localhost:19092"),
		Topic:               p.str("ETL_TOPIC", "coingecko.raw"),
		TopicPartitions:     p.int32("ETL_TOPIC_PARTITIONS", 6),
		TopicRetention:      p.duration("ETL_TOPIC_RETENTION", 168*time.Hour, false),
		PipelineEpoch:       p.uuid("ETL_PIPELINE_EPOCH", "00000000-0000-0000-0000-000000000001"),
		CoinGeckoBaseURL:    strings.TrimRight(p.str("ETL_COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"), "/"),
		CoinGeckoAPIKey:     p.str("COINGECKO_API_KEY", ""),
		MarketsInterval:     p.duration("ETL_MARKETS_INTERVAL", 30*time.Second, false),
		DetailInterval:      p.duration("ETL_DETAIL_INTERVAL", 60*time.Second, false),
		DetailOffset:        p.duration("ETL_DETAIL_OFFSET", 15*time.Second, true),
		Watchlist:           p.list("ETL_WATCHLIST", "bitcoin,ethereum,tether,usd-coin"),
		MarketsPerPage:      p.int("ETL_MARKETS_PER_PAGE", 100),
		MinRequestSpacing:   p.duration("ETL_MIN_REQUEST_SPACING", 15*time.Second, true),
		ExportInterval:      p.duration("ETL_EXPORT_INTERVAL", 15*time.Second, false),
		ExportBatchMax:      p.int("ETL_EXPORT_BATCH_MAX", 500),
		PublishLease:        p.duration("ETL_PUBLISH_LEASE", 2*time.Minute, false),
		PublishPoll:         p.duration("ETL_PUBLISH_POLL", 5*time.Second, false),
		TransformMaxRecords: p.int("ETL_TRANSFORM_MAX_RECORDS", 500),
		StaleAfter:          p.duration("ETL_STALE_AFTER", 10*time.Minute, false),
		TickerStaleAfter:    p.duration("ETL_TICKER_STALE_AFTER", time.Hour, false),
		TickerAnomalyPct:    p.float("ETL_TICKER_ANOMALY_PCT", 2.0),
	}
	if len(p.errs) > 0 {
		return Config{}, fmt.Errorf("config: %w", errors.Join(p.errs...))
	}
	return c, nil
}

// parser collects every bad variable instead of stopping at the first one.
type parser struct{ errs []error }

func (p *parser) raw(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (p *parser) fail(key, val, why string) {
	p.errs = append(p.errs, fmt.Errorf("%s=%q: %s", key, val, why))
}

func (p *parser) str(key, def string) string {
	if v, ok := p.raw(key); ok {
		return v
	}
	return def
}

func (p *parser) list(key, def string) []string {
	v := p.str(key, def)
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		p.fail(key, v, "must contain at least one comma-separated value")
	}
	return out
}

func (p *parser) duration(key string, def time.Duration, allowZero bool) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		p.fail(key, v, "not a Go duration (examples: 30s, 2m, 168h)")
	case d < 0 || (d == 0 && !allowZero):
		p.fail(key, v, "must be positive")
	}
	return d
}

func (p *parser) int(key string, def int) int {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		p.fail(key, v, "must be a positive integer")
	}
	return n
}

func (p *parser) int32(key string, def int32) int32 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		p.fail(key, v, "must be a positive 32-bit integer")
	}
	return int32(n)
}

func (p *parser) float(key string, def float64) float64 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		p.fail(key, v, "must be a positive number")
	}
	return f
}

func (p *parser) uuid(key, def string) string {
	v := p.str(key, def)
	u, err := uuid.Parse(v)
	if err != nil {
		p.fail(key, v, "must be a UUID")
		return v
	}
	return u.String()
}
