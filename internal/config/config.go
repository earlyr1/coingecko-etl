// Package config loads the runtime configuration of every etl command from
// environment variables. An unset or blank variable means "use the default".
// Every command gets a value for every variable, but validates only the
// variables its own code path reads (commandKeys), so a bad setting of one
// stage never stops another.
package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"coingecko-etl/internal/window"
)

type Config struct {
	Service             string        // the command Load was called for (Cmd*)
	HTTPAddr            string        // ETL_HTTP_ADDR, default ":8080"
	PushgatewayURL      string        // ETL_PUSHGATEWAY_URL, default "http://pushgateway:9091" (trailing "/" trimmed)
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
	ClaimsRetentionDays int           // ETL_CLAIMS_RETENTION_DAYS, default 8; >= ceil(TopicRetention/24h)+1 (checked by the retention job)
	PipelineEpoch       string        // ETL_PIPELINE_EPOCH (uuid), default "00000000-0000-0000-0000-000000000001"
	CoinGeckoBaseURL    string        // ETL_COINGECKO_BASE_URL, default "https://api.coingecko.com/api/v3"
	CoinGeckoAPIKey     string        // COINGECKO_API_KEY, optional
	MarketsInterval     time.Duration // ETL_MARKETS_INTERVAL, default 30s
	DetailInterval      time.Duration // ETL_DETAIL_INTERVAL, default 30s
	DetailOffset        time.Duration // ETL_DETAIL_OFFSET, default 15s
	Watchlist           []string      // ETL_WATCHLIST, default "bitcoin,ethereum,tether,usd-coin"
	MarketsPerPage      int           // ETL_MARKETS_PER_PAGE, default 100
	MinRequestSpacing   time.Duration // ETL_MIN_REQUEST_SPACING, default 15s
	ExportInterval      time.Duration // ETL_EXPORT_INTERVAL, default 15s; shorter than window.PublishGrace
	ExportBatchMax      int           // ETL_EXPORT_BATCH_MAX, default 500; maximum rows per raw part
	Window              time.Duration // ETL_WINDOW, default 5m; whole minutes in [1m,60m] dividing 60m; aligned to the UTC hour
	TransformMaxRecords int           // ETL_TRANSFORM_MAX_RECORDS, default 500
	StaleAfter          time.Duration // ETL_STALE_AFTER, default 10m
	TickerStaleAfter    time.Duration // ETL_TICKER_STALE_AFTER, default 1h
	TickerAnomalyPct    float64       // ETL_TICKER_ANOMALY_PCT, default 2.0
	AnomalyTolerancePct float64       // ETL_ANOMALY_TOLERANCE_PCT, default 0.5 (0 = strict range check)
}

// The commands Load knows, as typed after "etl".
const (
	CmdInit        = "init"
	CmdFetcher     = "fetcher"
	CmdExporter    = "exporter"
	CmdTransformer = "transformer"
	CmdPublish     = "publish"
	CmdRetention   = "maintenance retention"
	CmdVacuum      = "maintenance vacuum"
)

var keysS3 = []string{"ETL_S3_ENDPOINT", "ETL_S3_ACCESS_KEY", "ETL_S3_SECRET_KEY", "ETL_S3_REGION"}

// commandKeys lists, per command, the variables its code path reads: the
// logger (ETL_LOG_DIR), the HTTP server of the long-running services, the
// Pushgateway of the batch jobs, its clients and its own settings. Load
// reports errors only for these; TestCommandKeys pins the sets.
var commandKeys = map[string][]string{
	CmdInit: append([]string{"ETL_LOG_DIR", "ETL_PG_DSN", "ETL_BROKERS", "ETL_TOPIC", "ETL_TOPIC_PARTITIONS",
		"ETL_TOPIC_RETENTION"}, keysS3...),
	CmdFetcher: {"ETL_LOG_DIR", "ETL_HTTP_ADDR", "ETL_PG_DSN", "ETL_COINGECKO_BASE_URL", "COINGECKO_API_KEY",
		"ETL_MIN_REQUEST_SPACING", "ETL_MARKETS_INTERVAL", "ETL_DETAIL_INTERVAL", "ETL_DETAIL_OFFSET",
		"ETL_WATCHLIST", "ETL_MARKETS_PER_PAGE"},
	CmdExporter: append([]string{"ETL_LOG_DIR", "ETL_HTTP_ADDR", "ETL_PG_DSN", "ETL_EXPORT_INTERVAL",
		"ETL_EXPORT_BATCH_MAX", "ETL_WINDOW"}, keysS3...),
	CmdTransformer: append([]string{"ETL_LOG_DIR", "ETL_HTTP_ADDR", "ETL_PG_DSN", "ETL_BROKERS", "ETL_TOPIC",
		"ETL_PIPELINE_EPOCH", "ETL_TRANSFORM_MAX_RECORDS", "ETL_STALE_AFTER", "ETL_TICKER_STALE_AFTER",
		"ETL_TICKER_ANOMALY_PCT", "ETL_ANOMALY_TOLERANCE_PCT"}, keysS3...),
	CmdPublish: append([]string{"ETL_LOG_DIR", "ETL_PUSHGATEWAY_URL", "ETL_BROKERS", "ETL_TOPIC", "ETL_WINDOW"}, keysS3...),
	// ETL_TOPIC_RETENTION only feeds the claims-retention guard.
	CmdRetention: {"ETL_LOG_DIR", "ETL_PUSHGATEWAY_URL", "ETL_PG_DSN", "ETL_CLAIMS_RETENTION_DAYS", "ETL_TOPIC_RETENTION"},
	CmdVacuum:    {"ETL_LOG_DIR", "ETL_PUSHGATEWAY_URL", "ETL_PG_DSN"},
}

// Load reads the environment for command cmd (Cmd*), which is copied into
// Config.Service. Every field gets a value: the variable's, or the default
// when the variable is unset, blank, or invalid but not read by cmd. Invalid
// variables cmd reads are reported together.
func Load(cmd string) (Config, error) {
	keys, ok := commandKeys[cmd]
	if !ok {
		return Config{}, fmt.Errorf("config: unknown command %q", cmd)
	}
	p := parser{check: map[string]bool{}, bad: map[string]bool{}}
	for _, k := range keys {
		p.check[k] = true
	}
	c := Config{
		Service:             cmd,
		HTTPAddr:            p.str("ETL_HTTP_ADDR", ":8080"),
		PushgatewayURL:      p.httpURL("ETL_PUSHGATEWAY_URL", "http://pushgateway:9091"),
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
		ClaimsRetentionDays: p.int("ETL_CLAIMS_RETENTION_DAYS", 8),
		PipelineEpoch:       p.uuid("ETL_PIPELINE_EPOCH", "00000000-0000-0000-0000-000000000001"),
		CoinGeckoBaseURL:    strings.TrimRight(p.str("ETL_COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"), "/"),
		CoinGeckoAPIKey:     p.str("COINGECKO_API_KEY", ""),
		MarketsInterval:     p.duration("ETL_MARKETS_INTERVAL", 30*time.Second, false),
		DetailInterval:      p.duration("ETL_DETAIL_INTERVAL", 30*time.Second, false),
		DetailOffset:        p.duration("ETL_DETAIL_OFFSET", 15*time.Second, true),
		Watchlist:           p.list("ETL_WATCHLIST", "bitcoin,ethereum,tether,usd-coin"),
		MarketsPerPage:      p.int("ETL_MARKETS_PER_PAGE", 100),
		MinRequestSpacing:   p.duration("ETL_MIN_REQUEST_SPACING", 15*time.Second, true),
		ExportInterval:      p.duration("ETL_EXPORT_INTERVAL", 15*time.Second, false),
		ExportBatchMax:      p.int("ETL_EXPORT_BATCH_MAX", 500),
		Window:              p.windowSize("ETL_WINDOW", 5*time.Minute),
		TransformMaxRecords: p.int("ETL_TRANSFORM_MAX_RECORDS", 500),
		StaleAfter:          p.duration("ETL_STALE_AFTER", 10*time.Minute, false),
		TickerStaleAfter:    p.duration("ETL_TICKER_STALE_AFTER", time.Hour, false),
		TickerAnomalyPct:    p.float("ETL_TICKER_ANOMALY_PCT", 2.0),
		AnomalyTolerancePct: p.nonNegFloat("ETL_ANOMALY_TOLERANCE_PCT", 0.5),
	}
	// Claims must outlive every message the topic still holds, so a replay
	// from the earliest offset still finds them. Only the retention job reads
	// both; checked only when both parsed, so a bad input reports one error.
	if p.valid("ETL_CLAIMS_RETENTION_DAYS", "ETL_TOPIC_RETENTION") {
		const day = 24 * time.Hour
		minDays := int((c.TopicRetention+day-1)/day) + 1 // 168h -> 8, 169h -> 9, 24h -> 2
		if c.ClaimsRetentionDays < minDays {
			p.errs = append(p.errs, fmt.Errorf("ETL_CLAIMS_RETENTION_DAYS=%d: must be at least %d (ETL_TOPIC_RETENTION=%s in whole days, rounded up, plus 1)",
				c.ClaimsRetentionDays, minDays, c.TopicRetention))
		}
	}
	// The exporter must close a window before the publish run reads it, one
	// grace after the window end.
	if p.valid("ETL_EXPORT_INTERVAL") && c.ExportInterval >= window.PublishGrace {
		p.errs = append(p.errs, fmt.Errorf("ETL_EXPORT_INTERVAL=%s: must be shorter than the publisher's %s grace", c.ExportInterval, window.PublishGrace))
	}
	if len(p.errs) > 0 {
		return Config{}, fmt.Errorf("config: %w", errors.Join(p.errs...))
	}
	return c, nil
}

// parser collects every bad variable the command reads instead of stopping
// at the first one. A bad value is replaced by the default, so a variable the
// command does not read can never leave a broken field behind.
type parser struct {
	check map[string]bool // variables the command reads: their errors are reported
	bad   map[string]bool // variables whose value was invalid
	errs  []error
}

func (p *parser) raw(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (p *parser) fail(key, val, why string) {
	p.bad[key] = true
	if p.check[key] {
		p.errs = append(p.errs, fmt.Errorf("%s=%q: %s", key, val, why))
	}
}

// valid reports whether the command reads every key and each parsed: the
// condition of a cross-field check.
func (p *parser) valid(keys ...string) bool {
	return !slices.ContainsFunc(keys, func(k string) bool { return !p.check[k] || p.bad[k] })
}

func (p *parser) str(key, def string) string {
	if v, ok := p.raw(key); ok {
		return v
	}
	return def
}

func (p *parser) list(key, def string) []string {
	v := p.str(key, def)
	if out := splitList(v); len(out) > 0 {
		return out
	}
	p.fail(key, v, "must contain at least one comma-separated value")
	return splitList(def)
}

func splitList(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
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
	default:
		return d
	}
	return def
}

// windowSize parses ETL_WINDOW (named so it does not read like the window package).
func (p *parser) windowSize(key string, def time.Duration) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, v, "not a Go duration (examples: 1m, 5m, 15m)")
		return def
	}
	if err := window.Validate(d); err != nil {
		p.fail(key, v, err.Error())
		return def
	}
	return d
}

// httpURL parses an absolute http(s) URL with a host; the trailing "/" is trimmed.
func (p *parser) httpURL(key, def string) string {
	v := p.str(key, def)
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.fail(key, v, "must be an absolute http(s) URL, e.g. http://pushgateway:9091")
		v = def
	}
	return strings.TrimRight(v, "/")
}

func (p *parser) int(key string, def int) int {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		p.fail(key, v, "must be a positive integer")
		return def
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
		return def
	}
	return int32(n)
}

// float parses a positive, finite number. ParseFloat accepts "NaN" and
// "Inf", which pass a plain f <= 0 check.
func (p *parser) float(key string, def float64) float64 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || !(f > 0) || math.IsInf(f, 0) {
		p.fail(key, v, "must be a positive number")
		return def
	}
	return f
}

// nonNegFloat parses a finite number >= 0.
func (p *parser) nonNegFloat(key string, def float64) float64 {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || !(f >= 0) || math.IsInf(f, 0) {
		p.fail(key, v, "must be a number >= 0")
		return def
	}
	return f
}

func (p *parser) uuid(key, def string) string {
	v := p.str(key, def)
	u, err := uuid.Parse(v)
	if err != nil {
		p.fail(key, v, "must be a UUID")
		return def
	}
	return u.String()
}
