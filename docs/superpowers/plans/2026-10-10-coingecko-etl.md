# CoinGecko ETL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go ETL pipeline that polls CoinGecko, stores raw API calls in Postgres, exports them as raw JSONL to an S3-compatible lake, streams them through Redpanda, and writes cleaned, deduplicated, normalized JSONL to the processed zone with exactly-once content.

**Architecture:** One Go binary `etl` with subcommands (`init`, `fetcher`, `exporter`, `publisher`, `transformer`, `healthcheck`). Postgres holds raw API calls (`raw` schema) and pipeline state (`pipeline` schema). versitygw exposes `./data` over S3 as plain files. Redpanda (Kafka protocol) carries one message per API call. Every hop is at-least-once; the processed zone is effectively-once through deterministic keys, Postgres ledgers and write-once objects (`If-None-Match: *`).

**Tech Stack:** Go 1.27, franz-go v1.22.1 (+kadm v1.19.0, kprom v1.5.0), pgx v5.11.0, goose v3.28.0, santhosh-tekuri/jsonschema v6.0.3, aws-sdk-go-v2 (service/s3), google/uuid, shopspring/decimal, prometheus/client_golang, testcontainers-go; postgres:18.6, redpandadata/redpanda:v26.2.4, versity/versitygw:v1.8.0, Prometheus, Grafana, redpandadata/console:v3.12.0.

**Spec:** `docs/superpowers/specs/2026-10-10-coingecko-etl-design.md` — read it before any task.

## Global Constraints

- Go module path: `coingecko-etl`. Go directive `go 1.27`. Build with `CGO_ENABLED=0`.
- Pinned versions: `github.com/twmb/franz-go v1.22.1`, `github.com/twmb/franz-go/pkg/kadm v1.19.0`, `github.com/twmb/franz-go/plugin/kprom v1.5.0`, `github.com/jackc/pgx/v5 v5.11.0`, `github.com/pressly/goose/v3 v3.28.0`, `github.com/santhosh-tekuri/jsonschema/v6 v6.0.3`. Other modules: latest at implementation time, pinned in go.mod.
- Images: `postgres:18.6`, `redpandadata/redpanda:v26.2.4`, `versity/versitygw:v1.8.0`, `redpandadata/console:v3.12.0`, runtime `gcr.io/distroless/static-debian13:nonroot`, builder `golang:1.27`.
- Postgres volume mount path is `/var/lib/postgresql` (never `/var/lib/postgresql/data`). Healthcheck `pg_isready -h 127.0.0.1`.
- versitygw runs the posix backend with `--sidecar` metadata outside the gateway root; buckets `raw`, `processed`, `rejected`, `dlq` are directories under `./data`.
- Topic `coingecko.raw`, 6 partitions, retention 7 days, auto-create off. Message key: `coins_markets` for markets, the coin id for detail.
- Timestamps everywhere: RFC 3339 UTC with milliseconds, e.g. `2026-10-10T10:07:55.000Z` (`xtime.Format`).
- Money and quantities are decimal strings in plain notation (no exponent); percentages and ratios are JSON numbers. Never decode money through float64.
- Postgres `raw.api_calls.payload` is type `json` (byte-exact), never `jsonb`.
- No `now()`/`time.Now()` inside transforms or serialization; time comes from messages or the batch manifest.
- Never hold a Postgres transaction across S3 or broker I/O.
- Logs: `log/slog` JSON, UTC, written to `logs/<service>/etl.log` and stdout.
- Every long-running service serves `/healthz`, `/readyz`, `/metrics` on `:8080`.
- CoinGecko request budget: ~3 calls/min, one request in flight, honor `Retry-After`, skip ticks instead of queueing.
- Commit after every task with a message ending in `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.

## Review Focus

- CoinGecko answers HTTP 200 with a body that encoding/json accepts but strict JSON rejects (duplicate member names, invalid UTF-8) or a bare `null` — the call is stored as `http_error` with no payload and starts a backoff cooldown, instead of becoming an `ok` event that fails S1 and is silently not inserted; test `TestFetchRejectsNonStrictJSONBody` (pinned by Task 6, Step 5)
- Clock skew between hosts: an exchange reports `last_traded_at` 5 minutes after CoinGecko's `snapshot_at` — the ticker row stays in processed/ with the reported time normalized to UTC millis, `is_stale=false`, not rejected, event_date = snapshot date; test `TestTickerTradeAfterSnapshotIsKept` (pinned by Task 13, Step 1)
- Midnight snapshot spanning two event_dates (coins stamped 23:59:58 and 00:00:05 in one markets response) — every row takes `snapshot_at = max(last_updated)`, so the whole snapshot lands in the later event_date with stable keys and no stale flag caused by the date change; test `TestMarketsSnapshotSpanningMidnight` (pinned by Task 12, Step 6)
- versitygw restarted or unreachable in the middle of a transformer batch — `ProcessBatch` returns an error, the manifest row stays `planned` so no offset is committed, and the retry after recovery writes exactly the object keys TX1 stored with no duplicate rows; subtest "object storage down mid-batch" of `TestProcessBatch` (pinned by Task 15, Step 4)
- A replayed raw object or late snapshot carries an event_date outside the `dedup_claims` partition window (today-1 .. today+3) — `ProcessBatch` recognizes SQLSTATE 23514 without a constraint name, creates that day's partitions and writes the batch instead of failing forever; subtest "snapshot dated outside the dedup partition window creates its partition" (pinned by Task 15, Step 4)

---

## File Structure

```
go.mod, go.sum
Makefile
Dockerfile
docker-compose.yml
.env.example
.github/workflows/ci.yml
README.md
docs/adr/0001-*.md …
cmd/etl/main.go                      subcommand dispatch
internal/config/config.go            env → Config
internal/xtime/xtime.go              Format / Parse (all CoinGecko time formats)
internal/obs/logger.go               slog to file + stdout
internal/obs/metrics.go              all etl_* metric vectors
internal/obs/server.go               /healthz /readyz /metrics server
internal/event/event.go              RawEvent (S1) type, endpoints, idempotency key
internal/validation/registry.go      JSON Schema registry + Violation
internal/validation/ids.go           schema $id constants
internal/coingecko/client.go         HTTP client → event.RawEvent, Retry-After
internal/coingecko/limiter.go        single-flight + spacing + cooldown limiter
internal/storage/pg/pg.go            Connect, Migrate, EnsurePartitions
internal/storage/pg/apicalls.go      InsertAPICall
internal/storage/s3/s3.go            Client: PutIfAbsent, Get, EnsureBuckets, Ping
internal/queue/queue.go              franz-go client builders, EnsureTopic, TopicID, headers
internal/fetcher/fetcher.go          scheduler + insert
internal/exporter/exporter.go        xid8 cursor, claim, serialize, put, mark
internal/publisher/publisher.go      lease, read, validate, produce, mark
internal/transform/…                 pure transforms (markets.go, detail.go, normalize.go, types.go, hash.go)
internal/dedup/dedup.go              claims + decisions
internal/transformer/manifest.go     consumer_batches SQL
internal/transformer/batch.go        ProcessBatch (TX1, PUT, TX2)
internal/transformer/service.go      consumer loop, assignment recovery, offset commit
internal/bootstrap/bootstrap.go      etl init
schemas/embed.go                     package schemas; //go:embed
schemas/common/defs.v1.json
schemas/source/coingecko/coins_markets.v1.json, coin.v1.json
schemas/pipeline/raw_event.v1.json, rejected_record.v1.json, dlq_record.v1.json
schemas/processed/market_snapshot.v1.json, coin_snapshot.v1.json, coin_market_by_currency.v1.json, ticker.v1.json, coin_contract.v1.json
schemas/testdata/…                   fixtures (valid/invalid + recorded CoinGecko responses)
migrations/embed.go                  package migrations; //go:embed *.sql
migrations/00001_raw.sql, 00002_pipeline.sql
deploy/prometheus/prometheus.yml, alerts.yml
deploy/grafana/provisioning/…, dashboards/etl.json
internal/testutil/containers.go      testcontainers helpers (Postgres, Redpanda, versitygw)
```

## Interface Contract (authoritative for all tasks)

All tasks MUST use these exact names and signatures. Code inside a task may add unexported helpers but must not rename anything below.

```go
// internal/xtime
func Format(t time.Time) string                 // UTC, "2006-01-02T15:04:05.000Z"
func Parse(s string) (time.Time, error)         // accepts RFC3339 with Z or offset, with/without fraction
func FormatDate(t time.Time) string             // "2006-01-02" (UTC)

// internal/config
type Config struct {
    Service            string
    HTTPAddr           string        // ETL_HTTP_ADDR, default ":8080"
    LogDir             string        // ETL_LOG_DIR, default "logs"
    PostgresDSN        string        // ETL_PG_DSN, default "postgres://etl:etl@localhost:5432/etl?sslmode=disable"
    S3Endpoint         string        // ETL_S3_ENDPOINT, default "http://localhost:7070"
    S3AccessKey        string        // ETL_S3_ACCESS_KEY, default "etl"
    S3SecretKey        string        // ETL_S3_SECRET_KEY, default "etlsecret123"
    S3Region           string        // ETL_S3_REGION, default "us-east-1"
    Brokers            []string      // ETL_BROKERS (comma-separated), default "localhost:19092"
    Topic              string        // ETL_TOPIC, default "coingecko.raw"
    TopicPartitions    int32         // ETL_TOPIC_PARTITIONS, default 6
    TopicRetention     time.Duration // ETL_TOPIC_RETENTION, default 168h
    PipelineEpoch      string        // ETL_PIPELINE_EPOCH (uuid), default "00000000-0000-0000-0000-000000000001"
    CoinGeckoBaseURL   string        // ETL_COINGECKO_BASE_URL, default "https://api.coingecko.com/api/v3"
    CoinGeckoAPIKey    string        // COINGECKO_API_KEY, optional
    MarketsInterval    time.Duration // ETL_MARKETS_INTERVAL, default 30s
    DetailInterval     time.Duration // ETL_DETAIL_INTERVAL, default 60s
    DetailOffset       time.Duration // ETL_DETAIL_OFFSET, default 15s
    Watchlist          []string      // ETL_WATCHLIST, default "bitcoin,ethereum,tether,usd-coin"
    MarketsPerPage     int           // ETL_MARKETS_PER_PAGE, default 100
    MinRequestSpacing  time.Duration // ETL_MIN_REQUEST_SPACING, default 15s
    ExportInterval     time.Duration // ETL_EXPORT_INTERVAL, default 15s
    ExportBatchMax     int           // ETL_EXPORT_BATCH_MAX, default 500
    PublishLease       time.Duration // ETL_PUBLISH_LEASE, default 2m
    PublishPoll        time.Duration // ETL_PUBLISH_POLL, default 5s
    TransformMaxRecords int          // ETL_TRANSFORM_MAX_RECORDS, default 500
    StaleAfter         time.Duration // ETL_STALE_AFTER, default 10m
    TickerStaleAfter   time.Duration // ETL_TICKER_STALE_AFTER, default 1h
    TickerAnomalyPct   float64       // ETL_TICKER_ANOMALY_PCT, default 2.0
}
func Load(service string) (Config, error)

// internal/obs
func NewLogger(service, logDir string) (*slog.Logger, io.Closer, error)
type Check func(ctx context.Context) error
type Health struct { /* unexported */ }
func NewHealth() *Health
func (h *Health) AddReadiness(name string, c Check)
type Metrics struct {
    APIRequests        *prometheus.CounterVec   // etl_api_requests_total{endpoint,status}
    APIRequestDuration *prometheus.HistogramVec // etl_api_request_duration_seconds{endpoint}
    APIRateLimited     prometheus.Counter       // etl_api_rate_limited_total
    LastSuccess        *prometheus.GaugeVec     // etl_last_success_timestamp_seconds{endpoint}
    SourceDataAge      *prometheus.GaugeVec     // etl_source_data_age_seconds{endpoint}
    SchemaValidations  *prometheus.CounterVec   // etl_schema_validations_total{stage,schema,result}
    SchemaViolations   *prometheus.CounterVec   // etl_schema_violations_total{stage,schema,keyword}
    ExportLag          prometheus.Gauge         // etl_export_lag_seconds
    ExportBatches      *prometheus.CounterVec   // etl_export_batches_total{result}
    S3Puts             *prometheus.CounterVec   // etl_s3_put_total{result}  result=created|exists|error|mismatch
    PublishedRecords   prometheus.Counter       // etl_published_records_total
    Records            *prometheus.CounterVec   // etl_records_total{dataset,outcome}
    DedupDropped       *prometheus.CounterVec   // etl_dedup_dropped_total{reason}
    Rejected           *prometheus.CounterVec   // etl_rejected_total{reason}
    DLQ                *prometheus.CounterVec   // etl_dlq_total{stage,error_class}
    ConsumerLag        *prometheus.GaugeVec     // etl_consumer_lag_records{partition}
    BatchDuration      prometheus.Histogram     // etl_batch_duration_seconds
    E2ELatency         prometheus.Histogram     // etl_e2e_latency_seconds
}
func NewMetrics(reg prometheus.Registerer) *Metrics
func Serve(ctx context.Context, addr string, h *Health, g prometheus.Gatherer) error // blocks until ctx done

// internal/event
const (
    SchemaVersion       = 1
    SourceCoinGecko     = "coingecko"
    EndpointMarkets     = "coins_markets"
    EndpointCoinDetail  = "coin_detail"
    OutcomeOK           = "ok"
    OutcomeHTTPError    = "http_error"
    OutcomeNetworkError = "network_error"
)
type Request struct {
    Path   string            `json:"path"`
    Params map[string]string `json:"params"`
}
type HTTPMeta struct {
    Status        int     `json:"status"`
    LatencyMS     int64   `json:"latency_ms"`
    Date          *string `json:"date"`
    AgeSeconds    *int    `json:"age_s"`
    CFCacheStatus *string `json:"cf_cache_status"`
    ETag          *string `json:"etag"`
    RetryAfterS   *int    `json:"retry_after_s"`
}
type RawEvent struct {
    SchemaVersion  int             `json:"schema_version"`
    EventID        string          `json:"event_id"`         // UUIDv7
    IdempotencyKey *string         `json:"idempotency_key"`  // nil unless outcome ok
    Source         string          `json:"source"`
    Endpoint       string          `json:"endpoint"`
    Request        Request         `json:"request"`
    FetchedAt      string          `json:"fetched_at"`       // xtime.Format
    Outcome        string          `json:"outcome"`
    HTTP           *HTTPMeta       `json:"http"`
    Error          *string         `json:"error"`
    PayloadSHA256  *string         `json:"payload_sha256"`   // hex
    Payload        json.RawMessage `json:"payload"`          // verbatim bytes; JSON null when absent
}
func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string
func SHA256Hex(b []byte) string
func Marshal(ev RawEvent) ([]byte, error)   // deterministic, payload embedded verbatim, no trailing newline
func Unmarshal(b []byte) (RawEvent, error)  // keeps Payload bytes verbatim

// internal/validation
type Violation struct {
    InstanceLocation string `json:"instance_location"`
    KeywordLocation  string `json:"keyword_location"`
    Message          string `json:"message"`
}
type Registry struct { /* unexported */ }
func Load(fsys fs.FS) (*Registry, error)                                  // compiles every *.json under fsys
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error) // error = not JSON / unknown id; max 20 violations
func Keyword(v Violation) string                                           // last token of KeywordLocation, for metric labels
const (
    IDRawEvent              = "https://schemas.cg-etl.internal/pipeline/raw_event.v1.json"
    IDRejectedRecord        = "https://schemas.cg-etl.internal/pipeline/rejected_record.v1.json"
    IDDLQRecord             = "https://schemas.cg-etl.internal/pipeline/dlq_record.v1.json"
    IDSourceMarkets         = "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"
    IDSourceCoin            = "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json"
    IDMarketSnapshot        = "https://schemas.cg-etl.internal/processed/market_snapshot.v1.json"
    IDCoinSnapshot          = "https://schemas.cg-etl.internal/processed/coin_snapshot.v1.json"
    IDCoinMarketByCurrency  = "https://schemas.cg-etl.internal/processed/coin_market_by_currency.v1.json"
    IDTicker                = "https://schemas.cg-etl.internal/processed/ticker.v1.json"
    IDCoinContract          = "https://schemas.cg-etl.internal/processed/coin_contract.v1.json"
)
// package schemas (repo root dir schemas/): var FS embed.FS  — contains common/, source/, pipeline/, processed/ (not testdata/)

// internal/coingecko
type Limiter struct { /* unexported */ }
func NewLimiter(minSpacing time.Duration) *Limiter
func (l *Limiter) Wait(ctx context.Context) error       // blocks until allowed; returns ctx.Err()
func (l *Limiter) Cooldown(d time.Duration)             // no request before now+d
func (l *Limiter) Allowed(now time.Time) bool           // non-blocking check used to skip ticks
type Client struct { /* unexported */ }
func NewClient(baseURL, apiKey string, hc *http.Client, l *Limiter, now func() time.Time) *Client
func MarketsRequest(perPage int) event.Request
func CoinDetailRequest(coinID string) event.Request
func (c *Client) Fetch(ctx context.Context, endpoint string, req event.Request) event.RawEvent // never errors; outcome encodes failures; on 429 calls l.Cooldown(Retry-After)

// internal/storage/pg
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)
func Migrate(ctx context.Context, pool *pgxpool.Pool) error                      // goose, embedded migrations, advisory lock
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error // raw.api_calls + pipeline.dedup_claims daily partitions [today-1, today+daysAhead]
func InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error // ON CONFLICT DO NOTHING
// package migrations (repo root dir migrations/): var FS embed.FS

// internal/storage/s3
const (BucketRaw = "raw"; BucketProcessed = "processed"; BucketRejected = "rejected"; BucketDLQ = "dlq")
var ErrChecksumMismatch = errors.New("s3: object exists with different checksum")
type Config struct { Endpoint, AccessKey, SecretKey, Region string }
type Client struct { /* unexported */ }
func New(ctx context.Context, cfg Config) (*Client, error)               // path-style, BaseEndpoint
func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (created bool, err error)
    // If-None-Match:*, metadata sha256=hex; on 412 HEAD and compare → (false,nil) or ErrChecksumMismatch
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error)
func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error
func (c *Client) Ping(ctx context.Context) error

// internal/queue
const (
    HeaderEventID        = "event_id"
    HeaderIdempotencyKey = "idempotency_key"
    HeaderSchema         = "schema"
    HeaderRawObject      = "raw_object"
    HeaderLineNo         = "line_no"
)
func MessageKey(ev event.RawEvent) string // "coins_markets" or the coin id from Request.Path
func NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error) // idempotent, acks=all
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error
func TopicID(ctx context.Context, cl *kgo.Client, topic string, fallback uuid.UUID) (uuid.UUID, error) // fallback when broker returns zero id

// internal/transform  (pure; no I/O, no clock)
type Zone string
const (ZoneProcessed Zone = "processed"; ZoneRejected Zone = "rejected")
type Rules struct { StaleAfter, TickerStaleAfter time.Duration; TickerAnomalyPct float64 }
type Lineage struct {
    EventID      string `json:"_event_id"`
    RawObject    string `json:"_raw_object"`
    Partition    int32  `json:"_kafka_partition"`
    Offset       int64  `json:"_kafka_offset"`
    SchemaVer    int    `json:"_schema_version"`
    ProcessedAt  string `json:"_processed_at"`
}
type Record struct {
    Dataset       string   // "market_snapshot" | "coin_snapshot" | "coin_market_by_currency" | "ticker" | "coin_contract"
    Key           string   // natural key, fields joined by "|"
    EventDate     string   // FormatDate(snapshot_at)
    Ordinal       int      // index within the message, assignment order
    Zone          Zone
    Reason        string   // reject reason when Zone == ZoneRejected
    ContentSHA256 [32]byte // over business fields only
    Business      any      // business-field struct (hashed)
}
func Transform(ev event.RawEvent, rules Rules) ([]Record, error) // dispatches on ev.Endpoint; error = payload not decodable
func SnapshotAt(ev event.RawEvent) (time.Time, error)
func RowJSON(r Record, lin Lineage) ([]byte, error)              // business fields + lineage, fixed field order, one line
func SchemaIDFor(dataset string) string                          // validation.ID* for processed datasets
type Dec string                                                  // decimal string, plain notation; JSON null when empty
func DecFromNumber(n json.Number) (Dec, error)

// internal/dedup
type Position struct { TopicID uuid.UUID; Partition int32; Offset int64; Ordinal int }
type Claim struct {
    Dataset, Key, EventDate string
    Pos            Position
    EventID        string
    IdempotencyKey string
    ContentSHA256  [32]byte
}
type Decision int
const (Emit Decision = iota; Drop; Conflict)
type Outcome struct { Decision Decision; DropReason string } // DropReason: "replay" | "duplicate_response" | "duplicate_record"
func ClaimAndDecide(ctx context.Context, tx pgx.Tx, claims []Claim) ([]Outcome, error) // same order as claims

// internal/transformer
type Message struct { Partition int32; Offset int64; Key, Value []byte; Headers map[string]string }
type Batch struct { TopicID uuid.UUID; Topic string; Partition int32; Messages []Message } // contiguous offsets
type Deps struct { Pool *pgxpool.Pool; S3 *s3.Client; Reg *validation.Registry; Metrics *obs.Metrics; Log *slog.Logger; Rules transform.Rules }
type Result struct { Written bool; LastOffset int64 }
func ProcessBatch(ctx context.Context, d Deps, b Batch) (Result, error)
func CoveringBatch(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, offset int64) (first, last int64, status string, found bool, err error)
func ObjectKey(zone, prefix, topic string, partition int32, firstOffset int64) string
func Run(ctx context.Context, cfg config.Config, d Deps) error

// internal/fetcher, exporter, publisher, bootstrap
func fetcher.Run(ctx context.Context, cfg config.Config, cg *coingecko.Client, lim *coingecko.Limiter, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) error
func exporter.ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, max int) (exported int, err error)
func exporter.Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
func publisher.PublishOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, topic string, lease time.Duration, m *obs.Metrics, log *slog.Logger) (published bool, err error)
func publisher.Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
func bootstrap.Run(ctx context.Context, cfg config.Config, log *slog.Logger) error

// internal/testutil (tests only; build tag none, package testutil)
func StartPostgres(t *testing.T) (dsn string)                        // postgres:18.6
func StartRedpanda(t *testing.T) (brokers []string)                  // redpanda v26.2.4
func StartVersityGW(t *testing.T) (endpoint, dataDir string)         // versitygw v1.8.0, posix + sidecar, creds etl/etlsecret123
func SkipIfNoDocker(t *testing.T)
```

## Object keys (exact)

- Raw: `source=coingecko/ingest_date=<YYYY-MM-DD of claim time UTC>/batch=<%020d batch_id>.jsonl` in bucket `raw`.
- Processed: `dataset=<dataset>/v=1/event_date=<date>/<topic>+<partition>+<%020d first_offset>.jsonl` in bucket `processed`.
- Rejected: `dataset=<dataset>/event_date=<date>/<topic>+<partition>+<%020d first_offset>.jsonl` in bucket `rejected`.
- DLQ: `stage=<stage>/date=<date>/<topic or "export"/"publish">+<partition or batch>+<%020d offset or batch_id>.jsonl` in bucket `dlq`.

## Task Index

Tasks 1–18 follow in order. Contract notes at the top of each part record decisions the writers made where the contract was silent.

CONTRACT NOTES

- None blocking. Clarifications chosen within the contract:
  - `event.Marshal` writes a nil `Request.Params` as `{}` (never `null`) so the envelope always satisfies an object-typed `params` in `raw_event.v1.json`. `Unmarshal` of that output yields an empty non-nil map.
  - `event.Unmarshal` maps a JSON `null` payload to a nil `Payload`. Leading/trailing whitespace *around* the payload value is not preserved by `encoding/json`. Internal whitespace and key order are preserved byte for byte. CoinGecko bodies have no outer whitespace.
  - `config.Load` treats a blank env var as unset. `ETL_DETAIL_OFFSET` and `ETL_MIN_REQUEST_SPACING` may be `0`; every other duration must be > 0. `ETL_PIPELINE_EPOCH` is validated as a UUID and stored in canonical lowercase form. A trailing `/` is trimmed from `ETL_COINGECKO_BASE_URL`.
  - `obs.NewMetrics` registers only the `etl_*` catalogue. The Go runtime and process collectors are registered by the caller (Task 16 in `cmd/etl`) on the same registry.
  - The Makefile also has a `demo-report` target (`demo` = `up` + `demo-report`), so the report can be re-run without restarting the stack. Task 17 may adjust the URLs and ports.
  - `make test-unit` runs `go test -short`. Integration tests from later tasks must skip under `testing.Short()` as well as through `testutil.SkipIfNoDocker`.

### Task 1: Project scaffold (go.mod, Makefile, xtime, config, obs, cmd/etl)

**Files:**
- Create: `go.mod`, `go.sum` (generated), `.gitignore`, `Makefile`
- Create: `internal/xtime/xtime.go`, Test: `internal/xtime/xtime_test.go`
- Create: `internal/config/config.go`, Test: `internal/config/config_test.go`
- Create: `internal/obs/logger.go`, Test: `internal/obs/logger_test.go`
- Create: `internal/obs/metrics.go`, Test: `internal/obs/metrics_test.go`
- Create: `internal/obs/server.go`, Test: `internal/obs/server_test.go`
- Create: `cmd/etl/main.go`, Test: `cmd/etl/main_test.go`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
```go
// internal/xtime
func Format(t time.Time) string                 // UTC, "2006-01-02T15:04:05.000Z"
func Parse(s string) (time.Time, error)         // accepts RFC3339 with Z or offset, with/without fraction
func FormatDate(t time.Time) string             // "2006-01-02" (UTC)

// internal/config
type Config struct {
    Service            string
    HTTPAddr           string        // ETL_HTTP_ADDR, default ":8080"
    LogDir             string        // ETL_LOG_DIR, default "logs"
    PostgresDSN        string        // ETL_PG_DSN, default "postgres://etl:etl@localhost:5432/etl?sslmode=disable"
    S3Endpoint         string        // ETL_S3_ENDPOINT, default "http://localhost:7070"
    S3AccessKey        string        // ETL_S3_ACCESS_KEY, default "etl"
    S3SecretKey        string        // ETL_S3_SECRET_KEY, default "etlsecret123"
    S3Region           string        // ETL_S3_REGION, default "us-east-1"
    Brokers            []string      // ETL_BROKERS (comma-separated), default "localhost:19092"
    Topic              string        // ETL_TOPIC, default "coingecko.raw"
    TopicPartitions    int32         // ETL_TOPIC_PARTITIONS, default 6
    TopicRetention     time.Duration // ETL_TOPIC_RETENTION, default 168h
    PipelineEpoch      string        // ETL_PIPELINE_EPOCH (uuid), default "00000000-0000-0000-0000-000000000001"
    CoinGeckoBaseURL   string        // ETL_COINGECKO_BASE_URL, default "https://api.coingecko.com/api/v3"
    CoinGeckoAPIKey    string        // COINGECKO_API_KEY, optional
    MarketsInterval    time.Duration // ETL_MARKETS_INTERVAL, default 30s
    DetailInterval     time.Duration // ETL_DETAIL_INTERVAL, default 60s
    DetailOffset       time.Duration // ETL_DETAIL_OFFSET, default 15s
    Watchlist          []string      // ETL_WATCHLIST, default "bitcoin,ethereum,tether,usd-coin"
    MarketsPerPage     int           // ETL_MARKETS_PER_PAGE, default 100
    MinRequestSpacing  time.Duration // ETL_MIN_REQUEST_SPACING, default 15s
    ExportInterval     time.Duration // ETL_EXPORT_INTERVAL, default 15s
    ExportBatchMax     int           // ETL_EXPORT_BATCH_MAX, default 500
    PublishLease       time.Duration // ETL_PUBLISH_LEASE, default 2m
    PublishPoll        time.Duration // ETL_PUBLISH_POLL, default 5s
    TransformMaxRecords int          // ETL_TRANSFORM_MAX_RECORDS, default 500
    StaleAfter         time.Duration // ETL_STALE_AFTER, default 10m
    TickerStaleAfter   time.Duration // ETL_TICKER_STALE_AFTER, default 1h
    TickerAnomalyPct   float64       // ETL_TICKER_ANOMALY_PCT, default 2.0
}
func Load(service string) (Config, error)

// internal/obs
func NewLogger(service, logDir string) (*slog.Logger, io.Closer, error)
type Check func(ctx context.Context) error
type Health struct { /* unexported */ }
func NewHealth() *Health
func (h *Health) AddReadiness(name string, c Check)
type Metrics struct {
    APIRequests        *prometheus.CounterVec   // etl_api_requests_total{endpoint,status}
    APIRequestDuration *prometheus.HistogramVec // etl_api_request_duration_seconds{endpoint}
    APIRateLimited     prometheus.Counter       // etl_api_rate_limited_total
    LastSuccess        *prometheus.GaugeVec     // etl_last_success_timestamp_seconds{endpoint}
    SourceDataAge      *prometheus.GaugeVec     // etl_source_data_age_seconds{endpoint}
    SchemaValidations  *prometheus.CounterVec   // etl_schema_validations_total{stage,schema,result}
    SchemaViolations   *prometheus.CounterVec   // etl_schema_violations_total{stage,schema,keyword}
    ExportLag          prometheus.Gauge         // etl_export_lag_seconds
    ExportBatches      *prometheus.CounterVec   // etl_export_batches_total{result}
    S3Puts             *prometheus.CounterVec   // etl_s3_put_total{result}  result=created|exists|error|mismatch
    PublishedRecords   prometheus.Counter       // etl_published_records_total
    Records            *prometheus.CounterVec   // etl_records_total{dataset,outcome}
    DedupDropped       *prometheus.CounterVec   // etl_dedup_dropped_total{reason}
    Rejected           *prometheus.CounterVec   // etl_rejected_total{reason}
    DLQ                *prometheus.CounterVec   // etl_dlq_total{stage,error_class}
    ConsumerLag        *prometheus.GaugeVec     // etl_consumer_lag_records{partition}
    BatchDuration      prometheus.Histogram     // etl_batch_duration_seconds
    E2ELatency         prometheus.Histogram     // etl_e2e_latency_seconds
}
func NewMetrics(reg prometheus.Registerer) *Metrics
func Serve(ctx context.Context, addr string, h *Health, g prometheus.Gatherer) error // blocks until ctx done
```
- CLI: `etl <init|fetcher|exporter|publisher|transformer|healthcheck>`. `healthcheck` works now. The other subcommands print `etl <cmd>: not implemented` and exit 1 until Task 16 wires them up.

- [ ] **Step 1: Create the module, .gitignore and dependencies**

The repository may not be a git repository yet. Initialize it if needed, then create `go.mod`:

```bash
cd /Users/ruslan/code/trust-wallet-test
git rev-parse --git-dir >/dev/null 2>&1 || git init -b main
```

`go.mod`:

```
module coingecko-etl

go 1.27
```

`.gitignore`:

```
/bin/
/data/
/logs/
/.s3meta/
/.env
*.test
*.out
```

Fetch the two direct dependencies of this task. Name the Prometheus *packages*, not just the module, so that `go.sum` gets every transitive entry. Otherwise the build reports "missing go.sum entry":

```bash
go get github.com/google/uuid@v1.6.0 \
  github.com/prometheus/client_golang/prometheus@v1.25.0 \
  github.com/prometheus/client_golang/prometheus/promauto@v1.25.0 \
  github.com/prometheus/client_golang/prometheus/promhttp@v1.25.0
```

(If newer releases exist when you implement this, `@latest` is fine. Pin whatever resolves.)

- [ ] **Step 2: Write the Makefile**

`demo`, `up` and `down` call `docker compose`. The compose file arrives in Task 17, so only `build`, `test`, `test-unit` and `lint` work right now. Recipe lines must start with a TAB character.

`Makefile`:

```make
SHELL := /bin/bash
GO ?= go
COMPOSE ?= docker compose
BIN := bin/etl

# Overridable endpoints used by `make demo` (ports are fixed in docker-compose.yml, Task 17).
PROMETHEUS_URL ?= http://localhost:9090
GRAFANA_URL ?= http://localhost:3000
CONSOLE_URL ?= http://localhost:8081
DEMO_WAIT ?= 180

.PHONY: build test test-unit lint up down demo demo-report

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/etl

# All tests, including Docker-backed integration tests (skipped when Docker is unavailable).
test:
	$(GO) test -race -count=1 ./...

# Unit tests only: integration tests skip themselves under -short.
test-unit:
	$(GO) test -short -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...

up:
	mkdir -p data logs .s3meta
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

demo: up demo-report

# demo-report prints the summary without (re)starting the stack.
demo-report:
	@echo "Waiting up to $(DEMO_WAIT)s for the first processed object..."
	@for i in $$(seq 1 $(DEMO_WAIT)); do \
	  if [ -n "$$(find data/processed -name '*.jsonl' 2>/dev/null | head -n 1)" ]; then break; fi; \
	  sleep 1; \
	done
	@printf '%-10s %8s %10s\n' zone objects records
	@for zone in raw processed rejected dlq; do \
	  objects=$$(find data/$$zone -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' '); \
	  records=$$(find data/$$zone -name '*.jsonl' -exec cat {} + 2>/dev/null | wc -l | tr -d ' '); \
	  printf '%-10s %8s %10s\n' "$$zone" "$$objects" "$$records"; \
	done
	@set -o pipefail; echo -n "duplicate share (dropped / (dropped + written)): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=sum(etl_dedup_dropped_total) / (sum(etl_dedup_dropped_total) + sum(etl_records_total{outcome="written"}))' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p' || echo "n/a (Prometheus not reachable)"
	@echo "Grafana:          $(GRAFANA_URL)"
	@echo "Redpanda Console: $(CONSOLE_URL)"
	@echo "Prometheus:       $(PROMETHEUS_URL)"
```

- [ ] **Step 3: Write the failing test for internal/xtime**

CoinGecko emits two timestamp shapes: `2026-10-10T10:02:30.123Z` (markets, `last_updated`, `ath_date`) and `2026-10-10T10:02:30+00:00` (tickers' `last_traded_at`, `last_fetch_at`). Both must parse. Output is always UTC with exactly 3 fractional digits.

`internal/xtime/xtime_test.go`:

```go
package xtime

import (
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	kyiv := time.FixedZone("UTC+3", 3*60*60)
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"utc whole second", time.Date(2026, 10, 10, 10, 7, 55, 0, time.UTC), "2026-10-10T10:07:55.000Z"},
		{"offset converted to utc", time.Date(2026, 10, 10, 13, 7, 55, 0, kyiv), "2026-10-10T10:07:55.000Z"},
		{"truncates below ms", time.Date(2026, 10, 10, 10, 7, 55, 123999999, time.UTC), "2026-10-10T10:07:55.123Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Format(c.in); got != c.want {
				t.Fatalf("Format = %q, want %q", got, c.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	want := time.Date(2026, 10, 10, 10, 2, 30, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Time
	}{
		{"2026-10-10T10:02:30.000Z", want},
		{"2026-10-10T10:02:30Z", want},
		{"2026-10-10T10:02:30+00:00", want},
		{"2026-10-10T13:02:30+03:00", want},
		{"2026-10-10T10:02:30.123Z", want.Add(123 * time.Millisecond)},
		{"2026-10-10T10:02:30.123456+00:00", want.Add(123456 * time.Microsecond)},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := Parse(c.in)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if !got.Equal(c.want) || got.Location() != time.UTC {
				t.Fatalf("Parse = %v (%v), want %v UTC", got, got.Location(), c.want)
			}
		})
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "2026-10-10", "2026-10-10 10:02:30", "yesterday"} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", in)
		}
	}
}

func TestFormatDate(t *testing.T) {
	late := time.Date(2026, 10, 10, 23, 30, 0, 0, time.FixedZone("UTC-2", -2*60*60))
	if got := FormatDate(late); got != "2026-10-11" {
		t.Fatalf("FormatDate = %q, want 2026-10-11", got)
	}
}
```

- [ ] **Step 4: Run it to confirm it fails**

Run: `go test ./internal/xtime/`
Expected: FAIL, a build error `undefined: Format` (and `Parse`, `FormatDate`).

- [ ] **Step 5: Implement internal/xtime**

Go's parser accepts an optional fractional second even when the layout has none, and `time.RFC3339Nano` takes both `Z` and `±hh:mm`. Formatting with `.000` truncates and does not round.

`internal/xtime/xtime.go`:

```go
// Package xtime holds the single time format used everywhere in the pipeline:
// RFC 3339, UTC, millisecond precision, e.g. 2026-10-10T10:07:55.000Z.
package xtime

import (
	"fmt"
	"time"
)

const (
	layout     = "2006-01-02T15:04:05.000Z"
	dateLayout = "2006-01-02"
)

// Format renders t in UTC with exactly three fractional digits (truncated, not rounded).
func Format(t time.Time) string {
	return t.UTC().Format(layout)
}

// Parse accepts every timestamp shape CoinGecko emits: "2026-10-10T10:02:30.123Z",
// "2026-10-10T10:02:30Z" and "2026-10-10T10:02:30+00:00" (with or without a fraction).
// The result is always in UTC.
func Parse(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("xtime: parse %q: %w", s, err)
	}
	return t.UTC(), nil
}

// FormatDate renders the UTC calendar date of t as YYYY-MM-DD.
func FormatDate(t time.Time) string {
	return t.UTC().Format(dateLayout)
}
```

Run: `go test ./internal/xtime/`
Expected: PASS (`ok  coingecko-etl/internal/xtime`).

- [ ] **Step 6: Write the failing test for internal/config**

`t.Setenv(k, "")` blanks every variable for the duration of the test, and Load treats blank as unset. This isolates the tests from the developer's shell environment.

`internal/config/config_test.go`:

```go
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
	"ETL_TICKER_STALE_AFTER", "ETL_TICKER_ANOMALY_PCT",
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
		DetailInterval:      60 * time.Second,
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
```

- [ ] **Step 7: Run it to confirm it fails**

Run: `go test ./internal/config/`
Expected: FAIL, a build error `undefined: Load` / `undefined: Config`.

- [ ] **Step 8: Implement internal/config**

The parser collects every bad variable and reports them together through `errors.Join`, so one failed start shows every mistake at once.

`internal/config/config.go`:

```go
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
```

Run: `go test ./internal/config/`
Expected: PASS.

- [ ] **Step 9: Write the failing test for the logger**

`internal/obs/logger_test.go`:

```go
package obs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLoggerWritesJSONToServiceFile(t *testing.T) {
	dir := t.TempDir()
	log, closer, err := NewLogger("fetcher", dir)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	log.Info("api call", "endpoint", "coins_markets", "status", 200)
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "fetcher", "etl.log"))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("want 1 log line, got %d: %q", len(lines), b)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v: %s", err, lines[0])
	}
	if rec["service"] != "fetcher" || rec["msg"] != "api call" || rec["endpoint"] != "coins_markets" || rec["level"] != "INFO" {
		t.Fatalf("unexpected record: %v", rec)
	}
	ts, _ := rec["time"].(string)
	// 2026-10-10T10:07:55.000Z: 24 chars, UTC "Z", millisecond precision.
	if len(ts) != 24 || !strings.HasSuffix(ts, "Z") || ts[19] != '.' {
		t.Fatalf("time %q is not UTC millisecond RFC 3339", ts)
	}
}

func TestNewLoggerAppends(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2; i++ {
		log, closer, err := NewLogger("exporter", dir)
		if err != nil {
			t.Fatalf("NewLogger: %v", err)
		}
		log.Info("run", "i", i)
		closer.Close()
	}
	b, _ := os.ReadFile(filepath.Join(dir, "exporter", "etl.log"))
	if n := strings.Count(string(b), "\n"); n != 2 {
		t.Fatalf("want 2 lines after reopening, got %d", n)
	}
}

func TestNewLoggerRejectsBadService(t *testing.T) {
	for _, s := range []string{"", "a/b", ".."} {
		if _, _, err := NewLogger(s, t.TempDir()); err == nil {
			t.Errorf("NewLogger(%q) succeeded, want error", s)
		}
	}
}
```

Run: `go test ./internal/obs/`
Expected: FAIL, a build error `undefined: NewLogger`.

- [ ] **Step 10: Implement the logger**

`slog.HandlerOptions.ReplaceAttr` rewrites the built-in `time` attribute. By default slog writes local time with nanoseconds, so the hook replaces it with `xtime.Format` (UTC, ms). `io.MultiWriter` duplicates each record into the file and stdout. Each container has its own file (`logs/<service>/etl.log`), so the file is never shared.

`internal/obs/logger.go`:

```go
// Package obs holds the observability plumbing shared by every etl service:
// structured logging, Prometheus metrics and the /healthz /readyz /metrics server.
package obs

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"coingecko-etl/internal/xtime"
)

// NewLogger returns a JSON slog logger that writes every record both to
// <logDir>/<service>/etl.log (appended, created if missing) and to stdout.
// Each record carries service=<service> and a UTC millisecond timestamp.
// The returned Closer closes the log file; call it on shutdown.
func NewLogger(service, logDir string) (*slog.Logger, io.Closer, error) {
	if service == "" || strings.ContainsAny(service, `/\`) || service == "." || service == ".." {
		return nil, nil, fmt.Errorf("obs: invalid service name %q", service)
	}
	dir := filepath.Join(logDir, service)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("obs: create log dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "etl.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("obs: open log file: %w", err)
	}
	h := slog.NewJSONHandler(io.MultiWriter(f, os.Stdout), &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// slog writes local time with nanoseconds by default; force the pipeline format.
			if len(groups) == 0 && a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
				return slog.String(slog.TimeKey, xtime.Format(a.Value.Time()))
			}
			return a
		},
	})
	return slog.New(h).With(slog.String("service", service)), f, nil
}
```

Run: `go test ./internal/obs/`
Expected: PASS (the JSON log lines also show up on stdout during the test, which is expected).

- [ ] **Step 11: Write the failing test for metrics**

A `*Vec` metric does not appear in `Gather()` until at least one label combination exists, so the test touches every metric first. It then compares the exact set of metric names and label names against the contract.

`internal/obs/metrics_test.go`:

```go
package obs

import (
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetricsRegistersCatalogue(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// Vectors only appear in Gather once a child exists, so touch every metric.
	m.APIRequests.WithLabelValues("coins_markets", "200").Inc()
	m.APIRequestDuration.WithLabelValues("coins_markets").Observe(0.2)
	m.APIRateLimited.Inc()
	m.LastSuccess.WithLabelValues("coins_markets").SetToCurrentTime()
	m.SourceDataAge.WithLabelValues("coins_markets").Set(12)
	m.SchemaValidations.WithLabelValues("transformer", "coins_markets.v1", "pass").Inc()
	m.SchemaViolations.WithLabelValues("transformer", "coins_markets.v1", "required").Inc()
	m.ExportLag.Set(3)
	m.ExportBatches.WithLabelValues("ok").Inc()
	m.S3Puts.WithLabelValues("created").Inc()
	m.PublishedRecords.Inc()
	m.Records.WithLabelValues("market_snapshot", "written").Inc()
	m.DedupDropped.WithLabelValues("replay").Inc()
	m.Rejected.WithLabelValues("empty_record").Inc()
	m.DLQ.WithLabelValues("transform", "schema").Inc()
	m.ConsumerLag.WithLabelValues("0").Set(5)
	m.BatchDuration.Observe(0.1)
	m.E2ELatency.Observe(20)

	want := map[string][]string{
		"etl_api_requests_total":             {"endpoint", "status"},
		"etl_api_request_duration_seconds":   {"endpoint"},
		"etl_api_rate_limited_total":         nil,
		"etl_last_success_timestamp_seconds": {"endpoint"},
		"etl_source_data_age_seconds":        {"endpoint"},
		"etl_schema_validations_total":       {"result", "schema", "stage"},
		"etl_schema_violations_total":        {"keyword", "schema", "stage"},
		"etl_export_lag_seconds":             nil,
		"etl_export_batches_total":           {"result"},
		"etl_s3_put_total":                   {"result"},
		"etl_published_records_total":        nil,
		"etl_records_total":                  {"dataset", "outcome"},
		"etl_dedup_dropped_total":            {"reason"},
		"etl_rejected_total":                 {"reason"},
		"etl_dlq_total":                      {"error_class", "stage"},
		"etl_consumer_lag_records":           {"partition"},
		"etl_batch_duration_seconds":         nil,
		"etl_e2e_latency_seconds":            nil,
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string][]string{}
	for _, f := range families {
		var labels []string
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
		sort.Strings(labels)
		got[f.GetName()] = labels
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metric catalogue mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestNewMetricsTwiceOnSameRegistryPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
	defer func() {
		if recover() == nil {
			t.Fatal("second NewMetrics on the same registry did not panic")
		}
	}()
	NewMetrics(reg)
}
```

Run: `go test ./internal/obs/ -run Metrics`
Expected: FAIL, a build error `undefined: NewMetrics`.

- [ ] **Step 12: Implement metrics**

`promauto.With(reg)` returns a factory that creates each collector and registers it on `reg`, panicking on duplicates. Use one registry per process (`prometheus.NewRegistry()`) rather than the global default, so tests can create fresh ones.

`internal/obs/metrics.go`:

```go
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the full etl_* catalogue (spec §8.2). Every service registers all of
// them; a service only moves the ones it owns, the others stay absent or at zero.
type Metrics struct {
	APIRequests        *prometheus.CounterVec   // etl_api_requests_total{endpoint,status}
	APIRequestDuration *prometheus.HistogramVec // etl_api_request_duration_seconds{endpoint}
	APIRateLimited     prometheus.Counter       // etl_api_rate_limited_total
	LastSuccess        *prometheus.GaugeVec     // etl_last_success_timestamp_seconds{endpoint}
	SourceDataAge      *prometheus.GaugeVec     // etl_source_data_age_seconds{endpoint}
	SchemaValidations  *prometheus.CounterVec   // etl_schema_validations_total{stage,schema,result}
	SchemaViolations   *prometheus.CounterVec   // etl_schema_violations_total{stage,schema,keyword}
	ExportLag          prometheus.Gauge         // etl_export_lag_seconds
	ExportBatches      *prometheus.CounterVec   // etl_export_batches_total{result}
	S3Puts             *prometheus.CounterVec   // etl_s3_put_total{result}  result=created|exists|error|mismatch
	PublishedRecords   prometheus.Counter       // etl_published_records_total
	Records            *prometheus.CounterVec   // etl_records_total{dataset,outcome}
	DedupDropped       *prometheus.CounterVec   // etl_dedup_dropped_total{reason}
	Rejected           *prometheus.CounterVec   // etl_rejected_total{reason}
	DLQ                *prometheus.CounterVec   // etl_dlq_total{stage,error_class}
	ConsumerLag        *prometheus.GaugeVec     // etl_consumer_lag_records{partition}
	BatchDuration      prometheus.Histogram     // etl_batch_duration_seconds
	E2ELatency         prometheus.Histogram     // etl_e2e_latency_seconds
}

// NewMetrics creates and registers every metric on reg (panics on duplicate
// registration, like promauto). Pass a fresh prometheus.NewRegistry() per process;
// Go runtime / process collectors are registered by the caller (cmd/etl).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		APIRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_api_requests_total",
			Help: "CoinGecko HTTP calls by endpoint and status (HTTP code, or network_error).",
		}, []string{"endpoint", "status"}),
		APIRequestDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "etl_api_request_duration_seconds",
			Help:    "CoinGecko HTTP call latency, including body read.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}, []string{"endpoint"}),
		APIRateLimited: f.NewCounter(prometheus.CounterOpts{
			Name: "etl_api_rate_limited_total",
			Help: "CoinGecko responses with HTTP 429.",
		}),
		LastSuccess: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_last_success_timestamp_seconds",
			Help: "Unix time of the last successful (outcome ok) call per endpoint.",
		}, []string{"endpoint"}),
		SourceDataAge: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_source_data_age_seconds",
			Help: "fetched_at minus the newest last_updated in the response, per endpoint.",
		}, []string{"endpoint"}),
		SchemaValidations: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_schema_validations_total",
			Help: "JSON Schema validations by stage, schema and result (pass|fail).",
		}, []string{"stage", "schema", "result"}),
		SchemaViolations: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_schema_violations_total",
			Help: "JSON Schema violations by stage, schema and failing keyword.",
		}, []string{"stage", "schema", "keyword"}),
		ExportLag: f.NewGauge(prometheus.GaugeOpts{
			Name: "etl_export_lag_seconds",
			Help: "Age of the oldest raw.api_calls row not yet exported (0 when caught up).",
		}),
		ExportBatches: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_export_batches_total",
			Help: "Exporter batches by result.",
		}, []string{"result"}),
		S3Puts: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_s3_put_total",
			Help: "Conditional S3 PUTs by result: created, exists (412, same bytes), mismatch (412, different bytes), error.",
		}, []string{"result"}),
		PublishedRecords: f.NewCounter(prometheus.CounterOpts{
			Name: "etl_published_records_total",
			Help: "Records produced to the broker and acknowledged.",
		}),
		Records: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_records_total",
			Help: "Transformer records by dataset and outcome (written, duplicate, rejected, dlq).",
		}, []string{"dataset", "outcome"}),
		DedupDropped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_dedup_dropped_total",
			Help: "Records dropped by deduplication, by reason (replay, duplicate_response, duplicate_record).",
		}, []string{"reason"}),
		Rejected: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_rejected_total",
			Help: "Records written to the rejected zone, by reason.",
		}, []string{"reason"}),
		DLQ: f.NewCounterVec(prometheus.CounterOpts{
			Name: "etl_dlq_total",
			Help: "Records written to the dead-letter zone, by stage and error class.",
		}, []string{"stage", "error_class"}),
		ConsumerLag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "etl_consumer_lag_records",
			Help: "High watermark minus committed offset, per assigned partition.",
		}, []string{"partition"}),
		BatchDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_batch_duration_seconds",
			Help:    "Transformer batch processing time (TX1, PUTs, TX2).",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~20s
		}),
		E2ELatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "etl_e2e_latency_seconds",
			Help:    "From fetched_at to the processed object being written.",
			Buckets: []float64{1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 1800},
		}),
	}
}
```

Run: `go test ./internal/obs/ -run Metrics`
Expected: PASS.

- [ ] **Step 13: Write the failing test for the health/metrics server**

The tests call the unexported `newMux` through `httptest` and the unexported `serve(ctx, listener, ...)` with a `127.0.0.1:0` listener, so no fixed port is needed. `readyTimeout` is an unexported field so the timeout test can shorten it from 2s to 50ms.

`internal/obs/server_test.go`:

```go
package obs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func TestHealthzAlwaysOK(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return errors.New("down") })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/healthz")
	if code != http.StatusOK || !strings.Contains(body, `"ok"`) {
		t.Fatalf("/healthz = %d %s", code, body)
	}
}

func TestReadyzAllPass(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return nil })
	h.AddReadiness("s3", func(context.Context) error { return nil })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d %s, want 200", code, body)
	}
}

func TestReadyzReportsFailures(t *testing.T) {
	h := NewHealth()
	h.AddReadiness("postgres", func(context.Context) error { return nil })
	h.AddReadiness("broker", func(context.Context) error { return errors.New("dial tcp: connection refused") })
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", code)
	}
	var resp struct {
		Status   string            `json:"status"`
		Failures map[string]string `json:"failures"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, body)
	}
	want := map[string]string{"broker": "dial tcp: connection refused"}
	if resp.Status != "not_ready" || len(resp.Failures) != 1 || resp.Failures["broker"] != want["broker"] {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestReadyzTimesOutHungCheck(t *testing.T) {
	h := NewHealth()
	h.readyTimeout = 50 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	h.AddReadiness("stuck", func(context.Context) error { <-release; return nil }) // ignores ctx on purpose
	start := time.Now()
	code, body := get(t, newMux(h, prometheus.NewRegistry()), "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "timed out") {
		t.Fatalf("/readyz = %d %s, want 503 timed out", code, body)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("/readyz took %v, timeout not enforced", time.Since(start))
	}
}

func TestMetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.PublishedRecords.Add(3)
	code, body := get(t, newMux(NewHealth(), reg), "/metrics")
	if code != http.StatusOK || !strings.Contains(body, "etl_published_records_total 3") {
		t.Fatalf("/metrics = %d\n%s", code, body)
	}
}

func TestServeStopsOnCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, NewHealth(), prometheus.NewRegistry()) }()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
}

func TestServeReportsListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	err = Serve(context.Background(), ln.Addr().String(), NewHealth(), prometheus.NewRegistry())
	if err == nil {
		t.Fatal("Serve on a busy port returned nil")
	}
}
```

Run: `go test ./internal/obs/ -run 'Healthz|Readyz|Metrics|Serve'`
Expected: FAIL, a build error `undefined: NewHealth` / `undefined: newMux` / `undefined: serve`.

- [ ] **Step 14: Implement the server**

Behaviour:
- `/healthz` always returns 200 `{"status":"ok"}`.
- `/readyz` runs every registered check concurrently under one 2s timeout. If all pass it returns 200 `{"status":"ready"}`. Otherwise it returns 503 `{"status":"not_ready","failures":{"<name>":"<error>"}}`. A check that ignores its context is reported as `readiness check timed out`. Its goroutine writes into a buffered channel, so it never leaks a blocked send.
- `/metrics` uses `promhttp.HandlerFor(g, ...)`.
- `Serve` returns nil after a graceful shutdown when ctx is cancelled, and returns an error right away if the port is busy.

Method-qualified patterns such as `"GET /healthz"` need Go 1.22 or later. They also match HEAD.

`internal/obs/server.go`:

```go
package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check reports whether one dependency is reachable. It must honour ctx.
type Check func(ctx context.Context) error

type namedCheck struct {
	name  string
	check Check
}

// Health holds the readiness checks served on /readyz. /healthz is always 200.
type Health struct {
	mu           sync.RWMutex
	checks       []namedCheck
	readyTimeout time.Duration
}

func NewHealth() *Health {
	return &Health{readyTimeout: 2 * time.Second}
}

// AddReadiness registers a named dependency check. Safe to call while serving.
func (h *Health) AddReadiness(name string, c Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks = append(h.checks, namedCheck{name: name, check: c})
}

type checkResult struct {
	name string
	err  error
}

// ready runs all checks concurrently under one timeout and returns name -> error
// text for each failed (or unfinished) check. Empty map means ready.
func (h *Health) ready(ctx context.Context) map[string]string {
	h.mu.RLock()
	checks := append([]namedCheck(nil), h.checks...)
	h.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, h.readyTimeout)
	defer cancel()
	results := make(chan checkResult, len(checks)) // buffered: late checks never block
	for _, c := range checks {
		go func() { results <- checkResult{name: c.name, err: c.check(ctx)} }()
	}
	failures := map[string]string{}
	pending := map[string]bool{}
	for _, c := range checks {
		pending[c.name] = true
	}
	for range checks {
		select {
		case r := <-results:
			delete(pending, r.name)
			if r.err != nil {
				failures[r.name] = r.err.Error()
			}
		case <-ctx.Done():
			for name := range pending {
				failures[name] = "readiness check timed out"
			}
			return failures
		}
	}
	return failures
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newMux(h *Health, g prometheus.Gatherer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if failures := h.ready(r.Context()); len(failures) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "failures": failures})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	return mux
}

// Serve listens on addr and serves /healthz, /readyz and /metrics until ctx is
// cancelled, then shuts down gracefully and returns nil. A listen or serve
// failure is returned immediately.
func Serve(ctx context.Context, addr string, h *Health, g prometheus.Gatherer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("obs: listen %s: %w", addr, err)
	}
	return serve(ctx, ln, h, g)
}

func serve(ctx context.Context, ln net.Listener, h *Health, g prometheus.Gatherer) error {
	srv := &http.Server{Handler: newMux(h, g), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("obs: serve: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("obs: shutdown: %w", err)
		}
		if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("obs: serve: %w", err)
		}
		return nil
	}
}
```

Run: `go test ./internal/obs/`
Expected: PASS (all logger, metrics and server tests).

- [ ] **Step 15: Write the failing test for cmd/etl**

`internal/obs` is not needed here: `healthcheck` is a plain HTTP client, because the distroless runtime image has no curl or wget. The test points `ETL_HTTP_ADDR` at an `httptest` server.

`cmd/etl/main_test.go`:

```go
package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunDispatch(t *testing.T) {
	cases := []struct {
		args       []string
		wantCode   int
		wantStderr string
	}{
		{nil, 2, "usage: etl"},
		{[]string{"bogus"}, 2, `unknown command "bogus"`},
		{[]string{"init"}, 1, "etl init: not implemented"},
		{[]string{"fetcher"}, 1, "etl fetcher: not implemented"},
		{[]string{"exporter"}, 1, "etl exporter: not implemented"},
		{[]string{"publisher"}, 1, "etl publisher: not implemented"},
		{[]string{"transformer"}, 1, "etl transformer: not implemented"},
		{[]string{"help"}, 0, ""},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(c.args, &stdout, &stderr); code != c.wantCode {
				t.Fatalf("exit code %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

func TestReadyzURL(t *testing.T) {
	cases := map[string]string{
		"":               "http://127.0.0.1:8080/readyz",
		":8080":          "http://127.0.0.1:8080/readyz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/readyz",
		"[::]:8080":      "http://127.0.0.1:8080/readyz",
		"10.0.0.5:8080":  "http://10.0.0.5:8080/readyz",
		"localhost:8081": "http://localhost:8081/readyz",
	}
	for in, want := range cases {
		got, err := readyzURL(in)
		if err != nil || got != want {
			t.Errorf("readyzURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := readyzURL("8080"); err == nil {
		t.Error(`readyzURL("8080") succeeded, want error`)
	}
}

func TestHealthcheckCommand(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	t.Setenv("ETL_HTTP_ADDR", srv.Listener.Addr().String())

	var stderr bytes.Buffer
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 0 {
		t.Fatalf("ready server: exit %d (%s), want 0", code, stderr.String())
	}
	status = http.StatusServiceUnavailable
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("not-ready server: exit %d, want 1", code)
	}
}

func TestHealthcheckUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more
	t.Setenv("ETL_HTTP_ADDR", addr)
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
}
```

Run: `go test ./cmd/etl/`
Expected: FAIL, a build error `undefined: run` / `undefined: readyzURL`.

- [ ] **Step 16: Implement cmd/etl/main.go**

`run` returns an exit code so tests can call it without `os.Exit`. `healthcheck` reads `ETL_HTTP_ADDR` directly rather than through `config.Load`, so an unrelated bad variable cannot break the container healthcheck. It rewrites a wildcard listen host (`""`, `0.0.0.0`, `::`) to `127.0.0.1`. Its 3s client timeout is longer than the 2s readiness timeout, so compose healthchecks need `timeout:` ≥ 4s (Task 17).

`cmd/etl/main.go`:

```go
// Command etl is the single binary of the pipeline. The first argument selects
// the subcommand; see usage below.
package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

const usageText = `usage: etl <command>

commands:
  init         run migrations, create partitions, topic and buckets (one-shot)
  fetcher      poll CoinGecko into raw.api_calls
  exporter     export raw.api_calls to s3://raw as JSONL
  publisher    publish exported batches to the broker
  transformer  consume, clean, dedup and write s3://processed
  healthcheck  exit 0 if the local /readyz returns 200 (for container healthchecks)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "healthcheck":
		return healthcheck(os.Getenv("ETL_HTTP_ADDR"), stderr)
	case "init", "fetcher", "exporter", "publisher", "transformer":
		fmt.Fprintf(stderr, "etl %s: not implemented\n", args[0])
		return 1
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	default:
		fmt.Fprintf(stderr, "etl: unknown command %q\n%s", args[0], usageText)
		return 2
	}
}

// readyzURL turns a listen address such as ":8080" or "0.0.0.0:8080" into the
// loopback URL of /readyz. An empty addr means the default ":8080".
func readyzURL(addr string) (string, error) {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("bad ETL_HTTP_ADDR %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

// healthcheck is used by Docker healthchecks: the distroless image has no curl.
// The 3 s client timeout exceeds the 2 s readiness timeout of /readyz.
func healthcheck(addr string, stderr io.Writer) int {
	url, err := readyzURL(addr)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: %s returned %d: %s\n", url, resp.StatusCode, body)
		return 1
	}
	return 0
}
```

Run: `go test ./cmd/etl/`
Expected: PASS.

- [ ] **Step 17: Tidy and run the whole task's checks**

```bash
go mod tidy
go build ./...
go vet ./...
make lint
make test-unit
go test -race -count=1 ./...
make build && ./bin/etl fetcher; echo "exit=$?"
```

Expected: every package prints `ok`, `make lint` prints only `go vet ./...`, and the last command prints `etl fetcher: not implemented` and `exit=1`. Running `./bin/etl healthcheck` with nothing listening on :8080 prints a `connection refused` error and exits 1.

- [ ] **Step 18: Commit**

```bash
git add go.mod go.sum .gitignore Makefile internal/xtime internal/config internal/obs cmd/etl
git commit -m "$(cat <<'MSG'
Scaffold module, config, xtime, obs and etl CLI

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

### Task 2: internal/event (raw event envelope, idempotency key, deterministic JSON)

**Files:**
- Create: `internal/event/event.go`
- Test: `internal/event/event_test.go`

**Interfaces:**
- Consumes: nothing beyond the standard library (the module from Task 1).
- Produces:
```go
const (
    SchemaVersion       = 1
    SourceCoinGecko     = "coingecko"
    EndpointMarkets     = "coins_markets"
    EndpointCoinDetail  = "coin_detail"
    OutcomeOK           = "ok"
    OutcomeHTTPError    = "http_error"
    OutcomeNetworkError = "network_error"
)
type Request struct {
    Path   string            `json:"path"`
    Params map[string]string `json:"params"`
}
type HTTPMeta struct {
    Status        int     `json:"status"`
    LatencyMS     int64   `json:"latency_ms"`
    Date          *string `json:"date"`
    AgeSeconds    *int    `json:"age_s"`
    CFCacheStatus *string `json:"cf_cache_status"`
    ETag          *string `json:"etag"`
    RetryAfterS   *int    `json:"retry_after_s"`
}
type RawEvent struct {
    SchemaVersion  int             `json:"schema_version"`
    EventID        string          `json:"event_id"`         // UUIDv7
    IdempotencyKey *string         `json:"idempotency_key"`  // nil unless outcome ok
    Source         string          `json:"source"`
    Endpoint       string          `json:"endpoint"`
    Request        Request         `json:"request"`
    FetchedAt      string          `json:"fetched_at"`       // xtime.Format
    Outcome        string          `json:"outcome"`
    HTTP           *HTTPMeta       `json:"http"`
    Error          *string         `json:"error"`
    PayloadSHA256  *string         `json:"payload_sha256"`   // hex
    Payload        json.RawMessage `json:"payload"`          // verbatim bytes; JSON null when absent
}
func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string
func SHA256Hex(b []byte) string
func Marshal(ev RawEvent) ([]byte, error)   // deterministic, payload embedded verbatim, no trailing newline
func Unmarshal(b []byte) (RawEvent, error)  // keeps Payload bytes verbatim
```

Why this needs care: the same event bytes travel Postgres → raw JSONL → broker → transformer, and `payload_sha256` must verify at every hop. `json.Marshal` *re-encodes* a `json.RawMessage`: it compacts the whitespace and escapes `<`, `>` and `&` as `<`, `>` and `&`. That would change the payload bytes. So Marshal encodes everything except the payload with `encoding/json` (which keeps struct-declaration field order and sorts map keys) and splices the payload bytes in by hand. On the decode side, `json.Unmarshal` into a `json.RawMessage` copies the value's bytes untouched.

- [ ] **Step 1: Write the failing test**

The test payload uses odd whitespace, unsorted keys, `1e3`, a float with many digits and HTML characters. Each of these would change if anything re-encoded it.

`internal/event/event_test.go`:

```go
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
// normally compact or escape.
var oddPayload = []byte("[ {\"z\" : 1,\n\t\"a\":[ 1 , 2.50 ,1e3],  \"html\":\"<b>&</b>\", \"price\": 82800.000000001 } ]")

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

func TestUnmarshalRejectsGarbage(t *testing.T) {
	for _, in := range []string{``, `{`, `[]`, `{"schema_version":"one"}`} {
		if _, err := Unmarshal([]byte(in)); err == nil {
			t.Errorf("Unmarshal(%q) succeeded, want error", in)
		}
	}
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/event/`
Expected: FAIL, a build error such as `undefined: SHA256Hex`, `undefined: IdempotencyKey`, `undefined: RawEvent`.

- [ ] **Step 3: Implement internal/event**

The idempotency key is `sha256hex(source + "|" + endpoint + "|" + canonical(params) + "|" + payloadSHA256Hex)`, where `canonical(params)` is the `k=v` pairs sorted by key and joined by `&`, with no URL escaping. It is informational only (spec §4.1): deduplication does not use it.

`internal/event/event.go`:

```go
// Package event defines the raw event envelope (schema S1, pipeline/raw_event.v1):
// one RawEvent per CoinGecko HTTP call. The same bytes travel Postgres -> raw JSONL
// -> broker, so serialization is deterministic and the payload is never re-encoded.
package event

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	SchemaVersion       = 1
	SourceCoinGecko     = "coingecko"
	EndpointMarkets     = "coins_markets"
	EndpointCoinDetail  = "coin_detail"
	OutcomeOK           = "ok"
	OutcomeHTTPError    = "http_error"
	OutcomeNetworkError = "network_error"
)

type Request struct {
	Path   string            `json:"path"`
	Params map[string]string `json:"params"`
}

type HTTPMeta struct {
	Status        int     `json:"status"`
	LatencyMS     int64   `json:"latency_ms"`
	Date          *string `json:"date"`
	AgeSeconds    *int    `json:"age_s"`
	CFCacheStatus *string `json:"cf_cache_status"`
	ETag          *string `json:"etag"`
	RetryAfterS   *int    `json:"retry_after_s"`
}

type RawEvent struct {
	SchemaVersion  int             `json:"schema_version"`
	EventID        string          `json:"event_id"`        // UUIDv7
	IdempotencyKey *string         `json:"idempotency_key"` // nil unless outcome ok
	Source         string          `json:"source"`
	Endpoint       string          `json:"endpoint"`
	Request        Request         `json:"request"`
	FetchedAt      string          `json:"fetched_at"` // xtime.Format
	Outcome        string          `json:"outcome"`
	HTTP           *HTTPMeta       `json:"http"`
	Error          *string         `json:"error"`
	PayloadSHA256  *string         `json:"payload_sha256"` // hex
	Payload        json.RawMessage `json:"payload"`        // verbatim bytes; JSON null when absent
}

// SHA256Hex returns the lowercase hex SHA-256 of b.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// IdempotencyKey = sha256hex(source "|" endpoint "|" canonical(params) "|" payloadSHA256Hex),
// where canonical(params) is "k=v" pairs sorted by key and joined by "&" (no URL
// escaping). Two calls with the same request and byte-identical body get the same key.
func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]string, len(keys))
	for i, k := range keys {
		pairs[i] = k + "=" + params[k]
	}
	return SHA256Hex([]byte(source + "|" + endpoint + "|" + strings.Join(pairs, "&") + "|" + payloadSHA256Hex))
}

// envelope is RawEvent without the payload. encoding/json writes struct fields
// in declaration order and map keys sorted, which makes the output deterministic.
// The payload is appended by hand because json.Marshal would compact (re-encode)
// a json.RawMessage and change its bytes.
type envelope struct {
	SchemaVersion  int       `json:"schema_version"`
	EventID        string    `json:"event_id"`
	IdempotencyKey *string   `json:"idempotency_key"`
	Source         string    `json:"source"`
	Endpoint       string    `json:"endpoint"`
	Request        Request   `json:"request"`
	FetchedAt      string    `json:"fetched_at"`
	Outcome        string    `json:"outcome"`
	HTTP           *HTTPMeta `json:"http"`
	Error          *string   `json:"error"`
	PayloadSHA256  *string   `json:"payload_sha256"`
}

// Marshal serializes ev as one JSON object with fields in declaration order and
// no trailing newline. Payload is embedded byte-for-byte (an empty Payload is
// written as null); it must be valid JSON. A nil Request.Params is written as {}.
func Marshal(ev RawEvent) ([]byte, error) {
	env := envelope{
		SchemaVersion:  ev.SchemaVersion,
		EventID:        ev.EventID,
		IdempotencyKey: ev.IdempotencyKey,
		Source:         ev.Source,
		Endpoint:       ev.Endpoint,
		Request:        ev.Request,
		FetchedAt:      ev.FetchedAt,
		Outcome:        ev.Outcome,
		HTTP:           ev.HTTP,
		Error:          ev.Error,
		PayloadSHA256:  ev.PayloadSHA256,
	}
	if env.Request.Params == nil {
		env.Request.Params = map[string]string{}
	}
	payload := []byte("null")
	if len(ev.Payload) > 0 {
		if !json.Valid(ev.Payload) {
			return nil, errors.New("event: payload is not valid JSON")
		}
		payload = ev.Payload
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep "<", ">", "&" readable in error texts and URLs
	if err := enc.Encode(env); err != nil {
		return nil, fmt.Errorf("event: marshal envelope: %w", err)
	}
	head := bytes.TrimSuffix(buf.Bytes(), []byte("\n")) // Encode appends a newline
	head = bytes.TrimSuffix(head, []byte("}"))          // reopen the object

	out := make([]byte, 0, len(head)+len(`,"payload":}`)+len(payload))
	out = append(out, head...)
	out = append(out, `,"payload":`...)
	out = append(out, payload...)
	out = append(out, '}')
	return out, nil
}

// Unmarshal parses one serialized RawEvent. Payload keeps the exact bytes of the
// "payload" value (encoding/json copies a json.RawMessage verbatim); a JSON null
// payload becomes a nil Payload.
func Unmarshal(b []byte) (RawEvent, error) {
	var ev RawEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		return RawEvent{}, fmt.Errorf("event: unmarshal: %w", err)
	}
	if bytes.Equal(ev.Payload, []byte("null")) {
		ev.Payload = nil
	}
	return ev, nil
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `go test -race ./internal/event/ && go vet ./internal/event/`
Expected: PASS (`ok  coingecko-etl/internal/event`).

- [ ] **Step 5: Commit**

```bash
gofmt -l internal/event   # expect no output
git add internal/event
git commit -m "$(cat <<'MSG'
Add raw event envelope with deterministic JSON and idempotency key

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

CONTRACT NOTES

1. `schemas/embed.go` grows one `//go:embed` line per task, because a pattern that matches no file is a compile error and `source/` does not exist yet in Task 3. Task 3 embeds `common/*.json` and `pipeline/*.json`. Task 4 adds `//go:embed source/coingecko/*.json`. Task 12 adds `//go:embed processed/*.json`; nothing else in the file changes. The contract's "contains common/, source/, pipeline/, processed/" is true once Task 12 is done.
2. `validation.Load` also requires each file's `$id` to equal `https://schemas.cg-etl.internal/` + its path in the FS. The processed schemas in Task 12/13 must therefore use `$id` = `https://schemas.cg-etl.internal/processed/<dataset>.v1.json`, which the contract IDs already do. `Load` also rejects schema files with duplicate keys.
3. `Validate` returns violations **sorted** by (instance_location, keyword_location, message) before the cap of 20, so DLQ objects are byte-deterministic on reprocessing. jsonschema v6.0.3 returns a nil `KeywordPath()` for `not`, so the registry appends `"not"` itself; without that, `Keyword()` would return the parent property name.
4. S1 (`raw_event.v1.json`) requires all 12 keys of `event.RawEvent` and all 7 keys of `event.HTTPMeta`, because the Go structs have no `omitempty`. `request.params` must be a JSON object: producers must use an empty map, never a nil map, which marshals to `null` and fails S1. `http` is `null` or an object. When `outcome = "ok"`, `payload` must be non-null and `idempotency_key`/`payload_sha256` must be 64-hex. Otherwise `payload` must be `null`; `idempotency_key`/`payload_sha256` may be null or 64-hex.
5. DLQ record (`dlq_record.v1.json`), exact shape that the exporter, publisher and transformer tasks must emit: `{"schema_version":1,"stage":"export|publish|transform_input|transform_output","error_class":"<^[a-z][a-z0-9_]{0,63}$>","violations":[{"instance_location","keyword_location","message"}] (0..20, i.e. []validation.Violation),"origin":{"kind":"postgres|s3|kafka", optional: "topic","partition"(>=0),"offset"(>=0),"bucket","object_key","line_no"(>=1),"batch_id"(>=1),"seq"(>=1),"event_id","dataset","record_key"},"original_base64":"<std base64>","occurred_at":"<xtime.Format>"}` plus an optional top-level `"error"` string. These 7 top-level keys are required, only `error` may be added, and `origin` rejects unknown keys. `violations` must be `[]`, not `null`, when there are none (use a non-nil slice).
6. Rejected record (`rejected_record.v1.json`): `{"schema_version":1,"dataset","reason","record_key","event_date"(YYYY-MM-DD),"record"(object),"_event_id","_raw_object","_kafka_partition","_kafka_offset","_processed_at"}`, all required, nothing else allowed. It has no `_schema_version` (that is what `schema_version` is for).
7. S0: market items require `id, symbol, name, current_price, last_updated`. `id` is any string, so `""` reaches the transformer as the `missing_id` reject. A missing key or a non-string `id` is a schema failure (dlq `source_schema_violation`). `current_price` may be `null` (`null_price` reject). For `/coins/{id}`, only `id, symbol, name, last_updated` are required. `market_data` and `tickers` are optional because the recorded `coin_usd-coin_meta.json` was fetched without them. The transformer must handle their absence by emitting only coin_snapshot and coin_contract rows.

### Task 3: Schemas and validation registry

**Files:**
- Create: `schemas/embed.go`
- Create: `schemas/common/defs.v1.json`
- Create: `schemas/pipeline/raw_event.v1.json`, `schemas/pipeline/rejected_record.v1.json`, `schemas/pipeline/dlq_record.v1.json`
- Create: `internal/validation/ids.go`, `internal/validation/registry.go`
- Test: `internal/validation/registry_test.go`, `internal/validation/contract_test.go`, `internal/validation/evolution_test.go` (fuzz + schema evolution)
- Test fixtures: `schemas/testdata/pipeline/{raw_event,rejected_record,dlq_record}.v1/{valid,invalid}/*.json`

**Interfaces:**
- Consumes: nothing from earlier tasks (only `go.mod` with `module coingecko-etl`, `go 1.27`).
- Produces:
```go
// package schemas (dir schemas/)
var FS embed.FS // this task: common/*.json, pipeline/*.json (Task 4 adds source/, Task 12 adds processed/)

// package validation (internal/validation)
type Violation struct {
    InstanceLocation string `json:"instance_location"`
    KeywordLocation  string `json:"keyword_location"`
    Message          string `json:"message"`
}
type Registry struct { /* unexported */ }
func Load(fsys fs.FS) (*Registry, error)
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)
func Keyword(v Violation) string
const (
    IDRawEvent              = "https://schemas.cg-etl.internal/pipeline/raw_event.v1.json"
    IDRejectedRecord        = "https://schemas.cg-etl.internal/pipeline/rejected_record.v1.json"
    IDDLQRecord             = "https://schemas.cg-etl.internal/pipeline/dlq_record.v1.json"
    IDSourceMarkets         = "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"
    IDSourceCoin            = "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json"
    IDMarketSnapshot        = "https://schemas.cg-etl.internal/processed/market_snapshot.v1.json"
    IDCoinSnapshot          = "https://schemas.cg-etl.internal/processed/coin_snapshot.v1.json"
    IDCoinMarketByCurrency  = "https://schemas.cg-etl.internal/processed/coin_market_by_currency.v1.json"
    IDTicker                = "https://schemas.cg-etl.internal/processed/ticker.v1.json"
    IDCoinContract          = "https://schemas.cg-etl.internal/processed/coin_contract.v1.json"
)
```

Background for the engineer:
- **JSON Schema draft 2020-12.** Every schema file declares `"$schema": "https://json-schema.org/draft/2020-12/schema"` and an `$id` whose path mirrors the file path under `schemas/`. Relative `$ref`s such as `../common/defs.v1.json#/$defs/uuid` resolve against the `$id`, so they find the embedded file without any loader.
- **`github.com/santhosh-tekuri/jsonschema/v6` v6.0.3** is the validator.
  - `jsonschema.NewCompiler()` collects resources. `c.AddResource(id, doc)` registers an already-decoded schema, and `c.Compile(id)` resolves `$ref`s and returns a `*jsonschema.Schema`.
  - `c.DefaultDraft(jsonschema.Draft2020)` sets the draft for files without `$schema`.
  - `c.AssertFormat()` makes `format` (date-time, date, uuid) an assertion; by default it is only an annotation.
  - `c.UseLoader(jsonschema.SchemeURLLoader{})` installs a loader with no schemes registered, so any `$ref` to a URL that was not added with `AddResource` fails at compile time instead of reaching the file system or the network.
  - `schema.Validate(v)` takes a decoded value. `jsonschema.UnmarshalJSON` decodes with `json.Number`, so 2^53+ values such as VND market caps keep their exact digits.
  - Errors are a `*jsonschema.ValidationError` tree. Walk `Causes` down to the leaves.
    - Each leaf has `InstanceLocation []string` (JSON pointer tokens), `SchemaURL` (`<$id>#<pointer to the subschema>`) and `ErrorKind`, whose `KeywordPath()` gives the failing keyword and whose `LocalizedString(printer)` gives the message.
    - Do **not** use `BasicOutput()`: in v6.0.3 it loses the message for errors reached through `$ref`.
    - Do not use `propertyNames`: errors under it point at the wrong instance location (upstream issue #276).
- **`encoding/json/jsontext`** (Go 1.27 standard library). `jsontext.NewDecoder(r).SkipValue()` checks syntax, rejects duplicate member names and invalid UTF-8 by default, and catches the cases that `encoding/json` and the schema validator silently accept (`{"id":"a","id":"b"}`).
- **Messages** come from `golang.org/x/text/message` with an English printer.

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/santhosh-tekuri/jsonschema/v6@v6.0.3 golang.org/x/text@v0.43.0
```
Expected: `go.mod` lists both modules. `go mod tidy` in Step 5 moves them to the direct `require` block.

- [ ] **Step 2: Write the shared definitions and the three pipeline schemas, plus the embed package**

`schemas/common/defs.v1.json`. `utc_timestamp` keeps both `format` and `pattern`, because `format: date-time` alone also accepts offsets and missing milliseconds:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/common/defs.v1.json",
  "title": "Shared definitions",
  "$defs": {
    "utc_timestamp": {
      "description": "RFC 3339 instant in UTC with exactly millisecond precision, e.g. 2026-10-10T10:07:55.000Z (xtime.Format). format alone would also accept offsets.",
      "type": "string",
      "format": "date-time",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$"
    },
    "date": {
      "description": "Calendar date YYYY-MM-DD (xtime.FormatDate).",
      "type": "string",
      "format": "date",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
    },
    "decimal_string": {
      "description": "Money or quantity as a plain-notation decimal string: no exponent, no leading zeros, no plus sign.",
      "type": "string",
      "pattern": "^-?(0|[1-9]\\d*)(\\.\\d+)?$"
    },
    "sha256_hex": {
      "description": "SHA-256 digest as 64 lower-case hex characters.",
      "type": "string",
      "pattern": "^[0-9a-f]{64}$"
    },
    "uuid": {
      "description": "UUID in canonical lower-case form (google/uuid String()).",
      "type": "string",
      "format": "uuid",
      "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    }
  }
}
```

`schemas/pipeline/raw_event.v1.json` (S1, strict; mirrors `event.RawEvent` field by field). `if/then/else` ties the payload to the outcome. Nullable 64-hex fields use `"type": ["string","null"]` plus `pattern`: `pattern` only applies to strings, which keeps error output to one leaf instead of the noisy `anyOf` branches:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/pipeline/raw_event.v1.json",
  "title": "S1 raw event: one CoinGecko HTTP call (raw.api_calls row = raw JSONL line = Kafka message value)",
  "description": "STRICT. Mirrors event.RawEvent; every key is always present because the Go struct has no omitempty. payload is the upstream body verbatim and is validated separately against S0.",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "event_id",
    "idempotency_key",
    "source",
    "endpoint",
    "request",
    "fetched_at",
    "outcome",
    "http",
    "error",
    "payload_sha256",
    "payload"
  ],
  "properties": {
    "schema_version": { "const": 1 },
    "event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "idempotency_key": { "type": ["string", "null"], "pattern": "^[0-9a-f]{64}$" },
    "source": { "const": "coingecko" },
    "endpoint": { "enum": ["coins_markets", "coin_detail"] },
    "request": {
      "type": "object",
      "additionalProperties": false,
      "required": ["path", "params"],
      "properties": {
        "path": { "type": "string", "pattern": "^/" },
        "params": {
          "type": "object",
          "additionalProperties": { "type": "string" }
        }
      }
    },
    "fetched_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "outcome": { "enum": ["ok", "http_error", "network_error"] },
    "http": {
      "description": "null for network errors (no HTTP response).",
      "type": ["object", "null"],
      "additionalProperties": false,
      "required": ["status", "latency_ms", "date", "age_s", "cf_cache_status", "etag", "retry_after_s"],
      "properties": {
        "status": { "type": "integer", "minimum": 100, "maximum": 599 },
        "latency_ms": { "type": "integer", "minimum": 0 },
        "date": { "type": ["string", "null"] },
        "age_s": { "type": ["integer", "null"], "minimum": 0 },
        "cf_cache_status": { "type": ["string", "null"] },
        "etag": { "type": ["string", "null"] },
        "retry_after_s": { "type": ["integer", "null"], "minimum": 0 }
      }
    },
    "error": { "type": ["string", "null"] },
    "payload_sha256": { "type": ["string", "null"], "pattern": "^[0-9a-f]{64}$" },
    "payload": {
      "description": "Upstream response body embedded verbatim (any JSON value); JSON null when the call failed."
    }
  },
  "if": {
    "properties": { "outcome": { "const": "ok" } },
    "required": ["outcome"]
  },
  "then": {
    "properties": {
      "payload": { "not": { "type": "null" } },
      "idempotency_key": { "$ref": "../common/defs.v1.json#/$defs/sha256_hex" },
      "payload_sha256": { "$ref": "../common/defs.v1.json#/$defs/sha256_hex" }
    }
  },
  "else": {
    "properties": {
      "payload": { "type": "null" }
    }
  }
}
```

`schemas/pipeline/rejected_record.v1.json`:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/pipeline/rejected_record.v1.json",
  "title": "S3 rejected record: a record dropped by a cleaning rule (data/rejected/)",
  "description": "STRICT. record holds the record as the transformer saw it (business fields); its inner shape is not constrained.",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "dataset",
    "reason",
    "record_key",
    "event_date",
    "record",
    "_event_id",
    "_raw_object",
    "_kafka_partition",
    "_kafka_offset",
    "_processed_at"
  ],
  "properties": {
    "schema_version": { "const": 1 },
    "dataset": {
      "enum": ["market_snapshot", "coin_snapshot", "coin_market_by_currency", "ticker", "coin_contract"]
    },
    "reason": {
      "enum": ["missing_id", "null_price", "nonpositive_price", "empty_platform", "empty_record", "snapshot_conflict"]
    },
    "record_key": { "type": "string", "minLength": 1 },
    "event_date": { "$ref": "../common/defs.v1.json#/$defs/date" },
    "record": { "type": "object" },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  }
}
```

`schemas/pipeline/dlq_record.v1.json`. `error_class` is a pattern and not an enum, so every stage can add a class without a schema change, and it stays safe as a metric label:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/pipeline/dlq_record.v1.json",
  "title": "S3 dead-letter record: input that broke a stage and needs a human (data/dlq/)",
  "description": "STRICT. original_base64 carries the offending bytes unchanged (they may not be valid JSON).",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "stage",
    "error_class",
    "violations",
    "origin",
    "original_base64",
    "occurred_at"
  ],
  "properties": {
    "schema_version": { "const": 1 },
    "stage": { "enum": ["export", "publish", "transform_input", "transform_output"] },
    "error_class": {
      "description": "Low-cardinality class used as a metric label, e.g. decode_error, schema_violation, integrity_error, source_schema_violation, transform_error.",
      "type": "string",
      "pattern": "^[a-z][a-z0-9_]{0,63}$"
    },
    "violations": {
      "type": "array",
      "maxItems": 20,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["instance_location", "keyword_location", "message"],
        "properties": {
          "instance_location": { "type": "string" },
          "keyword_location": { "type": "string" },
          "message": { "type": "string" }
        }
      }
    },
    "origin": {
      "type": "object",
      "additionalProperties": false,
      "required": ["kind"],
      "properties": {
        "kind": { "enum": ["postgres", "s3", "kafka"] },
        "topic": { "type": "string", "minLength": 1 },
        "partition": { "type": "integer", "minimum": 0 },
        "offset": { "type": "integer", "minimum": 0 },
        "bucket": { "type": "string", "minLength": 1 },
        "object_key": { "type": "string", "minLength": 1 },
        "line_no": { "type": "integer", "minimum": 1 },
        "batch_id": { "type": "integer", "minimum": 1 },
        "seq": { "type": "integer", "minimum": 1 },
        "event_id": { "type": "string" },
        "dataset": { "type": "string", "minLength": 1 },
        "record_key": { "type": "string", "minLength": 1 }
      }
    },
    "error": { "description": "Optional human-readable error text (for example a Go error message).", "type": "string" },
    "original_base64": { "type": "string", "pattern": "^[A-Za-z0-9+/]*={0,2}$" },
    "occurred_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  }
}
```

`schemas/embed.go` for **this task**. `go:embed` patterns cannot use `..`, so the package must live next to the files. `testdata/` is deliberately not embedded:
```go
// Package schemas embeds the JSON Schema files of the pipeline so every
// binary carries the schema version it was built with. testdata/ is not
// embedded: fixtures are for tests only.
package schemas

import "embed"

// FS holds every schema file, at paths that mirror the $id
// (https://schemas.cg-etl.internal/<path>). One //go:embed line per
// directory: a pattern that matches no file is a compile error, so a
// directory is added here in the task that creates its first schema.
//
//go:embed common/*.json
//go:embed pipeline/*.json
var FS embed.FS
```
Task 4 adds the line `//go:embed source/coingecko/*.json` and Task 12 adds `//go:embed processed/*.json`, both directly above `var FS embed.FS`. Nothing else changes.

- [ ] **Step 3: Write the failing registry tests**

`internal/validation/registry_test.go`. It is an internal test (`package validation`), so it can use the unexported `baseURL` and `schemas` map. `fstest.MapFS` builds throwaway schema trees for the negative cases:
```go
package validation

import (
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"coingecko-etl/schemas"
)

func TestLoadEmbeddedSchemas(t *testing.T) {
	r, err := Load(schemas.FS)
	if err != nil {
		t.Fatalf("Load(schemas.FS): %v", err)
	}
	for _, id := range []string{IDRawEvent, IDRejectedRecord, IDDLQRecord} {
		if _, ok := r.schemas[id]; !ok {
			t.Errorf("schema %s not registered", id)
		}
	}
}

// mapFS builds an in-memory schema tree; each file's $id is derived from
// its path so Load's $id check passes.
func mapFS(files map[string]string) fstest.MapFS {
	m := fstest.MapFS{}
	for p, body := range files {
		m[p] = &fstest.MapFile{Data: []byte(fmt.Sprintf(body, baseURL+p))}
	}
	return m
}

func TestLoadRejectsExternalRef(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":%q,
			"$ref":"https://example.com/not-embedded.json"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load succeeded with a $ref to a non-embedded URL; want error")
	}
}

func TestLoadRejectsRelativeRefToMissingFile(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":%q,
			"$ref":"../common/missing.v1.json#/$defs/x"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load succeeded with a $ref to a missing file; want error")
	}
}

func TestLoadRejectsIDNotMatchingPath(t *testing.T) {
	fsys := fstest.MapFS{"t/a.v1.json": &fstest.MapFile{Data: []byte(
		`{"$id":"https://schemas.cg-etl.internal/t/other.v1.json","type":"object"}`)}}
	_, err := Load(fsys)
	if err == nil || !strings.Contains(err.Error(), "$id") {
		t.Fatalf("Load err = %v, want $id mismatch error", err)
	}
}

func TestLoadRejectsDuplicateKeysInSchema(t *testing.T) {
	fsys := mapFS(map[string]string{
		"t/a.v1.json": `{"$id":%q,"type":"object","type":"string"}`,
	})
	if _, err := Load(fsys); err == nil {
		t.Fatal("Load accepted a schema file with duplicate keys")
	}
}

func TestLoadRejectsEmptyFS(t *testing.T) {
	if _, err := Load(fstest.MapFS{}); err == nil {
		t.Fatal("Load(empty) succeeded; want error")
	}
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := Load(mapFS(map[string]string{
		"t/obj.v1.json": `{"$id":%q,"type":"object","required":["id"],
			"properties":{"id":{"type":"string"},"ts":{"$ref":"defs.v1.json#/$defs/ts"}}}`,
		"t/defs.v1.json": `{"$id":%q,"$defs":{"ts":{"type":"string","format":"date-time"}}}`,
		"t/list.v1.json": `{"$id":%q,"type":"array","items":{"type":"string"}}`,
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return r
}

func TestValidateRejectsInvalidJSON(t *testing.T) {
	r := testRegistry(t)
	id := baseURL + "t/obj.v1.json"
	cases := map[string][]byte{
		"syntax":         []byte(`{"id":`),
		"duplicate name": []byte(`{"id":"a","id":"b"}`),
		"invalid utf-8":  []byte("{\"id\":\"\xff\"}"),
		"trailing data":  []byte(`{"id":"a"} {}`),
		"empty":          []byte(``),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Validate(id, doc); err == nil {
				t.Fatalf("Validate(%q) err = nil, want invalid JSON error", doc)
			}
		})
	}
}

func TestValidateUnknownSchema(t *testing.T) {
	r := testRegistry(t)
	if _, err := r.Validate(baseURL+"t/nope.v1.json", []byte(`{}`)); err == nil {
		t.Fatal("Validate with unknown id: err = nil")
	}
}

func TestValidateValidDocument(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{"id":"a","ts":"2026-10-10T10:07:55.000Z"}`))
	if err != nil || vs != nil {
		t.Fatalf("Validate = %v, %v; want nil, nil", vs, err)
	}
}

func TestViolationShapeThroughRef(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{"id":"a","ts":"yesterday"}`))
	if err != nil {
		t.Fatal(err)
	}
	want := Violation{
		InstanceLocation: "/ts",
		KeywordLocation:  baseURL + "t/defs.v1.json#/$defs/ts/format",
	}
	if len(vs) != 1 || vs[0].InstanceLocation != want.InstanceLocation || vs[0].KeywordLocation != want.KeywordLocation {
		t.Fatalf("violations = %+v, want one at %+v", vs, want)
	}
	// The message must survive the $ref hop (BasicOutput loses it).
	if !strings.Contains(vs[0].Message, "date-time") {
		t.Errorf("message = %q, want it to mention date-time", vs[0].Message)
	}
	if got := Keyword(vs[0]); got != "format" {
		t.Errorf("Keyword = %q, want format", got)
	}
}

func TestViolationAtRoot(t *testing.T) {
	r := testRegistry(t)
	vs, err := r.Validate(baseURL+"t/obj.v1.json", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 1 || vs[0].InstanceLocation != "" ||
		vs[0].KeywordLocation != baseURL+"t/obj.v1.json#/required" || Keyword(vs[0]) != "required" {
		t.Fatalf("violations = %+v", vs)
	}
}

func TestViolationsCappedAndSorted(t *testing.T) {
	r := testRegistry(t)
	var items []string
	for i := 0; i < 30; i++ {
		items = append(items, fmt.Sprint(i))
	}
	doc := []byte("[" + strings.Join(items, ",") + "]") // 30 numbers, each violates items.type
	vs, err := r.Validate(baseURL+"t/list.v1.json", doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != 20 {
		t.Fatalf("got %d violations, want 20", len(vs))
	}
	for i := 1; i < len(vs); i++ {
		if vs[i-1].InstanceLocation > vs[i].InstanceLocation {
			t.Fatalf("violations not sorted: %q before %q", vs[i-1].InstanceLocation, vs[i].InstanceLocation)
		}
	}
	again, _ := r.Validate(baseURL+"t/list.v1.json", doc)
	if fmt.Sprint(again) != fmt.Sprint(vs) {
		t.Fatal("Validate is not deterministic")
	}
}

func TestKeyword(t *testing.T) {
	cases := map[string]string{
		"https://x/a.json#/properties/a/type": "type",
		"https://x/a.json#/required":          "required",
		"https://x/a.json#/$defs/a~1b":        "a/b",
		"":                                    "",
	}
	for loc, want := range cases {
		if got := Keyword(Violation{KeywordLocation: loc}); got != want {
			t.Errorf("Keyword(%q) = %q, want %q", loc, got, want)
		}
	}
}
```

Run:
```bash
go test ./internal/validation/
```
Expected: FAIL to build with `undefined: Load`, `undefined: baseURL`, `undefined: Registry`, `undefined: Violation` (among others).

- [ ] **Step 4: Implement the ids and the registry**

`internal/validation/ids.go`:
```go
package validation

// Schema $id constants. The path after the host is the file path under
// schemas/ (Load enforces this).
const (
	IDRawEvent             = "https://schemas.cg-etl.internal/pipeline/raw_event.v1.json"
	IDRejectedRecord       = "https://schemas.cg-etl.internal/pipeline/rejected_record.v1.json"
	IDDLQRecord            = "https://schemas.cg-etl.internal/pipeline/dlq_record.v1.json"
	IDSourceMarkets        = "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"
	IDSourceCoin           = "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json"
	IDMarketSnapshot       = "https://schemas.cg-etl.internal/processed/market_snapshot.v1.json"
	IDCoinSnapshot         = "https://schemas.cg-etl.internal/processed/coin_snapshot.v1.json"
	IDCoinMarketByCurrency = "https://schemas.cg-etl.internal/processed/coin_market_by_currency.v1.json"
	IDTicker               = "https://schemas.cg-etl.internal/processed/ticker.v1.json"
	IDCoinContract         = "https://schemas.cg-etl.internal/processed/coin_contract.v1.json"
)

// baseURL is the common prefix of every $id.
const baseURL = "https://schemas.cg-etl.internal/"
```

`internal/validation/registry.go`:
- Leaves are sorted before capping, so the output is deterministic, which DLQ checksums need.
- `keywordLocation` works around v6.0.3 returning a nil `KeywordPath()` for `not`.
- `kind` is the package `github.com/santhosh-tekuri/jsonschema/v6/kind`, which holds the concrete error kinds.
```go
// Package validation compiles the embedded JSON Schemas once and validates
// documents against them, returning flat, loggable violations.
package validation

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// maxViolations caps how many violations Validate returns (and a DLQ record
// carries).
const maxViolations = 20

// Violation is one leaf validation error.
type Violation struct {
	InstanceLocation string `json:"instance_location"` // JSON pointer into the document, "" = root
	KeywordLocation  string `json:"keyword_location"`  // absolute schema URL + "#" + JSON pointer to the failing keyword
	Message          string `json:"message"`
}

// Registry holds every compiled schema keyed by $id. Compiled schemas are
// immutable, so a Registry is safe for concurrent use.
type Registry struct {
	schemas map[string]*jsonschema.Schema
}

var printer = message.NewPrinter(language.English)

// Load compiles every *.json file under fsys. Each file must be valid JSON
// (no duplicate keys) and its $id must equal baseURL + its path in fsys.
// Any $ref that does not point at a file in fsys fails compilation:
// the loader has no URL schemes registered, so nothing is fetched from
// disk or the network.
func Load(fsys fs.FS) (*Registry, error) {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()                          // "format" is an assertion, not an annotation
	c.UseLoader(jsonschema.SchemeURLLoader{}) // empty: every non-embedded $ref is an error

	var ids []string
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || path.Ext(p) != ".json" {
			return nil
		}
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		if err := checkJSON(b); err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return fmt.Errorf("validation: schema %s: not a JSON object", p)
		}
		id, _ := obj["$id"].(string)
		if want := baseURL + p; id != want {
			return fmt.Errorf("validation: schema %s: $id %q, want %q", p, id, want)
		}
		if err := c.AddResource(id, doc); err != nil {
			return fmt.Errorf("validation: schema %s: %w", p, err)
		}
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("validation: no *.json schemas found")
	}
	sort.Strings(ids)
	r := &Registry{schemas: make(map[string]*jsonschema.Schema, len(ids))}
	for _, id := range ids {
		s, err := c.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("validation: compile %s: %w", id, err)
		}
		r.schemas[id] = s
	}
	return r, nil
}

// Validate checks doc against the schema with the given $id. It returns a
// non-nil error when the id is unknown or doc is not valid JSON (syntax
// error, duplicate object member names, invalid UTF-8, trailing data).
// Otherwise it returns the leaf violations (nil when valid), sorted and
// capped at 20.
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error) {
	s, ok := r.schemas[schemaID]
	if !ok {
		return nil, fmt.Errorf("validation: unknown schema %q", schemaID)
	}
	if err := checkJSON(doc); err != nil {
		return nil, err
	}
	// UnmarshalJSON decodes numbers as json.Number, so values above 2^53
	// keep their exact digits.
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("validation: invalid JSON: %w", err)
	}
	err = s.Validate(inst)
	if err == nil {
		return nil, nil
	}
	var ve *jsonschema.ValidationError
	if !errors.As(err, &ve) {
		return nil, fmt.Errorf("validation: %w", err)
	}
	return leaves(ve), nil
}

// Keyword returns the last token of v.KeywordLocation (e.g. "required",
// "type", "format"), for use as a low-cardinality metric label.
func Keyword(v Violation) string {
	loc := v.KeywordLocation
	if i := strings.LastIndexByte(loc, '#'); i >= 0 {
		loc = loc[i+1:]
	}
	if i := strings.LastIndexByte(loc, '/'); i >= 0 {
		loc = loc[i+1:]
	}
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(loc)
}

// checkJSON rejects bytes that are not exactly one valid JSON value.
// encoding/json/jsontext (standard library in Go 1.27) reports duplicate
// object member names and invalid UTF-8 as errors by default, which
// encoding/json v1 and the schema validator silently accept.
func checkJSON(b []byte) error {
	dec := jsontext.NewDecoder(bytes.NewReader(b))
	if err := dec.SkipValue(); err != nil {
		return fmt.Errorf("validation: invalid JSON: %w", err)
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return errors.New("validation: invalid JSON: trailing data after top-level value")
	}
	return nil
}

// leaves walks the ValidationError tree down to the leaf causes. It does
// not use BasicOutput(): in v6.0.3 BasicOutput replaces the message of
// errors reached through $ref with "validation failed".
func leaves(root *jsonschema.ValidationError) []Violation {
	var out []Violation
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			out = append(out, Violation{
				InstanceLocation: pointer(e.InstanceLocation),
				KeywordLocation:  keywordLocation(e),
				Message:          e.ErrorKind.LocalizedString(printer),
			})
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(root)
	// Sort for deterministic output: DLQ objects must be byte-identical
	// when a batch is reprocessed.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.InstanceLocation != b.InstanceLocation {
			return a.InstanceLocation < b.InstanceLocation
		}
		if a.KeywordLocation != b.KeywordLocation {
			return a.KeywordLocation < b.KeywordLocation
		}
		return a.Message < b.Message
	})
	if len(out) > maxViolations {
		out = out[:maxViolations]
	}
	return out
}

// keywordLocation is the absolute location of the failing keyword:
// e.SchemaURL is "<$id>#<pointer to the subschema>"; the keyword's own
// path (ErrorKind.KeywordPath) is appended to it. v6.0.3 returns a nil
// KeywordPath for "not", so that keyword is appended by hand.
func keywordLocation(e *jsonschema.ValidationError) string {
	u := e.SchemaURL
	if !strings.Contains(u, "#") {
		u += "#"
	}
	kp := e.ErrorKind.KeywordPath()
	if _, ok := e.ErrorKind.(*kind.Not); ok && len(kp) == 0 {
		kp = []string{"not"}
	}
	return u + pointer(kp)
}

// pointer renders tokens as an RFC 6901 JSON pointer ("" for the root).
func pointer(tokens []string) string {
	var sb strings.Builder
	for _, t := range tokens {
		sb.WriteByte('/')
		sb.WriteString(strings.NewReplacer("~", "~0", "/", "~1").Replace(t))
	}
	return sb.String()
}
```

Run:
```bash
go test -race ./internal/validation/
```
Expected: PASS (`ok  coingecko-etl/internal/validation`).

- [ ] **Step 5: Write the fixture contract test and the fixtures**

The fixture convention, which Task 12/13 reuses for processed schemas, is as follows:
- `schemas/testdata/<schema path without .json>/valid/*.json` must yield zero violations.
- `.../invalid/*.json` holds a wrapper `{"expect":{"instance_location":"<JSON pointer>","keyword":"<last keyword token>"},"doc":<document>}`. The test passes if any returned violation matches both fields.
- `doc` is read as `json.RawMessage`, so its bytes reach the validator unchanged.

`internal/validation/contract_test.go`:
```go
package validation

import (
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"coingecko-etl/schemas"
)

// Fixture layout (schemas/testdata):
//
//	<schema path without .json>/valid/*.json    must produce zero violations
//	<schema path without .json>/invalid/*.json  {"expect":{"instance_location":..,"keyword":..},"doc":<document>}
//
// e.g. pipeline/raw_event.v1/valid/ok_markets.json is checked against
// https://schemas.cg-etl.internal/pipeline/raw_event.v1.json. Directories
// with any other shape (testdata/coingecko/ recorded responses) are ignored
// here and have their own tests.
const testdataDir = "../../schemas/testdata"

type invalidFixture struct {
	Expect struct {
		InstanceLocation string `json:"instance_location"`
		Keyword          string `json:"keyword"`
	} `json:"expect"`
	Doc json.RawMessage `json:"doc"` // kept byte-for-byte
}

func TestSchemaFixtures(t *testing.T) {
	r, err := Load(schemas.FS)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	seen := map[string]map[string]int{} // schema id -> "valid"/"invalid" -> count
	fsys := os.DirFS(testdataDir)
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path.Ext(p) != ".json" {
			return err
		}
		kindDir := path.Dir(p)
		kind := path.Base(kindDir)
		if kind != "valid" && kind != "invalid" {
			return nil
		}
		id := baseURL + path.Dir(kindDir) + ".json"
		if seen[id] == nil {
			seen[id] = map[string]int{}
		}
		seen[id][kind]++
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		t.Run(p, func(t *testing.T) {
			if kind == "valid" {
				vs, err := r.Validate(id, b)
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				if len(vs) != 0 {
					t.Fatalf("valid fixture has violations: %+v", vs)
				}
				return
			}
			var f invalidFixture
			if err := json.Unmarshal(b, &f); err != nil {
				t.Fatalf("fixture wrapper: %v", err)
			}
			if f.Expect.Keyword == "" || len(f.Doc) == 0 {
				t.Fatal("fixture wrapper needs expect.keyword and doc")
			}
			vs, err := r.Validate(id, f.Doc)
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			for _, v := range vs {
				if v.InstanceLocation == f.Expect.InstanceLocation && Keyword(v) == f.Expect.Keyword {
					if !strings.HasPrefix(v.KeywordLocation, "https://schemas.cg-etl.internal/") || v.Message == "" {
						t.Fatalf("malformed violation %+v", v)
					}
					return
				}
			}
			t.Fatalf("want violation (%q, %s), got %+v", f.Expect.InstanceLocation, f.Expect.Keyword, vs)
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Every strict pipeline schema must have both kinds of fixtures.
	for _, id := range []string{IDRawEvent, IDRejectedRecord, IDDLQRecord} {
		if seen[id]["valid"] == 0 || seen[id]["invalid"] == 0 {
			t.Errorf("%s: need valid and invalid fixtures, have %v", id, seen[id])
		}
	}
}
```

Create the fixture files below with exactly this content (one file per block):

`schemas/testdata/pipeline/dlq_record.v1/invalid/error_class_uppercase.json`:
```json
{"expect":{"instance_location":"/error_class","keyword":"pattern"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"SchemaViolation","violations":[],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/occurred_at_no_millis.json`:
```json
{"expect":{"instance_location":"/occurred_at","keyword":"pattern"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/origin_extra_field.json`:
```json
{"expect":{"instance_location":"/origin","keyword":"additionalProperties"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[],"origin":{"kind":"kafka","host":"x","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/original_not_base64.json`:
```json
{"expect":{"instance_location":"/original_base64","keyword":"pattern"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"{}","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/too_many_violations.json`:
```json
{"expect":{"instance_location":"/violations","keyword":"maxItems"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"},{"instance_location":"","keyword_location":"k","message":"m"}],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/unknown_origin_kind.json`:
```json
{"expect":{"instance_location":"/origin/kind","keyword":"enum"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[],"origin":{"kind":"redis","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/unknown_stage.json`:
```json
{"expect":{"instance_location":"/stage","keyword":"enum"},
 "doc":{"schema_version":1,"stage":"transform","error_class":"schema_violation","violations":[],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/invalid/violation_missing_message.json`:
```json
{"expect":{"instance_location":"/violations/0","keyword":"required"},
 "doc":{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[{"instance_location":"","keyword_location":"k"}],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/dlq_record.v1/valid/decode_error_from_s3.json`:
```json
{"schema_version":1,"stage":"publish","error_class":"decode_error","violations":[],"origin":{"kind":"s3","bucket":"raw","object_key":"source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl","line_no":3,"batch_id":42},"original_base64":"eyJhIjoxLCJhIjoyfQ==","occurred_at":"2026-10-10T10:08:30.000Z"}
```

`schemas/testdata/pipeline/dlq_record.v1/valid/export_postgres.json`:
```json
{"schema_version":1,"stage":"export","error_class":"schema_violation","violations":[{"instance_location":"","keyword_location":"https://schemas.cg-etl.internal/pipeline/raw_event.v1.json#/required","message":"missing property 'payload'"}],"origin":{"kind":"postgres","batch_id":42,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11"},"original_base64":"","occurred_at":"2026-10-10T10:08:30.000Z"}
```

`schemas/testdata/pipeline/dlq_record.v1/valid/schema_violation.json`:
```json
{"schema_version":1,"stage":"transform_input","error_class":"schema_violation","violations":[{"instance_location":"/fetched_at","keyword_location":"https://schemas.cg-etl.internal/common/defs.v1.json#/$defs/utc_timestamp/pattern","message":"'2026-10-10T10:02:33+00:00' does not match pattern"}],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11"},"original_base64":"eyJzY2hlbWFfdmVyc2lvbiI6MX0=","occurred_at":"2026-10-10T10:08:30.512Z"}
```

`schemas/testdata/pipeline/dlq_record.v1/valid/export_with_seq.json` (the exporter's origin carries the `raw.api_calls.seq` of the row):
```json
{"schema_version":1,"stage":"export","error_class":"decode_error","violations":[{"instance_location":"","keyword_location":"","message":"payload is not valid JSON"}],"origin":{"kind":"postgres","batch_id":42,"seq":1077,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11"},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.000Z"}
```

`schemas/testdata/pipeline/dlq_record.v1/valid/transform_output.json` (a processed row that failed S2; `error` is optional free text):
```json
{"schema_version":1,"stage":"transform_output","error_class":"output_schema_violation","error":"processed row fails its schema","violations":[{"instance_location":"/price","keyword_location":"https://schemas.cg-etl.internal/processed/market_snapshot.v1.json#/properties/price/type","message":"got null, want string"}],"origin":{"kind":"kafka","topic":"coingecko.raw","partition":0,"offset":815,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","dataset":"market_snapshot","record_key":"bitcoin|2026-10-10T10:07:55.000Z"},"original_base64":"e30=","occurred_at":"2026-10-10T10:08:30.512Z"}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/bad_event_id.json`:
```json
{"expect":{"instance_location":"/event_id","keyword":"format"},
 "doc":{"schema_version":1,"event_id":"not-a-uuid","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/extra_field.json`:
```json
{"expect":{"instance_location":"","keyword":"additionalProperties"},
 "doc":{"extra":1,"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/failed_with_payload.json`:
```json
{"expect":{"instance_location":"/payload","keyword":"type"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":null,"source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"http_error","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":null,"payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/fetched_at_garbage.json`:
```json
{"expect":{"instance_location":"/fetched_at","keyword":"format"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-13-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/fetched_at_offset.json`:
```json
{"expect":{"instance_location":"/fetched_at","keyword":"pattern"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120+00:00","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/http_status_string.json`:
```json
{"expect":{"instance_location":"/http/status","keyword":"type"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":"200","latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/missing_payload_sha256.json`:
```json
{"expect":{"instance_location":"","keyword":"required"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/ok_null_idempotency_key.json`:
```json
{"expect":{"instance_location":"/idempotency_key","keyword":"type"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":null,"source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/ok_null_payload.json`:
```json
{"expect":{"instance_location":"/payload","keyword":"not"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":null}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/schema_version_2.json`:
```json
{"expect":{"instance_location":"/schema_version","keyword":"const"},
 "doc":{"schema_version":2,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/invalid/unknown_endpoint.json`:
```json
{"expect":{"instance_location":"/endpoint","keyword":"enum"},
 "doc":{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"/coins/markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":1,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[]}}
```

`schemas/testdata/pipeline/raw_event.v1/valid/http_error_429.json`:
```json
{"schema_version":1,"event_id":"0199d0a6-0000-7000-8000-000000000002","idempotency_key":null,"source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{}},"fetched_at":"2026-10-10T10:03:00.000Z","outcome":"http_error","http":{"status":429,"latency_ms":80,"date":"Sat, 10 Oct 2026 10:03:00 GMT","age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":60},"error":"http 429","payload_sha256":null,"payload":null}
```

`schemas/testdata/pipeline/raw_event.v1/valid/network_error.json`:
```json
{"schema_version":1,"event_id":"0199d0a6-0000-7000-8000-000000000003","idempotency_key":null,"source":"coingecko","endpoint":"coin_detail","request":{"path":"/coins/tether","params":{}},"fetched_at":"2026-10-10T10:04:00.000Z","outcome":"network_error","http":null,"error":"context deadline exceeded","payload_sha256":null,"payload":null}
```

`schemas/testdata/pipeline/raw_event.v1/valid/ok_coin_detail.json`:
```json
{"schema_version":1,"event_id":"0199d0a5-0a00-7000-8000-000000000001","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coin_detail","request":{"path":"/coins/bitcoin","params":{"localization":"false","tickers":"true"}},"fetched_at":"2026-10-10T10:02:48.000Z","outcome":"ok","http":{"status":200,"latency_ms":95,"date":null,"age_s":12,"cf_cache_status":"HIT","etag":null,"retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":{"id":"bitcoin","market_data":{"market_cap":{"vnd":43083803721443304}}}}
```

`schemas/testdata/pipeline/raw_event.v1/valid/ok_markets.json`:
```json
{"schema_version":1,"event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","idempotency_key":"0b8e2c7e5f3c1a9d4e6f8a0b2c4d6e8f0a1b3c5d7e9f1a2b4c6d8e0f2a4b6c8d","source":"coingecko","endpoint":"coins_markets","request":{"path":"/coins/markets","params":{"order":"market_cap_desc","page":"1","per_page":"100","vs_currency":"usd"}},"fetched_at":"2026-10-10T10:02:33.120Z","outcome":"ok","http":{"status":200,"latency_ms":412,"date":"Sat, 10 Oct 2026 10:02:33 GMT","age_s":null,"cf_cache_status":"MISS","etag":"W/\"c4924cc46ddaa91ee4660396b13c9c63\"","retry_after_s":null},"error":null,"payload_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","payload":[{"id":"bitcoin","symbol":"btc","name":"Bitcoin","current_price":82776,"market_cap":1663572458130,"last_updated":"2026-10-10T10:07:55.000Z"}]}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/bad_event_date.json`:
```json
{"expect":{"instance_location":"/event_date","keyword":"format"},
 "doc":{"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10T00:00:00Z","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/missing_raw_object.json`:
```json
{"expect":{"instance_location":"","keyword":"required"},
 "doc":{"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/negative_offset.json`:
```json
{"expect":{"instance_location":"/_kafka_offset","keyword":"minimum"},
 "doc":{"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":-1,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/record_not_object.json`:
```json
{"expect":{"instance_location":"/record","keyword":"type"},
 "doc":{"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":"x","_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/schema_version_field.json`:
```json
{"expect":{"instance_location":"","keyword":"additionalProperties"},
 "doc":{"_schema_version":1,"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/unknown_dataset.json`:
```json
{"expect":{"instance_location":"/dataset","keyword":"enum"},
 "doc":{"schema_version":1,"dataset":"markets","reason":"null_price","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/invalid/unknown_reason.json`:
```json
{"expect":{"instance_location":"/reason","keyword":"enum"},
 "doc":{"schema_version":1,"dataset":"market_snapshot","reason":"too_cheap","record_key":"x|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"raw/k.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}}
```

`schemas/testdata/pipeline/rejected_record.v1/valid/null_price.json`:
```json
{"schema_version":1,"dataset":"market_snapshot","reason":"null_price","record_key":"some-coin|2026-10-10T10:07:55.000Z","event_date":"2026-10-10","record":{"coin_id":"some-coin","current_price":null},"_event_id":"0199d0a4-6c1e-7cc2-9a51-3f1f2b9d8e11","_raw_object":"source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl","_kafka_partition":0,"_kafka_offset":815,"_processed_at":"2026-10-10T10:08:30.512Z"}
```

`schemas/testdata/pipeline/rejected_record.v1/valid/snapshot_conflict.json`:
```json
{"schema_version":1,"dataset":"ticker","reason":"snapshot_conflict","record_key":"bitcoin|binance|BTC|USDT|2026-10-10T10:01:51.000Z","event_date":"2026-10-10","record":{"coin_id":"bitcoin","exchange_id":"binance","last":"82878.57"},"_event_id":"0199d0a5-0a00-7000-8000-000000000001","_raw_object":"source=coingecko/ingest_date=2026-10-10/batch=00000000000000000043.jsonl","_kafka_partition":0,"_kafka_offset":816,"_processed_at":"2026-10-10T10:09:00.000Z"}
```

Run:
```bash
go mod tidy
go vet ./...
go test -race -count=1 ./internal/validation/
```
Expected: PASS. If an invalid fixture reports a different keyword, fix the **schema** so the intended rule fires; do not edit the expectation to match a wrong schema. Note that when both `format` and `pattern` fail on one value, v6 reports only `format` (see `bad_event_date.json`).

- [ ] **Step 6: Add the fuzz test and the schema-evolution test (spec §5.2, §5.3)**

Spec §5.3 asks that fuzzed structs marshal to valid documents, and §5.2 that a new schema version still accepts every fixture of the previous one. The new test file uses `event.Marshal` from Task 2. The fuzz seeds run as ordinary tests under `go test`; `go test -fuzz=FuzzRawEventMarshalsToValidS1 ./internal/validation/` explores further. With only `v1` schemas the evolution test finds no pair and passes; it starts checking the day a `v2` file is added next to a `v1` file.

`internal/validation/evolution_test.go`:
```go
package validation_test

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

// FuzzRawEventMarshalsToValidS1: every ok event the fetcher can build (any
// strict-JSON body except null, any string params, any 2xx status) marshals to a
// document that passes S1. A failure here means event.Marshal and
// raw_event.v1.json have drifted apart.
func FuzzRawEventMarshalsToValidS1(f *testing.F) {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(true, "vs_currency", "usd", []byte(`[{"id":"bitcoin","current_price":82878.57}]`), 200, int64(12), 34)
	f.Add(false, "localization", "false", []byte(`{"id":"bitcoin","name":"\u00e9 \"q\" <b>"}`), 203, int64(0), 0)
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
			FetchedAt:      time.Date(2026, 10, 10, 10, 2, 33, 120_000_000, time.UTC).Format("2006-01-02T15:04:05.000Z"),
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

var versionedSchema = regexp.MustCompile(`^(.+)\.v([0-9]+)\.json$`)

// TestNewerSchemaVersionsAcceptOlderFixtures: for every schema <name>.vN.json
// with N >= 2, every valid fixture of <name>.v(N-1) must pass the vN schema
// (a compatible change). A breaking change gets a new name instead.
func TestNewerSchemaVersionsAcceptOlderFixtures(t *testing.T) {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	pairs := 0
	err = fs.WalkDir(schemas.FS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		m := versionedSchema.FindStringSubmatch(p)
		if m == nil {
			return nil
		}
		n, _ := strconv.Atoi(m[2])
		if n < 2 {
			return nil
		}
		prev := fmt.Sprintf("%s.v%d", path.Base(m[1]), n-1)
		dir := filepath.Join("..", "..", "schemas", "testdata", filepath.FromSlash(path.Dir(p)), prev, "valid")
		files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
		if len(files) == 0 {
			t.Errorf("%s: no valid fixtures of %s under %s", p, prev, dir)
			return nil
		}
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				return err
			}
			if vs, err := reg.Validate("https://schemas.cg-etl.internal/"+p, b); err != nil || len(vs) > 0 {
				t.Errorf("%s rejects %s: err=%v violations=%+v", p, f, err, vs)
			}
		}
		pairs++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d schema version pairs checked", pairs)
}
```

Run:
```bash
go vet ./internal/validation/
go test -race -count=1 -run 'FuzzRawEventMarshalsToValidS1|TestNewerSchemaVersionsAcceptOlderFixtures' -v ./internal/validation/
go test -fuzz=FuzzRawEventMarshalsToValidS1 -fuzztime=20s ./internal/validation/
```
Expected: both PASS (the evolution test logs `0 schema version pairs checked`), and 20 s of fuzzing finds no failing input. A crasher means S1 or `event.Marshal` must be fixed, not the fuzz guard.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum schemas/embed.go schemas/common schemas/pipeline schemas/testdata/pipeline internal/validation
git commit -m "$(cat <<'EOF'
feat(validation): JSON Schema registry, shared defs and pipeline schemas S1/S3

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 4: Source schemas S0 + recorded fixtures

**Files:**
- Create: `schemas/testdata/coingecko/` (7 recorded responses + their `.headers.txt`)
- Create: `schemas/source/coingecko/coins_markets.v1.json`, `schemas/source/coingecko/coin.v1.json`
- Modify: `schemas/embed.go` (add `//go:embed source/coingecko/*.json`)
- Test: `internal/validation/source_contract_test.go`

**Interfaces:**
- Consumes (Task 3):
```go
func Load(fsys fs.FS) (*Registry, error)
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)
func Keyword(v Violation) string
const IDSourceMarkets = "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"
const IDSourceCoin    = "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json"
var schemas.FS embed.FS
```
- Produces: the S0 schema files registered under `IDSourceMarkets` and `IDSourceCoin`, and the recorded fixtures at `schemas/testdata/coingecko/<file>` that the transform tasks use.

Background:
- S0 is an *observed contract* for an API we do not control, so it is permissive:
  - It never uses `additionalProperties: false` or `propertyNames`, so new upstream fields are fine.
  - It requires and types only the fields the transformer reads (spec §6.2).
  - It allows `null` where CoinGecko returns or documents it.
  - Currency maps (`{"usd": 1, "vnd": 43083803721443304, ...}`) are open objects with `additionalProperties: {"type": ["number","null"]}`.
  - `format: date-time` applies only to fields the transformer parses: `last_updated`, `ath_date`/`atl_date` and the ticker times.
- CoinGecko encodes "absent" as `null`, `""`, `[]` or a missing key. For optional timestamps, `"if": {"type":"string","minLength":1}, "then": {"format":"date-time"}` accepts `null` and `""` and still yields exactly one `format` violation for garbage such as `"yesterday"`.
- An S0 failure sends the whole response to `dlq/` (`source_schema_violation`). A single bad record must instead become a cleaning reject, so values that cleaning rules handle are allowed through: `""` id and `null` price.

- [ ] **Step 1: Copy the recorded CoinGecko responses into the repo**

```bash
mkdir -p schemas/testdata/coingecko
SRC=docs/superpowers/plans/fixtures/coingecko
for f in markets_usd250_t0 markets_top10_t0 markets_top10_t1 markets_top10_t2 \
         coin_bitcoin_full_t0 coin_bitcoin_full_t1 coin_usd-coin_meta; do
  cp "$SRC/$f.json" "$SRC/$f.headers.txt" schemas/testdata/coingecko/
done
ls schemas/testdata/coingecko | wc -l
```
Expected: `14`. The files are byte-for-byte live responses from 2026-10-10; never reformat them.
- `markets_usd250_t0.json` is 250 coins with `per_page=250`.
- `markets_top10_t*.json` are three consecutive polls of the top 10.
- `coin_bitcoin_full_t*.json` is `/coins/bitcoin` with market_data and 100 tickers; t0 and t1 are byte-identical (CDN cache).
- `coin_usd-coin_meta.json` is `/coins/usd-coin` without market_data and tickers, with 36 platforms.

- [ ] **Step 2: Profile the samples to confirm the observed types**

The nullability and requiredness decisions in Step 4 come from this output. Rerun it whenever samples are added:
```bash
python3 -I - schemas/testdata/coingecko <<'EOF'
import json, sys, collections, os
d = sys.argv[1]
def t(v): return {dict:'object',list:'array',str:'string',bool:'boolean',type(None):'null',int:'integer',float:'number'}[type(v)]
rows = json.load(open(os.path.join(d, 'markets_usd250_t0.json')))
agg = collections.defaultdict(collections.Counter)
for r in rows:
    for k, v in r.items(): agg[k][t(v)] += 1
for k, c in agg.items(): print('markets', k, dict(c))
for f in ('coin_bitcoin_full_t0.json', 'coin_usd-coin_meta.json'):
    c = json.load(open(os.path.join(d, f)))
    print(f, 'top-level:', {k: t(v) for k, v in c.items() if k in ('id','symbol','name','asset_platform_id','genesis_date','market_cap_rank','last_updated','market_data','tickers','platforms','detail_platforms','categories')})
    tk = c.get('tickers') or []
    tagg = collections.defaultdict(collections.Counter)
    for x in tk:
        for k, v in x.items(): tagg[k][t(v)] += 1
    for k, cnt in tagg.items(): print('  ticker', k, dict(cnt))
EOF
```
Expected (excerpt):
- markets: `current_price {'integer': 1, 'number': 249}`, `price_change_24h {'number': 249, 'null': 1}`, `max_supply {'number': 144, 'null': 106}`, `roi {'null': 223, 'object': 27}`, `last_updated {'string': 250}`.
- `coin_bitcoin_full_t0.json`: `genesis_date: string`, `asset_platform_id: null`, `market_data: object`, `tickers: array`; ticker `bid_ask_spread_percentage {'number': 98, 'null': 2}`, `trust_score {'null': 100}`, `target_coin_id {'string': 86}` (missing in 14).
- `coin_usd-coin_meta.json`: no `market_data`, no `tickers`, `genesis_date: null`.

Ticker timestamps use `+00:00` offsets (`2026-10-10T10:00:45+00:00`) while everything else uses `.000Z`. Both are valid RFC 3339 `date-time`.

- [ ] **Step 3: Write the failing S0 contract test**

`internal/validation/source_contract_test.go`. Mutations decode with `UseNumber()`, so re-encoding keeps 2^53+ integers exact. Each mutation must produce **exactly one** violation, which proves the schema has no unrelated noise on real data:
```go
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
```

Run:
```bash
go test -count=1 -run 'S0' ./internal/validation/
```
Expected: FAIL, e.g. `Validate: validation: unknown schema "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json"` (the schemas are not embedded yet).

- [ ] **Step 4: Write the S0 schemas and embed them**

`schemas/source/coingecko/coins_markets.v1.json`:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/source/coingecko/coins_markets.v1.json",
  "title": "S0 CoinGecko GET /coins/markets response (observed contract)",
  "description": "PERMISSIVE: requires and types only the fields the transformer reads; unknown fields are allowed. Metric fields allow null (documented by CoinGecko; the cleaning rules null_price and empty_record depend on nulls reaching the transformer). Empty-string ids are a cleaning reject (missing_id), not a schema failure.",
  "type": "array",
  "items": { "$ref": "#/$defs/coin" },
  "$defs": {
    "num": { "type": ["number", "null"] },
    "opt_datetime": {
      "description": "Absent is encoded as null or \"\"; any other string must be an RFC 3339 date-time.",
      "type": ["string", "null"],
      "if": { "type": "string", "minLength": 1 },
      "then": { "format": "date-time" }
    },
    "coin": {
      "type": "object",
      "required": ["id", "symbol", "name", "current_price", "last_updated"],
      "properties": {
        "id": { "type": "string" },
        "symbol": { "type": "string" },
        "name": { "type": "string" },
        "current_price": { "$ref": "#/$defs/num" },
        "market_cap": { "$ref": "#/$defs/num" },
        "market_cap_rank": { "type": ["integer", "null"] },
        "fully_diluted_valuation": { "$ref": "#/$defs/num" },
        "total_volume": { "$ref": "#/$defs/num" },
        "high_24h": { "$ref": "#/$defs/num" },
        "low_24h": { "$ref": "#/$defs/num" },
        "price_change_24h": { "$ref": "#/$defs/num" },
        "price_change_percentage_24h": { "$ref": "#/$defs/num" },
        "market_cap_change_24h": { "$ref": "#/$defs/num" },
        "market_cap_change_percentage_24h": { "$ref": "#/$defs/num" },
        "circulating_supply": { "$ref": "#/$defs/num" },
        "total_supply": { "$ref": "#/$defs/num" },
        "max_supply": { "$ref": "#/$defs/num" },
        "ath": { "$ref": "#/$defs/num" },
        "ath_change_percentage": { "$ref": "#/$defs/num" },
        "ath_date": { "$ref": "#/$defs/opt_datetime" },
        "atl": { "$ref": "#/$defs/num" },
        "atl_change_percentage": { "$ref": "#/$defs/num" },
        "atl_date": { "$ref": "#/$defs/opt_datetime" },
        "roi": {
          "type": ["object", "null"],
          "properties": {
            "times": { "$ref": "#/$defs/num" },
            "currency": { "type": ["string", "null"] },
            "percentage": { "$ref": "#/$defs/num" }
          }
        },
        "last_updated": { "type": "string", "format": "date-time" },
        "price_change_percentage_1h_in_currency": { "$ref": "#/$defs/num" },
        "price_change_percentage_24h_in_currency": { "$ref": "#/$defs/num" },
        "price_change_percentage_7d_in_currency": { "$ref": "#/$defs/num" }
      }
    }
  }
}
```

`schemas/source/coingecko/coin.v1.json`. It covers the 23 `market_data` currency maps (21 numeric, plus `ath_date`/`atl_date` as timestamp maps), platforms and tickers:
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/source/coingecko/coin.v1.json",
  "title": "S0 CoinGecko GET /coins/{id} response (observed contract)",
  "description": "PERMISSIVE: requires and types only the fields the transformer reads; unknown fields are allowed. market_data and tickers are optional because the endpoint omits them when market_data=false / tickers=false (see testdata/coingecko/coin_usd-coin_meta.json). Currency maps are open maps keyed by currency code.",
  "type": "object",
  "required": ["id", "symbol", "name", "last_updated"],
  "properties": {
    "id": { "type": "string" },
    "symbol": { "type": "string" },
    "name": { "type": "string" },
    "asset_platform_id": { "type": ["string", "null"] },
    "platforms": {
      "type": ["object", "null"],
      "additionalProperties": { "type": ["string", "null"] }
    },
    "detail_platforms": {
      "type": ["object", "null"],
      "additionalProperties": {
        "type": ["object", "null"],
        "properties": {
          "decimal_place": { "type": ["integer", "null"] },
          "contract_address": { "type": ["string", "null"] }
        }
      }
    },
    "categories": {
      "type": ["array", "null"],
      "items": { "type": ["string", "null"] }
    },
    "hashing_algorithm": { "type": ["string", "null"] },
    "block_time_in_minutes": { "type": ["integer", "null"] },
    "genesis_date": {
      "description": "YYYY-MM-DD, or null / \"\" when unknown.",
      "type": ["string", "null"],
      "if": { "type": "string", "minLength": 1 },
      "then": { "format": "date" }
    },
    "market_cap_rank": { "type": ["integer", "null"] },
    "last_updated": { "type": "string", "format": "date-time" },
    "market_data": {
      "type": "object",
      "properties": {
        "current_price": { "$ref": "#/$defs/currency_map" },
        "ath": { "$ref": "#/$defs/currency_map" },
        "ath_change_percentage": { "$ref": "#/$defs/currency_map" },
        "ath_date": { "$ref": "#/$defs/currency_datetime_map" },
        "atl": { "$ref": "#/$defs/currency_map" },
        "atl_change_percentage": { "$ref": "#/$defs/currency_map" },
        "atl_date": { "$ref": "#/$defs/currency_datetime_map" },
        "market_cap": { "$ref": "#/$defs/currency_map" },
        "fully_diluted_valuation": { "$ref": "#/$defs/currency_map" },
        "total_volume": { "$ref": "#/$defs/currency_map" },
        "high_24h": { "$ref": "#/$defs/currency_map" },
        "low_24h": { "$ref": "#/$defs/currency_map" },
        "price_change_24h_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_1h_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_24h_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_7d_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_14d_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_30d_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_60d_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_200d_in_currency": { "$ref": "#/$defs/currency_map" },
        "price_change_percentage_1y_in_currency": { "$ref": "#/$defs/currency_map" },
        "market_cap_change_24h_in_currency": { "$ref": "#/$defs/currency_map" },
        "market_cap_change_percentage_24h_in_currency": { "$ref": "#/$defs/currency_map" },
        "market_cap_rank": { "type": ["integer", "null"] },
        "total_supply": { "$ref": "#/$defs/num" },
        "max_supply": { "$ref": "#/$defs/num" },
        "circulating_supply": { "$ref": "#/$defs/num" },
        "roi": {
          "type": ["object", "null"],
          "properties": {
            "times": { "$ref": "#/$defs/num" },
            "currency": { "type": ["string", "null"] },
            "percentage": { "$ref": "#/$defs/num" }
          }
        },
        "last_updated": { "$ref": "#/$defs/opt_datetime" }
      }
    },
    "tickers": {
      "type": ["array", "null"],
      "items": { "$ref": "#/$defs/ticker" }
    }
  },
  "$defs": {
    "num": { "type": ["number", "null"] },
    "opt_datetime": {
      "description": "Absent is encoded as null or \"\"; any other string must be an RFC 3339 date-time.",
      "type": ["string", "null"],
      "if": { "type": "string", "minLength": 1 },
      "then": { "format": "date-time" }
    },
    "currency_map": {
      "description": "Currency code -> number, e.g. {\"usd\": 82776, \"vnd\": 43083803721443304}.",
      "type": ["object", "null"],
      "additionalProperties": { "type": ["number", "null"] }
    },
    "currency_datetime_map": {
      "type": ["object", "null"],
      "additionalProperties": { "$ref": "#/$defs/opt_datetime" }
    },
    "ticker": {
      "type": "object",
      "required": ["base", "target", "market"],
      "properties": {
        "base": { "type": "string" },
        "target": { "type": "string" },
        "market": {
          "type": "object",
          "properties": {
            "name": { "type": ["string", "null"] },
            "identifier": { "type": ["string", "null"] },
            "has_trading_incentive": { "type": ["boolean", "null"] }
          }
        },
        "last": { "$ref": "#/$defs/num" },
        "volume": { "$ref": "#/$defs/num" },
        "converted_last": { "$ref": "#/$defs/currency_map" },
        "converted_volume": { "$ref": "#/$defs/currency_map" },
        "trust_score": { "type": ["string", "null"] },
        "bid_ask_spread_percentage": { "$ref": "#/$defs/num" },
        "timestamp": { "$ref": "#/$defs/opt_datetime" },
        "last_traded_at": { "$ref": "#/$defs/opt_datetime" },
        "last_fetch_at": { "$ref": "#/$defs/opt_datetime" },
        "is_anomaly": { "type": ["boolean", "null"] },
        "is_stale": { "type": ["boolean", "null"] },
        "coin_id": { "type": ["string", "null"] },
        "target_coin_id": { "type": ["string", "null"] }
      }
    }
  }
}
```

Replace `schemas/embed.go` with:
```go
// Package schemas embeds the JSON Schema files of the pipeline so every
// binary carries the schema version it was built with. testdata/ is not
// embedded: fixtures are for tests only.
package schemas

import "embed"

// FS holds every schema file, at paths that mirror the $id
// (https://schemas.cg-etl.internal/<path>). One //go:embed line per
// directory: a pattern that matches no file is a compile error, so a
// directory is added here in the task that creates its first schema.
//
//go:embed common/*.json
//go:embed pipeline/*.json
//go:embed source/coingecko/*.json
var FS embed.FS
```

- [ ] **Step 5: Run the tests**

```bash
go vet ./...
go test -race -count=1 ./internal/validation/
```
Expected: PASS, covering all 7 recorded samples, 11 mutations and the tolerance test, plus the Task 3 tests (the fixture walker ignores `testdata/coingecko/` because those files are not under `valid/` or `invalid/`).

- [ ] **Step 6: Commit**

```bash
git add schemas/embed.go schemas/source schemas/testdata/coingecko internal/validation/source_contract_test.go
git commit -m "$(cat <<'EOF'
feat(schemas): permissive CoinGecko source schemas S0 with recorded responses

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

CONTRACT NOTES

- None blocking. `pipeline.dedup_claims` carries the extra column `idempotency_key text` (nullable; the contract `dedup.Claim` has it and Task 14 needs it for the `duplicate_response` drop reason). The spec does not list it. No format CHECK on it, so test fixtures elsewhere may use any string; Task 14 stores an empty key as NULL. Task 14 relies on this column and adds no migration of its own.
- `InsertAPICall` parses `fetched_at` with `time.Parse(time.RFC3339Nano, …)` and does not call `xtime.Parse`. This keeps the package independent of `internal/xtime`, and every `xtime.Format` output is valid RFC 3339.
- `InsertAPICall` also repairs a missing daily partition: on SQLSTATE 23514 with no constraint name it calls `EnsurePartitions(ctx, pool, fetchedAt, 1)` and retries once. The signature is unchanged.
- `StartVersityGW` uses `testcontainers.Run` (the v0.44 replacement for `GenericContainer`, which builds the same generic container request) and bind-mounts through `WithHostConfigModifier`, because `testcontainers.BindMount` is deprecated in v0.44.0.

### Task 5: Postgres storage + migrations + test containers

**Files:**
- Create: `migrations/embed.go`
- Create: `migrations/00001_raw.sql`
- Create: `migrations/00002_pipeline.sql`
- Create: `internal/storage/pg/pg.go`
- Create: `internal/storage/pg/apicalls.go`
- Create: `internal/testutil/containers.go`
- Test: `internal/testutil/containers_test.go`
- Test: `internal/storage/pg/apicalls_test.go` (unit, no Docker)
- Test: `internal/storage/pg/pg_integration_test.go` (Docker; skipped without it)
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes:
  - `internal/event`: `type RawEvent struct { SchemaVersion int; EventID string; IdempotencyKey *string; Source, Endpoint string; Request Request; FetchedAt string; Outcome string; HTTP *HTTPMeta; Error *string; PayloadSHA256 *string; Payload json.RawMessage }`, `type Request struct { Path string; Params map[string]string }`, `type HTTPMeta struct { Status int; LatencyMS int64; Date *string; AgeSeconds *int; CFCacheStatus *string; ETag *string; RetryAfterS *int }`, constants `SchemaVersion`, `SourceCoinGecko`, `EndpointMarkets`, `OutcomeOK`, `OutcomeHTTPError`, `OutcomeNetworkError`.
  - Fixture `schemas/testdata/coingecko/markets_top10_t0.json` (copied by Task 4).
- Produces:
  - `func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)`
  - `func Migrate(ctx context.Context, pool *pgxpool.Pool) error // goose, embedded migrations, advisory lock`
  - `func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error // raw.api_calls + pipeline.dedup_claims daily partitions [today-1, today+daysAhead]`
  - `func InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error // ON CONFLICT DO NOTHING`
  - package `migrations`: `var FS embed.FS`
  - package `testutil`: `func StartPostgres(t *testing.T) (dsn string)`, `func StartRedpanda(t *testing.T) (brokers []string)`, `func StartVersityGW(t *testing.T) (endpoint, dataDir string)`, `func SkipIfNoDocker(t *testing.T)`
  - Tables: `raw.api_calls` (partitions `raw.api_calls_pYYYYMMDD`), `pipeline.export_batches`, `pipeline.dedup_claims` (partitions `pipeline.dedup_claims_pYYYYMMDD`), `pipeline.consumer_batches`. Named constraints that later tasks may match on: `api_calls_pkey`, `api_calls_payload_iff_ok`, `dedup_claims_pkey`, `consumer_batches_pkey`, `consumer_batches_no_overlap` (SQLSTATE 23P01 on overlap).

Background for the engineer:
- **goose v3 Provider API.** `goose.NewProvider(goose.DialectPostgres, *sql.DB, fs.FS, opts...)` reads `NNNNN_name.sql` files from the root of the FS. `provider.Up(ctx)` applies whatever is pending and records each version in `goose_db_version` (in `public`). Each file is split at `-- +goose Up` / `-- +goose Down`. Statements that contain `;` inside them (a `DO $$ … $$` block) must sit between `-- +goose StatementBegin` and `-- +goose StatementEnd`. `lock.NewPostgresSessionLocker()` takes a `pg_advisory_lock` on a dedicated connection, so two `etl init` processes that migrate at once serialize instead of racing.
- **goose needs `database/sql`.** `stdlib.OpenDBFromPool(pool)` (pgx v5) wraps the pgx pool as a `*sql.DB`. Closing that `*sql.DB` does **not** close the pool.
- **`json` vs `jsonb`.** A `json` column stores its input text verbatim, so `payload::text` returns exactly the bytes that were inserted and `payload_sha256` stays verifiable. pgx encodes a Go `string` argument as text, so passing `string(ev.Payload)` keeps the bytes as they are.
- **Partitions.** A partitioned table rejects rows that fall outside every partition, with SQLSTATE 23514 (`no partition of relation … found for row`). A failed CHECK constraint uses the same code, but the server then fills in `ConstraintName`. The partition-missing case has no constraint name, and the code uses that difference to tell the two apart. There is no DEFAULT partition, on purpose: a DEFAULT partition blocks `DETACH CONCURRENTLY`. Bounds on the `timestamptz` table carry an explicit `+00`, so the session `TimeZone` cannot shift day boundaries.
- **`ALTER DATABASE … SET`** affects new sessions only. The migration targets `current_database()` through `EXECUTE format(...)` inside a `DO` block, so it works for any database name. `transaction_timeout` exists only from PostgreSQL 17, which is why the version guard is there.
- **testcontainers-go v0.44.0.** `tcpostgres.Run` / `tcredpanda.Run` are the module constructors. `testcontainers.CleanupContainer(t, ctr)` registers removal in `t.Cleanup` and accepts a nil container. `testcontainers.SkipIfProviderIsNotHealthy(t)` skips when Docker is unreachable. The optional env var `ETL_TEST_CONTAINER_PREFIX` names the containers `<prefix><kind>-<random>` so they are easy to find.
- **versitygw (posix backend).** Every top-level directory under the root is a bucket, and an object key maps to a file path. Docker Desktop bind mounts have no xattrs, so metadata goes to a separate `--sidecar` directory. Running the container as the test user's uid:gid keeps the written files removable by `t.TempDir` cleanup on Linux CI.

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/jackc/pgx/v5@v5.11.0 \
  github.com/pressly/goose/v3@v3.28.0 \
  github.com/google/uuid@v1.6.0 \
  github.com/testcontainers/testcontainers-go@v0.44.0 \
  github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0 \
  github.com/testcontainers/testcontainers-go/modules/redpanda@v0.44.0 \
  github.com/moby/moby/api@v1.55.0
```

Expected: `go.mod` lists those modules (`go mod tidy` at the end of the task marks them direct).

- [ ] **Step 2: Write the migrations and their embed package**

`migrations/00001_raw.sql` follows spec §4.1 exactly (no payload validation columns):

```sql
-- +goose Up
-- Raw zone: one row per HTTP call to CoinGecko, including failed calls.
-- Rows are written before any deduplication; idempotency_key is informational.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.api_calls (
  seq              bigint      GENERATED ALWAYS AS IDENTITY,
  event_id         uuid        NOT NULL,
  idempotency_key  text        CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
  schema_version   smallint    NOT NULL,
  source           text        NOT NULL,
  endpoint         text        NOT NULL,
  request          jsonb       NOT NULL,
  fetched_at       timestamptz NOT NULL,
  outcome          text        NOT NULL CHECK (outcome IN ('ok','http_error','network_error')),
  http             jsonb,
  error            text,
  payload          json,
  payload_sha256   bytea       CHECK (octet_length(payload_sha256) = 32),
  ingest_txid      xid8        NOT NULL DEFAULT pg_current_xact_id(),
  CONSTRAINT api_calls_pkey PRIMARY KEY (event_id, fetched_at),
  CONSTRAINT api_calls_payload_iff_ok CHECK ((outcome = 'ok') = (payload IS NOT NULL))
) PARTITION BY RANGE (fetched_at);

COMMENT ON COLUMN raw.api_calls.payload IS 'Response body byte-for-byte (json, not jsonb).';
COMMENT ON COLUMN raw.api_calls.ingest_txid IS 'xid8 of the inserting transaction; exporter cursor (ingest_txid, seq).';

CREATE INDEX api_calls_export_cursor_idx ON raw.api_calls (ingest_txid, seq) WHERE outcome = 'ok';
CREATE INDEX api_calls_idempotency_key_idx ON raw.api_calls (idempotency_key);

-- +goose Down
DROP TABLE IF EXISTS raw.api_calls;
DROP SCHEMA IF EXISTS raw;
```

`migrations/00002_pipeline.sql` follows spec §7.2, plus `dedup_claims.idempotency_key` and the database-level timeouts from §7.7:

```sql
-- +goose Up
-- Pipeline state: exporter ledger, transformer dedup claims and batch manifest.
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.export_batches (
  batch_id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  from_txid           xid8   NOT NULL,
  from_seq            bigint NOT NULL,
  to_txid             xid8   NOT NULL,
  to_seq              bigint NOT NULL,
  watermark_xmin      xid8   NOT NULL,
  row_count           int    NOT NULL CHECK (row_count > 0),
  object_key          text   NOT NULL UNIQUE,
  content_sha256      bytea,
  status              text   NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','exported')),
  claimed_at          timestamptz NOT NULL DEFAULT now(),
  exported_at         timestamptz,
  publish_lease_until timestamptz,
  published_at        timestamptz,
  UNIQUE (from_txid, from_seq),
  CHECK ((to_txid, to_seq) > (from_txid, from_seq))
);

-- Publisher picks the oldest exported, unpublished batch.
CREATE INDEX export_batches_unpublished_idx ON pipeline.export_batches (batch_id)
  WHERE status = 'exported' AND published_at IS NULL;

CREATE TABLE pipeline.dedup_claims (
  dataset         text   NOT NULL,
  record_key      text   NOT NULL,
  event_date      date   NOT NULL,
  topic_id        uuid   NOT NULL,
  kafka_partition int    NOT NULL,
  kafka_offset    bigint NOT NULL,
  ordinal         int    NOT NULL,
  event_id        uuid   NOT NULL,
  idempotency_key text,              -- winner's idempotency key; drop-reason metrics only (Task 14). NULL when absent
  content_sha256  bytea  NOT NULL CHECK (octet_length(content_sha256) = 32),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT dedup_claims_pkey PRIMARY KEY (dataset, record_key, event_date)
) PARTITION BY RANGE (event_date);

-- Orphaned-claim cleanup and lineage lookups by position.
CREATE INDEX dedup_claims_position_idx ON pipeline.dedup_claims (topic_id, kafka_partition, kafka_offset);

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE pipeline.consumer_batches (
  topic_id        uuid   NOT NULL,
  kafka_partition int    NOT NULL,
  first_offset    bigint NOT NULL,
  last_offset     bigint NOT NULL CHECK (last_offset >= first_offset),
  object_keys     text[],
  content_sha256  bytea[],
  counts          jsonb,
  status          text   NOT NULL DEFAULT 'planned' CHECK (status IN ('planned','written')),
  planned_at      timestamptz NOT NULL DEFAULT now(),
  written_at      timestamptz,
  CONSTRAINT consumer_batches_pkey PRIMARY KEY (topic_id, kafka_partition, first_offset),
  CONSTRAINT consumer_batches_no_overlap EXCLUDE USING gist (
    topic_id WITH =,
    kafka_partition WITH =,
    int8range(first_offset, last_offset, '[]') WITH &&
  )
);

-- Server-side guards against forgotten transactions: a long transaction stalls
-- the xid8 exporter cursor (spec 7.7). Applied to the current database so it
-- works whatever the database is called; takes effect for new sessions.
-- transaction_timeout exists from PostgreSQL 17 on, hence the version guard.
-- +goose StatementBegin
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I SET idle_in_transaction_session_timeout = %L',
                 current_database(), '60s');
  IF current_setting('server_version_num')::int >= 170000 THEN
    EXECUTE format('ALTER DATABASE %I SET transaction_timeout = %L',
                   current_database(), '5min');
  END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I RESET idle_in_transaction_session_timeout', current_database());
  IF current_setting('server_version_num')::int >= 170000 THEN
    EXECUTE format('ALTER DATABASE %I RESET transaction_timeout', current_database());
  END IF;
END
$$;
-- +goose StatementEnd
DROP TABLE IF EXISTS pipeline.consumer_batches;
DROP TABLE IF EXISTS pipeline.dedup_claims;
DROP TABLE IF EXISTS pipeline.export_batches;
DROP SCHEMA IF EXISTS pipeline;
```

`migrations/embed.go`:

```go
// Package migrations embeds the goose SQL migrations so the etl binary can
// migrate the database without any files on disk.
package migrations

import "embed"

// FS holds every *.sql migration at its root (e.g. "00001_raw.sql").
//
//go:embed *.sql
var FS embed.FS
```

Run: `go build ./migrations/`
Expected: no output (exit 0).

- [ ] **Step 3: Write the failing test-container smoke tests**

`internal/testutil/containers_test.go`:

```go
package testutil

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestStartPostgres(t *testing.T) {
	dsn := StartPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var version string
	if err := conn.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "18.6") {
		t.Errorf("server_version = %s, want 18.6", version)
	}
}

func TestStartRedpanda(t *testing.T) {
	brokers := StartRedpanda(t)
	if len(brokers) != 1 {
		t.Fatalf("brokers = %v", brokers)
	}
	c, err := net.DialTimeout("tcp", brokers[0], 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", brokers[0], err)
	}
	_ = c.Close()
}

func TestStartVersityGW(t *testing.T) {
	endpoint, dataDir := StartVersityGW(t)
	resp, err := http.Get(endpoint + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health = %d", resp.StatusCode)
	}
	// Unsigned S3 calls must be refused: the gateway enforces credentials.
	resp, err = http.Get(endpoint + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("anonymous ListBuckets = %d, want 403", resp.StatusCode)
	}
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if fi, err := os.Stat(filepath.Join(dataDir, b)); err != nil || !fi.IsDir() {
			t.Errorf("bucket dir %s missing: %v", b, err)
		}
	}
}
```

Run: `go test ./internal/testutil/`
Expected: FAIL, build error `undefined: StartPostgres` (plus `StartRedpanda`, `StartVersityGW`).

- [ ] **Step 4: Implement the container helpers**

`internal/testutil/containers.go`:

```go
// Package testutil starts throwaway infrastructure containers for
// integration tests (testcontainers-go). Every helper skips the test when
// Docker is unavailable and removes its container in t.Cleanup.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	PostgresImage  = "postgres:18.6"
	RedpandaImage  = "redpandadata/redpanda:v26.2.4"
	VersityGWImage = "versity/versitygw:v1.8.0"

	// Credentials baked into StartVersityGW (match .env.example defaults).
	VersityGWAccessKey = "etl"
	VersityGWSecretKey = "etlsecret123"
)

// SkipIfNoDocker skips the test when no healthy Docker daemon is reachable,
// or when the test binary runs with -short.
func SkipIfNoDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// nameOpt names the container "<prefix><kind>-<random>" when
// ETL_TEST_CONTAINER_PREFIX is set (handy for spotting/cleaning test
// containers); otherwise Docker picks a random name.
func nameOpt(kind string) testcontainers.CustomizeRequestOption {
	prefix := os.Getenv("ETL_TEST_CONTAINER_PREFIX")
	if prefix == "" {
		return func(*testcontainers.GenericContainerRequest) error { return nil }
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return testcontainers.WithName(prefix + kind + "-" + hex.EncodeToString(b))
}

// StartPostgres runs postgres:18.6 (user/password/db = etl) and returns a DSN.
func StartPostgres(t *testing.T) (dsn string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("etl"),
		tcpostgres.WithUsername("etl"),
		tcpostgres.WithPassword("etl"),
		// The image restarts once after initdb; wait for the second "ready"
		// log line and then for the mapped TCP port.
		tcpostgres.BasicWaitStrategies(),
		nameOpt("pg"),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	return dsn
}

// StartRedpanda runs a single-node Redpanda in dev-container mode (topic
// auto-creation off, the module default) and returns the seed brokers.
func StartRedpanda(t *testing.T) (brokers []string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := tcredpanda.Run(ctx, RedpandaImage, nameOpt("redpanda"))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	seed, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("redpanda seed broker: %v", err)
	}
	return []string{seed}
}

// StartVersityGW runs versitygw with the posix backend: the gateway root
// /lake is a host temp dir (dataDir), bucket = top-level directory, object
// key = file path. Metadata goes to a separate sidecar dir (/meta) because
// Docker Desktop bind mounts do not support xattrs. The buckets raw,
// processed, rejected and dlq are pre-created as directories. Returns the
// S3 endpoint ("http://host:port") and the host path of the gateway root.
func StartVersityGW(t *testing.T) (endpoint, dataDir string) {
	t.Helper()
	SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir = t.TempDir()
	metaDir := t.TempDir()
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if err := os.MkdirAll(filepath.Join(dataDir, b), 0o755); err != nil {
			t.Fatalf("create bucket dir %s: %v", b, err)
		}
	}

	ctr, err := testcontainers.Run(ctx, VersityGWImage,
		testcontainers.WithEnv(map[string]string{
			"ROOT_ACCESS_KEY_ID":     VersityGWAccessKey,
			"ROOT_SECRET_ACCESS_KEY": VersityGWSecretKey,
		}),
		testcontainers.WithCmd("--health", "/health", "posix", "--sidecar", "/meta", "/lake"),
		testcontainers.WithExposedPorts("7070/tcp"),
		// Run as the test user so files written into dataDir stay removable
		// by t.TempDir cleanup on Linux CI.
		testcontainers.WithConfigModifier(func(c *container.Config) {
			c.User = fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, dataDir+":/lake", metaDir+":/meta")
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/health").
				WithPort("7070/tcp").
				WithStatusCodeMatcher(func(s int) bool { return s == http.StatusOK }).
				WithStartupTimeout(60*time.Second),
		),
		nameOpt("versitygw"),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start versitygw: %v", err)
	}
	hostPort, err := ctr.PortEndpoint(ctx, "7070/tcp", "http")
	if err != nil {
		t.Fatalf("versitygw endpoint: %v", err)
	}
	return hostPort, dataDir
}
```

Run: `go test -count=1 -v ./internal/testutil/`
Expected: PASS for `TestStartPostgres`, `TestStartRedpanda` and `TestStartVersityGW` when Docker is running; Redpanda takes about 20 s on the first pull. Without Docker, or with `-short`, all three report SKIP.

- [ ] **Step 5: Write the failing unit tests for the pg package**

`internal/storage/pg/apicalls_test.go`:

```go
package pg

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"coingecko-etl/internal/event"
)

func strp(s string) *string { return &s }

func okEvent() event.RawEvent {
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        "0199cd2e-7c1a-7000-8000-000000000001",
		IdempotencyKey: strp(strings.Repeat("ab", 32)),
		Source:         event.SourceCoinGecko,
		Endpoint:       event.EndpointMarkets,
		Request:        event.Request{Path: "/coins/markets", Params: map[string]string{"vs_currency": "usd"}},
		FetchedAt:      "2026-10-10T10:07:55.123Z",
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 42},
		PayloadSHA256:  strp(strings.Repeat("0f", 32)),
		Payload:        json.RawMessage(`[{"id":"bitcoin", "current_price":82800}]`),
	}
}

func TestAPICallArgsOK(t *testing.T) {
	args, err := apiCallArgs(okEvent())
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 12 {
		t.Fatalf("got %d args, want 12", len(args))
	}
	if got := args[0].(uuid.UUID).String(); got != "0199cd2e-7c1a-7000-8000-000000000001" {
		t.Errorf("event_id = %s", got)
	}
	if got := args[2].(int16); got != 1 {
		t.Errorf("schema_version = %d", got)
	}
	if got := args[5].(string); got != `{"path":"/coins/markets","params":{"vs_currency":"usd"}}` {
		t.Errorf("request = %s", got)
	}
	want := time.Date(2026, 10, 10, 10, 7, 55, 123e6, time.UTC)
	if got := args[6].(time.Time); !got.Equal(want) || got.Location() != time.UTC {
		t.Errorf("fetched_at = %v", got)
	}
	if got := args[8].(string); !strings.Contains(got, `"status":200`) {
		t.Errorf("http = %s", got)
	}
	// payload must be a string (pgx sends text -> json column stores bytes verbatim)
	if got, ok := args[10].(string); !ok || got != `[{"id":"bitcoin", "current_price":82800}]` {
		t.Errorf("payload = %#v", args[10])
	}
	if got := args[11].([]byte); len(got) != 32 || got[0] != 0x0f {
		t.Errorf("payload_sha256 = %x", got)
	}
}

func TestAPICallArgsFailedCallHasNulls(t *testing.T) {
	ev := okEvent()
	ev.Outcome = event.OutcomeNetworkError
	ev.IdempotencyKey = nil
	ev.HTTP = nil
	ev.Error = strp("dial tcp: timeout")
	ev.PayloadSHA256 = nil
	ev.Payload = json.RawMessage("null")
	args, err := apiCallArgs(ev)
	if err != nil {
		t.Fatal(err)
	}
	if args[1].(*string) != nil {
		t.Error("idempotency_key should be nil")
	}
	if args[8] != nil {
		t.Errorf("http = %#v, want nil", args[8])
	}
	if args[10] != nil {
		t.Errorf("payload = %#v, want nil", args[10])
	}
	if args[11].([]byte) != nil {
		t.Errorf("payload_sha256 = %#v, want nil", args[11])
	}
	if *args[9].(*string) != "dial tcp: timeout" {
		t.Errorf("error = %v", args[9])
	}
}

func TestAPICallArgsRejectsBadInput(t *testing.T) {
	cases := map[string]func(*event.RawEvent){
		"bad uuid":       func(e *event.RawEvent) { e.EventID = "not-a-uuid" },
		"bad fetched_at": func(e *event.RawEvent) { e.FetchedAt = "10/10/2026" },
		"bad sha hex":    func(e *event.RawEvent) { e.PayloadSHA256 = strp("zz") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ev := okEvent()
			mutate(&ev)
			if _, err := apiCallArgs(ev); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestPartitionDDL(t *testing.T) {
	got := partitionDDL(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	if len(got) != 2 {
		t.Fatalf("got %d statements", len(got))
	}
	wantRaw := "CREATE TABLE IF NOT EXISTS raw.api_calls_p20261231 PARTITION OF raw.api_calls\n" +
		"  FOR VALUES FROM ('2026-12-31 00:00:00+00') TO ('2027-01-01 00:00:00+00')"
	if got[0] != wantRaw {
		t.Errorf("raw DDL:\n%s\nwant:\n%s", got[0], wantRaw)
	}
	wantDedup := "CREATE TABLE IF NOT EXISTS pipeline.dedup_claims_p20261231 PARTITION OF pipeline.dedup_claims\n" +
		"  FOR VALUES FROM ('2026-12-31') TO ('2027-01-01')"
	if got[1] != wantDedup {
		t.Errorf("dedup DDL:\n%s\nwant:\n%s", got[1], wantDedup)
	}
}
```

Run: `go test ./internal/storage/pg/`
Expected: FAIL, build error `undefined: apiCallArgs` and `undefined: partitionDDL`.

- [ ] **Step 6: Write the failing integration test**

`internal/storage/pg/pg_integration_test.go` (needs Docker and the Task 4 fixture `schemas/testdata/coingecko/markets_top10_t0.json`):

```go
package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/testutil"
)

// TestPostgresIntegration shares one postgres:18.6 container across subtests.
func TestPostgresIntegration(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx := context.Background()

	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	t.Run("MigrateTwiceIsIdempotent", func(t *testing.T) {
		for i := range 2 {
			if err := Migrate(ctx, pool); err != nil {
				t.Fatalf("migrate #%d: %v", i+1, err)
			}
		}
		var version int64
		if err := pool.QueryRow(ctx,
			`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("goose version = %d, want 2", version)
		}
		for _, tbl := range []string{"raw.api_calls", "pipeline.export_batches",
			"pipeline.dedup_claims", "pipeline.consumer_batches"} {
			var ok bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&ok); err != nil || !ok {
				t.Errorf("table %s missing (err=%v)", tbl, err)
			}
		}
		// payload must be json (byte-exact), never jsonb.
		var typ string
		if err := pool.QueryRow(ctx, `SELECT data_type FROM information_schema.columns
			WHERE table_schema='raw' AND table_name='api_calls' AND column_name='payload'`).Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "json" {
			t.Errorf("payload type = %s, want json", typ)
		}
	})

	t.Run("DatabaseTimeoutsApplyToNewSessions", func(t *testing.T) {
		// ALTER DATABASE ... SET only affects sessions opened afterwards.
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var idle, txn string
		if err := conn.QueryRow(ctx, `SHOW idle_in_transaction_session_timeout`).Scan(&idle); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SHOW transaction_timeout`).Scan(&txn); err != nil {
			t.Fatal(err)
		}
		if idle != "1min" || txn != "5min" {
			t.Errorf("idle_in_transaction_session_timeout=%s transaction_timeout=%s, want 1min/5min", idle, txn)
		}
	})

	now := time.Date(2026, 10, 10, 23, 30, 0, 0, time.UTC)

	t.Run("EnsurePartitionsCreatesDailyPartitions", func(t *testing.T) {
		for range 2 { // idempotent
			if err := EnsurePartitions(ctx, pool, now, 3); err != nil {
				t.Fatalf("ensure partitions: %v", err)
			}
		}
		want := []string{"20261009", "20261010", "20261011", "20261012", "20261013"}
		for _, parent := range []string{"raw.api_calls", "pipeline.dedup_claims"} {
			got := partitionsOf(t, pool, parent)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s partitions = %v, want %v", parent, got, want)
			}
		}
		// Day boundaries are UTC regardless of the session TimeZone.
		var bound string
		if err := pool.QueryRow(ctx, `SELECT pg_get_expr(c.relpartbound, c.oid)
			FROM pg_class c WHERE c.oid = 'raw.api_calls_p20261010'::regclass`).Scan(&bound); err != nil {
			t.Fatal(err)
		}
		if bound != "FOR VALUES FROM ('2026-10-10 00:00:00+00') TO ('2026-10-11 00:00:00+00')" {
			t.Errorf("bound = %s", bound)
		}
	})

	t.Run("InsertSameEventTwiceStoresOneRow", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2026-10-10T10:07:55.123Z"
		for range 2 {
			if err := InsertAPICall(ctx, pool, ev); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw.api_calls WHERE event_id = $1`,
			uuid.MustParse(ev.EventID)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("rows = %d, want 1", n)
		}
	})

	t.Run("PayloadStoredByteExact", func(t *testing.T) {
		raw, err := os.ReadFile("../../../schemas/testdata/coingecko/markets_top10_t0.json")
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2026-10-10T10:08:25.000Z"
		ev.PayloadSHA256 = strp(hex.EncodeToString(sum[:]))
		ev.Payload = json.RawMessage(raw)
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var text string
		var storedSHA []byte
		var fetched time.Time
		var reqPath string
		if err := pool.QueryRow(ctx, `SELECT payload::text, payload_sha256, fetched_at, request->>'path'
			FROM raw.api_calls WHERE event_id = $1`, uuid.MustParse(ev.EventID)).
			Scan(&text, &storedSHA, &fetched, &reqPath); err != nil {
			t.Fatal(err)
		}
		if text != string(raw) {
			t.Fatalf("payload not byte-exact: got %d bytes, want %d", len(text), len(raw))
		}
		if hex.EncodeToString(storedSHA) != *ev.PayloadSHA256 {
			t.Errorf("payload_sha256 = %x", storedSHA)
		}
		if !fetched.Equal(time.Date(2026, 10, 10, 10, 8, 25, 0, time.UTC)) {
			t.Errorf("fetched_at = %v", fetched)
		}
		if reqPath != "/coins/markets" {
			t.Errorf("request.path = %s", reqPath)
		}
	})

	t.Run("FailedCallStoredWithoutPayload", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.Outcome = event.OutcomeHTTPError
		ev.IdempotencyKey = nil
		ev.HTTP = &event.HTTPMeta{Status: 429, LatencyMS: 80}
		ev.Error = strp("http 429")
		ev.PayloadSHA256 = nil
		ev.Payload = nil
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var isNull bool
		var status int
		if err := pool.QueryRow(ctx, `SELECT payload IS NULL, (http->>'status')::int
			FROM raw.api_calls WHERE event_id = $1`, uuid.MustParse(ev.EventID)).Scan(&isNull, &status); err != nil {
			t.Fatal(err)
		}
		if !isNull || status != 429 {
			t.Errorf("payload null=%v status=%d", isNull, status)
		}
	})

	t.Run("CheckRejectsOKWithoutPayload", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.Payload = nil
		err := InsertAPICall(ctx, pool, ev)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "api_calls_payload_iff_ok" {
			t.Fatalf("err = %v, want CHECK api_calls_payload_iff_ok violation", err)
		}
	})

	t.Run("InsertRepairsMissingPartition", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2027-03-01T12:00:00.000Z" // far outside the ensured window
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('raw.api_calls_p20270301') IS NOT NULL`).Scan(&ok); err != nil || !ok {
			t.Fatalf("partition raw.api_calls_p20270301 not created (err=%v)", err)
		}
	})

	t.Run("ConsumerBatchesRejectOverlap", func(t *testing.T) {
		topic := uuid.Must(uuid.NewV7())
		if _, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 0, 100, 199)`, topic); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 0, 150, 260)`, topic)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23P01" { // exclusion_violation
			t.Fatalf("err = %v, want exclusion violation", err)
		}
		// Same range on another partition is fine.
		if _, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 1, 150, 260)`, topic); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("DedupClaimsAcceptRowsInEnsuredPartition", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO pipeline.dedup_claims
			(dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal,
			 event_id, idempotency_key, content_sha256)
			VALUES ('market_snapshot', 'bitcoin|2026-10-10T10:07:55.000Z', '2026-10-10', $1, 0, 5, 0, $2, $3, $4)`,
			uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), strings.Repeat("ab", 32), make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
	})
}

func partitionsOf(t *testing.T, pool *pgxpool.Pool, parent string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass ORDER BY c.relname`, parent)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, n[strings.LastIndex(n, "_p")+2:])
	}
	return out
}
```

Run: `go test ./internal/storage/pg/`
Expected: FAIL, build errors `undefined: Connect`, `undefined: Migrate`, `undefined: EnsurePartitions`, `undefined: InsertAPICall` (plus the Step 5 errors).

- [ ] **Step 7: Implement Connect, Migrate and EnsurePartitions**

`internal/storage/pg/pg.go`:

```go
// Package pg owns the Postgres connection pool, schema migrations and
// partition maintenance for the raw and pipeline schemas.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"coingecko-etl/migrations"
)

// partitionLockKey serializes concurrent EnsurePartitions calls across
// processes (init, fetcher maintenance loop, insert-time repair).
// Arbitrary constant: "cgetl-pt" in ASCII.
const partitionLockKey int64 = 0x6367_6574_6c2d_7074

// Connect opens a pgx pool and verifies connectivity with a ping.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// Migrate applies all embedded goose migrations. A Postgres session-level
// advisory lock makes concurrent callers wait instead of racing.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("pg: goose locker: %w", err)
	}
	// goose works on database/sql; wrap the pool. Closing db does not close pool.
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("pg: goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("pg: migrate up: %w", err)
	}
	return nil
}

// EnsurePartitions creates the daily partitions of raw.api_calls and
// pipeline.dedup_claims for every UTC day in [today-1, today+daysAhead].
// It is idempotent and safe to call concurrently.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error {
	if daysAhead < 0 {
		return errors.New("pg: daysAhead must be >= 0")
	}
	u := now.UTC()
	today := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey); err != nil {
			return fmt.Errorf("pg: partition lock: %w", err)
		}
		for d := -1; d <= daysAhead; d++ {
			for _, stmt := range partitionDDL(today.AddDate(0, 0, d)) {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("pg: create partition: %w", err)
				}
			}
		}
		return nil
	})
}

// partitionDDL returns the CREATE statements for one UTC day.
// raw.api_calls is ranged on timestamptz, so bounds carry an explicit +00
// offset; otherwise the session TimeZone would shift the day boundaries.
func partitionDDL(day time.Time) []string {
	next := day.AddDate(0, 0, 1)
	suffix := day.Format("20060102")
	from, to := day.Format("2006-01-02"), next.Format("2006-01-02")
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS raw.api_calls_p%s PARTITION OF raw.api_calls
  FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')`, suffix, from, to),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS pipeline.dedup_claims_p%s PARTITION OF pipeline.dedup_claims
  FOR VALUES FROM ('%s') TO ('%s')`, suffix, from, to),
	}
}
```

- [ ] **Step 8: Implement InsertAPICall**

`internal/storage/pg/apicalls.go`:

```go
package pg

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/event"
)

const insertAPICallSQL = `
INSERT INTO raw.api_calls (
  event_id, idempotency_key, schema_version, source, endpoint, request,
  fetched_at, outcome, http, error, payload, payload_sha256
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (event_id, fetched_at) DO NOTHING`

// apiCallArgs maps a RawEvent to the INSERT parameters, in column order.
//
// payload is passed as a Go string so pgx sends it as text into the json
// column: Postgres stores json input verbatim, so the bytes (and therefore
// payload_sha256) survive unchanged. Absent payload (nil or literal null)
// becomes SQL NULL; the table CHECK then enforces outcome='ok' <=> payload.
func apiCallArgs(ev event.RawEvent) ([]any, error) {
	id, err := uuid.Parse(ev.EventID)
	if err != nil {
		return nil, fmt.Errorf("pg: event_id: %w", err)
	}
	fetchedAt, err := time.Parse(time.RFC3339Nano, ev.FetchedAt)
	if err != nil {
		return nil, fmt.Errorf("pg: fetched_at: %w", err)
	}
	reqJSON, err := json.Marshal(ev.Request)
	if err != nil {
		return nil, fmt.Errorf("pg: request: %w", err)
	}
	var httpJSON any // nil -> SQL NULL
	if ev.HTTP != nil {
		b, err := json.Marshal(ev.HTTP)
		if err != nil {
			return nil, fmt.Errorf("pg: http: %w", err)
		}
		httpJSON = string(b)
	}
	var payload any // nil -> SQL NULL
	if p := bytes.TrimSpace(ev.Payload); len(p) > 0 && !bytes.Equal(p, []byte("null")) {
		payload = string(ev.Payload)
	}
	var sha []byte // nil -> SQL NULL
	if ev.PayloadSHA256 != nil {
		sha, err = hex.DecodeString(*ev.PayloadSHA256)
		if err != nil {
			return nil, fmt.Errorf("pg: payload_sha256: %w", err)
		}
	}
	return []any{
		id, ev.IdempotencyKey, int16(ev.SchemaVersion), ev.Source, ev.Endpoint, string(reqJSON),
		fetchedAt.UTC(), ev.Outcome, httpJSON, ev.Error, payload, sha,
	}, nil
}

// InsertAPICall stores one API call. Retrying the same event is a no-op
// (ON CONFLICT on the primary key). If the day's partition is missing
// (SQLSTATE 23514 without a constraint name) it creates it and retries once.
func InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error {
	args, err := apiCallArgs(ev)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, insertAPICallSQL, args...)
	if isMissingPartition(err) {
		fetchedAt := args[6].(time.Time)
		if perr := EnsurePartitions(ctx, pool, fetchedAt, 1); perr != nil {
			return fmt.Errorf("pg: insert api call: %w (partition repair: %v)", err, perr)
		}
		_, err = pool.Exec(ctx, insertAPICallSQL, args...)
	}
	if err != nil {
		return fmt.Errorf("pg: insert api call %s: %w", ev.EventID, err)
	}
	return nil
}

// isMissingPartition distinguishes "no partition of relation ... found for row"
// from CHECK violations, which share SQLSTATE 23514 but name a constraint.
func isMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && pgErr.ConstraintName == ""
}
```

- [ ] **Step 9: Run unit and integration tests**

Run: `go mod tidy && go build ./... && go vet ./...`
Expected: no output.

Run: `go test -count=1 -v -run 'APICallArgs|PartitionDDL' ./internal/storage/pg/`
Expected: PASS for `TestAPICallArgsOK`, `TestAPICallArgsFailedCallHasNulls`, `TestAPICallArgsRejectsBadInput` (3 subtests) and `TestPartitionDDL`.

Run: `go test -count=1 -v -run TestPostgresIntegration ./internal/storage/pg/`
Expected: PASS for `TestPostgresIntegration` and all 10 subtests: `MigrateTwiceIsIdempotent`, `DatabaseTimeoutsApplyToNewSessions`, `EnsurePartitionsCreatesDailyPartitions`, `InsertSameEventTwiceStoresOneRow`, `PayloadStoredByteExact`, `FailedCallStoredWithoutPayload`, `CheckRejectsOKWithoutPayload`, `InsertRepairsMissingPartition`, `ConsumerBatchesRejectOverlap`, `DedupClaimsAcceptRowsInEnsuredPartition`.

Run: `go test -race -count=1 ./internal/storage/pg/ ./internal/testutil/`
Expected: `ok` for both packages.

- [ ] **Step 10: Commit**

```bash
git add go.mod go.sum migrations/ internal/storage/pg/ internal/testutil/
git commit -m "$(cat <<'MSG'
feat(storage): Postgres migrations, partitions, raw api_calls insert, test containers

- goose v3 embedded migrations: raw.api_calls (json payload, xid8 cursor),
  pipeline.export_batches, dedup_claims, consumer_batches (EXCLUDE overlap)
- database-level idle_in_transaction_session_timeout / transaction_timeout
- EnsurePartitions: daily UTC partitions, advisory-locked, idempotent
- InsertAPICall: byte-exact payload, ON CONFLICT DO NOTHING, partition repair
- testutil: postgres 18.6, redpanda v26.2.4, versitygw v1.8.0 helpers

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

CONTRACT NOTES

1. `obs.Metrics` has no counter for skipped ticks. Task 7 counts skipped ticks as `etl_api_requests_total{endpoint=<endpoint>,status="skipped"}`. The `status` label already holds the HTTP status (`"200"`, `"429"`, ...) or `"network_error"`, so no metric is added or renamed. Dashboards and alerts (Task for deploy/) must exclude `status="skipped"` when computing error ratios.
2. `fetcher.Run` takes no `*validation.Registry`, so it loads one itself with `validation.Load(schemas.FS)` at startup. A compile failure is returned as an error. Signature unchanged.
3. The contract leaves the meaning of `Limiter.Allowed` open. Here it returns false only while a request is in flight or a cooldown (429) is active. It does not consider `minSpacing`, because two schedules exactly `MinRequestSpacing` apart (markets at :00, detail at :15) would otherwise lose ticks to scheduler jitter. `Wait` (and the client's internal acquire) enforces spacing by sleeping, at most `minSpacing`. `Wait` records a request start but does not keep the in-flight slot. `Client.Fetch` uses unexported `acquire`/`release` for the single-flight slot.
4. `Client.Fetch` trims leading and trailing whitespace from a 2xx body (`bytes.TrimSpace`) before hashing and storing it. The JSON value and every byte inside it are unchanged, and a trailing newline can no longer break the raw JSONL line. `payload_sha256` is the hash of the trimmed bytes. CoinGecko bodies have no surrounding whitespace, so in practice the bytes are verbatim.
5. Labels for `SchemaValidations`/`SchemaViolations` at the fetch stage: `stage="fetch"`, `schema="raw_event.v1"`, `result="pass"|"fail"`. Every stage uses `result` = `pass|fail` and the schema file name without `.json` as the `schema` label (`raw_event.v1`, `coins_markets.v1`, `coin.v1`, `market_snapshot.v1`, ...).
6. Task 7's `Run` returns `nil` on context cancellation (graceful shutdown). It returns an error only for startup failures: schema load, zero intervals, or the first `EnsurePartitions`.
7. Tests in both tasks call `validation.Load(schemas.FS)` and validate the produced events against the real S1 (`pipeline/raw_event.v1.json` from Task 4). They expect S1 to accept: `http` = null for `network_error`; `payload`, `payload_sha256` and `idempotency_key` = null for non-ok outcomes; `error` = a string for non-ok outcomes. Before writing code, check these against the S1 in the repo.

### Task 6: CoinGecko client + limiter

**Files:**
- Create: `internal/coingecko/limiter.go`
- Create: `internal/coingecko/client.go`
- Test: `internal/coingecko/limiter_test.go`
- Test: `internal/coingecko/client_test.go`
- Uses fixtures (copied by Task 4): `schemas/testdata/coingecko/markets_top10_t0.json`, `schemas/testdata/coingecko/coin_bitcoin_full_t0.json`

**Interfaces:**
- Consumes:
  - `event.Request{Path string; Params map[string]string}`, `event.HTTPMeta`, `event.RawEvent` (fields per contract)
  - `const event.SchemaVersion, event.SourceCoinGecko, event.EndpointMarkets, event.EndpointCoinDetail, event.OutcomeOK, event.OutcomeHTTPError, event.OutcomeNetworkError`
  - `func event.IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string`
  - `func event.SHA256Hex(b []byte) string`
  - `func event.Marshal(ev RawEvent) ([]byte, error)` (tests only)
  - `func xtime.Format(t time.Time) string`
  - `func validation.Load(fsys fs.FS) (*Registry, error)`, `func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)`, `validation.IDRawEvent`, `schemas.FS` (tests only)
- Produces:
  - `type Limiter struct { /* unexported */ }`
  - `func NewLimiter(minSpacing time.Duration) *Limiter`
  - `func (l *Limiter) Wait(ctx context.Context) error`
  - `func (l *Limiter) Cooldown(d time.Duration)`
  - `func (l *Limiter) Allowed(now time.Time) bool`
  - `type Client struct { /* unexported */ }`
  - `func NewClient(baseURL, apiKey string, hc *http.Client, l *Limiter, now func() time.Time) *Client`
  - `func MarketsRequest(perPage int) event.Request`
  - `func CoinDetailRequest(coinID string) event.Request`
  - `func (c *Client) Fetch(ctx context.Context, endpoint string, req event.Request) event.RawEvent`

Background for the engineer:
- **CoinGecko budget.** The keyless API returns HTTP 429 with `Retry-After: 60` after a handful of calls in under 30 s. The client therefore runs at most one request at a time, keeps request starts at least `minSpacing` apart, and on a 429 puts the limiter into a cooldown of `Retry-After` seconds (60 s if the header is missing). `Fetch` never retries. The fetcher (Task 7) skips ticks while the limiter is cooling down, so a burst of retries can never happen.
- **Retry-After** can be delay-seconds (`60`) or an HTTP-date (RFC 9110). Both are parsed. `http.ParseTime` handles the date forms.
- **Backoff on transient failures (spec §3.2).** A 5xx, a 2xx whose body is not JSON (CDN maintenance page), a timeout or a connection error calls `Cooldown` with `2 s * 2^(n-1)` for the n-th consecutive failure, capped at 60 s, with equal jitter (a random value in `[d/2, d)`). Any other response resets the count. The fetcher sees the cooldown through `Allowed` and skips ticks; nothing is queued. A shutdown (parent context cancelled) does not count as a failure.
- **Gzip.** CoinGecko sends gzip. Go's `http.Transport` adds `Accept-Encoding: gzip` and decompresses transparently as long as we do **not** set `Accept-Encoding` ourselves, so the body we hash is the decoded JSON.
- **Optional API key.** It goes in the `x-cg-demo-api-key` header, never in the URL, so it cannot leak into `request.params`, logs or Postgres.
- **Hash and key.** `payload_sha256` is the hash of the exact body bytes, so later hops can verify integrity. `idempotency_key = event.IdempotencyKey(source, endpoint, params, payload_sha256)`. Two polls served the same cached response get the same key. Both fields are set only for outcome `ok`, because the DB check `(outcome = 'ok') = (payload IS NOT NULL)` requires it.
- **Error text** keeps an excerpt of at most 512 bytes of the body, converted to valid UTF-8 with NUL bytes removed. Postgres `text` rejects both invalid UTF-8 and NUL.
- **The URL** is `baseURL + Path + "?" + params sorted by key`. Commas stay literal (`price_change_percentage=1h,24h,7d`), as in CoinGecko's docs.

- [ ] **Step 1: Write the failing limiter tests**

Create `internal/coingecko/limiter_test.go`:

```go
package coingecko

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterFirstWaitIsImmediate(t *testing.T) {
	l := NewLimiter(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("first Wait took %v, want immediate", d)
	}
}

func TestLimiterEnforcesSpacing(t *testing.T) {
	const spacing = 150 * time.Millisecond
	l := NewLimiter(spacing)
	ctx := context.Background()
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < spacing-10*time.Millisecond {
		t.Fatalf("second Wait returned after %v, want >= %v", d, spacing)
	}
	// Spacing does not make Allowed false: ticks are only skipped for
	// in-flight requests and cooldowns.
	if !l.Allowed(time.Now()) {
		t.Fatal("Allowed = false right after a start; spacing must not skip ticks")
	}
}

func TestLimiterCooldown(t *testing.T) {
	l := NewLimiter(0)
	l.Cooldown(200 * time.Millisecond)
	now := time.Now()
	if l.Allowed(now) {
		t.Fatal("Allowed during cooldown")
	}
	if !l.Allowed(now.Add(250 * time.Millisecond)) {
		t.Fatal("not Allowed after cooldown")
	}
	// A shorter cooldown must not shorten the active one.
	l.Cooldown(time.Millisecond)
	if l.Allowed(time.Now().Add(50 * time.Millisecond)) {
		t.Fatal("shorter Cooldown shortened the active cooldown")
	}
	start := time.Now()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("Wait returned after %v during a 200ms cooldown", d)
	}
}

func TestLimiterWaitHonorsContext(t *testing.T) {
	l := NewLimiter(0)
	l.Cooldown(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := l.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err = %v, want context.DeadlineExceeded", err)
	}
}

func TestLimiterSingleFlight(t *testing.T) {
	l := NewLimiter(0)
	ctx := context.Background()
	if err := l.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Allowed(time.Now()) {
		t.Fatal("Allowed while a request is in flight")
	}
	got := make(chan time.Time, 1)
	go func() {
		if err := l.acquire(ctx); err == nil {
			got <- time.Now()
			l.release()
		}
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("second acquire succeeded while the first request was in flight")
	default:
	}
	released := time.Now()
	l.release()
	select {
	case at := <-got:
		if at.Before(released) {
			t.Fatal("second acquire finished before release")
		}
	case <-time.After(time.Second):
		t.Fatal("second acquire did not proceed after release")
	}
}
```

- [ ] **Step 2: Run the limiter tests to verify they fail**

Run: `go test ./internal/coingecko/ -run TestLimiter`
Expected: FAIL to build: `internal/coingecko/limiter_test.go:11:7: undefined: NewLimiter`

- [ ] **Step 3: Implement the limiter**

`acquire` loops: under the mutex it checks whether a start is allowed. If not, it waits on whichever comes first: the time when spacing or cooldown ends, a state change (the `changed` channel is closed and replaced on every `release`/`Cooldown`), or ctx. Closing a channel wakes every waiter at once without a `sync.Cond`, and unlike `sync.Cond` it can be combined with `ctx.Done()` in a `select`.

Create `internal/coingecko/limiter.go`:

```go
package coingecko

import (
	"context"
	"sync"
	"time"
)

// Limiter enforces the CoinGecko request budget for one process:
//   - at most one request in flight,
//   - at least minSpacing between two request starts,
//   - a cooldown (set after HTTP 429) during which no request may start.
//
// Allowed is the non-blocking check the fetcher uses to SKIP a tick: it
// reports false while a request is in flight or a cooldown is active.
// Spacing is deliberately not part of Allowed: two schedules that are
// minSpacing apart would otherwise lose ticks to scheduler jitter, so Wait
// absorbs spacing by sleeping (at most minSpacing).
type Limiter struct {
	mu            sync.Mutex
	minSpacing    time.Duration
	now           func() time.Time
	inFlight      bool
	lastStart     time.Time
	cooldownUntil time.Time
	changed       chan struct{} // closed and replaced on every state change
}

// NewLimiter returns a limiter that keeps request starts at least minSpacing apart.
func NewLimiter(minSpacing time.Duration) *Limiter {
	return &Limiter{minSpacing: minSpacing, now: time.Now, changed: make(chan struct{})}
}

// Wait blocks until a request may start and records that start (spacing is
// measured from it). It does not keep the in-flight slot. It returns
// ctx.Err() if ctx ends first.
func (l *Limiter) Wait(ctx context.Context) error {
	if err := l.acquire(ctx); err != nil {
		return err
	}
	l.release()
	return nil
}

// Cooldown forbids any request start before now+d. A shorter cooldown never
// shortens one that is already active.
func (l *Limiter) Cooldown(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if until := l.now().Add(d); until.After(l.cooldownUntil) {
		l.cooldownUntil = until
	}
	l.broadcastLocked()
}

// Allowed reports whether a tick at now should go ahead: no request in
// flight and no active cooldown. It never blocks.
func (l *Limiter) Allowed(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.inFlight && !now.Before(l.cooldownUntil)
}

// acquire blocks until no request is in flight and both spacing and
// cooldown have passed, then takes the in-flight slot. The caller must call
// release when the request is finished.
func (l *Limiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		var delay time.Duration = -1 // -1: wait for a state change only
		if !l.inFlight {
			now := l.now()
			ready := l.lastStart.Add(l.minSpacing)
			if l.cooldownUntil.After(ready) {
				ready = l.cooldownUntil
			}
			if !now.Before(ready) {
				l.inFlight = true
				l.lastStart = now
				l.mu.Unlock()
				return nil
			}
			delay = ready.Sub(now)
		}
		changed := l.changed
		l.mu.Unlock()

		var timerC <-chan time.Time
		var timer *time.Timer
		if delay >= 0 {
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		case <-changed:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// release frees the in-flight slot taken by acquire.
func (l *Limiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight = false
	l.broadcastLocked()
}

func (l *Limiter) broadcastLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}
```

- [ ] **Step 4: Run the limiter tests to verify they pass**

Run: `go test -race ./internal/coingecko/ -run TestLimiter -v`
Expected: PASS for `TestLimiterFirstWaitIsImmediate`, `TestLimiterEnforcesSpacing`, `TestLimiterCooldown`, `TestLimiterWaitHonorsContext`, `TestLimiterSingleFlight`.

- [ ] **Step 5: Write the failing client tests**

The tests use `httptest.Server` as a fake CoinGecko and a fixed `now` for `fetched_at`. `newTestClient` passes `srv.URL+"/api/v3/"` with a trailing slash on purpose: the client must trim it. The timeout test shortens the unexported `timeout` field (default 10 s) so it runs in 50 ms. `assertS1` validates every produced event against the real S1 schema, as the fetcher will in Task 7.

Create `internal/coingecko/client_test.go`:

```go
package coingecko

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
)

var fixedNow = time.Date(2026, 10, 10, 10, 2, 33, 123_000_000, time.UTC)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schemas/testdata/coingecko/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

func newTestClient(t *testing.T, h http.HandlerFunc, apiKey string) (*Client, *Limiter) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	lim := NewLimiter(0)
	c := NewClient(srv.URL+"/api/v3/", apiKey, srv.Client(), lim, func() time.Time { return fixedNow })
	return c, lim
}

// assertS1 checks the event against the real S1 schema, as the fetcher will.
func assertS1(t *testing.T, ev event.RawEvent) {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	b, err := event.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	vs, err := reg.Validate(validation.IDRawEvent, b)
	if err != nil || len(vs) > 0 {
		t.Fatalf("event violates S1: err=%v violations=%+v\n%s", err, vs, b)
	}
}

func TestMarketsRequest(t *testing.T) {
	r := MarketsRequest(100)
	if r.Path != "/coins/markets" {
		t.Fatalf("path = %q", r.Path)
	}
	want := map[string]string{
		"vs_currency": "usd", "order": "market_cap_desc", "per_page": "100", "page": "1",
		"price_change_percentage": "1h,24h,7d", "sparkline": "false", "precision": "full",
	}
	if len(r.Params) != len(want) {
		t.Fatalf("params = %v", r.Params)
	}
	for k, v := range want {
		if r.Params[k] != v {
			t.Errorf("param %s = %q, want %q", k, r.Params[k], v)
		}
	}
}

func TestCoinDetailRequest(t *testing.T) {
	r := CoinDetailRequest("usd-coin")
	if r.Path != "/coins/usd-coin" {
		t.Fatalf("path = %q", r.Path)
	}
	want := map[string]string{
		"localization": "false", "tickers": "true", "market_data": "true",
		"community_data": "false", "developer_data": "false", "sparkline": "false",
	}
	if len(r.Params) != len(want) {
		t.Fatalf("params = %v", r.Params)
	}
	for k, v := range want {
		if r.Params[k] != v {
			t.Errorf("param %s = %q, want %q", k, r.Params[k], v)
		}
	}
}

func TestFetchOK(t *testing.T) {
	body := fixture(t, "markets_top10_t0.json")
	var gotPath, gotQuery, gotAccept, gotKey string
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		gotAccept, gotKey = r.Header.Get("Accept"), r.Header.Get("x-cg-demo-api-key")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Age", "34")
		w.Header().Set("Cf-Cache-Status", "HIT")
		w.Header().Set("Etag", `W/"c4924cc46ddaa91ee4660396b13c9c63"`)
		_, _ = w.Write(body)
	}, "")

	req := MarketsRequest(10)
	ev := c.Fetch(context.Background(), event.EndpointMarkets, req)

	if gotPath != "/api/v3/coins/markets" {
		t.Errorf("path = %q", gotPath)
	}
	wantQuery := "order=market_cap_desc&page=1&per_page=10&precision=full&price_change_percentage=1h,24h,7d&sparkline=false&vs_currency=usd"
	if gotQuery != wantQuery {
		t.Errorf("query = %q\nwant    %q", gotQuery, wantQuery)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q", gotAccept)
	}
	if gotKey != "" {
		t.Errorf("api key header sent without a key: %q", gotKey)
	}

	if ev.Outcome != event.OutcomeOK || ev.Error != nil {
		t.Fatalf("outcome = %s error = %v", ev.Outcome, ev.Error)
	}
	id, err := uuid.Parse(ev.EventID)
	if err != nil || id.Version() != 7 {
		t.Errorf("event_id %q is not a UUIDv7 (%v)", ev.EventID, err)
	}
	if ev.SchemaVersion != 1 || ev.Source != "coingecko" || ev.Endpoint != "coins_markets" {
		t.Errorf("envelope = %d %s %s", ev.SchemaVersion, ev.Source, ev.Endpoint)
	}
	if ev.FetchedAt != "2026-10-10T10:02:33.123Z" {
		t.Errorf("fetched_at = %s", ev.FetchedAt)
	}
	if !bytes.Equal(ev.Payload, body) {
		t.Errorf("payload is not the verbatim body")
	}
	sum := sha256.Sum256(body)
	wantSHA := hex.EncodeToString(sum[:])
	if ev.PayloadSHA256 == nil || *ev.PayloadSHA256 != wantSHA {
		t.Errorf("payload_sha256 = %v, want %s", ev.PayloadSHA256, wantSHA)
	}
	wantKey := event.IdempotencyKey("coingecko", "coins_markets", req.Params, wantSHA)
	if ev.IdempotencyKey == nil || *ev.IdempotencyKey != wantKey {
		t.Errorf("idempotency_key = %v, want %s", ev.IdempotencyKey, wantKey)
	}
	h := ev.HTTP
	if h == nil || h.Status != 200 {
		t.Fatalf("http = %+v", h)
	}
	if h.Date == nil || h.AgeSeconds == nil || *h.AgeSeconds != 34 ||
		h.CFCacheStatus == nil || *h.CFCacheStatus != "HIT" ||
		h.ETag == nil || *h.ETag != `W/"c4924cc46ddaa91ee4660396b13c9c63"` || h.RetryAfterS != nil {
		t.Errorf("http meta = %+v", h)
	}
	if !lim.Allowed(time.Now()) {
		t.Error("limiter not released after Fetch")
	}
	assertS1(t, ev)
}

func TestFetchSameBodySameKey(t *testing.T) {
	body := fixture(t, "coin_bitcoin_full_t0.json")
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }, "")
	req := CoinDetailRequest("bitcoin")
	a := c.Fetch(context.Background(), event.EndpointCoinDetail, req)
	b := c.Fetch(context.Background(), event.EndpointCoinDetail, req)
	if a.EventID == b.EventID {
		t.Error("two calls share an event_id")
	}
	if a.IdempotencyKey == nil || b.IdempotencyKey == nil || *a.IdempotencyKey != *b.IdempotencyKey {
		t.Error("identical responses must share the idempotency key")
	}
}

func TestFetchSendsAPIKey(t *testing.T) {
	var got string
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("x-cg-demo-api-key")
		_, _ = w.Write([]byte(`[]`))
	}, "CG-demo-key")
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	if got != "CG-demo-key" {
		t.Fatalf("x-cg-demo-api-key = %q", got)
	}
	if ev.Outcome != event.OutcomeOK {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
}

func TestFetch429SetsCooldown(t *testing.T) {
	body := `{"status":{"error_code":429,"error_message":"You've exceeded the Rate Limit."}}`
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(body))
	}, "")
	before := time.Now()
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))

	if ev.Outcome != event.OutcomeHTTPError {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	if ev.HTTP == nil || ev.HTTP.Status != 429 || ev.HTTP.RetryAfterS == nil || *ev.HTTP.RetryAfterS != 60 {
		t.Fatalf("http = %+v", ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "HTTP 429") || !strings.Contains(*ev.Error, "Rate Limit") {
		t.Errorf("error = %v", ev.Error)
	}
	if ev.Payload != nil || ev.PayloadSHA256 != nil || ev.IdempotencyKey != nil {
		t.Error("failed call must not carry payload, hash or idempotency key")
	}
	if lim.Allowed(before.Add(59 * time.Second)) {
		t.Error("limiter allows requests 59s after a 429 with Retry-After: 60")
	}
	if !lim.Allowed(time.Now().Add(61 * time.Second)) {
		t.Error("limiter still cooling down after Retry-After elapsed")
	}
	assertS1(t, ev)
}

func TestFetch429WithoutRetryAfterCoolsDown60s(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}, "")
	before := time.Now()
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.HTTP == nil || ev.HTTP.Status != 429 || ev.HTTP.RetryAfterS != nil {
		t.Fatalf("http = %+v", ev.HTTP)
	}
	if lim.Allowed(before.Add(59 * time.Second)) {
		t.Error("no default 60s cooldown after a 429 without Retry-After")
	}
	if !lim.Allowed(time.Now().Add(61 * time.Second)) {
		t.Error("default cooldown longer than 60s")
	}
}

func TestFetch500(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusInternalServerError)
	}, "")
	ev := c.Fetch(context.Background(), event.EndpointCoinDetail, CoinDetailRequest("bitcoin"))
	if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 500 {
		t.Fatalf("outcome = %s http = %+v", ev.Outcome, ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "HTTP 500: upstream exploded") {
		t.Errorf("error = %v", ev.Error)
	}
	if lim.Allowed(time.Now()) {
		t.Error("500 must start a backoff cooldown")
	}
	assertS1(t, ev)
}

// Spec §3.2: exponential backoff with jitter on 5xx and timeouts. The
// cooldown doubles per consecutive failure, is capped, and resets after a
// good response.
func TestFetchBackoffGrowsCapsAndResets(t *testing.T) {
	status := http.StatusBadGateway
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			http.Error(w, "bad gateway", status)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}, "")
	var got []time.Duration
	c.backoffBase, c.backoffMax = time.Millisecond, 4*time.Millisecond
	c.jitter = func(d time.Duration) time.Duration { got = append(got, d); return d }
	for range 4 {
		c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	}
	status = http.StatusOK
	if ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1)); ev.Outcome != event.OutcomeOK {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	status = http.StatusServiceUnavailable
	c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(1))
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond, time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("cooldowns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cooldowns = %v, want %v", got, want)
		}
	}
	for range 1000 {
		if j := equalJitter(10 * time.Second); j < 5*time.Second || j >= 10*time.Second {
			t.Fatalf("equalJitter(10s) = %v, want [5s, 10s)", j)
		}
	}
}

func TestFetchInvalidJSONBody(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<html>maintenance\x00\xff</html>"))
	}, "")
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 200 {
		t.Fatalf("outcome = %s http = %+v", ev.Outcome, ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "invalid JSON body") {
		t.Fatalf("error = %v", ev.Error)
	}
	if strings.Contains(*ev.Error, "\x00") {
		t.Error("error text contains NUL")
	}
	if ev.Payload != nil || ev.PayloadSHA256 != nil || ev.IdempotencyKey != nil {
		t.Error("invalid body must not be stored as payload")
	}
	assertS1(t, ev)
}

// A 200 body that encoding/json accepts but strict JSON rejects (duplicate
// member names, invalid UTF-8) or a bare null must not become an ok event:
// S1 validation would reject it later and the call would vanish.
func TestFetchRejectsNonStrictJSONBody(t *testing.T) {
	for name, body := range map[string]string{
		"duplicate names": `[{"id":"bitcoin","id":"ethereum"}]`,
		"invalid utf-8":   "[{\"id\":\"bit\xffcoin\"}]",
		"null":            `null`,
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }, "")
			c.jitter = func(time.Duration) time.Duration { return 0 }
			ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
			if ev.Outcome != event.OutcomeHTTPError || ev.HTTP == nil || ev.HTTP.Status != 200 || ev.Payload != nil {
				t.Fatalf("outcome = %s http = %+v payload = %s", ev.Outcome, ev.HTTP, ev.Payload)
			}
			assertS1(t, ev)
		})
	}
}

func TestFetchTimeoutIsNetworkError(t *testing.T) {
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}, "")
	c.timeout = 50 * time.Millisecond
	c.jitter = func(time.Duration) time.Duration { return 0 } // isolate the release check from the backoff
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError {
		t.Fatalf("outcome = %s", ev.Outcome)
	}
	if ev.HTTP != nil {
		t.Errorf("network error must have http = nil, got %+v", ev.HTTP)
	}
	if ev.Error == nil || !strings.Contains(*ev.Error, "deadline exceeded") {
		t.Errorf("error = %v", ev.Error)
	}
	if ev.FetchedAt == "" {
		t.Error("fetched_at missing")
	}
	if !lim.Allowed(time.Now()) {
		t.Error("limiter not released after a timeout")
	}
	assertS1(t, ev)
}

func TestFetchConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := NewClient(url, "", &http.Client{}, NewLimiter(0), func() time.Time { return fixedNow })
	ev := c.Fetch(context.Background(), event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError || ev.HTTP != nil || ev.Error == nil {
		t.Fatalf("event = %+v", ev)
	}
}

func TestFetchCancelledWhileCoolingDown(t *testing.T) {
	calls := 0
	c, lim := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { calls++ }, "")
	lim.Cooldown(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ev := c.Fetch(ctx, event.EndpointMarkets, MarketsRequest(100))
	if ev.Outcome != event.OutcomeNetworkError || calls != 0 {
		t.Fatalf("outcome = %s calls = %d", ev.Outcome, calls)
	}
	if ev.Error == nil || !strings.HasPrefix(*ev.Error, "rate limiter: ") {
		t.Errorf("error = %v", ev.Error)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 10, 10, 8, 44, 0, time.UTC)
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{"60", 60, true},
		{" 5 ", 5, true},
		{"Sat, 10 Oct 2026 10:09:44 GMT", 60, true},
		{"Sat, 10 Oct 2026 10:00:00 GMT", 0, true},
		{"-1", 0, false},
		{"soon", 0, false},
	}
	for _, tc := range cases {
		got, ok := parseRetryAfter(tc.in, now)
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseRetryAfter(%q) = %d,%v want %d,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
```

- [ ] **Step 6: Run the client tests to verify they fail**

Run: `go test ./internal/coingecko/`
Expected: FAIL to build: `internal/coingecko/client_test.go:33:71: undefined: Client` and `undefined: NewClient`

- [ ] **Step 7: Implement the client**

`uuid.NewV7()` (github.com/google/uuid) returns a time-ordered UUID. `uuid.Must` panics only if the system random source fails. `io.LimitReader(body, max+1)` caps memory use, and reading one extra byte shows whether the body was larger than the limit. `json.Valid` checks syntax only; it does not decode, so no float64 rounding happens.

Create `internal/coingecko/client.go`:

```go
// Package coingecko is the transport layer for the CoinGecko REST API. It
// turns one HTTP call into one event.RawEvent (schema S1) and never inspects
// the payload beyond "is it JSON".
package coingecko

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"

	"github.com/google/uuid"
)

const (
	defaultTimeout    = 10 * time.Second
	defaultCooldown   = 60 * time.Second // used on 429 without a usable Retry-After
	backoffBase       = 2 * time.Second  // first cooldown after a 5xx / timeout / connection error
	backoffMax        = 60 * time.Second // cap of the exponential backoff
	maxBodyBytes      = 32 << 20         // a /coins/{id} body is ~100 KB
	maxErrorBodyBytes = 512              // body excerpt kept in event.Error
	userAgent         = "coingecko-etl/1"
)

// Client calls CoinGecko through a shared Limiter.
type Client struct {
	baseURL string
	apiKey  string
	hc      *http.Client
	lim     *Limiter
	now     func() time.Time
	timeout time.Duration // per request; tests shorten it

	// Exponential backoff with jitter on transient failures (spec §3.2).
	// failures is only touched while Fetch holds the limiter's in-flight
	// slot, so it needs no lock of its own.
	failures    int
	backoffBase time.Duration
	backoffMax  time.Duration
	jitter      func(time.Duration) time.Duration // tests replace it
}

// NewClient builds a client. baseURL is e.g. "https://api.coingecko.com/api/v3".
// apiKey may be empty (keyless). now supplies fetched_at (time.Now in production).
func NewClient(baseURL, apiKey string, hc *http.Client, l *Limiter, now func() time.Time) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	if l == nil {
		l = NewLimiter(0)
	}
	if now == nil {
		now = time.Now
	}
	return &Client{
		baseURL:     strings.TrimRight(baseURL, "/"),
		apiKey:      apiKey,
		hc:          hc,
		lim:         l,
		now:         now,
		timeout:     defaultTimeout,
		backoffBase: backoffBase,
		backoffMax:  backoffMax,
		jitter:      equalJitter,
	}
}

// equalJitter returns a duration in [d/2, d): half fixed, half random, so
// processes that failed together do not retry in lockstep.
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

// backoff records one more consecutive transient failure and cools the
// limiter down for backoffBase * 2^(failures-1), capped at backoffMax, with
// jitter. Ticks that fall inside the cooldown are skipped, never queued.
func (c *Client) backoff() {
	c.failures++
	d := c.backoffMax
	if shift := c.failures - 1; shift < 16 {
		if v := c.backoffBase << shift; v > 0 && v < d {
			d = v
		}
	}
	c.lim.Cooldown(c.jitter(d))
}

// MarketsRequest is GET /coins/markets for the top perPage coins by market cap.
func MarketsRequest(perPage int) event.Request {
	return event.Request{
		Path: "/coins/markets",
		Params: map[string]string{
			"vs_currency":             "usd",
			"order":                   "market_cap_desc",
			"per_page":                strconv.Itoa(perPage),
			"page":                    "1",
			"price_change_percentage": "1h,24h,7d",
			"sparkline":               "false",
			"precision":               "full",
		},
	}
}

// CoinDetailRequest is GET /coins/{id} with market data and tickers.
func CoinDetailRequest(coinID string) event.Request {
	return event.Request{
		Path: "/coins/" + url.PathEscape(coinID),
		Params: map[string]string{
			"localization":   "false",
			"tickers":        "true",
			"market_data":    "true",
			"community_data": "false",
			"developer_data": "false",
			"sparkline":      "false",
		},
	}
}

// requestURL joins baseURL, path and the params sorted by key. Commas stay
// literal (price_change_percentage=1h,24h,7d), everything else is escaped.
func requestURL(baseURL string, req event.Request) string {
	keys := make([]string, 0, len(req.Params))
	for k := range req.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(baseURL)
	sb.WriteString(req.Path)
	for i, k := range keys {
		if i == 0 {
			sb.WriteByte('?')
		} else {
			sb.WriteByte('&')
		}
		sb.WriteString(url.QueryEscape(k))
		sb.WriteByte('=')
		sb.WriteString(strings.ReplaceAll(url.QueryEscape(req.Params[k]), "%2C", ","))
	}
	return sb.String()
}

// Fetch performs exactly one HTTP call (never retries) and returns the event.
// It never returns an error: failures are encoded in Outcome/HTTP/Error.
//   - ok:            2xx and the body is valid JSON; Payload, PayloadSHA256, IdempotencyKey set
//   - http_error:    any other response (non-2xx, or 2xx with a non-JSON body); HTTP set
//   - network_error: no complete response (DNS, connect, timeout, body read, ctx); HTTP nil
//
// On HTTP 429 it calls Cooldown(Retry-After, or 60 s) on the limiter. On a
// 5xx, a 2xx with a non-JSON body, a timeout or a connection error it calls
// Cooldown with an exponential backoff (2 s, 4 s, ... capped at 60 s, with
// jitter); any other response resets the backoff.
func (c *Client) Fetch(ctx context.Context, endpoint string, req event.Request) event.RawEvent {
	ev := event.RawEvent{
		SchemaVersion: event.SchemaVersion,
		EventID:       uuid.Must(uuid.NewV7()).String(),
		Source:        event.SourceCoinGecko,
		Endpoint:      endpoint,
		Request:       req,
	}
	if err := c.lim.acquire(ctx); err != nil {
		ev.FetchedAt = xtime.Format(c.now())
		return failed(ev, event.OutcomeNetworkError, "rate limiter: "+err.Error())
	}
	defer c.lim.release()
	ev.FetchedAt = xtime.Format(c.now())

	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL(c.baseURL, req), nil)
	if err != nil {
		return failed(ev, event.OutcomeNetworkError, "build request: "+err.Error())
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", userAgent)
	if c.apiKey != "" {
		httpReq.Header.Set("x-cg-demo-api-key", c.apiKey)
	}

	start := time.Now()
	resp, err := c.hc.Do(httpReq)
	if err != nil {
		if parent.Err() == nil { // shutdown is not an upstream failure
			c.backoff()
		}
		return failed(ev, event.OutcomeNetworkError, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	latency := time.Since(start)
	if err != nil {
		if parent.Err() == nil {
			c.backoff()
		}
		return failed(ev, event.OutcomeNetworkError, fmt.Sprintf("read body (HTTP %d): %v", resp.StatusCode, err))
	}

	ev.HTTP = httpMeta(resp, latency, c.now())
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		cd := defaultCooldown
		if ra := ev.HTTP.RetryAfterS; ra != nil && *ra > 0 {
			cd = time.Duration(*ra) * time.Second
		}
		c.lim.Cooldown(cd)
	case resp.StatusCode >= 500:
		c.backoff()
	}

	switch {
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			c.failures = 0 // a 4xx is an answer, not an outage
		}
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, excerpt(body)))
	case len(body) > maxBodyBytes:
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: body larger than %d bytes", resp.StatusCode, maxBodyBytes))
	}
	// Surrounding whitespace is not part of the JSON value; trimming it keeps
	// the payload on one JSONL line. The bytes inside are kept verbatim.
	payload := bytes.TrimSpace(body)
	// Strict JSON (valid UTF-8, no duplicate member names), the same rule the
	// validator applies at every later hop. encoding/json's Valid is more
	// lenient: a body it accepts could be stored as ok and then fail S1.
	// A literal null is no snapshot either.
	if !jsontext.Value(payload).IsValid() || bytes.Equal(payload, []byte("null")) {
		// An HTML maintenance or challenge page served with 200 is transient.
		c.backoff()
		return failed(ev, event.OutcomeHTTPError, fmt.Sprintf("HTTP %d: invalid JSON body: %s", resp.StatusCode, excerpt(body)))
	}
	c.failures = 0 // a good response ends the backoff
	sha := event.SHA256Hex(payload)
	key := event.IdempotencyKey(ev.Source, ev.Endpoint, req.Params, sha)
	ev.Outcome = event.OutcomeOK
	ev.Payload = json.RawMessage(payload)
	ev.PayloadSHA256 = &sha
	ev.IdempotencyKey = &key
	return ev
}

// failed fills the outcome fields of a non-ok event: no payload, no hash, no key.
func failed(ev event.RawEvent, outcome, msg string) event.RawEvent {
	ev.Outcome = outcome
	ev.Error = &msg
	ev.Payload = nil
	ev.PayloadSHA256 = nil
	ev.IdempotencyKey = nil
	return ev
}

func httpMeta(resp *http.Response, latency time.Duration, now time.Time) *event.HTTPMeta {
	m := &event.HTTPMeta{Status: resp.StatusCode, LatencyMS: latency.Milliseconds()}
	m.Date = headerPtr(resp.Header, "Date")
	m.CFCacheStatus = headerPtr(resp.Header, "Cf-Cache-Status")
	m.ETag = headerPtr(resp.Header, "Etag")
	if v := resp.Header.Get("Age"); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			m.AgeSeconds = &n
		}
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		if n, ok := parseRetryAfter(v, now); ok {
			m.RetryAfterS = &n
		}
	}
	return m
}

// parseRetryAfter accepts both forms of RFC 9110 Retry-After: delay-seconds
// ("60") and an HTTP-date ("Sat, 10 Oct 2026 10:09:44 GMT").
func parseRetryAfter(v string, now time.Time) (int, bool) {
	v = strings.TrimSpace(v)
	if n, err := strconv.Atoi(v); err == nil {
		if n < 0 {
			return 0, false
		}
		return n, true
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	secs := int(math.Ceil(t.Sub(now).Seconds()))
	if secs < 0 {
		secs = 0
	}
	return secs, true
}

func headerPtr(h http.Header, name string) *string {
	v := h.Get(name)
	if v == "" {
		return nil
	}
	return &v
}

// excerpt returns at most maxErrorBodyBytes of body as valid UTF-8 without
// NUL bytes (Postgres text rejects both invalid UTF-8 and NUL).
func excerpt(body []byte) string {
	if len(body) > maxErrorBodyBytes {
		body = body[:maxErrorBodyBytes]
	}
	s := strings.ToValidUTF8(string(body), "�")
	return strings.ReplaceAll(s, "\x00", "")
}
```

If `github.com/google/uuid` is not in `go.mod` yet, run `go get github.com/google/uuid@v1.6.0`.

- [ ] **Step 8: Run all coingecko tests to verify they pass**

Run: `go test -race ./internal/coingecko/ -v`
Expected: PASS: all `TestLimiter*`, `TestMarketsRequest`, `TestCoinDetailRequest`, `TestFetchOK`, `TestFetchSameBodySameKey`, `TestFetchSendsAPIKey`, `TestFetch429SetsCooldown`, `TestFetch429WithoutRetryAfterCoolsDown60s`, `TestFetch500`, `TestFetchBackoffGrowsCapsAndResets`, `TestFetchInvalidJSONBody`, `TestFetchRejectsNonStrictJSONBody`, `TestFetchTimeoutIsNetworkError`, `TestFetchConnectionRefused`, `TestFetchCancelledWhileCoolingDown`, `TestParseRetryAfter`.

Then: `go vet ./internal/coingecko/` (expected: no output).

- [ ] **Step 9: Commit**

```bash
git add internal/coingecko/ go.mod go.sum
git commit -m "feat(coingecko): HTTP client with single-flight limiter and Retry-After cooldown

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

### Task 7: Fetcher service

**Files:**
- Create: `internal/fetcher/fetcher.go`
- Test: `internal/fetcher/fetcher_test.go` (unit: virtual-clock scheduler)
- Test: `internal/fetcher/fetcher_integration_test.go` (Postgres via testcontainers + fake CoinGecko)
- Uses fixtures: `schemas/testdata/coingecko/markets_top10_t0.json`, `markets_usd250_t0.json`, `coin_bitcoin_full_t0.json`, `coin_usd-coin_meta.json`

**Interfaces:**
- Consumes:
  - `type config.Config` (fields `MarketsInterval`, `DetailInterval`, `DetailOffset`, `Watchlist`, `MarketsPerPage`, `MinRequestSpacing`)
  - `func coingecko.MarketsRequest(perPage int) event.Request`, `func coingecko.CoinDetailRequest(coinID string) event.Request`
  - `func (c *coingecko.Client) Fetch(ctx context.Context, endpoint string, req event.Request) event.RawEvent`
  - `func (l *coingecko.Limiter) Allowed(now time.Time) bool`
  - `func coingecko.NewLimiter(minSpacing time.Duration) *Limiter`, `func coingecko.NewClient(baseURL, apiKey string, hc *http.Client, l *Limiter, now func() time.Time) *Client` (integration test)
  - `func pg.InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error`
  - `func pg.EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error`
  - `func pg.Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)`, `func pg.Migrate(ctx context.Context, pool *pgxpool.Pool) error` (integration test)
  - `func validation.Load(fsys fs.FS) (*Registry, error)`, `func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)`, `func validation.Keyword(v Violation) string`, `validation.IDRawEvent`, `schemas.FS`
  - `func event.Marshal(ev RawEvent) ([]byte, error)`, `func event.SHA256Hex`, `func event.IdempotencyKey`
  - `func xtime.Format(t time.Time) string`, `func xtime.Parse(s string) (time.Time, error)`
  - `obs.Metrics` fields `APIRequests`, `APIRequestDuration`, `APIRateLimited`, `LastSuccess`, `SourceDataAge`, `SchemaValidations`, `SchemaViolations`; `func obs.NewMetrics(reg prometheus.Registerer) *Metrics`
  - `func testutil.SkipIfNoDocker(t *testing.T)`, `func testutil.StartPostgres(t *testing.T) (dsn string)`
- Produces:
  - `func Run(ctx context.Context, cfg config.Config, cg *coingecko.Client, lim *coingecko.Limiter, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) error`

Background for the engineer:
- **Two fixed-rate schedules plus maintenance.**
  - Markets: at start, then every `MarketsInterval`.
  - Detail: at start + `DetailOffset`, then every `DetailInterval`, rotating round-robin over `Watchlist`.
  - Partitions: `EnsurePartitions(now, 3)` at start (a failure aborts startup), then hourly (a failure is logged at ERROR).
- **One goroutine runs everything, synchronously.** The client allows one request in flight anyway, and a single loop means a tick can never be queued behind a running request.
- **Skipping, not queueing.** When the loop gets back from a slow request, every slot that came due more than 1 s ago is skipped (reason `in_flight`). When the limiter is cooling down after a 429, the tick is skipped (reason `cooldown`). Each skip increments `etl_api_requests_total{endpoint,status="skipped"}` and logs at DEBUG. A skipped detail tick does not advance the rotation, so the coin whose tick was skipped is fetched next.
- **The `clock` interface** (`Now`, `After`) makes the scheduler testable in virtual time. The test clock's `After(d)` jumps time forward by `d` and fires at once, so 3 minutes of schedule run in microseconds and deterministically.
- **Envelope validation.** Every event is checked against S1 before INSERT. The fetcher built the envelope itself, so a violation is our bug: it is logged at ERROR, counted as `etl_schema_validations_total{stage="fetch",schema="raw_event.v1",result="fail"}` plus one `etl_schema_violations_total` per keyword, and not inserted. The payload is never validated here; S0 runs in the transformer.
- **Failed calls are rows too.** Rows with outcome `http_error` or `network_error` are inserted (the spec stores failures for SQL analysis). On shutdown (ctx cancelled), an in-progress call is dropped, because its "network error" was caused by us.
- **`etl_source_data_age_seconds`** is `fetched_at` minus the newest `last_updated` in the payload: the max over the markets array, or the top-level field of a `/coins/{id}` document. Only that field is decoded, into a one-field struct, so no money values pass through float64.

- [ ] **Step 1: Write the failing scheduler unit tests**

Create `internal/fetcher/fetcher_test.go`:

```go
package fetcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

// fakeClock runs virtual time: After(d) advances the clock by d and fires
// immediately, so run() executes hours of schedule in microseconds. When
// the next wake-up would pass end, it cancels the run instead.
type fakeClock struct {
	t      time.Time
	end    time.Time
	cancel context.CancelFunc
}

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	if c.t.Add(d).After(c.end) {
		c.cancel()
		return ch // never fires; run() returns via ctx.Done()
	}
	c.t = c.t.Add(d)
	ch <- c.t
	return ch
}

type call struct {
	at       time.Duration // offset from t0
	endpoint string
	path     string
}

type harness struct {
	r          *runner
	clk        *fakeClock
	m          *obs.Metrics
	calls      []call
	inserted   []event.RawEvent
	partitions int
	// respond builds the event for a call; default: 200 ok.
	respond func(endpoint string, req event.Request, now time.Time) event.RawEvent
	// cooldownUntil emulates the limiter: allowed is false before it.
	cooldownUntil time.Time
	// latency is added to the clock by every fetch.
	latency func(n int) time.Duration
}

func newHarness(t *testing.T, cfg config.Config, horizon time.Duration) (*harness, context.Context) {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &harness{clk: &fakeClock{t: t0, end: t0.Add(horizon), cancel: cancel}, m: obs.NewMetrics(prometheus.NewRegistry())}
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		if endpoint == event.EndpointMarkets {
			return okEvent(endpoint, req, now, []byte(`[{"id":"a","last_updated":"2026-10-10T09:59:30.000Z"},{"id":"b","last_updated":"2026-10-10T07:55:52.000Z"}]`))
		}
		return okEvent(endpoint, req, now, []byte(`{"id":"x","last_updated":"2026-10-10T09:59:30.000Z"}`))
	}
	h.r = &runner{
		cfg: cfg,
		clk: h.clk,
		fetch: func(ctx context.Context, endpoint string, req event.Request) event.RawEvent {
			now := h.clk.t
			h.calls = append(h.calls, call{at: now.Sub(t0), endpoint: endpoint, path: req.Path})
			if h.latency != nil {
				h.clk.t = h.clk.t.Add(h.latency(len(h.calls)))
			}
			return h.respond(endpoint, req, now)
		},
		allowed: func(now time.Time) bool { return !now.Before(h.cooldownUntil) },
		insert: func(ctx context.Context, ev event.RawEvent) error {
			h.inserted = append(h.inserted, ev)
			return nil
		},
		ensurePartitions: func(ctx context.Context, now time.Time) error { h.partitions++; return nil },
		reg:              reg,
		m:                h.m,
		log:              slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	return h, ctx
}

func okEvent(endpoint string, req event.Request, now time.Time, payload []byte) event.RawEvent {
	sha := event.SHA256Hex(payload)
	key := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	return event.RawEvent{
		SchemaVersion: event.SchemaVersion, EventID: uuid.Must(uuid.NewV7()).String(), IdempotencyKey: &key,
		Source: event.SourceCoinGecko, Endpoint: endpoint, Request: req, FetchedAt: xtime.Format(now),
		Outcome: event.OutcomeOK, HTTP: &event.HTTPMeta{Status: 200, LatencyMS: 120},
		PayloadSHA256: &sha, Payload: json.RawMessage(payload),
	}
}

func http429Event(endpoint string, req event.Request, now time.Time) event.RawEvent {
	msg := "HTTP 429: rate limited"
	ra := 60
	return event.RawEvent{
		SchemaVersion: event.SchemaVersion, EventID: uuid.Must(uuid.NewV7()).String(),
		Source: event.SourceCoinGecko, Endpoint: endpoint, Request: req, FetchedAt: xtime.Format(now),
		Outcome: event.OutcomeHTTPError, HTTP: &event.HTTPMeta{Status: 429, LatencyMS: 80, RetryAfterS: &ra},
		Error: &msg,
	}
}

func cfg(watchlist ...string) config.Config {
	return config.Config{
		MarketsInterval: 30 * time.Second, DetailInterval: 60 * time.Second, DetailOffset: 15 * time.Second,
		Watchlist: watchlist, MarketsPerPage: 100,
	}
}

func skipped(m *obs.Metrics, endpoint string) float64 {
	return promtest.ToFloat64(m.APIRequests.WithLabelValues(endpoint, "skipped"))
}

func TestScheduleAndRotation(t *testing.T) {
	h, ctx := newHarness(t, cfg("bitcoin", "ethereum", "tether"), 3*time.Minute)
	if err := h.r.run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	m, d := event.EndpointMarkets, event.EndpointCoinDetail
	want := []call{
		{0, m, "/coins/markets"},
		{15 * time.Second, d, "/coins/bitcoin"},
		{30 * time.Second, m, "/coins/markets"},
		{60 * time.Second, m, "/coins/markets"},
		{75 * time.Second, d, "/coins/ethereum"},
		{90 * time.Second, m, "/coins/markets"},
		{120 * time.Second, m, "/coins/markets"},
		{135 * time.Second, d, "/coins/tether"},
		{150 * time.Second, m, "/coins/markets"},
		{180 * time.Second, m, "/coins/markets"},
	}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if len(h.inserted) != len(want) {
		t.Errorf("inserted %d rows, want %d", len(h.inserted), len(want))
	}
	if h.partitions != 1 {
		t.Errorf("EnsurePartitions called %d times in 3 min, want 1 (startup)", h.partitions)
	}
	if got := promtest.ToFloat64(h.m.APIRequests.WithLabelValues(m, "200")); got != 7 {
		t.Errorf("api_requests_total{coins_markets,200} = %v, want 7", got)
	}
	if got := promtest.ToFloat64(h.m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "pass")); got != 10 {
		t.Errorf("schema_validations_total{valid} = %v, want 10", got)
	}
	if skipped(h.m, m)+skipped(h.m, d) != 0 {
		t.Error("ticks skipped without cooldown or slow requests")
	}
	// Source data age: fetched_at (10:03:00) - last_updated (09:59:30) = 210 s.
	if got := promtest.ToFloat64(h.m.SourceDataAge.WithLabelValues(m)); got != 210 {
		t.Errorf("source_data_age_seconds{coins_markets} = %v, want 210", got)
	}
	if got := promtest.ToFloat64(h.m.LastSuccess.WithLabelValues(m)); got != float64(t0.Add(3*time.Minute).Unix()) {
		t.Errorf("last_success = %v", got)
	}
}

func TestPartitionsHourly(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 2*time.Hour+time.Minute)
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	if h.partitions != 3 { // start, +1h, +2h
		t.Fatalf("EnsurePartitions calls = %d, want 3", h.partitions)
	}
}

func TestSkipsTicksDuringCooldown(t *testing.T) {
	h, ctx := newHarness(t, cfg("bitcoin", "ethereum"), 2*time.Minute)
	first := true
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		if first {
			first = false
			h.cooldownUntil = now.Add(60 * time.Second) // what Client.Fetch does on 429
			return http429Event(endpoint, req, now)
		}
		return okEvent(endpoint, req, now, []byte(`[]`))
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	m, d := event.EndpointMarkets, event.EndpointCoinDetail
	want := []call{
		{0, m, "/coins/markets"},                // 429 -> cooldown until 60 s
		{60 * time.Second, m, "/coins/markets"}, // 15 s detail and 30 s markets skipped
		{75 * time.Second, d, "/coins/bitcoin"}, // rotation did not advance on the skip
		{90 * time.Second, m, "/coins/markets"},
		{120 * time.Second, m, "/coins/markets"},
	}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if skipped(h.m, m) != 1 || skipped(h.m, d) != 1 {
		t.Errorf("skipped markets=%v detail=%v, want 1 and 1", skipped(h.m, m), skipped(h.m, d))
	}
	if got := promtest.ToFloat64(h.m.APIRateLimited); got != 1 {
		t.Errorf("api_rate_limited_total = %v, want 1", got)
	}
	if got := promtest.ToFloat64(h.m.APIRequests.WithLabelValues(m, "429")); got != 1 {
		t.Errorf("api_requests_total{coins_markets,429} = %v, want 1", got)
	}
	if len(h.inserted) != 5 || h.inserted[0].Outcome != event.OutcomeHTTPError {
		t.Fatalf("inserted %d rows, first outcome %q; the 429 must be stored as http_error", len(h.inserted), h.inserted[0].Outcome)
	}
}

func TestSkipsTicksThatCameDueWhileInFlight(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 2*time.Minute)
	h.latency = func(n int) time.Duration {
		if n == 1 {
			return 70 * time.Second // the first request hangs past two ticks
		}
		return 0
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	m := event.EndpointMarkets
	want := []call{{0, m, "/coins/markets"}, {90 * time.Second, m, "/coins/markets"}, {120 * time.Second, m, "/coins/markets"}}
	if !reflect.DeepEqual(h.calls, want) {
		t.Fatalf("calls:\n got %v\nwant %v", h.calls, want)
	}
	if got := skipped(h.m, m); got != 2 {
		t.Errorf("skipped = %v, want 2 (ticks at 30 s and 60 s)", got)
	}
}

func TestInvalidEnvelopeIsNotInserted(t *testing.T) {
	h, ctx := newHarness(t, cfg(), 0)
	h.respond = func(endpoint string, req event.Request, now time.Time) event.RawEvent {
		ev := okEvent(endpoint, req, now, []byte(`[]`))
		ev.EventID = "not-a-uuid" // a bug in our envelope code
		return ev
	}
	if err := h.r.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 1 {
		t.Fatalf("calls = %d", len(h.calls))
	}
	if len(h.inserted) != 0 {
		t.Fatal("invalid envelope was inserted")
	}
	if got := promtest.ToFloat64(h.m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "fail")); got != 1 {
		t.Errorf("schema_validations_total{invalid} = %v, want 1", got)
	}
}

func TestRunRejectsZeroIntervals(t *testing.T) {
	h, ctx := newHarness(t, config.Config{}, time.Minute)
	if err := h.r.run(ctx); err == nil {
		t.Fatal("run accepted zero intervals")
	}
}

func TestSourceLastUpdated(t *testing.T) {
	read := func(name string) []byte {
		b, err := os.ReadFile("../../schemas/testdata/coingecko/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		endpoint, file, want string
	}{
		// 248/250 rows share 10:07:55; the max wins over stale rows.
		{event.EndpointMarkets, "markets_usd250_t0.json", "2026-10-10T10:07:55.000Z"},
		{event.EndpointCoinDetail, "coin_bitcoin_full_t0.json", "2026-10-10T10:01:51.000Z"},
	}
	for _, tc := range cases {
		got, ok := sourceLastUpdated(tc.endpoint, read(tc.file))
		if !ok || xtime.Format(got) != tc.want {
			t.Errorf("%s: got %v %v, want %s", tc.file, xtime.Format(got), ok, tc.want)
		}
	}
	if _, ok := sourceLastUpdated(event.EndpointMarkets, []byte(`[{"last_updated":null}]`)); ok {
		t.Error("null last_updated reported as ok")
	}
	if _, ok := sourceLastUpdated(event.EndpointCoinDetail, []byte(`{"id":"x"}`)); ok {
		t.Error("missing last_updated reported as ok")
	}
}
```

- [ ] **Step 2: Run the unit tests to verify they fail**

Run: `go test ./internal/fetcher/`
Expected: FAIL to build: `internal/fetcher/fetcher_test.go:56:14: undefined: runner`

- [ ] **Step 3: Implement the fetcher**

Create `internal/fetcher/fetcher.go`:

```go
// Package fetcher is the `etl fetcher` service: it polls CoinGecko on two
// schedules and inserts one raw.api_calls row per HTTP call (including
// failed calls). It is pure transport: the payload is never validated here.
package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	stage              = "fetch"
	schemaLabel        = "raw_event.v1"
	partitionEvery     = time.Hour
	partitionDaysAhead = 3
	// A tick that is more than lateTolerance overdue came due while a
	// request was in flight; it is skipped, not run late.
	lateTolerance = time.Second
	statusSkipped = "skipped"
)

// clock abstracts time so the scheduler can be tested with virtual time.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// job is one fixed-rate schedule. Slots are start, start+every, ...
type job struct {
	endpoint string // "" for maintenance jobs (never counted as skipped)
	every    time.Duration
	next     time.Time
	run      func(ctx context.Context, now time.Time)
}

type runner struct {
	cfg              config.Config
	clk              clock
	fetch            func(ctx context.Context, endpoint string, req event.Request) event.RawEvent
	allowed          func(now time.Time) bool
	insert           func(ctx context.Context, ev event.RawEvent) error
	ensurePartitions func(ctx context.Context, now time.Time) error
	reg              *validation.Registry
	m                *obs.Metrics
	log              *slog.Logger
	detailIdx        int
}

// Run polls until ctx is cancelled, then returns nil. It returns an error
// only for startup failures (schemas, invalid intervals, partitions).
func Run(ctx context.Context, cfg config.Config, cg *coingecko.Client, lim *coingecko.Limiter, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) error {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		return fmt.Errorf("fetcher: load schemas: %w", err)
	}
	r := &runner{
		cfg:     cfg,
		clk:     realClock{},
		fetch:   cg.Fetch,
		allowed: lim.Allowed,
		insert: func(ctx context.Context, ev event.RawEvent) error {
			return pg.InsertAPICall(ctx, pool, ev)
		},
		ensurePartitions: func(ctx context.Context, now time.Time) error {
			return pg.EnsurePartitions(ctx, pool, now, partitionDaysAhead)
		},
		reg: reg,
		m:   m,
		log: log.With("stage", stage),
	}
	return r.run(ctx)
}

func (r *runner) run(ctx context.Context) error {
	if r.cfg.MarketsInterval <= 0 || r.cfg.DetailInterval <= 0 {
		return errors.New("fetcher: MarketsInterval and DetailInterval must be > 0")
	}
	start := r.clk.Now()
	if err := r.ensurePartitions(ctx, start); err != nil {
		return fmt.Errorf("fetcher: ensure partitions: %w", err)
	}
	jobs := []*job{
		{endpoint: event.EndpointMarkets, every: r.cfg.MarketsInterval, next: start, run: r.tickMarkets},
		{endpoint: "", every: partitionEvery, next: start.Add(partitionEvery), run: r.tickPartitions},
	}
	if len(r.cfg.Watchlist) > 0 {
		jobs = append(jobs, &job{endpoint: event.EndpointCoinDetail, every: r.cfg.DetailInterval,
			next: start.Add(r.cfg.DetailOffset), run: r.tickDetail})
	} else {
		r.log.Warn("watchlist is empty; coin detail polling disabled")
	}
	r.log.Info("fetcher started",
		"markets_interval", r.cfg.MarketsInterval.String(),
		"detail_interval", r.cfg.DetailInterval.String(),
		"detail_offset", r.cfg.DetailOffset.String(),
		"watchlist", r.cfg.Watchlist)

	for {
		for _, j := range jobs {
			now := r.clk.Now() // re-read: the previous job may have taken a while
			if now.Before(j.next) {
				continue
			}
			// Slots due so far: next, next+every, ... <= now.
			due := int(now.Sub(j.next)/j.every) + 1
			last := j.next.Add(time.Duration(due-1) * j.every)
			j.next = last.Add(j.every)
			skipped := due - 1
			runIt := now.Sub(last) <= lateTolerance || j.endpoint == ""
			if !runIt {
				skipped = due
			}
			if skipped > 0 && j.endpoint != "" {
				r.skip(j.endpoint, skipped, "in_flight")
			}
			if runIt {
				j.run(ctx, now)
			}
			if ctx.Err() != nil {
				return nil
			}
		}
		earliest := jobs[0].next
		for _, j := range jobs[1:] {
			if j.next.Before(earliest) {
				earliest = j.next
			}
		}
		wait := earliest.Sub(r.clk.Now())
		if wait < 0 {
			wait = 0
		}
		select {
		case <-ctx.Done():
			r.log.Info("fetcher stopped")
			return nil
		case <-r.clk.After(wait):
		}
	}
}

func (r *runner) tickMarkets(ctx context.Context, now time.Time) {
	if !r.allowed(now) {
		r.skip(event.EndpointMarkets, 1, "cooldown")
		return
	}
	r.call(ctx, event.EndpointMarkets, coingecko.MarketsRequest(r.cfg.MarketsPerPage))
}

// tickDetail fetches the next coin of the watchlist (round-robin). A
// skipped tick does not advance the rotation.
func (r *runner) tickDetail(ctx context.Context, now time.Time) {
	if !r.allowed(now) {
		r.skip(event.EndpointCoinDetail, 1, "cooldown")
		return
	}
	coin := r.cfg.Watchlist[r.detailIdx%len(r.cfg.Watchlist)]
	r.detailIdx++
	r.call(ctx, event.EndpointCoinDetail, coingecko.CoinDetailRequest(coin))
}

func (r *runner) tickPartitions(ctx context.Context, now time.Time) {
	if err := r.ensurePartitions(ctx, now); err != nil && ctx.Err() == nil {
		r.log.Error("ensure partitions failed", "err", err)
	}
}

func (r *runner) skip(endpoint string, n int, reason string) {
	r.m.APIRequests.WithLabelValues(endpoint, statusSkipped).Add(float64(n))
	r.log.Debug("tick skipped", "endpoint", endpoint, "reason", reason, "ticks", n)
}

// call performs one fetch, records metrics and logs, validates the envelope
// against S1 and inserts the row.
func (r *runner) call(ctx context.Context, endpoint string, req event.Request) {
	ev := r.fetch(ctx, endpoint, req)
	if ctx.Err() != nil {
		return // shutting down: the request was cut short by us, not by CoinGecko
	}

	status := ev.Outcome
	attrs := []any{"event_id", ev.EventID, "endpoint", endpoint, "path", req.Path, "outcome", ev.Outcome}
	if ev.HTTP != nil {
		status = strconv.Itoa(ev.HTTP.Status)
		r.m.APIRequestDuration.WithLabelValues(endpoint).Observe(float64(ev.HTTP.LatencyMS) / 1000)
		attrs = append(attrs, "status", ev.HTTP.Status, "latency_ms", ev.HTTP.LatencyMS)
		if ev.HTTP.CFCacheStatus != nil {
			attrs = append(attrs, "cf_cache_status", *ev.HTTP.CFCacheStatus)
		}
		if ev.HTTP.Status == 429 {
			r.m.APIRateLimited.Inc()
			if ev.HTTP.RetryAfterS != nil {
				attrs = append(attrs, "retry_after_s", *ev.HTTP.RetryAfterS)
			}
		}
	}
	if ev.Error != nil {
		attrs = append(attrs, "error", *ev.Error)
	}
	r.m.APIRequests.WithLabelValues(endpoint, status).Inc()
	r.log.Info("api call", attrs...)

	if !r.validEnvelope(ev) {
		return
	}
	if err := r.insert(ctx, ev); err != nil {
		r.log.Error("insert api call failed", "event_id", ev.EventID, "endpoint", endpoint, "err", err)
		return
	}
	r.log.Debug("api call stored", "event_id", ev.EventID, "endpoint", endpoint)
	if ev.Outcome != event.OutcomeOK {
		return
	}
	fetchedAt, err := xtime.Parse(ev.FetchedAt)
	if err != nil {
		return
	}
	r.m.LastSuccess.WithLabelValues(endpoint).Set(float64(fetchedAt.UnixMilli()) / 1000)
	if updated, ok := sourceLastUpdated(endpoint, ev.Payload); ok {
		r.m.SourceDataAge.WithLabelValues(endpoint).Set(fetchedAt.Sub(updated).Seconds())
	}
}

// validEnvelope checks the event against S1. A violation is a bug in our
// code (the fetcher builds the envelope), so it is logged at ERROR and the
// row is not inserted.
func (r *runner) validEnvelope(ev event.RawEvent) bool {
	b, err := event.Marshal(ev)
	var vs []validation.Violation
	if err == nil {
		vs, err = r.reg.Validate(validation.IDRawEvent, b)
	}
	if err != nil || len(vs) > 0 {
		r.m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
		for _, v := range vs {
			r.m.SchemaViolations.WithLabelValues(stage, schemaLabel, validation.Keyword(v)).Inc()
		}
		r.log.Error("raw event violates S1; not inserted (bug)",
			"event_id", ev.EventID, "endpoint", ev.Endpoint, "err", err, "violations", vs)
		return false
	}
	r.m.SchemaValidations.WithLabelValues(stage, schemaLabel, "pass").Inc()
	return true
}

// sourceLastUpdated returns when CoinGecko computed the data: the max
// last_updated over a markets array, or the top-level last_updated of a
// /coins/{id} document. Only that one field is decoded.
func sourceLastUpdated(endpoint string, payload []byte) (time.Time, bool) {
	var stamps []*string
	switch endpoint {
	case event.EndpointMarkets:
		var rows []struct {
			LastUpdated *string `json:"last_updated"`
		}
		if json.Unmarshal(payload, &rows) != nil {
			return time.Time{}, false
		}
		for _, row := range rows {
			stamps = append(stamps, row.LastUpdated)
		}
	case event.EndpointCoinDetail:
		var doc struct {
			LastUpdated *string `json:"last_updated"`
		}
		if json.Unmarshal(payload, &doc) != nil {
			return time.Time{}, false
		}
		stamps = append(stamps, doc.LastUpdated)
	}
	var best time.Time
	for _, s := range stamps {
		if s == nil {
			continue
		}
		if t, err := xtime.Parse(*s); err == nil && t.After(best) {
			best = t
		}
	}
	return best, !best.IsZero()
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `go test -race ./internal/fetcher/ -run 'TestSchedule|TestPartitions|TestSkips|TestInvalid|TestRunRejects|TestSourceLastUpdated' -v`
Expected: PASS for `TestScheduleAndRotation`, `TestPartitionsHourly`, `TestSkipsTicksDuringCooldown`, `TestSkipsTicksThatCameDueWhileInFlight`, `TestInvalidEnvelopeIsNotInserted`, `TestRunRejectsZeroIntervals`, `TestSourceLastUpdated`.

- [ ] **Step 5: Write the integration test**

This test wires the real `coingecko.Client` and `Limiter`, real Postgres (`testutil.StartPostgres`, migrated with `pg.Migrate`), and a fake CoinGecko (`httptest`) that serves the recorded fixtures. The first markets call returns 429 with `Retry-After: 1`. The intervals are shortened to hundreds of milliseconds. The test polls the table until rows exist for both endpoints, then cancels and checks:
- exactly one `http_error` 429 row, with no payload and no key;
- the stored `json` payload hashes to `payload_sha256` (byte-exact storage);
- identical markets bodies share one idempotency key;
- the cooldown skipped at least one tick;
- `etl_api_rate_limited_total` is 1.

It is skipped when Docker is unavailable.

Create `internal/fetcher/fetcher_integration_test.go`:

```go
package fetcher

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeCoinGecko serves recorded responses. The first /coins/markets call
// gets a 429 with Retry-After: 1, like the real API does under load.
func fakeCoinGecko(t *testing.T) *httptest.Server {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile("../../schemas/testdata/coingecko/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	markets := read("markets_top10_t0.json")
	bitcoin := read("coin_bitcoin_full_t0.json")
	usdc := read("coin_usd-coin_meta.json")
	var marketsCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/coins/markets", func(w http.ResponseWriter, r *http.Request) {
		if marketsCalls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"status":{"error_code":429,"error_message":"You've exceeded the Rate Limit."}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cf-Cache-Status", "HIT")
		w.Header().Set("Age", "12")
		_, _ = w.Write(markets)
	})
	mux.HandleFunc("GET /api/v3/coins/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch r.PathValue("id") {
		case "bitcoin":
			_, _ = w.Write(bitcoin)
		case "usd-coin":
			_, _ = w.Write(usdc)
		default:
			http.Error(w, `{"error":"coin not found"}`, http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestFetcherIntegration(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	dsn := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	srv := fakeCoinGecko(t)
	cfg := config.Config{
		MarketsInterval:   300 * time.Millisecond,
		DetailInterval:    400 * time.Millisecond,
		DetailOffset:      150 * time.Millisecond,
		Watchlist:         []string{"bitcoin", "usd-coin"},
		MarketsPerPage:    10,
		MinRequestSpacing: 50 * time.Millisecond,
	}
	lim := coingecko.NewLimiter(cfg.MinRequestSpacing)
	cg := coingecko.NewClient(srv.URL+"/api/v3", "", srv.Client(), lim, time.Now)
	m := obs.NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- Run(runCtx, cfg, cg, lim, pool, m, log) }()

	count := func(where string) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM raw.api_calls WHERE "+where).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if count("endpoint = 'coins_markets' AND outcome = 'ok'") >= 2 &&
			count("request->>'path' = '/coins/bitcoin' AND outcome = 'ok'") >= 1 &&
			count("request->>'path' = '/coins/usd-coin' AND outcome = 'ok'") >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}

	if n := count("endpoint = 'coins_markets' AND outcome = 'ok'"); n < 2 {
		t.Errorf("markets ok rows = %d, want >= 2", n)
	}
	if n := count("endpoint = 'coin_detail' AND outcome = 'ok' AND request->>'path' IN ('/coins/bitcoin','/coins/usd-coin')"); n < 2 {
		t.Errorf("detail ok rows = %d, want >= 2 (bitcoin and usd-coin)", n)
	}
	if n := count("outcome = 'http_error' AND (http->>'status')::int = 429 AND payload IS NULL AND idempotency_key IS NULL AND (http->>'retry_after_s')::int = 1"); n != 1 {
		t.Errorf("429 http_error rows = %d, want 1", n)
	}
	// Byte-exact payload: the hash of the stored json text equals payload_sha256.
	if n := count("outcome = 'ok' AND sha256(convert_to(payload::text, 'UTF8')) <> payload_sha256"); n != 0 {
		t.Errorf("%d rows whose payload bytes do not match payload_sha256", n)
	}
	// Identical markets responses share one idempotency key (raw keeps duplicates).
	if n := count("endpoint = 'coins_markets' AND outcome = 'ok' AND idempotency_key IS DISTINCT FROM (SELECT idempotency_key FROM raw.api_calls WHERE endpoint = 'coins_markets' AND outcome = 'ok' LIMIT 1)"); n != 0 {
		t.Errorf("identical markets bodies produced %d different idempotency keys", n)
	}
	if got := promtest.ToFloat64(m.APIRateLimited); got != 1 {
		t.Errorf("api_rate_limited_total = %v, want 1", got)
	}
	// The 1 s cooldown after the 429 must have skipped at least one tick.
	if got := promtest.ToFloat64(m.APIRequests.WithLabelValues("coins_markets", "skipped")) +
		promtest.ToFloat64(m.APIRequests.WithLabelValues("coin_detail", "skipped")); got < 1 {
		t.Errorf("skipped ticks = %v, want >= 1 during the cooldown", got)
	}
	if got := promtest.ToFloat64(m.SchemaValidations.WithLabelValues("fetch", "raw_event.v1", "fail")); got != 0 {
		t.Errorf("schema_validations_total{invalid} = %v", got)
	}
}
```

- [ ] **Step 6: Run the integration test**

Run: `go test -race -count=1 ./internal/fetcher/ -run TestFetcherIntegration -v`
Expected: PASS (about 10 s including container start; `SKIP: docker not available` without Docker). Step 3 already implemented the wiring, so this test passes straight away. It guards `Run` itself: partitions at start, real client and limiter, and INSERT.

Then run the whole package and vet: `go test -race -count=1 ./internal/fetcher/ ./internal/coingecko/ && go vet ./...`
Expected: `ok` for both packages; vet prints nothing.

- [ ] **Step 7: Commit**

```bash
git add internal/fetcher/ go.mod go.sum
git commit -m "feat(fetcher): scheduled CoinGecko polling with tick skipping, S1 check and raw insert

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

CONTRACT NOTES

- No contract names or signatures changed. Clarifications chosen within the contract:
  - **DLQ record shape.** The exporter writes DLQ lines matching `schemas/pipeline/dlq_record.v1.json` from Task 3 (`schema_version`, `stage`, `error_class`, `violations`, `origin`, `original_base64`, `occurred_at`), with `origin {kind: "postgres", batch_id, seq, event_id}`. Task 3's schema lists `seq` in `origin.properties` (fixture `dlq_record.v1/valid/export_with_seq.json`). If the exporter's own DLQ record ever fails S3, it logs ERROR with `original_base64` and still writes the object.
  - **Export DLQ object key.** The skeleton's DLQ pattern `<topic or "export"/"publish">+<partition or batch>+<%020d offset or batch_id>` is read as `stage=export/date=<claim date>/export+batch+<%020d batch_id>.jsonl`, with one object per batch for all of its invalid lines. The publisher (Task 11) uses `stage=publish/date=<date>/publish+batch+<%020d batch_id>.jsonl` to match.
  - **Metric label values.** `etl_schema_validations_total{stage="export",schema="raw_event.v1",result="pass|fail"}` (the pass/fail convention from Task 1; every stage uses `pass|fail` and the schema file name without `.json` as the `schema` label). `etl_export_batches_total{result="exported|error"}`. `etl_s3_put_total{result="created|exists|mismatch|error"}` is incremented by the exporter, because `s3.Client` has no metrics dependency in the contract. `etl_dlq_total{stage="export",error_class="schema_violation|decode_error"}` is counted only when the DLQ object is newly created, so a resumed batch does not count twice.
  - **`ExportOnce` return value** = the sum of `row_count` over the batches it completed in that call: resumed `claimed` batches plus at most one new batch. Rows sent to the DLQ count as exported, since they leave the cursor range.
  - `s3.Client.Get` wraps the SDK error. A missing key returns a non-nil error (`NoSuchKey` inside), and no sentinel is exported.
  - `PutIfAbsent` sends the full body with `ContentLength`. An empty body (a batch whose every row went to the DLQ) is a valid zero-byte object.
  - Integration tests skip under `go test -short` (Task 1 convention) as well as through `testutil.SkipIfNoDocker`.


### Task 8: S3 client (internal/storage/s3)

**Files:**
- Create: `internal/storage/s3/s3.go`
- Test: `internal/storage/s3/s3_test.go` (unit, no Docker)
- Test: `internal/storage/s3/s3_integration_test.go` (versitygw via `testutil.StartVersityGW`)
- Modify: `go.mod`, `go.sum` (AWS SDK modules)

**Interfaces:**
- Consumes:
```go
// internal/event
func SHA256Hex(b []byte) string
// internal/testutil (tests only)
func StartVersityGW(t *testing.T) (endpoint, dataDir string)         // versitygw v1.8.0, posix + sidecar, creds etl/etlsecret123
func SkipIfNoDocker(t *testing.T)
```
- Produces:
```go
// internal/storage/s3
const (BucketRaw = "raw"; BucketProcessed = "processed"; BucketRejected = "rejected"; BucketDLQ = "dlq")
var ErrChecksumMismatch = errors.New("s3: object exists with different checksum")
type Config struct { Endpoint, AccessKey, SecretKey, Region string }
type Client struct { /* unexported */ }
func New(ctx context.Context, cfg Config) (*Client, error)               // path-style, BaseEndpoint
func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (created bool, err error)
    // If-None-Match:*, metadata sha256=hex; on 412 HEAD and compare → (false,nil) or ErrChecksumMismatch
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error)
func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error
func (c *Client) Ping(ctx context.Context) error
```

Background for the engineer:
- **versitygw** (Versity S3 Gateway, posix backend) serves a directory over the S3 API. Each top-level directory is a bucket, and an object `a/b/c` in bucket `raw` is the plain file `<root>/raw/a/b/c`. That is how `./data/raw` stays `cat`-able. It has no virtual-host bucket DNS, so the client must use **path-style** URLs (`http://host:7070/raw/key`): `o.UsePathStyle = true`. `o.BaseEndpoint` points the SDK at it instead of AWS.
- **aws-sdk-go-v2 setup.** `config.LoadDefaultConfig` with `config.WithRegion` and `config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, ""))`, then `s3.NewFromConfig(cfg, func(o *s3.Options){...})`. Keep the SDK's default request checksums (CRC32). versitygw accepts them.
- **Seekable bodies only.** Over plain `http://`, the SDK must seek the body to compute the payload checksum. An unseekable `io.Reader` fails with `unseekable stream is not supported without TLS and trailing checksum` (reproduced in the spike). We always pass `bytes.NewReader(body)`.
- **Write-once.** `IfNoneMatch: aws.String("*")` makes the PUT fail with **HTTP 412 `PreconditionFailed`** if the key exists (versitygw honours it atomically: 20 parallel PUTs → one 200, nineteen 412). A 412 by itself does not say whether the existing bytes are ours. We store `x-amz-meta-sha256` (SDK field `Metadata: {"sha256": hex}`) on every PUT. After a 412 we `HeadObject` and compare. Equal → an idempotent retry `(false, nil)`. Different or missing → `ErrChecksumMismatch`. User-metadata keys may come back in any case, so the lookup is case-insensitive.
- **Error inspection.** Service errors implement `smithy.APIError` (`ErrorCode()`). Transport errors wrap `*awshttp.ResponseError` (`HTTPStatusCode()`). We check both: code `PreconditionFailed` or status 412. `CreateBucket` on an existing bucket returns `BucketAlreadyOwnedByYou` (or `BucketAlreadyExists`). These are typed errors in `service/s3/types`, and we also match the code string.
- Gotcha (documented, not handled): versitygw lets `If-None-Match:*` overwrite a file that was placed on the host directly, outside S3, because such files have no stored ETag. Nothing in the pipeline writes into `./data` except through S3.

- [ ] **Step 1: Add the AWS SDK modules**

```bash
go get github.com/aws/aws-sdk-go-v2@v1.47.3 \
  github.com/aws/aws-sdk-go-v2/config@v1.33.9 \
  github.com/aws/aws-sdk-go-v2/credentials@v1.20.9 \
  github.com/aws/aws-sdk-go-v2/service/s3@v1.114.3 \
  github.com/aws/smithy-go@v1.28.5
```

Expected: `go.mod` lists these five as requirements (they become direct after Step 4 and `go mod tidy`).

- [ ] **Step 2: Write the failing unit test** — `internal/storage/s3/s3_test.go`

```go
package s3

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsPreconditionFailed(t *testing.T) {
	byCode := fmt.Errorf("wrapped: %w", &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"})
	byStatus := &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 412}},
		Err:      errors.New("boom"),
	}}
	other := &smithy.GenericAPIError{Code: "AccessDenied"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"error code", byCode, true},
		{"http status", byStatus, true},
		{"other api error", other, false},
		{"plain error", errors.New("dial tcp: refused"), false},
	}
	for _, c := range cases {
		if got := isPreconditionFailed(c.err); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestIsBucketExists(t *testing.T) {
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou"}) {
		t.Error("BucketAlreadyOwnedByYou not recognised")
	}
	if !isBucketExists(&smithy.GenericAPIError{Code: "BucketAlreadyExists"}) {
		t.Error("BucketAlreadyExists not recognised")
	}
	if isBucketExists(&smithy.GenericAPIError{Code: "AccessDenied"}) {
		t.Error("AccessDenied treated as exists")
	}
}

func TestMetaValueCaseInsensitive(t *testing.T) {
	for _, k := range []string{"sha256", "Sha256", "SHA256"} {
		v, ok := metaValue(map[string]string{k: "abc"}, "sha256")
		if !ok || v != "abc" {
			t.Errorf("key %q: got %q %v", k, v, ok)
		}
	}
	if _, ok := metaValue(map[string]string{"other": "x"}, "sha256"); ok {
		t.Error("missing key reported present")
	}
}
```

- [ ] **Step 3: Write the failing integration test** — `internal/storage/s3/s3_integration_test.go`

The key contains `=` on purpose (Hive-style `source=coingecko/ingest_date=…`). The SDK signs it correctly. Some other clients, such as curl `--aws-sigv4`, do not.

```go
package s3_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
)

func newClient(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint, dataDir := testutil.StartVersityGW(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets: %v", err)
	}
	// Second call must be a no-op, not an error.
	if err := c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets again: %v", err)
	}
	return c, dataDir
}

func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	key := "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl"
	body := []byte("{\"a\":1}\n{\"b\":2}\n")

	created, err := c.PutIfAbsent(ctx, s3.BucketRaw, key, body)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}

	created, err = c.PutIfAbsent(ctx, s3.BucketRaw, key, append([]byte(nil), body...))
	if err != nil || created {
		t.Fatalf("same-content put: want (false,nil), got created=%v err=%v", created, err)
	}

	created, err = c.PutIfAbsent(ctx, s3.BucketRaw, key, []byte("{\"other\":true}\n"))
	if !errors.Is(err, s3.ErrChecksumMismatch) || created {
		t.Fatalf("different-content put: want ErrChecksumMismatch, got created=%v err=%v", created, err)
	}

	// The object is a plain file in the bucket directory, byte-identical,
	// and the different-content put did not overwrite it.
	onDisk, err := os.ReadFile(filepath.Join(dataDir, s3.BucketRaw, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("read plain file: %v", err)
	}
	if !bytes.Equal(onDisk, body) {
		t.Fatalf("plain file = %q, want %q", onDisk, body)
	}

	got, err := c.Get(ctx, s3.BucketRaw, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}

	if _, err := c.Get(ctx, s3.BucketRaw, "source=coingecko/missing.jsonl"); err == nil {
		t.Fatal("Get of a missing key returned no error")
	}
}

// A bucket directory created on the host (mkdir data/raw before the first
// start, or after .s3meta was wiped) must not make EnsureBuckets fail:
// versitygw answers CreateBucket on it with 500, HeadBucket with 200.
func TestEnsureBucketsAcceptsHostCreatedDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := os.MkdirAll(filepath.Join(dataDir, "handmade"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureBuckets(ctx, "handmade"); err != nil {
		t.Fatalf("EnsureBuckets on a host-created directory: %v", err)
	}
	if created, err := c.PutIfAbsent(ctx, "handmade", "k.jsonl", []byte("{}\n")); err != nil || !created {
		t.Fatalf("put into host-created bucket: created=%v err=%v", created, err)
	}
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `go test ./internal/storage/s3/`
Expected: build failure, e.g.
```
internal/storage/s3/s3_test.go:32:13: undefined: isPreconditionFailed
internal/storage/s3/s3_test.go:39:6: undefined: isBucketExists
```

- [ ] **Step 5: Write the implementation** — `internal/storage/s3/s3.go`

```go
// Package s3 is the only way the pipeline touches object storage. It wraps
// aws-sdk-go-v2 for an S3-compatible endpoint (versitygw locally) with
// write-once semantics: PutIfAbsent never overwrites an existing object.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"coingecko-etl/internal/event"
)

const (
	BucketRaw       = "raw"
	BucketProcessed = "processed"
	BucketRejected  = "rejected"
	BucketDLQ       = "dlq"
)

// ErrChecksumMismatch means the key already holds different bytes. Callers
// treat it as fatal for the batch (non-determinism or a foreign writer).
var ErrChecksumMismatch = errors.New("s3: object exists with different checksum")

const (
	metaSHA256  = "sha256" // sent as x-amz-meta-sha256
	contentType = "application/x-ndjson"
)

type Config struct{ Endpoint, AccessKey, SecretKey, Region string }

type Client struct{ api *s3.Client }

// New builds a path-style client (http://host:port/bucket/key). Path style is
// required because versitygw has no virtual-host bucket DNS.
func New(ctx context.Context, cfg Config) (*Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("s3: load config: %w", err)
	}
	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})
	return &Client{api: api}, nil
}

// PutIfAbsent writes body under bucket/key only if the key does not exist.
// created=true: this call wrote it. created=false, err=nil: the object was
// already there with the same sha256 (an idempotent retry). ErrChecksumMismatch:
// the object exists with other content.
func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (bool, error) {
	sum := event.SHA256Hex(body)
	_, err := c.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		// bytes.Reader is seekable: the SDK must rewind the body to compute the
		// payload checksum; an unseekable io.Reader fails over plain http.
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   aws.String(contentType),
		IfNoneMatch:   aws.String("*"),
		Metadata:      map[string]string{metaSHA256: sum},
	})
	if err == nil {
		return true, nil
	}
	if !isPreconditionFailed(err) {
		return false, fmt.Errorf("s3: put %s/%s: %w", bucket, key, err)
	}
	head, herr := c.api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if herr != nil {
		return false, fmt.Errorf("s3: head %s/%s after 412: %w", bucket, key, herr)
	}
	if got, ok := metaValue(head.Metadata, metaSHA256); ok && strings.EqualFold(got, sum) {
		return false, nil
	}
	return false, fmt.Errorf("%w: %s/%s", ErrChecksumMismatch, bucket, key)
}

// Get returns the whole object body.
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("s3: get %s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3: read %s/%s: %w", bucket, key, err)
	}
	return b, nil
}

// EnsureBuckets creates each bucket; an existing bucket is not an error.
// HeadBucket goes first: versitygw v1.8.0 answers 500 InternalError to
// CreateBucket when the bucket directory already exists on disk but was not
// created through the gateway (mkdir data/raw, or a deleted .s3meta), while
// HeadBucket on it returns 200 and PUT/GET work.
func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error {
	for _, b := range buckets {
		if _, err := c.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(b)}); err == nil {
			continue
		}
		_, err := c.api.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)})
		if err == nil || isBucketExists(err) {
			continue
		}
		return fmt.Errorf("s3: create bucket %s: %w", b, err)
	}
	return nil
}

// Ping is the readiness check: a signed ListBuckets proves endpoint and credentials.
func (c *Client) Ping(ctx context.Context) error {
	if _, err := c.api.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		return fmt.Errorf("s3: ping: %w", err)
	}
	return nil
}

// isPreconditionFailed reports whether err is the 412 answer to If-None-Match:*.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusPreconditionFailed
}

func isBucketExists(err error) bool {
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists")
}

// metaValue looks up user metadata case-insensitively: servers and the SDK
// may return "Sha256" or "sha256".
func metaValue(m map[string]string, key string) (string, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}
```

Then run `go mod tidy`.

- [ ] **Step 6: Run the unit tests**

Run: `go test -short ./internal/storage/s3/ -v`
Expected: `TestIsPreconditionFailed`, `TestIsBucketExists`, `TestMetaValueCaseInsensitive` PASS; `TestClientIntegration` and `TestEnsureBucketsAcceptsHostCreatedDirectory` SKIP.

- [ ] **Step 7: Run the integration test (Docker required)**

Run: `go test ./internal/storage/s3/ -run 'TestClientIntegration|TestEnsureBucketsAcceptsHostCreatedDirectory' -count=1 -v`
Expected: `--- PASS: TestClientIntegration`. The test checks five things: the first PUT creates the object; the same bytes again → `(false, nil)`; different bytes → `ErrChecksumMismatch` with the file left untouched; the file exists at `<dataDir>/raw/source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl` with identical bytes; `Get` round-trips. `--- PASS: TestEnsureBucketsAcceptsHostCreatedDirectory` shows that a bucket directory created on the host is accepted (HeadBucket first). Then run `go vet ./internal/storage/s3/` (no output).

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/storage/s3/
git commit -m "feat(s3): write-once S3 client with sha256 verification on 412

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```


### Task 9: Exporter (internal/exporter)

**Files:**
- Create: `internal/exporter/exporter.go`
- Test: `internal/exporter/exporter_test.go` (unit, no Docker)
- Test: `internal/exporter/exporter_integration_test.go` (Postgres + versitygw)

**Interfaces:**
- Consumes:
```go
// internal/config
type Config struct { /* ... */ ExportInterval time.Duration; ExportBatchMax int /* ... */ }
// internal/xtime
func Format(t time.Time) string
func FormatDate(t time.Time) string
// internal/event
const (SchemaVersion = 1; SourceCoinGecko = "coingecko"; EndpointMarkets = "coins_markets"; OutcomeOK = "ok")
type Request struct { Path string `json:"path"`; Params map[string]string `json:"params"` }
type HTTPMeta struct { Status int `json:"status"`; LatencyMS int64 `json:"latency_ms"`; Date *string `json:"date"`; AgeSeconds *int `json:"age_s"`; CFCacheStatus *string `json:"cf_cache_status"`; ETag *string `json:"etag"`; RetryAfterS *int `json:"retry_after_s"` }
type RawEvent struct { /* S1 envelope, see contract */ }
func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string
func SHA256Hex(b []byte) string
func Marshal(ev RawEvent) ([]byte, error)
func Unmarshal(b []byte) (RawEvent, error)
// internal/validation
type Violation struct { InstanceLocation, KeywordLocation, Message string }
func Load(fsys fs.FS) (*Registry, error)
func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)
func Keyword(v Violation) string
const IDRawEvent, IDDLQRecord
// package schemas: var FS embed.FS
// internal/obs
type Metrics struct { SchemaValidations, SchemaViolations, ExportBatches, S3Puts, DLQ *prometheus.CounterVec; ExportLag prometheus.Gauge; /* ... */ }
func NewMetrics(reg prometheus.Registerer) *Metrics
// internal/storage/pg (tests)
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)
func Migrate(ctx context.Context, pool *pgxpool.Pool) error
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error
// internal/storage/s3 (Task 8)
const BucketRaw, BucketDLQ
var ErrChecksumMismatch
func New(ctx context.Context, cfg Config) (*Client, error)
func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (created bool, err error)
func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error)
func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error
// internal/testutil (tests)
func StartPostgres(t *testing.T) (dsn string)
func StartVersityGW(t *testing.T) (endpoint, dataDir string)
func SkipIfNoDocker(t *testing.T)
// fixture: schemas/testdata/coingecko/markets_top10_t0.json (copied by Task 4)
```
- Produces:
```go
func exporter.ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, max int) (exported int, err error)
func exporter.Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
```

Background for the engineer (spec §4.2, §7.7):
- **Why not `WHERE seq > last_seq`?** `seq` is assigned at INSERT, but rows become visible at COMMIT. Transaction A takes seq 10, B takes seq 11, B commits, and the exporter exports 11 and remembers 11. A commits later, and row 10 is behind the cursor forever. This was reproduced on Postgres 18.6.
- **The xid8 cursor.** Every row stores `ingest_txid xid8 DEFAULT pg_current_xact_id()`, the id of its inserting transaction. `pg_snapshot_xmin(pg_current_snapshot())` is the oldest transaction id still running in the cluster. Every id below it is finished (committed or rolled back). The exporter selects only rows with `ingest_txid < xmin`, ordered by `(ingest_txid, seq)` and strictly after the last exported `(to_txid, to_seq)`. A row can never commit later with a position below the cursor. A long-running transaction makes the exporter **stall** (no rows past it), never **skip**. The `etl_export_lag_seconds` alert catches the stall. Postgres has `transaction_timeout = 5min` and `idle_in_transaction_session_timeout = 60s` (Task 5 migration).
- **xid8 in Go.** We never scan xid8 into Go types. SQL renders it as text (`ingest_txid::text`) and parses it back with `$1::text::xid8`. Row comparison `(ingest_txid, seq) > (x, s)` works because xid8 has btree operators.
- **Claim ledger.** Step 1 is a short transaction that inserts the range into `pipeline.export_batches` and commits *before* any S3 call. The object key comes from the batch id, which is taken with `nextval(pg_get_serial_sequence('pipeline.export_batches','batch_id'))` (works for identity columns), so the key is known in the same INSERT. A crash after the claim leaves a `claimed` row. The next `ExportOnce` rebuilds exactly that range, which yields the same bytes, and `PutIfAbsent` returns 200 or 412-with-equal-checksum. `content_sha256` is stored before the PUT and compared on every retry, which catches non-determinism.
- **Advisory lock.** `pg_try_advisory_lock(hashtext('etl.exporter'))` is a *session* lock. It belongs to one connection, so `Run` acquires a dedicated `*pgxpool.Conn` and keeps it until exit. It pings that connection each cycle: if the connection dies the lock is gone, and `Run` returns an error so the process restarts and re-acquires it. A session lock held outside any transaction is not affected by `idle_in_transaction_session_timeout`.
- **Determinism.** No `time.Now()` in the bytes. `fetched_at` comes from the row. DLQ `occurred_at` is the batch `claimed_at`. Violations are sorted, because the validator may report them in map order.

- [ ] **Step 1: Write the failing unit tests** — `internal/exporter/exporter_test.go`

```go
package exporter

import (
	"bytes"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
)

func TestObjectKeys(t *testing.T) {
	at := time.Date(2026, 10, 10, 23, 59, 59, 0, time.FixedZone("x", 3*3600)) // 20:59:59Z
	if got, want := rawObjectKey(42, at), "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl"; got != want {
		t.Errorf("rawObjectKey = %q, want %q", got, want)
	}
	if got, want := dlqObjectKey(42, at), "stage=export/date=2026-10-10/export+batch+00000000000000000042.jsonl"; got != want {
		t.Errorf("dlqObjectKey = %q, want %q", got, want)
	}
}

func TestJoinLines(t *testing.T) {
	if got := joinLines(nil); len(got) != 0 {
		t.Errorf("joinLines(nil) = %q, want empty", got)
	}
	got := joinLines([][]byte{[]byte(`{"a":1}`), []byte(`{"b":2}`)})
	if want := []byte("{\"a\":1}\n{\"b\":2}\n"); !bytes.Equal(got, want) {
		t.Errorf("joinLines = %q, want %q", got, want)
	}
}

func TestRowToEventKeepsPayloadVerbatim(t *testing.T) {
	payload := `[{"id":"bitcoin","current_price":61000.10,"market_cap":1234567890123456789}]`
	sha := event.SHA256Hex([]byte(payload))
	idem := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointMarkets, map[string]string{"page": "1"}, sha)
	httpJSON := `{"status": 200, "latency_ms": 85, "date": null, "age_s": 12, "cf_cache_status": "HIT", "etag": null, "retry_after_s": null}`
	r := apiRow{
		Seq: 7, EventID: "0199c8a0-0000-7000-8000-000000000001", IdempotencyKey: &idem, SchemaVersion: 1,
		Source: event.SourceCoinGecko, Endpoint: event.EndpointMarkets,
		Request:   `{"path": "/coins/markets", "params": {"page": "1"}}`,
		FetchedAt: time.Date(2026, 10, 10, 10, 7, 55, 123456000, time.UTC),
		Outcome:   event.OutcomeOK, HTTP: &httpJSON, Payload: &payload, PayloadSHA256: &sha,
	}
	ev, err := rowToEvent(r)
	if err != nil {
		t.Fatalf("rowToEvent: %v", err)
	}
	if string(ev.Payload) != payload {
		t.Errorf("payload = %s, want verbatim %s", ev.Payload, payload)
	}
	if ev.FetchedAt != "2026-10-10T10:07:55.123Z" {
		t.Errorf("fetched_at = %q", ev.FetchedAt)
	}
	if ev.Request.Path != "/coins/markets" || ev.Request.Params["page"] != "1" {
		t.Errorf("request = %+v", ev.Request)
	}
	if ev.HTTP == nil || ev.HTTP.Status != 200 || ev.HTTP.AgeSeconds == nil || *ev.HTTP.AgeSeconds != 12 {
		t.Errorf("http = %+v", ev.HTTP)
	}
	if ev.SchemaVersion != 1 || ev.EventID != r.EventID || *ev.PayloadSHA256 != sha || *ev.IdempotencyKey != idem {
		t.Errorf("envelope fields not copied: %+v", ev)
	}
}

func TestSortViolationsDeterministic(t *testing.T) {
	v := []validation.Violation{
		{InstanceLocation: "/source", KeywordLocation: "/properties/source/const", Message: "b"},
		{InstanceLocation: "/endpoint", KeywordLocation: "/properties/endpoint/enum", Message: "a"},
	}
	sortViolations(v)
	if v[0].InstanceLocation != "/endpoint" || v[1].InstanceLocation != "/source" {
		t.Errorf("not sorted: %+v", v)
	}
}
```

- [ ] **Step 2: Write the failing integration tests** — `internal/exporter/exporter_integration_test.go`

The tests are in package `exporter` (internal), so they can call `claimNext` to simulate a crash after the claim and can shorten the lock retry variables. Rows are inserted with plain SQL rather than `pg.InsertAPICall`. That lets the out-of-order test control transactions and the DLQ test store an S1-invalid row: the table has no CHECK on `source`/`endpoint`, while S1 does.

```go
package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

type env struct {
	pool    *pgxpool.Pool
	s3      *s3.Client
	reg     *validation.Registry
	m       *obs.Metrics
	log     *slog.Logger
	dataDir string
	payload []byte
}

func setup(t *testing.T) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dsn := testutil.StartPostgres(t)
	endpoint, dataDir := testutil.StartVersityGW(t)

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pg.Connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("pg.Migrate: %v", err)
	}
	if err := pg.EnsurePartitions(ctx, pool, time.Now(), 3); err != nil {
		t.Fatalf("pg.EnsurePartitions: %v", err)
	}

	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets: %v", err)
	}

	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("validation.Load: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join("..", "..", "schemas", "testdata", "coingecko", "markets_top10_t0.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return &env{
		pool: pool, s3: s3c, reg: reg, dataDir: dataDir, payload: bytes.TrimSpace(payload),
		m:   obs.NewMetrics(prometheus.NewRegistry()),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

const insertSQL = `
INSERT INTO raw.api_calls (event_id, idempotency_key, schema_version, source, endpoint, request,
                           fetched_at, outcome, http, error, payload, payload_sha256)
VALUES ($1, $2, 1, $3, $4, $5::jsonb, $6, 'ok', $7::jsonb, NULL, $8::json, decode($9, 'hex'))
RETURNING seq`

func (e *env) okArgs(source, endpoint string) (eventID string, args []any) {
	eventID = uuid.Must(uuid.NewV7()).String()
	sha := event.SHA256Hex(e.payload)
	params := map[string]string{"vs_currency": "usd", "page": "1"}
	req, _ := json.Marshal(event.Request{Path: "/coins/markets", Params: params})
	httpMeta, _ := json.Marshal(event.HTTPMeta{Status: 200, LatencyMS: 90})
	return eventID, []any{eventID, event.IdempotencyKey(source, endpoint, params, sha), source, endpoint,
		string(req), time.Now().UTC(), string(httpMeta), string(e.payload), sha}
}

func (e *env) insertCommitted(t *testing.T, source, endpoint string) (string, int64) {
	t.Helper()
	id, args := e.okArgs(source, endpoint)
	var seq int64
	if err := e.pool.QueryRow(context.Background(), insertSQL, args...).Scan(&seq); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id, seq
}

func (e *env) insertFailedCall(t *testing.T) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(), `
INSERT INTO raw.api_calls (event_id, schema_version, source, endpoint, request, fetched_at, outcome, http, error)
VALUES ($1, 1, 'coingecko', 'coins_markets', '{"path":"/coins/markets","params":{}}', now(), 'http_error',
        '{"status":429,"latency_ms":40,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":60}', 'HTTP 429')`,
		uuid.Must(uuid.NewV7()).String())
	if err != nil {
		t.Fatalf("insert failed call: %v", err)
	}
}

func (e *env) exportOnce(t *testing.T, max int) int {
	t.Helper()
	n, err := ExportOnce(context.Background(), e.pool, e.s3, e.reg, e.m, e.log, max)
	if err != nil {
		t.Fatalf("ExportOnce: %v", err)
	}
	return n
}

type ledgerRow struct {
	id     int64
	key    string
	status string
	sha    []byte
	rows   int
}

func (e *env) ledger(t *testing.T) []ledgerRow {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `SELECT batch_id, object_key, status, content_sha256, row_count FROM pipeline.export_batches ORDER BY batch_id`)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	defer rows.Close()
	var out []ledgerRow
	for rows.Next() {
		var r ledgerRow
		if err := rows.Scan(&r.id, &r.key, &r.status, &r.sha, &r.rows); err != nil {
			t.Fatalf("ledger scan: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// exportedEventIDs reads every raw object named in the ledger and returns the
// event ids of all lines in order, checking object bytes and checksums.
func (e *env) exportedEventIDs(t *testing.T) []string {
	t.Helper()
	keyRe := regexp.MustCompile(`^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/batch=\d{20}\.jsonl$`)
	var ids []string
	for _, l := range e.ledger(t) {
		if l.status != "exported" {
			t.Fatalf("batch %d status %q, want exported", l.id, l.status)
		}
		if !keyRe.MatchString(l.key) {
			t.Fatalf("object key %q does not match the raw key layout", l.key)
		}
		body, err := e.s3.Get(context.Background(), s3.BucketRaw, l.key)
		if err != nil {
			t.Fatalf("get %s: %v", l.key, err)
		}
		onDisk, err := os.ReadFile(filepath.Join(e.dataDir, s3.BucketRaw, filepath.FromSlash(l.key)))
		if err != nil || !bytes.Equal(onDisk, body) {
			t.Fatalf("plain file %s differs from S3 object (err=%v)", l.key, err)
		}
		if sum := sha256.Sum256(body); !bytes.Equal(sum[:], l.sha) {
			t.Fatalf("batch %d: content_sha256 does not match object bytes", l.id)
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
			if line == "" {
				continue
			}
			if v, err := e.reg.Validate(validation.IDRawEvent, []byte(line)); err != nil || len(v) > 0 {
				t.Fatalf("exported line fails S1: err=%v violations=%v", err, v)
			}
			ev, err := event.Unmarshal([]byte(line))
			if err != nil {
				t.Fatalf("unmarshal line: %v", err)
			}
			if event.SHA256Hex(ev.Payload) != *ev.PayloadSHA256 {
				t.Fatalf("event %s: payload bytes do not match payload_sha256", ev.EventID)
			}
			ids = append(ids, ev.EventID)
		}
	}
	return ids
}

func TestExportOnceExportsEveryRowExactlyOnce(t *testing.T) {
	e := setup(t)
	want := map[string]bool{}
	for i := 0; i < 3; i++ {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
		want[id] = true
	}
	e.insertFailedCall(t) // outcome http_error: never exported

	if n := e.exportOnce(t, 2); n != 2 {
		t.Fatalf("first ExportOnce = %d, want 2", n)
	}
	if n := e.exportOnce(t, 2); n != 1 {
		t.Fatalf("second ExportOnce = %d, want 1", n)
	}
	if n := e.exportOnce(t, 2); n != 0 {
		t.Fatalf("third ExportOnce = %d, want 0", n)
	}

	ids := e.exportedEventIDs(t)
	if len(ids) != len(want) {
		t.Fatalf("exported %d lines, want %d", len(ids), len(want))
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected or duplicate event %s", id)
		}
		delete(want, id)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("created")); got != 2 {
		t.Fatalf("s3_put_total{created} = %v, want 2", got)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v after full export, want 0", got)
	}
}

func TestExportOnceResumesAfterCrash(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)

	// Crash 1: the claim transaction committed, nothing was written to S3.
	b, ok, err := claimNext(ctx, e.pool, 500)
	if err != nil || !ok || b.RowCount != 2 {
		t.Fatalf("claimNext: ok=%v rows=%d err=%v", ok, b.RowCount, err)
	}
	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce after claim-crash = %d, want 2 (resumed batch)", n)
	}
	l := e.ledger(t)
	if len(l) != 1 || l[0].id != b.ID || l[0].key != b.ObjectKey || l[0].status != "exported" {
		t.Fatalf("ledger after resume = %+v, want the claimed batch %d exported", l, b.ID)
	}
	first, err := e.s3.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Crash 2: the object was written but the batch was not marked exported.
	if _, err := e.pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'claimed', exported_at = NULL WHERE batch_id = $1`, b.ID); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}
	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce after put-crash = %d, want 2", n)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("exists")); got != 1 {
		t.Fatalf("s3_put_total{exists} = %v, want 1 (412 with equal checksum)", got)
	}
	again, err := e.s3.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil || !bytes.Equal(first, again) {
		t.Fatalf("object changed after retry (err=%v)", err)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 2 {
		t.Fatalf("exported %d lines, want 2", len(ids))
	}
}

// TestExportOnceNeverSkipsOutOfOrderCommits is the reason for the xid8 cursor:
// transaction A inserts first but commits last. An identity/seq watermark
// would export B, move past A's seq and lose A forever.
func TestExportOnceNeverSkipsOutOfOrderCommits(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	txA, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer txA.Rollback(ctx)
	idA, argsA := e.okArgs(event.SourceCoinGecko, event.EndpointMarkets)
	var seqA int64
	if err := txA.QueryRow(ctx, insertSQL, argsA...).Scan(&seqA); err != nil {
		t.Fatalf("insert A: %v", err)
	}

	idB, seqB := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets) // tx B commits first
	if seqB <= seqA {
		t.Fatalf("seq B (%d) should be greater than seq A (%d)", seqB, seqA)
	}

	// A is still open: the exporter must stall rather than export B past A.
	if n := e.exportOnce(t, 500); n != 0 {
		t.Fatalf("ExportOnce with A open = %d, want 0 (stall)", n)
	}
	if l := e.ledger(t); len(l) != 0 {
		t.Fatalf("a batch was claimed while A was open: %+v", l)
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce after A committed = %d, want 2", n)
	}
	ids := e.exportedEventIDs(t)
	if len(ids) != 2 || ids[0] != idA || ids[1] != idB {
		t.Fatalf("exported %v, want [A=%s B=%s] in ingest_txid order", ids, idA, idB)
	}
	if n := e.exportOnce(t, 500); n != 0 {
		t.Fatalf("extra ExportOnce = %d, want 0", n)
	}
}

func TestExportOnceSendsInvalidRowsToDLQ(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	goodID, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	// The table has no CHECK on source/endpoint, S1 does: this row is invalid.
	badID, badSeq := e.insertCommitted(t, "not-coingecko", "bogus_endpoint")

	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2", n)
	}
	ids := e.exportedEventIDs(t)
	if len(ids) != 1 || ids[0] != goodID {
		t.Fatalf("raw object holds %v, want only %s", ids, goodID)
	}

	l := e.ledger(t)
	var claimedAt time.Time
	if err := e.pool.QueryRow(ctx, `SELECT claimed_at FROM pipeline.export_batches WHERE batch_id = $1`, l[0].id).Scan(&claimedAt); err != nil {
		t.Fatalf("claimed_at: %v", err)
	}
	key := fmt.Sprintf("stage=export/date=%s/export+batch+%020d.jsonl", xtime.FormatDate(claimedAt), l[0].id)
	body, err := e.s3.Get(ctx, s3.BucketDLQ, key)
	if err != nil {
		t.Fatalf("get dlq object %s: %v", key, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("dlq object has %d lines, want 1", len(lines))
	}
	if v, err := e.reg.Validate(validation.IDDLQRecord, []byte(lines[0])); err != nil || len(v) > 0 {
		t.Fatalf("dlq line fails S3 dlq_record schema: err=%v violations=%v", err, v)
	}
	var rec struct {
		Stage      string                 `json:"stage"`
		ErrorClass string                 `json:"error_class"`
		Violations []validation.Violation `json:"violations"`
		Origin     struct {
			Kind    string `json:"kind"`
			BatchID int64  `json:"batch_id"`
			Seq     int64  `json:"seq"`
			EventID string `json:"event_id"`
		} `json:"origin"`
		OriginalBase64 string `json:"original_base64"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("dlq line: %v", err)
	}
	if rec.Stage != "export" || rec.ErrorClass != "schema_violation" || len(rec.Violations) == 0 ||
		rec.Origin.Kind != "postgres" || rec.Origin.Seq != badSeq || rec.Origin.BatchID != l[0].id || rec.Origin.EventID != badID {
		t.Fatalf("dlq record = %+v, want stage export, schema_violation, origin postgres batch %d seq %d event %s", rec, l[0].id, badSeq, badID)
	}
	original, err := base64.StdEncoding.DecodeString(rec.OriginalBase64)
	if err != nil {
		t.Fatalf("original_base64: %v", err)
	}
	if ev, err := event.Unmarshal(original); err != nil || ev.EventID != badID || ev.Source != "not-coingecko" {
		t.Fatalf("original line does not decode to the bad event: ev=%+v err=%v", ev, err)
	}
	if got := promtest.ToFloat64(e.m.DLQ.WithLabelValues("export", "schema_violation")); got != 1 {
		t.Fatalf("dlq_total = %v, want 1", got)
	}
}

func TestRunExportsAndStopsOnCancel(t *testing.T) {
	e := setup(t)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)

	cfg := config.Config{ExportInterval: 50 * time.Millisecond, ExportBatchMax: 500}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		l := e.ledger(t)
		if len(l) == 1 && l[0].status == "exported" {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("Run did not export within 20s; ledger=%+v", l)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRunFailsWhenLockHeld(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	holder, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock(hashtext('etl.exporter'))`); err != nil {
		t.Fatalf("hold lock: %v", err)
	}
	defer holder.Exec(ctx, `SELECT pg_advisory_unlock(hashtext('etl.exporter'))`)

	oldAttempts, oldDelay := lockAttempts, lockRetryDelay
	lockAttempts, lockRetryDelay = 2, 10*time.Millisecond
	defer func() { lockAttempts, lockRetryDelay = oldAttempts, oldDelay }()

	cfg := config.Config{ExportInterval: time.Second, ExportBatchMax: 500}
	err = Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log)
	if err == nil || !strings.Contains(err.Error(), "advisory lock") {
		t.Fatalf("Run with lock held = %v, want advisory lock error", err)
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/exporter/`
Expected: build failure, e.g.
```
internal/exporter/exporter_integration_test.go:132:12: undefined: ExportOnce
internal/exporter/exporter_integration_test.go:253:16: undefined: claimNext
internal/exporter/exporter_integration_test.go:404:22: undefined: Run
internal/exporter/exporter_integration_test.go:442:27: undefined: lockAttempts
```

- [ ] **Step 4: Write the implementation** — `internal/exporter/exporter.go`

```go
// Package exporter copies committed raw.api_calls rows (outcome ok) into
// write-once JSONL objects in the raw bucket.
//
// Correctness rests on three pieces:
//   - an (ingest_txid, seq) cursor bounded by pg_snapshot_xmin(pg_current_snapshot()):
//     every transaction id below the snapshot xmin has finished, so no row can
//     later appear behind the cursor (an identity watermark would skip rows
//     whose transactions commit out of order);
//   - the pipeline.export_batches ledger: the row range is claimed and committed
//     before any S3 I/O, so a retry after a crash rebuilds exactly the same bytes;
//   - PutIfAbsent (If-None-Match: *): a repeated PUT gets 412 and is accepted
//     when the stored sha256 matches.
package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
)

const (
	stage      = "export"
	schemaName = "raw_event.v1"
	lockName   = "etl.exporter"
)

// Lock acquisition retries; variables so tests can shorten them.
var (
	lockAttempts   = 12
	lockRetryDelay = 5 * time.Second
)

// batch is one row of pipeline.export_batches. Transaction ids travel as
// decimal strings and are cast in SQL with $n::text::xid8, so no pgx xid8
// codec is needed.
type batch struct {
	ID        int64
	FromTxid  string
	FromSeq   int64
	ToTxid    string
	ToSeq     int64
	RowCount  int
	ObjectKey string
	SHA256    []byte // nil until the content was built once
	ClaimedAt time.Time
}

// apiRow is one raw.api_calls row as text/scalar columns.
type apiRow struct {
	Seq            int64
	EventID        string
	IdempotencyKey *string
	SchemaVersion  int16
	Source         string
	Endpoint       string
	Request        string // jsonb rendered as text
	FetchedAt      time.Time
	Outcome        string
	HTTP           *string // jsonb rendered as text
	Error          *string
	Payload        *string // json column: the original bytes
	PayloadSHA256  *string // hex
}

// dlqRecord is one line of a dlq/ object (schema S3 pipeline/dlq_record.v1.json).
type dlqRecord struct {
	SchemaVersion  int                    `json:"schema_version"`
	Stage          string                 `json:"stage"`
	ErrorClass     string                 `json:"error_class"`
	Violations     []validation.Violation `json:"violations"`
	Origin         dlqOrigin              `json:"origin"`
	OriginalBase64 string                 `json:"original_base64"`
	OccurredAt     string                 `json:"occurred_at"`
}

type dlqOrigin struct {
	Kind    string `json:"kind"` // "postgres"
	BatchID int64  `json:"batch_id"`
	Seq     int64  `json:"seq"`
	EventID string `json:"event_id"`
}

// rawObjectKey: source=coingecko/ingest_date=<claim date>/batch=<%020d>.jsonl
func rawObjectKey(batchID int64, claimedAt time.Time) string {
	return fmt.Sprintf("source=%s/ingest_date=%s/batch=%020d.jsonl", event.SourceCoinGecko, xtime.FormatDate(claimedAt), batchID)
}

// dlqObjectKey: stage=export/date=<claim date>/export+batch+<%020d batch_id>.jsonl
func dlqObjectKey(batchID int64, claimedAt time.Time) string {
	return fmt.Sprintf("stage=%s/date=%s/export+batch+%020d.jsonl", stage, xtime.FormatDate(claimedAt), batchID)
}

// joinLines builds a JSONL body: lines joined by "\n" plus a trailing newline.
// No lines → empty body.
func joinLines(lines [][]byte) []byte {
	var buf bytes.Buffer
	for _, l := range lines {
		buf.Write(l)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// rowToEvent rebuilds the S1 event from the stored columns. The payload is
// taken verbatim from payload::text, so payload_sha256 stays verifiable.
func rowToEvent(r apiRow) (event.RawEvent, error) {
	ev := event.RawEvent{
		SchemaVersion:  int(r.SchemaVersion),
		EventID:        r.EventID,
		IdempotencyKey: r.IdempotencyKey,
		Source:         r.Source,
		Endpoint:       r.Endpoint,
		FetchedAt:      xtime.Format(r.FetchedAt),
		Outcome:        r.Outcome,
		Error:          r.Error,
		PayloadSHA256:  r.PayloadSHA256,
	}
	if err := json.Unmarshal([]byte(r.Request), &ev.Request); err != nil {
		return ev, fmt.Errorf("seq %d: request column: %w", r.Seq, err)
	}
	if r.HTTP != nil {
		var h event.HTTPMeta
		if err := json.Unmarshal([]byte(*r.HTTP), &h); err != nil {
			return ev, fmt.Errorf("seq %d: http column: %w", r.Seq, err)
		}
		ev.HTTP = &h
	}
	if r.Payload != nil {
		ev.Payload = json.RawMessage(*r.Payload)
	} else {
		ev.Payload = json.RawMessage("null")
	}
	return ev, nil
}

// sortViolations makes DLQ content deterministic: the validator may report
// errors in map-iteration order, and a retried batch must produce identical bytes.
func sortViolations(v []validation.Violation) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].InstanceLocation != v[j].InstanceLocation {
			return v[i].InstanceLocation < v[j].InstanceLocation
		}
		if v[i].KeywordLocation != v[j].KeywordLocation {
			return v[i].KeywordLocation < v[j].KeywordLocation
		}
		return v[i].Message < v[j].Message
	})
}

// ExportOnce first finishes batches left in status 'claimed' by a previous
// (crashed) run, then claims and exports at most one new batch of up to max
// rows. It returns the number of rows covered by the batches it exported.
func ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, max int) (int, error) {
	exported := 0
	defer updateLag(ctx, pool, m, log)

	pending, err := claimedBatches(ctx, pool)
	if err != nil {
		return 0, err
	}
	for _, b := range pending {
		log.Warn("resuming claimed batch", "stage", stage, "batch", b.ID, "object_key", b.ObjectKey)
		if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
			return exported, err
		}
		exported += b.RowCount
	}

	b, ok, err := claimNext(ctx, pool, max)
	if err != nil {
		return exported, err
	}
	if !ok {
		return exported, nil
	}
	if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
		return exported, err
	}
	return exported + b.RowCount, nil
}

// claimedBatches returns ledger rows that were claimed but never marked exported.
func claimedBatches(ctx context.Context, pool *pgxpool.Pool) ([]batch, error) {
	rows, err := pool.Query(ctx, `
SELECT batch_id, from_txid::text, from_seq, to_txid::text, to_seq, row_count, object_key, content_sha256, claimed_at
FROM pipeline.export_batches WHERE status = 'claimed' ORDER BY batch_id`)
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (batch, error) {
		var b batch
		err := r.Scan(&b.ID, &b.FromTxid, &b.FromSeq, &b.ToTxid, &b.ToSeq, &b.RowCount, &b.ObjectKey, &b.SHA256, &b.ClaimedAt)
		return b, err
	})
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	return out, nil
}

// claimNext runs the short claim transaction. It returns ok=false when no
// committed row lies between the cursor and the snapshot xmin.
func claimNext(ctx context.Context, pool *pgxpool.Pool, max int) (batch, bool, error) {
	var b batch
	tx, err := pool.Begin(ctx)
	if err != nil {
		return b, false, fmt.Errorf("exporter: begin claim: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// hi: every xid below it is committed or aborted. Evaluated in its own
	// statement; the SELECT below runs on a newer snapshot (READ COMMITTED),
	// in which every transaction below hi is already resolved.
	var hi string
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT pg_snapshot_xmin(pg_current_snapshot())::text, now()`).Scan(&hi, &now); err != nil {
		return b, false, fmt.Errorf("exporter: watermark: %w", err)
	}

	b.FromTxid, b.FromSeq = "0", 0
	err = tx.QueryRow(ctx, `
SELECT to_txid::text, to_seq FROM pipeline.export_batches
ORDER BY to_txid DESC, to_seq DESC LIMIT 1`).Scan(&b.FromTxid, &b.FromSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return b, false, fmt.Errorf("exporter: cursor: %w", err)
	}

	rows, err := tx.Query(ctx, `
SELECT ingest_txid::text, seq FROM raw.api_calls
WHERE outcome = 'ok'
  AND (ingest_txid, seq) > ($1::text::xid8, $2::bigint)
  AND ingest_txid < $3::text::xid8
ORDER BY ingest_txid, seq
LIMIT $4`, b.FromTxid, b.FromSeq, hi, max)
	if err != nil {
		return b, false, fmt.Errorf("exporter: select range: %w", err)
	}
	for rows.Next() {
		if err := rows.Scan(&b.ToTxid, &b.ToSeq); err != nil {
			rows.Close()
			return b, false, fmt.Errorf("exporter: scan range: %w", err)
		}
		b.RowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, false, fmt.Errorf("exporter: select range: %w", err)
	}
	if b.RowCount == 0 {
		return b, false, nil
	}

	// Take the id first so the object key can be stored in the same INSERT.
	// pg_get_serial_sequence also works for identity columns.
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('pipeline.export_batches', 'batch_id'))`).Scan(&b.ID); err != nil {
		return b, false, fmt.Errorf("exporter: next batch id: %w", err)
	}
	b.ClaimedAt = now
	b.ObjectKey = rawObjectKey(b.ID, now)
	_, err = tx.Exec(ctx, `
INSERT INTO pipeline.export_batches
  (batch_id, from_txid, from_seq, to_txid, to_seq, watermark_xmin, row_count, object_key, claimed_at)
VALUES ($1, $2::text::xid8, $3, $4::text::xid8, $5, $6::text::xid8, $7, $8, $9)`,
		b.ID, b.FromTxid, b.FromSeq, b.ToTxid, b.ToSeq, hi, b.RowCount, b.ObjectKey, b.ClaimedAt)
	if err != nil {
		return b, false, fmt.Errorf("exporter: insert batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return b, false, fmt.Errorf("exporter: commit claim: %w", err)
	}
	return b, true, nil
}

// exportBatch runs steps 2-4 for a claimed batch. No transaction is open
// while S3 is called.
func exportBatch(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, b batch) (err error) {
	defer func() {
		if err != nil {
			m.ExportBatches.WithLabelValues("error").Inc()
		}
	}()

	rows, err := readRange(ctx, pool, b)
	if err != nil {
		return err
	}
	if len(rows) != b.RowCount {
		return fmt.Errorf("exporter: batch %d: claimed %d rows, found %d", b.ID, b.RowCount, len(rows))
	}

	lines := make([][]byte, 0, len(rows))
	var dlqLines [][]byte
	var dlqClasses []string
	for _, r := range rows {
		line, dlq, class, err := buildLine(r, b, reg, m, log)
		if err != nil {
			return err
		}
		if dlq != nil {
			dlqLines = append(dlqLines, dlq)
			dlqClasses = append(dlqClasses, class)
			continue
		}
		lines = append(lines, line)
	}
	body := joinLines(lines)
	sum := sha256.Sum256(body)

	// Persist (or verify) the checksum before the PUT: a rebuilt batch must
	// produce the same bytes as the first attempt.
	if b.SHA256 != nil {
		if !bytes.Equal(b.SHA256, sum[:]) {
			log.Error("export batch is not deterministic", "stage", stage, "batch", b.ID,
				"stored_sha256", hex.EncodeToString(b.SHA256), "new_sha256", hex.EncodeToString(sum[:]))
			return fmt.Errorf("exporter: batch %d: content checksum changed between attempts", b.ID)
		}
	} else {
		if _, err := pool.Exec(ctx, `UPDATE pipeline.export_batches SET content_sha256 = $2 WHERE batch_id = $1 AND content_sha256 IS NULL`, b.ID, sum[:]); err != nil {
			return fmt.Errorf("exporter: store checksum: %w", err)
		}
	}

	if len(dlqLines) > 0 {
		key := dlqObjectKey(b.ID, b.ClaimedAt)
		created, err := put(ctx, s3c, m, s3.BucketDLQ, key, joinLines(dlqLines))
		if err != nil {
			return err
		}
		if created { // a resumed batch must not count its DLQ records twice
			for _, c := range dlqClasses {
				m.DLQ.WithLabelValues(stage, c).Inc()
			}
		}
		log.Error("raw events sent to dlq", "stage", stage, "batch", b.ID, "bucket", s3.BucketDLQ, "object_key", key, "count", len(dlqLines))
	}

	created, err := put(ctx, s3c, m, s3.BucketRaw, b.ObjectKey, body)
	if err != nil {
		return err
	}

	if _, err := pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'exported', exported_at = now() WHERE batch_id = $1 AND status = 'claimed'`, b.ID); err != nil {
		return fmt.Errorf("exporter: mark exported: %w", err)
	}
	m.ExportBatches.WithLabelValues("exported").Inc()
	log.Info("raw batch saved", "stage", stage, "batch", b.ID, "bucket", s3.BucketRaw, "object_key", b.ObjectKey,
		"rows", len(lines), "dlq", len(dlqLines), "bytes", len(body), "created", created, "sha256", hex.EncodeToString(sum[:]))
	return nil
}

// put wraps PutIfAbsent with the etl_s3_put_total{result} metric.
func put(ctx context.Context, s3c *s3.Client, m *obs.Metrics, bucket, key string, body []byte) (bool, error) {
	created, err := s3c.PutIfAbsent(ctx, bucket, key, body)
	switch {
	case errors.Is(err, s3.ErrChecksumMismatch):
		m.S3Puts.WithLabelValues("mismatch").Inc()
		return false, fmt.Errorf("exporter: %w", err)
	case err != nil:
		m.S3Puts.WithLabelValues("error").Inc()
		return false, fmt.Errorf("exporter: %w", err)
	case created:
		m.S3Puts.WithLabelValues("created").Inc()
	default:
		m.S3Puts.WithLabelValues("exists").Inc()
	}
	return created, nil
}

func readRange(ctx context.Context, pool *pgxpool.Pool, b batch) ([]apiRow, error) {
	rows, err := pool.Query(ctx, `
SELECT seq, event_id::text, idempotency_key, schema_version, source, endpoint, request::text,
       fetched_at, outcome, http::text, error, payload::text, encode(payload_sha256, 'hex')
FROM raw.api_calls
WHERE outcome = 'ok'
  AND (ingest_txid, seq) >  ($1::text::xid8, $2::bigint)
  AND (ingest_txid, seq) <= ($3::text::xid8, $4::bigint)
ORDER BY ingest_txid, seq`, b.FromTxid, b.FromSeq, b.ToTxid, b.ToSeq)
	if err != nil {
		return nil, fmt.Errorf("exporter: read batch %d: %w", b.ID, err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (apiRow, error) {
		var a apiRow
		err := r.Scan(&a.Seq, &a.EventID, &a.IdempotencyKey, &a.SchemaVersion, &a.Source, &a.Endpoint, &a.Request,
			&a.FetchedAt, &a.Outcome, &a.HTTP, &a.Error, &a.Payload, &a.PayloadSHA256)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("exporter: read batch %d: %w", b.ID, err)
	}
	return out, nil
}

// buildLine returns either the S1 line for the raw object or a DLQ line with
// its error class.
func buildLine(r apiRow, b batch, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) (line, dlq []byte, class string, err error) {
	ev, convErr := rowToEvent(r)
	if convErr == nil {
		line, convErr = event.Marshal(ev)
	}
	if convErr != nil {
		orig := ""
		if r.Payload != nil {
			orig = *r.Payload
		}
		log.Error("raw event cannot be rebuilt", "stage", stage, "batch", b.ID, "event_id", r.EventID, "seq", r.Seq, "error", convErr)
		rec := newDLQ(r, b, "decode_error", []validation.Violation{{Message: convErr.Error()}}, []byte(orig))
		d, err := marshalDLQ(rec, reg, log)
		return nil, d, "decode_error", err
	}

	viol, verr := reg.Validate(validation.IDRawEvent, line)
	if verr != nil {
		return nil, nil, "", fmt.Errorf("exporter: validate seq %d: %w", r.Seq, verr)
	}
	if len(viol) == 0 {
		m.SchemaValidations.WithLabelValues(stage, schemaName, "pass").Inc()
		return line, nil, "", nil
	}
	m.SchemaValidations.WithLabelValues(stage, schemaName, "fail").Inc()
	for _, v := range viol {
		m.SchemaViolations.WithLabelValues(stage, schemaName, validation.Keyword(v)).Inc()
	}
	sortViolations(viol)
	log.Error("raw event failed S1 validation", "stage", stage, "batch", b.ID, "event_id", r.EventID, "seq", r.Seq, "violations", viol)
	rec := newDLQ(r, b, "schema_violation", viol, line)
	d, err := marshalDLQ(rec, reg, log)
	return nil, d, "schema_violation", err
}

func newDLQ(r apiRow, b batch, class string, viol []validation.Violation, original []byte) dlqRecord {
	return dlqRecord{
		SchemaVersion:  1,
		Stage:          stage,
		ErrorClass:     class,
		Violations:     viol,
		Origin:         dlqOrigin{Kind: "postgres", BatchID: b.ID, Seq: r.Seq, EventID: r.EventID},
		OriginalBase64: base64.StdEncoding.EncodeToString(original),
		// Claim time, not time.Now(): a retried batch must rebuild identical bytes.
		OccurredAt: xtime.Format(b.ClaimedAt),
	}
}

// marshalDLQ serializes a DLQ record and checks it against S3. A DLQ record
// that fails its own schema is still written (the data must not be lost) and
// logged at ERROR with the original bytes in base64.
func marshalDLQ(rec dlqRecord, reg *validation.Registry, log *slog.Logger) ([]byte, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("exporter: marshal dlq record: %w", err)
	}
	viol, verr := reg.Validate(validation.IDDLQRecord, b)
	if verr != nil || len(viol) > 0 {
		log.Error("dlq record failed S3 validation", "stage", stage, "error", verr, "violations", viol,
			"original_base64", rec.OriginalBase64)
	}
	return b, nil
}

// updateLag sets etl_export_lag_seconds = now - fetched_at of the oldest ok
// row after the last exported batch (0 when everything is exported).
func updateLag(ctx context.Context, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) {
	if ctx.Err() != nil {
		return
	}
	txid, seq := "0", int64(0)
	err := pool.QueryRow(ctx, `
SELECT to_txid::text, to_seq FROM pipeline.export_batches WHERE status = 'exported'
ORDER BY to_txid DESC, to_seq DESC LIMIT 1`).Scan(&txid, &seq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Warn("export lag: cursor query failed", "stage", stage, "error", err)
		return
	}
	var lag float64
	err = pool.QueryRow(ctx, `
SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(fetched_at)), 0)::float8
FROM raw.api_calls
WHERE outcome = 'ok' AND (ingest_txid, seq) > ($1::text::xid8, $2::bigint)`, txid, seq).Scan(&lag)
	if err != nil {
		log.Warn("export lag query failed", "stage", stage, "error", err)
		return
	}
	if lag < 0 {
		lag = 0
	}
	m.ExportLag.Set(lag)
}

// Run holds the exporter advisory lock on a dedicated connection for the
// whole process lifetime and calls ExportOnce every cfg.ExportInterval,
// draining immediately while full batches keep coming. It returns nil when
// ctx is cancelled and an error when the lock cannot be taken or is lost.
func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error {
	conn, err := acquireLock(ctx, pool, log)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer releaseLock(conn, log)
	log.Info("exporter lock acquired", "stage", stage)

	ticker := time.NewTicker(cfg.ExportInterval)
	defer ticker.Stop()
	for {
		// A dead lock connection means the session lock is gone: stop so a
		// restarted process re-acquires it instead of running unprotected.
		if err := conn.Ping(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("exporter: lock connection lost: %w", err)
		}
		for {
			n, err := ExportOnce(ctx, pool, s3c, reg, m, log, cfg.ExportBatchMax)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Error("export failed", "stage", stage, "error", err)
				break
			}
			if n < cfg.ExportBatchMax {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func acquireLock(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("exporter: acquire lock connection: %w", err)
	}
	for attempt := 1; attempt <= lockAttempts; attempt++ {
		var ok bool
		// Session-level lock: held until unlocked or the connection closes.
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockName).Scan(&ok); err != nil {
			conn.Release()
			return nil, fmt.Errorf("exporter: try advisory lock: %w", err)
		}
		if ok {
			return conn, nil
		}
		log.Warn("exporter lock held by another instance", "stage", stage, "attempt", attempt)
		select {
		case <-ctx.Done():
			conn.Release()
			return nil, ctx.Err()
		case <-time.After(lockRetryDelay):
		}
	}
	conn.Release()
	return nil, fmt.Errorf("exporter: advisory lock %q not acquired after %d attempts", lockName, lockAttempts)
}

func releaseLock(conn *pgxpool.Conn, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, lockName); err != nil {
		log.Warn("exporter unlock failed; closing connection", "stage", stage, "error", err)
		_ = conn.Conn().Close(ctx) // closing the session drops the lock; the pool discards closed conns
	}
	conn.Release()
}
```

- [ ] **Step 5: Run the unit tests**

Run: `go test -short ./internal/exporter/ -v`
Expected: `TestObjectKeys`, `TestJoinLines`, `TestRowToEventKeepsPayloadVerbatim`, `TestSortViolationsDeterministic` PASS; the six integration tests SKIP.

- [ ] **Step 6: Run the integration tests (Docker required)**

Run: `go test -race ./internal/exporter/ -count=1 -v`
Expected: all PASS (about 15 s, one Postgres and one versitygw container per test):
```
--- PASS: TestExportOnceExportsEveryRowExactlyOnce
--- PASS: TestExportOnceResumesAfterCrash
--- PASS: TestExportOnceNeverSkipsOutOfOrderCommits
--- PASS: TestExportOnceSendsInvalidRowsToDLQ
--- PASS: TestRunExportsAndStopsOnCancel
--- PASS: TestRunFailsWhenLockHeld
```
If `TestExportOnceSendsInvalidRowsToDLQ` fails with `dlq line fails S3 dlq_record schema` and a violation on `/origin`, add `"seq": { "type": "integer", "minimum": 1 }` to `origin.properties` in `schemas/pipeline/dlq_record.v1.json` (see CONTRACT NOTES) and re-run. Then run `go vet ./internal/exporter/` (no output).

- [ ] **Step 7: Commit**

```bash
git add internal/exporter/
git commit -m "feat(exporter): xid8-cursor exporter with claim ledger and write-once raw objects

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 8: Prove the out-of-order test bites (mutation check, no commit)**

In `claimNext`, temporarily replace the line `  AND ingest_txid < $3::text::xid8` with `  AND $3::text IS NOT NULL`. This removes the snapshot bound and keeps the parameter count. Run: `go test ./internal/exporter/ -run OutOfOrder -count=1`
Expected: FAIL with `ExportOnce with A open = 1, want 0 (stall)`.
Restore it with `git checkout -- internal/exporter/exporter.go` and re-run the same command. Expected: PASS.

---

CONTRACT NOTES

1. DLQ records use the Task 3 shape of `schemas/pipeline/dlq_record.v1.json`: `{"schema_version":1,"stage":"publish","error_class":"decode_error"|"schema_violation","violations":[…],"origin":{"kind":"s3","bucket":"raw","object_key":…,"line_no":…,"batch_id":…,"event_id"?},"original_base64":…,"occurred_at":…}`. `occurred_at` and the DLQ object date come from the batch's `exported_at`, not the clock, so a retried batch rebuilds byte-identical DLQ objects. Task 11 has a unit test (`TestDLQRecordsSatisfySchema`) that fails if the schema and this struct drift apart.
2. The DLQ key template `<topic or "export"/"publish">+<partition or batch>+<%020d offset or batch_id>` is read as the literal word `batch` in the middle slot: `stage=publish/date=<UTC date of failure>/publish+batch+<%020d batch_id>.jsonl`. The exporter should use `export+batch+<%020d batch_id>` for consistency.
3. The `raw_object` header holds the raw object key without the bucket, for example `source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl`. The transformer should copy it into `Lineage.RawObject` unchanged.
4. Metric label values used by the publisher: `etl_schema_validations_total{stage="publish",schema="raw_event.v1",result="pass"|"fail"}` (the convention shared by every stage), `etl_schema_violations_total{stage="publish",schema="raw_event.v1",keyword=…}`, `etl_dlq_total{stage="publish",error_class=…}`, `etl_s3_put_total{result=…}` (DLQ PUT only).
5. When the topic already exists, `EnsureTopic` checks both the partition count and `retention.ms`, and it creates topics with replication factor `-1` (broker default). Redpanda v26.2.4 returned a non-zero topic id in verification, so `TopicID` uses the broker id there.
6. kprom registers its collectors through `promauto` when the client is created, and it panics on a duplicate registration. Create at most one producer per registry.

### Task 10: Broker client (internal/queue)

**Files:**
- Create: `internal/queue/queue.go`
- Test: `internal/queue/queue_test.go` (unit, no Docker)
- Test: `internal/queue/queue_integration_test.go` (Redpanda via testcontainers)
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes:
  ```go
  // internal/event
  const EndpointMarkets = "coins_markets"; const EndpointCoinDetail = "coin_detail"
  type Request struct { Path string `json:"path"`; Params map[string]string `json:"params"` }
  type RawEvent struct { /* … */ Endpoint string `json:"endpoint"`; Request Request `json:"request"` /* … */ }
  // internal/testutil
  func StartRedpanda(t *testing.T) (brokers []string)
  func SkipIfNoDocker(t *testing.T)
  ```
- Produces:
  ```go
  // internal/queue
  const (
      HeaderEventID        = "event_id"
      HeaderIdempotencyKey = "idempotency_key"
      HeaderSchema         = "schema"
      HeaderRawObject      = "raw_object"
      HeaderLineNo         = "line_no"
  )
  func MessageKey(ev event.RawEvent) string // "coins_markets" or the coin id from Request.Path
  func NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error) // idempotent, acks=all
  func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error
  func TopicID(ctx context.Context, cl *kgo.Client, topic string, fallback uuid.UUID) (uuid.UUID, error) // fallback when broker returns zero id
  ```

Background for the engineer:
- **franz-go** (`github.com/twmb/franz-go/pkg/kgo`) is a pure-Go Kafka client. `kgo.NewClient(opts...)` does not connect at construction time; it connects lazily on first use. One client can produce and also run admin requests.
- The producer is **idempotent by default** (the broker drops duplicate retries using a producer id and sequence numbers). Idempotence requires `acks=all`, which `kgo.RequiredAcks(kgo.AllISRAcks())` sets. Do **not** add `kgo.DisableIdempotentWrite()`.
- **kadm** (`github.com/twmb/franz-go/pkg/kadm`) is the admin helper that wraps a `*kgo.Client`. `CreateTopics` does not return a Go error for per-topic failures. Look at `resp.Err` for each topic and compare it with `errors.Is(err, kerr.TopicAlreadyExists)`. `ListTopics(ctx, topic)` sends a metadata request, and `TopicDetail.ID` is the 16-byte topic id (all zeros when the broker has none). `DescribeTopicConfigs` returns per-topic config entries, and `Config.MaybeValue()` reads a value.
- **kprom** (`github.com/twmb/franz-go/plugin/kprom`) is a client hook that exports producer and broker metrics. It registers its collectors when the client is built (in `OnNewClient`) through `promauto`, so a second producer on the same registry panics. Pass `nil` to skip client metrics, which tests do.
- Redpanda speaks the Kafka protocol. Auto-create is off in our stack, so the topic must exist before anything produces to it. franz-go gives up on a record after 4 `UNKNOWN_TOPIC_OR_PARTITION` responses.

- [ ] **Step 1: Add the franz-go modules**

```bash
go get github.com/twmb/franz-go@v1.22.1 github.com/twmb/franz-go/pkg/kadm@v1.19.0 github.com/twmb/franz-go/plugin/kprom@v1.5.0 github.com/google/uuid github.com/prometheus/client_golang
```

- [ ] **Step 2: Write the failing unit tests**

Create `internal/queue/queue_test.go`:

```go
package queue

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"coingecko-etl/internal/event"
)

func TestMessageKey(t *testing.T) {
	cases := []struct {
		name string
		ev   event.RawEvent
		want string
	}{
		{"markets", event.RawEvent{Endpoint: event.EndpointMarkets, Request: event.Request{Path: "/coins/markets"}}, "coins_markets"},
		{"detail", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/coins/bitcoin"}}, "bitcoin"},
		{"detail with api prefix", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/api/v3/coins/usd-coin"}}, "usd-coin"},
		{"detail trailing slash", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/coins/ethereum/"}}, "ethereum"},
		{"empty path falls back to endpoint", event.RawEvent{Endpoint: event.EndpointCoinDetail}, "coin_detail"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := MessageKey(c.ev); got != c.want {
				t.Fatalf("MessageKey = %q, want %q", got, c.want)
			}
		})
	}
}

func TestNewProducerRequiresBrokers(t *testing.T) {
	if _, err := NewProducer(nil, "x", nil); err == nil {
		t.Fatal("expected error for empty broker list")
	}
}

func TestNewProducerNilRegistry(t *testing.T) {
	cl, err := NewProducer([]string{"127.0.0.1:1"}, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}

func TestNewProducerRegistersClientMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	cl, err := NewProducer([]string{"127.0.0.1:1"}, "test", reg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// kprom registered etl_publisher_connects_total{node_id} when the client was built.
	dup := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etl_publisher", Name: "connects_total", Help: "Total number of connections opened",
	}, []string{"node_id"})
	err = reg.Register(dup)
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		t.Fatalf("expected kprom collector to be registered, Register returned %v", err)
	}
}
```

- [ ] **Step 3: Run the unit tests to confirm they fail**

Run: `go test ./internal/queue/ -run 'TestMessageKey|TestNewProducer' -v`
Expected: FAIL with a build error such as `undefined: MessageKey` and `undefined: NewProducer`.

- [ ] **Step 4: Implement the package**

Create `internal/queue/queue.go`:

```go
// Package queue builds franz-go clients and manages the raw topic.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"coingecko-etl/internal/event"
)

// Record header names. Values are UTF-8 strings.
const (
	HeaderEventID        = "event_id"
	HeaderIdempotencyKey = "idempotency_key"
	HeaderSchema         = "schema"
	HeaderRawObject      = "raw_object"
	HeaderLineNo         = "line_no"
)

// MessageKey is the partitioning key: "coins_markets" for the markets
// endpoint, the coin id (last segment of Request.Path, e.g. "/coins/bitcoin")
// for the detail endpoint. Every repeat of the same data lands on one partition.
func MessageKey(ev event.RawEvent) string {
	if ev.Endpoint == event.EndpointMarkets {
		return event.EndpointMarkets
	}
	p := strings.TrimRight(ev.Request.Path, "/")
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		p = p[i+1:]
	}
	if p == "" {
		return ev.Endpoint
	}
	return p
}

// NewProducer returns an idempotent producer with acks=all.
// kprom client metrics are registered on reg under namespace etl_publisher
// (reg may be nil: no client metrics). kprom registers its collectors when
// the client is created and panics on duplicate registration, so build at
// most one producer per registry.
func NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("queue: no brokers configured")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotence is on by default and requires acks=all
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20 * time.Millisecond),
		kgo.RecordDeliveryTimeout(2 * time.Minute),
	}
	if reg != nil {
		kopts := []kprom.Opt{kprom.Registerer(reg)}
		if g, ok := reg.(prometheus.Gatherer); ok {
			kopts = append(kopts, kprom.Gatherer(g))
		}
		opts = append(opts, kgo.WithHooks(kprom.NewMetrics("etl_publisher", kopts...)))
	}
	return kgo.NewClient(opts...)
}

// EnsureTopic creates topic with the given partition count and retention
// (replication factor = broker default). If the topic already exists it
// verifies the partition count and retention.ms and returns an error on mismatch.
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error {
	adm := kadm.NewClient(cl)
	retentionMS := strconv.FormatInt(retention.Milliseconds(), 10)
	resps, err := adm.CreateTopics(ctx, partitions, -1, map[string]*string{
		"retention.ms": kadm.StringPtr(retentionMS),
	}, topic)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	resp, ok := resps[topic]
	if !ok {
		return fmt.Errorf("create topic %s: no response for topic", topic)
	}
	if resp.Err == nil {
		return nil
	}
	if !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w (%s)", topic, resp.Err, resp.ErrMessage)
	}

	// Topic exists: verify it matches what we expect.
	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe topic %s: %w", topic, err)
	}
	td, ok := details[topic]
	if !ok {
		return fmt.Errorf("describe topic %s: not in metadata", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("describe topic %s: %w", topic, td.Err)
	}
	if got := int32(len(td.Partitions)); got != partitions {
		return fmt.Errorf("topic %s has %d partitions, expected %d", topic, got, partitions)
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, err)
	}
	if rc.Err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, rc.Err)
	}
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.MaybeValue() != retentionMS {
			return fmt.Errorf("topic %s has retention.ms=%s, expected %s", topic, c.MaybeValue(), retentionMS)
		}
	}
	return nil
}

// TopicID returns the broker-assigned topic id (the topic's incarnation).
// Brokers without topic-id support return all zeros; then fallback is returned.
func TopicID(ctx context.Context, cl *kgo.Client, topic string, fallback uuid.UUID) (uuid.UUID, error) {
	details, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return uuid.Nil, fmt.Errorf("metadata for %s: %w", topic, err)
	}
	td, ok := details[topic]
	if !ok {
		return uuid.Nil, fmt.Errorf("metadata for %s: topic not returned", topic)
	}
	if td.Err != nil {
		return uuid.Nil, fmt.Errorf("metadata for %s: %w", topic, td.Err)
	}
	if td.ID == (kadm.TopicID{}) {
		return fallback, nil
	}
	return uuid.UUID(td.ID), nil
}
```

Notes:
- `retention.ms` is sent as a string config value (`kadm.StringPtr`). Replication factor `-1` uses the broker default (1 on a dev Redpanda, 3 in production).
- The existing-topic check returns an error rather than changing the topic. Changing the partition count would remap keys to partitions and break per-partition dedup locality (spec §7.6).
- `uuid.UUID(td.ID)` is a plain conversion because both types are `[16]byte`.

- [ ] **Step 5: Run the unit tests to confirm they pass**

Run: `go test ./internal/queue/ -run 'TestMessageKey|TestNewProducer' -v`
Expected: PASS for `TestMessageKey` (5 subtests), `TestNewProducerRequiresBrokers`, `TestNewProducerNilRegistry` and `TestNewProducerRegistersClientMetrics`.

- [ ] **Step 6: Write the integration test**

Create `internal/queue/queue_integration_test.go`:

```go
package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/testutil"
)

func TestQueueIntegration(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	brokers := testutil.StartRedpanda(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	prod, err := NewProducer(brokers, "queue-it", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	const topic = "coingecko.raw"
	const retention = 168 * time.Hour

	t.Run("EnsureTopic is idempotent", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 6, retention); err != nil {
			t.Fatalf("first EnsureTopic: %v", err)
		}
		if err := EnsureTopic(ctx, prod, topic, 6, retention); err != nil {
			t.Fatalf("second EnsureTopic: %v", err)
		}
	})

	t.Run("EnsureTopic detects partition mismatch", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 3, retention); err == nil {
			t.Fatal("expected partition mismatch error")
		}
	})

	t.Run("EnsureTopic detects retention mismatch", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 6, time.Hour); err == nil {
			t.Fatal("expected retention mismatch error")
		}
	})

	t.Run("TopicID", func(t *testing.T) {
		fallback := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		id1, err := TopicID(ctx, prod, topic, fallback)
		if err != nil {
			t.Fatal(err)
		}
		if id1 == uuid.Nil {
			t.Fatal("TopicID returned the nil uuid")
		}
		if id1 == fallback {
			t.Logf("broker returned no topic id; using fallback %s", id1)
		} else {
			t.Logf("broker topic id %s", id1)
		}
		id2, err := TopicID(ctx, prod, topic, fallback)
		if err != nil {
			t.Fatal(err)
		}
		if id1 != id2 {
			t.Fatalf("TopicID not stable: %s vs %s", id1, id2)
		}
		if _, err := TopicID(ctx, prod, "no.such.topic", fallback); err == nil {
			t.Fatal("expected error for unknown topic")
		}
	})

	t.Run("produce and consume with headers", func(t *testing.T) {
		value := []byte(`{"event_id":"x","payload":{"a":1}}`)
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte("bitcoin"),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: HeaderEventID, Value: []byte("0199c8a0-0000-7000-8000-000000000001")},
				{Key: HeaderLineNo, Value: []byte("7")},
			},
		}
		if err := prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
		cons, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cons.Close()
		var got *kgo.Record
		for got == nil && ctx.Err() == nil {
			fs := cons.PollFetches(ctx)
			fs.EachError(func(_ string, _ int32, err error) { t.Logf("fetch error: %v", err) })
			fs.EachRecord(func(r *kgo.Record) { got = r })
		}
		if got == nil {
			t.Fatal("no record consumed")
		}
		if string(got.Key) != "bitcoin" || string(got.Value) != string(value) {
			t.Fatalf("got key=%q value=%q", got.Key, got.Value)
		}
		h := map[string]string{}
		for _, rh := range got.Headers {
			h[rh.Key] = string(rh.Value)
		}
		if h[HeaderEventID] != "0199c8a0-0000-7000-8000-000000000001" || h[HeaderLineNo] != "7" {
			t.Fatalf("headers = %v", h)
		}
	})
}
```

It is not known in advance whether Redpanda supports topic ids, so the `TopicID` subtest passes with either the broker id or the fallback and logs which one it got. (Redpanda v26.2.4 returned a real id during plan verification.)

- [ ] **Step 7: Run the integration test**

Run: `go test ./internal/queue/ -run TestQueueIntegration -v -count=1`
Expected: PASS for all five subtests. The log contains either `broker topic id <uuid>` or `broker returned no topic id; using fallback 00000000-0000-0000-0000-000000000001`. Without Docker the test is skipped (`SKIP`).

- [ ] **Step 8: Vet and commit**

```bash
go vet ./internal/queue/
git add go.mod go.sum internal/queue/
git commit -m "feat(queue): franz-go producer, topic bootstrap, topic id and message keys

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

### Task 11: Publisher (internal/publisher)

**Files:**
- Create: `internal/publisher/publisher.go`
- Test: `internal/publisher/publisher_test.go` (unit, no Docker)
- Test: `internal/publisher/publisher_integration_test.go` (Postgres + versitygw + Redpanda)
- Uses fixtures (copied by Task 4): `schemas/testdata/coingecko/markets_top10_t0.json`, `markets_top10_t1.json`, `markets_top10_t2.json`, `coin_bitcoin_full_t0.json`

**Interfaces:**
- Consumes:
  ```go
  // internal/config
  type Config struct { /* … */ Topic string; PublishLease time.Duration; PublishPoll time.Duration /* … */ }
  // internal/event
  func Unmarshal(b []byte) (RawEvent, error)
  func Marshal(ev RawEvent) ([]byte, error)
  func SHA256Hex(b []byte) string
  func IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string
  // internal/validation
  type Violation struct { InstanceLocation string `json:"instance_location"`; KeywordLocation string `json:"keyword_location"`; Message string `json:"message"` }
  func Load(fsys fs.FS) (*Registry, error)
  func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)
  func Keyword(v Violation) string
  const IDRawEvent, IDDLQRecord
  // schemas
  var FS embed.FS
  // internal/queue
  func MessageKey(ev event.RawEvent) string
  func NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error)
  func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error
  const HeaderEventID, HeaderIdempotencyKey, HeaderSchema, HeaderRawObject, HeaderLineNo
  // internal/storage/s3
  const BucketRaw, BucketProcessed, BucketRejected, BucketDLQ
  var ErrChecksumMismatch
  func New(ctx context.Context, cfg Config) (*Client, error)
  func (c *Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (created bool, err error)
  func (c *Client) Get(ctx context.Context, bucket, key string) ([]byte, error)
  func (c *Client) EnsureBuckets(ctx context.Context, buckets ...string) error
  // internal/storage/pg
  func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)
  func Migrate(ctx context.Context, pool *pgxpool.Pool) error
  func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error
  func InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error
  // internal/exporter
  func ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, max int) (exported int, err error)
  // internal/coingecko
  func MarketsRequest(perPage int) event.Request
  func CoinDetailRequest(coinID string) event.Request
  // internal/obs
  func NewMetrics(reg prometheus.Registerer) *Metrics // fields used: SchemaValidations, SchemaViolations, DLQ, S3Puts, PublishedRecords
  // internal/xtime
  func Format(t time.Time) string
  func FormatDate(t time.Time) string
  // internal/testutil
  func StartPostgres(t *testing.T) (dsn string)
  func StartRedpanda(t *testing.T) (brokers []string)
  func StartVersityGW(t *testing.T) (endpoint, dataDir string)
  func SkipIfNoDocker(t *testing.T)
  ```
- Produces:
  ```go
  // internal/publisher
  func PublishOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, topic string, lease time.Duration, m *obs.Metrics, log *slog.Logger) (published bool, err error)
  func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
  ```

Background for the engineer (spec §4.3, §7.1):
- The publisher moves raw objects (one JSONL line = one S1 event = one API call) from `s3://raw` to the topic `coingecko.raw`. Every message value is the raw line **byte for byte**. Downstream checks `sha256(payload)`, so the line is never re-encoded.
- **Lease protocol.** One `UPDATE … WHERE batch_id = (SELECT … FOR UPDATE SKIP LOCKED) RETURNING …` statement runs as its own short transaction. It picks the oldest batch with `status='exported'`, `published_at IS NULL` and a free or expired lease, and sets `publish_lease_until = now() + lease`. `SKIP LOCKED` lets several publishers work at once without blocking each other. No transaction is open during S3 or broker I/O.
- **At-least-once.** The batch is marked `published_at = now()` only after every record is acknowledged. If a produce fails, or the process crashes before the mark, the lease expires and the batch is published again. The duplicate messages carry the same `event_id`, and the transformer drops them (spec §7.3).
- **Invalid lines.** A line that is not JSON or fails S1 is not produced. It becomes a DLQ record (schema `pipeline/dlq_record.v1.json`), and all DLQ records of one batch go into one write-once object `dlq/stage=publish/date=<date>/publish+batch+<%020d batch_id>.jsonl`. Each DLQ record is validated against its own schema before it is written. If that validation fails, the record is logged at ERROR with the original bytes in base64 (spec §5.2). DLQ lines contain no clock reading or random id (`occurred_at` is the batch's `exported_at`), so a retried batch gets 412 with an equal checksum. If a retry still finds a different checksum (`s3.ErrChecksumMismatch`, possible only when a redeploy changed validator messages), the first object is kept and the batch continues.
- `kgo.Client.ProduceSync(ctx, recs...)` hands every record to the client's batching producer and waits until all of them are acknowledged. `.FirstErr()` returns the first failure. `Flush` is a cheap extra barrier.
- `make_interval(secs => $1)` turns a float64 number of seconds into a Postgres interval, which avoids encoding a `time.Duration` as an interval in pgx.

- [ ] **Step 1: Write the failing unit tests**

Create `internal/publisher/publisher_test.go`:

```go
package publisher

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

func TestSplitLines(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a\n", []string{"a"}},
		{"a\nb\n", []string{"a", "b"}},
		{"a\nb", []string{"a", "b"}},
		{"a\n\nb\n", []string{"a", "", "b"}},
	}
	for _, c := range cases {
		got := splitLines([]byte(c.in))
		if len(got) != len(c.want) {
			t.Fatalf("splitLines(%q) = %d lines, want %d", c.in, len(got), len(c.want))
		}
		for i := range got {
			if string(got[i]) != c.want[i] {
				t.Fatalf("splitLines(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestDLQObjectKey(t *testing.T) {
	now := time.Date(2026, 10, 10, 23, 59, 59, 0, time.UTC)
	got := dlqObjectKey(now, 42)
	want := "stage=publish/date=2026-10-10/publish+batch+00000000000000000042.jsonl"
	if got != want {
		t.Fatalf("dlqObjectKey = %q, want %q", got, want)
	}
}

func TestDLQRecordsSatisfySchema(t *testing.T) {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	b := batch{ID: 1, ObjectKey: "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl",
		ExportedAt: time.Date(2026, 10, 10, 10, 7, 55, 0, time.UTC)}
	cases := map[string][]byte{
		"valid json, wrong shape": []byte(`{"event_id":"0199c8a0-0000-7000-8000-000000000001","idempotency_key":"` + sixtyFourHex() + `","extra":1}`),
		"json with bad ids":       []byte(`{"event_id":"nope","idempotency_key":"short"}`),
		"not json":                []byte("{not json"),
		"empty line":              []byte(""),
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			r := newDLQRecord(b, 3, line, errClassSchema, []validation.Violation{{InstanceLocation: "/x", KeywordLocation: "/additionalProperties", Message: "bad"}})
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			viols, err := reg.Validate(validation.IDDLQRecord, b)
			if err != nil {
				t.Fatal(err)
			}
			if len(viols) > 0 {
				t.Fatalf("dlq record violates schema: %+v\n%s", viols, b)
			}
		})
	}
}

func TestDLQRecordKeepsOriginal(t *testing.T) {
	b := batch{ID: 7, ObjectKey: "k", ExportedAt: time.Date(2026, 10, 10, 23, 59, 59, 0, time.UTC)}
	r := newDLQRecord(b, 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}})
	if r.OriginalBase64 != "e3g=" {
		t.Fatalf("original_base64 = %q", r.OriginalBase64)
	}
	if r.Origin.Kind != "s3" || r.Origin.Bucket != "raw" || r.Origin.ObjectKey != "k" || r.Origin.LineNo != 1 ||
		r.Origin.BatchID != 7 || r.Stage != "publish" || r.OccurredAt != "2026-10-10T23:59:59.000Z" {
		t.Fatalf("record = %+v", r)
	}
	// No clock, no random id: a retried batch rebuilds the same bytes.
	again := newDLQRecord(b, 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}})
	b1, _ := json.Marshal(r)
	b2, _ := json.Marshal(again)
	if !bytes.Equal(b1, b2) {
		t.Fatalf("dlq record is not deterministic:\n%s\n%s", b1, b2)
	}
}

func sixtyFourHex() string {
	const h = "0123456789abcdef"
	b := make([]byte, 64)
	for i := range b {
		b[i] = h[i%16]
	}
	return string(b)
}
```

- [ ] **Step 2: Run the unit tests to confirm they fail**

Run: `go test ./internal/publisher/ -v`
Expected: FAIL with a build error such as `undefined: splitLines`, `undefined: dlqObjectKey`, `undefined: newDLQRecord` and `undefined: errClassSchema`.

- [ ] **Step 3: Implement the publisher**

Create `internal/publisher/publisher.go`:

```go
// Package publisher leases exported raw batches, validates each JSONL line
// against S1 and produces it verbatim to the broker.
package publisher

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
)

const (
	stage            = "publish"
	schemaLabel      = "raw_event.v1"
	errClassDecode   = "decode_error"
	errClassSchema   = "schema_violation"
	maxMessageLength = 1024
)

// leaseSQL leases the oldest exported, unpublished batch whose lease is free
// or expired. FOR UPDATE SKIP LOCKED lets several publishers run without
// blocking each other; the single statement is its own short transaction.
const leaseSQL = `
UPDATE pipeline.export_batches
   SET publish_lease_until = now() + make_interval(secs => $1)
 WHERE batch_id = (
        SELECT batch_id
          FROM pipeline.export_batches
         WHERE status = 'exported'
           AND published_at IS NULL
           AND (publish_lease_until IS NULL OR publish_lease_until < now())
         ORDER BY batch_id
         LIMIT 1
         FOR UPDATE SKIP LOCKED)
RETURNING batch_id, object_key, COALESCE(exported_at, claimed_at)`

const markSQL = `
UPDATE pipeline.export_batches
   SET published_at = now(), publish_lease_until = NULL
 WHERE batch_id = $1 AND published_at IS NULL`

type batch struct {
	ID         int64
	ObjectKey  string
	ExportedAt time.Time // DLQ occurred_at and date: stable across retries of the batch
}

// leaseBatch returns ok=false when no batch is available.
func leaseBatch(ctx context.Context, pool *pgxpool.Pool, lease time.Duration) (batch, bool, error) {
	var b batch
	err := pool.QueryRow(ctx, leaseSQL, lease.Seconds()).Scan(&b.ID, &b.ObjectKey, &b.ExportedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return batch{}, false, nil
	}
	if err != nil {
		return batch{}, false, fmt.Errorf("lease batch: %w", err)
	}
	return b, true, nil
}

func markPublished(ctx context.Context, pool *pgxpool.Pool, batchID int64) error {
	if _, err := pool.Exec(ctx, markSQL, batchID); err != nil {
		return fmt.Errorf("mark batch %d published: %w", batchID, err)
	}
	return nil
}

// splitLines splits a JSONL object into lines. The trailing newline does not
// produce an empty last line; empty lines in the middle are kept (they fail
// validation and go to the DLQ).
func splitLines(body []byte) [][]byte {
	if len(body) == 0 {
		return nil
	}
	lines := bytes.Split(body, []byte("\n"))
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// dlqRecord is one line of a DLQ object (schema pipeline/dlq_record.v1.json,
// Task 3). The exporter and the transformer write the same shape.
type dlqRecord struct {
	SchemaVersion  int                    `json:"schema_version"`
	Stage          string                 `json:"stage"`
	ErrorClass     string                 `json:"error_class"`
	Violations     []validation.Violation `json:"violations"`
	Origin         dlqOrigin              `json:"origin"`
	OriginalBase64 string                 `json:"original_base64"`
	OccurredAt     string                 `json:"occurred_at"`
}

type dlqOrigin struct {
	Kind      string `json:"kind"` // "s3"
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	LineNo    int    `json:"line_no"`
	BatchID   int64  `json:"batch_id"`
	EventID   string `json:"event_id,omitempty"`
}

func truncate(s string) string {
	if len(s) > maxMessageLength {
		return s[:maxMessageLength]
	}
	return s
}

// newDLQRecord builds a DLQ record for one bad line. Nothing in it depends on
// the clock: occurred_at is the batch's exported_at, so a retried batch
// rebuilds byte-identical DLQ lines and the write-once PUT gets 412 with an
// equal checksum. event_id is copied only when the line carries a valid UUID.
func newDLQRecord(b batch, lineNo int, line []byte, class string, viols []validation.Violation) dlqRecord {
	r := dlqRecord{
		SchemaVersion:  1,
		Stage:          stage,
		ErrorClass:     class,
		Violations:     make([]validation.Violation, 0, len(viols)), // [] not null
		Origin:         dlqOrigin{Kind: "s3", Bucket: s3.BucketRaw, ObjectKey: b.ObjectKey, LineNo: lineNo, BatchID: b.ID},
		OriginalBase64: base64.StdEncoding.EncodeToString(line),
		OccurredAt:     xtime.Format(b.ExportedAt),
	}
	for _, v := range viols {
		v.Message = truncate(v.Message)
		r.Violations = append(r.Violations, v)
	}
	var ids struct {
		EventID string `json:"event_id"`
	}
	if json.Unmarshal(line, &ids) == nil {
		if _, err := uuid.Parse(ids.EventID); err == nil {
			r.Origin.EventID = ids.EventID
		}
	}
	return r
}

// dlqObjectKey follows the DLQ layout: stage=publish/date=<date>/publish+batch+<%020d batch_id>.jsonl
func dlqObjectKey(now time.Time, batchID int64) string {
	return fmt.Sprintf("stage=%s/date=%s/publish+batch+%020d.jsonl", stage, xtime.FormatDate(now), batchID)
}

// writeDLQ validates each DLQ record against S3 and writes the valid ones as
// one write-once object. A record that fails S3 is logged at ERROR with the
// original bytes in base64 and left out. log already carries stage and batch.
func writeDLQ(ctx context.Context, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, b batch, recs []dlqRecord, originals [][]byte) error {
	var buf bytes.Buffer
	for i, r := range recs {
		line, err := json.Marshal(r)
		if err == nil {
			var viols []validation.Violation
			viols, err = reg.Validate(validation.IDDLQRecord, line)
			if err == nil && len(viols) > 0 {
				err = fmt.Errorf("dlq record violates schema: %s at %s", viols[0].Message, viols[0].InstanceLocation)
			}
		}
		if err != nil {
			log.Error("dlq record invalid; original logged",
				"line_no", r.Origin.LineNo, "err", err,
				"original_base64", base64.StdEncoding.EncodeToString(originals[i]))
			continue
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if buf.Len() == 0 {
		return nil
	}
	key := dlqObjectKey(b.ExportedAt, b.ID)
	created, err := s3c.PutIfAbsent(ctx, s3.BucketDLQ, key, buf.Bytes())
	switch {
	case errors.Is(err, s3.ErrChecksumMismatch):
		// DLQ lines are deterministic, so this only happens when the
		// validator's messages changed between attempts (a redeploy with a
		// new schema build). Keep the first object.
		m.S3Puts.WithLabelValues("mismatch").Inc()
		log.Warn("dlq object already exists from an earlier attempt", "key", key)
		return nil
	case err != nil:
		m.S3Puts.WithLabelValues("error").Inc()
		return fmt.Errorf("put dlq object %s: %w", key, err)
	case created:
		m.S3Puts.WithLabelValues("created").Inc()
	default:
		m.S3Puts.WithLabelValues("exists").Inc()
	}
	log.Error("records sent to dlq", "count", len(recs), "key", key)
	return nil
}

// PublishOnce leases one exported batch, publishes every valid line and marks
// the batch published. It returns published=false when nothing was leased.
// Any produce error returns before the mark: the lease expires and the batch
// is republished (the transformer drops the duplicates).
func PublishOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, topic string, lease time.Duration, m *obs.Metrics, log *slog.Logger) (published bool, err error) {
	b, ok, err := leaseBatch(ctx, pool, lease)
	if err != nil || !ok {
		return false, err
	}
	log = log.With("stage", stage, "batch", b.ID)

	body, err := s3c.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil {
		return false, fmt.Errorf("get raw object %s: %w", b.ObjectKey, err)
	}

	var (
		recs      []*kgo.Record
		dlq       []dlqRecord
		originals [][]byte
	)
	for i, line := range splitLines(body) {
		lineNo := i + 1
		viols, verr := reg.Validate(validation.IDRawEvent, line)
		if verr != nil {
			m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
			m.DLQ.WithLabelValues(stage, errClassDecode).Inc()
			dlq = append(dlq, newDLQRecord(b, lineNo, line, errClassDecode,
				[]validation.Violation{{InstanceLocation: "", KeywordLocation: "", Message: verr.Error()}}))
			originals = append(originals, line)
			continue
		}
		if len(viols) > 0 {
			m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
			for _, v := range viols {
				m.SchemaViolations.WithLabelValues(stage, schemaLabel, validation.Keyword(v)).Inc()
			}
			m.DLQ.WithLabelValues(stage, errClassSchema).Inc()
			dlq = append(dlq, newDLQRecord(b, lineNo, line, errClassSchema, viols))
			originals = append(originals, line)
			continue
		}
		m.SchemaValidations.WithLabelValues(stage, schemaLabel, "pass").Inc()

		ev, uerr := event.Unmarshal(line)
		if uerr != nil {
			m.DLQ.WithLabelValues(stage, errClassDecode).Inc()
			dlq = append(dlq, newDLQRecord(b, lineNo, line, errClassDecode,
				[]validation.Violation{{InstanceLocation: "", KeywordLocation: "", Message: uerr.Error()}}))
			originals = append(originals, line)
			continue
		}
		idem := ""
		if ev.IdempotencyKey != nil {
			idem = *ev.IdempotencyKey
		}
		recs = append(recs, &kgo.Record{
			Topic: topic,
			Key:   []byte(queue.MessageKey(ev)),
			Value: line, // verbatim bytes of the raw JSONL line
			Headers: []kgo.RecordHeader{
				{Key: queue.HeaderEventID, Value: []byte(ev.EventID)},
				{Key: queue.HeaderIdempotencyKey, Value: []byte(idem)},
				{Key: queue.HeaderSchema, Value: []byte(validation.IDRawEvent)},
				{Key: queue.HeaderRawObject, Value: []byte(b.ObjectKey)},
				{Key: queue.HeaderLineNo, Value: []byte(strconv.Itoa(lineNo))},
			},
		})
	}

	if len(dlq) > 0 {
		if err := writeDLQ(ctx, s3c, reg, m, log, b, dlq, originals); err != nil {
			return false, err
		}
	}

	if len(recs) > 0 {
		// ProduceSync produces every record (batched by the client) and waits
		// for all acks; FirstErr is the first per-record failure.
		if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			return false, fmt.Errorf("produce batch %d: %w", b.ID, err)
		}
		if err := prod.Flush(ctx); err != nil {
			return false, fmt.Errorf("flush batch %d: %w", b.ID, err)
		}
	}

	if err := markPublished(ctx, pool, b.ID); err != nil {
		return false, err
	}
	m.PublishedRecords.Add(float64(len(recs)))
	log.Info("batch published", "object_key", b.ObjectKey, "records", len(recs), "dlq", len(dlq))
	return true, nil
}

// Run publishes batches until ctx is cancelled. After an idle pass or an
// error it sleeps cfg.PublishPoll.
func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		published, err := PublishOnce(ctx, pool, s3c, prod, reg, cfg.Topic, cfg.PublishLease, m, log)
		if err != nil && ctx.Err() == nil {
			log.Error("publish failed", "stage", stage, "err", err)
		}
		if published && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.PublishPoll):
		}
	}
}
```

- [ ] **Step 4: Run the unit tests to confirm they pass**

Run: `go test ./internal/publisher/ -run 'TestSplitLines|TestDLQ' -v`
Expected: PASS for `TestSplitLines`, `TestDLQObjectKey`, `TestDLQRecordsSatisfySchema` (4 subtests) and `TestDLQRecordKeepsOriginal`. If `TestDLQRecordsSatisfySchema` fails, `dlq_record.v1.json` and `dlqRecord` have drifted apart (for example, `"publish"` is missing from the `stage` enum). Fix the schema, which is the contract, and do not weaken the test.

- [ ] **Step 5: Write the integration test**

Create `internal/publisher/publisher_integration_test.go`:

```go
package publisher

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/exporter"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

const itTopic = "coingecko.raw"

type env struct {
	pool    *pgxpool.Pool
	s3c     *s3.Client
	prod    *kgo.Client
	reg     *validation.Registry
	m       *obs.Metrics
	log     *slog.Logger
	brokers []string
}

func setup(t *testing.T, ctx context.Context) env {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	dsn := testutil.StartPostgres(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, _ := testutil.StartVersityGW(t)

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsurePartitions(ctx, pool, time.Now(), 1); err != nil {
		t.Fatal(err)
	}
	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatal(err)
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	prod, err := queue.NewProducer(brokers, "publisher-it", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	if err := queue.EnsureTopic(ctx, prod, itTopic, 6, 168*time.Hour); err != nil {
		t.Fatal(err)
	}
	return env{
		pool: pool, s3c: s3c, prod: prod, reg: reg,
		m:       obs.NewMetrics(prometheus.NewRegistry()),
		log:     slog.New(slog.NewTextHandler(t.Output(), nil)),
		brokers: brokers,
	}
}

// okEvent builds a successful RawEvent around a recorded CoinGecko response.
func okEvent(t *testing.T, endpoint string, req event.Request, fixture string) event.RawEvent {
	t.Helper()
	payload, err := os.ReadFile("../../schemas/testdata/coingecko/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        uuid.Must(uuid.NewV7()).String(),
		IdempotencyKey: &idem,
		Source:         event.SourceCoinGecko,
		Endpoint:       endpoint,
		Request:        req,
		FetchedAt:      xtime.Format(time.Now()),
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 120},
		PayloadSHA256:  &sha,
		Payload:        payload,
	}
}

// consumeAll reads the topic from the beginning until want records arrived
// (or the deadline), then waits briefly to catch unexpected extras.
func consumeAll(t *testing.T, ctx context.Context, brokers []string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(itTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	deadline := time.Now().Add(30 * time.Second)
	extra := time.Time{}
	for time.Now().Before(deadline) {
		if len(out) >= want {
			if extra.IsZero() {
				extra = time.Now().Add(2 * time.Second)
			} else if time.Now().After(extra) {
				break
			}
		}
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fs := cl.PollFetches(pctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func headers(r *kgo.Record) map[string]string {
	h := map[string]string{}
	for _, rh := range r.Headers {
		h[rh.Key] = string(rh.Value)
	}
	return h
}

func TestPublisherIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := setup(t, ctx)

	evs := []event.RawEvent{
		okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json"),
		okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t1.json"),
		okEvent(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json"),
	}
	for _, ev := range evs {
		if err := pg.InsertAPICall(ctx, e.pool, ev); err != nil {
			t.Fatal(err)
		}
	}
	n, err := exporter.ExportOnce(ctx, e.pool, e.s3c, e.reg, e.m, e.log, 500)
	if err != nil || n != len(evs) {
		t.Fatalf("ExportOnce = %d, %v; want %d", n, err, len(evs))
	}
	var batchID int64
	var objectKey string
	if err := e.pool.QueryRow(ctx, `SELECT batch_id, object_key FROM pipeline.export_batches ORDER BY batch_id DESC LIMIT 1`).Scan(&batchID, &objectKey); err != nil {
		t.Fatal(err)
	}
	body, err := e.s3c.Get(ctx, s3.BucketRaw, objectKey)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(body)
	if len(lines) != len(evs) {
		t.Fatalf("raw object has %d lines, want %d", len(lines), len(evs))
	}

	t.Run("publishes every line verbatim with headers", func(t *testing.T) {
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines))
		if len(recs) != len(lines) {
			t.Fatalf("consumed %d records, want %d", len(recs), len(lines))
		}
		for _, r := range recs {
			h := headers(r)
			lineNo, err := strconv.Atoi(h[queue.HeaderLineNo])
			if err != nil || lineNo < 1 || lineNo > len(lines) {
				t.Fatalf("bad line_no header %q", h[queue.HeaderLineNo])
			}
			if string(r.Value) != string(lines[lineNo-1]) {
				t.Fatalf("record value differs from raw line %d", lineNo)
			}
			ev, err := event.Unmarshal(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if string(r.Key) != queue.MessageKey(ev) {
				t.Fatalf("key = %q, want %q", r.Key, queue.MessageKey(ev))
			}
			if h[queue.HeaderEventID] != ev.EventID || h[queue.HeaderIdempotencyKey] != *ev.IdempotencyKey ||
				h[queue.HeaderSchema] != validation.IDRawEvent || h[queue.HeaderRawObject] != objectKey {
				t.Fatalf("headers = %v", h)
			}
		}
		var publishedAt *time.Time
		if err := e.pool.QueryRow(ctx, `SELECT published_at FROM pipeline.export_batches WHERE batch_id = $1`, batchID).Scan(&publishedAt); err != nil {
			t.Fatal(err)
		}
		if publishedAt == nil {
			t.Fatal("published_at not set")
		}
	})

	t.Run("second run publishes nothing", func(t *testing.T) {
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || published {
			t.Fatalf("PublishOnce = %v, %v; want false, nil", published, err)
		}
	})

	t.Run("expired lease republishes", func(t *testing.T) {
		ev := okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t2.json")
		if err := pg.InsertAPICall(ctx, e.pool, ev); err != nil {
			t.Fatal(err)
		}
		if n, err := exporter.ExportOnce(ctx, e.pool, e.s3c, e.reg, e.m, e.log, 500); err != nil || n != 1 {
			t.Fatalf("ExportOnce = %d, %v", n, err)
		}
		lease := time.Second
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		// Simulate a crash after the produce and before the mark: the batch
		// is unpublished and its lease is still held.
		if _, err := e.pool.Exec(ctx, `
UPDATE pipeline.export_batches SET published_at = NULL, publish_lease_until = now() + interval '1 second'
 WHERE batch_id = (SELECT max(batch_id) FROM pipeline.export_batches)`); err != nil {
			t.Fatal(err)
		}
		published, err = PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || published {
			t.Fatalf("PublishOnce while leased = %v, %v; want false, nil", published, err)
		}
		time.Sleep(1500 * time.Millisecond)
		published, err = PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce after lease expiry = %v, %v", published, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines)+2)
		copies := 0
		for _, r := range recs {
			if headers(r)[queue.HeaderEventID] == ev.EventID {
				copies++
			}
		}
		if copies != 2 {
			t.Fatalf("event %s published %d times, want 2 (republish duplicates)", ev.EventID, copies)
		}
	})
	// Runs last: it inserts a hand-made ledger row whose txid bounds are
	// not real, which would confuse a later ExportOnce cursor.
	t.Run("invalid line goes to dlq, valid line is published", func(t *testing.T) {
		good := okEvent(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json")
		goodLine, err := event.Marshal(good)
		if err != nil {
			t.Fatal(err)
		}
		bad := []byte(`{"schema_version":1,"event_id":"` + uuid.Must(uuid.NewV7()).String() + `"}`)
		obj := append(append(append(goodLine, '\n'), bad...), '\n')
		key := "source=coingecko/ingest_date=2026-10-10/batch=manual-dlq.jsonl"
		if _, err := e.s3c.PutIfAbsent(ctx, s3.BucketRaw, key, obj); err != nil {
			t.Fatal(err)
		}
		var id int64
		var exportedAt time.Time
		err = e.pool.QueryRow(ctx, `
INSERT INTO pipeline.export_batches (from_txid, from_seq, to_txid, to_seq, watermark_xmin, row_count, object_key, status, exported_at)
VALUES ('1000000', 0, '1000000', 1, '1000000', 2, $1, 'exported', now()) RETURNING batch_id, exported_at`, key).Scan(&id, &exportedAt)
		if err != nil {
			t.Fatal(err)
		}
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		dlqBody, err := e.s3c.Get(ctx, s3.BucketDLQ, dlqObjectKey(exportedAt, id))
		if err != nil {
			t.Fatalf("dlq object: %v", err)
		}
		dlqLines := splitLines(dlqBody)
		if len(dlqLines) != 1 {
			t.Fatalf("dlq has %d lines, want 1", len(dlqLines))
		}
		if viols, err := e.reg.Validate(validation.IDDLQRecord, dlqLines[0]); err != nil || len(viols) > 0 {
			t.Fatalf("dlq line invalid: %v %v", viols, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines)+2+1)
		found := 0
		for _, r := range recs {
			if headers(r)[queue.HeaderEventID] == good.EventID {
				found++
				if string(r.Value) != string(goodLine) {
					t.Fatal("valid line not published verbatim")
				}
			}
		}
		if found != 1 || len(recs) != len(lines)+2+1 {
			t.Fatalf("found=%d total=%d", found, len(recs))
		}
	})

}
```

What it checks:
1. Three `ok` API calls (two markets responses and one bitcoin detail) go through `ExportOnce` into one raw object. `PublishOnce` then produces exactly 3 messages. Each value equals the raw line byte for byte (matched through the `line_no` header). Key and headers (`event_id`, `idempotency_key`, `schema`, `raw_object`, `line_no`) are correct, and `published_at` is set.
2. A second `PublishOnce` returns `false` (nothing left to lease).
3. Crash simulation with a 1 s lease. The batch is published, then reset to "produced but not marked" (`published_at = NULL`, lease still held). A `PublishOnce` while the lease is held publishes nothing. After the lease expires, the batch is republished, and the topic holds the event twice, which is expected because the transformer deduplicates.
4. A hand-made raw object with one valid line and one S1-invalid line produces exactly one message, plus one DLQ line that passes `dlq_record.v1.json`. This subtest runs last because its ledger row has made-up xid8 bounds.

- [ ] **Step 6: Run the integration test**

Run: `go test ./internal/publisher/ -run TestPublisherIntegration -v -count=1`
Expected: PASS for all 4 subtests (about 15–60 s, depending on image cache). The log shows `batch published … records=3`, the same batch republished after lease expiry, and an ERROR line `records sent to dlq … key="stage=publish/date=…/publish+batch+…jsonl"`. Without Docker the test is skipped.

- [ ] **Step 7: Full package run with the race detector, vet, and commit**

```bash
go vet ./internal/publisher/ ./internal/queue/
go test -race ./internal/publisher/ ./internal/queue/ -count=1
git add internal/publisher/
git commit -m "feat(publisher): lease exported batches, validate S1, produce raw lines verbatim

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

CONTRACT NOTES

- No contract name or signature changed. Exported identifiers added to `internal/transform` beyond the contract (additive only): dataset constants (`DatasetMarketSnapshot`, `DatasetCoinSnapshot`, `DatasetCoinMarketByCurrency`, `DatasetTicker`, `DatasetCoinContract`), reject-reason constants (`ReasonMissingID`, `ReasonNullPrice`, `ReasonNonpositivePrice`, `ReasonEmptyPlatform`, `ReasonEmptyRecord`), Business structs (`MarketSnapshot`, `CoinSnapshot`, `CoinMarketByCurrency`, `Ticker`, `CoinContract`), `ContentSHA256(business any) ([32]byte, error)`, `(Dec) Decimal()`, `(*Dec) UnmarshalJSON`. Consumers may use the constants instead of string literals.
- The processed schemas `$ref` `../common/defs.v1.json#/$defs/decimal_string`, `#/$defs/utc_timestamp` and `#/$defs/uuid` (the `$id` is `https://schemas.cg-etl.internal/common/defs.v1.json`). `decimal_string` must accept negative values and plain decimals as printed by shopspring `Decimal.String()` (`"-34.3464"`, `"0.00004"`, `"1663572458130"`). `utc_timestamp` must accept `2026-10-10T10:07:55.000Z`. Task 3's `defs.v1.json` (`^-?(0|[1-9]\d*)(\.\d+)?$` and `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`) accepts both; the reviewer re-ran every Task 12/13 test against it.
- Task 12 adds the line `//go:embed processed/*.json` to `schemas/embed.go` (Task 3 embeds `common/*.json` and `pipeline/*.json`, Task 4 adds `source/coingecko/*.json`). The step shows the full file. `processed/` cannot be embedded before Task 12, because a pattern that matches no file is a compile error.
- Ticker rows: `coin_id` holds the parent document id (`/coins/{id}`). This keeps the child → parent reference `(coin_id, snapshot_at)` and the natural key `coin|exchange|base|target|snapshot_at` intact. Upstream `tickers[].coin_id` is kept as `base_coin_id` (it is `ethereum` for an ETH/BTC ticker inside the bitcoin document), and `target_coin_id` is kept as is.
- `Record.Ordinal` runs from 0 across every record a message produces, in assignment order. Detail order is coin_snapshot, currencies (sorted), tickers (payload order), contracts (sorted by platform). Records that are rejected with `missing_id` have no id to key on, so their `Key` is `#<index>|<snapshot_at>`.
- Rejected records carry the same Business struct as processed ones (normalized, flags computed), so the rejected writer can embed `Business` unchanged.
- `Transform` returns an error (the message goes to dlq) when the payload has no usable `last_updated`, including an empty markets array `[]`. It also errors on a malformed timestamp or a non-numeric number.

### Task 12: Transform — markets + processed schemas foundation

**Files:**
- Create: `internal/transform/types.go`, `internal/transform/normalize.go`, `internal/transform/hash.go`, `internal/transform/markets.go`, `internal/transform/transform.go`
- Create: `schemas/processed/market_snapshot.v1.json`
- Modify: `schemas/embed.go` (full new content below), `go.mod`, `go.sum`
- Test: `internal/transform/normalize_test.go`, `internal/transform/helpers_test.go`, `internal/transform/markets_test.go` (fixtures `schemas/testdata/coingecko/markets_usd250_t0.json`, `markets_top10_t0.json`, `markets_top10_t1.json`, `markets_top10_t2.json` from Task 4)

**Interfaces:**
- Consumes:
  ```go
  // internal/event
  const (SchemaVersion = 1; SourceCoinGecko = "coingecko"; EndpointMarkets = "coins_markets"; EndpointCoinDetail = "coin_detail"; OutcomeOK = "ok")
  type Request struct { Path string `json:"path"`; Params map[string]string `json:"params"` }
  type RawEvent struct { SchemaVersion int; EventID string; IdempotencyKey *string; Source, Endpoint string; Request Request; FetchedAt, Outcome string; HTTP *HTTPMeta; Error, PayloadSHA256 *string; Payload json.RawMessage }
  // internal/xtime
  func Format(t time.Time) string
  func Parse(s string) (time.Time, error)
  func FormatDate(t time.Time) string
  // internal/validation
  func Load(fsys fs.FS) (*Registry, error)
  func (r *Registry) Validate(schemaID string, doc []byte) ([]Violation, error)
  const IDMarketSnapshot, IDCoinSnapshot, IDCoinMarketByCurrency, IDTicker, IDCoinContract
  // package schemas
  var FS embed.FS
  ```
- Produces:
  ```go
  // internal/transform  (pure; no I/O, no clock)
  type Zone string
  const (ZoneProcessed Zone = "processed"; ZoneRejected Zone = "rejected")
  type Rules struct { StaleAfter, TickerStaleAfter time.Duration; TickerAnomalyPct float64 }
  type Lineage struct {
      EventID      string `json:"_event_id"`
      RawObject    string `json:"_raw_object"`
      Partition    int32  `json:"_kafka_partition"`
      Offset       int64  `json:"_kafka_offset"`
      SchemaVer    int    `json:"_schema_version"`
      ProcessedAt  string `json:"_processed_at"`
  }
  type Record struct {
      Dataset       string
      Key           string
      EventDate     string
      Ordinal       int
      Zone          Zone
      Reason        string
      ContentSHA256 [32]byte
      Business      any
  }
  func Transform(ev event.RawEvent, rules Rules) ([]Record, error)
  func SnapshotAt(ev event.RawEvent) (time.Time, error)
  func RowJSON(r Record, lin Lineage) ([]byte, error)
  func SchemaIDFor(dataset string) string
  type Dec string
  func DecFromNumber(n json.Number) (Dec, error)
  // additive: Dataset*/Reason* constants, MarketSnapshot, ContentSHA256(business any) ([32]byte, error)
  ```

Background for this task:
- CoinGecko `/coins/markets` returns a JSON array with one object per coin. Prices can be tiny (`4e-05`, which is exponent notation) or larger than 2^53. The spec requires money as plain decimal strings, never float64. Decoding into `json.Number` fields keeps the exact literal text, and `github.com/shopspring/decimal` converts it to plain notation (`decimal.NewFromString("4e-05").String() == "0.00004"`; `String()` also drops trailing zeros: `"20096053.0"` → `"20096053"`). Percentages and ratios stay JSON numbers (`*float64`).
- `snapshot_at` for a markets response is the **max** `last_updated` over all rows. CoinGecko recomputes all coins in one pass and stamps them with the same time; stale coins lag (in the fixture, `figure-heloc` and `a7a5` lag by more than 1.5 h).
- The content hash covers the Business struct only. Lineage (`_event_id`, offsets, `_processed_at`) and `fetched_at` are never part of it. Two polls served the same cached response therefore produce equal hashes, and dedup (a later task) drops the copy.
- `RowJSON` builds the row by splicing two `json.Marshal` outputs: business fields in struct order, then lineage fields. Go's `encoding/json` writes struct fields in declaration order, so the bytes are deterministic.

- [ ] **Step 1: Add the decimal dependency**

```bash
go get github.com/shopspring/decimal@v1.5.0
```

Expected: `go.mod` gains `github.com/shopspring/decimal v1.5.0` (it is marked `// indirect` until code imports it; `go mod tidy` in Step 11 fixes that).

- [ ] **Step 2: Write the failing normalization tests**

Create `internal/transform/normalize_test.go`:

```go
package transform

import (
	"encoding/json"
	"testing"
)

func TestDecFromNumber(t *testing.T) {
	cases := []struct {
		in      json.Number
		want    Dec
		wantErr bool
	}{
		{"", "", false},
		{"82776", "82776", false},
		{"20096053.0", "20096053", false},
		{"4e-05", "0.00004", false},
		{"1.5E+3", "1500", false},
		{"-34.3464", "-34.3464", false},
		{"43083803721443304123", "43083803721443304123", false},
		{"0", "0", false},
		{"abc", "", true},
	}
	for _, c := range cases {
		got, err := DecFromNumber(c.in)
		if (err != nil) != c.wantErr {
			t.Fatalf("DecFromNumber(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
		}
		if got != c.want {
			t.Errorf("DecFromNumber(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDecJSON(t *testing.T) {
	b, err := json.Marshal(struct {
		A Dec `json:"a"`
		B Dec `json:"b"`
	}{A: "1.5"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":"1.5","b":null}` {
		t.Fatalf("got %s", b)
	}
}

func TestNormalizeHelpers(t *testing.T) {
	s := func(v string) *string { return &v }
	if nilIfEmpty(s("")) != nil || nilIfEmpty(nil) != nil || *nilIfEmpty(s("x")) != "x" {
		t.Error("nilIfEmpty")
	}
	if got := *upperSymbol(s("btc")); got != "BTC" {
		t.Errorf("upperSymbol = %q", got)
	}
	if got := nonEmptyStrings([]string{}); got != nil {
		t.Errorf("nonEmptyStrings([]) = %#v, want nil", got)
	}
	if got := nonEmptyStrings([]string{"", "Layer 1"}); len(got) != 1 || got[0] != "Layer 1" {
		t.Errorf("nonEmptyStrings = %#v", got)
	}
	if got := normAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"); got != "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" {
		t.Errorf("normAddress EVM = %q", got)
	}
	if got := normAddress("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"); got != "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
		t.Errorf("normAddress non-EVM changed: %q", got)
	}
	if got := normAsset("0X0B7C6C138DC75622F4200D157B13FB93E6BD94FA"); got != "0x0b7c6c138dc75622f4200d157b13fb93e6bd94fa" {
		t.Errorf("normAsset EVM = %q", got)
	}
	if got := normAsset("thor.rune"); got != "THOR.RUNE" {
		t.Errorf("normAsset symbol = %q", got)
	}
	got, err := normTime(s("2026-10-10T10:00:45+00:00"))
	if err != nil || *got != "2026-10-10T10:00:45.000Z" {
		t.Errorf("normTime = %v, %v", got, err)
	}
	if got, err := normTime(s("")); got != nil || err != nil {
		t.Errorf("normTime(\"\") = %v, %v", got, err)
	}
	if _, err := normTime(s("yesterday")); err == nil {
		t.Error("normTime accepted garbage")
	}
}
```

- [ ] **Step 3: Run the tests and watch them fail**

```bash
go test ./internal/transform/ -run 'TestDec|TestNormalize'
```

Expected: build failure, e.g. `undefined: DecFromNumber`, `undefined: Dec`, `undefined: nilIfEmpty`.

- [ ] **Step 4: Write types.go and normalize.go**

Create `internal/transform/types.go`:

```go
// Package transform holds the pure CoinGecko → processed-record transforms.
// Nothing in this package performs I/O or reads the clock: the same RawEvent
// and Rules always yield byte-identical records.
package transform

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// Zone is where a record is written.
type Zone string

const (
	ZoneProcessed Zone = "processed"
	ZoneRejected  Zone = "rejected"
)

// Dataset names (also the dataset=<name> object-key segment).
const (
	DatasetMarketSnapshot       = "market_snapshot"
	DatasetCoinSnapshot         = "coin_snapshot"
	DatasetCoinMarketByCurrency = "coin_market_by_currency"
	DatasetTicker               = "ticker"
	DatasetCoinContract         = "coin_contract"
)

// Reject reasons produced by the transforms (snapshot_conflict is decided by dedup).
const (
	ReasonMissingID        = "missing_id"
	ReasonNullPrice        = "null_price"
	ReasonNonpositivePrice = "nonpositive_price"
	ReasonEmptyPlatform    = "empty_platform"
	ReasonEmptyRecord      = "empty_record"
)

// Rules are the configurable cleaning thresholds (config.StaleAfter etc.).
type Rules struct {
	StaleAfter, TickerStaleAfter time.Duration
	TickerAnomalyPct             float64
}

// Lineage holds the technical columns appended to every written row.
// They are never part of Business, so they never enter the content hash.
type Lineage struct {
	EventID     string `json:"_event_id"`
	RawObject   string `json:"_raw_object"`
	Partition   int32  `json:"_kafka_partition"`
	Offset      int64  `json:"_kafka_offset"`
	SchemaVer   int    `json:"_schema_version"`
	ProcessedAt string `json:"_processed_at"`
}

// Record is one dataset row produced from one message.
type Record struct {
	Dataset       string // "market_snapshot" | "coin_snapshot" | "coin_market_by_currency" | "ticker" | "coin_contract"
	Key           string // natural key, fields joined by "|"
	EventDate     string // FormatDate(snapshot_at)
	Ordinal       int    // index within the message, assignment order
	Zone          Zone
	Reason        string   // reject reason when Zone == ZoneRejected
	ContentSHA256 [32]byte // over business fields only
	Business      any      // business-field struct (hashed)
}

// Dec is a decimal number serialized as a JSON string in plain notation
// ("0.00004", never "4e-05"). The empty Dec is JSON null.
type Dec string

// MarshalJSON writes null for the empty Dec and a JSON string otherwise.
func (d Dec) MarshalJSON() ([]byte, error) {
	if d == "" {
		return []byte("null"), nil
	}
	return json.Marshal(string(d))
}

// UnmarshalJSON accepts null or a JSON string (used by tests and readers).
func (d *Dec) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*d = ""
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("transform: Dec: %w", err)
	}
	*d = Dec(s)
	return nil
}

// Decimal converts a non-empty Dec to a shopspring decimal.
func (d Dec) Decimal() (decimal.Decimal, bool) {
	if d == "" {
		return decimal.Decimal{}, false
	}
	v, err := decimal.NewFromString(string(d))
	if err != nil {
		return decimal.Decimal{}, false
	}
	return v, true
}

// DecFromNumber converts a JSON number literal (exact text, e.g. from
// json.Decoder.UseNumber) into a plain-notation decimal string without ever
// going through float64. An empty json.Number (JSON null) yields "".
func DecFromNumber(n json.Number) (Dec, error) {
	if n == "" {
		return "", nil
	}
	v, err := decimal.NewFromString(string(n))
	if err != nil {
		return "", fmt.Errorf("transform: not a decimal number %q: %w", string(n), err)
	}
	// String() prints plain notation and drops trailing zeros: "20096053.0" → "20096053".
	return Dec(v.String()), nil
}

// MarketSnapshot is one market_snapshot row (one coin from /coins/markets).
// Field order is the serialized column order. Only business fields: no
// lineage, no fetched_at.
type MarketSnapshot struct {
	CoinID                string   `json:"coin_id"`
	SnapshotAt            string   `json:"snapshot_at"`
	Symbol                *string  `json:"symbol"`
	Name                  *string  `json:"name"`
	VsCurrency            string   `json:"vs_currency"`
	MarketCapRank         *int64   `json:"market_cap_rank"`
	Price                 Dec      `json:"price"`
	MarketCap             Dec      `json:"market_cap"`
	FDV                   Dec      `json:"fdv"`
	TotalVolume           Dec      `json:"total_volume"`
	High24h               Dec      `json:"high_24h"`
	Low24h                Dec      `json:"low_24h"`
	PriceChange24h        Dec      `json:"price_change_24h"`
	PriceChangePct1h      *float64 `json:"price_change_pct_1h"`
	PriceChangePct24h     *float64 `json:"price_change_pct_24h"`
	PriceChangePct7d      *float64 `json:"price_change_pct_7d"`
	MarketCapChange24h    Dec      `json:"market_cap_change_24h"`
	MarketCapChangePct24h *float64 `json:"market_cap_change_pct_24h"`
	CirculatingSupply     Dec      `json:"circulating_supply"`
	TotalSupply           Dec      `json:"total_supply"`
	MaxSupply             Dec      `json:"max_supply"`
	ATH                   Dec      `json:"ath"`
	ATHChangePct          *float64 `json:"ath_change_pct"`
	ATHAt                 *string  `json:"ath_at"`
	ATL                   Dec      `json:"atl"`
	ATLChangePct          *float64 `json:"atl_change_pct"`
	ATLAt                 *string  `json:"atl_at"`
	ROITimes              *float64 `json:"roi_times"`
	ROICurrency           *string  `json:"roi_currency"`
	ROIPct                *float64 `json:"roi_pct"`
	SourceUpdatedAt       *string  `json:"source_updated_at"`
	IsStale               bool     `json:"is_stale"`
	IsInactive            bool     `json:"is_inactive"`
	IsAnomaly             bool     `json:"is_anomaly"`
}
```

Create `internal/transform/normalize.go`. The EVM regexp is case-insensitive (`(?i)`), so `0X…` ticker bases from CoinGecko are matched too. Non-EVM addresses (Solana, Tron, Stellar) are case-sensitive and are never changed. `decErr` lets a long struct literal call `e.dec(...)`, `e.pct(...)`, `e.ts(...)` and then check one error at the end:

```go
package transform

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/xtime"
)

// evmAddress matches an EVM address in any letter case ("0x…" or "0X…").
var evmAddress = regexp.MustCompile(`(?i)^0x[0-9a-f]{40}$`)

// nilIfEmpty maps a missing or "" string to nil (CoinGecko encodes "absent"
// as null, "" or a missing key).
func nilIfEmpty(s *string) *string {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	v := *s
	return &v
}

// strOrEmpty dereferences s, mapping nil to "".
func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nonEmptyStrings drops "" elements and maps an empty result to nil
// (serialized as JSON null, never []).
func nonEmptyStrings(in []string) []string {
	var out []string
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

// upperSymbol upper-cases a ticker symbol; nil/"" → nil.
func upperSymbol(s *string) *string {
	s = nilIfEmpty(s)
	if s == nil {
		return nil
	}
	v := strings.ToUpper(strings.TrimSpace(*s))
	return &v
}

// normAddress lower-cases EVM addresses and leaves every other address
// format untouched (Solana/Tron/Stellar addresses are case-sensitive).
func normAddress(s string) string {
	s = strings.TrimSpace(s)
	if evmAddress.MatchString(s) {
		return strings.ToLower(s)
	}
	return s
}

// normAsset normalizes a ticker base/target: EVM address → lower case,
// anything else is a symbol → upper case.
func normAsset(s string) string {
	s = strings.TrimSpace(s)
	if evmAddress.MatchString(s) {
		return strings.ToLower(s)
	}
	return strings.ToUpper(s)
}

// normTime parses any CoinGecko timestamp and re-formats it as RFC 3339 UTC
// with milliseconds; nil/"" → nil.
func normTime(s *string) (*string, error) {
	s = nilIfEmpty(s)
	if s == nil {
		return nil, nil
	}
	t, err := xtime.Parse(*s)
	if err != nil {
		return nil, fmt.Errorf("transform: bad timestamp %q: %w", *s, err)
	}
	v := xtime.Format(t)
	return &v, nil
}

// parseTime parses a non-empty timestamp; nil/"" → zero time and ok=false.
func parseTime(s *string) (time.Time, bool, error) {
	s = nilIfEmpty(s)
	if s == nil {
		return time.Time{}, false, nil
	}
	t, err := xtime.Parse(*s)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("transform: bad timestamp %q: %w", *s, err)
	}
	return t, true, nil
}

// pct converts a JSON number to a float64 (percentages and ratios are JSON
// numbers in the processed zone); "" (JSON null) → nil.
func pct(n json.Number) (*float64, error) {
	if n == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return nil, fmt.Errorf("transform: not a number %q: %w", string(n), err)
	}
	return &f, nil
}

// intOrNil converts a JSON integer to *int64; "" → nil. "5.0" is accepted.
func intOrNil(n json.Number) (*int64, error) {
	if n == "" {
		return nil, nil
	}
	if i, err := strconv.ParseInt(string(n), 10, 64); err == nil {
		return &i, nil
	}
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil || f != float64(int64(f)) {
		return nil, fmt.Errorf("transform: not an integer %q", string(n))
	}
	i := int64(f)
	return &i, nil
}

// decErr collects the first DecFromNumber/pct error so long field lists stay readable.
type decErr struct{ err error }

func (e *decErr) dec(n json.Number) Dec {
	d, err := DecFromNumber(n)
	if err != nil && e.err == nil {
		e.err = err
	}
	return d
}

func (e *decErr) pct(n json.Number) *float64 {
	f, err := pct(n)
	if err != nil && e.err == nil {
		e.err = err
	}
	return f
}

func (e *decErr) ts(s *string) *string {
	t, err := normTime(s)
	if err != nil && e.err == nil {
		e.err = err
	}
	return t
}
```

- [ ] **Step 5: Run the normalization tests**

```bash
go test ./internal/transform/ -run 'TestDec|TestNormalize' -v
```

Expected: `PASS` for `TestDecFromNumber`, `TestDecJSON`, `TestNormalizeHelpers`.

- [ ] **Step 6: Write the failing markets tests**

Create `internal/transform/helpers_test.go` (shared by Task 13):

```go
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
		RawObject:   "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl",
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
```

Create `internal/transform/markets_test.go`. Expected values come from the recorded fixture `markets_usd250_t0.json`: 250 rows; every coin was recomputed at `2026-10-10T10:07:55.000Z` except `figure-heloc` (07:55:52) and `a7a5` (08:33:48), which are stale; 14 rows have `total_volume: 0`; 3 rows (`shuffle-2`, `sonic-3`, `ultima`) have a price outside `[low_24h, high_24h]`; `roi` is non-null in 27 rows; no row has a null price. `markets_top10_t0.json` and `markets_top10_t1.json` are byte-identical (served from the CDN cache):

```go
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

func TestMarketsROIFlattened(t *testing.T) {
	payload := []byte(`[{"id":"ethereum","symbol":"eth","name":"Ethereum","current_price":2000,"total_volume":1,
	  "roi":{"times":42.012142715693386,"currency":"btc","percentage":4201.214271569338},
	  "last_updated":"2026-10-10T10:07:55.000Z"}]`)
	recs := mustTransform(t, rawEvent(event.EndpointMarkets, nil, payload))
	m := recs[0].Business.(MarketSnapshot)
	if m.ROITimes == nil || *m.ROITimes != 42.012142715693386 || *m.ROICurrency != "BTC" || *m.ROIPct != 4201.214271569338 {
		t.Fatalf("roi = %v %v %v", m.ROITimes, m.ROICurrency, m.ROIPct)
	}
	if m.VsCurrency != "usd" {
		t.Fatalf("vs_currency default = %q", m.VsCurrency)
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
		{ZoneRejected, ReasonMissingID, "#0|2026-10-10T10:07:55.000Z"},
		{ZoneRejected, ReasonMissingID, "#1|2026-10-10T10:07:55.000Z"},
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
```

- [ ] **Step 7: Run the markets tests and watch them fail**

```bash
go test ./internal/transform/
```

Expected: build failure, e.g. `undefined: Transform`, `undefined: RowJSON`, `undefined: MarketSnapshot`, `undefined: SchemaIDFor`, `undefined: SnapshotAt`.

- [ ] **Step 8: Write hash.go, markets.go and transform.go**

Create `internal/transform/hash.go`:

```go
package transform

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// ContentSHA256 hashes the canonical JSON of a Business struct. encoding/json
// writes struct fields in declaration order and Dec/float64 values in one
// canonical form, so equal business content always hashes equally. Business
// structs contain no technical columns (_event_id, _kafka_offset, fetched_at, …),
// so the same coin snapshot arriving in two different messages hashes the same.
func ContentSHA256(business any) ([32]byte, error) {
	b, err := json.Marshal(business)
	if err != nil {
		return [32]byte{}, fmt.Errorf("transform: hash: %w", err)
	}
	return sha256.Sum256(b), nil
}
```

Create `internal/transform/markets.go`. Notes on decoding: a `json.Number` field receives the literal text of a number, and JSON `null` leaves it as `""`, which every helper treats as null. Unknown fields (`image`, …) are dropped by decoding; that is the "drop unneeded fields" step. `price_change_percentage_24h_in_currency` duplicates `price_change_percentage_24h`, so it is used only as a fallback:

```go
package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"
)

// marketsRow is one element of the /coins/markets array. Numbers are
// json.Number so money keeps its exact text; JSON null leaves "" / nil.
// Fields not listed (image, …) are dropped by decoding.
type marketsRow struct {
	ID                           *string     `json:"id"`
	Symbol                       *string     `json:"symbol"`
	Name                         *string     `json:"name"`
	CurrentPrice                 json.Number `json:"current_price"`
	MarketCap                    json.Number `json:"market_cap"`
	MarketCapRank                json.Number `json:"market_cap_rank"`
	FullyDilutedValuation        json.Number `json:"fully_diluted_valuation"`
	TotalVolume                  json.Number `json:"total_volume"`
	High24h                      json.Number `json:"high_24h"`
	Low24h                       json.Number `json:"low_24h"`
	PriceChange24h               json.Number `json:"price_change_24h"`
	PriceChangePercentage24h     json.Number `json:"price_change_percentage_24h"`
	MarketCapChange24h           json.Number `json:"market_cap_change_24h"`
	MarketCapChangePercentage24h json.Number `json:"market_cap_change_percentage_24h"`
	CirculatingSupply            json.Number `json:"circulating_supply"`
	TotalSupply                  json.Number `json:"total_supply"`
	MaxSupply                    json.Number `json:"max_supply"`
	ATH                          json.Number `json:"ath"`
	ATHChangePercentage          json.Number `json:"ath_change_percentage"`
	ATHDate                      *string     `json:"ath_date"`
	ATL                          json.Number `json:"atl"`
	ATLChangePercentage          json.Number `json:"atl_change_percentage"`
	ATLDate                      *string     `json:"atl_date"`
	ROI                          *struct {
		Times      json.Number `json:"times"`
		Currency   *string     `json:"currency"`
		Percentage json.Number `json:"percentage"`
	} `json:"roi"`
	LastUpdated         *string     `json:"last_updated"`
	PriceChangePct1hIn  json.Number `json:"price_change_percentage_1h_in_currency"`
	PriceChangePct24hIn json.Number `json:"price_change_percentage_24h_in_currency"`
	PriceChangePct7dIn  json.Number `json:"price_change_percentage_7d_in_currency"`
}

// decodeJSON decodes b into v with UseNumber (numbers never pass through
// float64) and rejects trailing data.
func decodeJSON(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("trailing data after JSON value")
	}
	return nil
}

func decodeMarkets(payload []byte) ([]marketsRow, error) {
	var rows []marketsRow
	if err := decodeJSON(payload, &rows); err != nil {
		return nil, fmt.Errorf("transform: decode coins_markets payload: %w", err)
	}
	return rows, nil
}

// marketsSnapshotAt is the max last_updated over all rows: CoinGecko stamps
// every coin recomputed in one pass with the same time, stale coins lag.
func marketsSnapshotAt(rows []marketsRow) (time.Time, error) {
	var max time.Time
	found := false
	for _, r := range rows {
		t, ok, err := parseTime(r.LastUpdated)
		if err != nil {
			return time.Time{}, err
		}
		if ok && (!found || t.After(max)) {
			max, found = t, true
		}
	}
	if !found {
		return time.Time{}, fmt.Errorf("transform: coins_markets payload has no last_updated")
	}
	return max, nil
}

func transformMarkets(ev event.RawEvent, rules Rules) ([]Record, error) {
	rows, err := decodeMarkets(ev.Payload)
	if err != nil {
		return nil, err
	}
	snap, err := marketsSnapshotAt(rows)
	if err != nil {
		return nil, err
	}
	snapAt := xtime.Format(snap)
	eventDate := xtime.FormatDate(snap)
	vs := strings.ToLower(strings.TrimSpace(ev.Request.Params["vs_currency"]))
	if vs == "" {
		vs = "usd"
	}

	out := make([]Record, 0, len(rows))
	for i, r := range rows {
		b, err := marketSnapshotFrom(r, snap, snapAt, vs, rules)
		if err != nil {
			return nil, fmt.Errorf("transform: coins_markets row %d: %w", i, err)
		}
		rec := Record{
			Dataset:   DatasetMarketSnapshot,
			Key:       b.CoinID + "|" + snapAt,
			EventDate: eventDate,
			Ordinal:   i,
			Zone:      ZoneProcessed,
			Business:  b,
		}
		switch {
		case b.CoinID == "":
			rec.Zone, rec.Reason = ZoneRejected, ReasonMissingID
			// No coin id to key on: the row's position in the payload keeps the key unique.
			rec.Key = "#" + strconv.Itoa(i) + "|" + snapAt
		case b.Price == "":
			rec.Zone, rec.Reason = ZoneRejected, ReasonNullPrice
		case !positive(b.Price):
			rec.Zone, rec.Reason = ZoneRejected, ReasonNonpositivePrice
		}
		if rec.ContentSHA256, err = ContentSHA256(b); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func marketSnapshotFrom(r marketsRow, snap time.Time, snapAt, vs string, rules Rules) (MarketSnapshot, error) {
	var e decErr
	m := MarketSnapshot{
		CoinID:                strings.TrimSpace(strOrEmpty(r.ID)),
		SnapshotAt:            snapAt,
		Symbol:                upperSymbol(r.Symbol),
		Name:                  nilIfEmpty(r.Name),
		VsCurrency:            vs,
		Price:                 e.dec(r.CurrentPrice),
		MarketCap:             e.dec(r.MarketCap),
		FDV:                   e.dec(r.FullyDilutedValuation),
		TotalVolume:           e.dec(r.TotalVolume),
		High24h:               e.dec(r.High24h),
		Low24h:                e.dec(r.Low24h),
		PriceChange24h:        e.dec(r.PriceChange24h),
		PriceChangePct1h:      e.pct(r.PriceChangePct1hIn),
		PriceChangePct24h:     e.pct(r.PriceChangePercentage24h),
		PriceChangePct7d:      e.pct(r.PriceChangePct7dIn),
		MarketCapChange24h:    e.dec(r.MarketCapChange24h),
		MarketCapChangePct24h: e.pct(r.MarketCapChangePercentage24h),
		CirculatingSupply:     e.dec(r.CirculatingSupply),
		TotalSupply:           e.dec(r.TotalSupply),
		MaxSupply:             e.dec(r.MaxSupply),
		ATH:                   e.dec(r.ATH),
		ATHChangePct:          e.pct(r.ATHChangePercentage),
		ATHAt:                 e.ts(r.ATHDate),
		ATL:                   e.dec(r.ATL),
		ATLChangePct:          e.pct(r.ATLChangePercentage),
		ATLAt:                 e.ts(r.ATLDate),
		SourceUpdatedAt:       e.ts(r.LastUpdated),
	}
	if m.PriceChangePct24h == nil {
		// The *_in_currency duplicate is the fallback when the plain field is null.
		m.PriceChangePct24h = e.pct(r.PriceChangePct24hIn)
	}
	if r.ROI != nil {
		m.ROITimes = e.pct(r.ROI.Times)
		m.ROICurrency = upperSymbol(r.ROI.Currency)
		m.ROIPct = e.pct(r.ROI.Percentage)
	}
	rank, err := intOrNil(r.MarketCapRank)
	if err != nil {
		return MarketSnapshot{}, err
	}
	m.MarketCapRank = rank
	if e.err != nil {
		return MarketSnapshot{}, e.err
	}

	if t, ok, _ := parseTime(r.LastUpdated); ok {
		m.IsStale = snap.Sub(t) > rules.StaleAfter
	}
	m.IsInactive = isZero(m.TotalVolume)
	m.IsAnomaly = outsideRange(m.Price, m.Low24h, m.High24h)
	return m, nil
}

// positive reports whether d is a decimal > 0.
func positive(d Dec) bool {
	v, ok := d.Decimal()
	return ok && v.IsPositive()
}

// isZero reports whether d is present and equal to 0.
func isZero(d Dec) bool {
	v, ok := d.Decimal()
	return ok && v.IsZero()
}

// outsideRange reports price ∉ [low, high]; false unless all three are present.
func outsideRange(price, low, high Dec) bool {
	p, okP := price.Decimal()
	l, okL := low.Decimal()
	h, okH := high.Decimal()
	if !okP || !okL || !okH {
		return false
	}
	return p.LessThan(l) || p.GreaterThan(h)
}
```

Create `internal/transform/transform.go`. The `coin_detail` branch returns an error until Task 13 replaces exactly the line `return nil, fmt.Errorf("transform: endpoint %q not implemented", ev.Endpoint)` with `return transformDetail(ev, rules)`. `SnapshotAt` already handles both endpoints:

```go
package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/validation"
)

// Transform turns one raw event into dataset records. It is pure: the result
// depends only on ev and rules. An error means the payload is not decodable
// (the caller sends the message to dlq/).
func Transform(ev event.RawEvent, rules Rules) ([]Record, error) {
	switch ev.Endpoint {
	case event.EndpointMarkets:
		return transformMarkets(ev, rules)
	case event.EndpointCoinDetail:
		return nil, fmt.Errorf("transform: endpoint %q not implemented", ev.Endpoint)
	default:
		return nil, fmt.Errorf("transform: unknown endpoint %q", ev.Endpoint)
	}
}

// SnapshotAt is the event time of a raw event: max last_updated of a markets
// response, the document's last_updated for /coins/{id}.
func SnapshotAt(ev event.RawEvent) (time.Time, error) {
	switch ev.Endpoint {
	case event.EndpointMarkets:
		rows, err := decodeMarkets(ev.Payload)
		if err != nil {
			return time.Time{}, err
		}
		return marketsSnapshotAt(rows)
	case event.EndpointCoinDetail:
		var doc struct {
			LastUpdated *string `json:"last_updated"`
		}
		if err := decodeJSON(ev.Payload, &doc); err != nil {
			return time.Time{}, fmt.Errorf("transform: decode coin_detail payload: %w", err)
		}
		t, ok, err := parseTime(doc.LastUpdated)
		if err != nil {
			return time.Time{}, err
		}
		if !ok {
			return time.Time{}, fmt.Errorf("transform: coin_detail payload has no last_updated")
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("transform: unknown endpoint %q", ev.Endpoint)
	}
}

// RowJSON serializes one row: the Business fields in struct order followed by
// the lineage fields (_event_id … _processed_at), as a single line without a
// trailing newline.
func RowJSON(r Record, lin Lineage) ([]byte, error) {
	biz, err := json.Marshal(r.Business)
	if err != nil {
		return nil, fmt.Errorf("transform: marshal %s: %w", r.Dataset, err)
	}
	tech, err := json.Marshal(lin)
	if err != nil {
		return nil, fmt.Errorf("transform: marshal lineage: %w", err)
	}
	if len(biz) < 2 || biz[0] != '{' || biz[len(biz)-1] != '}' {
		return nil, fmt.Errorf("transform: business of %s is not a JSON object", r.Dataset)
	}
	var buf bytes.Buffer
	buf.Grow(len(biz) + len(tech))
	buf.Write(biz[:len(biz)-1]) // drop the closing brace
	if len(biz) > 2 {
		buf.WriteByte(',')
	}
	buf.Write(tech[1:]) // drop the opening brace
	return buf.Bytes(), nil
}

// SchemaIDFor maps a processed dataset to its S2 schema $id ("" if unknown).
func SchemaIDFor(dataset string) string {
	switch dataset {
	case DatasetMarketSnapshot:
		return validation.IDMarketSnapshot
	case DatasetCoinSnapshot:
		return validation.IDCoinSnapshot
	case DatasetCoinMarketByCurrency:
		return validation.IDCoinMarketByCurrency
	case DatasetTicker:
		return validation.IDTicker
	case DatasetCoinContract:
		return validation.IDCoinContract
	default:
		return ""
	}
}
```

- [ ] **Step 9: Run the tests and watch the schema tests fail**

```bash
go test ./internal/transform/
```

Expected: FAIL in `TestMarketsRowsValidateAgainstSchema`, `TestMarketsSchemaRejectsBadRow` and `TestMarketsRejects`. `Registry.Validate` returns an unknown-schema error for `https://schemas.cg-etl.internal/processed/market_snapshot.v1.json`, because the schema does not exist yet. Every other test passes.

- [ ] **Step 10: Write the market_snapshot schema and embed processed/**

Create `schemas/processed/market_snapshot.v1.json`. The schema is strict: `additionalProperties: false`, and every column is required, including the nullable ones, because `RowJSON` always writes every key. `price` is a non-null decimal because a processed row always has a positive price (null and non-positive prices go to `rejected/`):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/processed/market_snapshot.v1.json",
  "title": "market_snapshot v1: one coin from /coins/markets in one snapshot",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "coin_id", "snapshot_at", "symbol", "name", "vs_currency", "market_cap_rank",
    "price", "market_cap", "fdv", "total_volume", "high_24h", "low_24h",
    "price_change_24h", "price_change_pct_1h", "price_change_pct_24h", "price_change_pct_7d",
    "market_cap_change_24h", "market_cap_change_pct_24h",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_pct", "ath_at", "atl", "atl_change_pct", "atl_at",
    "roi_times", "roi_currency", "roi_pct",
    "source_updated_at", "is_stale", "is_inactive", "is_anomaly",
    "_event_id", "_raw_object", "_kafka_partition", "_kafka_offset", "_schema_version", "_processed_at"
  ],
  "properties": {
    "coin_id": { "type": "string", "minLength": 1 },
    "snapshot_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "symbol": { "type": ["string", "null"], "minLength": 1 },
    "name": { "type": ["string", "null"], "minLength": 1 },
    "vs_currency": { "type": "string", "pattern": "^[a-z0-9]{2,10}$" },
    "market_cap_rank": { "type": ["integer", "null"], "minimum": 1 },
    "price": { "$ref": "../common/defs.v1.json#/$defs/decimal_string" },
    "market_cap": { "$ref": "#/$defs/decimal_or_null" },
    "fdv": { "$ref": "#/$defs/decimal_or_null" },
    "total_volume": { "$ref": "#/$defs/decimal_or_null" },
    "high_24h": { "$ref": "#/$defs/decimal_or_null" },
    "low_24h": { "$ref": "#/$defs/decimal_or_null" },
    "price_change_24h": { "$ref": "#/$defs/decimal_or_null" },
    "price_change_pct_1h": { "type": ["number", "null"] },
    "price_change_pct_24h": { "type": ["number", "null"] },
    "price_change_pct_7d": { "type": ["number", "null"] },
    "market_cap_change_24h": { "$ref": "#/$defs/decimal_or_null" },
    "market_cap_change_pct_24h": { "type": ["number", "null"] },
    "circulating_supply": { "$ref": "#/$defs/decimal_or_null" },
    "total_supply": { "$ref": "#/$defs/decimal_or_null" },
    "max_supply": { "$ref": "#/$defs/decimal_or_null" },
    "ath": { "$ref": "#/$defs/decimal_or_null" },
    "ath_change_pct": { "type": ["number", "null"] },
    "ath_at": { "$ref": "#/$defs/timestamp_or_null" },
    "atl": { "$ref": "#/$defs/decimal_or_null" },
    "atl_change_pct": { "type": ["number", "null"] },
    "atl_at": { "$ref": "#/$defs/timestamp_or_null" },
    "roi_times": { "type": ["number", "null"] },
    "roi_currency": { "type": ["string", "null"], "minLength": 1 },
    "roi_pct": { "type": ["number", "null"] },
    "source_updated_at": { "$ref": "#/$defs/timestamp_or_null" },
    "is_stale": { "type": "boolean" },
    "is_inactive": { "type": "boolean" },
    "is_anomaly": { "type": "boolean" },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_schema_version": { "const": 1 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  },
  "$defs": {
    "decimal_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/decimal_string" }, { "type": "null" }]
    },
    "timestamp_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }, { "type": "null" }]
    }
  }
}
```

Replace `schemas/embed.go` with this exact content (Task 3/4 version plus the line `//go:embed processed/*.json` directly above `var FS embed.FS`):

```go
// Package schemas embeds the JSON Schema files of the pipeline so every
// binary carries the schema version it was built with. testdata/ is not
// embedded: fixtures are for tests only.
package schemas

import "embed"

// FS holds every schema file, at paths that mirror the $id
// (https://schemas.cg-etl.internal/<path>). One //go:embed line per
// directory: a pattern that matches no file is a compile error, so a
// directory is added here in the task that creates its first schema.
//
//go:embed common/*.json
//go:embed pipeline/*.json
//go:embed source/coingecko/*.json
//go:embed processed/*.json
var FS embed.FS
```

- [ ] **Step 11: Run all tests, vet and tidy**

```bash
go mod tidy
go vet ./...
go test -race ./internal/transform/ ./schemas/... ./internal/validation/...
```

Expected: `ok  coingecko-etl/internal/transform`. The validation and schema contract tests from earlier tasks still pass: they walk `schemas.FS`, which now also contains `processed/market_snapshot.v1.json`.

- [ ] **Step 12: Commit**

```bash
git add go.mod go.sum schemas/embed.go schemas/processed/market_snapshot.v1.json internal/transform/
git commit -m "$(cat <<'MSG'
feat(transform): markets transform, normalization, content hash and market_snapshot schema

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

### Task 13: Transform — coin detail

**Files:**
- Create: `internal/transform/detail.go`
- Create: `schemas/processed/coin_snapshot.v1.json`, `schemas/processed/coin_market_by_currency.v1.json`, `schemas/processed/ticker.v1.json`, `schemas/processed/coin_contract.v1.json`
- Modify: `internal/transform/transform.go` (one line in `Transform`)
- Test: `internal/transform/detail_test.go` (fixtures `schemas/testdata/coingecko/coin_bitcoin_full_t0.json`, `coin_bitcoin_full_t1.json`, `coin_usd-coin_meta.json`)

**Interfaces:**
- Consumes (from Task 12, same package): `type Rules`, `type Record`, `type Zone`, `ZoneProcessed`, `ZoneRejected`, `type Dec`, `DecFromNumber`, `ContentSHA256(business any) ([32]byte, error)`, `Dataset*`/`Reason*` constants, unexported helpers `decodeJSON`, `parseTime`, `nilIfEmpty`, `strOrEmpty`, `nonEmptyStrings`, `upperSymbol`, `normAddress`, `normAsset`, `intOrNil`, `decErr`, `positive`, `isZero`, `outsideRange`; test helpers `fixtureEvent`, `rawEvent`, `mustTransform`, `loadRegistry`, `validateProcessed`, `testLineage`, `testRules`. From other packages: `event.RawEvent`, `event.EndpointCoinDetail`, `xtime.Format`, `xtime.FormatDate`, `validation.Load`, `schemas.FS`.
- Produces:
  ```go
  func Transform(ev event.RawEvent, rules Rules) ([]Record, error) // now also handles event.EndpointCoinDetail
  // additive: CoinSnapshot, CoinMarketByCurrency, Ticker, CoinContract business structs
  ```

Background for this task:
- `/coins/{id}` returns a deep document. `market_data` holds 23 currency maps of the form `{"usd": 82776, "eur": …}`, each with the same 63 currency keys; they are zipped into one row per currency. `tickers[]` holds 100 exchange tickers with a nested `market` object and `converted_last` / `converted_volume` maps (`usd`, `btc`, `eth`). `platforms` (`{"ethereum": "0xa0b8…"}`) and `detail_platforms` (`{"ethereum": {"decimal_place": 6, "contract_address": "0xa0b8…"}}`) describe token contracts. Bitcoin has the placeholder `{"": ""}`, which is rejected with `empty_platform`.
- `snapshot_at` for detail is the document's top-level `last_updated`. A coin's `source_updated_at` is `market_data.last_updated` when present, else the top-level value.
- Go map iteration is random, so every map is iterated in sorted key order to keep transforms deterministic.
- Ticker anomaly: the median of the positive `last_usd` values of the coin's tickers in this snapshot. A ticker is anomalous when `|last_usd − median| / median × 100 > TickerAnomalyPct` or when the upstream `is_anomaly` is true. All arithmetic uses `shopspring/decimal`. Ticker stale: upstream `is_stale` or `snapshot_at − last_traded_at > TickerStaleAfter`.
- Duplicate ticker keys inside one message are not collapsed here. Each record keeps its own ordinal, and dedup (a later task) keeps the first and drops equal copies.
- `coin_market_by_currency` rows also get `is_inactive` (zero volume) and `is_anomaly` (price outside `[low_24h, high_24h]`), the same flag rules as `market_snapshot` (spec §6.4).

- [ ] **Step 1: Write the failing detail tests**

Create `internal/transform/detail_test.go`. Expected values come from the fixtures: bitcoin has 63 currencies, 100 tickers with no stale or anomalous ones (median `last_usd` 82809), and one empty platform; usd-coin (fetched with `market_data=false`, `tickers=false`) has 36 contracts, has neither `market_data` nor `tickers`, and has `genesis_date: null`:

```go
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
		tickerJSON("a", "btc", "usdt", "100", false, ts),                     // median of {100,101,99,102,110} is 101 → |100-101|/101 = 0.99%
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
```

- [ ] **Step 2: Run the tests and watch them fail**

```bash
go test ./internal/transform/ -run 'Detail|Ticker|Currency'
```

Expected: build failure, e.g. `undefined: CoinSnapshot`, `undefined: CoinMarketByCurrency`, `undefined: Ticker`, `undefined: CoinContract`.

- [ ] **Step 3: Write detail.go**

Create `internal/transform/detail.go`:

```go
package transform

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"
)

// CoinSnapshot is the coin_snapshot parent row (scalars of /coins/{id}).
type CoinSnapshot struct {
	CoinID            string   `json:"coin_id"`
	SnapshotAt        string   `json:"snapshot_at"`
	Symbol            *string  `json:"symbol"`
	Name              *string  `json:"name"`
	Categories        []string `json:"categories"`
	HashingAlgorithm  *string  `json:"hashing_algorithm"`
	GenesisDate       *string  `json:"genesis_date"`
	MarketCapRank     *int64   `json:"market_cap_rank"`
	CirculatingSupply Dec      `json:"circulating_supply"`
	TotalSupply       Dec      `json:"total_supply"`
	MaxSupply         Dec      `json:"max_supply"`
	MaxSupplyInfinite *bool    `json:"max_supply_infinite"`
	SourceUpdatedAt   *string  `json:"source_updated_at"`
	IsStale           bool     `json:"is_stale"`
}

// CoinMarketByCurrency is one currency of the 23 market_data maps zipped together.
type CoinMarketByCurrency struct {
	CoinID                string   `json:"coin_id"`
	Currency              string   `json:"currency"`
	SnapshotAt            string   `json:"snapshot_at"`
	Price                 Dec      `json:"price"`
	MarketCap             Dec      `json:"market_cap"`
	FDV                   Dec      `json:"fdv"`
	TotalVolume           Dec      `json:"total_volume"`
	High24h               Dec      `json:"high_24h"`
	Low24h                Dec      `json:"low_24h"`
	ATH                   Dec      `json:"ath"`
	ATHChangePct          *float64 `json:"ath_change_pct"`
	ATHAt                 *string  `json:"ath_at"`
	ATL                   Dec      `json:"atl"`
	ATLChangePct          *float64 `json:"atl_change_pct"`
	ATLAt                 *string  `json:"atl_at"`
	PriceChange24h        Dec      `json:"price_change_24h"`
	PriceChangePct1h      *float64 `json:"price_change_pct_1h"`
	PriceChangePct24h     *float64 `json:"price_change_pct_24h"`
	PriceChangePct7d      *float64 `json:"price_change_pct_7d"`
	PriceChangePct14d     *float64 `json:"price_change_pct_14d"`
	PriceChangePct30d     *float64 `json:"price_change_pct_30d"`
	PriceChangePct60d     *float64 `json:"price_change_pct_60d"`
	PriceChangePct200d    *float64 `json:"price_change_pct_200d"`
	PriceChangePct1y      *float64 `json:"price_change_pct_1y"`
	MarketCapChange24h    Dec      `json:"market_cap_change_24h"`
	MarketCapChangePct24h *float64 `json:"market_cap_change_pct_24h"`
	IsInactive            bool     `json:"is_inactive"`
	IsAnomaly             bool     `json:"is_anomaly"`
}

// Ticker is one element of tickers[] of /coins/{id}.
type Ticker struct {
	CoinID              string   `json:"coin_id"` // the /coins/{id} document's coin (parent key)
	ExchangeID          string   `json:"exchange_id"`
	Base                string   `json:"base"`
	Target              string   `json:"target"`
	SnapshotAt          string   `json:"snapshot_at"`
	ExchangeName        *string  `json:"exchange_name"`
	HasTradingIncentive *bool    `json:"has_trading_incentive"`
	BaseCoinID          *string  `json:"base_coin_id"` // tickers[].coin_id (e.g. "ethereum" for ETH/BTC)
	TargetCoinID        *string  `json:"target_coin_id"`
	Last                Dec      `json:"last"`
	Volume              Dec      `json:"volume"`
	LastUSD             Dec      `json:"last_usd"`
	LastBTC             Dec      `json:"last_btc"`
	LastETH             Dec      `json:"last_eth"`
	VolumeUSD           Dec      `json:"volume_usd"`
	VolumeBTC           Dec      `json:"volume_btc"`
	VolumeETH           Dec      `json:"volume_eth"`
	BidAskSpreadPct     *float64 `json:"bid_ask_spread_pct"`
	TrustScore          *string  `json:"trust_score"`
	TickerTimestamp     *string  `json:"ticker_timestamp"`
	LastTradedAt        *string  `json:"last_traded_at"`
	LastFetchAt         *string  `json:"last_fetch_at"`
	UpstreamIsStale     *bool    `json:"upstream_is_stale"`
	UpstreamIsAnomaly   *bool    `json:"upstream_is_anomaly"`
	IsStale             bool     `json:"is_stale"`
	IsAnomaly           bool     `json:"is_anomaly"`
}

// CoinContract is one platform of platforms + detail_platforms.
type CoinContract struct {
	CoinID          string `json:"coin_id"`
	PlatformID      string `json:"platform_id"`
	SnapshotAt      string `json:"snapshot_at"`
	ContractAddress string `json:"contract_address"`
	DecimalPlace    *int64 `json:"decimal_place"`
}

// coinDoc is the subset of /coins/{id} we use; everything else is dropped.
type coinDoc struct {
	ID               *string            `json:"id"`
	Symbol           *string            `json:"symbol"`
	Name             *string            `json:"name"`
	Categories       []string           `json:"categories"`
	HashingAlgorithm *string            `json:"hashing_algorithm"`
	GenesisDate      *string            `json:"genesis_date"`
	MarketCapRank    json.Number        `json:"market_cap_rank"`
	Platforms        map[string]*string `json:"platforms"`
	DetailPlatforms  map[string]*struct {
		DecimalPlace    json.Number `json:"decimal_place"`
		ContractAddress *string     `json:"contract_address"`
	} `json:"detail_platforms"`
	LastUpdated *string        `json:"last_updated"`
	MarketData  *coinMarketDoc `json:"market_data"`
	Tickers     []tickerDoc    `json:"tickers"`
}

// numMap is a CoinGecko currency map ("usd": 82776, …); null values decode to "".
type numMap map[string]json.Number

type coinMarketDoc struct {
	CurrentPrice                    numMap             `json:"current_price"`
	MarketCap                       numMap             `json:"market_cap"`
	FullyDilutedValuation           numMap             `json:"fully_diluted_valuation"`
	TotalVolume                     numMap             `json:"total_volume"`
	High24h                         numMap             `json:"high_24h"`
	Low24h                          numMap             `json:"low_24h"`
	ATH                             numMap             `json:"ath"`
	ATHChangePercentage             numMap             `json:"ath_change_percentage"`
	ATHDate                         map[string]*string `json:"ath_date"`
	ATL                             numMap             `json:"atl"`
	ATLChangePercentage             numMap             `json:"atl_change_percentage"`
	ATLDate                         map[string]*string `json:"atl_date"`
	PriceChange24hInCurrency        numMap             `json:"price_change_24h_in_currency"`
	PriceChangePct1hInCurrency      numMap             `json:"price_change_percentage_1h_in_currency"`
	PriceChangePct24hInCurrency     numMap             `json:"price_change_percentage_24h_in_currency"`
	PriceChangePct7dInCurrency      numMap             `json:"price_change_percentage_7d_in_currency"`
	PriceChangePct14dInCurrency     numMap             `json:"price_change_percentage_14d_in_currency"`
	PriceChangePct30dInCurrency     numMap             `json:"price_change_percentage_30d_in_currency"`
	PriceChangePct60dInCurrency     numMap             `json:"price_change_percentage_60d_in_currency"`
	PriceChangePct200dInCurrency    numMap             `json:"price_change_percentage_200d_in_currency"`
	PriceChangePct1yInCurrency      numMap             `json:"price_change_percentage_1y_in_currency"`
	MarketCapChange24hInCurrency    numMap             `json:"market_cap_change_24h_in_currency"`
	MarketCapChangePct24hInCurrency numMap             `json:"market_cap_change_percentage_24h_in_currency"`
	TotalSupply                     json.Number        `json:"total_supply"`
	MaxSupply                       json.Number        `json:"max_supply"`
	MaxSupplyInfinite               *bool              `json:"max_supply_infinite"`
	CirculatingSupply               json.Number        `json:"circulating_supply"`
	LastUpdated                     *string            `json:"last_updated"`
}

type tickerDoc struct {
	Base   *string `json:"base"`
	Target *string `json:"target"`
	Market *struct {
		Name                *string `json:"name"`
		Identifier          *string `json:"identifier"`
		HasTradingIncentive *bool   `json:"has_trading_incentive"`
	} `json:"market"`
	Last                   json.Number `json:"last"`
	Volume                 json.Number `json:"volume"`
	ConvertedLast          numMap      `json:"converted_last"`
	ConvertedVolume        numMap      `json:"converted_volume"`
	TrustScore             *string     `json:"trust_score"`
	BidAskSpreadPercentage json.Number `json:"bid_ask_spread_percentage"`
	Timestamp              *string     `json:"timestamp"`
	LastTradedAt           *string     `json:"last_traded_at"`
	LastFetchAt            *string     `json:"last_fetch_at"`
	IsAnomaly              *bool       `json:"is_anomaly"`
	IsStale                *bool       `json:"is_stale"`
	CoinID                 *string     `json:"coin_id"`
	TargetCoinID           *string     `json:"target_coin_id"`
}

// detailBuilder accumulates records of one /coins/{id} message; ordinals
// are assigned in append order across all datasets of the message.
type detailBuilder struct {
	coinID    string
	snap      time.Time
	snapAt    string
	eventDate string
	rules     Rules
	out       []Record
}

func (b *detailBuilder) add(dataset, key string, zone Zone, reason string, business any) error {
	sum, err := ContentSHA256(business)
	if err != nil {
		return err
	}
	b.out = append(b.out, Record{
		Dataset:       dataset,
		Key:           key,
		EventDate:     b.eventDate,
		Ordinal:       len(b.out),
		Zone:          zone,
		Reason:        reason,
		ContentSHA256: sum,
		Business:      business,
	})
	return nil
}

func transformDetail(ev event.RawEvent, rules Rules) ([]Record, error) {
	var doc coinDoc
	if err := decodeJSON(ev.Payload, &doc); err != nil {
		return nil, fmt.Errorf("transform: decode coin_detail payload: %w", err)
	}
	snap, ok, err := parseTime(doc.LastUpdated)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("transform: coin_detail payload has no last_updated")
	}
	b := &detailBuilder{
		coinID:    strings.TrimSpace(strOrEmpty(doc.ID)),
		snap:      snap,
		snapAt:    xtime.Format(snap),
		eventDate: xtime.FormatDate(snap),
		rules:     rules,
	}

	parent, err := coinSnapshotFrom(&doc, b)
	if err != nil {
		return nil, err
	}
	if b.coinID == "" {
		// Children cannot reference a parent without an id: only the reject is emitted.
		if err := b.add(DatasetCoinSnapshot, "#0|"+b.snapAt, ZoneRejected, ReasonMissingID, parent); err != nil {
			return nil, err
		}
		return b.out, nil
	}
	if err := b.add(DatasetCoinSnapshot, b.coinID+"|"+b.snapAt, ZoneProcessed, "", parent); err != nil {
		return nil, err
	}
	if err := b.addCurrencies(doc.MarketData); err != nil {
		return nil, err
	}
	if err := b.addTickers(doc.Tickers); err != nil {
		return nil, err
	}
	if err := b.addContracts(&doc); err != nil {
		return nil, err
	}
	return b.out, nil
}

func coinSnapshotFrom(doc *coinDoc, b *detailBuilder) (CoinSnapshot, error) {
	var e decErr
	c := CoinSnapshot{
		CoinID:           b.coinID,
		SnapshotAt:       b.snapAt,
		Symbol:           upperSymbol(doc.Symbol),
		Name:             nilIfEmpty(doc.Name),
		Categories:       nonEmptyStrings(doc.Categories),
		HashingAlgorithm: nilIfEmpty(doc.HashingAlgorithm),
	}
	if g := nilIfEmpty(doc.GenesisDate); g != nil {
		t, err := time.Parse("2006-01-02", *g)
		if err != nil {
			return CoinSnapshot{}, fmt.Errorf("transform: bad genesis_date %q: %w", *g, err)
		}
		v := t.Format("2006-01-02")
		c.GenesisDate = &v
	}
	rank, err := intOrNil(doc.MarketCapRank)
	if err != nil {
		return CoinSnapshot{}, err
	}
	c.MarketCapRank = rank
	updated := doc.LastUpdated
	if md := doc.MarketData; md != nil {
		c.CirculatingSupply = e.dec(md.CirculatingSupply)
		c.TotalSupply = e.dec(md.TotalSupply)
		c.MaxSupply = e.dec(md.MaxSupply)
		c.MaxSupplyInfinite = md.MaxSupplyInfinite
		if nilIfEmpty(md.LastUpdated) != nil {
			updated = md.LastUpdated
		}
	}
	c.SourceUpdatedAt = e.ts(updated)
	if e.err != nil {
		return CoinSnapshot{}, e.err
	}
	if t, ok, _ := parseTime(updated); ok {
		c.IsStale = b.snap.Sub(t) > b.rules.StaleAfter
	}
	return c, nil
}

// addCurrencies zips the 23 currency maps on the currency code, sorted.
func (b *detailBuilder) addCurrencies(md *coinMarketDoc) error {
	if md == nil {
		return nil
	}
	numMaps := []numMap{
		md.CurrentPrice, md.MarketCap, md.FullyDilutedValuation, md.TotalVolume, md.High24h, md.Low24h,
		md.ATH, md.ATHChangePercentage, md.ATL, md.ATLChangePercentage,
		md.PriceChange24hInCurrency, md.PriceChangePct1hInCurrency, md.PriceChangePct24hInCurrency,
		md.PriceChangePct7dInCurrency, md.PriceChangePct14dInCurrency, md.PriceChangePct30dInCurrency,
		md.PriceChangePct60dInCurrency, md.PriceChangePct200dInCurrency, md.PriceChangePct1yInCurrency,
		md.MarketCapChange24hInCurrency, md.MarketCapChangePct24hInCurrency,
	}
	set := map[string]bool{}
	for _, m := range numMaps {
		for k := range m {
			set[k] = true
		}
	}
	for _, m := range []map[string]*string{md.ATHDate, md.ATLDate} {
		for k := range m {
			set[k] = true
		}
	}
	currencies := make([]string, 0, len(set))
	for k := range set {
		currencies = append(currencies, k)
	}
	sort.Strings(currencies)

	for _, cur := range currencies {
		var e decErr
		r := CoinMarketByCurrency{
			CoinID:                b.coinID,
			Currency:              strings.ToLower(strings.TrimSpace(cur)),
			SnapshotAt:            b.snapAt,
			Price:                 e.dec(md.CurrentPrice[cur]),
			MarketCap:             e.dec(md.MarketCap[cur]),
			FDV:                   e.dec(md.FullyDilutedValuation[cur]),
			TotalVolume:           e.dec(md.TotalVolume[cur]),
			High24h:               e.dec(md.High24h[cur]),
			Low24h:                e.dec(md.Low24h[cur]),
			ATH:                   e.dec(md.ATH[cur]),
			ATHChangePct:          e.pct(md.ATHChangePercentage[cur]),
			ATHAt:                 e.ts(md.ATHDate[cur]),
			ATL:                   e.dec(md.ATL[cur]),
			ATLChangePct:          e.pct(md.ATLChangePercentage[cur]),
			ATLAt:                 e.ts(md.ATLDate[cur]),
			PriceChange24h:        e.dec(md.PriceChange24hInCurrency[cur]),
			PriceChangePct1h:      e.pct(md.PriceChangePct1hInCurrency[cur]),
			PriceChangePct24h:     e.pct(md.PriceChangePct24hInCurrency[cur]),
			PriceChangePct7d:      e.pct(md.PriceChangePct7dInCurrency[cur]),
			PriceChangePct14d:     e.pct(md.PriceChangePct14dInCurrency[cur]),
			PriceChangePct30d:     e.pct(md.PriceChangePct30dInCurrency[cur]),
			PriceChangePct60d:     e.pct(md.PriceChangePct60dInCurrency[cur]),
			PriceChangePct200d:    e.pct(md.PriceChangePct200dInCurrency[cur]),
			PriceChangePct1y:      e.pct(md.PriceChangePct1yInCurrency[cur]),
			MarketCapChange24h:    e.dec(md.MarketCapChange24hInCurrency[cur]),
			MarketCapChangePct24h: e.pct(md.MarketCapChangePct24hInCurrency[cur]),
		}
		if e.err != nil {
			return fmt.Errorf("transform: market_data currency %q: %w", cur, e.err)
		}
		r.IsInactive = isZero(r.TotalVolume)
		r.IsAnomaly = outsideRange(r.Price, r.Low24h, r.High24h)

		zone, reason := ZoneProcessed, ""
		switch {
		case allMetricsNull(r):
			zone, reason = ZoneRejected, ReasonEmptyRecord
		case r.Price == "":
			zone, reason = ZoneRejected, ReasonNullPrice
		case !positive(r.Price):
			zone, reason = ZoneRejected, ReasonNonpositivePrice
		}
		key := b.coinID + "|" + r.Currency + "|" + b.snapAt
		if err := b.add(DatasetCoinMarketByCurrency, key, zone, reason, r); err != nil {
			return err
		}
	}
	return nil
}

func allMetricsNull(r CoinMarketByCurrency) bool {
	for _, d := range []Dec{r.Price, r.MarketCap, r.FDV, r.TotalVolume, r.High24h, r.Low24h, r.ATH, r.ATL, r.PriceChange24h, r.MarketCapChange24h} {
		if d != "" {
			return false
		}
	}
	for _, f := range []*float64{r.ATHChangePct, r.ATLChangePct, r.PriceChangePct1h, r.PriceChangePct24h, r.PriceChangePct7d,
		r.PriceChangePct14d, r.PriceChangePct30d, r.PriceChangePct60d, r.PriceChangePct200d, r.PriceChangePct1y, r.MarketCapChangePct24h} {
		if f != nil {
			return false
		}
	}
	return r.ATHAt == nil && r.ATLAt == nil
}

// addTickers emits one record per tickers[] element, in payload order.
// Duplicate keys inside one message are not resolved here: each keeps its
// own ordinal and dedup decides (first ordinal wins, equal content drops).
func (b *detailBuilder) addTickers(docs []tickerDoc) error {
	rows := make([]Ticker, 0, len(docs))
	for i, d := range docs {
		var e decErr
		t := Ticker{
			CoinID:            b.coinID,
			Base:              normAsset(strOrEmpty(d.Base)),
			Target:            normAsset(strOrEmpty(d.Target)),
			SnapshotAt:        b.snapAt,
			BaseCoinID:        nilIfEmpty(d.CoinID),
			TargetCoinID:      nilIfEmpty(d.TargetCoinID),
			Last:              e.dec(d.Last),
			Volume:            e.dec(d.Volume),
			LastUSD:           e.dec(d.ConvertedLast["usd"]),
			LastBTC:           e.dec(d.ConvertedLast["btc"]),
			LastETH:           e.dec(d.ConvertedLast["eth"]),
			VolumeUSD:         e.dec(d.ConvertedVolume["usd"]),
			VolumeBTC:         e.dec(d.ConvertedVolume["btc"]),
			VolumeETH:         e.dec(d.ConvertedVolume["eth"]),
			BidAskSpreadPct:   e.pct(d.BidAskSpreadPercentage),
			TrustScore:        nilIfEmpty(d.TrustScore),
			TickerTimestamp:   e.ts(d.Timestamp),
			LastTradedAt:      e.ts(d.LastTradedAt),
			LastFetchAt:       e.ts(d.LastFetchAt),
			UpstreamIsStale:   d.IsStale,
			UpstreamIsAnomaly: d.IsAnomaly,
		}
		if d.Market != nil {
			t.ExchangeID = strings.TrimSpace(strOrEmpty(d.Market.Identifier))
			t.ExchangeName = nilIfEmpty(d.Market.Name)
			t.HasTradingIncentive = d.Market.HasTradingIncentive
		}
		if e.err != nil {
			return fmt.Errorf("transform: ticker %d: %w", i, e.err)
		}
		stale := d.IsStale != nil && *d.IsStale
		if lt, ok, _ := parseTime(d.LastTradedAt); ok && b.snap.Sub(lt) > b.rules.TickerStaleAfter {
			stale = true
		}
		t.IsStale = stale
		rows = append(rows, t)
	}

	median, hasMedian := medianLastUSD(rows)
	limit := decimal.NewFromFloat(b.rules.TickerAnomalyPct)
	hundred := decimal.NewFromInt(100)
	for i := range rows {
		t := &rows[i]
		t.IsAnomaly = t.UpstreamIsAnomaly != nil && *t.UpstreamIsAnomaly
		if v, ok := t.LastUSD.Decimal(); ok && hasMedian {
			dev := v.Sub(median).Abs().Div(median).Mul(hundred)
			if dev.GreaterThan(limit) {
				t.IsAnomaly = true
			}
		}
		zone, reason := ZoneProcessed, ""
		key := b.coinID + "|" + t.ExchangeID + "|" + t.Base + "|" + t.Target + "|" + b.snapAt
		if t.ExchangeID == "" || t.Base == "" || t.Target == "" {
			zone, reason = ZoneRejected, ReasonMissingID
			key = fmt.Sprintf("#%d|%s", i, b.snapAt)
		}
		if err := b.add(DatasetTicker, key, zone, reason, *t); err != nil {
			return err
		}
	}
	return nil
}

// medianLastUSD is the median of the present, positive last_usd values.
func medianLastUSD(rows []Ticker) (decimal.Decimal, bool) {
	var vals []decimal.Decimal
	for _, t := range rows {
		if v, ok := t.LastUSD.Decimal(); ok && v.IsPositive() {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return decimal.Decimal{}, false
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].LessThan(vals[j]) })
	n := len(vals)
	if n%2 == 1 {
		return vals[n/2], true
	}
	return vals[n/2-1].Add(vals[n/2]).Div(decimal.NewFromInt(2)), true
}

// addContracts zips platforms and detail_platforms on the platform key, sorted.
func (b *detailBuilder) addContracts(doc *coinDoc) error {
	set := map[string]bool{}
	for k := range doc.Platforms {
		set[k] = true
	}
	for k := range doc.DetailPlatforms {
		set[k] = true
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		addr := strOrEmpty(doc.Platforms[k])
		var places json.Number
		if dp := doc.DetailPlatforms[k]; dp != nil {
			if strings.TrimSpace(addr) == "" {
				addr = strOrEmpty(dp.ContractAddress)
			}
			places = dp.DecimalPlace
		}
		dpl, err := intOrNil(places)
		if err != nil {
			return fmt.Errorf("transform: platform %q decimal_place: %w", k, err)
		}
		c := CoinContract{
			CoinID:          b.coinID,
			PlatformID:      strings.TrimSpace(k),
			SnapshotAt:      b.snapAt,
			ContractAddress: normAddress(addr),
			DecimalPlace:    dpl,
		}
		zone, reason := ZoneProcessed, ""
		if c.PlatformID == "" || c.ContractAddress == "" {
			zone, reason = ZoneRejected, ReasonEmptyPlatform
		}
		key := b.coinID + "|" + c.PlatformID + "|" + b.snapAt
		if err := b.add(DatasetCoinContract, key, zone, reason, c); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Route coin_detail to the new transform**

In `internal/transform/transform.go`, inside `Transform`, replace exactly:

```go
	case event.EndpointCoinDetail:
		return nil, fmt.Errorf("transform: endpoint %q not implemented", ev.Endpoint)
```

with:

```go
	case event.EndpointCoinDetail:
		return transformDetail(ev, rules)
```

Nothing else in `transform.go` changes (`fmt` is still used by the other branches).

- [ ] **Step 5: Run the tests and watch the schema tests fail**

```bash
go test ./internal/transform/
```

Expected: FAIL in `TestDetailRowsValidateAgainstSchemas` (all three subtests), `TestTickerMedianAnomaly` and `TestCurrencyRejects`, each with an unknown-schema error for `…/processed/coin_snapshot.v1.json` (or `coin_market_by_currency`, `ticker`, `coin_contract`). All other tests pass.

- [ ] **Step 6: Write the four processed schemas**

All four are strict (`additionalProperties: false`, every column required, nullable columns typed `[T, "null"]`) and end with the same six lineage columns as `market_snapshot`.

Create `schemas/processed/coin_snapshot.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/processed/coin_snapshot.v1.json",
  "title": "coin_snapshot v1: scalar attributes of one /coins/{id} snapshot (parent of the other detail datasets)",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "coin_id", "snapshot_at", "symbol", "name", "categories", "hashing_algorithm", "genesis_date",
    "market_cap_rank", "circulating_supply", "total_supply", "max_supply", "max_supply_infinite",
    "source_updated_at", "is_stale",
    "_event_id", "_raw_object", "_kafka_partition", "_kafka_offset", "_schema_version", "_processed_at"
  ],
  "properties": {
    "coin_id": { "type": "string", "minLength": 1 },
    "snapshot_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "symbol": { "type": ["string", "null"], "minLength": 1 },
    "name": { "type": ["string", "null"], "minLength": 1 },
    "categories": {
      "type": ["array", "null"],
      "minItems": 1,
      "items": { "type": "string", "minLength": 1 }
    },
    "hashing_algorithm": { "type": ["string", "null"], "minLength": 1 },
    "genesis_date": {
      "anyOf": [
        { "type": "string", "format": "date", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
        { "type": "null" }
      ]
    },
    "market_cap_rank": { "type": ["integer", "null"], "minimum": 1 },
    "circulating_supply": { "$ref": "#/$defs/decimal_or_null" },
    "total_supply": { "$ref": "#/$defs/decimal_or_null" },
    "max_supply": { "$ref": "#/$defs/decimal_or_null" },
    "max_supply_infinite": { "type": ["boolean", "null"] },
    "source_updated_at": { "$ref": "#/$defs/timestamp_or_null" },
    "is_stale": { "type": "boolean" },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_schema_version": { "const": 1 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  },
  "$defs": {
    "decimal_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/decimal_string" }, { "type": "null" }]
    },
    "timestamp_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }, { "type": "null" }]
    }
  }
}
```

Create `schemas/processed/coin_market_by_currency.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/processed/coin_market_by_currency.v1.json",
  "title": "coin_market_by_currency v1: the 23 market_data maps of /coins/{id} zipped on one currency",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "coin_id", "currency", "snapshot_at", "price", "market_cap", "fdv", "total_volume", "high_24h", "low_24h",
    "ath", "ath_change_pct", "ath_at", "atl", "atl_change_pct", "atl_at",
    "price_change_24h", "price_change_pct_1h", "price_change_pct_24h", "price_change_pct_7d",
    "price_change_pct_14d", "price_change_pct_30d", "price_change_pct_60d", "price_change_pct_200d",
    "price_change_pct_1y", "market_cap_change_24h", "market_cap_change_pct_24h",
    "is_inactive", "is_anomaly",
    "_event_id", "_raw_object", "_kafka_partition", "_kafka_offset", "_schema_version", "_processed_at"
  ],
  "properties": {
    "coin_id": { "type": "string", "minLength": 1 },
    "currency": { "type": "string", "pattern": "^[a-z0-9]{2,10}$" },
    "snapshot_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "price": { "$ref": "../common/defs.v1.json#/$defs/decimal_string" },
    "market_cap": { "$ref": "#/$defs/decimal_or_null" },
    "fdv": { "$ref": "#/$defs/decimal_or_null" },
    "total_volume": { "$ref": "#/$defs/decimal_or_null" },
    "high_24h": { "$ref": "#/$defs/decimal_or_null" },
    "low_24h": { "$ref": "#/$defs/decimal_or_null" },
    "ath": { "$ref": "#/$defs/decimal_or_null" },
    "ath_change_pct": { "type": ["number", "null"] },
    "ath_at": { "$ref": "#/$defs/timestamp_or_null" },
    "atl": { "$ref": "#/$defs/decimal_or_null" },
    "atl_change_pct": { "type": ["number", "null"] },
    "atl_at": { "$ref": "#/$defs/timestamp_or_null" },
    "price_change_24h": { "$ref": "#/$defs/decimal_or_null" },
    "price_change_pct_1h": { "type": ["number", "null"] },
    "price_change_pct_24h": { "type": ["number", "null"] },
    "price_change_pct_7d": { "type": ["number", "null"] },
    "price_change_pct_14d": { "type": ["number", "null"] },
    "price_change_pct_30d": { "type": ["number", "null"] },
    "price_change_pct_60d": { "type": ["number", "null"] },
    "price_change_pct_200d": { "type": ["number", "null"] },
    "price_change_pct_1y": { "type": ["number", "null"] },
    "market_cap_change_24h": { "$ref": "#/$defs/decimal_or_null" },
    "market_cap_change_pct_24h": { "type": ["number", "null"] },
    "is_inactive": { "type": "boolean" },
    "is_anomaly": { "type": "boolean" },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_schema_version": { "const": 1 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  },
  "$defs": {
    "decimal_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/decimal_string" }, { "type": "null" }]
    },
    "timestamp_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }, { "type": "null" }]
    }
  }
}
```

Create `schemas/processed/ticker.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/processed/ticker.v1.json",
  "title": "ticker v1: one exchange ticker from tickers[] of /coins/{id}",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "coin_id", "exchange_id", "base", "target", "snapshot_at", "exchange_name", "has_trading_incentive",
    "base_coin_id", "target_coin_id", "last", "volume", "last_usd", "last_btc", "last_eth",
    "volume_usd", "volume_btc", "volume_eth", "bid_ask_spread_pct", "trust_score",
    "ticker_timestamp", "last_traded_at", "last_fetch_at",
    "upstream_is_stale", "upstream_is_anomaly", "is_stale", "is_anomaly",
    "_event_id", "_raw_object", "_kafka_partition", "_kafka_offset", "_schema_version", "_processed_at"
  ],
  "properties": {
    "coin_id": { "type": "string", "minLength": 1 },
    "exchange_id": { "type": "string", "minLength": 1 },
    "base": { "type": "string", "minLength": 1 },
    "target": { "type": "string", "minLength": 1 },
    "snapshot_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "exchange_name": { "type": ["string", "null"], "minLength": 1 },
    "has_trading_incentive": { "type": ["boolean", "null"] },
    "base_coin_id": { "type": ["string", "null"], "minLength": 1 },
    "target_coin_id": { "type": ["string", "null"], "minLength": 1 },
    "last": { "$ref": "#/$defs/decimal_or_null" },
    "volume": { "$ref": "#/$defs/decimal_or_null" },
    "last_usd": { "$ref": "#/$defs/decimal_or_null" },
    "last_btc": { "$ref": "#/$defs/decimal_or_null" },
    "last_eth": { "$ref": "#/$defs/decimal_or_null" },
    "volume_usd": { "$ref": "#/$defs/decimal_or_null" },
    "volume_btc": { "$ref": "#/$defs/decimal_or_null" },
    "volume_eth": { "$ref": "#/$defs/decimal_or_null" },
    "bid_ask_spread_pct": { "type": ["number", "null"] },
    "trust_score": { "type": ["string", "null"], "minLength": 1 },
    "ticker_timestamp": { "$ref": "#/$defs/timestamp_or_null" },
    "last_traded_at": { "$ref": "#/$defs/timestamp_or_null" },
    "last_fetch_at": { "$ref": "#/$defs/timestamp_or_null" },
    "upstream_is_stale": { "type": ["boolean", "null"] },
    "upstream_is_anomaly": { "type": ["boolean", "null"] },
    "is_stale": { "type": "boolean" },
    "is_anomaly": { "type": "boolean" },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_schema_version": { "const": 1 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  },
  "$defs": {
    "decimal_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/decimal_string" }, { "type": "null" }]
    },
    "timestamp_or_null": {
      "anyOf": [{ "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }, { "type": "null" }]
    }
  }
}
```

Create `schemas/processed/coin_contract.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://schemas.cg-etl.internal/processed/coin_contract.v1.json",
  "title": "coin_contract v1: one platform/contract of /coins/{id} (platforms + detail_platforms)",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "coin_id", "platform_id", "snapshot_at", "contract_address", "decimal_place",
    "_event_id", "_raw_object", "_kafka_partition", "_kafka_offset", "_schema_version", "_processed_at"
  ],
  "properties": {
    "coin_id": { "type": "string", "minLength": 1 },
    "platform_id": { "type": "string", "minLength": 1 },
    "snapshot_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" },
    "contract_address": { "type": "string", "minLength": 1 },
    "decimal_place": { "type": ["integer", "null"], "minimum": 0 },
    "_event_id": { "$ref": "../common/defs.v1.json#/$defs/uuid" },
    "_raw_object": { "type": "string", "minLength": 1 },
    "_kafka_partition": { "type": "integer", "minimum": 0 },
    "_kafka_offset": { "type": "integer", "minimum": 0 },
    "_schema_version": { "const": 1 },
    "_processed_at": { "$ref": "../common/defs.v1.json#/$defs/utc_timestamp" }
  }
}
```

- [ ] **Step 7: Run all tests and vet**

```bash
go vet ./...
go test -race ./internal/transform/ ./schemas/... ./internal/validation/...
```

Expected: `ok  coingecko-etl/internal/transform`, along with the earlier schema/validation packages.

- [ ] **Step 8: Commit**

```bash
git add internal/transform/detail.go internal/transform/detail_test.go internal/transform/transform.go schemas/processed/
git commit -m "$(cat <<'MSG'
feat(transform): coin detail transform (coin_snapshot, coin_market_by_currency, ticker, coin_contract)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

CONTRACT NOTES

1. `pipeline.dedup_claims` in spec §7.2 has no `idempotency_key` column, but the drop reason `duplicate_response` (spec §7.3) needs the winner's idempotency key. Task 5 declares it in `00002_pipeline.sql` as a nullable `idempotency_key text`; `ClaimAndDecide` stores an empty key as NULL. No extra migration.
2. The rejected and transformer-DLQ line formats follow the Task 3 schemas `pipeline/rejected_record.v1.json` and `pipeline/dlq_record.v1.json`, and the transformer validates every rejected/dlq line before writing it:
   - rejected line: `{"schema_version":1,"dataset","reason":missing_id|null_price|nonpositive_price|empty_platform|empty_record|snapshot_conflict,"record_key","event_date":"YYYY-MM-DD","record":object,"_event_id","_raw_object","_kafka_partition","_kafka_offset","_processed_at"}`, where `record` is `transform.RowJSON` output (business fields plus the `_` lineage columns) and the top-level `_` columns repeat the lineage.
   - transformer dlq line: `{"schema_version":1,"stage":"transform_input"|"transform_output","error_class","error","violations":[…],"origin":{"kind":"kafka","topic","partition","offset","event_id"?,"dataset"?,"record_key"?},"original_base64","occurred_at":<manifest planned_at>}`. Error classes: `decode_error`, `schema_violation`, `integrity_error`, `source_schema_violation`, `transform_error`, `output_schema_violation`. The exporter (Task 9) and publisher (Task 11) write the same shape with `origin.kind` `postgres` / `s3`.
3. `ObjectKey(zone, prefix, topic, partition, firstOffset)` returns the key relative to the bucket. `zone` is the bucket (`s3.Bucket*`) and is not part of the key. `prefix` is the Hive-style directory (`dataset=<d>/v=1/event_date=<date>`, `dataset=<d>/event_date=<date>`, `stage=transform/date=<date>`). The result is `<prefix>/<topic>+<partition>+<%020d>.jsonl`.
4. Assignment recovery uses `kgo.AdjustFetchOffsetsFn` instead of `OnPartitionsAssigned` + kadm `FetchOffsets` + `SetOffsets`. The franz-go v1.22.1 docs say `SetOffsets` "is invalid to set topics that were not yet returned from a PollFetches", so it cannot move a newly assigned partition. `AdjustFetchOffsetsFn` receives the committed offsets after assignment and before fetching starts, which is exactly this hook. The "commit last+1" for a skipped written batch is done from the poll loop, because committing from inside a group callback can deadlock with the join. The behaviour matches spec §7.4: written → seek past and commit; planned → reprocess from its exact start.
5. Exported names added beyond the contract (additions only, nothing renamed): `dedup.ReasonReplay`, `dedup.ReasonDuplicateResponse`, `dedup.ReasonDuplicateRecord`, `dedup.Decision.String`; `transformer.ErrNonDeterministic`, `transformer.IncompleteBatchError`, `transformer.RewindError`, `transformer.ConsumerGroup`. The manifest helpers named in the task text (`InsertPlanned`, `SetObjects`, `MarkWritten`) are unexported (`insertPlanned`, `setObjects`, `markWritten`) because the contract does not list them.
6. Dependencies on other tasks that the tests rely on: `testutil.StartVersityGW` returns a host `dataDir` where objects appear as `<dataDir>/<bucket>/<key>`. `pg.EnsurePartitions(ctx, pool, 2026-10-10, 2)` creates `pipeline.dedup_claims` partitions covering 2026-10-10. `obs.NewMetrics` fills every field the transformer uses (`S3Puts`, `Records`, `DedupDropped`, `Rejected`, `DLQ`, `ConsumerLag`, `BatchDuration`, `E2ELatency`, `SchemaValidations`, `SchemaViolations`). `event.Unmarshal` keeps `Payload` byte-exact. `encoding/json/jsontext` is importable on Go 1.27 without `GOEXPERIMENT` (verified with go1.27.1).

---

### Task 14: Dedup

Implements the single deduplication mechanism of spec §7.3. Every record bound for `processed/` or `rejected/` claims its natural key `(dataset, record_key, event_date)` in `pipeline.dedup_claims`, and the first claimant wins. `ClaimAndDecide` runs inside the caller's transaction (the transformer's TX1), so claims commit atomically with the batch manifest.

How it decides, per claim:

| Stored winner for the key | Decision |
|---|---|
| has this claim's own position (topic_id, partition, offset, ordinal) | `Emit`: first sight, or a redelivery after a crash between TX1 and the PUT |
| other position, same `content_sha256` | `Drop`, reason `replay` (same event_id), `duplicate_response` (same idempotency_key), else `duplicate_record` |
| other position, different content | `Conflict`: the caller writes it to `rejected/` as `snapshot_conflict` |

Postgres details the engineer needs:
- `INSERT ... ON CONFLICT (pk) DO NOTHING` followed by a **separate** `SELECT` statement. Under READ COMMITTED (the pgx default) each statement takes a fresh snapshot. If another transaction holds an uncommitted claim for the same key, our INSERT waits on the unique index. Once that transaction commits, our INSERT does nothing and the SELECT sees the committed winner. A single CTE statement would not see it.
- One INSERT statement must not contain the same key twice. Claims are therefore pre-deduplicated in Go (lowest `(offset, ordinal)` wins) and inserted in sorted key order, so that two transactions lock keys in the same order and cannot deadlock.
- Arrays go in as `text[]` / `int[]` / `bigint[]` / `bytea[]` through `unnest(...)` and are cast in SQL (`::date`, `::uuid`), so pgx needs no custom codecs.
- `pipeline.dedup_claims` is `PARTITION BY RANGE (event_date)`. A claim for a date with no partition fails with SQLSTATE 23514. `pg.EnsurePartitions` creates the partitions; the tests call it.

**Files:**
- Create: `internal/dedup/dedup.go`
- Test: `internal/dedup/decide_test.go` (pure, no Docker)
- Test: `internal/dedup/dedup_test.go` (integration, Postgres via testcontainers)

**Interfaces:**
- Consumes:
  - `func pg.Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)`
  - `func pg.Migrate(ctx context.Context, pool *pgxpool.Pool) error`
  - `func pg.EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error`
  - `func testutil.StartPostgres(t *testing.T) (dsn string)`
  - `pipeline.dedup_claims` from Task 5's `migrations/00002_pipeline.sql`, including the nullable `idempotency_key text` column
- Produces:
  ```go
  // internal/dedup
  type Position struct { TopicID uuid.UUID; Partition int32; Offset int64; Ordinal int }
  type Claim struct {
      Dataset, Key, EventDate string
      Pos            Position
      EventID        string
      IdempotencyKey string
      ContentSHA256  [32]byte
  }
  type Decision int
  const (Emit Decision = iota; Drop; Conflict)
  type Outcome struct { Decision Decision; DropReason string } // DropReason: "replay" | "duplicate_response" | "duplicate_record"
  func ClaimAndDecide(ctx context.Context, tx pgx.Tx, claims []Claim) ([]Outcome, error) // same order as claims
  ```

- [ ] **Step 1: Confirm the `idempotency_key` column from Task 5**

`dedup_claims` stores the winner's idempotency key so that a drop can be labelled `duplicate_response` (same HTTP response body served twice, e.g. a CDN cache hit) rather than `duplicate_record`. Task 5's `migrations/00002_pipeline.sql` already declares it as a nullable `idempotency_key text` column; this task adds no migration. Check it:

```bash
grep -n 'idempotency_key text,' migrations/00002_pipeline.sql
```

Expected: one match inside `CREATE TABLE pipeline.dedup_claims`. The column is informational only: the decision never uses it, and an empty `Claim.IdempotencyKey` is stored as NULL.

- [ ] **Step 2: Write the failing pure unit tests**

Create `internal/dedup/decide_test.go`. It tests the two pure helpers (`pickWinners`, `decide`) without Docker:

```go
package dedup

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
)

func TestPickWinnersKeepsLowestPositionPerKey(t *testing.T) {
	tid := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ev := uuid.NewString()
	mk := func(key string, off int64, ord int) Claim {
		return Claim{Dataset: "ticker", Key: key, EventDate: "2026-10-10", EventID: ev,
			Pos: Position{TopicID: tid, Partition: 2, Offset: off, Ordinal: ord}}
	}
	claims := []Claim{mk("b", 7, 0), mk("a", 9, 4), mk("a", 9, 2), mk("b", 5, 9), mk("a", 10, 0)}
	keys, winners, err := pickWinners(claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].key != "a" || keys[1].key != "b" {
		t.Fatalf("keys not sorted/unique: %+v", keys)
	}
	if got := winners[keys[0]]; got != 2 {
		t.Errorf("winner for a = claim %d, want 2 (offset 9 ordinal 2)", got)
	}
	if got := winners[keys[1]]; got != 3 {
		t.Errorf("winner for b = claim %d, want 3 (offset 5)", got)
	}
}

func TestPickWinnersRejectsBadClaims(t *testing.T) {
	if _, _, err := pickWinners([]Claim{{Dataset: "x", Key: "k", EventDate: "2026-10-10", EventID: "nope"}}); err == nil {
		t.Error("expected error for a non-uuid event_id")
	}
	if _, _, err := pickWinners([]Claim{{Key: "k", EventDate: "2026-10-10", EventID: uuid.NewString()}}); err == nil {
		t.Error("expected error for an empty dataset")
	}
}

func TestDecide(t *testing.T) {
	pos := Position{TopicID: uuid.New(), Partition: 1, Offset: 10, Ordinal: 0}
	other := pos
	other.Offset = 11
	sumA, sumB := sha256.Sum256([]byte("a")), sha256.Sum256([]byte("b"))
	ev1, ev2 := uuid.NewString(), uuid.NewString()
	winner := stored{pos: pos, eventID: ev1, idempotencyKey: "k1", sha: sumA[:]}

	cases := []struct {
		name string
		c    Claim
		want Outcome
	}{
		{"own position", Claim{Pos: pos, EventID: ev1, ContentSHA256: sumA}, Outcome{Decision: Emit}},
		{"own position, content ignored", Claim{Pos: pos, EventID: ev1, ContentSHA256: sumB}, Outcome{Decision: Emit}},
		{"replay", Claim{Pos: other, EventID: ev1, IdempotencyKey: "k1", ContentSHA256: sumA}, Outcome{Drop, ReasonReplay}},
		{"duplicate response", Claim{Pos: other, EventID: ev2, IdempotencyKey: "k1", ContentSHA256: sumA}, Outcome{Drop, ReasonDuplicateResponse}},
		{"duplicate record", Claim{Pos: other, EventID: ev2, IdempotencyKey: "k2", ContentSHA256: sumA}, Outcome{Drop, ReasonDuplicateRecord}},
		{"conflict", Claim{Pos: other, EventID: ev2, IdempotencyKey: "k1", ContentSHA256: sumB}, Outcome{Decision: Conflict}},
	}
	for _, tc := range cases {
		if got := decide(winner, tc.c); got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
```

- [ ] **Step 3: Run them and see them fail**

Run: `go test ./internal/dedup/ -run 'TestPickWinners|TestDecide' -v`
Expected: FAIL. The build fails with `undefined: pickWinners`, `undefined: Claim`, `undefined: decide`, and so on.

- [ ] **Step 4: Write the failing integration tests**

Create `internal/dedup/dedup_test.go`. One Postgres container serves all subtests, and every subtest uses its own keys. The concurrency subtest holds each transaction open for 20 ms after claiming, so the second transaction really blocks on the unique index.

```go
package dedup_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/dedup"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var topicID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

const day = "2026-10-10"

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := testutil.StartPostgres(t)
	ctx := context.Background()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// dedup_claims is partitioned by event_date: create 2026-10-09 .. 2026-10-12.
	if err := pg.EnsurePartitions(ctx, pool, time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	return pool
}

func claim(key string, offset int64, ordinal int, eventID, idem, content string) dedup.Claim {
	return dedup.Claim{
		Dataset: "market_snapshot", Key: key, EventDate: day,
		Pos:     dedup.Position{TopicID: topicID, Partition: 0, Offset: offset, Ordinal: ordinal},
		EventID: eventID, IdempotencyKey: idem, ContentSHA256: sha256.Sum256([]byte(content)),
	}
}

// decide runs ClaimAndDecide in its own committed transaction.
func decide(t *testing.T, pool *pgxpool.Pool, claims ...dedup.Claim) []dedup.Outcome {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	out, err := dedup.ClaimAndDecide(ctx, tx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return out
}

func want(t *testing.T, got []dedup.Outcome, want ...dedup.Outcome) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d outcomes, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("outcome[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

var (
	emit     = dedup.Outcome{Decision: dedup.Emit}
	conflict = dedup.Outcome{Decision: dedup.Conflict}
)

func drop(reason string) dedup.Outcome {
	return dedup.Outcome{Decision: dedup.Drop, DropReason: reason}
}

func TestClaimAndDecide(t *testing.T) {
	pool := setup(t)
	e1, e2, e3 := uuid.NewString(), uuid.NewString(), uuid.NewString()

	t.Run("first claim emits and redelivery of the same position emits again", func(t *testing.T) {
		c := claim("bitcoin|2026-10-10T10:00:00.000Z", 100, 0, e1, "idem-a", "v1")
		want(t, decide(t, pool, c), emit)
		want(t, decide(t, pool, c), emit) // crash after TX1, same batch again
	})

	t.Run("other position with the same content is dropped with the right reason", func(t *testing.T) {
		k := "ethereum|2026-10-10T10:00:00.000Z"
		want(t, decide(t, pool, claim(k, 200, 3, e1, "idem-a", "v1")), emit)
		// same event_id at another offset: the publisher republished the message
		want(t, decide(t, pool, claim(k, 250, 3, e1, "idem-a", "v1")), drop(dedup.ReasonReplay))
		// other event, same idempotency_key: the CDN served the same response twice
		want(t, decide(t, pool, claim(k, 260, 3, e2, "idem-a", "v1")), drop(dedup.ReasonDuplicateResponse))
		// other event, other response, same business content
		want(t, decide(t, pool, claim(k, 270, 3, e3, "idem-b", "v1")), drop(dedup.ReasonDuplicateRecord))
		// empty idempotency keys never match each other
		want(t, decide(t, pool, claim(k, 280, 3, uuid.NewString(), "", "v1")), drop(dedup.ReasonDuplicateRecord))
	})

	t.Run("other position with different content conflicts", func(t *testing.T) {
		k := "tether|2026-10-10T10:00:00.000Z"
		want(t, decide(t, pool, claim(k, 300, 0, e1, "idem-a", "v1")), emit)
		want(t, decide(t, pool, claim(k, 301, 0, e2, "idem-c", "v2")), conflict)
		// the winner itself still emits on redelivery
		want(t, decide(t, pool, claim(k, 300, 0, e1, "idem-a", "v1")), emit)
	})

	t.Run("within one call the lowest offset and ordinal wins regardless of input order", func(t *testing.T) {
		k := "solana|2026-10-10T10:00:00.000Z"
		got := decide(t, pool,
			claim(k, 412, 0, e3, "idem-z", "v1"),
			claim(k, 410, 3, e2, "idem-y", "v1"),
			claim(k, 410, 1, e1, "idem-x", "v2"),
		)
		want(t, got, conflict, conflict, emit)

		k2 := "cardano|2026-10-10T10:00:00.000Z"
		got = decide(t, pool,
			claim(k2, 501, 0, e2, "idem-q", "same"),
			claim(k2, 500, 0, e1, "idem-q", "same"),
		)
		want(t, got, drop(dedup.ReasonDuplicateResponse), emit)
	})

	t.Run("dataset and event_date are part of the key", func(t *testing.T) {
		k := "dogecoin|x"
		a := claim(k, 600, 0, e1, "i", "v1")
		b := claim(k, 601, 0, e2, "i", "v2")
		b.Dataset = "coin_snapshot"
		c := claim(k, 602, 0, e3, "i", "v3")
		c.EventDate = "2026-10-11"
		want(t, decide(t, pool, a, b, c), emit, emit, emit)
	})

	t.Run("concurrent transactions claiming the same key: exactly one emits", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("race-%d|2026-10-10T10:00:00.000Z", i)
			var wg sync.WaitGroup
			results := make([]dedup.Outcome, 2)
			errs := make([]error, 2)
			start := make(chan struct{})
			for g := 0; g < 2; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					ctx := context.Background()
					tx, err := pool.Begin(ctx)
					if err != nil {
						errs[g] = err
						return
					}
					defer tx.Rollback(ctx)
					<-start
					out, err := dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{claim(k, int64(1000+g), 0, uuid.NewString(), "", "same")})
					if err != nil {
						errs[g] = err
						return
					}
					time.Sleep(20 * time.Millisecond) // hold the claim so the other tx has to wait
					errs[g] = tx.Commit(ctx)
					results[g] = out[0]
				}(g)
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			emits := 0
			for _, r := range results {
				switch r {
				case emit:
					emits++
				case drop(dedup.ReasonDuplicateRecord):
				default:
					t.Fatalf("unexpected outcome %+v", r)
				}
			}
			if emits != 1 {
				t.Fatalf("iteration %d: %d emits, want exactly 1 (%+v)", i, emits, results)
			}
		}
	})

	t.Run("rolled back claims do not count", func(t *testing.T) {
		ctx := context.Background()
		k := "polkadot|2026-10-10T10:00:00.000Z"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{claim(k, 700, 0, e1, "", "v1")}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		want(t, decide(t, pool, claim(k, 701, 0, e2, "", "v2")), emit)
	})

	t.Run("missing event_date partition is an error", func(t *testing.T) {
		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		c := claim("x|y", 800, 0, e1, "", "v1")
		c.EventDate = "2031-01-01"
		if _, err := dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{c}); err == nil {
			t.Fatal("expected an error for a date without a partition")
		}
	})

	t.Run("no claims is a no-op", func(t *testing.T) {
		if out := decide(t, pool); len(out) != 0 {
			t.Fatalf("got %v", out)
		}
	})
}
```

- [ ] **Step 5: Implement `internal/dedup/dedup.go`**

```go
// Package dedup implements the single deduplication mechanism of the pipeline
// (spec §7.3): every record bound for processed/ or rejected/ claims its
// natural key in pipeline.dedup_claims; the first claimant wins, and every
// later claimant is compared against the stored winner.
package dedup

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Position identifies one record inside the log: topic incarnation,
// partition, offset of the message and index of the record in the message.
type Position struct {
	TopicID   uuid.UUID
	Partition int32
	Offset    int64
	Ordinal   int
}

// Claim is one record asking for its natural key.
type Claim struct {
	Dataset, Key, EventDate string // EventDate is "YYYY-MM-DD"
	Pos                     Position
	EventID                 string // uuid of the raw event (message)
	IdempotencyKey          string // informational; "" is stored as NULL
	ContentSHA256           [32]byte
}

type Decision int

const (
	Emit     Decision = iota // the key belongs to this record's own position: write it
	Drop                     // another position won with identical content: drop
	Conflict                 // another position won with different content: reject as snapshot_conflict
)

func (d Decision) String() string {
	switch d {
	case Emit:
		return "emit"
	case Drop:
		return "drop"
	case Conflict:
		return "conflict"
	}
	return fmt.Sprintf("Decision(%d)", int(d))
}

// Drop reasons (metric label values only; they do not change behaviour).
const (
	ReasonReplay            = "replay"             // same event_id: the same message published twice
	ReasonDuplicateResponse = "duplicate_response" // same idempotency_key: byte-identical API response from another poll
	ReasonDuplicateRecord   = "duplicate_record"   // same business content from a different response
)

type Outcome struct {
	Decision   Decision
	DropReason string // set only when Decision == Drop
}

type claimKey struct{ dataset, key, date string }

type stored struct {
	pos            Position
	eventID        string
	idempotencyKey string
	sha            []byte
}

// less orders positions: topic id, partition, offset, ordinal.
func less(a, b Position) bool {
	if c := bytes.Compare(a.TopicID[:], b.TopicID[:]); c != 0 {
		return c < 0
	}
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	if a.Offset != b.Offset {
		return a.Offset < b.Offset
	}
	return a.Ordinal < b.Ordinal
}

// ClaimAndDecide inserts claims inside the caller's transaction and returns one
// Outcome per claim, in the same order as claims.
//
// The transaction MUST use READ COMMITTED (pgx default). The INSERT and the
// SELECT are two statements on purpose: when another transaction holds an
// uncommitted claim for the same key, our INSERT waits for it; after it commits
// our INSERT does nothing, and the following SELECT (new snapshot under READ
// COMMITTED) sees the winner. A single CTE statement could not see it.
//
// Partitions of pipeline.dedup_claims for every EventDate must already exist
// (pg.EnsurePartitions); otherwise Postgres returns SQLSTATE 23514.
func ClaimAndDecide(ctx context.Context, tx pgx.Tx, claims []Claim) ([]Outcome, error) {
	if len(claims) == 0 {
		return nil, nil
	}

	// 1. Pre-dedup in Go: one claim per key, the lowest position.
	keys, winners, err := pickWinners(claims)
	if err != nil {
		return nil, err
	}

	n := len(keys)
	var (
		datasets = make([]string, n)
		recKeys  = make([]string, n)
		dates    = make([]string, n)
		topicIDs = make([]string, n)
		parts    = make([]int32, n)
		offsets  = make([]int64, n)
		ordinals = make([]int32, n)
		eventIDs = make([]string, n)
		idemKeys = make([]string, n)
		shas     = make([][]byte, n)
	)
	for i, k := range keys {
		c := claims[winners[k]]
		datasets[i], recKeys[i], dates[i] = k.dataset, k.key, k.date
		topicIDs[i] = c.Pos.TopicID.String()
		parts[i] = c.Pos.Partition
		offsets[i] = c.Pos.Offset
		ordinals[i] = int32(c.Pos.Ordinal)
		eventIDs[i] = c.EventID
		idemKeys[i] = c.IdempotencyKey
		sha := c.ContentSHA256
		shas[i] = sha[:]
	}

	// 2. Bulk insert. Arrays are passed as text[] and cast in SQL so pgx needs
	//    no custom codecs for date/uuid. WITH ORDINALITY + ORDER BY keeps the
	//    sorted insertion order.
	const insertSQL = `
INSERT INTO pipeline.dedup_claims
  (dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal, event_id, idempotency_key, content_sha256)
SELECT u.dataset, u.record_key, u.event_date::date, u.topic_id::uuid, u.kafka_partition, u.kafka_offset, u.ordinal,
       u.event_id::uuid, NULLIF(u.idempotency_key, ''), u.content_sha256
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::bigint[], $7::int[], $8::text[], $9::text[], $10::bytea[])
     WITH ORDINALITY AS u(dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal, event_id, idempotency_key, content_sha256, ord)
ORDER BY u.ord
ON CONFLICT (dataset, record_key, event_date) DO NOTHING`
	if _, err := tx.Exec(ctx, insertSQL, datasets, recKeys, dates, topicIDs, parts, offsets, ordinals, eventIDs, idemKeys, shas); err != nil {
		return nil, fmt.Errorf("dedup: insert claims: %w", err)
	}

	// 3. Read back the stored winner of every key (ours or someone else's).
	const selectSQL = `
SELECT c.dataset, c.record_key, c.event_date::text, c.topic_id::text, c.kafka_partition, c.kafka_offset, c.ordinal,
       c.event_id::text, COALESCE(c.idempotency_key, ''), c.content_sha256
FROM pipeline.dedup_claims c
JOIN unnest($1::text[], $2::text[], $3::text[]) AS k(dataset, record_key, event_date)
  ON c.dataset = k.dataset AND c.record_key = k.record_key AND c.event_date = k.event_date::date`
	rows, err := tx.Query(ctx, selectSQL, datasets, recKeys, dates)
	if err != nil {
		return nil, fmt.Errorf("dedup: select claims: %w", err)
	}
	got := make(map[claimKey]stored, n)
	for rows.Next() {
		var (
			k       claimKey
			s       stored
			topicID string
			part    int32
			ordinal int32
		)
		if err := rows.Scan(&k.dataset, &k.key, &k.date, &topicID, &part, &s.pos.Offset, &ordinal, &s.eventID, &s.idempotencyKey, &s.sha); err != nil {
			rows.Close()
			return nil, fmt.Errorf("dedup: scan claim: %w", err)
		}
		tid, err := uuid.Parse(topicID)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("dedup: stored topic_id: %w", err)
		}
		s.pos.TopicID, s.pos.Partition, s.pos.Ordinal = tid, part, int(ordinal)
		got[k] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dedup: read claims: %w", err)
	}

	// 4. Decide per claim, in input order.
	out := make([]Outcome, len(claims))
	for i, c := range claims {
		s, ok := got[claimKey{c.Dataset, c.Key, c.EventDate}]
		if !ok {
			return nil, fmt.Errorf("dedup: claim for %s/%s/%s vanished", c.Dataset, c.Key, c.EventDate)
		}
		out[i] = decide(s, c)
	}
	return out, nil
}

// pickWinners keeps, per key, only the claim with the lowest position. One
// INSERT statement must not contain the same key twice (DO NOTHING would keep
// an arbitrary one), and the lowest position must win deterministically. The
// keys are returned sorted so that two transactions claiming overlapping key
// sets lock index entries in the same order (no deadlocks).
func pickWinners(claims []Claim) ([]claimKey, map[claimKey]int, error) {
	winners := make(map[claimKey]int, len(claims))
	for i, c := range claims {
		if c.Dataset == "" || c.EventDate == "" {
			return nil, nil, fmt.Errorf("dedup: claim %d has empty dataset or event_date", i)
		}
		if _, err := uuid.Parse(c.EventID); err != nil {
			return nil, nil, fmt.Errorf("dedup: claim %d: event_id %q: %w", i, c.EventID, err)
		}
		k := claimKey{c.Dataset, c.Key, c.EventDate}
		if j, ok := winners[k]; !ok || less(c.Pos, claims[j].Pos) {
			winners[k] = i
		}
	}
	keys := make([]claimKey, 0, len(winners))
	for k := range winners {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool {
		x, y := keys[a], keys[b]
		if x.dataset != y.dataset {
			return x.dataset < y.dataset
		}
		if x.key != y.key {
			return x.key < y.key
		}
		return x.date < y.date
	})
	return keys, winners, nil
}

// decide compares a claim with the stored winner of its key (spec §7.3).
func decide(s stored, c Claim) Outcome {
	sha := c.ContentSHA256
	switch {
	case s.pos == c.Pos:
		return Outcome{Decision: Emit}
	case bytes.Equal(s.sha, sha[:]):
		return Outcome{Decision: Drop, DropReason: dropReason(s, c)}
	default:
		return Outcome{Decision: Conflict}
	}
}

func dropReason(s stored, c Claim) string {
	switch {
	case strings.EqualFold(s.eventID, c.EventID):
		return ReasonReplay
	case c.IdempotencyKey != "" && s.idempotencyKey == c.IdempotencyKey:
		return ReasonDuplicateResponse
	default:
		return ReasonDuplicateRecord
	}
}
```

- [ ] **Step 6: Run the unit tests**

Run: `go test ./internal/dedup/ -run 'TestPickWinners|TestDecide' -v -count=1`
Expected: PASS for `TestPickWinnersKeepsLowestPositionPerKey`, `TestPickWinnersRejectsBadClaims` and `TestDecide`.

- [ ] **Step 7: Run the integration tests (Docker required; skipped without Docker or with `-short`)**

Run: `go mod tidy && go test ./internal/dedup/ -race -count=1 -v`
Expected: PASS, including the subtests `first_claim_emits_and_redelivery_of_the_same_position_emits_again`, `other_position_with_the_same_content_is_dropped_with_the_right_reason`, `other_position_with_different_content_conflicts`, `within_one_call_the_lowest_offset_and_ordinal_wins_regardless_of_input_order`, `concurrent_transactions_claiming_the_same_key:_exactly_one_emits` and `missing_event_date_partition_is_an_error`.

- [ ] **Step 8: Vet and commit**

```bash
go vet ./internal/dedup/ ./migrations/
git add internal/dedup/ go.mod go.sum
git commit -m "$(cat <<'MSG'
feat(dedup): claim natural keys and decide emit/drop/conflict

First claimant wins per (dataset, record_key, event_date). Claims are
pre-deduplicated in Go (lowest offset/ordinal) and inserted in sorted key
order, then read back in a second statement under READ COMMITTED.
Adds idempotency_key to dedup_claims for the duplicate_response reason.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

### Task 15: Transformer

Implements spec §7.4: the batch algorithm (`ProcessBatch`), the `pipeline.consumer_batches` manifest SQL, and the franz-go consumer loop (`Run`). Together they give effectively-once content in `processed/` under crashes, redeliveries, rebalances and zombie consumers.

Background the engineer needs:
- **Manifest (`pipeline.consumer_batches`).** One row per batch `[first..last]` of one partition, keyed by `(topic_id, partition, first_offset)`. An `EXCLUDE USING gist (... int8range(first,last,'[]') WITH &&)` constraint forbids overlapping ranges. A conflicting insert raises SQLSTATE `23P01` (exclusion_violation). That error aborts the current statement, and inside a transaction it aborts the whole transaction, so the insert runs in a **savepoint**. In pgx, `tx.Begin(ctx)` on a `pgx.Tx` creates a savepoint; `Commit` releases it and `Rollback` rolls back to it. `ON CONFLICT (pk) DO NOTHING` handles a retry of the same first offset. Both conflicts wait for a concurrent uncommitted insert of the same range, which serializes zombie consumers.
- **Missing `dedup_claims` partition:** a replay or late snapshot can carry an `event_date` outside the window that `etl init` and the fetcher maintain (today-1 .. today+3). The claim INSERT then fails with SQLSTATE 23514 and no constraint name. `ProcessBatch` detects that, creates day-1 .. day partitions for the snapshot date of every message (`pg.EnsurePartitions(ctx, pool, snapshotAt, 0)`, outside any transaction) and plans once more, the same repair `pg.InsertAPICall` does for `raw.api_calls`.
- **Range rules when the manifest already has a row:** same first offset and planned → reuse the stored range exactly: trim the poll if it is longer (`Result.LastOffset` = stored last), and return `*IncompleteBatchError` if it is shorter. Same or earlier first offset and written → skip (`Result{Written:true, LastOffset: stored last}`). Earlier first offset and planned → `*RewindError` (seek back). A later batch starting inside the poll → trim the poll to end just before it.
- **Determinism.** `_processed_at` is the manifest's `planned_at`, never `now()`. Rows are ordered by offset, then ordinal. Objects are ordered by bucket, then key. Violations are sorted. A retry therefore produces identical bytes. Object keys and SHA-256s are stored on the manifest in TX1; a retry compares them and stops with `ErrNonDeterministic` on any difference.
- **Write-once objects.** `s3.PutIfAbsent` sends `If-None-Match: *`. A 412 means the object exists; equal checksum metadata counts as success, a different one returns `s3.ErrChecksumMismatch` (fatal: alert and stop). No DB transaction is open during PUTs.
- **franz-go.** `DisableAutoCommit` plus synchronous `CommitOffsetsSync` after each written batch. `BlockRebalanceOnPoll` means no partition can move between `PollRecords` and `AllowRebalance()`, so the batch, its commit and any `SetOffsets` seek happen while we own the partition. `AdjustFetchOffsetsFn` runs after committed offsets are fetched for newly assigned partitions and before fetching starts; we use it for assignment recovery (see CONTRACT NOTES §4). Never call `Close`/`LeaveGroup` inside group callbacks: it deadlocks.

**Files:**
- Create: `internal/transformer/manifest.go`
- Create: `internal/transformer/batch.go`
- Create: `internal/transformer/service.go`
- Test: `internal/transformer/keys_test.go` (pure, no Docker)
- Test: `internal/transformer/helpers_test.go` (fixtures and environment helpers)
- Test: `internal/transformer/batch_test.go` (integration: Postgres + versitygw)
- Test: `internal/transformer/service_test.go` (end to end: Redpanda + Postgres + versitygw)

Fixtures used, already copied by Task 4: `schemas/testdata/coingecko/markets_top10_t0.json`, `markets_top10_t1.json` (byte-identical to t0: a CDN cache hit), `markets_top10_t2.json` (the next snapshot), `coin_bitcoin_full_t0.json`, `coin_bitcoin_full_t1.json` (byte-identical to t0).

**Interfaces:**
- Consumes:
  ```go
  func dedup.ClaimAndDecide(ctx context.Context, tx pgx.Tx, claims []dedup.Claim) ([]dedup.Outcome, error)
  func transform.Transform(ev event.RawEvent, rules transform.Rules) ([]transform.Record, error)
  func transform.RowJSON(r transform.Record, lin transform.Lineage) ([]byte, error)
  func transform.SchemaIDFor(dataset string) string
  func event.Unmarshal(b []byte) (event.RawEvent, error)
  func event.Marshal(ev event.RawEvent) ([]byte, error)
  func event.SHA256Hex(b []byte) string
  func event.IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string
  func (r *validation.Registry) Validate(schemaID string, doc []byte) ([]validation.Violation, error)
  func validation.Keyword(v validation.Violation) string
  func validation.Load(fsys fs.FS) (*validation.Registry, error); var schemas.FS embed.FS
  func (c *s3.Client) PutIfAbsent(ctx context.Context, bucket, key string, body []byte) (created bool, err error)
  func (c *s3.Client) Get(ctx context.Context, bucket, key string) ([]byte, error)
  func (c *s3.Client) EnsureBuckets(ctx context.Context, buckets ...string) error
  var s3.ErrChecksumMismatch
  func queue.TopicID(ctx context.Context, cl *kgo.Client, topic string, fallback uuid.UUID) (uuid.UUID, error)
  const queue.HeaderRawObject, queue.HeaderEventID
  func xtime.Format(t time.Time) string; func xtime.FormatDate(t time.Time) string; func xtime.Parse(s string) (time.Time, error)
  func obs.NewMetrics(reg prometheus.Registerer) *obs.Metrics
  func coingecko.MarketsRequest(perPage int) event.Request; func coingecko.CoinDetailRequest(coinID string) event.Request
  func pg.Connect / pg.Migrate / pg.EnsurePartitions (as in Task 14)
  func testutil.StartPostgres(t) string; func testutil.StartVersityGW(t) (endpoint, dataDir string); func testutil.StartRedpanda(t) []string
  ```
- Produces:
  ```go
  // internal/transformer
  type Message struct { Partition int32; Offset int64; Key, Value []byte; Headers map[string]string }
  type Batch struct { TopicID uuid.UUID; Topic string; Partition int32; Messages []Message } // contiguous offsets
  type Deps struct { Pool *pgxpool.Pool; S3 *s3.Client; Reg *validation.Registry; Metrics *obs.Metrics; Log *slog.Logger; Rules transform.Rules }
  type Result struct { Written bool; LastOffset int64 }
  func ProcessBatch(ctx context.Context, d Deps, b Batch) (Result, error)
  func CoveringBatch(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, offset int64) (first, last int64, status string, found bool, err error)
  func ObjectKey(zone, prefix, topic string, partition int32, firstOffset int64) string
  func Run(ctx context.Context, cfg config.Config, d Deps) error
  ```

- [ ] **Step 1: Write the failing pure unit tests**

Create `internal/transformer/keys_test.go`:

```go
package transformer

import (
	"bytes"
	"testing"

	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
)

func TestObjectKey(t *testing.T) {
	cases := []struct {
		zone, prefix string
		want         string
	}{
		{s3.BucketProcessed, "dataset=ticker/v=1/event_date=2026-10-10",
			"dataset=ticker/v=1/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
		{s3.BucketRejected, "dataset=coin_contract/event_date=2026-10-10/",
			"dataset=coin_contract/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
		{s3.BucketDLQ, "stage=transform/date=2026-10-10",
			"stage=transform/date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
	}
	for _, c := range cases {
		if got := ObjectKey(c.zone, c.prefix, "coingecko.raw", 3, 1200); got != c.want {
			t.Errorf("ObjectKey(%q, %q) = %q, want %q", c.zone, c.prefix, got, c.want)
		}
	}
}

func TestBuildObjectsIsDeterministic(t *testing.T) {
	mk := func() map[groupKey][]line {
		return map[groupKey][]line{
			{s3.BucketProcessed, "dataset=b/v=1/event_date=2026-10-10"}: {{offset: 7, ordinal: 1, data: []byte(`{"x":3}`)}, {offset: 5, ordinal: 2, data: []byte(`{"x":2}`)}, {offset: 5, ordinal: 0, data: []byte(`{"x":1}`)}},
			{s3.BucketDLQ, "stage=transform/date=2026-10-10"}:           {{offset: 6, ordinal: -1, data: []byte(`{"d":1}`)}},
			{s3.BucketProcessed, "dataset=a/v=1/event_date=2026-10-10"}: {{offset: 5, ordinal: 0, data: []byte(`{"a":1}`)}},
		}
	}
	a := buildObjects(mk(), "t", 0, 5)
	b := buildObjects(mk(), "t", 0, 5)
	if len(a) != 3 {
		t.Fatalf("got %d objects", len(a))
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Body, b[i].Body) || a[i].SHA256 != b[i].SHA256 {
			t.Fatalf("object %d differs between runs", i)
		}
	}
	if a[0].Bucket != s3.BucketDLQ || a[1].Key != "dataset=a/v=1/event_date=2026-10-10/t+0+00000000000000000005.jsonl" {
		t.Errorf("objects not sorted by bucket then key: %s/%s, %s/%s", a[0].Bucket, a[0].Key, a[1].Bucket, a[1].Key)
	}
	if got := string(a[2].Body); got != "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n" {
		t.Errorf("rows not ordered by offset then ordinal: %q", got)
	}
}

func TestUpTo(t *testing.T) {
	msgs := []Message{{Offset: 10}, {Offset: 11}, {Offset: 13}}
	if n := len(upTo(msgs, 12)); n != 2 {
		t.Errorf("upTo(12) kept %d, want 2", n)
	}
	if n := len(upTo(msgs, 9)); n != 0 {
		t.Errorf("upTo(9) kept %d, want 0", n)
	}
	if n := len(upTo(msgs, 13)); n != 3 {
		t.Errorf("upTo(13) kept %d, want 3", n)
	}
}

func TestSortViolations(t *testing.T) {
	in := []validation.Violation{{InstanceLocation: "/b", KeywordLocation: "k"}, {InstanceLocation: "/a", KeywordLocation: "z"}, {InstanceLocation: "/a", KeywordLocation: "y"}}
	out := sortViolations(in)
	if out[0].KeywordLocation != "y" || out[1].KeywordLocation != "z" || out[2].InstanceLocation != "/b" {
		t.Errorf("unexpected order %+v", out)
	}
	if in[0].InstanceLocation != "/b" {
		t.Error("input slice was modified")
	}
}
```

- [ ] **Step 2: Run them and see them fail**

Run: `go test ./internal/transformer/ -short -v`
Expected: FAIL. The build fails with `undefined: ObjectKey`, `undefined: groupKey`, `undefined: line`, `undefined: buildObjects`, `undefined: Message`, `undefined: upTo`, `undefined: sortViolations`.

- [ ] **Step 3: Write the test helpers**

Create `internal/transformer/helpers_test.go`. `fixtureEvent` wraps a recorded CoinGecko response in an S1 raw event the way the fetcher does. `expected` recomputes the §7.3 outcome from `transform.Transform` independently of the dedup package, so the assertions do not hard-code row counts that depend on the transform task. `countLines` validates every written line against its schema and fails on duplicate processed rows.

```go
package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const fixtureDir = "../../schemas/testdata/coingecko"

// fixtureEvent wraps a recorded CoinGecko response in a raw event (S1), the
// way the fetcher would. fetchedAt must be RFC 3339 UTC with milliseconds.
func fixtureEvent(t *testing.T, file, endpoint, coinID, fetchedAt string) event.RawEvent {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, file))
	if err != nil {
		t.Fatal(err)
	}
	// encoding/json drops whitespace around a RawMessage when decoding, so the
	// payload must not carry any; otherwise payload_sha256 would not verify.
	payload := bytes.TrimSpace(b)
	req := coingecko.MarketsRequest(100)
	if endpoint == event.EndpointCoinDetail {
		req = coingecko.CoinDetailRequest(coinID)
	}
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	date, cache := "Sat, 10 Oct 2026 10:02:33 GMT", "MISS"
	age := 0
	etag := `W/"fixture"`
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        uuid.Must(uuid.NewV7()).String(),
		IdempotencyKey: &idem,
		Source:         event.SourceCoinGecko,
		Endpoint:       endpoint,
		Request:        req,
		FetchedAt:      fetchedAt,
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 120, Date: &date, AgeSeconds: &age, CFCacheStatus: &cache, ETag: &etag},
		PayloadSHA256:  &sha,
		Payload:        payload,
	}
}

func message(t *testing.T, partition int32, offset int64, ev event.RawEvent) Message {
	t.Helper()
	v, err := event.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	key := "test-key" // routing is not under test here
	return Message{Partition: partition, Offset: offset, Key: []byte(key), Value: v, Headers: map[string]string{
		queue.HeaderEventID:   ev.EventID,
		queue.HeaderRawObject: "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl",
	}}
}

// fixtureEvents returns the standard sequence used by the tests:
//
//	0 markets t0, 1 markets t1 (byte-identical CDN copy of t0, new event_id),
//	2 bitcoin detail t0, 3 bitcoin detail t1 (byte-identical), 4 markets t2 (new snapshot)
func fixtureEvents(t *testing.T) []event.RawEvent {
	return []event.RawEvent{
		fixtureEvent(t, "markets_top10_t0.json", event.EndpointMarkets, "", "2026-10-10T10:02:32.000Z"),
		fixtureEvent(t, "markets_top10_t1.json", event.EndpointMarkets, "", "2026-10-10T10:03:07.000Z"),
		fixtureEvent(t, "coin_bitcoin_full_t0.json", event.EndpointCoinDetail, "bitcoin", "2026-10-10T10:02:38.000Z"),
		fixtureEvent(t, "coin_bitcoin_full_t1.json", event.EndpointCoinDetail, "bitcoin", "2026-10-10T10:03:12.000Z"),
		fixtureEvent(t, "markets_top10_t2.json", event.EndpointMarkets, "", "2026-10-10T10:03:42.000Z"),
	}
}

// expected computes what processed/ and rejected/ must contain for a sequence
// of valid events, applying the §7.3 rules independently of the dedup package:
// first sight of a key wins; same content later is dropped; different content
// later is a snapshot_conflict reject.
func expected(t *testing.T, evs []event.RawEvent, rules transform.Rules) (processed, rejected int) {
	t.Helper()
	first := map[string][32]byte{}
	for _, ev := range evs {
		recs, err := transform.Transform(ev, rules)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			k := r.Dataset + "\x00" + r.Key + "\x00" + r.EventDate
			sum, seen := first[k]
			switch {
			case !seen:
				first[k] = r.ContentSHA256
				if r.Zone == transform.ZoneProcessed {
					processed++
				} else {
					rejected++
				}
			case sum != r.ContentSHA256:
				rejected++
			}
		}
	}
	return processed, rejected
}

type env struct {
	deps    Deps
	dataDir string
	reg     *prometheus.Registry
}

// newEnv starts Postgres and versitygw and returns ready Deps.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	dsn := testutil.StartPostgres(t)
	endpoint, dataDir := testutil.StartVersityGW(t)
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Fixture snapshots are dated 2026-10-10: create dedup_claims partitions around it.
	if err := pg.EnsurePartitions(ctx, pool, time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatal(err)
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	preg := prometheus.NewRegistry()
	return &env{
		deps: Deps{
			Pool: pool, S3: s3c, Reg: reg, Metrics: obs.NewMetrics(preg),
			Log:   slog.New(slog.DiscardHandler),
			Rules: transform.Rules{StaleAfter: 10 * time.Minute, TickerStaleAfter: time.Hour, TickerAnomalyPct: 2},
		},
		dataDir: dataDir,
		reg:     preg,
	}
}

// reset empties the pipeline state so subtests do not see each other's claims.
func (e *env) reset(t *testing.T) {
	t.Helper()
	if _, err := e.deps.Pool.Exec(context.Background(),
		`TRUNCATE pipeline.dedup_claims, pipeline.consumer_batches`); err != nil {
		t.Fatal(err)
	}
}

// objects reads every object a manifest row lists, keyed by "bucket/key".
func (e *env) objects(t *testing.T, topicID uuid.UUID, partition int32, first int64) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var status string
	if err := e.deps.Pool.QueryRow(ctx, `SELECT object_keys, status FROM pipeline.consumer_batches
		WHERE topic_id=$1 AND kafka_partition=$2 AND first_offset=$3`, topicID, partition, first).Scan(&keys, &status); err != nil {
		t.Fatal(err)
	}
	if status != statusWritten {
		t.Fatalf("batch %d status = %s, want written", first, status)
	}
	out := map[string][]byte{}
	for _, k := range keys {
		bucket, key, _ := strings.Cut(k, "/")
		b, err := e.deps.S3.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		out[k] = b
	}
	return out
}

// countLines counts JSONL lines per bucket and validates each processed line
// against its dataset schema, each rejected line against S3 rejected_record and
// each dlq line against S3 dlq_record. It also fails on duplicate processed rows
// (same business content).
func (e *env) countLines(t *testing.T, objs map[string][]byte) map[string]int {
	t.Helper()
	counts := map[string]int{}
	seen := map[string]bool{}
	for k, body := range objs {
		bucket, key, _ := strings.Cut(k, "/")
		if len(body) == 0 || body[len(body)-1] != '\n' {
			t.Fatalf("%s: body must end with a newline", k)
		}
		for _, l := range bytes.Split(bytes.TrimSuffix(body, []byte("\n")), []byte("\n")) {
			counts[bucket]++
			var schemaID string
			switch bucket {
			case s3.BucketProcessed:
				ds := strings.TrimPrefix(strings.SplitN(key, "/", 2)[0], "dataset=")
				schemaID = transform.SchemaIDFor(ds)
				biz := businessOnly(t, l)
				if seen[ds+biz] {
					t.Errorf("%s: duplicate processed row %s", k, biz)
				}
				seen[ds+biz] = true
			case s3.BucketRejected:
				schemaID = validation.IDRejectedRecord
			case s3.BucketDLQ:
				schemaID = validation.IDDLQRecord
			}
			vs, err := e.deps.Reg.Validate(schemaID, l)
			if err != nil || len(vs) > 0 {
				t.Errorf("%s: line fails %s: %v %v", k, schemaID, err, vs)
			}
		}
	}
	return counts
}

// businessOnly drops the technical "_" columns of a processed row.
func businessOnly(t *testing.T, row []byte) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(row, &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
```

- [ ] **Step 4: Write the failing integration tests for `ProcessBatch`**

Create `internal/transformer/batch_test.go`. The tests call `ProcessBatch` directly on batches built from the fixtures (partition 0, offsets from 100). They call the internal `planBatch` (TX1 only, no PUT) to simulate a crash after TX1. Each subtest truncates the pipeline tables and uses its own topic name, so object keys never collide in the shared versitygw.

```go
package transformer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/storage/s3"

	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

// batchOf builds a batch on partition 0 starting at offset 100.
func batchOf(t *testing.T, topic string, evs []event.RawEvent, extra ...[]byte) Batch {
	t.Helper()
	b := Batch{TopicID: uuid.New(), Topic: topic, Partition: 0}
	off := int64(100)
	for _, ev := range evs {
		b.Messages = append(b.Messages, message(t, 0, off, ev))
		off++
	}
	for _, v := range extra {
		b.Messages = append(b.Messages, Message{Partition: 0, Offset: off, Value: v, Headers: map[string]string{}})
		off++
	}
	return b
}

func TestProcessBatch(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := e.deps

	t.Run("happy path writes valid objects, drops CDN copies and replays, sends broken messages to dlq", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		// a replay: the publisher sent markets t0 again (same event_id) at a later offset
		withReplay := append(append([]event.RawEvent{}, evs...), evs[0])
		bad := evs[4]
		badSHA := "0000000000000000000000000000000000000000000000000000000000000000"
		bad.PayloadSHA256 = &badSHA
		badValue, err := event.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		b := batchOf(t, "test.happy", withReplay, badValue, []byte(`{"schema_version":1,"schema_version":1}`), []byte(`not json`))

		res, err := ProcessBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		lastOff := b.Messages[len(b.Messages)-1].Offset
		if !res.Written || res.LastOffset != lastOff {
			t.Fatalf("result = %+v, want written up to %d", res, lastOff)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		counts := e.countLines(t, objs)
		wantProcessed, wantRejected := expected(t, evs, d.Rules)
		if wantProcessed == 0 {
			t.Fatal("fixtures produced no processed rows")
		}
		if counts[s3.BucketProcessed] != wantProcessed {
			t.Errorf("processed rows = %d, want %d", counts[s3.BucketProcessed], wantProcessed)
		}
		if counts[s3.BucketRejected] != wantRejected {
			t.Errorf("rejected rows = %d, want %d", counts[s3.BucketRejected], wantRejected)
		}
		if counts[s3.BucketDLQ] != 3 {
			t.Errorf("dlq rows = %d, want 3 (integrity, duplicate key, not json)", counts[s3.BucketDLQ])
		}
		if got := promtest.ToFloat64(d.Metrics.DedupDropped.WithLabelValues("duplicate_response")); got == 0 {
			t.Error("CDN copies (markets t1, bitcoin t1) must be dropped as duplicate_response")
		}
		if got := promtest.ToFloat64(d.Metrics.DedupDropped.WithLabelValues("replay")); got == 0 {
			t.Error("the republished markets t0 must be dropped as replay")
		}
		if got := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageInput, classIntegrity)); got != 1 {
			t.Errorf("dlq integrity_error = %v, want 1", got)
		}
		if got := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageInput, classDecode)); got != 2 {
			t.Errorf("dlq decode_error = %v, want 2", got)
		}
		// The lake is plain files: the processed objects exist on the host.
		for k := range objs {
			if _, err := os.Stat(filepath.Join(e.dataDir, k)); err != nil {
				t.Errorf("object %s is not a plain file under the data dir: %v", k, err)
			}
		}
		// Processing the same batch again (redelivery after TX2) is a no-op.
		res2, err := ProcessBatch(ctx, d, b)
		if err != nil || res2 != res {
			t.Fatalf("second run = %+v, %v; want %+v", res2, err, res)
		}
	})

	t.Run("crash after TX1 then retry writes identical objects without duplicates", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		b := batchOf(t, "test.crash-tx1", evs)
		p, err := planBatch(ctx, d, b) // TX1 committed, process "dies" before any PUT
		if err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if res.LastOffset != 104 {
			t.Fatalf("LastOffset = %d, want 104", res.LastOffset)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		if len(objs) != len(p.objects) {
			t.Fatalf("got %d objects, planned %d", len(objs), len(p.objects))
		}
		for _, o := range p.objects {
			if !bytes.Equal(objs[o.Bucket+"/"+o.Key], o.Body) {
				t.Errorf("%s/%s differs from the first attempt", o.Bucket, o.Key)
			}
		}
		wantProcessed, _ := expected(t, evs, d.Rules)
		if got := e.countLines(t, objs)[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d (records lost or duplicated)", got, wantProcessed)
		}
	})

	t.Run("partial PUTs then retry: existing objects answer 412 with an equal checksum", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.partial", fixtureEvents(t))
		p, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.objects) < 2 {
			t.Fatalf("need at least 2 objects, got %d", len(p.objects))
		}
		if _, err := d.S3.PutIfAbsent(ctx, p.objects[0].Bucket, p.objects[0].Key, p.objects[0].Body); err != nil {
			t.Fatal(err)
		}
		before := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("exists"))
		if _, err := ProcessBatch(ctx, d, b); err != nil {
			t.Fatal(err)
		}
		if got := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("exists")) - before; got != 1 {
			t.Errorf("exists puts = %v, want 1", got)
		}
		e.objects(t, b.TopicID, 0, 100) // all objects readable, status written
	})

	t.Run("an object with different bytes under the same key stops the batch", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.mismatch", fixtureEvents(t))
		p, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.S3.PutIfAbsent(ctx, p.objects[0].Bucket, p.objects[0].Key, []byte("{}\n")); err != nil {
			t.Fatal(err)
		}
		_, err = ProcessBatch(ctx, d, b)
		if !errors.Is(err, s3.ErrChecksumMismatch) {
			t.Fatalf("err = %v, want ErrChecksumMismatch", err)
		}
		_, _, status, found, err := CoveringBatch(ctx, d.Pool, b.TopicID, 0, 100)
		if err != nil || !found || status != statusPlanned {
			t.Fatalf("batch must stay planned: found=%v status=%s err=%v", found, status, err)
		}
	})

	t.Run("zombie: two consumers process the same batch concurrently", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.zombie", fixtureEvents(t))
		var wg sync.WaitGroup
		results := make([]Result, 2)
		errs := make([]error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = ProcessBatch(ctx, d, b)
			}(i)
		}
		wg.Wait()
		for i := range errs {
			if errs[i] != nil {
				t.Fatalf("consumer %d: %v", i, errs[i])
			}
		}
		if results[0] != results[1] || results[0].LastOffset != 104 {
			t.Fatalf("results differ: %+v", results)
		}
		var rows int
		if err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches WHERE topic_id=$1`, b.TopicID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("manifest rows = %d, want 1", rows)
		}
		wantProcessed, _ := expected(t, fixtureEvents(t), d.Rules)
		if got := e.countLines(t, e.objects(t, b.TopicID, 0, 100))[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d", got, wantProcessed)
		}
	})

	t.Run("boundary shift: a planned [F..L] wins over a longer poll [F..L+n]", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		long := batchOf(t, "test.shift", evs)
		short := long
		short.Messages = long.Messages[:3] // [100..102]
		p, err := planBatch(ctx, d, short)
		if err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, long) // polled [100..104] after a restart
		if err != nil {
			t.Fatal(err)
		}
		if res.LastOffset != 102 {
			t.Fatalf("LastOffset = %d, want 102 (the planned range)", res.LastOffset)
		}
		objs := e.objects(t, long.TopicID, 0, 100)
		for _, o := range p.objects {
			if !bytes.Equal(objs[o.Bucket+"/"+o.Key], o.Body) {
				t.Errorf("%s/%s differs from the planned batch", o.Bucket, o.Key)
			}
		}
		// The caller continues with the rest as a new batch.
		rest := long
		rest.Messages = long.Messages[3:]
		res, err = ProcessBatch(ctx, d, rest)
		if err != nil || res.LastOffset != 104 {
			t.Fatalf("rest: %+v %v", res, err)
		}
		// Together the two batches hold every expected row exactly once.
		all := e.objects(t, long.TopicID, 0, 100)
		for k, v := range e.objects(t, long.TopicID, 0, 103) {
			all[k] = v
		}
		wantProcessed, _ := expected(t, evs, d.Rules)
		if got := e.countLines(t, all)[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d", got, wantProcessed)
		}
	})

	t.Run("a shorter poll than the planned range asks for more records", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.incomplete", fixtureEvents(t))
		if _, err := planBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		short := long
		short.Messages = long.Messages[:2]
		_, err := ProcessBatch(ctx, d, short)
		var inc *IncompleteBatchError
		if !errors.As(err, &inc) || inc.NeedLast != 104 {
			t.Fatalf("err = %v, want IncompleteBatchError needing 104", err)
		}
	})

	t.Run("a poll starting inside a planned batch must rewind; inside a written batch it skips", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.rewind", fixtureEvents(t))
		if _, err := planBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		mid := long
		mid.Messages = long.Messages[2:]
		_, err := ProcessBatch(ctx, d, mid)
		var rw *RewindError
		if !errors.As(err, &rw) || rw.Offset != 100 {
			t.Fatalf("err = %v, want RewindError to 100", err)
		}
		if _, err := ProcessBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, mid)
		if err != nil || !res.Written || res.LastOffset != 104 {
			t.Fatalf("mid after written = %+v, %v; want skip to 104", res, err)
		}
	})

	t.Run("a later planned batch inside the poll trims the poll before it", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.trim", fixtureEvents(t))
		tail := long
		tail.Messages = long.Messages[3:] // [103..104]
		if _, err := planBatch(ctx, d, tail); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, long)
		if err != nil || res.LastOffset != 102 {
			t.Fatalf("got %+v, %v; want LastOffset 102", res, err)
		}
		first, last, status, found, err := CoveringBatch(ctx, d.Pool, long.TopicID, 0, 101)
		if err != nil || !found || first != 100 || last != 102 || status != statusWritten {
			t.Fatalf("covering = %d..%d %s %v %v", first, last, status, found, err)
		}
	})

	// versitygw restarted or unreachable in the middle of a batch: the PUTs
	// fail, the manifest stays planned (so the caller commits no offset), and
	// the retry after recovery writes exactly the objects TX1 planned.
	t.Run("object storage down mid-batch: batch stays planned, retry writes the planned objects", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.s3down", fixtureEvents(t))
		down, err := s3.New(ctx, s3.Config{Endpoint: "http://127.0.0.1:1", AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
		if err != nil {
			t.Fatal(err)
		}
		dd := d
		dd.S3 = down
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = ProcessBatch(tctx, dd, b)
		cancel()
		if err == nil {
			t.Fatal("ProcessBatch succeeded although object storage is unreachable")
		}
		_, _, status, found, err := CoveringBatch(ctx, d.Pool, b.TopicID, 0, 100)
		if err != nil || !found || status == statusWritten {
			t.Fatalf("after failed PUTs: status=%s found=%v err=%v, want planned", status, found, err)
		}
		var planned []string
		if err := d.Pool.QueryRow(ctx, `SELECT object_keys FROM pipeline.consumer_batches
			WHERE topic_id=$1 AND kafka_partition=0 AND first_offset=100`, b.TopicID).Scan(&planned); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, b)
		if err != nil || !res.Written || res.LastOffset != 104 {
			t.Fatalf("retry = %+v, %v", res, err)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		if len(objs) != len(planned) {
			t.Fatalf("written %d objects, TX1 planned %d", len(objs), len(planned))
		}
		for _, k := range planned {
			if _, ok := objs[k]; !ok {
				t.Errorf("planned object %s missing after retry", k)
			}
		}
		wantProcessed, wantRejected := expected(t, fixtureEvents(t), d.Rules)
		got := e.countLines(t, objs)
		if got[s3.BucketProcessed] != wantProcessed || got[s3.BucketRejected] != wantRejected {
			t.Fatalf("lines = %v, want processed %d rejected %d", got, wantProcessed, wantRejected)
		}
	})

	// A replay of old raw objects (or a late snapshot) carries an event_date
	// outside the partition window that init and the fetcher maintain. The
	// claim INSERT fails with "no partition"; the batch must create the
	// partition and succeed instead of failing forever.
	t.Run("snapshot dated outside the dedup partition window creates its partition", func(t *testing.T) {
		e.reset(t)
		old := fixtureEvents(t)[0]
		payload := bytes.ReplaceAll(old.Payload, []byte(`"last_updated":"2026-10-10T`), []byte(`"last_updated":"2026-08-01T`))
		if bytes.Equal(payload, old.Payload) {
			t.Fatal("fixture has no 2026-10-10 last_updated to move")
		}
		sha := event.SHA256Hex(payload)
		idem := event.IdempotencyKey(old.Source, old.Endpoint, old.Request.Params, sha)
		old.Payload, old.PayloadSHA256, old.IdempotencyKey = payload, &sha, &idem
		b := batchOf(t, "test.oldpartition", []event.RawEvent{old})
		res, err := ProcessBatch(ctx, d, b)
		if err != nil || !res.Written {
			t.Fatalf("ProcessBatch = %+v, %v", res, err)
		}
		var claims int
		if err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.dedup_claims WHERE event_date = '2026-08-01'`).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims == 0 {
			t.Fatal("no claims stored for 2026-08-01")
		}
		found := false
		for k := range e.objects(t, b.TopicID, 0, 100) {
			found = found || strings.Contains(k, "/event_date=2026-08-01/")
		}
		if !found {
			t.Fatal("no processed object under event_date=2026-08-01")
		}
	})
}
```

- [ ] **Step 5: Implement the manifest SQL in `internal/transformer/manifest.go`**

```go
package transformer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Manifest statuses of pipeline.consumer_batches.
const (
	statusPlanned = "planned"
	statusWritten = "written"
)

// ErrNonDeterministic means a retry of a planned batch produced objects whose
// keys or checksums differ from the ones stored by an earlier attempt. The
// transform or serialization is not deterministic (or the code changed between
// attempts). Alert and stop: never overwrite.
var ErrNonDeterministic = errors.New("transformer: batch objects differ from the stored manifest (non-deterministic transform)")

// manifestRow is one row of pipeline.consumer_batches.
type manifestRow struct {
	First, Last int64
	Status      string
	PlannedAt   time.Time
	ObjectKeys  []string // nil until setObjects
	SHAs        [][]byte
}

const manifestCols = `first_offset, last_offset, status, planned_at, object_keys, content_sha256`

func scanManifest(row pgx.Row) (manifestRow, error) {
	var m manifestRow
	err := row.Scan(&m.First, &m.Last, &m.Status, &m.PlannedAt, &m.ObjectKeys, &m.SHAs)
	return m, err
}

// insertPlanned tries to plan [first..last] for (topicID, partition).
//
// It returns inserted=true and the new row when the range was free. Otherwise
// it returns the stored batch that blocks it: the batch with the same first
// offset (primary key conflict, handled by ON CONFLICT DO NOTHING), or the
// earliest batch overlapping the range (EXCLUDE constraint, SQLSTATE 23P01).
// The INSERT runs inside a savepoint (pgx: tx.Begin on a Tx) because an
// exclusion violation aborts the statement and would otherwise poison TX1.
//
// Both conflicts wait for a concurrent uncommitted insert of the same range to
// finish, so two consumers planning the same batch are serialized here.
func insertPlanned(ctx context.Context, tx pgx.Tx, topicID uuid.UUID, partition int32, first, last int64) (manifestRow, bool, error) {
	sp, err := tx.Begin(ctx) // SAVEPOINT
	if err != nil {
		return manifestRow{}, false, err
	}
	m, err := scanManifest(sp.QueryRow(ctx, `
INSERT INTO pipeline.consumer_batches (topic_id, kafka_partition, first_offset, last_offset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (topic_id, kafka_partition, first_offset) DO NOTHING
RETURNING `+manifestCols, topicID, partition, first, last))
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		if err := sp.Commit(ctx); err != nil { // RELEASE SAVEPOINT
			return manifestRow{}, false, err
		}
		return m, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		if err := sp.Commit(ctx); err != nil {
			return manifestRow{}, false, err
		}
	case errors.As(err, &pgErr) && pgErr.Code == "23P01": // exclusion_violation
		if err := sp.Rollback(ctx); err != nil { // ROLLBACK TO SAVEPOINT
			return manifestRow{}, false, err
		}
	default:
		_ = sp.Rollback(ctx)
		return manifestRow{}, false, fmt.Errorf("plan batch: %w", err)
	}
	m, err = scanManifest(tx.QueryRow(ctx, `
SELECT `+manifestCols+`
FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2
  AND int8range(first_offset, last_offset, '[]') && int8range($3, $4, '[]')
ORDER BY first_offset
LIMIT 1`, topicID, partition, first, last))
	if err != nil {
		return manifestRow{}, false, fmt.Errorf("load conflicting batch: %w", err)
	}
	return m, false, nil
}

// CoveringBatch returns the manifest batch whose [first..last] contains offset.
func CoveringBatch(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, offset int64) (first, last int64, status string, found bool, err error) {
	err = pool.QueryRow(ctx, `
SELECT first_offset, last_offset, status
FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2
  AND int8range(first_offset, last_offset, '[]') @> $3::bigint`,
		topicID, partition, offset).Scan(&first, &last, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, "", false, nil
	}
	if err != nil {
		return 0, 0, "", false, err
	}
	return first, last, status, true, nil
}

// setObjects stores the object keys, checksums and counts of a planned batch.
// If an earlier attempt already stored them, it compares instead and returns
// ErrNonDeterministic on any difference.
func setObjects(ctx context.Context, tx pgx.Tx, topicID uuid.UUID, partition int32, first int64, keys []string, shas [][]byte, counts []byte) error {
	tag, err := tx.Exec(ctx, `
UPDATE pipeline.consumer_batches
SET object_keys = $4, content_sha256 = $5, counts = $6
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3 AND object_keys IS NULL`,
		topicID, partition, first, keys, shas, counts)
	if err != nil {
		return fmt.Errorf("store batch objects: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var storedKeys []string
	var storedSHAs [][]byte
	if err := tx.QueryRow(ctx, `
SELECT object_keys, content_sha256 FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3`,
		topicID, partition, first).Scan(&storedKeys, &storedSHAs); err != nil {
		return fmt.Errorf("load stored batch objects: %w", err)
	}
	if !slices.Equal(storedKeys, keys) || !slices.EqualFunc(storedSHAs, shas, bytes.Equal) {
		return fmt.Errorf("%w: partition %d first offset %d: stored %v, now %v", ErrNonDeterministic, partition, first, storedKeys, keys)
	}
	return nil
}

// markWritten flips a planned batch to written (TX2). It reports whether this
// call did the flip, so metrics are counted once per batch even when a zombie
// consumer finishes the same batch.
func markWritten(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, first int64) (bool, error) {
	tag, err := pool.Exec(ctx, `
UPDATE pipeline.consumer_batches
SET status = 'written', written_at = now()
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3 AND status = 'planned'`,
		topicID, partition, first)
	if err != nil {
		return false, fmt.Errorf("mark batch written: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
```

- [ ] **Step 6: Implement `ProcessBatch` in `internal/transformer/batch.go`**

Notes on the code:
- `jsontext.Value(b).IsValid()` (package `encoding/json/jsontext`, Go 1.27) rejects invalid UTF-8 and duplicate object member names. `encoding/json` v1 accepts both silently.
- `planBatch` is steps 1–2 of §7.4 and ends with COMMIT. `ProcessBatch` adds steps 3–4. Metrics are counted only by the call that flips the manifest to `written`, so a zombie or a retry does not double-count.
- Messages that fail input checks stay inside the batch range (their offsets get committed) and go to one dlq object per batch: `dlq/stage=transform/date=<planned_at date>/<topic>+<partition>+<first>.jsonl`.

```go
// Package transformer consumes raw events from the broker and writes the
// processed, rejected and dlq zones with effectively-once content (spec §7.4).
package transformer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"coingecko-etl/internal/dedup"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message is one broker record of the raw topic.
type Message struct {
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// Batch is a contiguous offset range [F..L] of one partition.
type Batch struct {
	TopicID   uuid.UUID
	Topic     string
	Partition int32
	Messages  []Message
}

type Deps struct {
	Pool    *pgxpool.Pool
	S3      *s3.Client
	Reg     *validation.Registry
	Metrics *obs.Metrics
	Log     *slog.Logger
	Rules   transform.Rules
}

// Result: Written is true when every offset up to LastOffset is durably in the
// lake and the manifest says so; the caller commits LastOffset+1. LastOffset can
// be lower than the last message (the manifest fixed a shorter range: process
// the rest in a new batch) or higher (a written batch already covers more:
// seek past it).
type Result struct {
	Written    bool
	LastOffset int64
}

// IncompleteBatchError: an earlier attempt planned this batch with a longer
// range than the messages given. The caller must fetch up to NeedLast and call
// ProcessBatch again with the longer batch.
type IncompleteBatchError struct {
	Partition       int32
	First, NeedLast int64
	HaveLast        int64
}

func (e *IncompleteBatchError) Error() string {
	return fmt.Sprintf("transformer: partition %d: manifest planned [%d..%d], batch ends at %d; fetch more records",
		e.Partition, e.First, e.NeedLast, e.HaveLast)
}

// RewindError: a planned (unwritten) batch starts before the first message and
// covers it. The caller must seek the partition to Offset and retry.
type RewindError struct {
	Partition int32
	Offset    int64
}

func (e *RewindError) Error() string {
	return fmt.Sprintf("transformer: partition %d: planned batch starts at %d; rewind", e.Partition, e.Offset)
}

// Stage and error-class labels for dlq records and metrics.
const (
	stageInput   = "transform_input"
	stageOutput  = "transform_output"
	dlqPathStage = "transform" // the stage=<...> segment of dlq object keys

	classDecode       = "decode_error"
	classSchema       = "schema_violation"
	classIntegrity    = "integrity_error"
	classSourceSchema = "source_schema_violation"
	classTransform    = "transform_error"
	classOutputSchema = "output_schema_violation"

	reasonSnapshotConflict = "snapshot_conflict"

	processedSchemaVersion = 1
	putAttempts            = 5
)

// rejectedRow is one line of a rejected/ object (schema pipeline/rejected_record.v1,
// Task 3): the reject envelope, the lineage columns and the row itself.
type rejectedRow struct {
	SchemaVersion int             `json:"schema_version"`
	Dataset       string          `json:"dataset"`
	Reason        string          `json:"reason"`
	RecordKey     string          `json:"record_key"`
	EventDate     string          `json:"event_date"`
	Record        json.RawMessage `json:"record"` // transform.RowJSON: business fields + lineage
	EventID       string          `json:"_event_id"`
	RawObject     string          `json:"_raw_object"`
	Partition     int32           `json:"_kafka_partition"`
	Offset        int64           `json:"_kafka_offset"`
	ProcessedAt   string          `json:"_processed_at"`
}

// dlqRow is one line of a dlq/ object written by the transformer (schema
// pipeline/dlq_record.v1, Task 3; the exporter and publisher write the same shape).
type dlqRow struct {
	SchemaVersion  int                    `json:"schema_version"`
	Stage          string                 `json:"stage"`
	ErrorClass     string                 `json:"error_class"`
	Error          string                 `json:"error"`
	Violations     []validation.Violation `json:"violations"`
	Origin         dlqOrigin              `json:"origin"`
	OriginalBase64 string                 `json:"original_base64"` // message value (input) or row (output)
	OccurredAt     string                 `json:"occurred_at"`     // manifest planned_at, never now()
}

type dlqOrigin struct {
	Kind      string `json:"kind"` // "kafka"
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	EventID   string `json:"event_id,omitempty"`
	Dataset   string `json:"dataset,omitempty"`
	RecordKey string `json:"record_key,omitempty"`
}

// tally is stored as consumer_batches.counts and drives metrics once the batch is written.
type tally struct {
	Messages      int            `json:"messages"`
	Written       map[string]int `json:"written"`        // dataset -> processed rows
	Rejected      map[string]int `json:"rejected"`       // dataset -> rejected rows
	RejectReasons map[string]int `json:"reject_reasons"` // reason -> rows
	Duplicates    map[string]int `json:"duplicates"`     // dataset -> dropped rows
	DropReasons   map[string]int `json:"drop_reasons"`   // reason -> dropped rows
	DLQ           map[string]int `json:"dlq"`            // "<stage>/<error_class>" -> records
	Unwritable    int            `json:"unwritable"`     // rejected/dlq rows failing their own schema (logged only)
}

func newTally() tally {
	return tally{Written: map[string]int{}, Rejected: map[string]int{}, RejectReasons: map[string]int{},
		Duplicates: map[string]int{}, DropReasons: map[string]int{}, DLQ: map[string]int{}}
}

// object is one write-once file of the batch.
type object struct {
	Bucket, Key string
	Body        []byte
	SHA256      [32]byte
}

// plan is the durable outcome of TX1.
type plan struct {
	first, last    int64
	alreadyWritten bool // a written batch covers [first..last]: nothing to do
	plannedAt      time.Time
	objects        []object
	counts         tally
	fetchedAt      []time.Time // of valid messages, for the e2e latency metric
}

// line is one serialized row with its sort position.
type line struct {
	offset  int64
	ordinal int
	data    []byte
}

type groupKey struct{ bucket, prefix string }

// ProcessBatch runs the batch algorithm of spec §7.4:
// input checks, TX1 (plan + transform + dedup + validate + build objects),
// PUT each object with If-None-Match, TX2 (mark written). It never holds a
// database transaction across S3 I/O. Calling it again for the same batch,
// concurrently or after a crash at any point, produces the same objects.
func ProcessBatch(ctx context.Context, d Deps, b Batch) (Result, error) {
	start := time.Now()
	p, err := planBatch(ctx, d, b)
	if isMissingPartition(err) {
		// A replay of old raw objects or a late snapshot can carry an
		// event_date outside the dedup_claims partitions kept by init and the
		// fetcher (today-1 .. today+3). TX1 rolled back; create the partitions
		// this batch needs, outside any transaction, and plan once more.
		if perr := ensureClaimPartitions(ctx, d, b); perr != nil {
			return Result{}, errors.Join(err, perr)
		}
		d.Log.Warn("created dedup_claims partitions for an out-of-window event_date", "stage", "transform", "partition", b.Partition)
		p, err = planBatch(ctx, d, b)
	}
	if err != nil {
		return Result{}, err
	}
	if p.alreadyWritten {
		return Result{Written: true, LastOffset: p.last}, nil
	}
	for _, o := range p.objects {
		if err := putWithRetry(ctx, d, o); err != nil {
			return Result{}, err
		}
	}
	flipped, err := markWritten(ctx, d.Pool, b.TopicID, b.Partition, p.first)
	if err != nil {
		return Result{}, err
	}
	if flipped {
		recordMetrics(d, p, start)
		d.Log.Info("batch written", "stage", "transform", "partition", b.Partition,
			"batch", fmt.Sprintf("%d-%d", p.first, p.last), "objects", len(p.objects), "counts", p.counts)
	}
	return Result{Written: true, LastOffset: p.last}, nil
}

// isMissingPartition reports Postgres' "no partition of relation found for
// row": SQLSTATE 23514 like a CHECK violation, but without a constraint name.
func isMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && pgErr.ConstraintName == ""
}

// ensureClaimPartitions creates the daily partitions (day-1 .. day) for the
// snapshot date of every decodable ok message in the batch.
func ensureClaimPartitions(ctx context.Context, d Deps, b Batch) error {
	done := map[string]bool{}
	for _, m := range b.Messages {
		ev, err := event.Unmarshal(m.Value)
		if err != nil || ev.Outcome != event.OutcomeOK {
			continue
		}
		at, err := transform.SnapshotAt(ev)
		if err != nil || done[xtime.FormatDate(at)] {
			continue
		}
		done[xtime.FormatDate(at)] = true
		if err := pg.EnsurePartitions(ctx, d.Pool, at, 0); err != nil {
			return err
		}
	}
	return nil
}

// putWithRetry writes one object with If-None-Match: *. A 412 with an equal
// checksum counts as success. A different checksum is fatal (alert and stop).
// Transient errors are retried with backoff and finally returned, so the
// caller never commits the offset.
func putWithRetry(ctx context.Context, d Deps, o object) error {
	backoff := 250 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= putAttempts; attempt++ {
		created, err := d.S3.PutIfAbsent(ctx, o.Bucket, o.Key, o.Body)
		switch {
		case err == nil && created:
			d.Metrics.S3Puts.WithLabelValues("created").Inc()
			return nil
		case err == nil:
			d.Metrics.S3Puts.WithLabelValues("exists").Inc()
			return nil
		case errors.Is(err, s3.ErrChecksumMismatch):
			d.Metrics.S3Puts.WithLabelValues("mismatch").Inc()
			d.Log.Error("object exists with a different checksum", "stage", "transform", "bucket", o.Bucket, "key", o.Key)
			return fmt.Errorf("put %s/%s: %w", o.Bucket, o.Key, err)
		}
		d.Metrics.S3Puts.WithLabelValues("error").Inc()
		lastErr = err
		d.Log.Warn("put failed, retrying", "stage", "transform", "bucket", o.Bucket, "key", o.Key, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("put %s/%s: %w", o.Bucket, o.Key, lastErr)
}

func recordMetrics(d Deps, p *plan, start time.Time) {
	m := d.Metrics
	for ds, n := range p.counts.Written {
		m.Records.WithLabelValues(ds, "written").Add(float64(n))
	}
	for ds, n := range p.counts.Rejected {
		m.Records.WithLabelValues(ds, "rejected").Add(float64(n))
	}
	for r, n := range p.counts.RejectReasons {
		m.Rejected.WithLabelValues(r).Add(float64(n))
	}
	for ds, n := range p.counts.Duplicates {
		m.Records.WithLabelValues(ds, "duplicate").Add(float64(n))
	}
	for r, n := range p.counts.DropReasons {
		m.DedupDropped.WithLabelValues(r).Add(float64(n))
	}
	for k, n := range p.counts.DLQ {
		stage, class, _ := strings.Cut(k, "/")
		m.DLQ.WithLabelValues(stage, class).Add(float64(n))
		m.Records.WithLabelValues("none", "dlq").Add(float64(n))
	}
	now := time.Now()
	for _, t := range p.fetchedAt {
		m.E2ELatency.Observe(now.Sub(t).Seconds())
	}
	m.BatchDuration.Observe(time.Since(start).Seconds())
}

// planBatch runs steps 1 and 2 of §7.4 and commits TX1. It does no S3 I/O.
func planBatch(ctx context.Context, d Deps, b Batch) (*plan, error) {
	msgs := b.Messages
	if len(msgs) == 0 {
		return nil, errors.New("transformer: empty batch")
	}
	for i, m := range msgs {
		if m.Partition != b.Partition {
			return nil, fmt.Errorf("transformer: message at offset %d is from partition %d, batch is %d", m.Offset, m.Partition, b.Partition)
		}
		if i > 0 && m.Offset <= msgs[i-1].Offset {
			return nil, fmt.Errorf("transformer: offsets not increasing at index %d", i)
		}
	}

	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// 2a. Fix the batch range in the manifest.
	first, last := msgs[0].Offset, msgs[len(msgs)-1].Offset
	var plannedAt time.Time
	for {
		row, inserted, err := insertPlanned(ctx, tx, b.TopicID, b.Partition, first, last)
		if err != nil {
			return nil, err
		}
		if inserted {
			plannedAt = row.PlannedAt
			break
		}
		if row.Status == statusWritten && row.First <= first {
			// Already done (redelivery after TX2, zombie, or rewind into a written batch).
			return &plan{first: row.First, last: row.Last, alreadyWritten: true}, nil
		}
		if row.First < first {
			return nil, &RewindError{Partition: b.Partition, Offset: row.First}
		}
		if row.First > first {
			// A later batch starts inside our range: stop just before it.
			msgs = upTo(msgs, row.First-1)
			last = msgs[len(msgs)-1].Offset
			continue
		}
		// Same first offset, planned by an earlier attempt: reuse its exact range.
		if row.Last > last {
			return nil, &IncompleteBatchError{Partition: b.Partition, First: first, NeedLast: row.Last, HaveLast: last}
		}
		msgs = upTo(msgs, row.Last)
		last = row.Last
		plannedAt = row.PlannedAt
		break
	}
	processedAt := xtime.Format(plannedAt)

	p := &plan{first: first, last: last, plannedAt: plannedAt, counts: newTally()}
	p.counts.Messages = len(msgs)
	groups := map[groupKey][]line{}
	var dlqLines []line
	addDLQ := func(r dlqRow, offset int64, ordinal int) {
		r.SchemaVersion = 1
		r.Origin.Kind, r.Origin.Topic, r.Origin.Partition, r.Origin.Offset = "kafka", b.Topic, b.Partition, offset
		r.OccurredAt = processedAt
		if r.Violations == nil {
			r.Violations = []validation.Violation{}
		}
		data, err := json.Marshal(r)
		if err == nil {
			if vs, verr := d.Reg.Validate(validation.IDDLQRecord, data); verr != nil || len(vs) > 0 {
				err = fmt.Errorf("dlq record fails its schema: %v %v", verr, vs)
			}
		}
		if err != nil {
			p.counts.Unwritable++
			d.Log.Error("cannot write dlq record", "stage", "transform", "offset", offset, "err", err,
				"original_base64", r.OriginalBase64)
			return
		}
		p.counts.DLQ[r.Stage+"/"+r.ErrorClass]++
		dlqLines = append(dlqLines, line{offset: offset, ordinal: ordinal, data: data})
	}

	// 1. Input checks (no DB). Failures go to dlq; the offsets stay in the batch.
	type valid struct {
		msg Message
		ev  event.RawEvent
	}
	var good []valid
	for _, m := range msgs {
		ev, class, vs, err := checkInput(d, m.Value)
		if err != nil {
			r := dlqRow{Stage: stageInput, ErrorClass: class, Error: err.Error(), Violations: vs,
				Origin: dlqOrigin{EventID: ev.EventID}, OriginalBase64: base64.StdEncoding.EncodeToString(m.Value)}
			addDLQ(r, m.Offset, -1)
			continue
		}
		good = append(good, valid{m, ev})
		if t, err := xtime.Parse(ev.FetchedAt); err == nil {
			p.fetchedAt = append(p.fetchedAt, t)
		}
	}

	// 2b. Transform.
	type pending struct {
		msg Message
		ev  event.RawEvent
		rec transform.Record
	}
	var recs []pending
	var claims []dedup.Claim
	for _, g := range good {
		out, err := transform.Transform(g.ev, d.Rules)
		if err != nil {
			addDLQ(dlqRow{Stage: stageInput, ErrorClass: classTransform, Error: err.Error(), Origin: dlqOrigin{EventID: g.ev.EventID},
				OriginalBase64: base64.StdEncoding.EncodeToString(g.msg.Value)}, g.msg.Offset, -1)
			continue
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
		idem := ""
		if g.ev.IdempotencyKey != nil {
			idem = *g.ev.IdempotencyKey
		}
		for _, r := range out {
			recs = append(recs, pending{g.msg, g.ev, r})
			claims = append(claims, dedup.Claim{
				Dataset: r.Dataset, Key: r.Key, EventDate: r.EventDate,
				Pos:     dedup.Position{TopicID: b.TopicID, Partition: b.Partition, Offset: g.msg.Offset, Ordinal: r.Ordinal},
				EventID: g.ev.EventID, IdempotencyKey: idem, ContentSHA256: r.ContentSHA256,
			})
		}
	}

	// 2c. Claim natural keys and decide.
	outcomes, err := dedup.ClaimAndDecide(ctx, tx, claims)
	if err != nil {
		return nil, err
	}

	// 2d. Serialize and validate.
	for i, pr := range recs {
		r := pr.rec
		switch outcomes[i].Decision {
		case dedup.Drop:
			p.counts.Duplicates[r.Dataset]++
			p.counts.DropReasons[outcomes[i].DropReason]++
			continue
		case dedup.Conflict:
			r.Zone, r.Reason = transform.ZoneRejected, reasonSnapshotConflict
		}
		lin := transform.Lineage{
			EventID: pr.ev.EventID, RawObject: pr.msg.Headers[queue.HeaderRawObject],
			Partition: b.Partition, Offset: pr.msg.Offset, SchemaVer: processedSchemaVersion, ProcessedAt: processedAt,
		}
		row, err := transform.RowJSON(r, lin)
		if err != nil {
			return nil, fmt.Errorf("serialize %s %s: %w", r.Dataset, r.Key, err)
		}
		if r.Zone == transform.ZoneRejected {
			data, err := json.Marshal(rejectedRow{SchemaVersion: 1, Dataset: r.Dataset, Reason: r.Reason,
				RecordKey: r.Key, EventDate: r.EventDate, Record: row,
				EventID: lin.EventID, RawObject: lin.RawObject, Partition: lin.Partition, Offset: lin.Offset, ProcessedAt: lin.ProcessedAt})
			if err == nil {
				if vs, verr := d.Reg.Validate(validation.IDRejectedRecord, data); verr != nil || len(vs) > 0 {
					err = fmt.Errorf("rejected record fails its schema: %v %v", verr, vs)
				}
			}
			if err != nil {
				p.counts.Unwritable++
				d.Log.Error("cannot write rejected record", "stage", "transform", "offset", pr.msg.Offset,
					"dataset", r.Dataset, "err", err, "original_base64", base64.StdEncoding.EncodeToString(row))
				continue
			}
			p.counts.Rejected[r.Dataset]++
			p.counts.RejectReasons[r.Reason]++
			k := groupKey{s3.BucketRejected, fmt.Sprintf("dataset=%s/event_date=%s", r.Dataset, r.EventDate)}
			groups[k] = append(groups[k], line{pr.msg.Offset, r.Ordinal, data})
			continue
		}
		schemaID := transform.SchemaIDFor(r.Dataset)
		vs, verr := d.Reg.Validate(schemaID, row)
		result := "pass"
		if verr != nil || len(vs) > 0 {
			result = "fail"
		}
		d.Metrics.SchemaValidations.WithLabelValues(stageOutput, r.Dataset+".v1", result).Inc()
		if result == "fail" {
			for _, v := range vs {
				d.Metrics.SchemaViolations.WithLabelValues(stageOutput, r.Dataset+".v1", validation.Keyword(v)).Inc()
			}
			msg := "processed row fails its schema"
			if verr != nil {
				msg = verr.Error()
			}
			addDLQ(dlqRow{Stage: stageOutput, ErrorClass: classOutputSchema, Error: msg, Violations: sortViolations(vs),
				Origin:         dlqOrigin{EventID: pr.ev.EventID, Dataset: r.Dataset, RecordKey: r.Key},
				OriginalBase64: base64.StdEncoding.EncodeToString(row)},
				pr.msg.Offset, r.Ordinal)
			continue
		}
		p.counts.Written[r.Dataset]++
		k := groupKey{s3.BucketProcessed, fmt.Sprintf("dataset=%s/v=1/event_date=%s", r.Dataset, r.EventDate)}
		groups[k] = append(groups[k], line{pr.msg.Offset, r.Ordinal, row})
	}
	if len(dlqLines) > 0 {
		k := groupKey{s3.BucketDLQ, fmt.Sprintf("stage=%s/date=%s", dlqPathStage, xtime.FormatDate(plannedAt))}
		groups[k] = dlqLines
	}

	// 2e. Build deterministic objects and store keys + checksums on the manifest.
	p.objects = buildObjects(groups, b.Topic, b.Partition, first)
	keys := make([]string, len(p.objects))
	shas := make([][]byte, len(p.objects))
	for i, o := range p.objects {
		keys[i] = o.Bucket + "/" + o.Key
		sum := o.SHA256
		shas[i] = sum[:]
	}
	counts, err := json.Marshal(p.counts) // map keys are sorted by encoding/json
	if err != nil {
		return nil, err
	}
	if err := setObjects(ctx, tx, b.TopicID, b.Partition, first, keys, shas, counts); err != nil {
		if errors.Is(err, ErrNonDeterministic) {
			d.Metrics.S3Puts.WithLabelValues("mismatch").Inc()
			d.Log.Error("non-deterministic batch", "stage", "transform", "partition", b.Partition, "first", first, "err", err)
		}
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit TX1: %w", err)
	}
	return p, nil
}

// checkInput runs the §7.4 step 1 checks on one message value.
func checkInput(d Deps, value []byte) (event.RawEvent, string, []validation.Violation, error) {
	// Strict JSON: valid UTF-8 and no duplicate object member names.
	if !jsontext.Value(value).IsValid() {
		return event.RawEvent{}, classDecode, nil, errors.New("message value is not valid JSON (syntax, UTF-8 or duplicate names)")
	}
	vs, err := d.Reg.Validate(validation.IDRawEvent, value)
	if err != nil {
		return event.RawEvent{}, classDecode, nil, err
	}
	d.Metrics.SchemaValidations.WithLabelValues(stageInput, "raw_event.v1", passFail(vs)).Inc()
	if len(vs) > 0 {
		for _, v := range vs {
			d.Metrics.SchemaViolations.WithLabelValues(stageInput, "raw_event.v1", validation.Keyword(v)).Inc()
		}
		return event.RawEvent{}, classSchema, sortViolations(vs), errors.New("raw event violates " + validation.IDRawEvent)
	}
	ev, err := event.Unmarshal(value)
	if err != nil {
		return event.RawEvent{}, classDecode, nil, err
	}
	if ev.Outcome != event.OutcomeOK || ev.PayloadSHA256 == nil {
		return ev, classIntegrity, nil, fmt.Errorf("outcome %q has no payload to transform", ev.Outcome)
	}
	if got := event.SHA256Hex(ev.Payload); got != *ev.PayloadSHA256 {
		return ev, classIntegrity, nil, fmt.Errorf("payload sha256 %s does not match payload_sha256 %s", got, *ev.PayloadSHA256)
	}
	var schemaID, schemaName string
	switch ev.Endpoint {
	case event.EndpointMarkets:
		schemaID, schemaName = validation.IDSourceMarkets, "coins_markets.v1"
	case event.EndpointCoinDetail:
		schemaID, schemaName = validation.IDSourceCoin, "coin.v1"
	default:
		return ev, classSourceSchema, nil, fmt.Errorf("unknown endpoint %q", ev.Endpoint)
	}
	vs, err = d.Reg.Validate(schemaID, ev.Payload)
	if err != nil {
		return ev, classDecode, nil, err
	}
	d.Metrics.SchemaValidations.WithLabelValues(stageInput, schemaName, passFail(vs)).Inc()
	if len(vs) > 0 {
		for _, v := range vs {
			d.Metrics.SchemaViolations.WithLabelValues(stageInput, schemaName, validation.Keyword(v)).Inc()
		}
		return ev, classSourceSchema, sortViolations(vs), errors.New("payload violates " + schemaID)
	}
	return ev, "", nil, nil
}

// passFail is the result label of etl_schema_validations_total (all stages use pass|fail).
func passFail(vs []validation.Violation) string {
	if len(vs) > 0 {
		return "fail"
	}
	return "pass"
}

// sortViolations makes dlq bytes independent of validator traversal order.
func sortViolations(vs []validation.Violation) []validation.Violation {
	out := append([]validation.Violation(nil), vs...)
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.InstanceLocation != b.InstanceLocation {
			return a.InstanceLocation < b.InstanceLocation
		}
		if a.KeywordLocation != b.KeywordLocation {
			return a.KeywordLocation < b.KeywordLocation
		}
		return a.Message < b.Message
	})
	return out
}

// upTo returns the messages with offset <= last.
func upTo(msgs []Message, last int64) []Message {
	n := sort.Search(len(msgs), func(i int) bool { return msgs[i].Offset > last })
	return msgs[:n]
}

// buildObjects serializes each group: rows ordered by offset then ordinal,
// one JSON document per line, "\n" after every line. Objects are ordered by
// bucket then key so the manifest arrays are deterministic.
func buildObjects(groups map[groupKey][]line, topic string, partition int32, first int64) []object {
	out := make([]object, 0, len(groups))
	for k, lines := range groups {
		sort.SliceStable(lines, func(i, j int) bool {
			if lines[i].offset != lines[j].offset {
				return lines[i].offset < lines[j].offset
			}
			return lines[i].ordinal < lines[j].ordinal
		})
		var buf bytes.Buffer
		for _, l := range lines {
			buf.Write(l.data)
			buf.WriteByte('\n')
		}
		body := buf.Bytes()
		out = append(out, object{Bucket: k.bucket, Key: ObjectKey(k.bucket, k.prefix, topic, partition, first),
			Body: body, SHA256: sha256.Sum256(body)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bucket != out[j].Bucket {
			return out[i].Bucket < out[j].Bucket
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// ObjectKey returns the key (relative to the bucket named by zone) of the
// object a batch writes: <prefix>/<topic>+<partition>+<%020d first offset>.jsonl,
// the Confluent S3 sink convention. prefix is the Hive-style directory part:
//
//	processed: dataset=<dataset>/v=1/event_date=<YYYY-MM-DD>
//	rejected:  dataset=<dataset>/event_date=<YYYY-MM-DD>
//	dlq:       stage=<stage>/date=<YYYY-MM-DD>
//
// zone is the bucket (s3.BucketProcessed, s3.BucketRejected or s3.BucketDLQ);
// it is not part of the key and is accepted so call sites name both halves of
// the object address together. An empty zone or prefix panics: it is a bug.
func ObjectKey(zone, prefix, topic string, partition int32, firstOffset int64) string {
	if zone == "" || prefix == "" {
		panic("transformer.ObjectKey: empty zone or prefix")
	}
	return fmt.Sprintf("%s/%s+%d+%020d.jsonl", strings.Trim(prefix, "/"), topic, partition, firstOffset)
}
```

- [ ] **Step 7: Run the unit and integration tests**

Run: `go mod tidy && go test ./internal/transformer/ -short -count=1 -v`
Expected: PASS for `TestObjectKey`, `TestBuildObjectsIsDeterministic`, `TestUpTo`, `TestSortViolations`. `TestProcessBatch` is SKIPPED in short mode.

Run (Docker required): `go test ./internal/transformer/ -race -count=1 -run 'TestProcessBatch' -v`
Expected: PASS for all subtests: the happy path (CDN copies dropped as `duplicate_response`, the republished message dropped as `replay`, 3 dlq lines: `integrity_error` and 2× `decode_error`, objects visible as plain files under the versitygw data dir); crash after TX1; partial PUTs (exactly 1 `exists` put); checksum mismatch (batch stays `planned`); zombie (one manifest row, identical results); boundary shift (LastOffset 102 of a 100..104 poll); incomplete poll; rewind/skip; trim before a later batch; object storage down mid-batch (batch stays `planned`, the retry writes exactly the planned objects); a snapshot dated outside the partition window (2026-08-01) creates its `dedup_claims` partition and is written.

- [ ] **Step 8: Commit the batch algorithm**

```bash
go vet ./internal/transformer/
git add internal/transformer/manifest.go internal/transformer/batch.go internal/transformer/keys_test.go internal/transformer/helpers_test.go internal/transformer/batch_test.go go.mod go.sum
git commit -m "$(cat <<'MSG'
feat(transformer): batch manifest and ProcessBatch (TX1, PUT, TX2)

Input checks to dlq, manifest range fixing with savepoint-guarded
exclusion handling, transform + dedup claims, S2/S3 validation,
deterministic objects with stored checksums, write-once PUTs.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

- [ ] **Step 9: Write the failing end-to-end test for `Run`**

Create `internal/transformer/service_test.go`. It creates the topic with kadm (`CreateTopic(ctx, partitions, replicationFactor, configs, topic)`) and produces the fixtures plus a replay. Some copies carry other keys so that several partitions have work and the bitcoin CDN copy lands on a different partition than the original (dedup claims are global). It then runs two `Run` replicas in one consumer group until every partition's committed offset reaches its end offset (kadm `ListEndOffsets` / `FetchOffsets`). Finally it checks that the processed rows equal the unique keys and that a restart creates no new batch.

```go
package transformer

import (
	"context"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunEndToEnd(t *testing.T) {
	e := newEnv(t)
	brokers := testutil.StartRedpanda(t)
	ctx := context.Background()
	const topic = "coingecko.raw"

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(ctx, 6, 1, map[string]*string{"retention.ms": ptr("604800000")}, topic); err != nil {
		t.Fatal(err)
	}

	evs := fixtureEvents(t)
	var recs []*kgo.Record
	// Message keys pick partitions. Production keys are "coins_markets" and the
	// coin id; here some copies get other keys on purpose so that several
	// partitions (and so both replicas) have work, and the CDN copy of the
	// bitcoin response lands on another partition than the original: it must
	// still be dropped, because dedup claims are global.
	keys := []string{"coins_markets", "coins_markets", "bitcoin", "ethereum", "tether", "coins_markets"}
	for i, ev := range append(evs, evs[0]) { // the last one is a publisher replay of markets t0
		v, err := event.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		key := keys[i]
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: v, Headers: []kgo.RecordHeader{
			{Key: queue.HeaderEventID, Value: []byte(ev.EventID)},
			{Key: queue.HeaderRawObject, Value: []byte("source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl")},
		}})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: "00000000-0000-0000-0000-000000000001", TransformMaxRecords: 500}
	runFor := func(d time.Duration, done func() bool) {
		t.Helper()
		rctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := 0; i < 2; i++ { // two transformer replicas in one consumer group
			wg.Add(1)
			go func(i int) { defer wg.Done(); errs[i] = Run(rctx, cfg, e.deps) }(i)
		}
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) && !done() {
			time.Sleep(500 * time.Millisecond)
		}
		cancel()
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("Run %d: %v", i, err)
			}
		}
	}

	committedAll := func() bool {
		offs, err := adm.FetchOffsets(ctx, ConsumerGroup)
		if err != nil {
			return false
		}
		ok := true
		ends.Each(func(lo kadm.ListedOffset) {
			if lo.Offset == 0 {
				return
			}
			c, found := offs.Lookup(topic, lo.Partition)
			if !found || c.At != lo.Offset {
				ok = false
			}
		})
		return ok
	}
	runFor(60*time.Second, committedAll)
	if !committedAll() {
		t.Fatal("not every partition was committed to its end offset within 60s")
	}

	// Collect every object listed by written manifest rows.
	rows, err := e.deps.Pool.Query(ctx, `SELECT topic_id, kafka_partition, first_offset FROM pipeline.consumer_batches WHERE status = 'written'`)
	if err != nil {
		t.Fatal(err)
	}
	type bk struct {
		id    uuid.UUID
		p     int32
		first int64
	}
	var batches []bk
	for rows.Next() {
		var b bk
		if err := rows.Scan(&b.id, &b.p, &b.first); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	all := map[string][]byte{}
	for _, b := range batches {
		for k, v := range e.objects(t, b.id, b.p, b.first) {
			all[k] = v
		}
	}
	wantProcessed, wantRejected := expected(t, evs, e.deps.Rules)
	counts := e.countLines(t, all)
	t.Logf("batches=%d objects=%d lines=%v", len(batches), len(all), counts)
	if wantProcessed == 0 {
		t.Fatal("fixtures produced no processed rows")
	}
	if counts[s3.BucketProcessed] != wantProcessed || counts[s3.BucketRejected] != wantRejected {
		t.Fatalf("processed=%d rejected=%d, want %d and %d", counts[s3.BucketProcessed], counts[s3.BucketRejected], wantProcessed, wantRejected)
	}

	// A restart re-reads nothing: offsets are committed and no new batch appears.
	var before int
	if err := e.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	runFor(8*time.Second, func() bool { return false })
	var after int
	if err := e.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("restart created %d new batches", after-before)
	}
}

func ptr(s string) *string { return &s }
```

Run: `go test ./internal/transformer/ -run TestRunEndToEnd -v`
Expected: FAIL. The build fails with `undefined: Run` and `undefined: ConsumerGroup`.

- [ ] **Step 10: Implement the consumer loop in `internal/transformer/service.go`**

Notes on the code:
- The topic id is looked up with a short-lived plain client before the group client exists, because `AdjustFetchOffsetsFn` needs it as soon as the group is joined.
- `PollRecords(ctx, TransformMaxRecords)` returns records grouped by partition. Each partition is processed as one or more batches. `ProcessBatch` may return a `LastOffset` lower than the last record (the manifest fixed a shorter range), in which case the loop continues with the remaining records.
- `CommitOffsetsSync` with `kgo.EpochOffset{Epoch: leaderEpoch, Offset: last+1}`. Partition errors are read from the response with `kerr.ErrorForCode`. A rejected commit means the partition moved: stop working on it.
- Transient failures (Postgres or S3 down): seek the partition back to the batch start with `SetOffsets`, wait 2 s, and continue. No offset is committed. `ErrChecksumMismatch` and `ErrNonDeterministic` make `Run` return an error, so the process exits non-zero and an operator is alerted.
- The lag gauge is `HighWatermark - (last offset + 1)` from the fetch, so it costs no extra request.

```go
package transformer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ConsumerGroup is the Kafka consumer group of all transformer replicas.
const ConsumerGroup = "coingecko-transformer"

// retryBackoff is how long a partition waits after a transient failure
// (Postgres or S3 down) before its records are fetched again.
const retryBackoff = 2 * time.Second

// runner holds the per-process consumer state.
type runner struct {
	d       Deps
	topic   string
	topicID uuid.UUID
	max     int

	mu        sync.Mutex
	pending   map[int32][]Message // records held after IncompleteBatchError
	recovered map[int32]int64     // next offsets to commit after assignment recovery
}

// Run consumes the raw topic until ctx is cancelled (returns nil) or a fatal
// error occurs: an object exists with different bytes, or a batch is
// non-deterministic. Both need a human; the process exits non-zero.
func Run(ctx context.Context, cfg config.Config, d Deps) error {
	epoch, err := uuid.Parse(cfg.PipelineEpoch)
	if err != nil {
		return fmt.Errorf("ETL_PIPELINE_EPOCH: %w", err)
	}
	topicID, err := lookupTopicID(ctx, cfg, epoch)
	if err != nil {
		return err
	}
	max := cfg.TransformMaxRecords
	if max <= 0 {
		max = 500
	}
	r := &runner{d: d, topic: cfg.Topic, topicID: topicID, max: max,
		pending: map[int32][]Message{}, recovered: map[int32]int64{}}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("etl-transformer"),
		kgo.ConsumerGroup(ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // no committed offset: read everything
		kgo.DisableAutoCommit(),                           // we commit only after the objects are written
		kgo.BlockRebalanceOnPoll(),                        // no partition moves while a batch is in flight
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMinBytes(1<<20),
		kgo.FetchMaxWait(5*time.Second),
		kgo.AdjustFetchOffsetsFn(r.adjustOffsets),
		kgo.OnPartitionsRevoked(r.forget),
		kgo.OnPartitionsLost(r.forget),
	)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	d.Log.Info("transformer started", "stage", "transform", "topic", cfg.Topic, "topic_id", topicID.String(), "group", ConsumerGroup)

	for {
		fetches := cl.PollRecords(ctx, r.max)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				d.Log.Warn("fetch error", "stage", "transform", "topic", topic, "partition", p, "err", err)
			}
		})
		r.commitRecovered(ctx, cl)

		byPartition := map[int32][]*kgo.Record{}
		highWatermark := map[int32]int64{}
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			if fp.Topic != r.topic || len(fp.Records) == 0 {
				return
			}
			byPartition[fp.Partition] = append(byPartition[fp.Partition], fp.Records...)
			highWatermark[fp.Partition] = fp.HighWatermark
		})
		parts := make([]int32, 0, len(byPartition))
		for p := range byPartition {
			parts = append(parts, p)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

		for _, p := range parts {
			recs := byPartition[p]
			if err := r.processPartition(ctx, cl, p, recs); err != nil {
				cl.AllowRebalance()
				return err
			}
			lag := highWatermark[p] - (recs[len(recs)-1].Offset + 1)
			d.Metrics.ConsumerLag.WithLabelValues(strconv.Itoa(int(p))).Set(float64(lag))
		}
		cl.AllowRebalance()
	}
}

// lookupTopicID asks the broker for the topic id with a short-lived plain client.
func lookupTopicID(ctx context.Context, cfg config.Config, epoch uuid.UUID) (uuid.UUID, error) {
	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-transformer-admin"))
	if err != nil {
		return uuid.Nil, err
	}
	defer admin.Close()
	return queue.TopicID(ctx, admin, cfg.Topic, epoch)
}

// processPartition turns the polled records of one partition into one or more
// batches and commits after each written batch.
func (r *runner) processPartition(ctx context.Context, cl *kgo.Client, p int32, recs []*kgo.Record) error {
	epochs := map[int64]int32{}
	r.mu.Lock()
	msgs := r.pending[p]
	delete(r.pending, p)
	r.mu.Unlock()
	for _, rec := range recs {
		if n := len(msgs); n > 0 && rec.Offset <= msgs[n-1].Offset {
			continue // already held from an earlier poll
		}
		msgs = append(msgs, toMessage(rec))
		epochs[rec.Offset] = rec.LeaderEpoch
	}

	for len(msgs) > 0 {
		res, err := ProcessBatch(ctx, r.d, Batch{TopicID: r.topicID, Topic: r.topic, Partition: p, Messages: msgs})
		var incomplete *IncompleteBatchError
		var rewind *RewindError
		switch {
		case errors.As(err, &incomplete):
			// An earlier attempt planned a longer range: keep the records and
			// wait for the next poll to bring the rest.
			r.mu.Lock()
			r.pending[p] = msgs
			r.mu.Unlock()
			return nil
		case errors.As(err, &rewind):
			r.d.Log.Warn("rewinding to the start of a planned batch", "stage", "transform", "partition", p, "offset", rewind.Offset)
			r.seek(cl, p, rewind.Offset)
			return nil
		case errors.Is(err, s3.ErrChecksumMismatch), errors.Is(err, ErrNonDeterministic):
			return err
		case err != nil:
			if ctx.Err() != nil {
				return nil
			}
			r.d.Log.Error("batch failed, will retry", "stage", "transform", "partition", p,
				"batch", fmt.Sprintf("%d-%d", msgs[0].Offset, msgs[len(msgs)-1].Offset), "err", err)
			r.seek(cl, p, msgs[0].Offset) // fetch the same records again
			select {
			case <-ctx.Done():
			case <-time.After(retryBackoff):
			}
			return nil
		}

		epoch, ok := epochs[res.LastOffset]
		if !ok {
			epoch = -1
		}
		if !r.commit(ctx, cl, p, res.LastOffset+1, epoch) {
			return nil // the partition moved to another consumer; stop working on it
		}
		if res.LastOffset >= msgs[len(msgs)-1].Offset {
			if res.LastOffset > msgs[len(msgs)-1].Offset {
				r.seek(cl, p, res.LastOffset+1) // a written batch covered more than we polled
			}
			return nil
		}
		msgs = msgs[len(upTo(msgs, res.LastOffset)):]
	}
	return nil
}

func toMessage(rec *kgo.Record) Message {
	h := make(map[string]string, len(rec.Headers))
	for _, kv := range rec.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Message{Partition: rec.Partition, Offset: rec.Offset, Key: rec.Key, Value: rec.Value, Headers: h}
}

// seek moves the fetch position of one partition. Only called between
// PollRecords and AllowRebalance, so the partition cannot be revoked meanwhile.
func (r *runner) seek(cl *kgo.Client, p int32, offset int64) {
	r.mu.Lock()
	delete(r.pending, p)
	r.mu.Unlock()
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{r.topic: {p: {Epoch: -1, Offset: offset}}})
}

// commit synchronously commits next for partition p. A rejected commit means
// this member no longer owns the partition (spec §7.4 step 5).
func (r *runner) commit(ctx context.Context, cl *kgo.Client, p int32, next int64, epoch int32) bool {
	var commitErr error
	cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{r.topic: {p: {Epoch: epoch, Offset: next}}},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, tp := range t.Partitions {
					if err := kerr.ErrorForCode(tp.ErrorCode); err != nil {
						commitErr = err
					}
				}
			}
		})
	if commitErr != nil {
		r.d.Log.Warn("offset commit rejected", "stage", "transform", "partition", p, "offset", next, "err", commitErr)
		return false
	}
	return true
}

// adjustOffsets runs after the group fetched committed offsets for newly
// assigned partitions and before fetching starts (franz-go AdjustFetchOffsetsFn).
// It applies the assignment rules of spec §7.4: a written manifest batch that
// covers the committed offset is skipped (and committed later in the poll
// loop); a planned one is reprocessed from its exact start.
func (r *runner) adjustOffsets(ctx context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	for p, o := range offsets[r.topic] {
		committed := o.EpochOffset().Offset
		if committed < 0 {
			continue // nothing committed yet: ConsumeResetOffset applies
		}
		first, last, status, found, err := CoveringBatch(ctx, r.d.Pool, r.topicID, p, committed)
		if err != nil {
			// Not fatal: ProcessBatch applies the same rules from the manifest.
			r.d.Log.Warn("assignment recovery lookup failed", "stage", "transform", "partition", p, "err", err)
			continue
		}
		switch {
		case found && status == statusWritten:
			offsets[r.topic][p] = kgo.NewOffset().At(last + 1)
			r.mu.Lock()
			r.recovered[p] = last + 1
			r.mu.Unlock()
			r.d.Log.Info("skipping written batch on assignment", "stage", "transform", "partition", p, "batch", fmt.Sprintf("%d-%d", first, last))
		case found && first != committed:
			offsets[r.topic][p] = kgo.NewOffset().At(first)
			r.d.Log.Info("reprocessing planned batch on assignment", "stage", "transform", "partition", p, "batch", fmt.Sprintf("%d-%d", first, last))
		}
	}
	return offsets, nil
}

// commitRecovered commits the offsets moved past written batches on assignment.
func (r *runner) commitRecovered(ctx context.Context, cl *kgo.Client) {
	r.mu.Lock()
	rec := r.recovered
	r.recovered = map[int32]int64{}
	r.mu.Unlock()
	for p, next := range rec {
		r.commit(ctx, cl, p, next, -1)
	}
}

// forget drops local state of revoked or lost partitions. Offsets are not
// committed here: every written batch was committed right after it was written.
func (r *runner) forget(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range lost[r.topic] {
		delete(r.pending, p)
		delete(r.recovered, p)
	}
}
```

- [ ] **Step 11: Run the whole package**

Run: `go mod tidy && go vet ./... && go test ./internal/transformer/ -race -count=1 -v`
Expected: PASS for the unit tests, `TestProcessBatch`, and `TestRunEndToEnd`, which logs about `batches=3 objects=4` (the exact numbers depend on the transform task). The processed line count must equal the `expected` count, with no duplicate rows. The test takes about 20 s: fetches wait up to `FetchMaxWait` = 5 s, and a restart phase runs for 8 s.

- [ ] **Step 12: Commit**

```bash
git add internal/transformer/service.go internal/transformer/service_test.go go.mod go.sum
git commit -m "$(cat <<'MSG'
feat(transformer): franz-go consumer loop with manifest-based recovery

Consumer group coingecko-transformer, manual sync commits after each
written batch, BlockRebalanceOnPoll, assignment recovery through
AdjustFetchOffsetsFn, rewind/backoff on transient errors, lag gauge.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
MSG
)"
```

---

CONTRACT NOTES

- No contract signature is impossible. Notes for other tasks (verified in a full-stack compose run):
  - **Task 8 (`s3.EnsureBuckets`)**: versitygw v1.8.0 answers `500 InternalError` ("get bucket acl: no such key") to `CreateBucket` when the bucket directory already exists on disk but was not created through the gateway (for example `mkdir data/raw` before the first start, or `./.s3meta` deleted). `HeadBucket` on such a directory returns 200, and PUT/GET work. Task 8's `EnsureBuckets` calls `HeadBucket` first and skips `CreateBucket` when it succeeds (pinned by `TestEnsureBucketsAcceptsHostCreatedDirectory`). Task 17 also avoids the situation: `make up` creates only `./data`, never the bucket directories, and the Task 16 integration test removes the directories that `testutil.StartVersityGW` pre-creates before running init.
  - **Metric label values**: every stage uses `etl_schema_validations_total{result="pass"|"fail"}` and the schema file name without `.json` as the `schema` label (`raw_event.v1`, `coins_markets.v1`, `coin.v1`, `<dataset>.v1`); the reviewer aligned Tasks 7, 9, 11 and 15 to this. The alert rules and dashboard (Task 17) match `result="fail"` only. `etl_api_requests_total{status="skipped"}` counts ticks skipped by the limiter (Task 7) and is excluded from error ratios.
  - **`obs.Serve` shutdown**: `http.Server.Shutdown` waits up to 5 s for connections that never sent a request, then `Serve` returns an error. `cmd/etl` (Task 16) logs a shutdown error as a warning instead of exiting 1, and the Task 16 integration test uses a client without keep-alive. Not a contract change.
  - **Log files for replicas**: two transformer replicas share `./logs`, so with `ETL_LOG_PER_HOST=true` (set only on the transformer in compose) `cmd/etl` passes `<ETL_LOG_DIR>/<hostname>` to `obs.NewLogger`, which gives `logs/<container id>/transformer/etl.log`. Single-instance services keep `logs/<service>/etl.log`. No contract change.
  - **Redpanda**: `redpanda start --set redpanda.auto_create_topics_enabled=false` makes v26.2.4 exit with "unrecognised option '--set=…'". Auto-create is switched off with a bootstrap file mounted at `/etc/redpanda/.bootstrap.yaml` (`deploy/redpanda/bootstrap.yaml`); verified with `rpk cluster config get auto_create_topics_enabled` → `false`.

### Task 16: etl init + wiring

`etl init` prepares every dependency once (`internal/bootstrap`), and `cmd/etl/main.go` gets its final form: every subcommand loads its configuration, logger, metrics registry, health checks and HTTP server, and stops cleanly on SIGINT/SIGTERM.

**Files:**
- Create: `internal/bootstrap/bootstrap.go`
- Test: `internal/bootstrap/bootstrap_test.go`, `internal/bootstrap/bootstrap_integration_test.go`
- Modify: `cmd/etl/main.go` (full replacement of the Task 1 version)
- Test: `cmd/etl/main_test.go` (full replacement), `cmd/etl/main_integration_test.go` (new)

**Interfaces:**
- Consumes:
```go
func config.Load(service string) (config.Config, error)
func obs.NewLogger(service, logDir string) (*slog.Logger, io.Closer, error)
func obs.NewHealth() *obs.Health
func (h *obs.Health) AddReadiness(name string, c obs.Check)
func obs.NewMetrics(reg prometheus.Registerer) *obs.Metrics
func obs.Serve(ctx context.Context, addr string, h *obs.Health, g prometheus.Gatherer) error
func pg.Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error)
func pg.Migrate(ctx context.Context, pool *pgxpool.Pool) error
func pg.EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error
const (s3.BucketRaw = "raw"; s3.BucketProcessed = "processed"; s3.BucketRejected = "rejected"; s3.BucketDLQ = "dlq")
type s3.Config struct { Endpoint, AccessKey, SecretKey, Region string }
func s3.New(ctx context.Context, cfg s3.Config) (*s3.Client, error)
func (c *s3.Client) EnsureBuckets(ctx context.Context, buckets ...string) error
func (c *s3.Client) Ping(ctx context.Context) error
func queue.NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error)
func queue.EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error
func validation.Load(fsys fs.FS) (*validation.Registry, error)
var schemas.FS embed.FS
func coingecko.NewLimiter(minSpacing time.Duration) *coingecko.Limiter
func coingecko.NewClient(baseURL, apiKey string, hc *http.Client, l *coingecko.Limiter, now func() time.Time) *coingecko.Client
type transform.Rules struct { StaleAfter, TickerStaleAfter time.Duration; TickerAnomalyPct float64 }
type transformer.Deps struct { Pool *pgxpool.Pool; S3 *s3.Client; Reg *validation.Registry; Metrics *obs.Metrics; Log *slog.Logger; Rules transform.Rules }
func fetcher.Run(ctx context.Context, cfg config.Config, cg *coingecko.Client, lim *coingecko.Limiter, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) error
func exporter.Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
func publisher.Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error
func transformer.Run(ctx context.Context, cfg config.Config, d transformer.Deps) error
func testutil.StartPostgres(t *testing.T) (dsn string)
func testutil.StartRedpanda(t *testing.T) (brokers []string)
func testutil.StartVersityGW(t *testing.T) (endpoint, dataDir string)
func testutil.SkipIfNoDocker(t *testing.T)
```
- Produces:
```go
func bootstrap.Run(ctx context.Context, cfg config.Config, log *slog.Logger) error
// cmd/etl: `etl init|fetcher|exporter|publisher|transformer|healthcheck`
```

Background for this task:

- `kgo.NewClient` (franz-go) does not dial anything; the first request does. `(*kgo.Client).Ping(ctx)` sends a metadata request to any broker and is the cheapest "broker reachable" check. It is used both in `etl init` and as the readiness check of the publisher and transformer.
- `signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)` returns a context that is cancelled on the first signal. `docker compose stop` sends SIGTERM and waits 10 s before SIGKILL; every service loop returns `nil` when its context is cancelled.
- `context.WithCancelCause` lets the HTTP-server goroutine stop the service loop with a reason (for example "address already in use"); `context.Cause(ctx)` reads it back.
- `etl init` retries reaching each dependency for up to 60 s with exponential backoff (500 ms doubling to 5 s). Compose starts it only after the healthchecks pass, but "healthy" can come a few seconds before a dependency accepts every request. A step that keeps failing (for example a topic that exists with 3 partitions instead of 6) fails after the 60 s window, with every attempt logged.

- [ ] **Step 1: Write the failing unit tests for `internal/bootstrap`**

These tests need no Docker: they cover the retry helper and the "Postgres unreachable" path. The test shortens the package variable `dependencyTimeout` so it does not wait 60 s.

`internal/bootstrap/bootstrap_test.go`:
```go
package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/config"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	attempts, err := retry(context.Background(), time.Second, time.Millisecond, 4*time.Millisecond,
		func(ctx context.Context, attempt int) error {
			calls++
			if attempt != calls {
				t.Fatalf("attempt %d on call %d", attempt, calls)
			}
			if calls < 3 {
				return errors.New("not yet")
			}
			return nil
		})
	if err != nil || attempts != 3 {
		t.Fatalf("retry = (%d, %v), want (3, nil)", attempts, err)
	}
}

func TestRetryGivesUpAtTimeoutWithLastError(t *testing.T) {
	start := time.Now()
	_, err := retry(context.Background(), 50*time.Millisecond, time.Millisecond, 5*time.Millisecond,
		func(ctx context.Context, attempt int) error { return errors.New("connection refused") })
	if err == nil {
		t.Fatal("retry succeeded, want error")
	}
	if !strings.Contains(err.Error(), "connection refused") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want last error joined with DeadlineExceeded", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("retry took %v, want about 50ms", el)
	}
}

func TestRetryStopsOnParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts, err := retry(ctx, time.Minute, time.Second, time.Second,
		func(ctx context.Context, attempt int) error { return errors.New("down") })
	if attempts != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("retry = (%d, %v), want (1, Canceled)", attempts, err)
	}
}

// closedPort returns a loopback address on which nothing listens.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestRunFailsWhenPostgresUnreachable(t *testing.T) {
	old := dependencyTimeout
	dependencyTimeout = 300 * time.Millisecond
	t.Cleanup(func() { dependencyTimeout = old })

	cfg := config.Config{
		PostgresDSN: "postgres://etl:etl@" + closedPort(t) + "/etl?sslmode=disable&connect_timeout=1",
		Brokers:     []string{closedPort(t)},
		Topic:       "coingecko.raw", TopicPartitions: 6, TopicRetention: 168 * time.Hour,
		S3Endpoint: "http://" + closedPort(t), S3AccessKey: "etl", S3SecretKey: "etlsecret123", S3Region: "us-east-1",
	}
	err := Run(context.Background(), cfg, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "postgres_connect") {
		t.Fatalf("Run = %v, want postgres_connect error", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short -count=1 ./internal/bootstrap/`
Expected: FAIL. The build fails with `undefined: retry`, `undefined: dependencyTimeout` and `undefined: Run`.

- [ ] **Step 3: Implement `internal/bootstrap`**

Each dependency has a "reach" step (connect or ping) and "configure" steps (migrate, partitions, topic, buckets); every step goes through `step`, which retries and logs `init step done` with the attempt count and duration, or `init step gave up`. Partitions use `time.Now()`: `etl init` is not a transform, so the clock is allowed here.

`internal/bootstrap/bootstrap.go`:
```go
// Package bootstrap implements `etl init`: a one-shot, idempotent setup of
// every dependency the long-running services expect to exist.
//
//   - Postgres: goose migrations, then daily partitions [today-1, today+3].
//   - Broker: the raw topic with the configured partitions and retention
//     (an existing topic is verified, never altered).
//   - Object storage: the raw, processed, rejected and dlq buckets.
//
// Running it twice is safe: every step is a no-op when its target exists.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
)

// partitionDaysAhead matches the fetcher's maintenance loop: today … today+3.
const partitionDaysAhead = 3

// Retry policy for reaching a dependency. Compose starts init only after the
// healthchecks pass, but "healthy" can precede "accepts our requests" by a
// few seconds (e.g. Redpanda's controller election), so every step retries.
var (
	dependencyTimeout = 60 * time.Second
	initialBackoff    = 500 * time.Millisecond
	maxBackoff        = 5 * time.Second
)

// Run executes every init step in order and returns the first error.
// It logs one line per completed step with its duration.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	log = log.With("stage", "init")
	started := time.Now()
	log.Info("init started",
		"brokers", cfg.Brokers, "topic", cfg.Topic, "s3_endpoint", cfg.S3Endpoint)

	if err := initPostgres(ctx, cfg, log); err != nil {
		return err
	}
	if err := initBroker(ctx, cfg, log); err != nil {
		return err
	}
	if err := initBuckets(ctx, cfg, log); err != nil {
		return err
	}
	log.Info("init finished", "duration_ms", time.Since(started).Milliseconds())
	return nil
}

func initPostgres(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	var pool *pgxpool.Pool
	err := step(ctx, log, "postgres_connect", func(ctx context.Context) error {
		p, err := pg.Connect(ctx, cfg.PostgresDSN)
		if err != nil {
			return err
		}
		pool = p
		return nil
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := step(ctx, log, "postgres_migrate", func(ctx context.Context) error {
		return pg.Migrate(ctx, pool)
	}); err != nil {
		return err
	}
	return step(ctx, log, "postgres_partitions", func(ctx context.Context) error {
		return pg.EnsurePartitions(ctx, pool, time.Now(), partitionDaysAhead)
	})
}

func initBroker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// A plain client is enough for admin requests; kgo.NewClient does not dial
	// until the first request, so it cannot fail on an unreachable broker.
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-init"))
	if err != nil {
		return fmt.Errorf("init: broker client: %w", err)
	}
	defer cl.Close()

	if err := step(ctx, log, "broker_ping", cl.Ping); err != nil {
		return err
	}
	return step(ctx, log, "broker_topic", func(ctx context.Context) error {
		return queue.EnsureTopic(ctx, cl, cfg.Topic, cfg.TopicPartitions, cfg.TopicRetention)
	})
}

func initBuckets(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	s3c, err := s3.New(ctx, s3.Config{
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		Region:    cfg.S3Region,
	})
	if err != nil {
		return fmt.Errorf("init: s3 client: %w", err)
	}
	if err := step(ctx, log, "s3_ping", s3c.Ping); err != nil {
		return err
	}
	return step(ctx, log, "s3_buckets", func(ctx context.Context) error {
		return s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ)
	})
}

// step runs fn with retry and logs the outcome under the given step name.
func step(ctx context.Context, log *slog.Logger, name string, fn func(context.Context) error) error {
	start := time.Now()
	attempts, err := retry(ctx, dependencyTimeout, initialBackoff, maxBackoff, func(ctx context.Context, attempt int) error {
		err := fn(ctx)
		if err != nil {
			log.Warn("init step failed, retrying", "step", name, "attempt", attempt, "error", err)
		}
		return err
	})
	if err != nil {
		log.Error("init step gave up", "step", name, "attempts", attempts, "error", err)
		return fmt.Errorf("init: %s: %w", name, err)
	}
	log.Info("init step done", "step", name, "attempts", attempts,
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// retry calls fn until it succeeds, ctx is done, or timeout elapses. The
// wait between attempts doubles from initial up to maxWait. It returns the
// number of attempts and, on failure, the last error of fn joined with the
// reason retrying stopped.
func retry(ctx context.Context, timeout, initial, maxWait time.Duration, fn func(ctx context.Context, attempt int) error) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wait := initial
	for attempt := 1; ; attempt++ {
		err := fn(ctx, attempt)
		if err == nil {
			return attempt, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempt, errors.Join(err, fmt.Errorf("gave up after %d attempts: %w", attempt, ctx.Err()))
		case <-timer.C:
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `go test -short -count=1 ./internal/bootstrap/`
Expected: PASS (`ok coingecko-etl/internal/bootstrap`, about 1 s).

- [ ] **Step 5: Write the integration test (Postgres + Redpanda + versitygw)**

It runs `bootstrap.Run` twice against real containers and checks the result in each system. `testutil.StartVersityGW` pre-creates the four bucket directories; the test deletes them first, so it proves that init creates the buckets through the S3 API. A third run with 3 partitions must fail, because `queue.EnsureTopic` verifies an existing topic instead of accepting it.

`internal/bootstrap/bootstrap_integration_test.go`:
```go
package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"
)

func TestRunTwiceAgainstRealDependencies(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	dsn := testutil.StartPostgres(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, dataDir := testutil.StartVersityGW(t)

	// StartVersityGW pre-creates the bucket directories; remove them so the
	// test proves that init creates the buckets itself.
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if err := os.RemoveAll(filepath.Join(dataDir, b)); err != nil {
			t.Fatal(err)
		}
	}

	cfg := config.Config{
		Service:         "init",
		PostgresDSN:     dsn,
		Brokers:         brokers,
		Topic:           "coingecko.raw",
		TopicPartitions: 6,
		TopicRetention:  168 * time.Hour,
		S3Endpoint:      endpoint,
		S3AccessKey:     testutil.VersityGWAccessKey,
		S3SecretKey:     testutil.VersityGWSecretKey,
		S3Region:        "us-east-1",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	for i := 1; i <= 2; i++ {
		if err := Run(ctx, cfg, discardLogger()); err != nil {
			t.Fatalf("run #%d: %v", i, err)
		}
	}

	// Postgres: tables exist and today's partitions were created.
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	today := time.Now().UTC().Format("20060102")
	for _, tbl := range []string{"raw.api_calls", "pipeline.export_batches", "pipeline.consumer_batches",
		"raw.api_calls_p" + today, "pipeline.dedup_claims_p" + today} {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&ok); err != nil || !ok {
			t.Errorf("relation %s missing (err=%v)", tbl, err)
		}
	}

	// Broker: the topic has 6 partitions.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(ctx, cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if td, ok := topics[cfg.Topic]; !ok || td.Err != nil || len(td.Partitions) != 6 {
		t.Fatalf("topic %s: %+v", cfg.Topic, topics[cfg.Topic])
	}

	// Object storage: every bucket is a directory under the gateway root.
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if fi, err := os.Stat(filepath.Join(dataDir, b)); err != nil || !fi.IsDir() {
			t.Errorf("bucket dir %s: %v", b, err)
		}
	}

	// A third run with a different partition count must fail: init verifies
	// an existing topic instead of silently accepting it.
	bad := cfg
	bad.TopicPartitions = 3
	old := dependencyTimeout
	dependencyTimeout = 2 * time.Second
	defer func() { dependencyTimeout = old }()
	if err := Run(ctx, bad, discardLogger()); err == nil {
		t.Fatal("run with 3 partitions succeeded, want topic mismatch error")
	}
}
```

- [ ] **Step 6: Run the integration test**

Run: `go test -count=1 -run TestRunTwiceAgainstRealDependencies -v ./internal/bootstrap/`
Expected: PASS (`--- PASS: TestRunTwiceAgainstRealDependencies`, 10–20 s with images cached). Without Docker, or with `-short`, it is reported as SKIP.

- [ ] **Step 7: Write the failing tests for `cmd/etl`**

Replace `cmd/etl/main_test.go` completely. The Task 1 dispatch cases that expected "not implemented" are gone; the new tests check that every service rejects bad configuration, that a service exits 1 (and logs `service failed`) when Postgres is unreachable, and the `ETL_LOG_PER_HOST` log directory. The healthcheck tests from Task 1 are kept unchanged.

`cmd/etl/main_test.go`:
```go
package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunDispatch(t *testing.T) {
	cases := []struct {
		args       []string
		wantCode   int
		wantStderr string
	}{
		{nil, 2, "usage: etl"},
		{[]string{"bogus"}, 2, `unknown command "bogus"`},
		{[]string{"help"}, 0, ""},
	}
	for _, c := range cases {
		t.Run(strings.Join(c.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(c.args, &stdout, &stderr); code != c.wantCode {
				t.Fatalf("exit code %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

func TestServiceRejectsBadConfig(t *testing.T) {
	t.Setenv("ETL_LOG_DIR", t.TempDir())
	t.Setenv("ETL_TOPIC_PARTITIONS", "six")
	for _, svc := range []string{"init", "fetcher", "exporter", "publisher", "transformer"} {
		var stderr bytes.Buffer
		if code := run([]string{svc}, &bytes.Buffer{}, &stderr); code != 1 {
			t.Fatalf("%s: exit %d, want 1", svc, code)
		}
		if !strings.Contains(stderr.String(), "ETL_TOPIC_PARTITIONS") {
			t.Fatalf("%s: stderr %q does not name the bad variable", svc, stderr.String())
		}
	}
}

// closedAddr returns a loopback address on which nothing listens.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestServiceExitsWhenPostgresUnreachable(t *testing.T) {
	logDir := t.TempDir()
	t.Setenv("ETL_LOG_DIR", logDir)
	t.Setenv("ETL_PG_DSN", "postgres://etl:etl@"+closedAddr(t)+"/etl?sslmode=disable&connect_timeout=1")
	t.Setenv("ETL_HTTP_ADDR", closedAddr(t))
	for _, svc := range []string{"fetcher", "exporter", "publisher", "transformer"} {
		if code := run([]string{svc}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
			t.Fatalf("%s: exit %d, want 1", svc, code)
		}
		logged, err := os.ReadFile(filepath.Join(logDir, svc, "etl.log"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(logged), `"msg":"service failed"`) {
			t.Fatalf("%s: log does not record the failure:\n%s", svc, logged)
		}
	}
}

func TestReadyzURL(t *testing.T) {
	cases := map[string]string{
		"":               "http://127.0.0.1:8080/readyz",
		":8080":          "http://127.0.0.1:8080/readyz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/readyz",
		"[::]:8080":      "http://127.0.0.1:8080/readyz",
		"10.0.0.5:8080":  "http://10.0.0.5:8080/readyz",
		"localhost:8081": "http://localhost:8081/readyz",
	}
	for in, want := range cases {
		got, err := readyzURL(in)
		if err != nil || got != want {
			t.Errorf("readyzURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := readyzURL("8080"); err == nil {
		t.Error(`readyzURL("8080") succeeded, want error`)
	}
}

func TestHealthcheckCommand(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	t.Setenv("ETL_HTTP_ADDR", srv.Listener.Addr().String())

	var stderr bytes.Buffer
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 0 {
		t.Fatalf("ready server: exit %d (%s), want 0", code, stderr.String())
	}
	status = http.StatusServiceUnavailable
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &stderr); code != 1 {
		t.Fatalf("not-ready server: exit %d, want 1", code)
	}
}

func TestHealthcheckUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more
	t.Setenv("ETL_HTTP_ADDR", addr)
	if code := run([]string{"healthcheck"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
}

func TestLogDirPerHost(t *testing.T) {
	t.Setenv("ETL_LOG_PER_HOST", "")
	if got := logDir("logs"); got != "logs" {
		t.Fatalf("logDir without ETL_LOG_PER_HOST = %q, want logs", got)
	}
	t.Setenv("ETL_LOG_PER_HOST", "true")
	host, err := os.Hostname()
	if err != nil {
		t.Skip("no hostname")
	}
	if got, want := logDir("logs"), filepath.Join("logs", host); got != want {
		t.Fatalf("logDir = %q, want %q", got, want)
	}
}
```

- [ ] **Step 8: Run the tests to verify they fail**

Run: `go test -short -count=1 ./cmd/etl/`
Expected: FAIL. The build fails with `undefined: logDir`.

- [ ] **Step 9: Write the final `cmd/etl/main.go`**

Replace the file completely. What each service gets:

| Service | Dependencies built here | Readiness checks (`/readyz`) | Loop |
|---|---|---|---|
| init | none (bootstrap builds its own) | no HTTP server (one-shot) | `bootstrap.Run`, then exit 0/1 |
| fetcher | pgx pool, `coingecko.Limiter` (`MinRequestSpacing`), `coingecko.Client` with a 20 s HTTP timeout | postgres | `fetcher.Run` |
| exporter | pool, S3 client, schema registry | postgres, s3 | `exporter.Run` |
| publisher | pool, S3, registry, idempotent producer (`queue.NewProducer`, kprom metrics on the same registry) | postgres, s3, broker (`prod.Ping`) | `publisher.Run` |
| transformer | pool, S3, registry, `transformer.Deps` with `transform.Rules` from config, a small kgo client used only for the broker ping | postgres, s3, broker | `transformer.Run` (builds its own consumer) |

Every long-running service registers the Go runtime and process collectors plus `obs.NewMetrics` on one fresh `prometheus.Registry`, serves `/healthz /readyz /metrics` on `cfg.HTTPAddr` in a goroutine, and stops both the loop and the server on SIGINT/SIGTERM. A failure to connect within 30 s exits 1; compose restarts the container (`restart: unless-stopped`).

`cmd/etl/main.go`:
```go
// Command etl is the single binary of the pipeline. The first argument selects
// the subcommand; see usage below.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/bootstrap"
	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/exporter"
	"coingecko-etl/internal/fetcher"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/publisher"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/transformer"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

const usageText = `usage: etl <command>

commands:
  init         run migrations, create partitions, topic and buckets (one-shot)
  fetcher      poll CoinGecko into raw.api_calls
  exporter     export raw.api_calls to s3://raw as JSONL
  publisher    publish exported batches to the broker
  transformer  consume, clean, dedup and write s3://processed
  healthcheck  exit 0 if the local /readyz returns 200 (for container healthchecks)
`

// startupTimeout bounds connecting to dependencies when a service starts.
// init has already waited for them, so a failure here is real; the process
// exits 1 and the compose restart policy tries again.
const startupTimeout = 30 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns the process exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "healthcheck":
		return healthcheck(os.Getenv("ETL_HTTP_ADDR"), stderr)
	case "init", "fetcher", "exporter", "publisher", "transformer":
		return runService(args[0], stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return 0
	default:
		fmt.Fprintf(stderr, "etl: unknown command %q\n%s", args[0], usageText)
		return 2
	}
}

// runService loads the configuration and logger, installs the SIGINT/SIGTERM
// handler and runs one subcommand until it returns or a signal arrives.
func runService(name string, stderr io.Writer) int {
	cfg, err := config.Load(name)
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", name, err)
		return 1
	}
	log, closer, err := obs.NewLogger(name, logDir(cfg.LogDir))
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", name, err)
		return 1
	}
	defer closer.Close()

	// SIGTERM is what `docker compose stop` sends; SIGINT is Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if name == "init" {
		if err := bootstrap.Run(ctx, cfg, log); err != nil {
			log.Error("init failed", "error", err)
			return 1
		}
		return 0
	}
	if err := serveAndRun(ctx, cfg, log); err != nil {
		log.Error("service failed", "error", err)
		return 1
	}
	log.Info("service stopped")
	return 0
}

// logDir returns the directory handed to obs.NewLogger. Replicas of one
// service (the transformer runs with deploy.replicas: 2) share the ./logs
// bind mount, and two processes must never append to one file, so with
// ETL_LOG_PER_HOST=true each replica writes to <dir>/<hostname>/<service>/etl.log.
// In compose the hostname is the container id, as shown by `docker ps`.
func logDir(dir string) string {
	if os.Getenv("ETL_LOG_PER_HOST") != "true" {
		return dir
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		return dir
	}
	return filepath.Join(dir, host)
}

// serveAndRun builds the dependencies of one long-running service, serves
// /healthz /readyz /metrics on cfg.HTTPAddr and runs the service loop. When
// either the loop or the HTTP server stops, the other one is stopped too.
func serveAndRun(parent context.Context, cfg config.Config, log *slog.Logger) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := obs.NewMetrics(reg)
	health := obs.NewHealth()

	startCtx, cancelStart := context.WithTimeout(parent, startupTimeout)
	svc, cleanup, err := buildService(startCtx, cfg, reg, m, health, log)
	cancelStart()
	defer cleanup()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	srvDone := make(chan error, 1)
	go func() {
		err := obs.Serve(ctx, cfg.HTTPAddr, health, reg)
		if err != nil {
			cancel(fmt.Errorf("http server: %w", err)) // stop the service loop too
		}
		srvDone <- err
	}()
	log.Info("service started", "http_addr", cfg.HTTPAddr)

	runErr := svc(ctx)
	if runErr == nil {
		// The loop returns nil when ctx is cancelled. If the cancel came from
		// the HTTP server rather than a signal, report that cause.
		if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			runErr = cause
		}
	}
	cancel(nil) // stop the HTTP server
	if srvErr := <-srvDone; srvErr != nil && runErr == nil {
		// The loop already stopped cleanly; a slow HTTP shutdown (e.g. a
		// scrape still in flight after 5 s) is not worth a failing exit code.
		log.Warn("http server shutdown", "error", srvErr)
	}
	return runErr
}

// serviceFunc is one service loop; it returns nil after ctx is cancelled.
type serviceFunc func(ctx context.Context) error

// buildService connects the dependencies of cfg.Service, registers its
// readiness checks and returns its loop. cleanup is always non-nil and
// releases whatever was opened, even when err != nil.
func buildService(ctx context.Context, cfg config.Config, reg *prometheus.Registry, m *obs.Metrics,
	health *obs.Health, log *slog.Logger) (svc serviceFunc, cleanup func(), err error) {
	var closers []func()
	cleanup = func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}

	pool, err := pg.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, cleanup, err
	}
	closers = append(closers, pool.Close)
	health.AddReadiness("postgres", pool.Ping)

	if cfg.Service == "fetcher" {
		lim := coingecko.NewLimiter(cfg.MinRequestSpacing)
		hc := &http.Client{Timeout: 20 * time.Second}
		cg := coingecko.NewClient(cfg.CoinGeckoBaseURL, cfg.CoinGeckoAPIKey, hc, lim, time.Now)
		return func(ctx context.Context) error {
			return fetcher.Run(ctx, cfg, cg, lim, pool, m, log)
		}, cleanup, nil
	}

	// exporter, publisher and transformer need object storage and schemas.
	s3c, err := s3.New(ctx, s3.Config{
		Endpoint: cfg.S3Endpoint, AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey, Region: cfg.S3Region,
	})
	if err != nil {
		return nil, cleanup, err
	}
	health.AddReadiness("s3", s3c.Ping)
	schemaReg, err := validation.Load(schemas.FS)
	if err != nil {
		return nil, cleanup, fmt.Errorf("load schemas: %w", err)
	}

	switch cfg.Service {
	case "exporter":
		return func(ctx context.Context) error {
			return exporter.Run(ctx, cfg, pool, s3c, schemaReg, m, log)
		}, cleanup, nil

	case "publisher":
		prod, err := queue.NewProducer(cfg.Brokers, "etl-publisher", reg)
		if err != nil {
			return nil, cleanup, fmt.Errorf("producer: %w", err)
		}
		closers = append(closers, prod.Close)
		health.AddReadiness("broker", prod.Ping)
		return func(ctx context.Context) error {
			return publisher.Run(ctx, cfg, pool, s3c, prod, schemaReg, m, log)
		}, cleanup, nil

	case "transformer":
		// transformer.Run builds its own consumer-group client; this small
		// client only answers readiness probes.
		pinger, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-transformer-health"))
		if err != nil {
			return nil, cleanup, fmt.Errorf("broker client: %w", err)
		}
		closers = append(closers, pinger.Close)
		health.AddReadiness("broker", pinger.Ping)
		deps := transformer.Deps{
			Pool: pool, S3: s3c, Reg: schemaReg, Metrics: m, Log: log,
			Rules: transform.Rules{
				StaleAfter:       cfg.StaleAfter,
				TickerStaleAfter: cfg.TickerStaleAfter,
				TickerAnomalyPct: cfg.TickerAnomalyPct,
			},
		}
		return func(ctx context.Context) error {
			return transformer.Run(ctx, cfg, deps)
		}, cleanup, nil
	}
	return nil, cleanup, fmt.Errorf("unknown service %q", cfg.Service)
}

// readyzURL turns a listen address such as ":8080" or "0.0.0.0:8080" into the
// loopback URL of /readyz. An empty addr means the default ":8080".
func readyzURL(addr string) (string, error) {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("bad ETL_HTTP_ADDR %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

// healthcheck is used by Docker healthchecks: the distroless image has no curl.
// The 3 s client timeout exceeds the 2 s readiness timeout of /readyz.
func healthcheck(addr string, stderr io.Writer) int {
	url, err := readyzURL(addr)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: %s returned %d: %s\n", url, resp.StatusCode, body)
		return 1
	}
	return 0
}
```

- [ ] **Step 10: Run the unit tests to verify they pass**

Run: `go test -short -count=1 ./cmd/etl/`
Expected: PASS.

- [ ] **Step 11: Write the end-to-end integration test for a service**

It runs `etl fetcher` in-process against a real Postgres and a fake CoinGecko (`httptest.Server` serving the recorded `schemas/testdata/coingecko/markets_top10_t0.json` from Task 4), waits for `/readyz`, checks `/healthz` and `/metrics`, then sends SIGTERM to the test process. Because `signal.NotifyContext` is installed before the HTTP server starts, a ready server proves that the signal will be caught rather than kill the test binary. The client disables keep-alive: an idle pooled connection would make `http.Server.Shutdown` wait 5 s.

`cmd/etl/main_integration_test.go`:
```go
package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"
)

// TestFetcherServesAndStopsOnSIGTERM runs `etl fetcher` in-process against a
// real Postgres and a fake CoinGecko, checks the HTTP endpoints, then sends
// SIGTERM to the test process and expects a clean exit 0.
func TestFetcherServesAndStopsOnSIGTERM(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	markets, err := os.ReadFile("../../schemas/testdata/coingecko/markets_top10_t0.json")
	if err != nil {
		t.Fatal(err)
	}
	fakeCG := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(markets)
	}))
	defer fakeCG.Close()

	addr := closedAddr(t) // a free port for the service's HTTP server
	t.Setenv("ETL_LOG_DIR", t.TempDir())
	t.Setenv("ETL_PG_DSN", dsn)
	t.Setenv("ETL_HTTP_ADDR", addr)
	t.Setenv("ETL_COINGECKO_BASE_URL", fakeCG.URL)
	t.Setenv("ETL_MIN_REQUEST_SPACING", "0s")
	t.Setenv("ETL_WATCHLIST", "bitcoin")

	exit := make(chan int, 1)
	go func() { exit <- run([]string{"fetcher"}, &bytes.Buffer{}, &bytes.Buffer{}) }()

	// /readyz turns 200 once the server is up and Postgres answers. The signal
	// handler is installed before the server starts, so after this point a
	// SIGTERM is caught by signal.NotifyContext instead of killing the test.
	waitFor(t, 30*time.Second, func() bool { return get(t, "http://"+addr+"/readyz") == http.StatusOK })
	if code := get(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d", code)
	}

	// The first markets poll happens immediately; wait until it is stored and counted.
	waitFor(t, 30*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM raw.api_calls WHERE endpoint = 'coins_markets'`).Scan(&n)
		return n > 0
	})
	resp, err := noKeepAlive.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`etl_api_requests_total{endpoint="coins_markets",status="200"}`, "go_goroutines"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("fetcher exit code %d after SIGTERM, want 0", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("fetcher did not stop within 15s of SIGTERM")
	}
}

// noKeepAlive closes every connection after its response. Pooled idle
// connections would otherwise make http.Server.Shutdown wait up to 5 s.
var noKeepAlive = &http.Client{
	Timeout:   2 * time.Second,
	Transport: &http.Transport{DisableKeepAlives: true},
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := noKeepAlive.Get(url)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitFor(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", d)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
```

- [ ] **Step 12: Run the whole package with the race detector**

Run: `go test -race -count=1 ./cmd/etl/ ./internal/bootstrap/`
Expected: PASS for both packages (`TestFetcherServesAndStopsOnSIGTERM` takes 2–5 s once the Postgres image is cached).

- [ ] **Step 13: Vet and build**

Run: `gofmt -l . && go vet ./... && CGO_ENABLED=0 go build -trimpath -o bin/etl ./cmd/etl && ./bin/etl help`
Expected: no gofmt output, no vet findings, and the usage text listing `init fetcher exporter publisher transformer healthcheck`.

- [ ] **Step 14: Commit**

```bash
git add internal/bootstrap cmd/etl
git commit -m "$(cat <<'EOF'
feat: etl init bootstrap and final service wiring

bootstrap.Run migrates Postgres, creates partitions, the topic and the
buckets, retrying each dependency for up to 60 s. cmd/etl wires config,
logging, metrics, readiness checks and graceful shutdown for every
subcommand.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 17: Container + compose + monitoring

One image for all subcommands, the full docker compose stack, Prometheus scrape config and alert rules with unit tests, a provisioned Grafana dashboard, and the Makefile targets `up`, `down`, `demo`.

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `.env.example`
- Create: `deploy/redpanda/bootstrap.yaml`
- Create: `deploy/prometheus/prometheus.yml`, `deploy/prometheus/alerts.yml`, Test: `deploy/prometheus/alerts_test.yml`
- Create: `deploy/grafana/provisioning/datasources/prometheus.yml`, `deploy/grafana/provisioning/dashboards/etl.yml`, `deploy/grafana/dashboards/etl.json`
- Modify: `Makefile` (full replacement of the Task 1 version)

**Interfaces:**
- Consumes: the `etl` binary and its subcommands (Task 16): `etl init`, `etl fetcher`, `etl exporter`, `etl publisher`, `etl transformer`, `etl healthcheck`; environment variables of `config.Load` (Task 1); `ETL_LOG_PER_HOST` (Task 16); the metric names of `obs.Metrics`:
```go
etl_api_requests_total{endpoint,status}            etl_api_request_duration_seconds{endpoint}
etl_api_rate_limited_total                         etl_last_success_timestamp_seconds{endpoint}
etl_source_data_age_seconds{endpoint}              etl_schema_validations_total{stage,schema,result}
etl_schema_violations_total{stage,schema,keyword}  etl_export_lag_seconds
etl_export_batches_total{result}                   etl_s3_put_total{result}
etl_published_records_total                        etl_records_total{dataset,outcome}
etl_dedup_dropped_total{reason}                    etl_rejected_total{reason}
etl_dlq_total{stage,error_class}                   etl_consumer_lag_records{partition}
etl_batch_duration_seconds                         etl_e2e_latency_seconds
```
- Produces: image `coingecko-etl:local`; compose services `postgres`, `redpanda`, `versitygw`, `redpanda-console`, `prometheus`, `grafana`, `init`, `fetcher`, `exporter`, `publisher`, `transformer` (2 replicas); host ports (127.0.0.1 only) Postgres 5432, Kafka 19092, S3 7070, Console 8081, Prometheus 9090, Grafana 3000; `make up | down | demo | demo-report | alerts-test`.

Background for this task:

- **distroless/static:nonroot** has no shell, no curl and runs as uid 65532. Container healthchecks therefore call `/etl healthcheck` (Task 1), which GETs the local `/readyz`.
- **Bind-mount ownership.** `./data`, `./logs` and `./.s3meta` are bind mounts. On Linux a container running as 65532 cannot write into a directory owned by the host user, so the etl services and versitygw run as `${ETL_UID}:${ETL_GID}`; the Makefile exports the invoking user's ids. Docker Desktop on macOS maps any uid to the host user.
- **Postgres 18** stores data under `/var/lib/postgresql/18/docker`; the volume must be mounted at `/var/lib/postgresql`. Mounting `/var/lib/postgresql/data` makes the container refuse to start. `pg_isready -h 127.0.0.1` checks over TCP: during initdb the server listens only on the Unix socket, so a socket check reports healthy too early.
- **Redpanda dev-container mode** (`--mode=dev-container --smp=1 --memory=1G`) is the documented single-node laptop setup. Two listeners: `internal://redpanda:9092` for containers, `external://localhost:19092` for tools on the host. v26.2.4 rejects `--set redpanda.auto_create_topics_enabled=false` ("unrecognised option"); cluster properties for the first start go into `/etc/redpanda/.bootstrap.yaml`, which Redpanda reads once when the data volume is empty.
- **versitygw** `--sidecar /meta` stores object metadata in a separate directory because Docker Desktop bind mounts have no xattrs. Do not pre-create `data/raw` etc.: versitygw v1.8.0 returns 500 to `CreateBucket` for a directory it did not create (see CONTRACT NOTES); `etl init` creates the buckets.
- **Prometheus `dns_sd_configs`** with `type: A` resolves a compose service name through Docker's DNS to one address per container, so both transformer replicas are scraped without listing them.
- **`promtool test rules`** evaluates alert rules against synthetic series (`values: '0+1x20'` means start at 0, add 1 per interval, 20 times; `_` is a missing sample). It is the unit test for `alerts.yml`.
- **Grafana provisioning** loads datasources and dashboards from files at startup. The datasource gets the fixed uid `prometheus`, which every panel references.

- [ ] **Step 1: Write the Dockerfile and .dockerignore**

`Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1

# ---- build: static Go binary ----
FROM golang:1.27 AS build
WORKDIR /src
# Download modules in their own layer so code changes do not refetch them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/etl ./cmd/etl

# ---- runtime: no shell, no package manager, runs as uid 65532 ----
FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/etl /etl
ENV ETL_LOG_DIR=/app/logs \
    ETL_HTTP_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/etl"]
```

`.dockerignore` (keeps `data/`, `logs/`, `.env` and docs out of the build context):
```
# Keep the build context small and free of runtime data and secrets.
.git
.github
.env
bin/
data/
logs/
.s3meta/
docs/
deploy/
*.md
*.test
*.out
Dockerfile
docker-compose.yml
```

- [ ] **Step 2: Build the image**

Run: `docker build -t coingecko-etl:local . && docker run --rm coingecko-etl:local help && docker image ls coingecko-etl:local --format '{{.Size}}'`
Expected: the build succeeds, `help` prints the usage text, and the image is about 35 MB.

- [ ] **Step 3: Write the environment template and the Redpanda bootstrap file**

`.env.example`:
```bash
# Copy to .env (`make up` does it for you) and adjust as needed.

# Optional CoinGecko Demo API key (sent as x-cg-demo-api-key). Keyless works
# at the default request budget (~3 calls/min).
COINGECKO_API_KEY=

# Host user that owns ./data, ./logs and ./.s3meta. `make up` exports the
# current user's ids; set them here when running docker compose directly on Linux.
ETL_UID=65532
ETL_GID=65532

# Host ports (bound to 127.0.0.1).
POSTGRES_PORT=5432
REDPANDA_EXTERNAL_PORT=19092
S3_PORT=7070
CONSOLE_PORT=8081
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000

# Object storage credentials (versitygw root account).
ETL_S3_ACCESS_KEY=etl
ETL_S3_SECRET_KEY=etlsecret123

# Polling and pipeline tuning (Go durations).
ETL_MARKETS_INTERVAL=30s
ETL_DETAIL_INTERVAL=60s
ETL_DETAIL_OFFSET=15s
ETL_WATCHLIST=bitcoin,ethereum,tether,usd-coin
ETL_MIN_REQUEST_SPACING=15s
ETL_EXPORT_INTERVAL=15s

# Cleaning thresholds (spec §6.4).
ETL_STALE_AFTER=10m
ETL_TICKER_STALE_AFTER=1h
ETL_TICKER_ANOMALY_PCT=2.0

GRAFANA_ADMIN_PASSWORD=admin
```

`deploy/redpanda/bootstrap.yaml`:
```yaml
# Cluster properties applied on the first start of an empty Redpanda data
# volume (Redpanda reads /etc/redpanda/.bootstrap.yaml once). Topics are
# created only by `etl init`, with explicit partitions and retention.
auto_create_topics_enabled: false
```

- [ ] **Step 4: Write docker-compose.yml**

The `x-etl` anchor holds everything the six `etl` containers share: image and build, user, `.env` (optional), internal addresses, the `./logs` mount, `restart: unless-stopped`, `depends_on: init: service_completed_successfully` and the `/etl healthcheck` healthcheck. `init` overrides `depends_on` with the three `service_healthy` conditions, never restarts and has no healthcheck. The transformer adds `ETL_LOG_PER_HOST=true` (its two replicas share `./logs`); because a service-level `environment:` replaces the anchor's map, it merges `*etl-env` explicitly.

`docker-compose.yml`:
```yaml
name: coingecko-etl

# Shared settings of every `etl` container (one image, one subcommand each).
x-etl: &etl
  image: coingecko-etl:local
  build: .
  user: "${ETL_UID:-65532}:${ETL_GID:-65532}"
  env_file:
    - path: .env
      required: false
  environment: &etl-env
    ETL_PG_DSN: postgres://etl:etl@postgres:5432/etl?sslmode=disable
    ETL_BROKERS: redpanda:9092
    ETL_S3_ENDPOINT: http://versitygw:7070
    ETL_HTTP_ADDR: ":8080"
    ETL_LOG_DIR: /app/logs
  volumes:
    - ./logs:/app/logs
  restart: unless-stopped
  depends_on:
    init:
      condition: service_completed_successfully
  healthcheck:
    test: ["CMD", "/etl", "healthcheck"]
    interval: 10s
    timeout: 5s
    retries: 3
    start_period: 20s

services:
  # ---------------- infrastructure ----------------
  postgres:
    image: postgres:18.6
    environment:
      POSTGRES_USER: etl
      POSTGRES_PASSWORD: etl
      POSTGRES_DB: etl
    # Postgres 18 keeps PGDATA under /var/lib/postgresql/<major>/docker;
    # mounting /var/lib/postgresql/data makes the container refuse to start.
    volumes:
      - pgdata:/var/lib/postgresql
    ports:
      - "127.0.0.1:${POSTGRES_PORT:-5432}:5432"
    healthcheck:
      # -h forces TCP: the init phase listens on the Unix socket only, so a
      # socket check would report healthy before the real server is up.
      test: ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "etl", "-d", "etl"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

  redpanda:
    image: redpandadata/redpanda:v26.2.4
    command:
      - redpanda
      - start
      - --mode=dev-container
      - --smp=1
      - --memory=1G
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda:9092,external://localhost:${REDPANDA_EXTERNAL_PORT:-19092}
    volumes:
      - redpanda:/var/lib/redpanda/data
      # Sets auto_create_topics_enabled=false on first start: topics are
      # created only by `etl init`, with explicit partitions and retention.
      - ./deploy/redpanda/bootstrap.yaml:/etc/redpanda/.bootstrap.yaml:ro
    ports:
      - "127.0.0.1:${REDPANDA_EXTERNAL_PORT:-19092}:19092"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 10s
    restart: unless-stopped

  versitygw:
    image: versity/versitygw:v1.8.0
    # posix backend: every top-level dir of /lake is a bucket, every object a
    # plain file. Metadata (sha256, content type) lives in the sidecar dir
    # because Docker Desktop bind mounts have no xattrs.
    command: ["--port", ":7070", "--health", "/health", "posix", "--sidecar", "/meta", "/lake"]
    user: "${ETL_UID:-65532}:${ETL_GID:-65532}"
    environment:
      ROOT_ACCESS_KEY_ID: ${ETL_S3_ACCESS_KEY:-etl}
      ROOT_SECRET_ACCESS_KEY: ${ETL_S3_SECRET_KEY:-etlsecret123}
    volumes:
      - ./data:/lake
      - ./.s3meta:/meta
    ports:
      - "127.0.0.1:${S3_PORT:-7070}:7070"
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:7070/health"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

  redpanda-console:
    image: redpandadata/console:v3.12.0
    environment:
      KAFKA_BROKERS: redpanda:9092
    ports:
      - "127.0.0.1:${CONSOLE_PORT:-8081}:8080"
    depends_on:
      redpanda:
        condition: service_healthy
    restart: unless-stopped

  prometheus:
    image: prom/prometheus:v3.15.0
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time=7d
    volumes:
      - ./deploy/prometheus:/etc/prometheus:ro
      - prometheus:/prometheus
    ports:
      - "127.0.0.1:${PROMETHEUS_PORT:-9090}:9090"
    restart: unless-stopped

  grafana:
    image: grafana/grafana:13.2.3
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /etc/grafana/dashboards/etl.json
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/grafana/dashboards:/etc/grafana/dashboards:ro
      - grafana:/var/lib/grafana
    ports:
      - "127.0.0.1:${GRAFANA_PORT:-3000}:3000"
    depends_on:
      - prometheus
    restart: unless-stopped

  # ---------------- pipeline ----------------
  init:
    <<: *etl
    command: ["init"]
    restart: "no"
    healthcheck:
      disable: true
    depends_on:
      postgres:
        condition: service_healthy
      redpanda:
        condition: service_healthy
      versitygw:
        condition: service_healthy

  fetcher:
    <<: *etl
    command: ["fetcher"]

  exporter:
    <<: *etl
    command: ["exporter"]

  publisher:
    <<: *etl
    command: ["publisher"]

  transformer:
    <<: *etl
    command: ["transformer"]
    environment:
      <<: *etl-env
      ETL_LOG_PER_HOST: "true" # replicas share ./logs: one file per container
    deploy:
      replicas: 2

volumes:
  pgdata:
  redpanda:
  prometheus:
  grafana:
```

- [ ] **Step 5: Validate the compose file**

Run: `docker compose config -q && echo OK`
Expected: `OK` (works without a `.env` file because `env_file` is `required: false`).

- [ ] **Step 6: Write the failing alert-rule unit tests**

Six test cases, one per alert from spec §8.4. The DLQ case covers a brand-new series (`increase()` alone misses the first increment of a counter that starts at 1); the schema-drift case checks that validations of other schemas do not dilute the ratio.

`deploy/prometheus/alerts_test.yml`:
```yaml
# Unit tests for alerts.yml: docker run --rm --entrypoint promtool \
#   -v "$PWD/deploy/prometheus:/p:ro" -w /p prom/prometheus:v3.15.0 test rules alerts_test.yml
rule_files:
  - alerts.yml
evaluation_interval: 1m

tests:
  - name: stale data fires after two quiet minutes
    interval: 1m
    input_series:
      - series: 'etl_last_success_timestamp_seconds{endpoint="coins_markets"}'
        values: '0x10' # last success at unix time 0, never again
    alert_rule_test:
      - eval_time: 1m
        alertname: ETLDataStale
        exp_alerts: []
      - eval_time: 5m
        alertname: ETLDataStale
        exp_alerts:
          - exp_labels: {severity: critical, endpoint: coins_markets}
            exp_annotations:
              summary: "No successful CoinGecko poll for coins_markets in over 2 minutes"
              description: "Check the fetcher logs (logs/fetcher/etl.log) for HTTP errors or a 429 cooldown."

  - name: a brand-new dlq series fires immediately
    interval: 1m
    input_series:
      - series: 'etl_dlq_total{stage="transform_input",error_class="decode_error"}'
        values: '_x20 1x10' # series appears at minute 21 with value 1
    alert_rule_test:
      - eval_time: 15m
        alertname: ETLDeadLetterRecords
        exp_alerts: []
      - eval_time: 22m
        alertname: ETLDeadLetterRecords
        exp_alerts:
          - exp_labels: {severity: warning, stage: transform_input, error_class: decode_error}
            exp_annotations:
              summary: "Records went to the dead-letter zone (transform_input / decode_error)"
              description: "Inspect data/dlq/; a human is needed."

  - name: source schema drift above 5 percent
    interval: 1m
    input_series:
      - series: 'etl_schema_validations_total{stage="transform_input",schema="coins_markets.v1",result="pass"}'
        values: '0+9x30'
      - series: 'etl_schema_validations_total{stage="transform_input",schema="coins_markets.v1",result="fail"}'
        values: '0+1x30'
      - series: 'etl_schema_validations_total{stage="fetch",schema="raw_event.v1",result="pass"}'
        values: '0+100x30' # other schemas do not dilute the ratio
    alert_rule_test:
      - eval_time: 25m
        alertname: ETLSourceSchemaDrift
        exp_alerts:
          - exp_labels: {severity: critical}
            exp_annotations:
              summary: "Over 5% of CoinGecko responses fail the source schema (upstream format change?)"
              description: "See data/dlq/stage=transform/ for source_schema_violation records."

  - name: export lag above five minutes
    interval: 1m
    input_series:
      - series: 'etl_export_lag_seconds'
        values: '400x10'
    alert_rule_test:
      - eval_time: 5m
        alertname: ETLExportLagHigh
        exp_alerts:
          - exp_labels: {severity: warning}
            exp_annotations:
              summary: "Exporter is 6m 40s behind raw.api_calls"
              description: "A long Postgres transaction stalls the xid8 cursor, or S3 is failing."

  - name: consumer lag growing for ten minutes
    interval: 1m
    input_series:
      - series: 'etl_consumer_lag_records{partition="0"}'
        values: '0+5x40'
    alert_rule_test:
      - eval_time: 5m
        alertname: ETLConsumerLagGrowing
        exp_alerts: []
      - eval_time: 30m
        alertname: ETLConsumerLagGrowing
        exp_alerts:
          - exp_labels: {severity: warning}
            exp_annotations:
              summary: "Transformer consumer lag has grown for 10 minutes"
              description: "Transformers are slower than the publisher, stuck, or crash-looping."

  - name: frequent 429s
    interval: 1m
    input_series:
      - series: 'etl_api_rate_limited_total'
        values: '0 0 0 1 2 3 4 4 4 4 4 4 4 4 4 4'
    alert_rule_test:
      - eval_time: 2m
        alertname: ETLFrequentRateLimiting
        exp_alerts: []
      - eval_time: 10m
        alertname: ETLFrequentRateLimiting
        exp_alerts:
          - exp_labels: {severity: warning}
            exp_annotations:
              summary: "CoinGecko returned HTTP 429 4 times in 15 minutes"
              description: "Lower the request rate or set COINGECKO_API_KEY."
```

- [ ] **Step 7: Run promtool to verify the tests fail**

Run: `docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus prom/prometheus:v3.15.0 test rules alerts_test.yml`
Expected: FAIL (exit 1): `WARNING: no file match pattern alerts.yml`, then a `FAILED:` block for each of the six test cases with `got:[]`, because `alerts.yml` does not exist yet.

- [ ] **Step 8: Write the Prometheus config and alert rules**

`deploy/prometheus/prometheus.yml`:
```yaml
# Scrapes every etl service on :8080/metrics. dns_sd_configs resolves the
# compose service name to one A record per container, so both transformer
# replicas (deploy.replicas: 2) are discovered without listing them.
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/alerts.yml

scrape_configs:
  - job_name: fetcher
    dns_sd_configs:
      - names: [fetcher]
        type: A
        port: 8080
        refresh_interval: 15s

  - job_name: exporter
    dns_sd_configs:
      - names: [exporter]
        type: A
        port: 8080
        refresh_interval: 15s

  - job_name: publisher
    dns_sd_configs:
      - names: [publisher]
        type: A
        port: 8080
        refresh_interval: 15s

  - job_name: transformer
    dns_sd_configs:
      - names: [transformer]
        type: A
        port: 8080
        refresh_interval: 15s

  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]
```

`deploy/prometheus/alerts.yml` (PromQL for spec §8.4):
```yaml
# Alert rules from spec §8.4. Label values follow the services' metrics:
# etl_schema_validations_total{result} is "pass" or "fail" in every stage, and
# schema is the schema file name without .json (coins_markets.v1, coin.v1, ...).
groups:
  - name: etl
    rules:
      - alert: ETLDataStale
        # No successful CoinGecko call for more than 2 minutes on an endpoint
        # that has succeeded before, or no successful markets call at all yet.
        expr: |
          time() - max by (endpoint) (etl_last_success_timestamp_seconds) > 120
          or
          absent(etl_last_success_timestamp_seconds{endpoint="coins_markets"})
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No successful CoinGecko poll for {{ $labels.endpoint }} in over 2 minutes"
          description: "Check the fetcher logs (logs/fetcher/etl.log) for HTTP errors or a 429 cooldown."

      - alert: ETLDeadLetterRecords
        # increase() misses the first sample of a brand-new series, so new
        # series (absent 10 minutes ago) are caught by the second branch.
        expr: |
          sum by (stage, error_class) (increase(etl_dlq_total[10m])) > 0
          or
          sum by (stage, error_class) (etl_dlq_total unless etl_dlq_total offset 10m) > 0
        labels:
          severity: warning
        annotations:
          summary: "Records went to the dead-letter zone ({{ $labels.stage }} / {{ $labels.error_class }})"
          description: "Inspect data/dlq/; a human is needed."

      - alert: ETLSourceSchemaDrift
        # More than 5% of CoinGecko responses fail the source schema (S0) in 15 minutes.
        expr: |
          sum(rate(etl_schema_validations_total{schema=~"coins_markets.v1|coin.v1", result="fail"}[15m]))
          /
          sum(rate(etl_schema_validations_total{schema=~"coins_markets.v1|coin.v1"}[15m]))
          > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Over 5% of CoinGecko responses fail the source schema (upstream format change?)"
          description: "See data/dlq/stage=transform/ for source_schema_violation records."

      - alert: ETLExportLagHigh
        expr: max(etl_export_lag_seconds) > 300
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Exporter is {{ $value | humanizeDuration }} behind raw.api_calls"
          description: "A long Postgres transaction stalls the xid8 cursor, or S3 is failing."

      - alert: ETLConsumerLagGrowing
        # Total lag has been rising for 10 minutes and is not trivially small.
        expr: |
          deriv(sum(etl_consumer_lag_records)[10m:1m]) > 0
          and
          sum(etl_consumer_lag_records) > 10
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Transformer consumer lag has grown for 10 minutes"
          description: "Transformers are slower than the publisher, stuck, or crash-looping."

      - alert: ETLFrequentRateLimiting
        expr: sum(increase(etl_api_rate_limited_total[15m])) >= 3
        labels:
          severity: warning
        annotations:
          summary: "CoinGecko returned HTTP 429 {{ $value }} times in 15 minutes"
          description: "Lower the request rate or set COINGECKO_API_KEY."
```

- [ ] **Step 9: Run promtool to verify config and rules**

Run:
```bash
docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" prom/prometheus:v3.15.0 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus prom/prometheus:v3.15.0 test rules alerts_test.yml
```
Expected: `SUCCESS: /etc/prometheus/prometheus.yml is valid prometheus config file syntax`, `SUCCESS: 6 rules found`, then `SUCCESS` for the rule tests.

- [ ] **Step 10: Write the Grafana provisioning and dashboard**

`deploy/grafana/provisioning/datasources/prometheus.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

`deploy/grafana/provisioning/dashboards/etl.yml`:
```yaml
apiVersion: 1
providers:
  - name: etl
    folder: ETL
    type: file
    disableDeletion: true
    allowUiUpdates: false
    options:
      path: /etc/grafana/dashboards
```

The dashboard has the panels of spec §8.5: headline stats (freshness, duplicate share, DLQ, p95 latency), records per stage, freshness and source age, end-to-end latency quantiles, duplicate share by reason, rejects by reason, DLQ by stage and class, consumer lag, API status codes, export lag with S3 PUT results, and schema validations. Note `group_left()` with empty parentheses in the "Duplicate share" panel: without them PromQL reads the following parenthesised expression as a label list and the query fails with HTTP 400.

`deploy/grafana/dashboards/etl.json`:
```json
{
  "uid": "coingecko-etl",
  "title": "CoinGecko ETL",
  "tags": ["etl", "coingecko"],
  "timezone": "utc",
  "schemaVersion": 41,
  "version": 1,
  "editable": false,
  "graphTooltip": 1,
  "refresh": "15s",
  "time": {"from": "now-1h", "to": "now"},
  "timepicker": {},
  "templating": {"list": []},
  "annotations": {"list": []},
  "links": [],
  "panels": [
    {
      "id": 1,
      "type": "stat",
      "title": "Data freshness",
      "description": "Seconds since the last successful markets poll (alert at 120 s).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 0, "w": 6, "h": 4},
      "fieldConfig": {"defaults": {"unit": "s", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "orange", "value": 60}, {"color": "red", "value": 120}]}, "color": {"mode": "thresholds"}}, "overrides": []},
      "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "colorMode": "value", "graphMode": "area", "textMode": "auto", "justifyMode": "auto", "orientation": "auto"},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "time() - max(etl_last_success_timestamp_seconds{endpoint=\"coins_markets\"})", "instant": false}
      ]
    },
    {
      "id": 2,
      "type": "stat",
      "title": "Duplicate share (1h)",
      "description": "Records dropped by dedup / (dropped + written) over the last hour. ~50% is expected: the CDN serves the same snapshot for 60 s and we poll every 30 s.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 6, "y": 0, "w": 6, "h": 4},
      "fieldConfig": {"defaults": {"unit": "percentunit", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}, "color": {"mode": "thresholds"}, "decimals": 1}, "overrides": []},
      "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "colorMode": "value", "graphMode": "area", "textMode": "auto", "justifyMode": "auto", "orientation": "auto"},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum(increase(etl_dedup_dropped_total[1h])) / (sum(increase(etl_dedup_dropped_total[1h])) + sum(increase(etl_records_total{outcome=\"written\"}[1h])))", "instant": false}
      ]
    },
    {
      "id": 3,
      "type": "stat",
      "title": "DLQ records (24h)",
      "description": "Records sent to the dead-letter zone in the last 24 hours; anything above 0 needs a human.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 12, "y": 0, "w": 6, "h": 4},
      "fieldConfig": {"defaults": {"unit": "short", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "red", "value": 1}]}, "color": {"mode": "thresholds"}}, "overrides": []},
      "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "colorMode": "value", "graphMode": "area", "textMode": "auto", "justifyMode": "auto", "orientation": "auto"},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum(increase(etl_dlq_total[24h])) or vector(0)", "instant": false}
      ]
    },
    {
      "id": 4,
      "type": "stat",
      "title": "End-to-end latency p95",
      "description": "95th percentile from fetched_at to the processed object being written (15 min window).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 18, "y": 0, "w": 6, "h": 4},
      "fieldConfig": {"defaults": {"unit": "s", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}, "color": {"mode": "thresholds"}}, "overrides": []},
      "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "colorMode": "value", "graphMode": "area", "textMode": "auto", "justifyMode": "auto", "orientation": "auto"},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "histogram_quantile(0.95, sum by (le) (rate(etl_e2e_latency_seconds_bucket[15m])))", "instant": false}
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Records per stage",
      "description": "Throughput of every hop: API calls stored, raw records published to the broker, processed records by outcome.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 4, "w": 24, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum(rate(etl_api_requests_total{status=\"200\"}[5m])) * 60", "legendFormat": "fetched calls / min"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "sum(rate(etl_published_records_total[5m])) * 60", "legendFormat": "published / min"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "C", "expr": "sum by (outcome) (rate(etl_records_total[5m])) * 60", "legendFormat": "transformer {{outcome}} / min"}
      ]
    },
    {
      "id": 6,
      "type": "timeseries",
      "title": "Data freshness and source age",
      "description": "Seconds since the last successful call per endpoint, and how old CoinGecko's own data was when fetched (fetched_at - last_updated).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 12, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "s", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "time() - etl_last_success_timestamp_seconds", "legendFormat": "since last success: {{endpoint}}"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "etl_source_data_age_seconds", "legendFormat": "source data age: {{endpoint}}"}
      ]
    },
    {
      "id": 7,
      "type": "timeseries",
      "title": "End-to-end latency",
      "description": "Quantiles of fetched_at -> processed object written.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 12, "y": 12, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "s", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "histogram_quantile(0.5, sum by (le) (rate(etl_e2e_latency_seconds_bucket[5m])))", "legendFormat": "p50"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "histogram_quantile(0.95, sum by (le) (rate(etl_e2e_latency_seconds_bucket[5m])))", "legendFormat": "p95"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "C", "expr": "histogram_quantile(0.99, sum by (le) (rate(etl_e2e_latency_seconds_bucket[5m])))", "legendFormat": "p99"}
      ]
    },
    {
      "id": 8,
      "type": "timeseries",
      "title": "Duplicate share",
      "description": "Share of records dropped by deduplication, total and by reason (replay = same event redelivered, duplicate_response = same API response, duplicate_record = same record in another snapshot).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 20, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "percentunit", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum(rate(etl_dedup_dropped_total[15m])) / (sum(rate(etl_dedup_dropped_total[15m])) + sum(rate(etl_records_total{outcome=\"written\"}[15m])))", "legendFormat": "total"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "sum by (reason) (rate(etl_dedup_dropped_total[15m])) / ignoring(reason) group_left() (sum(rate(etl_dedup_dropped_total[15m])) + sum(rate(etl_records_total{outcome=\"written\"}[15m])))", "legendFormat": "{{reason}}"}
      ]
    },
    {
      "id": 9,
      "type": "timeseries",
      "title": "Rejected records by reason",
      "description": "Cleaning-rule drops written to the rejected zone (missing_id, null_price, nonpositive_price, empty_platform, empty_record, snapshot_conflict).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 12, "y": 20, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false, "stacking": {"mode": "normal", "group": "A"}}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum by (reason) (increase(etl_rejected_total[15m]))", "legendFormat": "{{reason}}"}
      ]
    },
    {
      "id": 10,
      "type": "timeseries",
      "title": "Dead-letter records",
      "description": "Records written to the dlq zone by stage and error class.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 28, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false, "stacking": {"mode": "normal", "group": "A"}}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum by (stage, error_class) (increase(etl_dlq_total[15m]))", "legendFormat": "{{stage}} / {{error_class}}"}
      ]
    },
    {
      "id": 11,
      "type": "timeseries",
      "title": "Consumer lag",
      "description": "High watermark minus committed offset per partition, summed over transformer replicas.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 12, "y": 28, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum by (partition) (etl_consumer_lag_records)", "legendFormat": "partition {{partition}}"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "sum(etl_consumer_lag_records)", "legendFormat": "total"}
      ]
    },
    {
      "id": 12,
      "type": "timeseries",
      "title": "CoinGecko API status codes",
      "description": "Calls per minute by endpoint and HTTP status (429 = rate limited, network_error, skipped = tick skipped by the limiter).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 36, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false, "stacking": {"mode": "normal", "group": "A"}}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum by (endpoint, status) (rate(etl_api_requests_total[5m])) * 60", "legendFormat": "{{endpoint}} {{status}}"}
      ]
    },
    {
      "id": 13,
      "type": "timeseries",
      "title": "Export lag and S3 writes",
      "description": "Age of the oldest unexported raw row, and conditional S3 PUT results (exists = idempotent retry, mismatch = alert).",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 12, "y": 36, "w": 12, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "max(etl_export_lag_seconds)", "legendFormat": "export lag (s)"},
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "B", "expr": "sum by (result) (rate(etl_s3_put_total[5m])) * 60", "legendFormat": "s3 put {{result}} / min"}
      ]
    },
    {
      "id": 14,
      "type": "timeseries",
      "title": "Schema validations",
      "description": "Validations per minute by stage, schema and result.",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "gridPos": {"x": 0, "y": 44, "w": 24, "h": 8},
      "fieldConfig": {"defaults": {"unit": "short", "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never", "spanNulls": false}}, "overrides": []},
      "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": true}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"datasource": {"type": "prometheus", "uid": "prometheus"}, "refId": "A", "expr": "sum by (stage, schema, result) (rate(etl_schema_validations_total[5m])) * 60", "legendFormat": "{{stage}} {{schema}} {{result}}"}
      ]
    }
  ]
}
```

Run: `jq empty deploy/grafana/dashboards/etl.json && echo OK`
Expected: `OK`.

- [ ] **Step 11: Replace the Makefile**

It keeps the Task 1 targets (`build`, `test`, `test-unit`, `lint`) and adds `alerts-test`. `up` creates only `data`, `logs` and `.s3meta` (not the bucket directories, see background) and copies `.env.example` to `.env` when missing. `demo` waits 150 s and prints the report; `demo-report` can be re-run at any time.

`Makefile`:
```makefile
SHELL := /bin/bash
GO ?= go
COMPOSE ?= docker compose
BIN := bin/etl

# Files written through bind mounts (./data, ./logs, ./.s3meta) belong to the
# invoking user, so the containers run with the same uid/gid (see docker-compose.yml).
export ETL_UID ?= $(shell id -u)
export ETL_GID ?= $(shell id -g)

PROMETHEUS_URL ?= http://localhost:9090
GRAFANA_URL ?= http://localhost:3000
CONSOLE_URL ?= http://localhost:8081
DEMO_WAIT ?= 150

.PHONY: build test test-unit lint alerts-test up down demo demo-report

build:
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/etl

# All tests, including Docker-backed integration tests (skipped when Docker is unavailable).
test:
	$(GO) test -race -count=1 ./...

# Unit tests only: integration tests skip themselves under -short.
test-unit:
	$(GO) test -short -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi
	$(GO) vet ./...

# Validate the Prometheus config and unit-test the alert rules with promtool.
alerts-test:
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" \
	  prom/prometheus:v3.15.0 check config /etc/prometheus/prometheus.yml
	docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus \
	  prom/prometheus:v3.15.0 test rules alerts_test.yml

up:
	mkdir -p data logs .s3meta
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

demo: up
	@echo "Pipeline started; waiting $(DEMO_WAIT)s for data to flow through every stage..."
	@sleep $(DEMO_WAIT)
	@$(MAKE) --no-print-directory demo-report

# demo-report prints the summary without (re)starting the stack.
demo-report:
	@printf '%-10s %8s %10s\n' bucket objects records
	@for b in raw processed rejected dlq; do \
	  objects=$$(find data/$$b -type f -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' '); \
	  records=$$(find data/$$b -type f -name '*.jsonl' -exec cat {} + 2>/dev/null | wc -l | tr -d ' '); \
	  printf '%-10s %8s %10s\n' "$$b" "$$objects" "$$records"; \
	done
	@set -o pipefail; echo -n "duplicate share (dropped / (dropped + written)): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=sum(etl_dedup_dropped_total) / (sum(etl_dedup_dropped_total) + sum(etl_records_total{outcome="written"}))' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p' | grep . || echo "n/a (no data yet or Prometheus not reachable)"
	@echo "Grafana:          $(GRAFANA_URL)  (dashboard: ETL / CoinGecko ETL)"
	@echo "Redpanda Console: $(CONSOLE_URL)"
	@echo "Prometheus:       $(PROMETHEUS_URL)/alerts"
	@echo "Lake on disk:     $(CURDIR)/data/{raw,processed,rejected,dlq}"
```

Run: `make -n up demo-report >/dev/null && make alerts-test`
Expected: no make errors; the promtool output from Step 9.

- [ ] **Step 12: Smoke-test the full stack**

Run:
```bash
make up
until [ -n "$(find data/processed -name '*.jsonl' | head -1)" ]; do sleep 5; done   # 1–2 minutes
docker compose ps -a --format '{{.Name}} {{.Status}}'
docker compose exec redpanda rpk cluster config get auto_create_topics_enabled
curl -s localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | "\(.labels.job) \(.health)"'
make demo-report
```
Expected: `init` is `Exited (0)`; every other container is `Up`, the etl ones `(healthy)`; auto-create prints `false`; Prometheus lists fetcher, exporter, publisher, two transformer targets and itself, all `up`; after ~3 minutes the report shows objects in `raw` and `processed` (for example 7 raw objects, 12 processed objects with ~700 records), 0 in `dlq`, and a duplicate share between 0.3 and 0.5. Logs appear in `logs/fetcher/etl.log`, `logs/exporter/etl.log`, `logs/publisher/etl.log`, `logs/init/etl.log` and `logs/<container id>/transformer/etl.log` (one per replica). Then `make down`; a second `make up` reruns `init` (exit 0) and the pipeline continues with the next batch number.

- [ ] **Step 13: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml .env.example Makefile deploy
git commit -m "$(cat <<'EOF'
feat: container image, compose stack and monitoring

Multi-stage distroless image, docker compose with Postgres, Redpanda,
versitygw, Console, Prometheus and Grafana, alert rules with promtool
tests, a provisioned dashboard and make up/down/demo.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 18: README, ADRs, CI

**Files:**
- Create: `README.md`
- Create: `docs/adr/0001-object-storage-versitygw.md`, `docs/adr/0002-single-dedup-in-transformer.md`, `docs/adr/0003-redpanda.md`, `docs/adr/0004-postgres-pipeline-state.md`
- Create: `.golangci.yml`, `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: everything above: make targets (`up`, `demo`, `down`, `test`, `test-unit`, `lint`, `alerts-test`), compose services and ports (Task 17), metric names (Task 1), alert names (Task 17: `ETLDataStale`, `ETLDeadLetterRecords`, `ETLSourceSchemaDrift`, `ETLExportLagHigh`, `ETLConsumerLagGrowing`, `ETLFrequentRateLimiting`), consumer group `transformer.ConsumerGroup = "coingecko-transformer"` (Task 15).
- Produces: documentation and CI only; no Go API.

- [ ] **Step 1: Write the README**

It follows spec §12: overview and diagram, the broker rationale with the broker table, quick start, browsing the lake, data and time model, schemas, delivery guarantees with the crash table, metrics explained, alerts, logs, `docker run -v` persistence steps, assumptions, productionization (AWS mapping, scalability, reliability), layout and tests, and the CoinGecko attribution (top and bottom). The `docker run` block was executed as written (with prefixed container names) and produced raw and processed objects.

`README.md`:
````markdown
# CoinGecko ETL

A small but complete ETL pipeline in Go. It polls the public CoinGecko API, keeps every raw API call in Postgres, exports the raw calls as JSON Lines to an S3-compatible data lake, streams them through a Kafka-protocol broker (Redpanda), and writes cleaned, deduplicated and normalized JSON Lines to a processed zone. Every hop is at-least-once; the processed zone still contains every record exactly once, because every write is idempotent and deduplication happens in one place.

Everything runs locally with `docker compose`. The lake is plain files under `./data/`, so you can `cat` any object.

Data provided by [CoinGecko](https://www.coingecko.com/en/api).

## Contents

1. [Architecture](#architecture)
2. [Why the Kafka protocol, and why Redpanda](#why-the-kafka-protocol-and-why-redpanda)
3. [Quick start](#quick-start)
4. [Browsing the lake](#browsing-the-lake)
5. [Data model and time model](#data-model-and-time-model)
6. [Schemas and validation](#schemas-and-validation)
7. [Delivery guarantees](#delivery-guarantees)
8. [Metrics](#metrics)
9. [Alerts](#alerts)
10. [Logs](#logs)
11. [Running the image with `docker run -v`](#running-the-image-with-docker-run--v)
12. [Assumptions](#assumptions)
13. [Productionization](#productionization)
14. [Repository layout and tests](#repository-layout-and-tests)

## Architecture

```mermaid
flowchart LR
    CG[CoinGecko API] -->|"/coins/markets every 30 s<br/>/coins/{id} every 60 s"| F[etl fetcher]
    F -->|"INSERT, one row per HTTP call"| PG[("Postgres<br/>raw.api_calls")]
    PG -->|"xid8 cursor +<br/>pipeline.export_batches"| E[etl exporter]
    E -->|"PUT If-None-Match: *"| RAW[("s3://raw<br/>./data/raw")]
    RAW --> P[etl publisher]
    PG -. "lease batch" .- P
    P -->|"key = coins_markets / coin id"| K[["Redpanda<br/>coingecko.raw<br/>6 partitions"]]
    K -->|consumer group| T["etl transformer x2<br/>validate, clean, dedup,<br/>normalize, validate"]
    T -->|claims + batch manifest| PG
    T --> PROC[("s3://processed<br/>./data/processed")]
    T --> REJ[("s3://rejected<br/>./data/rejected")]
    T --> DLQ[("s3://dlq<br/>./data/dlq")]
```

All services are subcommands of one Go binary, `etl`, built into one container image:

| Command | What it does | Replicas |
|---|---|---|
| `etl init` | One-shot and idempotent: Postgres migrations (goose) and daily partitions, the broker topic (created via `kadm`, verified if it exists), the four S3 buckets. Retries each dependency for up to 60 s. | 1, runs to completion |
| `etl fetcher` | Pure transport. Polls CoinGecko through a rate limiter, wraps each HTTP call in an event envelope with an idempotency key and inserts it into `raw.api_calls`, failures included. | 1 |
| `etl exporter` | Every ~15 s claims the next range of new rows and writes one raw JSONL object. | 1 (Postgres advisory lock) |
| `etl publisher` | Leases exported batches, reads the object through S3, validates and produces each line. | 1 (leases allow more) |
| `etl transformer` | Consumer group. Validates, cleans, deduplicates and normalizes; writes processed, rejected and dlq objects. | 2 |
| `etl healthcheck` | Exit code for container healthchecks (the distroless image has no curl). | — |

Infrastructure: Postgres 18.6, Redpanda v26.2.4, versitygw v1.8.0 (S3 gateway over plain files), Redpanda Console, Prometheus and Grafana.

Why this chain and not a shorter one? Fetcher → broker directly would be simpler, but the assignment asks for raw data in Postgres first. Debezium CDC (Postgres → Kafka in commit order) is the production choice for the Postgres → broker leg, but it adds a JVM Kafka Connect stack. Kafka Streams would keep dedup state in RocksDB with a changelog, but it is JVM-only and its exactly-once stops at the Kafka boundary. All three are listed under [Productionization](#productionization).

## Why the Kafka protocol, and why Redpanda

The design needs three things from the broker:

- a **replayable log**: after a schema fix, raw data is republished and reprocessed; a consumer crash resumes from a committed offset;
- **per-key ordering**: every repeat of the same data (same endpoint or coin) lands on one partition, so deduplication decisions are made sequentially and deterministically;
- **consumer groups**: transformer replicas share partitions and take over each other's work on failure.

The Kafka protocol provides all three, and it is the de-facto standard for data engineering at this scale: connectors, stream processors, lakehouse sinks and managed offerings all speak it.

Redpanda is used for the local stack because it speaks the same protocol as a single C++ binary without a JVM: it starts in about two seconds, idles at well under 200 MiB and needs no separate controller. The Go client ([franz-go](https://github.com/twmb/franz-go)) does not know which broker it talks to, so the same code runs unchanged against Apache Kafka, Amazon MSK or Confluent Cloud in production. The main thing Redpanda lacks is the Kafka Connect ecosystem, and this project does not use Kafka Connect. Redpanda's licence (BSL 1.1) allows this use.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen for the local stack: same protocol and client code, no JVM, lighter and faster to start |
| Apache Kafka, Amazon MSK, Confluent Cloud | Drop-in for production: the code speaks the Kafka protocol |
| NATS JetStream, Apache Pulsar | Viable with caveats: a different consumer model (JetStream) or heavier operations (Pulsar, with BookKeeper), and a weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, Amazon SQS, Google Pub/Sub | Rejected: no offset-based replay or per-key ordering at scale |

## Quick start

Prerequisites: Docker with Compose v2 (Docker Desktop on macOS works), GNU make, curl. About 1.2 GB of free RAM.

```bash
make up      # creates ./data ./logs ./.s3meta, copies .env.example to .env, builds and starts everything
make demo    # make up, waits 150 s, prints object/record counts per bucket, duplicate share and links
make down    # stops the stack; data, logs and named volumes are kept
```

`make demo` prints something like:

```
bucket      objects    records
raw               7          9
processed        12        703
rejected          0          0
dlq               0          0
duplicate share (dropped / (dropped + written)): 0.36
Grafana:          http://localhost:3000  (dashboard: ETL / CoinGecko ETL)
Redpanda Console: http://localhost:8081
Prometheus:       http://localhost:9090/alerts
```

A duplicate share around one third to one half is expected: CoinGecko's CDN caches responses for 60 s and the fetcher polls every 30 s, so roughly every second markets response is byte-identical to the previous one.

Configuration lives in `.env` (see `.env.example`): an optional `COINGECKO_API_KEY` (sent as `x-cg-demo-api-key`; the keyless tier works at the default ~3 calls per minute), host ports, polling intervals and cleaning thresholds.

To wipe everything and start from scratch: `make down`, `docker compose down -v`, then `rm -rf data logs .s3meta`.

## Browsing the lake

The S3 gateway (versitygw, posix backend) stores every bucket as a directory and every object as a plain file under `./data`. Object metadata (content type, the `sha256` we attach) lives separately in `./.s3meta`, because Docker Desktop bind mounts do not support extended attributes.

```
data/raw/source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl
data/processed/dataset=ticker/v=1/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl
data/rejected/dataset=coin_contract/event_date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
data/dlq/stage=transform/date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
```

Four ways to look at it:

- **Plain files.** `find data -name '*.jsonl'`, then `head -n1 <file> | jq .`. Do not edit or add files by hand: the gateway's write-once check relies on the metadata it stored itself.
- **Any S3 client.** Endpoint `http://localhost:7070`, access key `etl`, secret `etlsecret123`, region `us-east-1`, path-style addressing. For example with the AWS CLI:
  ```bash
  export AWS_ACCESS_KEY_ID=etl AWS_SECRET_ACCESS_KEY=etlsecret123 AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url http://localhost:7070 s3 ls s3://processed/ --recursive | head
  ```
- **Redpanda Console** at <http://localhost:8081>: topic `coingecko.raw`, messages with their headers (`event_id`, `idempotency_key`, `schema`, `raw_object`, `line_no`), and the `coingecko-transformer` consumer group with its lag.
- **Postgres** at `localhost:5432` (user, password and database `etl`): `raw.api_calls` holds every HTTP call; `pipeline.*` holds the ledgers.
  ```sql
  SELECT outcome, http->>'status' AS status, count(*) FROM raw.api_calls GROUP BY 1, 2;
  SELECT idempotency_key, count(*) FROM raw.api_calls WHERE outcome = 'ok'
  GROUP BY 1 HAVING count(*) > 1;                          -- identical responses before dedup
  ```
- **Grafana** at <http://localhost:3000> (anonymous viewer; admin password in `.env`): the provisioned dashboard *ETL / CoinGecko ETL*.

## Data model and time model

### Source

| Endpoint | Schedule | Why |
|---|---|---|
| `/coins/markets?vs_currency=usd&per_page=100&price_change_percentage=1h,24h,7d&precision=full` | every 30 s | a flat 30-second time series of the top 100 coins |
| `/coins/{id}?tickers=true&market_data=true` | every 60 s, rotating over bitcoin, ethereum, tether, usd-coin, 15 s after the markets call | deep nesting: 23 maps × 63 currencies, 100 tickers with nested objects, contracts per chain |

The keyless CoinGecko tier allows a dynamic 10–30 calls per minute per IP; we saw HTTP 429 with `Retry-After: 60` on the sixth call within 27 s. The fetcher therefore spends ~3 calls per minute, keeps one request in flight, honours `Retry-After`, backs off with jitter on 5xx and timeouts, and skips ticks instead of queueing them.

### Raw zone

`raw.api_calls` in Postgres has one row per HTTP call, including failures (`outcome` is `ok`, `http_error` or `network_error`). The response body is stored in a `json` column (not `jsonb`), which keeps the exact bytes, so `payload_sha256` can be re-verified at every hop. Each row also carries `idempotency_key = sha256(source | endpoint | canonical params | payload_sha256)`; two polls that received the same cached response share the key. The table is partitioned by day on `fetched_at`.

The exporter writes the same rows (successful calls only) as JSON Lines into `s3://raw`, one line per call, with the response embedded verbatim.

### Processed datasets

| Dataset | Source | Natural key | Rows per snapshot |
|---|---|---|---|
| `market_snapshot` | `/coins/markets` | coin_id, snapshot_at | 100 |
| `coin_snapshot` | `/coins/{id}` scalars (rank, supply, categories, genesis date, …) | coin_id, snapshot_at | 1 |
| `coin_market_by_currency` | the 23 `market_data` maps zipped on the currency key | coin_id, currency, snapshot_at | 63 |
| `ticker` | `tickers[]`, with `market` flattened to `exchange_id`/`exchange_name` and converted values to `_usd`/`_btc`/`_eth` columns | coin_id, exchange_id, base, target, snapshot_at | 100 |
| `coin_contract` | `platforms` + `detail_platforms` zipped on the platform key | coin_id, platform_id, snapshot_at | up to 36 |

The child datasets reference `coin_snapshot` by `(coin_id, snapshot_at)`. Images, descriptions, links, sparklines and duplicated fields are dropped. Every row carries technical lineage columns: `_event_id`, `_raw_object`, `_kafka_partition`, `_kafka_offset`, `_schema_version` and `_processed_at` (taken from the batch manifest, never from the wall clock, so a retried batch produces identical bytes).

### Formats

- Timestamps: RFC 3339 in UTC with milliseconds, e.g. `2026-10-10T10:07:55.000Z`; `genesis_date` as `YYYY-MM-DD`.
- Money and quantities (prices, market caps, volumes, supplies) are decimal strings in plain notation, e.g. `"43083803721443304"`. They are decoded with `json.Number` and a decimal type and never pass through `float64`: some values exceed 2^53. Percentages and ratios are JSON numbers.
- Symbols are upper-case; EVM contract addresses lower-case.
- Files are plain `.jsonl` (no compression) so they can be read with `cat`.

### Cleaning rules

- **Normalize:** `""` and `[]` become `null`; timestamps and numbers as above; `roi` is flattened to `roi_times`, `roi_currency`, `roi_pct`.
- **Reject** to `s3://rejected` with a reason: `missing_id`, `null_price`, `nonpositive_price`, `empty_platform`, `empty_record` (all metrics null), `snapshot_conflict` (see below).
- **Flag and keep:**
  - `is_stale`: `snapshot_at − source_updated_at > 10 min`; for tickers, the upstream flag or a last trade older than 1 h;
  - `is_inactive`: zero volume;
  - `is_anomaly`: price outside `[low_24h, high_24h]`; for tickers, the upstream flag or `last_usd` more than 2% away from the coin's median over all its tickers in the snapshot.

The thresholds are configuration (`ETL_STALE_AFTER`, `ETL_TICKER_STALE_AFTER`, `ETL_TICKER_ANOMALY_PCT`).

### Time model

| Field | Source | Meaning |
|---|---|---|
| `snapshot_at` | derived: the newest `last_updated` in a markets response; the document's `last_updated` for `/coins/{id}` | when CoinGecko assembled the snapshot; event time and the partition key of the processed zone (`event_date`) |
| `source_updated_at` | the coin's `last_updated` | when CoinGecko last recomputed this coin; lags `snapshot_at` by hours for stale coins |
| `fetched_at` | our clock | when we polled; lineage only |
| `last_traded_at`, `last_fetch_at` (tickers) | exchange data | exchange trade time; when CoinGecko fetched the exchange |

CoinGecko recomputes all coins in one pass every 60 s and stamps them with the same `last_updated`. An event is a source snapshot: a stale coin observed in two different snapshots yields two valid rows (both flagged `is_stale`), while two polls that were served the same cached response yield one. Clocks of different systems are not synchronized (we have seen a trade at 10:00:45 reported by a fetch at 10:00:40), so no logic depends on comparing them.

## Schemas and validation

Every stage has a JSON Schema (draft 2020-12) in `schemas/`, embedded into the binary with `go:embed` and compiled at startup (a compile error stops the service). The major version is part of the file name, the `$id`, the `schema_version` field of each record and the `v=` path segment.

| Schema | Describes | Policy |
|---|---|---|
| S0 `source/coingecko/coins_markets.v1.json`, `coin.v1.json` | CoinGecko responses | Permissive: requires and types only the fields we use, allows extra fields, allows `null` where observed |
| S1 `pipeline/raw_event.v1.json` | one API call: the Postgres row = the raw JSONL line = the message value | Strict (`additionalProperties: false` except for the payload) |
| S2 `processed/<dataset>.v1.json` | processed records | Strict |
| S3 `pipeline/rejected_record.v1.json`, `dlq_record.v1.json` | rejected and broken records | Strict |

| Stage | Validates | On failure |
|---|---|---|
| fetcher, before INSERT | S1 envelope | ERROR log (a bug on our side) |
| exporter, publisher | S1 | the line goes to `s3://dlq`, not downstream |
| transformer, input | S1, payload checksum, then the payload against S0 | `s3://dlq` with class `decode_error`, `schema_violation`, `integrity_error` or `source_schema_violation` |
| transformer, output | S2 | `s3://dlq`; never written to processed |

`rejected/` is for records removed by a cleaning rule; `dlq/` means "something broke and a human is needed". Because schemas ship inside the binary, a schema change is a redeploy: consumers (transformer) first, then producers. A breaking change gets a new major version (`v2`) that runs side by side.

## Delivery guarantees

Every hop is at-least-once. The processed zone nevertheless contains each record exactly once, because every step can be repeated safely: deterministic object keys, ledgers in Postgres, and write-once objects (`PUT` with `If-None-Match: *`). **Deduplication happens once, in the transformer; idempotency happens at every write.**

| Hop | Mechanism | On retry |
|---|---|---|
| fetcher → `raw.api_calls` | primary key `event_id`, `ON CONFLICT DO NOTHING` | at most one missed poll |
| exporter → `s3://raw` | xid8 cursor + `pipeline.export_batches` ledger; `If-None-Match: *` | same object: 412, checksums equal |
| publisher → broker | lease from the ledger; idempotent producer, `acks=all` | copies with the same `event_id`, dropped by the transformer |
| transformer → `s3://processed` | batch manifest; write-once objects; offset commit after the write | same objects: 412, checksums equal |

**Exporter cursor.** An identity-column watermark loses rows when transactions commit out of order. The exporter reads rows by `(ingest_txid, seq)` bounded by `pg_snapshot_xmin(pg_current_snapshot())`, so it can only stall behind a long transaction, never skip a row.

**Deduplication.** Every record bound for `processed/` or `rejected/` claims its natural key in `pipeline.dedup_claims` (`INSERT … ON CONFLICT DO NOTHING`); the first claimant wins:

| The claim for the key points to | Action |
|---|---|
| this record's own position (partition, offset, ordinal) | write it (first sight, or a redelivery after a crash) |
| another position, same content hash | drop as a duplicate |
| another position, different content hash | write to `rejected/` as `snapshot_conflict` |

The content hash covers business fields only, so the same record arriving in two messages matches even though its lineage columns differ. Broker offsets cannot answer "was key X seen?", S3 can only guard single objects, and in-memory state is lost on rebalance, which is why the claims live in Postgres.

**Transformer batch.** A batch is a contiguous offset range of one partition. (1) Input checks without the database. (2) A short transaction records the range in `pipeline.consumer_batches` (`planned`), claims keys, decides, validates, and stores the object keys and checksums. (3) Objects are written with `If-None-Match: *`. (4) The batch is marked `written`. (5) The offset is committed. On partition assignment the manifest decides: `written` → skip past it, `planned` → reprocess exactly that range.

| Crash point | Recovery | Loss | Duplicates |
|---|---|---|---|
| before the first transaction commits | the batch starts over | no | no |
| after the transaction, before any PUT | manifest `planned` → same range; claims point to the same positions → same decisions, same bytes | no | no |
| after some PUTs | remaining PUTs succeed; finished ones return 412 with an equal checksum | no | no |
| after marking `written`, before the offset commit | manifest `written` → seek past the batch, commit | no | no |
| a zombie consumer and the new owner process the same range | same range from the manifest, identical bytes; the first PUT wins, the second gets 412 with an equal checksum; the zombie's offset commit is rejected | no | no |

The naive rule "drop if the key was already claimed" would lose every record of a batch that crashed between its transaction and the PUT; claims that remember their position avoid that.

Invariants: transforms are pure (no clock, no random order); `dedup_claims` and `consumer_batches` outlive topic retention (30 days vs 7); Postgres runs with `idle_in_transaction_session_timeout = 60s` and `transaction_timeout = 5min`, so a stuck transaction cannot stall the exporter forever.

## Metrics

Every long-running service listens on `:8080` with `/healthz` (the process is alive), `/readyz` (its dependencies answer: Postgres for all, S3 for exporter, publisher and transformer, the broker for publisher and transformer) and `/metrics`. Prometheus discovers the containers through Docker DNS, including both transformer replicas. All pipeline metrics start with `etl_`.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `etl_api_requests_total` | counter | endpoint, status | CoinGecko calls by HTTP status, `network_error`, or `skipped` (tick skipped by the limiter) |
| `etl_api_request_duration_seconds` | histogram | endpoint | API latency, body read included |
| `etl_api_rate_limited_total` | counter | — | HTTP 429 responses |
| `etl_last_success_timestamp_seconds` | gauge | endpoint | Unix time of the last successful call; freshness = `time() - this` |
| `etl_source_data_age_seconds` | gauge | endpoint | how old CoinGecko's data was when we fetched it |
| `etl_schema_validations_total` | counter | stage, schema, result | validations per stage and schema; the failure share is the drift signal |
| `etl_schema_violations_total` | counter | stage, schema, keyword | which JSON Schema keyword failed (`required`, `type`, …) |
| `etl_export_lag_seconds` | gauge | — | age of the oldest row not yet exported; grows when a long transaction blocks the cursor |
| `etl_export_batches_total` | counter | result | exporter batches |
| `etl_s3_put_total` | counter | result | conditional PUTs: `created`, `exists` (an idempotent retry), `mismatch` (must never happen), `error` |
| `etl_published_records_total` | counter | — | messages acknowledged by the broker |
| `etl_records_total` | counter | dataset, outcome | transformer output: `written`, `duplicate`, `rejected`, `dlq` |
| `etl_dedup_dropped_total` | counter | reason | duplicates removed: `replay` (same event redelivered), `duplicate_response` (same API response polled twice), `duplicate_record` (same record in another snapshot) |
| `etl_rejected_total` | counter | reason | records removed by cleaning rules |
| `etl_dlq_total` | counter | stage, error_class | broken records |
| `etl_consumer_lag_records` | gauge | partition | high watermark minus committed offset |
| `etl_batch_duration_seconds` | histogram | — | transformer batch time (transaction, PUTs, mark) |
| `etl_e2e_latency_seconds` | histogram | — | from `fetched_at` to the processed object being written |

The publisher also exposes franz-go client metrics (`etl_publisher_*`, via kprom), and every service exposes the standard Go runtime and process metrics. Labels stay low-cardinality: no coin ids or offsets.

The Grafana dashboard shows records per stage, data freshness and end-to-end latency, duplicate share, rejects by reason, DLQ records, consumer lag, API status codes, export lag with S3 PUT results, and schema validations.

## Alerts

The rules are in `deploy/prometheus/alerts.yml` and have unit tests (`make alerts-test` runs `promtool`). Open <http://localhost:9090/alerts> to see their state.

| Alert | Fires when | Likely cause |
|---|---|---|
| `ETLDataStale` | no successful CoinGecko call on an endpoint for more than 2 minutes | rate limiting, network, CoinGecko outage, fetcher down |
| `ETLDeadLetterRecords` | any record reached `dlq/` in the last 10 minutes | a bug, a broken message, an integrity failure |
| `ETLSourceSchemaDrift` | more than 5% of CoinGecko responses fail the source schema over 15 minutes | CoinGecko changed its response format |
| `ETLExportLagHigh` | export lag above 5 minutes | a long Postgres transaction, S3 failures, exporter down |
| `ETLConsumerLagGrowing` | consumer lag has grown for 10 minutes | transformers too slow, stuck or crash-looping |
| `ETLFrequentRateLimiting` | 3 or more HTTP 429 responses in 15 minutes | another client on the same IP, or a too aggressive schedule |

Alertmanager is not part of the stack; in production the rules route to on-call through Alertmanager or Grafana alerting.

## Logs

Every service logs JSON through Go's `log/slog`, in UTC, to stdout (`docker compose logs -f fetcher`) and to `logs/<service>/etl.log`. The two transformer replicas share the `./logs` mount, so each writes its own file, `logs/<container id>/transformer/etl.log`, and no file ever has two writers. Lines carry `service`, `stage` and, where relevant, `event_id` and `batch`. They cover API success and failure, transformation errors and every object saved.

## Running the image with `docker run -v`

`docker compose` is the supported way to run the stack. The same can be done with plain `docker run`; the bind mounts below are what makes data survive container restarts.

```bash
docker build -t coingecko-etl:local .
docker network create etl
mkdir -p data logs .s3meta

# Postgres 18 keeps its data under /var/lib/postgresql (not .../data).
docker run -d --name postgres --network etl -v pgdata:/var/lib/postgresql \
  -e POSTGRES_USER=etl -e POSTGRES_PASSWORD=etl -e POSTGRES_DB=etl postgres:18.6

docker run -d --name redpanda --network etl -v redpanda:/var/lib/redpanda/data \
  -v "$PWD/deploy/redpanda/bootstrap.yaml:/etc/redpanda/.bootstrap.yaml:ro" \
  redpandadata/redpanda:v26.2.4 redpanda start --mode=dev-container --smp=1 --memory=1G \
  --kafka-addr=internal://0.0.0.0:9092 --advertise-kafka-addr=internal://redpanda:9092

# The lake: ./data holds the buckets as plain directories, ./.s3meta the object metadata.
docker run -d --name versitygw --network etl --user "$(id -u):$(id -g)" \
  -v "$PWD/data:/lake" -v "$PWD/.s3meta:/meta" \
  -e ROOT_ACCESS_KEY_ID=etl -e ROOT_SECRET_ACCESS_KEY=etlsecret123 \
  versity/versitygw:v1.8.0 --port :7070 --health /health posix --sidecar /meta /lake

ETL_ENV=(-e ETL_PG_DSN='postgres://etl:etl@postgres:5432/etl?sslmode=disable'
         -e ETL_BROKERS=redpanda:9092 -e ETL_S3_ENDPOINT=http://versitygw:7070)

# One-shot setup (waits up to 60 s for each dependency), then the services.
docker run --rm --network etl --user "$(id -u):$(id -g)" -v "$PWD/logs:/app/logs" "${ETL_ENV[@]}" coingecko-etl:local init
for svc in fetcher exporter publisher transformer; do
  docker run -d --name "$svc" --network etl --user "$(id -u):$(id -g)" \
    -v "$PWD/logs:/app/logs" "${ETL_ENV[@]}" coingecko-etl:local "$svc"
done
```

What persists where:

| Data | Location | Survives |
|---|---|---|
| raw, processed, rejected, dlq objects | `./data` (bind mount) | container removal, `docker compose down -v` |
| object metadata (sha256 used by the write-once check) | `./.s3meta` (bind mount) | as above; keep it together with `./data` |
| logs | `./logs` (bind mount) | as above |
| Postgres (raw calls, ledgers, dedup claims) | named volume `pgdata` | container removal; deleted by `down -v` |
| broker log and consumer offsets | named volume `redpanda` | container removal; deleted by `down -v` |

## Assumptions

The assignment leaves some points open. This is how they were read:

- **"Fetch every 30 seconds and store to Postgres"**: every call is stored, including failures and byte-identical cached responses. Raw storage happens before deduplication; the idempotency key is just a column there.
- **"Export to local storage as raw JSON"**: raw JSON Lines objects in the `raw` bucket, which is the local directory `./data/raw`. The application only talks S3, so the same code writes to Amazon S3 in production.
- **"Append-only"**: objects are write-once (`If-None-Match: *`); nothing is ever overwritten or deleted by the pipeline.
- **"Remove unnecessary fields"**: images, descriptions, links, sparklines, trade URLs and duplicated fields are dropped; everything analytical is kept.
- **"Timestamps in UTC ISO 8601"**: RFC 3339 with milliseconds and a `Z` suffix.
- **Duplicates** come in two kinds and both are removed from the processed zone: redelivered messages (same event) and true duplicates (the same snapshot polled twice, or the same record in two messages).
- **Questionable data** is kept with flags (`is_stale`, `is_inactive`, `is_anomaly`) instead of being dropped; only empty or unusable records are rejected.
- **"`logs/etl.log`"**: one file per service, `logs/<service>/etl.log`, because several containers cannot safely append to one file.
- **Scale**: the free CoinGecko tier caps the volume at a few calls per minute; the design does not depend on that (see scalability below).

## Productionization

### AWS mapping

| Local | AWS |
|---|---|
| versitygw over `./data` | Amazon S3 with raw and processed zones, lifecycle rules (raw to Glacier after N days), Parquet compaction with a Glue Data Catalog for Athena, or Apache Iceberg tables |
| Postgres container | Amazon RDS or Aurora PostgreSQL, Multi-AZ |
| Redpanda | Amazon MSK (the franz-go code is unchanged; MSK IAM auth is supported by the client) |
| compose services | ECS on Fargate or EKS; the fetcher scheduled by EventBridge Scheduler if polling moves to a job model |
| `.env` | AWS Secrets Manager (CoinGecko key, database credentials) and SSM Parameter Store |
| Prometheus + Grafana | Amazon Managed Service for Prometheus + Amazon Managed Grafana |
| stdout logs | CloudWatch Logs (or a Fluent Bit sidecar to OpenSearch) |

### Scalability

- **More partitions, finer keys.** Today one markets response is one message, keyed `coins_markets`, so markets data goes to one partition. Splitting responses into per-coin messages (100 keys) spreads the load over all partitions; the partition count can then grow beyond 6 and the transformer can run more replicas (up to one per partition).
- **Compaction to Parquet.** Small JSONL objects suit a demo; production adds a periodic compaction into partitioned Parquet (or Iceberg) for analytics.
- **Debezium CDC instead of the exporter.** Change data capture streams `raw.api_calls` to Kafka in commit order without a cursor table; the raw S3 objects would then be produced by a sink.
- **JVM alternative.** Kafka Streams (dedup state in RocksDB with `exactly_once_v2`) plus the Kafka Connect S3 Sink is the standard JVM way to build the same pipeline.
- **Schema registry.** With more producers and consumers, the embedded JSON Schemas move to a registry (AWS Glue Schema Registry or a Confluent-compatible one) with BACKWARD compatibility checks.

### Reliability

- Broker: replication factor 3, `min.insync.replicas=2`, producers with `acks=all` (already set) across three availability zones.
- Postgres: Multi-AZ, automated backups and PITR; partition retention managed by `pg_partman` (raw calls and dedup claims are partitioned by day).
- Object storage: S3 versioning and replication for the raw zone, which is the replay source.
- Source: a paid CoinGecko plan removes the 429 risk and allows higher frequency and more coins.
- Operations: alerts routed through Alertmanager to on-call; dashboards per stage; runbooks for DLQ triage and replay (republish raw objects after a schema fix).

## Repository layout and tests

```
cmd/etl/                main and subcommand wiring
internal/bootstrap/     etl init
internal/coingecko/     HTTP client, rate limiter, Retry-After
internal/fetcher/ exporter/ publisher/ transformer/
internal/transform/     pure transforms: parse, normalize, clean, flag
internal/dedup/         claims and decisions
internal/validation/    JSON Schema registry
internal/storage/pg/ storage/s3/ queue/
internal/obs/           slog logger, metrics, health server
schemas/                JSON Schemas S0–S3 and test fixtures (recorded CoinGecko responses)
migrations/             goose SQL migrations
deploy/                 Prometheus config and alerts, Grafana provisioning and dashboard, Redpanda bootstrap
docs/                   design spec and architecture decision records (docs/adr/)
```

```bash
make test-unit    # unit tests, no Docker
make test         # all tests with -race; integration tests start Postgres, Redpanda and versitygw containers
make lint         # gofmt + go vet (CI also runs golangci-lint)
make alerts-test  # promtool: Prometheus config and alert rule unit tests
```

Integration tests use testcontainers and skip themselves when Docker is unavailable. They cover crashes after the manifest transaction and after partial PUTs, a zombie consumer, publisher replays and out-of-order Postgres commits, and assert no loss and no duplicates in the processed zone. CI (GitHub Actions) runs lint, the full test suite with Docker, the deploy-config checks and the image build.

Architecture decisions are recorded in [`docs/adr/`](docs/adr/).

---

Data provided by [CoinGecko](https://www.coingecko.com/en/api).
````

- [ ] **Step 2: Write the ADRs**

`docs/adr/0001-object-storage-versitygw.md`:
````markdown
# ADR 0001: Object storage through versitygw over plain files

- Status: accepted
- Date: 2026-10-10

## Context

The assignment wants raw and processed data under `data/raw/` and `data/processed/`, append-only. We also want the pipeline code to use the same API it would use in production (Amazon S3), and we need write-once semantics: a retried write must never replace an object with different bytes and must be recognisable as a retry.

Options considered:

- **Local filesystem writes from Go.** Simple, but the code would not be the production code, and "create if absent" plus a checksum check would have to be built by hand on every writer.
- **MinIO.** The usual choice, but the repository was archived on 2026-04-24 and the official images can no longer be pulled anonymously.
- **rclone serve s3.** Serves a directory over S3, but silently ignores `If-None-Match`, so write-once cannot be enforced.
- **versitygw (posix backend).** An S3 gateway that maps buckets to directories and objects to plain files. A spike confirmed `If-None-Match: *` (412 on an existing key, exactly one winner among 20 parallel writers), user metadata, ListObjectsV2 and multipart uploads with aws-sdk-go-v2.

## Decision

Run `versity/versitygw:v1.8.0` with the posix backend. The gateway root `/lake` is the bind mount `./data`; the buckets `raw`, `processed`, `rejected` and `dlq` are its top-level directories. Metadata is stored with `--sidecar /meta` in `./.s3meta`, because Docker Desktop bind mounts on macOS do not support extended attributes (the default xattr mode refuses to start). All code talks to it through aws-sdk-go-v2 with path-style addressing. Every write is `PutObject` with `If-None-Match: *` and a `sha256` user-metadata entry; on 412 the writer reads the stored checksum and treats equal bytes as success.

## Consequences

- Reviewers can `cat` every object; the same code targets Amazon S3 by changing the endpoint.
- Files placed in `./data` by hand have no stored ETag, and versitygw then lets `If-None-Match: *` overwrite them. The lake must only be written through the gateway; the README says so.
- `./data` and `./.s3meta` belong together: deleting only the metadata makes existing objects look foreign. Buckets are created through the gateway by `etl init` so that they get metadata; `make up` creates only `./data`, never the bucket directories.
- Sidecar metadata is marked experimental upstream; the integration tests run against the same image and mode.
````

`docs/adr/0002-single-dedup-in-transformer.md`:
````markdown
# ADR 0002: One deduplication mechanism, in the transformer

- Status: accepted
- Date: 2026-10-10

## Context

Duplicates enter the pipeline in two ways:

- **Redelivery**: every hop is at-least-once, so the same event can reach the transformer more than once (publisher restarts, consumer rebalances, crashes before an offset commit).
- **True duplicates**: CoinGecko's CDN caches responses for 60 s and we poll every 30 s, so about half of the markets responses are byte-identical; a stale coin also appears unchanged in consecutive snapshots.

The assignment requires raw data in Postgres before deduplication. Deduplicating at several stages (fetcher, exporter, publisher, transformer) would mean several rules to keep consistent and several places where a crash can lose data.

An in-memory "seen keys" cache was tried in a spike and lost its state on rebalance: two records were written twice after a partition moved.

## Decision

Deduplicate exactly once, in the transformer, on the natural key of each processed record (for example `coin_id | snapshot_at` for `market_snapshot`). Each record claims its key in `pipeline.dedup_claims` with `INSERT … ON CONFLICT DO NOTHING` inside the batch transaction and reads the claim back:

- the claim points to this record's own position (topic, partition, offset, ordinal) → write it;
- another position, same content hash (business fields only) → drop as a duplicate;
- another position, different content hash → write to `rejected/` as `snapshot_conflict`.

The fetcher's `idempotency_key` is an informational column used for SQL analysis and the drop-reason metric, not for decisions. Every other hop is made idempotent instead (primary keys, ledgers, write-once objects).

## Consequences

- Raw storage keeps everything, including duplicates, which makes the source behaviour observable.
- Because a claim remembers its position, reprocessing a batch after a crash reproduces the same decisions and the same bytes; the naive rule "drop if already claimed" would have lost the batch.
- The claim table grows with data volume; it is partitioned by `event_date` and kept longer than topic retention (30 days vs 7) so replays within retention are still deduplicated.
- Every record costs one Postgres round trip inside a short transaction; at higher volume the claims can move to a key-value store with conditional writes (e.g. DynamoDB).
````

`docs/adr/0003-redpanda.md`:
````markdown
# ADR 0003: Kafka protocol, with Redpanda as the local broker

- Status: accepted
- Date: 2026-10-10

## Context

Between the raw zone and the transformer we want a durable, replayable log with per-key ordering and consumer groups: replay after a schema fix, sequential dedup decisions per key, and replicas that share and take over partitions. The stack must run in docker compose on a laptop.

Candidates: Apache Kafka 4.3 (KRaft), Redpanda, NATS JetStream, Apache Pulsar, RabbitMQ (classic queues or streams), Redis Streams, and cloud queues (SQS, Pub/Sub).

## Decision

Use the Kafka protocol with the franz-go client (`kgo`, `kadm`, `kprom`), and run Redpanda `v26.2.4` in dev-container mode (`--smp 1 --memory 1G`) locally. One topic, `coingecko.raw`, 6 partitions, 7 days retention, created by `etl init`; broker-side topic auto-creation is off. Messages are keyed by `coins_markets` or the coin id.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen locally: Kafka protocol in one C++ binary, no JVM, starts in ~2 s, ~200 MiB |
| Apache Kafka, Amazon MSK, Confluent Cloud | Production targets; drop-in because the code speaks the protocol |
| NATS JetStream, Apache Pulsar | Viable with caveats: different consumer model or heavier operations; weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, SQS, Pub/Sub | Rejected: no offset replay or per-key ordering at scale |

## Consequences

- The same client code was verified against both Apache Kafka 4.3.1 and Redpanda v26.2.4.
- Redpanda lacks the Kafka Connect ecosystem; the design does not use it (Debezium and the S3 sink are documented as production alternatives).
- Redpanda is licensed under BSL 1.1, which permits this use; production would use MSK.
- Broker transactions (exactly-once) are not used: the S3 sink is outside Kafka anyway, and idempotent writes plus the dedup claims give exactly-once content in the processed zone.
````

`docs/adr/0004-postgres-pipeline-state.md`:
````markdown
# ADR 0004: Pipeline state in Postgres

- Status: accepted
- Date: 2026-10-10

## Context

Several pieces of state must survive crashes, be shared by replicas and change atomically with each other:

- which `raw.api_calls` rows have been exported, and in which object (exporter);
- which exported objects have been published, with leases so a crashed publisher's batch is picked up again (publisher);
- which offset ranges a transformer has planned and written, with the object keys and checksums of each batch (transformer manifest);
- which natural keys have been claimed, by which position, with which content hash (deduplication).

Alternatives: broker offsets only (cannot answer "was key X seen?"), S3 conditional writes only (guard single objects, not multi-object batches), in-memory state (lost on rebalance), Kafka Streams state stores (JVM-only) or goka (no transactions spanning state and output).

## Decision

Keep all pipeline state in the Postgres instance that already holds the raw zone, in schema `pipeline`:

- `pipeline.export_batches`: the exporter ledger (xid8 range, object key, checksum, status) and the publisher lease and `published_at` columns;
- `pipeline.consumer_batches`: the transformer manifest per (topic id, partition, first offset), with an exclusion constraint that forbids overlapping ranges;
- `pipeline.dedup_claims`: the dedup claims, partitioned by `event_date`.

Transactions are short and never span S3 or broker I/O. Postgres runs with `idle_in_transaction_session_timeout = 60s` and `transaction_timeout = 5min`.

## Consequences

- Batch planning, claims and object checksums commit atomically, which is what makes the crash table in the README hold.
- Postgres is on the critical path of every stage; in production it is RDS/Aurora Multi-AZ.
- The exporter uses a transaction-id (xid8) cursor rather than an identity watermark, which loses rows when transactions commit out of order.
- `dedup_claims` grows with data; daily partitions make retention a `DROP TABLE` (pg_partman in production).
````

- [ ] **Step 3: Write the golangci-lint configuration**

golangci-lint v2 config format (`version: "2"`). The standard linter set flags every unchecked `Close`, `Rollback` and `fmt.Fprint*`; these are the only findings in the codebase and none of them can be acted on, so they are excluded by function rather than with `//nolint` comments.

`.golangci.yml`:
```yaml
version: "2"

linters:
  # The "standard" set: errcheck, govet, ineffassign, staticcheck, unused.
  default: standard
  settings:
    errcheck:
      # Errors that cannot be acted on: closing read bodies, listeners and
      # files on cleanup, rolling back a transaction after Commit (a no-op),
      # and printing usage text to stdout/stderr.
      exclude-functions:
        - (io.Closer).Close
        - (net.Listener).Close
        - (*database/sql.DB).Close
        - (github.com/jackc/pgx/v5.Tx).Rollback
        - fmt.Fprint
        - fmt.Fprintf
        - fmt.Fprintln
        - io.Copy

formatters:
  enable:
    - gofmt
```

Run (optional locally): `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...`
Expected: `0 issues.`

- [ ] **Step 4: Write the CI workflow**

Four jobs: `lint` (gofmt, go vet, golangci-lint), `test` (`go test -race ./...`; GitHub-hosted Ubuntu runners have Docker, so the testcontainers integration tests run), `deploy-config` (compose config, promtool config check and alert unit tests, dashboard JSON), `image` (Buildx build without push, GitHub Actions cache).

`.github/workflows/ci.yml`:
```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

permissions:
  contents: read

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version: "1.27"
      - name: gofmt
        run: |
          unformatted="$(gofmt -l .)"
          if [ -n "$unformatted" ]; then echo "gofmt needed on:"; echo "$unformatted"; exit 1; fi
      - name: go vet
        run: go vet ./...
      - uses: golangci/golangci-lint-action@v9
        with:
          version: v2.14.0

  test:
    # GitHub-hosted Ubuntu runners ship a Docker daemon, so the
    # testcontainers-based integration tests (Postgres, Redpanda, versitygw)
    # run here instead of skipping.
    runs-on: ubuntu-24.04
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version: "1.27"
      - name: go test -race
        run: go test -race -count=1 -timeout 15m ./...

  deploy-config:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v7
      - name: docker compose config
        run: docker compose config -q
      - name: promtool check config and alert unit tests
        run: make alerts-test
      - name: dashboard is valid JSON
        run: jq empty deploy/grafana/dashboards/etl.json

  image:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v7
      - uses: docker/setup-buildx-action@v4
      - uses: docker/build-push-action@v7
        with:
          context: .
          push: false
          tags: coingecko-etl:ci
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

- [ ] **Step 5: Lint the workflow and check the docs**

Run:
```bash
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest -color=false .github/workflows/ci.yml && echo ACTIONLINT_OK
grep -c "Data provided by" README.md
```
Expected: `ACTIONLINT_OK`; `2` (attribution at the top and the bottom).

- [ ] **Step 6: Commit**

```bash
git add README.md docs/adr .golangci.yml .github/workflows/ci.yml
git commit -m "$(cat <<'EOF'
docs: README, architecture decision records and CI

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
EOF
)"
```
