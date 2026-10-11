# CoinGecko ETL — Design Spec

- **Date:** 2026-10-10
- **Status:** Approved (design review and written-spec review, 2026-10-10)
- **Context:** Trust Wallet Data Engineer take-home. Deliverable is a public GitHub repository.

## 1. Goals

### 1.1 Assignment requirements and how this design meets them

| Assignment item | Where it is satisfied |
|---|---|
| Extract from a public REST API | CoinGecko `/coins/markets` and `/coins/{id}` (§3) |
| Go package fetches every 30 s and stores to Postgres | `etl fetcher` → `raw.api_calls` (§4.1) |
| Go function exports data to local storage (raw JSON) | `etl exporter` → `data/raw/` via S3 API (§4.2) |
| Transform: drop unneeded fields, normalize, timestamps to UTC ISO 8601, separate output | `etl transformer` → `data/processed/` (§6) |
| Raw under `data/raw/`, processed under `data/processed/`, append-only | versitygw buckets are plain host directories; write-once objects with `If-None-Match: *` (§6.5, §7) |
| Logging (API success/failure, transformation errors, data saved) into `logs/etl.log` with Go's logging package | `log/slog` JSON to `logs/<service>/etl.log` and stdout (§8.3) |
| Health and Prometheus metrics endpoints; be ready to explain the metrics | `/healthz`, `/readyz`, `/metrics` on every service; metric catalogue in §8 |
| Dockerfile; container runs; steps to mount volumes for persistence | Multi-stage Dockerfile, docker compose, `docker run -v` steps in README (§9) |
| Detailed README: productionization, cloud services, scalability, reliability | README outline in §12 |

### 1.2 Additional requirements set by the candidate

- A schema exists for the data at every stage, and every stage validates against it.
- The fetcher attaches an idempotency key to each event. Raw storage (Postgres) receives data **before** deduplication; the key is just a column.
- The transformer runs a full cleaning pipeline: drop empty records, deduplicate, normalize, flag questionable data.
- Pipeline: API → Go fetcher → Postgres → Go exporter → raw JSONL → Go publisher → message broker (Kafka protocol; Redpanda) → Go transformer consumers → processed JSONL.
- Everything runs in docker compose. Object storage is accessed only through the S3 API, while the bytes on disk stay plain files.
- Both kinds of duplicates (redelivered messages and true duplicates) are removed from the processed zone.

### 1.3 Non-goals

- Analytics, a query layer or a warehouse (README production note only).
- Parquet output, compaction, schema registry (production notes).
- Broker-side exactly-once transactions (not needed; see §7).

## 2. Architecture

```
CoinGecko ──(markets every 30 s, /coins/{id} every 30 s: one coin per call, in turn)──▶ fetcher
                                                                  │ INSERT, one row per HTTP call
                                                                  ▼
                                          Postgres raw.api_calls (pre-dedup, includes failed calls)
                                                                  │ xid8 cursor + pipeline.export_batches
                                                                  ▼
                                                              exporter ──PUT If-None-Match:*──▶ s3://raw  (= ./data/raw)
                                                                  │
                                                              publisher (leases batches from the ledger, reads via S3)
                                                                  │ produce, key = endpoint / coin id
                                                                  ▼
                                          Redpanda (Kafka API) topic coingecko.raw (6 partitions)
                                                                  │ consumer group
                                                                  ▼
                     transformer ×2: validate → clean → dedup → normalize → validate
                        ├─▶ s3://processed = ./data/processed   (clean rows + is_stale / is_inactive / is_anomaly)
                        ├─▶ s3://rejected  = ./data/rejected    (empty records, with reason)
                        └─▶ s3://dlq       = ./data/dlq         (broken records; alerted)
```

### 2.1 Services

All Go services are subcommands of one binary `etl`, built into one image.

| Command | Role | Replicas |
|---|---|---|
| `etl init` | One-shot, idempotent: goose migrations and daily partitions in Postgres; topics via `kadm` against Redpanda (verifies config if they exist); S3 buckets | 1, runs to completion |
| `etl fetcher` | Pure transport: polls CoinGecko with a rate limiter, builds the event envelope and hash, inserts into `raw.api_calls`. No business logic, no source-schema validation | 1 |
| `etl exporter` | Every ~15 s claims a batch of new rows and writes one raw JSONL object | 1 (Postgres advisory lock) |
| `etl publisher` | Leases exported batches, reads the object via S3, publishes to the broker | 1 (leases allow more) |
| `etl transformer` | Consumer group; writes processed, rejected and dlq objects | 2 |
| `etl healthcheck` | Exit code for compose healthchecks (distroless has no curl) | — |

### 2.2 Infrastructure (pinned)

| Component | Image / version | Notes |
|---|---|---|
| Postgres | `postgres:18.6` | Volume at `/var/lib/postgresql` (18+ changed PGDATA; mounting `/var/lib/postgresql/data` makes the container refuse to start). Healthcheck over TCP: `pg_isready -h 127.0.0.1`. |
| Message broker | `redpandadata/redpanda:v26.2.4` | Kafka-API compatible, single C++ binary, no JVM. Dev container mode: `--mode dev-container --smp 1 --memory 1G`, auto-create off, healthcheck `rpk cluster health`. The franz-go code was verified unchanged against this version. Built-in schema registry and HTTP proxy stay unused. License: BSL 1.1 (free for this use). |
| Object storage | `versity/versitygw:v1.8.0` | posix backend, root `/lake` = `./data`, `--sidecar` metadata in `./.s3meta` (required on Docker Desktop for macOS: bind mounts have no xattrs) |
| Monitoring | Prometheus, Grafana | Provisioned scrape config, alert rules, dashboard |
| Broker UI | `redpandadata/console:v3.12.0` | ~50 MiB RAM |
| Go | 1.27.x | `github.com/twmb/franz-go` v1.22.1 (+ `kadm`, `kprom`), `github.com/jackc/pgx/v5` v5.11.0, `github.com/pressly/goose/v3` v3.28.0, `github.com/santhosh-tekuri/jsonschema/v6` v6.0.3, `github.com/aws/aws-sdk-go-v2`, `github.com/google/uuid` |

MinIO is not used: the repository was archived on 2026-04-24 and the official images can no longer be pulled. `rclone serve s3` was rejected because it silently ignores `If-None-Match`.

### 2.3 Why this chain and not a shorter one

- Fetcher → Kafka directly is simpler but drops the requirement that raw data lands in Postgres before deduplication.
- Debezium CDC (Postgres → Kafka in commit order) is the production choice but adds a JVM Kafka Connect stack.
- Kafka Streams would hold dedup state in RocksDB + changelog with `exactly_once_v2`, but it is JVM-only and its exactly-once covers Kafka → Kafka only (S3 would need Kafka Connect S3 Sink).

All three are documented in the README as production alternatives.

### 2.4 Why Redpanda

The design needs a replayable log, per-key ordering within partitions, and consumer groups. The Kafka protocol provides all three and is the de-facto standard at Trust Wallet's scale. Redpanda speaks the same protocol as a single C++ binary without a JVM: lighter, faster to start, and the same franz-go client code runs unchanged against Apache Kafka, MSK or Confluent Cloud in production. Its main gap, the Kafka Connect ecosystem, is not used here.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen for the stack |
| Apache Kafka, Amazon MSK, Confluent Cloud | Drop-in for production (same protocol) |
| NATS JetStream, Apache Pulsar | Viable with caveats: different consumer model / heavier operations, weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, SQS, Google Pub/Sub | Rejected: no offset replay or per-key ordering at scale |

### 2.5 Decisions taken in design review

| Topic | Decision |
|---|---|
| Object storage | versitygw with plain files; MinIO archived, rclone ignores `If-None-Match` |
| Raw table | One table `raw.api_calls`, one row per HTTP call incl. failures; entity splitting belongs to the transformer |
| Event semantics | An event is a source snapshot; a stale coin seen in two snapshots is two valid rows |
| Deduplication | One mechanism: natural record key in the transformer; `idempotency_key` is an informational column |
| Pipeline state | Postgres schema `pipeline`; Kafka Streams / goka rejected (JVM-only / no transactions) |
| Schema registry, Kafka Connect | Not used; README production notes |
| Bootstrapping | One idempotent `etl init` for Postgres migrations, topics and buckets, started after its dependencies are healthy |
| Logs | One file per replica: `logs/<service>/etl.log`, transformer replicas `logs/<container id>/transformer/etl.log`; + stdout |
| Monitoring | Prometheus + Grafana with alert rules and a provisioned dashboard |
| Cleaning | Empty records → `rejected/`; questionable records kept with flags |
| Fetcher scope | Transport only; source-schema (S0) validation lives in the transformer |
| Schema distribution | Schemas embedded via `go:embed`; updates ship as redeploys; no schema registry |
| Topic name | `coingecko.raw` without a version suffix: the version travels in every message (`schema_version`, `schema` header); breaking changes are handled in the same topic by dispatching on version |
| Dedup content hash | Business fields only; excludes schema version and technical columns |
| Exporter cursor | xid8 cursor below `pg_snapshot_xmin`: never skips, may stall; a pragmatic workaround for Postgres as the raw store (a transactional table is not a change log); production: raw events straight to a log or object storage, or Debezium CDC (ADR 0005) |
| Polling cadence | 4 calls/min, 15 s apart: markets at :00 and :30 (the assignment's "every 30 s"), one coin at :15 and :45 in turn, so each coin every 2 min; all four coins every minute (6 calls/min) and markets every 60 s (breaks "every 30 s") were rejected |

## 3. Source: CoinGecko

### 3.1 Endpoints

| Endpoint | Schedule | Purpose |
|---|---|---|
| `GET /api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&price_change_percentage=1h,24h,7d&sparkline=false&precision=full` | Every 30 s | Flat 30-second time series |
| `GET /api/v3/coins/{id}?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false` | Every 30 s, between the markets calls (at :15 and :45), one coin per call, in turn: `bitcoin → ethereum → tether → usd-coin → bitcoin …`, so each coin is refreshed every 2 min | Deep nesting: 23 maps × 63 currencies, 100 tickers with nested objects, platforms by chain |

### 3.2 Observed source behaviour (2026-10-10)

- CoinGecko recomputes all coins in one pass every 60 s and stamps them with the same `last_updated`. The CDN caches with `s-maxage=60`, so polling every 30 s yields ~50% byte-identical responses.
- Keyless limit is dynamic, 10–30 calls/min per IP; HTTP 429 with `Retry-After: 60` was observed on the 6th call within 27 s. Budget: 4 calls/min, at least 15 s apart (markets at :00 and :30, a coin at :15 and :45), one request in flight, honor `Retry-After`, exponential backoff with jitter on 5xx/timeouts, skip ticks rather than queueing them.
- Optional `COINGECKO_API_KEY` sends `x-cg-demo-api-key` (Demo plan: 100 calls/min, 10k calls/month ≈ 1.7 days at 4 calls/min).
- `community_data` and `developer_data` stopped being returned on 2026-08-28; the source schema must tolerate drift.
- Values exceed 2^53 (`market_data.market_cap.vnd`); "absent" is encoded as `null`, `""`, `[]` or a missing key; bitcoin has `platforms: {"": ""}`.
- README must include the "Data provided by CoinGecko" attribution.

## 4. Stages

### 4.1 Fetcher → `raw.api_calls`

One row per HTTP call, including failures. Columns mirror schema S1 (§5.1):

```sql
CREATE SCHEMA raw;
CREATE TABLE raw.api_calls (
  seq              bigint      GENERATED ALWAYS AS IDENTITY,
  event_id         uuid        NOT NULL,              -- UUIDv7 generated in Go
  idempotency_key  text        CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),  -- NOT unique; null for failed calls
  schema_version   smallint    NOT NULL,
  source           text        NOT NULL,              -- 'coingecko'
  endpoint         text        NOT NULL,              -- 'coins_markets' | 'coin_detail'
  request          jsonb       NOT NULL,              -- {path, params}
  fetched_at       timestamptz NOT NULL,              -- partition key, UTC
  outcome          text        NOT NULL CHECK (outcome IN ('ok','http_error','network_error')),
  http             jsonb,                             -- {status, latency_ms, date, age_s, cf_cache_status, etag, retry_after_s}
  error            text,
  payload          json,                              -- response body as stored by the fetcher (json, not jsonb)
  payload_sha256   bytea       CHECK (octet_length(payload_sha256) = 32),
  ingest_txid      xid8        NOT NULL DEFAULT pg_current_xact_id(),
  PRIMARY KEY (event_id, fetched_at),
  CHECK ((outcome = 'ok') = (payload IS NOT NULL))
) PARTITION BY RANGE (fetched_at);
CREATE INDEX ON raw.api_calls (ingest_txid, seq) WHERE outcome = 'ok';
CREATE INDEX ON raw.api_calls (idempotency_key);
```

- `payload` is the response body with surrounding JSON whitespace trimmed, byte for byte. A body that contains a line break is stored compacted instead (insignificant whitespace removed; string contents and number text verbatim), so one call stays one JSONL line and one message; `event.Marshal` refuses a payload with CR or LF. `payload_sha256` is computed over the stored bytes.
- `payload` is `json`, not `jsonb`: `json` keeps the exact bytes, so `payload_sha256` stays verifiable at every hop. SQL access works the same (`payload->>'id'`).
- `idempotency_key = sha256(source | endpoint | canonical params | payload_sha256)`. It is informational: SQL analysis of raw duplicates and a metric. Deduplication does not use it (§7.3).
- Failed calls (429, 5xx, timeouts) are inspected with SQL: `WHERE outcome <> 'ok'`. Source-schema violations are found in `dlq/` (validated in the transformer).
- Retries of the same insert are idempotent: `ON CONFLICT DO NOTHING` on the primary key.
- Daily partitions are created by `etl init` and a maintenance loop (today … today+3); no DEFAULT partition.

### 4.2 Exporter → `s3://raw`

- Single active instance via `pg_try_advisory_lock`.
- Why an xid8 cursor: The assignment places raw data in Postgres first. That gives a queryable record of every API call, failures included, before any deduplication. But a transactional table is not a change log: reading "new rows" from it safely needs either change data capture or a cursor that respects commit order. We chose a cursor over `(ingest_txid, seq)` below `pg_snapshot_xmin`: it never skips a row, at the cost of depending on Postgres internals and stalling behind a long transaction (bounded by timeouts and the export-lag alert). It is a pragmatic workaround rather than an elegant one. In production we would land raw events directly in a log or object storage, or stream the table with Debezium CDC.
- Cursor over `(ingest_txid, seq)` bounded by `pg_snapshot_xmin(pg_current_snapshot())`. An identity/BIGSERIAL watermark loses rows when transactions commit out of order (reproduced on 18.6); the xid8 bound can only stall, never skip.
- Rejected alternative: an `exported` flag on `raw.api_calls` with `SELECT … WHERE NOT exported … FOR UPDATE SKIP LOCKED LIMIT n`. It never loses a late-committing row and never stalls, but it UPDATEs every raw row (the raw table stops being append-only: dead tuples, vacuum, a partial index) and a crash between PUT and the flag update lets the retry pick a different row set, so a byte-identical retry still needs a claimed batch — the ledger this design already has, at one row per batch instead of one update per raw row.
- Steps: (1) short claim transaction inserts the range into `pipeline.export_batches` and derives the object key from the batch id; (2) read the range, serialize deterministic JSONL (each line is the S1 event, payload embedded verbatim), validate each line against S1; (3) PUT with `If-None-Match: *` and `x-amz-meta-sha256`; on 412, HEAD and compare the checksum; (4) mark `exported`.
- Never hold a DB transaction across S3 or broker I/O.
- Only `outcome = 'ok'` rows are exported.

### 4.3 Publisher → broker

- Leases one exported batch: `UPDATE … SET publish_lease_until = now() + '2 min' WHERE batch_id = (SELECT … FOR UPDATE SKIP LOCKED)`.
- Streams the object, validates each line against S1 (failures go to `dlq/`), produces each line verbatim.
- Topic `coingecko.raw`, 6 partitions, retention (TTL) 7 days: messages older than 7 days are deleted, while `s3://raw` and `raw.api_calls` keep the history, so the topic is a 7-day replay window. Key: `coins_markets` for markets, the coin id for detail. Headers: `event_id`, `idempotency_key`, `schema`, `raw_object`, `line_no`.
- Idempotent producer, `acks=all`. Marks `published_at` after a successful flush. A crash before the mark republishes the object; the transformer drops the copies.

### 4.4 Transformer → processed / rejected / dlq

See §7 for the batch algorithm.

## 5. Schemas and validation

### 5.1 Schema set

JSON Schema draft 2020-12 files in `schemas/`, embedded with `go:embed`, compiled once at startup (a compile failure exits non-zero), validated with `santhosh-tekuri/jsonschema/v6`. The major version is in the file name and `$id`, in `schema_version` on every record, and in the `v=` path segment.

| Schema | Describes | Policy |
|---|---|---|
| S0 `source/coingecko/coins_markets.v1.json`, `source/coingecko/coin.v1.json` | CoinGecko responses | Permissive: require and type only the fields we use; extra fields allowed; null allowed where observed; dynamic maps via `additionalProperties`; never `propertyNames` (upstream issue #276) |
| S1 `pipeline/raw_event.v1.json` | One API call: the `api_calls` row = the raw JSONL line = the message value, byte-for-byte | Strict (`additionalProperties: false` except `payload`) |
| S2 `processed/<dataset>.v1.json` | Processed records (§6) | Strict |
| S3 `pipeline/rejected_record.v1.json`, `pipeline/dlq_record.v1.json` | Rejected and broken records | Strict |
| `common/defs.v1.json` | `utc_timestamp`, `decimal_string`, `sha256_hex`, `uuid` | — |

### 5.2 Where validation happens

| Stage | Schema | On failure |
|---|---|---|
| fetcher, before INSERT | S1 (envelope only; the payload is not inspected) | Reject (our bug): ERROR log and `etl_schema_validations_total{stage="fetch",result="fail"}`; no dedicated alert, a persistent failure trips the stale-data alert |
| exporter, publisher | S1 | Write to `dlq/`, do not pass on |
| transformer, input | S1, then the payload against S0 | `dlq/` (`schema_violation` / `source_schema_violation`). After a schema fix, the raw objects are republished and reprocessed |
| transformer, output | S2 | `dlq/`, never written to processed |
| writing rejected / dlq | S3 | ERROR log with the original bytes in base64 |

Schemas are embedded in the binary, so producers and consumers always carry the schema version of their build; a schema change ships as a redeploy. Rollout order for a compatible change: consumers (transformer) first, then producers (publisher, exporter, fetcher). CI checks that a new schema version still accepts every fixture of the previous one. A breaking change gets a new major version (`v2`) handled side by side.

Cleaning-rule drops are not schema failures: they go to `rejected/`, not `dlq/`. `dlq/` means "something broke, a human is needed".

### 5.3 Go types

Go structs are hand-written. Contract tests keep them in sync: every schema compiles offline; valid/invalid fixtures behave as expected; struct ↔ schema drift test; recorded live CoinGecko responses pass S0; fuzzed structs marshal to valid documents. Code generation is not used (go-jsonschema generates a date-time type that marshals to `{}`).

## 6. Data model

### 6.1 Time model

| Field | Source | Meaning |
|---|---|---|
| `snapshot_at` | Derived: max `last_updated` in a markets response; the document's `last_updated` for `/coins/{id}` | When CoinGecko assembled the snapshot. Event time and partition key. |
| `source_updated_at` | API `last_updated` of the coin | When CoinGecko last recomputed this coin. Lags `snapshot_at` by hours for stale coins. |
| `fetched_at` | Ours | When we polled. Lineage. |
| `tickers[].last_traded_at`, `last_fetch_at` | API | Exchange trade time; when CoinGecko fetched exchange data |

An event is a source snapshot. A stale coin observed in two different snapshots yields two valid rows (both `is_stale = true`); two polls served the same cached response yield one. Clocks of different systems are not synchronized (a trade at 10:00:45 appeared after CoinGecko's fetch at 10:00:40), so no logic depends on their ordering.

### 6.2 Processed datasets

| Dataset | Source | Natural key | Rows per snapshot |
|---|---|---|---|
| `market_snapshot` | `/coins/markets` | coin_id, snapshot_at | 100 |
| `coin_snapshot` (parent) | `/coins/{id}` scalars: rank, supply, categories, genesis_date, … | coin_id, snapshot_at | 1 |
| `coin_market_by_currency` | 23 `market_data` maps zipped on the currency key: price, market cap, FDV, volume, high/low 24h, ATH/ATL (+ dates), changes 1h…1y | coin_id, currency, snapshot_at | 63 |
| `ticker` | `tickers[]`; `market` → `exchange_id`, `exchange_name`; `converted_last/volume` → `_usd`, `_btc`, `_eth` columns | coin_id, exchange_id, base, target, snapshot_at | 100 |
| `coin_contract` | `platforms` + `detail_platforms` zipped on the platform key | coin_id, platform_id, snapshot_at | up to 36 |

Child datasets reference the parent by `(coin_id, snapshot_at)`. Dropped as unnecessary: images, descriptions, links, sparkline, `trade_url`, `token_info_url`, duplicated fields (`coin_mcap_usd`).

Every row carries technical columns `_event_id`, `_raw_object`, `_kafka_partition`, `_kafka_offset`, `_schema_version`, `_processed_at` (taken from the batch manifest, never `now()`).

### 6.3 Formats

- Timestamps: RFC 3339 UTC with milliseconds (`2026-10-10T10:07:55.000Z`); `genesis_date` as `YYYY-MM-DD`.
- Money and quantities (prices, caps, volumes, supplies): decimal strings (`"43083803721443304"`), decoded with `json.Number` and a decimal type; never float64. Percentages and ratios: JSON numbers.
- Symbols upper-case; EVM addresses lower-case.
- Files: plain `.jsonl` (no gzip) so reviewers can `cat` them.

### 6.4 Cleaning rules

- **Normalize:** `""` and `[]` → `null`; timestamps and numbers as in §6.3; `roi` (null in 223 of 250 rows) flattened to `roi_times`, `roi_currency`, `roi_pct`.
- **Reject to `rejected/` with reason:** `missing_id`, `null_price`, `nonpositive_price`, `empty_platform`, `empty_record` (all metrics null), `snapshot_conflict` (§7.3).
- **Flag and keep:**
  - `is_stale`: `snapshot_at − source_updated_at > 10 min`; for tickers, the upstream flag or last trade older than 1 h.
  - `is_inactive` (`market_snapshot`, `coin_market_by_currency`): zero volume.
  - `is_anomaly`: price outside `[low_24h, high_24h]`; for tickers, the upstream flag or `last_usd` more than 2% from the coin's median across tickers in the snapshot. For `market_snapshot` the range is widened by `ETL_ANOMALY_TOLERANCE_PCT` percent on each side (default 0.5; 0 = strict): `price < low_24h × (1 − t/100)` or `price > high_24h × (1 + t/100)`, in decimal, because `precision=full` prices carry more digits than the rounded 24 h bounds. `coin_market_by_currency` keeps the strict range (`/coins/{id}` sends price and bounds at one precision).
- Thresholds are configuration.

### 6.5 Object layout

```
data/raw/source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl
data/processed/dataset=ticker/v=1/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl
data/rejected/dataset=coin_contract/event_date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
data/dlq/stage=transform/date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
```

`event_date` is the date of `snapshot_at`. Processed/rejected/dlq object names are topic + partition + first offset of the batch (the Confluent S3 sink convention), which makes writes idempotent.

## 7. Delivery guarantees

### 7.1 Principle

Every hop is at-least-once. Exactly-once content in the processed zone comes from making every step safely repeatable: deterministic keys, ledgers in Postgres, and write-once objects. **Deduplication happens once (in the transformer); idempotency happens at every write.**

| Hop | Mechanism | On retry |
|---|---|---|
| fetcher → `api_calls` | PK `event_id`, `ON CONFLICT DO NOTHING` | At most one missed poll (inherent to polling; metric) |
| exporter → `raw/` | xid8 cursor + `export_batches`; `If-None-Match: *` | Same object: 412 + equal checksum |
| publisher → broker | Lease from the ledger; idempotent producer, `acks=all` | Copies with the same `event_id`; transformer drops them |
| transformer → `processed/` | Batch manifest; write-once objects; offset commit after write | Same objects: 412 + equal checksum |

### 7.2 Pipeline-state tables (schema `pipeline`)

```sql
CREATE SCHEMA pipeline;

CREATE TABLE pipeline.export_batches (
  batch_id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  from_txid xid8 NOT NULL, from_seq bigint NOT NULL,      -- exclusive lower bound
  to_txid   xid8 NOT NULL, to_seq   bigint NOT NULL,      -- inclusive upper bound
  watermark_xmin      xid8 NOT NULL,
  row_count           int  NOT NULL CHECK (row_count > 0),
  object_key          text NOT NULL UNIQUE,
  content_sha256      bytea,
  status              text NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','exported')),
  claimed_at          timestamptz NOT NULL DEFAULT now(),
  exported_at         timestamptz,
  publish_lease_until timestamptz,
  published_at        timestamptz,
  UNIQUE (from_txid, from_seq),
  CHECK ((to_txid, to_seq) > (from_txid, from_seq))
);

CREATE TABLE pipeline.dedup_claims (
  dataset         text   NOT NULL,
  record_key      text   NOT NULL,          -- natural key, e.g. 'bitcoin|2026-10-10T10:07:55.000Z'
  event_date      date   NOT NULL,          -- date of snapshot_at; partition key
  topic_id        uuid   NOT NULL,          -- topic id from metadata (topic incarnation); falls back to a configured pipeline epoch if the broker returns none
  kafka_partition int    NOT NULL,
  kafka_offset    bigint NOT NULL,
  ordinal         int    NOT NULL,          -- record index within the message
  event_id        uuid   NOT NULL,
  idempotency_key text,                     -- of the claiming message; only for the drop-reason metric
  content_sha256  bytea  NOT NULL,
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset, record_key, event_date)
) PARTITION BY RANGE (event_date);

CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE pipeline.consumer_batches (
  topic_id        uuid   NOT NULL,
  kafka_partition int    NOT NULL,
  first_offset    bigint NOT NULL,
  last_offset     bigint NOT NULL CHECK (last_offset >= first_offset),
  object_keys     text[],
  content_sha256  bytea[],
  counts          jsonb,
  status          text NOT NULL DEFAULT 'planned' CHECK (status IN ('planned','written')),
  planned_at      timestamptz NOT NULL DEFAULT now(),
  written_at      timestamptz,
  PRIMARY KEY (topic_id, kafka_partition, first_offset),
  EXCLUDE USING gist (topic_id WITH =, kafka_partition WITH =,
                      int8range(first_offset, last_offset, '[]') WITH &&)
);
```

Postgres holds this state because it must survive crashes, be shared by all transformer replicas, and change atomically. Broker offsets cannot answer "was key X seen", S3 can only guard single objects, and in-memory state is lost on rebalance (reproduced: 2 records rewritten after a rebalance).

### 7.3 Deduplication rules (single mechanism)

Every record bound for `processed/` or `rejected/` claims its natural key (§6.2) with `INSERT … ON CONFLICT DO NOTHING`; the first claimant wins.

| Claim for the record key | Action |
|---|---|
| Points to this record's own position (partition, offset, ordinal) | Write it (first sight or redelivery after a crash) |
| Other position, same content hash | Drop as duplicate |
| Other position, different content hash | Write to `rejected/` with `snapshot_conflict` |

The content hash (`content_sha256`) is computed over the record's business fields only, in canonical form. It excludes `_schema_version` and every technical column (`_event_id`, `_raw_object`, `_kafka_partition`, `_kafka_offset`, `_processed_at`) and `fetched_at`; otherwise identical records arriving in different messages would never match.

Drop reasons for metrics only (no separate logic): same `event_id` → `replay`; same `idempotency_key` → `duplicate_response`; otherwise `duplicate_record`. `dlq/` records are not deduplicated.

### 7.4 Transformer batch algorithm

franz-go: `DisableAutoCommit`, `BlockRebalanceOnPoll`, cooperative-sticky balancer, `FetchMinBytes` 1 MB / `FetchMaxWait` 5 s, max 500 records. At most one in-flight batch per partition. A batch is a contiguous offset range `[F..L]` of one partition.

1. **Input checks (no DB):** valid JSON (no duplicate keys, valid UTF-8) else `dlq` `decode_error`; S1 else `dlq` `schema_violation`; `sha256(payload) == payload_sha256` else `dlq` `integrity_error`; payload against S0 else `dlq` `source_schema_violation`.
2. **TX1 (milliseconds, no network I/O):**
   a. Insert the range into `consumer_batches` (`planned`). The EXCLUDE constraint forbids overlap; if a row with this start exists, use its stored range instead of the polled one.
   b. Transform passing messages into dataset records (cleaning rules §6.4).
   c. Validate the records bound for `processed/` against S2 (failures → `dlq`, without a claim, so a republish after a schema fix still writes them).
   d. Insert claims for the records bound for `processed/` or `rejected/`; read them back; decide per record (§7.3).
   e. Build objects per (zone, dataset, event_date), serialized deterministically (records by offset then ordinal, fixed field order, `_processed_at = planned_at`). Store object keys, checksums and counts on the manifest row; if stored checksums from a previous attempt differ, alert and stop (non-determinism).
   COMMIT.
3. **PUT** each object with `If-None-Match: *` and `x-amz-meta-sha256`. 200 → done; 412 → HEAD and compare checksum (equal → done, different → alert and stop); 5xx / network → retry with backoff, never commit the offset.
4. **TX2:** mark the batch `written`.
5. **Commit offset `L+1` synchronously**, then allow rebalance. A rejected commit means the partition moved; stop work on it.

**Alert and stop** (a different checksum on PUT in step 3, or a non-deterministic re-plan in step 2e) halts the affected partition in place: the transformer pauses fetching it, does not commit, keeps its `etl_consumer_lag_records` at the high watermark minus the first uncommitted offset (refreshed from the topic's end offsets while the partition is paused), keeps processing its other partitions and serving `/metrics`, and counts `etl_s3_put_total{result="mismatch"}`, which fires the critical alert `ETLObjectChecksumMismatch`. The process does not exit (the consumer loop returns only when its context is cancelled), so the counter and the alert stay up until a human resolves the conflict and restarts the transformer. A halted partition that moves to another replica halts there too.

**On partition assignment:** find the manifest batch covering the committed offset. `written` → seek past it and commit; `planned` → reprocess exactly that range; none → normal processing.

### 7.5 Crash analysis

| Crash point | Recovery | Loss | Duplicates |
|---|---|---|---|
| Before TX1 commit | Batch starts over with any boundaries | No | No |
| After TX1, before PUT | Manifest `planned` → exact range again; claims point to own positions → same decisions, same bytes; PUT 200 | No | No |
| After some PUTs | Remaining PUTs 200; written ones 412 + equal checksum | No | No |
| After TX2, before offset commit | Manifest `written` → seek to `L+1`, commit | No | No |
| Zombie consumer and new owner on the same range | Same range from the manifest, identical bytes; first PUT wins, second gets 412 with equal checksum; zombie's offset commit is rejected | No | No |

The naive rule "drop if the key was already claimed" loses every record of a batch that crashed between TX1 and the PUT.

### 7.6 Partitions

Partitions are the unit of ordering and parallelism, not endpoint separation. Keys put every repeat of the same data on one partition, so dedup decisions are made sequentially and deterministically. With 6 partitions: `coins_markets` and `bitcoin` → 0, `tether` → 1, `ethereum` → 2, `usd-coin` → 4 (the collision is harmless). Five keys land on four partitions, so at most 4 consumers can be busy; we run 2. Scaling path: split markets responses into per-coin messages (100 keys).

### 7.7 Invariants (README)

- Transforms are pure and deterministic: no `now()`, no random order.
- `dedup_claims` and `consumer_batches` outlive topic retention (30 days vs 7).
- A long Postgres transaction stalls the exporter without losing data: alert on `etl_export_lag_seconds`; `idle_in_transaction_session_timeout = 60s`, `transaction_timeout = 5min`.

## 8. Observability

### 8.1 Endpoints

Every long-running service listens on `:8080`: `/healthz` (process alive), `/readyz` (its dependencies reachable), `/metrics` (Prometheus).

### 8.2 Metrics (prefix `etl_`)

| Service | Metrics |
|---|---|
| fetcher | `api_requests_total{endpoint,status}`, `api_request_duration_seconds`, `api_rate_limited_total`, `last_success_timestamp_seconds{endpoint}`, `source_data_age_seconds` (fetched_at − last_updated) |
| all | `schema_validations_total{stage,schema,result}`, `schema_violations_total{stage,schema,keyword}` |
| exporter / publisher | `export_lag_seconds`, `export_batches_total{result}`, `s3_put_total{result}` (412 counted separately), `published_records_total`, kprom client metrics |
| transformer | `records_total{dataset,outcome}` (written, duplicate, rejected, dlq), `dedup_dropped_total{reason}`, `rejected_total{reason}`, `dlq_total{stage,error_class}`, `consumer_lag_records{partition}`, `batch_duration_seconds`, `e2e_latency_seconds` |

Labels stay low-cardinality.

### 8.3 Logs

`log/slog` JSON handler, UTC time, fields `service`, `stage`, `event_id`, `batch`. Written to stdout and to one file per replica: `logs/<service>/etl.log`, and `logs/<container id>/transformer/etl.log` for each transformer replica (one writer per file; no shared file across containers). Covers API success/failure, transformation errors, and data saved.

### 8.4 Alerts (Prometheus rules)

- Data stale: no successful poll for more than 2 minutes.
- `dlq` records appeared.
- More than 5% of responses fail S0 over 15 minutes (upstream format change).
- Export lag above 5 minutes, or no export lag reported by the exporter (only the exporter's series counts; the other services export the gauge at 0).
- Consumer lag growing for 10 minutes.
- Frequent 429 responses.
- A pipeline service is down for 2 minutes: its scrape fails (`up == 0`) or it has no scrape target at all (critical).
- The pipeline does not progress: CoinGecko calls succeed but no transformer record is counted over 15 minutes (a stalled exporter, publisher, broker or transformer).
- An object key already holds different bytes, or a re-planned batch differs from its manifest (`etl_s3_put_total{result="mismatch"}` above 0; critical, fires at once and stays up while a transformer partition is halted, §7.4).

### 8.5 Grafana dashboard (provisioned)

Records per stage, data freshness and end-to-end latency, duplicate share, rejects by reason, DLQ, consumer lag, API status codes.

## 9. Deployment

### 9.1 Compose

- Infrastructure: postgres, redpanda, versitygw, prometheus, grafana, redpanda-console.
- `etl init` depends on postgres, redpanda and versitygw being healthy (`service_healthy`); all `etl` services depend on `init` completing successfully (`service_completed_successfully`).
- Services: fetcher, exporter, publisher, transformer (2 replicas).
- Persistence: bind mounts `./data`, `./logs`, `./.s3meta`; named volumes for Postgres and Redpanda. README also documents `docker run -v` equivalents.
- `.env`: optional `COINGECKO_API_KEY`; ports; thresholds.
- `make up`, `make down`, `make demo` (start, wait, print counts in raw/processed/rejected, duplicate share, Grafana and Console links).
- Estimated footprint: ~12 containers (1 one-shot), ~1.2 GB RAM.

### 9.2 Image

Multi-stage Dockerfile: `golang:1.27` builder, `CGO_ENABLED=0` static binary, runtime `gcr.io/distroless/static-debian13:nonroot`. Healthchecks use `etl healthcheck`.

## 10. Repository layout

```
cmd/etl/                 main, subcommands
internal/
  coingecko/             HTTP client, rate limiter, Retry-After
  fetcher/ exporter/ publisher/ transformer/
  transform/             pure functions: parse, normalize, clean
  dedup/                 claims and decisions
  schemas/ validation/   JSON Schema registry and validators
  storage/s3/ storage/pg/ queue/kafka/
  obs/                   slog, metrics, health
schemas/                 S0–S3 JSON Schemas, testdata fixtures
migrations/              goose SQL
deploy/                  prometheus (rules), grafana (dashboard), versitygw
docs/                    spec, ADRs, architecture diagram
Makefile, docker-compose.yml, Dockerfile, README.md
```

## 11. Testing

1. **Unit:** table-driven tests of `transform` on recorded live CoinGecko responses — normalization, flags, rejects, time and number formatting.
2. **Schema contracts:** compile, fixture expectations, struct ↔ schema drift, live samples pass S0, fuzzing.
3. **Delivery guarantees (integration, testcontainers: Postgres, Redpanda, versitygw):** crash after TX1, after partial PUTs, zombie consumer, publisher replay; assert no loss and no duplicates in processed.
4. **Exporter:** out-of-order commits never skip rows.
5. `go test -race`; GitHub Actions CI: lint, tests, image build.

## 12. README outline

Overview and diagram; why the Kafka protocol and why Redpanda (replayable log, per-key ordering, consumer groups, DE ecosystem; Redpanda chosen for the local stack: same protocol and client code, no JVM, lighter and faster to start; Apache Kafka / MSK / Confluent Cloud are drop-in for production since the code speaks the Kafka protocol; Kafka Connect ecosystem is the main thing Redpanda lacks and we do not use it; NATS JetStream and Pulsar viable with caveats; RabbitMQ classic queues, Redis Streams, SQS, Pub/Sub rejected: no offset replay or per-key ordering); quick start (`make up`, `make demo`); how to browse the lake (plain files, S3 CLI, Console, Grafana); data model and time model; schemas and validation; delivery guarantees and crash table; metrics and alerts explained; assumptions (assignment ambiguities and our interpretation); productionization:
- AWS: S3 (raw/processed zones, lifecycle, Parquet + Glue catalog / Athena or Iceberg), RDS/Aurora Postgres, MSK, ECS/EKS, EventBridge Scheduler, Secrets Manager, Managed Prometheus + Grafana, CloudWatch.
- Scalability: per-coin message keys and more partitions, more transformer replicas, compaction to Parquet, Debezium CDC instead of the exporter, Kafka Streams + Kafka Connect S3 Sink as a JVM alternative, schema registry with BACKWARD compatibility.
- Reliability: replication factor 3, `min.insync.replicas=2`, pg_partman retention, paid CoinGecko plan.

CoinGecko attribution.
