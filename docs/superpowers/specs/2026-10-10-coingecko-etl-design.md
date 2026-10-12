# CoinGecko ETL — Design Spec

- **Date:** 2026-10-10
- **Revised:** 2026-10-12 (windowed export and publish, SKIP LOCKED claim, processing-time object names, maintenance jobs)
- **Status:** Approved (design review and written-spec review, 2026-10-10)
- **Context:** Trust Wallet Data Engineer take-home. Deliverable is a public GitHub repository.

## 1. Goals

### 1.1 Assignment requirements and how this design meets them

| Assignment item | Where it is satisfied |
|---|---|
| Extract from a public REST API | CoinGecko `/coins/markets` and `/coins/{id}` (§3) |
| Go package fetches every 30 s and stores to Postgres | `etl fetcher` → `raw.api_calls` (§4.1) |
| Go function exports data to local storage (raw JSON) | `etl exporter` → `data/raw/` via S3 API, in `ETL_WINDOW` windows closed by a `_SUCCESS` manifest (§4.2) |
| Transform: drop unneeded fields, normalize, timestamps to UTC ISO 8601, separate output | `etl transformer` → `data/processed/` (§6) |
| Raw under `data/raw/`, processed under `data/processed/`, append-only | versitygw buckets are plain host directories; write-once objects with `If-None-Match: *` (§6.5, §7) |
| Logging (API success/failure, transformation errors, data saved) into `logs/etl.log` with Go's logging package | `log/slog` JSON to `logs/<service>/etl.log` and stdout; the scheduled jobs append to `logs/publisher/etl.log` and `logs/maintenance/etl.log` (§8.3) |
| Health and Prometheus metrics endpoints; be ready to explain the metrics | `/healthz`, `/readyz`, `/metrics` on every long-running service; the scheduled jobs push their results to a Pushgateway; metric catalogue in §8 |
| Dockerfile; container runs; steps to mount volumes for persistence | Multi-stage Dockerfile, docker compose, `docker run -v` steps in README (§9) |
| Detailed README: productionization, cloud services, scalability, reliability | README outline in §12 |

### 1.2 Additional requirements set by the candidate

- A schema exists for the data at every stage, and every stage validates against it.
- The fetcher attaches an idempotency key to each event. Raw storage (Postgres) receives data **before** deduplication; the key is a column.
- The transformer runs a full cleaning pipeline: drop empty records, deduplicate, normalize, flag questionable data.
- Pipeline: API → Go fetcher → Postgres → Go exporter → raw JSONL windows → Go publisher job (one run per closed window) → message broker (Kafka protocol; Redpanda) → Go transformer consumers → processed JSONL.
- Stages meet only through the artifacts they produce: `raw.api_calls`, raw windows with `_SUCCESS`, topic messages and processed objects.
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
                                         │ claim rows (FOR UPDATE SKIP LOCKED) + pipeline.export_windows / export_batches
                                         ▼
                                     exporter ──window parts, then _SUCCESS (PUT If-None-Match: *)──▶ s3://raw (= ./data/raw)
                                                                                                         │ parts listed in _SUCCESS
  Cronicle ──every window + 1 min──▶ etl publish --now (job, no Postgres) ◀──────────────────────────────┘
     │                                   │ produce, key = idempotency_key
     │                                   ▼
     │              Redpanda (Kafka API) topic coingecko.raw (6 partitions)
     │                                   │ consumer group
     │                                   ▼
     │         transformer ×2: validate input → transform, clean → validate output → dedup
     │            ├─▶ s3://processed = ./data/processed   (clean rows + is_stale / is_inactive / is_anomaly)
     │            ├─▶ s3://rejected  = ./data/rejected    (empty records, with reason)
     │            └─▶ s3://dlq       = ./data/dlq         (broken records; alerted)
     │
     └──etl maintenance retention / vacuum, daily──▶ Postgres (drop expired claim partitions, VACUUM)

Every scheduled job pushes its result to the Pushgateway; Prometheus scrapes it with the long-running services.
```

### 2.1 Services and jobs

All Go code is one binary `etl`, built into one image; each service and job is a subcommand.

Services (docker compose):

| Command | Role | Replicas |
|---|---|---|
| `etl init` | One-shot, idempotent: goose migrations and daily partitions in Postgres; topics via `kadm` against Redpanda (verifies config if they exist); S3 buckets | 1, runs to completion |
| `etl fetcher` | Pure transport: polls CoinGecko with a rate limiter, builds the event envelope and hash, inserts into `raw.api_calls`. No business logic, no source-schema validation | 1 |
| `etl exporter` | Closes each `ETL_WINDOW` window: claims the window's unexported rows, writes the parts and then `_SUCCESS` (§4.2) | 1 (Postgres advisory lock) |
| `etl transformer` | Consumer group; writes processed, rejected and dlq objects | 2 |
| `etl healthcheck` | Exit code for compose healthchecks (distroless has no curl) | — |

Jobs run by Cronicle (§9.3), each a short process with exit code 0 (success), 1 (failure) or 2 (usage error):

| Command | Role | Schedule |
|---|---|---|
| `etl publish --now <unix seconds or RFC 3339>` | Publishes one closed window from S3 to the broker; no Postgres; fails when the window's `_SUCCESS` is missing (§4.3) | Every `ETL_WINDOW`, one minute after the window end (UTC grid) |
| `etl maintenance retention` | Drops expired `pipeline.dedup_claims` partitions and old `written` consumer batches (§4.5) | Daily 11:00 (`ETL_SCHEDULE_TZ`) |
| `etl maintenance vacuum` | `VACUUM (ANALYZE)` of the tables the pipeline updates (§4.5) | Daily 11:30 (`ETL_SCHEDULE_TZ`) |

### 2.2 Infrastructure (pinned)

| Component | Image / version | Notes |
|---|---|---|
| Postgres | `postgres:18.6` | Volume at `/var/lib/postgresql` (the 18+ data layout). Healthcheck `pg_isready` over TCP. |
| Message broker | `redpandadata/redpanda:v26.2.4` | Kafka-API compatible, single C++ binary, no JVM. Dev-container mode, topic auto-create off, healthcheck `rpk cluster health`. Built-in schema registry and HTTP proxy stay unused. License: BSL 1.1 (free for this use). |
| Object storage | `versity/versitygw:v1.8.0` | posix backend, root `/lake` = `./data`; metadata in a sidecar directory, `./.s3meta`, because Docker Desktop bind mounts lack extended attributes |
| Scheduler | Cronicle v0.9.135 on `node:24.21.0-trixie-slim` | Our image (Dockerfile target `cronicle`): Cronicle built from the upstream release tarball (checksum pinned) plus the `etl` binary (§9.3) |
| Job metrics | `prom/pushgateway:v1.11.4` | Persisted to a volume; scraped with `honor_labels` |
| Monitoring | `prom/prometheus:v3.15.0`, `grafana/grafana:13.2.3` | Provisioned scrape config, alert rules, dashboard |
| Broker UI | `redpandadata/console:v3.12.0` | |
| Go | 1.27 (`golang:1.27` builder; runtime `gcr.io/distroless/static-debian13:nonroot`) | `github.com/twmb/franz-go` v1.22.1 (+ `kadm`), `github.com/jackc/pgx/v5` v5.11.0, `github.com/pressly/goose/v3` v3.28.0, `github.com/santhosh-tekuri/jsonschema/v6` v6.0.3, `github.com/prometheus/client_golang` v1.25.0 (+ `push`), `github.com/aws/aws-sdk-go-v2`, `github.com/google/uuid`, `github.com/shopspring/decimal` |

MinIO and rclone rejected: ADR 0001.

### 2.3 Why this chain and not a shorter one

- Fetcher → Kafka directly is simpler but drops the requirement that raw data lands in Postgres before deduplication.
- Debezium CDC (Postgres → Kafka in commit order) is the production choice but adds a JVM Kafka Connect stack.
- Kafka Streams would hold dedup state in RocksDB + changelog with `exactly_once_v2`, but it is JVM-only and its exactly-once covers Kafka → Kafka only (S3 would need Kafka Connect S3 Sink).

All three are documented in the README as production alternatives.

### 2.4 Why Redpanda

The design needs a replayable log, keyed partitions (every copy of one API response carries the same `idempotency_key` and lands on one partition, so one transformer judges the copies in offset order; other overlaps are settled by the Postgres claims, §7.3), and consumer groups. The publisher runs in batches, but the transformer is a consumer group: offsets make its batches replayable and crash-safe, replicas share and take over partitions, and the topic is a 7-day replay buffer independent of the publisher's schedule. Message order per coin is not kept, and nothing depends on it (§7.6). The Kafka protocol provides all of this and is the de-facto standard at Trust Wallet's scale. Redpanda speaks the same protocol as a single C++ binary without a JVM: lighter, faster to start, and the same franz-go client code runs unchanged against Apache Kafka, MSK or Confluent Cloud in production. Its main gap, the Kafka Connect ecosystem, is not used here.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen for the stack |
| Apache Kafka, Amazon MSK, Confluent Cloud | Drop-in for production (same protocol) |
| NATS JetStream, Apache Pulsar | Viable with caveats: different consumer model / heavier operations, weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, SQS, Google Pub/Sub | Rejected: no offset replay, or no keyed partitions shared by a consumer group |

### 2.5 Decisions taken in design review

| Topic | Decision |
|---|---|
| Object storage | versitygw with plain files (ADR 0001) |
| Raw table | One table `raw.api_calls`, one row per HTTP call incl. failures; entity splitting belongs to the transformer |
| Event semantics | An event is a source snapshot; a stale coin seen in two snapshots is two valid rows |
| Deduplication | One mechanism: natural record key in the transformer (§7.3); `idempotency_key` is the Kafka message key and a metric label, not a dedup input |
| Pipeline state | Postgres schema `pipeline`; Kafka Streams / goka rejected (JVM-only / no transactions) |
| Schema registry, Kafka Connect | Not used; README production notes |
| Bootstrapping | One idempotent `etl init` for Postgres migrations, topics and buckets, started after its dependencies are healthy |
| Logs | One file per replica: `logs/<service>/etl.log`, transformer replicas `logs/<container id>/transformer/etl.log`; scheduled jobs `logs/publisher/etl.log` and `logs/maintenance/etl.log`; + stdout |
| Monitoring | Prometheus + Grafana with alert rules and a provisioned dashboard; scheduled jobs report through a Pushgateway |
| Cleaning | Empty records → `rejected/`; questionable records kept with flags |
| Fetcher scope | Transport only; source-schema (S0) validation lives in the transformer |
| Schema distribution | Schemas embedded via `go:embed`; updates ship as redeploys; no schema registry |
| Topic name | `coingecko.raw` without a version suffix: the version travels in every message (`schema_version`, `schema` header); breaking changes are handled in the same topic by dispatching on version |
| Dedup content hash | Business fields only; excludes schema version and technical columns |
| Exporter claim | Rows claimed with `FOR UPDATE SKIP LOCKED` and stamped with their batch id in the same transaction as the ledger row: never skips a late commit, never stalls, a retry rewrites the same bytes; costs one UPDATE per raw row. Production: raw events straight to a log or object storage, or Debezium CDC (§4.2, ADR 0005) |
| Raw windows | `ETL_WINDOW` (default 5 min) windows aligned to the UTC hour, `_SUCCESS` written last; a window is ready when its `_SUCCESS` exists (§4.2, §6.5, ADR 0007) |
| Publisher job | A batch job per closed window that reads only S3: parts and checksums from `_SUCCESS`, no Postgres (§4.3, ADR 0007) |
| Scheduler | Cronicle locally (one process, web UI, catch-up, retries, timeouts, events provisioned as code); Airflow / Amazon MWAA in production (§9.3, ADR 0007) |
| Message key | The event's `idempotency_key`: responses spread over all partitions, copies of one response share a partition (§7.6, ADR 0003) |
| Object names | Window, or the batch's processing time plus a short hash of its Kafka position; no raw database or broker counters (the Kafka position enters only hashed with the topic id), so the lake does not depend on their lifetimes (§6.5, ADR 0008) |
| Maintenance and claims retention | Daily daytime jobs: drop `dedup_claims` partitions older than `ETL_CLAIMS_RETENTION_DAYS` (default 8; configuration refuses less than the topic retention + 1 day) and old `written` consumer batches; `VACUUM (ANALYZE)` (§4.5) |
| File format | JSON Lines: arrays, null vs empty string, nested fields in rejected and dlq, self-describing lines under schema evolution; Parquet is a production compaction step |
| Stage independence | Stages meet only through `raw.api_calls`, raw windows with `_SUCCESS`, topic messages and processed objects; `export_batch_id` is the exporter's own column |
| Polling cadence | 4 calls/min, 15 s apart: markets at :00 and :30 (the assignment's "every 30 s"), one coin at :15 and :45 in turn, so each coin every 2 min; all four coins every minute (6 calls/min, 60% of the worst-case keyless limit of 10, little room for retries) and markets every 60 s (breaks "every 30 s") were rejected |

## 3. Source: CoinGecko

### 3.1 Endpoints

| Endpoint | Schedule | Purpose |
|---|---|---|
| `GET /api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&price_change_percentage=1h,24h,7d&sparkline=false&precision=full` | Every 30 s | Flat 30-second time series |
| `GET /api/v3/coins/{id}?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false` | Every 30 s, between the markets calls (at :15 and :45), one coin per call, in turn: `bitcoin → ethereum → tether → usd-coin → bitcoin …`, so each coin is refreshed every 2 min | Deep nesting: 23 maps × 63 currencies, 100 tickers with nested objects, platforms by chain |

### 3.2 Observed source behaviour (2026-10-10)

- CoinGecko recomputes all coins in one pass every 60 s and stamps them with the same `last_updated`. The CDN caches with `s-maxage=60`, so polling every 30 s yields half or more byte-identical markets responses (two in three on the demo stack, where cached copies were served at ages of 29 to 59 s).
- Keyless limit is dynamic, 10–30 calls/min per IP; HTTP 429 comes with `Retry-After: 60`. Budget: 4 calls/min, at least 15 s apart (markets at :00 and :30, a coin at :15 and :45), one request in flight, honor `Retry-After`, exponential backoff with jitter on 5xx/timeouts, skip ticks rather than queueing them.
- Optional `COINGECKO_API_KEY` sends `x-cg-demo-api-key` (Demo plan: 100 calls/min, 10k calls/month ≈ 1.7 days at 4 calls/min).
- `community_data` and `developer_data` are no longer returned; the source schema must tolerate drift.
- Values exceed 2^53 (`market_data.market_cap.vnd`); "absent" is encoded as `null`, `""`, `[]` or a missing key; bitcoin has `platforms: {"": ""}`.
- README must include the "Data provided by CoinGecko" attribution.

## 4. Stages

### 4.1 Fetcher → `raw.api_calls`

One row per HTTP call, including failures. Columns mirror schema S1 (§5.1); `export_batch_id` is the exporter's (`migrations/00001_raw.sql`):

```sql
CREATE SCHEMA IF NOT EXISTS raw;

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
  payload          json,                              -- response body byte for byte (json, not jsonb)
  payload_sha256   bytea       CHECK (octet_length(payload_sha256) = 32),
  export_batch_id  bigint,                            -- pipeline.export_batches.batch_id; NULL = not exported yet
  CONSTRAINT api_calls_pkey PRIMARY KEY (event_id, fetched_at),
  CONSTRAINT api_calls_payload_iff_ok CHECK ((outcome = 'ok') = (payload IS NOT NULL))
) PARTITION BY RANGE (fetched_at);

COMMENT ON COLUMN raw.api_calls.payload IS 'Response body byte-for-byte (json, not jsonb).';
COMMENT ON COLUMN raw.api_calls.export_batch_id IS 'pipeline.export_batches.batch_id of the batch that exported this row; NULL = not exported yet (rows with outcome <> ok stay NULL forever).';

-- Exporter claim: unexported ok rows in seq order (a queue-like index).
CREATE INDEX api_calls_unexported_idx ON raw.api_calls (seq) WHERE outcome = 'ok' AND export_batch_id IS NULL;
-- Exporter read-back of one part.
CREATE INDEX api_calls_export_batch_idx ON raw.api_calls (export_batch_id, seq) WHERE export_batch_id IS NOT NULL;
CREATE INDEX api_calls_idempotency_key_idx ON raw.api_calls (idempotency_key);
```

The foreign key `api_calls_export_batch_fk` (`export_batch_id` → `pipeline.export_batches`) is added in `00002` after the ledger exists (§7.2).

- `payload` is the response body with surrounding JSON whitespace trimmed, byte for byte. A body that contains a line break is stored compacted instead (insignificant whitespace removed; string contents and number text verbatim), so one call stays one JSONL line and one message; `event.Marshal` refuses a payload with CR or LF. `payload_sha256` is computed over the stored bytes.
- `payload` is `json`, not `jsonb`: `json` keeps the exact bytes, so `payload_sha256` stays verifiable at every hop. SQL access works the same (`payload->>'id'`).
- `idempotency_key = sha256(source | endpoint | canonical params | payload_sha256)`. It is the Kafka message key (§7.6), labels the `duplicate_response` drop reason and serves SQL analysis of raw duplicates. Deduplication does not use it (§7.3).
- `export_batch_id` is the only column ever updated, once, by the exporter's claim (§4.2). Rows with `outcome <> 'ok'` keep it NULL.
- Failed calls (429, 5xx, timeouts) are inspected with SQL: `WHERE outcome <> 'ok'`. Source-schema violations are found in `dlq/` (validated in the transformer).
- Retries of the same insert are idempotent: `ON CONFLICT DO NOTHING` on the primary key.
- Daily UTC partitions of `raw.api_calls` and `pipeline.dedup_claims` (yesterday … today+3) are created by `etl init` and kept ahead by the fetcher; no DEFAULT partition. Raw partitions are never dropped in this stack (production: pg_partman); claim partitions are dropped by retention (§4.5).

### 4.2 Exporter → `s3://raw`

- **Single active instance** via `pg_try_advisory_lock`, held on a dedicated connection for the life of the process. Every `ETL_EXPORT_INTERVAL` (default 15 s; configuration requires it to be shorter than the publisher's 1-minute grace) a tick closes every window that is due.
- **Why a `SKIP LOCKED` claim.** The assignment places raw data in Postgres first, which gives a queryable record of every API call, failures included, before any deduplication. A transactional table is not a change log, so the exporter treats it as a work queue: one short transaction picks unexported rows `FOR UPDATE SKIP LOCKED`, inserts the ledger row and stamps the rows with its batch id. A row whose transaction commits late is claimed by a later window, so nothing is skipped and nothing waits for a long transaction; a retry after a crash reads the stamped rows back and writes the same bytes. The cost is one non-HOT UPDATE per exported row (a dead tuple for vacuum and new index entries; the large payload stays in TOAST and is not copied), so `raw.api_calls` is not strictly append-only, while the lake stays write-once. In production raw events would land directly in a log or object storage, or the table would be streamed with Debezium CDC (ADR 0005).
- **Windows.** `ETL_WINDOW` (default `5m`) must be whole minutes from 1m to 60m that divide 60m (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60); windows `[start, end)` are aligned to the UTC hour. Window state lives in `pipeline.export_windows`: `claiming` (takes parts) → `sealed` (manifest frozen, its sha256 stored) → `closed` (`_SUCCESS` written). At most one window is not closed (`export_windows_one_pending`), so windows close strictly in order. The next window starts at the first boundary of the current size at or after the last closed end, so a changed `ETL_WINDOW` switches at the next boundary of the new size.
- **Tick.** First finish parts a crashed run left `claimed`. Then, for every window that is due (`now` ≥ its end), in order: open it, claim and export parts until no row is left, seal it, write its `_SUCCESS`, mark it closed. After downtime the missed windows close one after another.
- **Claim** (one short transaction, no S3 I/O; `internal/exporter/exporter.go` `claimIn`):

  ```sql
  -- Lock the window row: claims of one window serialize, and none follows the seal.
  SELECT 1 FROM pipeline.export_windows WHERE window_start = $1 AND status = 'claiming' FOR UPDATE;
  -- Pick: the predicates are literals, so pgx's generic plan uses the partial index.
  SELECT event_id::text, fetched_at FROM raw.api_calls
   WHERE outcome = 'ok' AND export_batch_id IS NULL AND fetched_at < $1   -- window end
   ORDER BY seq
   LIMIT $2                                                                  -- ETL_EXPORT_BATCH_MAX (500)
     FOR UPDATE SKIP LOCKED;
  -- Only if rows were found (batch ids stay dense): next part index, nextval for batch_id, then the ledger row first (the FK needs it).
  INSERT INTO pipeline.export_batches (batch_id, window_start, part_index, row_count, min_fetched_at, max_fetched_at, object_key, claimed_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
  -- Stamp by primary key, never by seq; the redundant BETWEEN prunes partitions.
  UPDATE raw.api_calls a
     SET export_batch_id = $1
    FROM unnest($2::text[]::uuid[], $3::timestamptz[]) AS p(event_id, fetched_at)
   WHERE a.event_id = p.event_id
     AND a.fetched_at = p.fetched_at
     AND a.export_batch_id IS NULL
     AND a.fetched_at BETWEEN $4 AND $5;
  ```

  The stamp must touch exactly the picked rows, or the transaction rolls back; `row_count` is the number stamped.
- **Which rows a window holds.** The rows the exporter claims when it closes the window: `outcome = 'ok'`, not exported yet, `fetched_at` < window end. A row that commits after its window was sealed, or is locked by another session at claim time, goes into the next window, so nothing is lost. The claim has no lower bound, so after a `ETL_WINDOW` change the rows between the last end and the new boundary go into the new window.
- **The first window.** On an empty ledger the exporter starts at the window holding the oldest unexported ok row; when no row is older than the current window, it closes the window just before the current one at once (the bootstrap window, usually empty), so the first publish run finds a `_SUCCESS`. If the lake already holds that window's `_SUCCESS` (a lake kept from an earlier stack), the exporter starts at the current window and never overwrites it.
- **Exporting a part.** Read the stamped rows back: `WHERE export_batch_id = $1 AND fetched_at BETWEEN <ledger bounds> ORDER BY seq, event_id, fetched_at` (`seq` alone is not unique on the partitioned table); their number must equal `row_count`. Serialize deterministic JSONL (each line is the S1 event, payload embedded verbatim) and validate each line against S1; failures go to the part's DLQ object. Store `content_sha256` and `line_count` before the first PUT (a rebuilt part must match them). PUT the DLQ object, then the part, with `If-None-Match: *` and `x-amz-meta-sha256`; on 412, HEAD and compare the checksum. Mark the part `exported`. A part whose every row went to the DLQ is still written (empty), so every claimed part has one raw object.
- **Seal, then `_SUCCESS` last.** The seal builds the manifest from the exported parts (part index order) and stores its sha256. Then `_SUCCESS` is PUT (every retry rebuilds identical bytes and checks them against the stored sha256) and the window is marked `closed`. Every part and DLQ object was written before the seal, so `_SUCCESS` is the window's last object. An empty window gets `{"window_start":…,"window_end":…,"rows":0,"parts":[]}`, so a missing `_SUCCESS` always means the exporter is behind or down. Keys and the manifest format: §6.5.
- **Export lag.** `etl_export_lag_seconds` is the time past the end of the oldest window not closed yet: 0 on schedule (and before the first window), growing while S3 fails, a claimed part keeps failing, Postgres is unreachable, or a catch-up runs. Rows locked by another session do not count: they go into a later window.
- Never hold a DB transaction across S3 or broker I/O.
- Only `outcome = 'ok'` rows are exported.

### 4.3 Publisher → broker (`etl publish`, a scheduled job)

- A one-shot command that publishes one closed window and exits. Cronicle runs it one minute after every window end as `etl publish --now "$JOB_NOW"` (§9.3). `--now` takes unix seconds or RFC 3339; without the flag the clock is used; `--now ""` (an unset `JOB_NOW`) is a usage error, exit 2.
- **Window:** end = floor(now − 1 min, `ETL_WINDOW`), start = end − `ETL_WINDOW`. `now` is the scheduler's logical run time, not the clock, so a catch-up run or a rerun takes its own window.
- **Ready check:** read `raw/source=coingecko/ingest_date=<d>/window=<HHMM>/_SUCCESS`. Missing → log `window not ready`, exit 1 (Cronicle retries 3 times, 60 s apart). A manifest that does not parse strictly, describes another window (the exporter and the publisher disagree on `ETL_WINDOW`), or lists a part that is missing or has another sha256 or line count is an integrity error: exit 1, nothing produced. Every listed part is read and verified before anything is produced.
- **Lines:** each line is validated against S1 and must be an `ok` event with an `idempotency_key`. Failures (`decode_error`, `schema_violation`, `unexpected_outcome`) go to one deterministic DLQ object per window, `dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl`, written before producing. If a rerun finds that object with other bytes (validator messages changed between builds), the first object is kept and an ERROR is logged.
- Topic `coingecko.raw`, 6 partitions, retention (TTL) 7 days: messages older than 7 days are deleted, while `s3://raw` and `raw.api_calls` keep the history, so the topic is a 7-day replay window. Key: the event's `idempotency_key` (§7.6). Headers: `event_id`, `idempotency_key`, `schema`, `raw_object` (the part key), `line_no`. Each valid line is produced verbatim.
- Idempotent producer, `acks=all`; the run succeeds only after the broker acknowledged every record. A rerun produces the same records again, and the transformer drops the copies as `replay`.
- **No Postgres:** the job reads only S3 and writes only the broker and `dlq/`.
- **Run frame** (shared with §4.5): a 4-minute timeout, below Cronicle's 5-minute job timeout, so a stuck S3 or broker still ends in `job failed` and a push. On SIGTERM the job logs `job failed`, pushes `etl_job_success 0` and exits 1 within Cronicle's 10 s kill grace. At the end of every run it pushes once to the Pushgateway, group `job="etl_publisher"` (§8.2); `etl_publish_records` and `etl_publish_dlq_lines` only on success. A failed push is logged at ERROR and does not change the exit code. Log: `logs/publisher/etl.log` (`job started`, `publish started`, then `window published` or `window not ready`, then `job finished` or `job failed`).

### 4.4 Transformer → processed / rejected / dlq

See §7 for the batch algorithm.

### 4.5 Maintenance → Postgres (`etl maintenance`, scheduled jobs)

- **`etl maintenance retention`** (Cronicle `etl-retention`, daily 11:00). Cutoff = UTC midnight of (today − `ETL_CLAIMS_RETENTION_DAYS`). It drops the `pipeline.dedup_claims` partitions whose upper bound is at or before the cutoff, oldest first (`DROP`, not `DELETE`: no dead rows), each DROP under a 3 s `lock_timeout`, up to 5 attempts 2 s apart. Then it deletes `pipeline.consumer_batches` rows with status `written` and `written_at` before the cutoff. `planned` rows are never touched: re-planning and manual repair need them. Example: on 2026-10-12 with 8 days the cutoff is 2026-10-04, so claims of event dates up to 2026-10-03 go.
- **Why at least the topic retention + 1 day.** Claim partitions follow `event_date`, while replays follow publish time: an exporter that was down for a day publishes yesterday's events today. Configuration refuses `ETL_CLAIMS_RETENTION_DAYS` below `ceil(ETL_TOPIC_RETENTION / 24 h) + 1` (8 with the 7-day topic): every service and job refuses to start with such a value. Redeliveries, topic replays, window reruns and raw republishes are deduplicated only within this period; older ones may write duplicates to `processed/`. Production would keep 30 days, which also covers a raw republish after a schema fix.
- `raw.api_calls` partitions and the export ledger are not removed (production: pg_partman; raw rows first, then ledger rows, because of the foreign key).
- **`etl maintenance vacuum`** (Cronicle `etl-vacuum`, daily 11:30): `VACUUM (ANALYZE)` of `raw.api_calls` (dead versions from the claim UPDATE; on the partitioned parent it covers every partition and refreshes the parent statistics autovacuum never collects), `pipeline.export_batches` and `pipeline.consumer_batches`, one autocommit statement each on a dedicated session with `statement_timeout` and `transaction_timeout` off. It complements autovacuum.
- Both jobs use the run frame of §4.3: exit codes, SIGTERM handling, one push per run under `job="etl_retention"` or `job="etl_vacuum"` (retention pushes `etl_retention_partitions_dropped` and `etl_retention_batches_deleted` also on failure: what it really removed), log `logs/maintenance/etl.log`. They run in the daytime on purpose: the pipeline load is flat around the clock, and a failure is seen by people at work.

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
| exporter, publish job | S1 | Write to `dlq/`, do not pass on |
| transformer, input | S1, then the payload against S0 | `dlq/` (`schema_violation` / `source_schema_violation`). After a schema fix, the raw windows are republished (`etl publish` rerun) and reprocessed, within the claims retention (§4.5) |
| transformer, output | S2 | `dlq/`, never written to processed |
| writing rejected / dlq | S3 | ERROR log with the original bytes in base64 |

Schemas are embedded in the binary, so producers and consumers always carry the schema version of their build; a schema change ships as a redeploy. Rollout order for a compatible change: consumers (transformer) first, then producers (publish job, exporter, fetcher). CI checks that a new schema version still accepts every fixture of the previous one. A breaking change gets a new major version (`v2`) handled side by side.

Cleaning-rule drops are not schema failures: they go to `rejected/`, not `dlq/`. `dlq/` means "something broke, a human is needed".

### 5.3 Go types

Go structs are hand-written. Contract tests keep them in sync: every schema compiles offline; valid/invalid fixtures behave as expected; struct ↔ schema drift test; recorded live CoinGecko responses pass S0; fuzzed structs marshal to valid documents. Code generation is not used (go-jsonschema generates a date-time type that marshals to `{}`).

## 6. Data model

### 6.1 Time model

| Field | Source | Meaning |
|---|---|---|
| `snapshot_at` | Derived: max `last_updated` in a markets response; the document's `last_updated` for `/coins/{id}` | When CoinGecko assembled the snapshot. Event time; its date (`event_date`) is a dedup-key column and the partition key of `pipeline.dedup_claims`; lake partitions use processing time (§6.5) |
| `source_updated_at` | API `last_updated` of the coin | When CoinGecko last recomputed this coin. Lags `snapshot_at` by hours for stale coins. |
| `fetched_at` | Ours | When we polled. Picks the raw window (§4.2). Lineage. |
| `planned_at` | Ours (transformer batch manifest) | When the transformer planned the batch: `_processed_at` of its rows and the processing time in its object names (§6.5) |
| `tickers[].last_traded_at`, `last_fetch_at` | API | Exchange trade time; when CoinGecko fetched exchange data |

An event is a source snapshot. A stale coin observed in two different snapshots yields two valid rows (both `is_stale = true`); two polls served the same cached response yield one. Clocks of different systems are not synchronized, so no logic depends on their ordering.

### 6.2 Processed datasets

| Dataset | Source | Natural key | Rows per snapshot |
|---|---|---|---|
| `market_snapshot` | `/coins/markets` | coin_id, snapshot_at | 100 |
| `coin_snapshot` (parent) | `/coins/{id}` scalars: rank, supply, categories, genesis_date, … | coin_id, snapshot_at | 1 |
| `coin_market_by_currency` | 23 `market_data` maps zipped on the currency key: price, market cap, FDV, volume, high/low 24h, ATH/ATL (+ dates), changes 1h…1y | coin_id, currency, snapshot_at | 63 |
| `ticker` | `tickers[]`; `market` → `exchange_id`, `exchange_name`; `converted_last/volume` → `_usd`, `_btc`, `_eth` columns | coin_id, exchange_id, base, target, snapshot_at | 100 |
| `coin_contract` | `platforms` + `detail_platforms` zipped on the platform key | coin_id, platform_id, snapshot_at | up to 36 |

Child datasets reference the parent by `(coin_id, snapshot_at)`. Dropped as unnecessary: images, descriptions, links, sparkline, `trade_url`, `token_info_url`, duplicated fields (`coin_mcap_usd`).

Every row carries technical columns `_event_id`, `_raw_object` (the raw part key), `_kafka_partition`, `_kafka_offset`, `_schema_version`, `_processed_at` (the batch manifest's `planned_at`, never `now()`; it also names the object).

### 6.3 Formats

- Timestamps: RFC 3339 UTC with milliseconds (`2026-10-10T10:07:55.000Z`); `genesis_date` as `YYYY-MM-DD`.
- Money and quantities (prices, caps, volumes, supplies): decimal strings (`"43083803721443304"`), decoded with `json.Number` and a decimal type; never float64. Percentages and ratios: JSON numbers.
- Symbols upper-case; EVM addresses lower-case.
- Files: plain `.jsonl` (no gzip) so reviewers can `cat` them.

### 6.4 Cleaning rules

- **Normalize:** `""` and `[]` → `null`; timestamps and numbers as in §6.3; `roi` flattened to `roi_times`, `roi_currency`, `roi_pct`.
- **Reject to `rejected/` with reason:** `missing_id`, `null_price`, `nonpositive_price`, `empty_platform`, `empty_record` (all metrics null), `snapshot_conflict` (§7.3).
- **Flag and keep:**
  - `is_stale`: `snapshot_at − source_updated_at > 10 min`; for tickers, the upstream flag or last trade older than 1 h.
  - `is_inactive` (`market_snapshot`, `coin_market_by_currency`): zero volume.
  - `is_anomaly`: price outside `[low_24h, high_24h]`; for tickers, the upstream flag or `last_usd` more than 2% from the coin's median across tickers in the snapshot. For `market_snapshot` the range is widened by `ETL_ANOMALY_TOLERANCE_PCT` percent on each side (default 0.5; 0 = strict): `price < low_24h × (1 − t/100)` or `price > high_24h × (1 + t/100)`, in decimal, because `precision=full` prices carry more digits than the rounded 24 h bounds. `coin_market_by_currency` keeps the strict range (`/coins/{id}` sends price and bounds at one precision).
- Thresholds are configuration.

### 6.5 Object layout

```
data/raw/source=coingecko/ingest_date=2026-10-12/window=0409/part-00000.jsonl
data/raw/source=coingecko/ingest_date=2026-10-12/window=0409/_SUCCESS
data/processed/dataset=ticker/v=1/processed_date=2026-10-12/hour=04/part-20261012T041500Z-83422856.jsonl
data/rejected/dataset=coin_contract/processed_date=2026-10-12/hour=04/part-20261012T041600Z-46f75e7c.jsonl
data/dlq/stage=transform/processed_date=2026-10-12/hour=04/part-20261012T041500Z-83422856.jsonl
data/dlq/stage=export/ingest_date=2026-10-12/window=0409/part-00000.jsonl
data/dlq/stage=publish/ingest_date=2026-10-12/window=0409/part-00000.jsonl
```

The raw, processed and rejected keys are from a real run; the dlq keys are illustrative.

**Raw zone and the export and publish DLQ: named by window.** `ingest_date` and `HHMM` are the UTC date and time of the window start. Parts are numbered from `part-00000` (five digits); an export DLQ object carries the index of the raw part whose lines failed; the publish DLQ has one object per window, always `part-00000`.

`_SUCCESS` is compact JSON with the keys in this order and the parts in part-index order; each part `key` is an object key inside bucket `raw`; a part's `rows` counts the lines of the part (lines the exporter sent to its DLQ are not in it), and the top-level `rows` is their sum. A real 1-minute window:

```json
{"window_start":"2026-10-12T04:09:00.000Z","window_end":"2026-10-12T04:10:00.000Z","rows":4,"parts":[{"key":"source=coingecko/ingest_date=2026-10-12/window=0409/part-00000.jsonl","sha256":"61732189b09306c562dc75cdba0a0899f6f129ab59bcab8409394becfb95a779","rows":4}]}
```

An empty window has `"rows":0,"parts":[]`. The contract for a reader of the raw zone: a window is ready when its `_SUCCESS` exists; read exactly the parts it lists and check their sha256. How many parts there are (at most `ETL_EXPORT_BATCH_MAX` rows each) is the exporter's business.

**Processed, rejected and the transform DLQ: named by processing time.** `processed_date` and `hour` are the UTC date and hour of the batch's `planned_at` (from its manifest row); `part-<T>-<id8>`: `T` is `planned_at` as `YYYYMMDDTHHMMSSZ` (UTC, truncated to the second), `id8` the first 8 hex characters of `sha256("<topic_id>|<partition>|<first_offset>")`. A batch writes one object per (zone, dataset), all with the same `T` and `id8`. A re-planned batch keeps its manifest `planned_at`, so it writes the same keys; a re-created topic has a new `topic_id` and a new time, so its keys never collide with older ones. Names carry no database or broker counters, so the lake does not depend on their lifetimes and may be kept when the stack is re-created (ADR 0008).

`event_date` (the UTC date of `snapshot_at`) is part of the dedup key and a column of rejected records and of `dedup_claims`, not a directory; processed rows have no `event_date` column (it is derived from `snapshot_at`): queries by event time scan more files (production compaction to Parquet would partition by event date). An hour directory can still receive a batch written after a crash: a batch planned at 10:59:59 and written later lands in `hour=10`. There is no hour-completeness marker.

## 7. Delivery guarantees

### 7.1 Principle

Every hop after the insert into Postgres is at-least-once; the source hop is best effort (a failed insert loses that poll, and the next poll covers it). Exactly-once content in the processed zone comes from making every step safely repeatable: deterministic keys, ledgers in Postgres, and write-once objects. **Deduplication happens once (in the transformer); idempotency happens at every write.**

| Hop | Mechanism | On retry |
|---|---|---|
| fetcher → `api_calls` | PK `event_id`, `ON CONFLICT DO NOTHING` | No retry: a failed insert loses that poll (ERROR log; the next poll 30 s later covers it) |
| exporter → `raw/` | Rows claimed with `FOR UPDATE SKIP LOCKED` and stamped with the batch id in the ledger transaction; keys fixed at claim; `If-None-Match: *`; `_SUCCESS` last | Same object: 412 + equal checksum |
| `s3://raw` → broker (`etl publish`) | Window from the scheduler's time; parts and sha256 from `_SUCCESS`; idempotent producer, `acks=all` | A rerun produces copies with the same `event_id`; the transformer drops them as `replay` |
| transformer → `processed/` | Batch manifest; write-once objects; offset commit after write | Same objects: 412 + equal checksum |

### 7.2 Pipeline-state tables (schema `pipeline`)

`migrations/00002_pipeline.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Exporter windows: one row per ETL_WINDOW window, aligned to the UTC hour.
CREATE TABLE pipeline.export_windows (
  window_start   timestamptz PRIMARY KEY,
  window_end     timestamptz NOT NULL CHECK (window_end > window_start),
  status         text NOT NULL DEFAULT 'claiming' CHECK (status IN ('claiming','sealed','closed')),
  success_key    text NOT NULL,
  success_sha256 bytea CHECK (octet_length(success_sha256) = 32),
  row_count      int CHECK (row_count >= 0),   -- lines in the window's raw parts
  part_count     int CHECK (part_count >= 0),
  opened_at      timestamptz NOT NULL DEFAULT now(),
  closed_at      timestamptz,
  CONSTRAINT export_windows_sealed CHECK ((status = 'claiming') = (success_sha256 IS NULL)
                                       AND (success_sha256 IS NULL) = (row_count IS NULL)
                                       AND (success_sha256 IS NULL) = (part_count IS NULL)),
  CONSTRAINT export_windows_closed CHECK ((status = 'closed') = (closed_at IS NOT NULL))
);
-- Windows close strictly in order: at most one window is not closed yet.
CREATE UNIQUE INDEX export_windows_one_pending ON pipeline.export_windows ((true)) WHERE status <> 'closed';

-- Exporter ledger: one row per raw part of a window.
CREATE TABLE pipeline.export_batches (
  batch_id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  window_start        timestamptz NOT NULL REFERENCES pipeline.export_windows (window_start),
  part_index          int    NOT NULL CHECK (part_index >= 0),
  row_count           int    NOT NULL CHECK (row_count > 0),                     -- rows claimed (raw lines + dlq lines)
  line_count          int    CHECK (line_count >= 0 AND line_count <= row_count), -- lines in the raw part; set with content_sha256
  min_fetched_at      timestamptz NOT NULL,   -- fetched_at bounds of the claimed rows: partition pruning for the read-back
  max_fetched_at      timestamptz NOT NULL,
  object_key          text   NOT NULL UNIQUE,
  content_sha256      bytea,
  status              text   NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','exported')),
  claimed_at          timestamptz NOT NULL DEFAULT now(),
  exported_at         timestamptz,
  UNIQUE (window_start, part_index),
  CHECK (max_fetched_at >= min_fetched_at),
  CHECK ((content_sha256 IS NULL) = (line_count IS NULL))
);

ALTER TABLE raw.api_calls ADD CONSTRAINT api_calls_export_batch_fk
  FOREIGN KEY (export_batch_id) REFERENCES pipeline.export_batches (batch_id);

CREATE TABLE pipeline.dedup_claims (
  dataset         text   NOT NULL,
  record_key      text   NOT NULL,          -- natural key, e.g. 'bitcoin|2026-10-10T10:07:55.000Z'
  event_date      date   NOT NULL,          -- date of snapshot_at; partition key
  topic_id        uuid   NOT NULL,          -- topic id from metadata (topic incarnation); falls back to ETL_PIPELINE_EPOCH if the broker returns none
  kafka_partition int    NOT NULL,
  kafka_offset    bigint NOT NULL,
  ordinal         int    NOT NULL,          -- record index within the message
  event_id        uuid   NOT NULL,
  idempotency_key text,                     -- winner's idempotency key, for the duplicate_response drop reason; NULL when absent
  content_sha256  bytea  NOT NULL CHECK (octet_length(content_sha256) = 32),
  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT dedup_claims_pkey PRIMARY KEY (dataset, record_key, event_date)
) PARTITION BY RANGE (event_date);

COMMENT ON COLUMN pipeline.dedup_claims.first_seen_at IS 'Audit only: when the key was first claimed. No code reads it.';

-- Claims by Kafka position, for manual repair (ADR 0004); no pipeline code queries it.
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
```

The same migration sets two database defaults for new sessions: `idle_in_transaction_session_timeout = 60s` and, on PostgreSQL 17 and later, `transaction_timeout = 5min` (§7.7).

Postgres holds this state because it must survive crashes, be shared by all transformer replicas, and change atomically. Broker offsets cannot answer "was key X seen", S3 can only guard single objects, and in-memory state is lost on rebalance.

### 7.3 Deduplication rules (single mechanism)

Every record bound for `processed/` or `rejected/` claims its natural key (§6.2) with `INSERT … ON CONFLICT DO NOTHING`; the first claimant wins. Claims are inserted in sorted key order, so two transformers with overlapping key sets cannot deadlock.

| Claim for the record key | Action |
|---|---|
| Points to this record's own position (partition, offset, ordinal) | Write it (first sight or redelivery after a crash) |
| Other position, same content hash | Drop as duplicate |
| Other position, different content hash | Write to `rejected/` with `snapshot_conflict` |

The content hash (`content_sha256`) is computed over the record's business fields only, in canonical form. It excludes `_schema_version` and every technical column (`_event_id`, `_raw_object`, `_kafka_partition`, `_kafka_offset`, `_processed_at`) and `fetched_at`; otherwise identical records arriving in different messages would never match.

Drop reasons for metrics only (no separate logic): same `event_id` → `replay` (the same message published twice); same `idempotency_key` → `duplicate_response` (a byte-identical response from another poll); otherwise `duplicate_record` (the same content from another response). `dlq/` records are not deduplicated.

Records without an id of their own (rejected as `missing_id`) are keyed `#<idempotency_key, or event_id>|<index in the payload>|<snapshot_at>`, tickers prefixed with `<coin_id>|`: keys from different coins and responses never collide, and copies of one response still dedup. `dedup_claims.first_seen_at` is an audit column; no code reads it.

### 7.4 Transformer batch algorithm

franz-go: `DisableAutoCommit`, `BlockRebalanceOnPoll`, cooperative-sticky balancer, `FetchMinBytes` 1 MB / `FetchMaxWait` 5 s, max 500 records. At most one in-flight batch per partition. A batch is a contiguous offset range `[F..L]` of one partition.

Steps 1 and 2 run inside one transaction, TX1, in this order: TX1 begins and fixes the range first (2a: `planned_at` is the batch's only clock); the input checks (1) then run inside TX1 without database queries; then 2b–2e.

1. **Input checks (no database queries):** valid JSON (no duplicate keys, valid UTF-8) else `dlq` `decode_error`; S1 else `dlq` `schema_violation`; `sha256(payload) == payload_sha256` else `dlq` `integrity_error`; payload against S0 else `dlq` `source_schema_violation`.
2. **TX1 (milliseconds, no network I/O):**
   a. **First:** insert the range into `consumer_batches` (`planned`, with `planned_at`). The EXCLUDE constraint forbids overlap; if a row with this start exists, use its stored range and `planned_at` instead of the polled ones. `planned_at` is `_processed_at` of every row, `occurred_at` of every dlq record and the time in every object name.
   b. Transform passing messages into dataset records (cleaning rules §6.4); a message that cannot be transformed goes to `dlq`.
   c. Validate the records bound for `processed/` against S2 (failures → `dlq`, without a claim, so a republish after a schema fix still writes them).
   d. Insert claims for the records bound for `processed/` or `rejected/`; read them back; decide per record (§7.3).
   e. Build objects per (zone, dataset), keyed by `planned_at` and `id8` (§6.5), serialized deterministically (records by offset then ordinal, fixed field order). Store object keys, checksums and counts on the manifest row; if stored checksums from a previous attempt differ, alert and stop (non-determinism).
   COMMIT.
3. **PUT** each object with `If-None-Match: *` and `x-amz-meta-sha256`. 200 → done; 412 → HEAD and compare checksum (equal → done, different → alert and stop); 5xx / network → retry with backoff, never commit the offset.
4. **TX2:** mark the batch `written`.
5. **Commit offset `L+1` synchronously**, then allow rebalance. A rejected commit means the partition moved; stop work on it.

**Alert and stop** (a different checksum on PUT in step 3, or a non-deterministic re-plan in step 2e) halts the affected partition in place: the transformer pauses fetching it, does not commit, keeps its `etl_consumer_lag_records` at the high watermark minus the first uncommitted offset (refreshed from the topic's end offsets while the partition is paused), keeps processing its other partitions and serving `/metrics`, and counts `etl_s3_put_total{result="mismatch"}`, which fires the critical alert `ETLObjectChecksumMismatch`. The process does not exit (the consumer loop returns only when its context is cancelled), so the counter and the alert stay up until a human resolves the conflict and restarts the transformer. A halted partition that moves to another replica halts there too.

**On partition assignment:** find the manifest batch covering the committed offset. `written` → seek past it and commit; `planned` → reprocess exactly that range; none → normal processing.

A batch's `event_date` can fall outside the existing claim partitions (a replay of old raw windows, a late snapshot): TX1 then rolls back, the transformer creates the missing partitions outside any transaction and plans again.

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

Partitions are the unit of ordering and parallelism, not endpoint separation. Messages are keyed by the event's `idempotency_key` (sha256 of source, endpoint, parameters and payload hash), hashed by franz-go's default murmur2 partitioner, as in the Java client. Responses therefore spread over all 6 partitions, and every copy of one response (a CDN repeat, a publish rerun) lands on the same partition, where one transformer judges the copies in offset order. Other overlaps, such as the same record in two different responses on two partitions, are settled by the Postgres claims (§7.3).

Message order per coin is not kept, and nothing depends on it: records are independent, `snapshot_at` is in every key, and the stale and anomaly flags are computed within one response.

With 6 partitions, 2 transformers take 3 each; up to 6 replicas can be busy. The next scaling step is splitting a markets response (one message of 100 coins) into per-coin messages.

### 7.7 Invariants (README)

- Transforms are pure and deterministic: no `now()`, no random order. `planned_at` from the manifest is the batch's only clock and names its objects.
- Claims and `written` manifests are kept `ETL_CLAIMS_RETENTION_DAYS` (default 8, never less than the topic's 7 days + 1; §4.5): redeliveries, topic replays and window reruns within that period are deduplicated; a raw republish older than that may write duplicates.
- Postgres session and transaction timeouts (`idle_in_transaction_session_timeout = 60s`, `transaction_timeout = 5min`) stop a forgotten session from holding a connection and row locks, and from keeping VACUUM away from the dead versions the claim leaves behind. The exporter's `SKIP LOCKED` claim never waits for such a session.

## 8. Observability

### 8.1 Endpoints

Every long-running service (fetcher, exporter, transformer) listens on `:8080`: `/healthz` (process alive), `/readyz` (its dependencies reachable: Postgres for all, S3 for the exporter and the transformer, the broker for the transformer), `/metrics` (Prometheus). The scheduled jobs serve no endpoints: each run pushes its result to the Pushgateway, which Prometheus scrapes with `honor_labels`.

### 8.2 Metrics (prefix `etl_`)

| Source | Metrics |
|---|---|
| fetcher | `api_requests_total{endpoint,status}` (HTTP status, `network_error` or `skipped`), `api_request_duration_seconds`, `api_rate_limited_total`, `last_success_timestamp_seconds{endpoint}`, `source_data_age_seconds` (fetched_at − last_updated) |
| all services | `schema_validations_total{stage,schema,result}`, `schema_violations_total{stage,schema,keyword}` |
| exporter | `export_lag_seconds` (time past the end of the oldest window not closed yet; 0 on schedule), `export_batches_total{result}` |
| exporter, transformer | `s3_put_total{result}` (created, exists = 412 with equal bytes, mismatch, error), `dlq_total{stage,error_class}` |
| transformer | `records_total{dataset,outcome}` (written, duplicate, rejected, dlq), `dedup_dropped_total{reason}`, `rejected_total{reason}`, `consumer_lag_records{partition}`, `batch_duration_seconds`, `e2e_latency_seconds` (from `fetched_at` to the processed object; includes the wait for the window to close and the 1-minute publish grace) |
| scheduled jobs (Pushgateway, label `job` = `etl_publisher`, `etl_retention`, `etl_vacuum`) | `job_last_run_timestamp_seconds`, `job_last_success_timestamp_seconds` (pushed only on success), `job_duration_seconds`, `job_success` (1 or 0); `publish_records`, `publish_dlq_lines` (last successful publish run); `retention_partitions_dropped`, `retention_batches_deleted` (last retention run) |

Labels stay low-cardinality. The jobs push with POST ("add"): only the metric names in a push are replaced, so a failed run, which omits `etl_job_last_success_timestamp_seconds`, keeps the last success. The Pushgateway persists its series to a volume.

### 8.3 Logs

`log/slog` JSON handler, UTC time, fields `service`, `stage`, `event_id`, `batch`. Written to stdout and to one file per replica: `logs/<service>/etl.log`, and `logs/<container id>/transformer/etl.log` for each transformer replica (one writer per file; no shared file across containers). Covers API success/failure, transformation errors, and data saved.

The scheduled jobs append to `logs/publisher/etl.log` and `logs/maintenance/etl.log`; Cronicle runs one publish job and one maintenance job at a time, so each file keeps one writer. Every job line carries `job`; a run logs `job started`, then `job finished` or `job failed`. A job's stdout is also its log in the Cronicle UI.

### 8.4 Alerts (Prometheus rules)

`deploy/prometheus/alerts.yml`, 12 rules, unit-tested with promtool (`make alerts-test`):

- `ETLDataStale`: no successful poll for more than 2 minutes.
- `ETLDeadLetterRecords`: `dlq` records appeared (exporter and transformer counters, and the publish job's pushed `etl_publish_dlq_lines`).
- `ETLSourceSchemaDrift`: more than 5% of responses fail S0 over 15 minutes (upstream format change).
- `ETLExportLagHigh`: export lag above 5 minutes past a window end, or no export lag reported by the exporter (only the exporter's series counts; the other services export the gauge at 0). Causes: S3 failing, a claimed part that keeps failing, Postgres unreachable, the exporter down.
- `ETLConsumerLagGrowing`: consumer lag growing for 10 minutes.
- `ETLFrequentRateLimiting`: frequent 429 responses.
- `ETLServiceDown`: a long-running service (fetcher, exporter, transformer) is down for 2 minutes: its scrape fails (`up == 0`) or it has no scrape target at all (critical). The publish job is covered by `ETLPublisherStale`.
- `ETLPipelineNotProgressing`: CoinGecko calls succeed but no transformer record is counted over 15 minutes (a stalled exporter, publish job, broker or transformer).
- `ETLObjectChecksumMismatch`: an object key already holds different bytes, or a re-planned batch differs from its manifest (`etl_s3_put_total{result="mismatch"}` above 0; critical, fires at once and stays up while a transformer partition is halted, §7.4). Causes: a non-deterministic transform, a foreign writer, or a clock that went backwards.
- `ETLPublisherStale`: no successful `etl publish` in 15 minutes, or none at all after 15 minutes of Prometheus uptime (critical).
- `ETLMaintenanceStale`: retention or vacuum has not succeeded for 26 hours; `ETLMaintenanceNeverSucceeded`: one of them has no success series for 26 hours.

The thresholds of `ETLPublisherStale` and `ETLPipelineNotProgressing` assume `ETL_WINDOW` ≤ 5m; `ETLExportLagHigh` does not depend on the window size.

### 8.5 Grafana dashboard (provisioned)

Stats: data freshness, duplicate share (1 h), DLQ records (24 h), end-to-end latency p95, seconds since each scheduled job's last success. Time series: records per stage, data freshness and source age, end-to-end latency, duplicate share by reason, rejected records by reason, dead-letter records, consumer lag, API status codes, export lag and S3 writes, schema validations, published per window, batch job runs, retention.

## 9. Deployment

### 9.1 Compose

- Infrastructure: postgres, redpanda, versitygw, pushgateway, prometheus, grafana, redpanda-console; the scheduler, cronicle (§9.3).
- `etl init` depends on postgres, redpanda and versitygw being healthy (`service_healthy`); every `etl` service depends on `init` completing successfully (`service_completed_successfully`). `cronicle` also waits for a healthy pushgateway and a healthy exporter: on an empty ledger the exporter's first tick closes the window before the current one, so the first publish run finds its `_SUCCESS`, with 1-minute windows too.
- Services: fetcher, exporter, transformer (2 replicas). The jobs run inside the `cronicle` container, from the same `etl` binary, with the same `ETL_*` environment (one `ETL_WINDOW` for exporter, transformers and scheduler).
- Persistence: bind mounts `./data`, `./logs`, `./.s3meta`; named volumes `pgdata`, `redpanda`, `prometheus`, `grafana`, `pushgateway`, `cronicle` (compose project `coingecko-etl`). README also documents `docker run -v` equivalents.
- `.env`: optional `COINGECKO_API_KEY`; host ports (Cronicle `CRONICLE_PORT` 3012, Pushgateway `PUSHGATEWAY_PORT` 9091, …); `ETL_WINDOW`, `ETL_CLAIMS_RETENTION_DAYS`, `ETL_SCHEDULE_TZ`; Cronicle credentials; thresholds. `ETL_WINDOW` can be changed on a running stack (`make up`, also after `make demo`): the exporter starts the new size at its first boundary after the last closed window, and the rows in between go into that first new window, so no raw row is lost. The publish schedule switches when Cronicle restarts, so a window of the old size whose publish time falls after the switch gets no scheduled run, and the first run on the new grid can fail; such windows are published by hand with their own size (`docker compose exec -e ETL_WINDOW=<old> cronicle etl publish --now <end + 1 min>`).
- `make up`, `make down`, `make clean` (containers, named volumes, the lake and the logs), `make demo` (runs the stack with `ETL_WINDOW=1m`, waits for the first processed object, at most `DEMO_TIMEOUT` (300) seconds, then one more window, and prints `make demo-report`: objects and records per bucket, duplicate share, raw windows closed, seconds since the last successful publish, and the Grafana, Cronicle, Console and Prometheus links).
- Footprint: 12 services, 13 containers (`init` one-shot, transformer ×2), of which 12 keep running, about 1.2 GB RAM on the running demo stack (Grafana and Redpanda take most of it).

### 9.2 Image

Multi-stage Dockerfile (versions in §2.2): a Go builder, a `CGO_ENABLED=0` static binary, and the distroless `nonroot` runtime as the last stage, so it is the default target (`ENTRYPOINT ["/etl"]`). A second target, `cronicle`, builds Cronicle from the pinned upstream release on Node and adds the same `etl` binary. Healthchecks use `etl healthcheck`; the cronicle container is healthy once its events are provisioned and its UI answers.

### 9.3 Scheduler

Cronicle is the local stand-in for Airflow: one Node process with a web UI (`http://localhost:3012`, login `admin` / `CRONICLE_ADMIN_PASSWORD`), catch-up, retries, timeouts, run history, and an API to provision events as code. Upstream is in maintenance mode (bug fixes only; the author's successor project is xyOps) and publishes no official image, so the image is built from the pinned release.

**Events as code.** `deploy/cronicle/provision.js` creates or updates the three events at every container start (fixed ids, every owned field sent again, so it is idempotent); the container is healthy only after it succeeded, and a failure (an invalid `ETL_WINDOW` or `ETL_SCHEDULE_TZ`, for example) stops Cronicle, so the container restarts instead of running without its events. On a fresh `cronicle` volume, `deploy/cronicle/setup-gen.js` stores the admin password and the provisioning API key (`CRONICLE_API_KEY`); editing `.env` later does not change them: the password is changed under My Account in the UI, a new API key needs a fresh `cronicle` volume (`docker compose rm -sf cronicle && docker volume rm coingecko-etl_cronicle && make up`). Cronicle's transaction log on stdout includes request headers, so `docker compose logs cronicle` shows that key: it is a local development credential.

| Event (id) | Schedule | Command | Policy |
|---|---|---|---|
| `etl-publish` (`etlpublish`) | Every window end + 1 min on the UTC grid (5m: minutes 1, 6, …, 56; 1m: every minute) | `etl publish --now "$JOB_NOW"` | Catch-Up (Run All), 1 concurrent, 3 retries 60 s apart, timeout 5 min |
| `etl-retention` (`etlretention`) | Daily 11:00, `ETL_SCHEDULE_TZ` (default UTC) | `etl maintenance retention` | Catch-Up, category `maintenance` (one maintenance job at a time), 3 retries 60 s apart, timeout 30 min |
| `etl-vacuum` (`etlvacuum`) | Daily 11:30, `ETL_SCHEDULE_TZ` | `etl maintenance vacuum` | As retention |

`JOB_NOW` is the job's scheduled time in unix seconds, so a catch-up run and "Run Again" publish the window of their scheduled time. `ETL_SCHEDULE_TZ` places only the maintenance events; `etl-publish` always follows the UTC window grid. An optional `CRONICLE_WEB_HOOK` receives job notifications.

**First run and a kept lake.** Cronicle runs a new event first in the minute after its creation, so on a fresh stack the first publish run targets the bootstrap window (§4.2) or a later one. When the stack is re-created over a kept lake, nothing is published twice: the exporter never overwrites an earlier `_SUCCESS`, and the new scheduler only runs publish times (window end + 1 min) that come after it started. Windows the old stack closed but had not published are therefore not published automatically, and with a new Postgres volume the claims start empty, so republishing a window the old stack had already published writes duplicates.

**Rerun.** One window: "Run Again" on the job in the UI, or `POST /api/app/run_event/v1` with `{"id":"etlpublish","now":<window end epoch + 60>}` and the `X-API-Key` header; both respect the one-at-a-time limit. A range: the event's "Time Machine" with Catch-Up (Run All) on. `docker compose exec cronicle etl publish --now <time>` also works, but bypasses that limit. Reruns are safe within the claims retention (§4.5). A window that stays red after its retries is not re-run automatically, and no alert fires while later windows succeed: re-run it by hand. Strict window order (a `_published` marker, the `depends_on_past` analogue) is a production option.

**Stops.** On SIGTERM (`docker compose stop`, a restart) Cronicle aborts the running job (10 s kill grace; the job pushes `etl_job_success 0` and exits 1) and rewinds the event cursor, so the window runs again after the restart. After SIGKILL the in-flight window stays red and must be re-run by hand.

**Production.** Amazon MWAA (Airflow): `schedule='1-59/5 * * * *'`, `catchup=True`, `max_active_runs=1`, an `S3KeySensor` on the window's `_SUCCESS`, `retries=3`, `retry_delay` 60 s, `execution_timeout` 5 min; maintenance as a daily DAG; job metrics from Airflow, or the Pushgateway feeding Amazon Managed Prometheus.

## 10. Repository layout

```
cmd/etl/                 main, service and job subcommands (publish, maintenance)
internal/
  coingecko/             HTTP client, rate limiter, Retry-After
  fetcher/ exporter/ publisher/ transformer/ maintenance/
  window/                raw window contract: ETL_WINDOW arithmetic, raw and DLQ keys, _SUCCESS
  transform/             pure functions: parse, normalize, clean
  dedup/                 claims and decisions
  event/                 the S1 raw event
  validation/            JSON Schema registry and validators
  storage/s3/ storage/pg/ queue/
  bootstrap/             etl init
  config/ obs/ xtime/    configuration; slog, metrics, health, Pushgateway push; UTC timestamps
  testutil/              throwaway containers for integration tests
schemas/                 S0–S3 JSON Schemas (embedded), testdata fixtures
migrations/              goose SQL
deploy/                  prometheus (rules), grafana (dashboard), redpanda (bootstrap), cronicle (entrypoint, provisioning)
docs/                    ADRs, architecture page; superpowers/: this spec and the implementation plans
Makefile, docker-compose.yml, Dockerfile, README.md
```

## 11. Testing

1. **Unit:** table-driven tests of `transform` on recorded live CoinGecko responses — normalization, flags, rejects, id-less keys, time and number formatting.
2. **Schema contracts:** compile, fixture expectations, struct ↔ schema drift, live samples pass S0, fuzzing.
3. **Delivery guarantees (integration, testcontainers: Postgres, Redpanda, versitygw):** crash after TX1, after partial PUTs, zombie consumer, replay; keys from the manifest `planned_at` and new keys after a topic re-creation; assert no loss and no duplicates in processed. Dedup: concurrent claims of one key, overlapping key sets in opposite order without deadlock.
4. **Exporter:** parts then `_SUCCESS`, empty windows, missed windows closed in order after downtime, a late commit goes to the next window, a crash after the claim, the part PUT or the `_SUCCESS` PUT rewrites the same bytes, an open transaction does not stall export, concurrent claims serialize on their window, a partition created after a claim, a stuck part keeps its window open while the lag grows, the bootstrap window (also over a kept lake), a window-size change, keys independent of the local time zone (the exporter and transformer packages run in a +05:30 zone).
5. **Publisher job:** the window from `--now` (catch-up included), `_SUCCESS` checks (window mismatch included), not ready then ready then rerun, sha256 and line-count mismatch, invalid lines to the window's DLQ object, an empty window, a non-zero exit when S3 is unreachable, and a run stopped by SIGTERM still pushing its failure.
6. **Maintenance:** retention drops only partitions up to the cutoff, never raw partitions, deletes only old `written` batches, is idempotent and retries a busy partition; vacuum covers the partitioned tables and survives the database `transaction_timeout`; configuration refuses a claims retention below the topic retention + 1 day.
7. **Scheduler and alerts:** Cronicle provisioning (`node --test`, `make cronicle-test`, which also checks that compose hands one `ETL_WINDOW` to exporter, transformers and scheduler); alert rules with promtool (`make alerts-test`).
8. `go test -race`; GitHub Actions CI: lint, tests, promtool, provisioning tests, both image builds.

## 12. README outline

Overview with a map from each assignment requirement to its section, diagram and the services and jobs tables; Key decisions with their costs and rejected alternatives; why the Kafka protocol and why Redpanda (replayable log, keyed partitions, consumer groups behind a batch publisher, DE ecosystem; Redpanda for the local stack: same protocol and client code, no JVM; Apache Kafka / MSK / Confluent Cloud drop-in for production; NATS JetStream and Pulsar viable with caveats; RabbitMQ classic queues, Redis Streams, SQS, Pub/Sub rejected); quick start (`make up`, `make demo`, `make clean`; the main variables; changing `ETL_WINDOW`; a kept lake); scheduled jobs: publishing and maintenance (Cronicle UI and events, which window a run publishes, rerunning one window or a range, the Pushgateway, retention and VACUUM); browsing the lake (plain files, S3 CLI, Console, Grafana; how long each store keeps data); data model and time model, with the raw window contract; schemas and validation; delivery guarantees (the conditions of exactly once) and crash table, with resolving a halted partition; metrics; alerts; logs; running the image with `docker run -v`; assumptions (assignment ambiguities and our interpretation); productionization:
- AWS: S3 (raw/processed zones, lifecycle, Parquet + Glue catalog / Athena or Iceberg), RDS/Aurora Postgres with pg_partman, MSK, ECS/EKS, Amazon MWAA for the publish and maintenance jobs, Secrets Manager, Managed Prometheus + Grafana, CloudWatch.
- Scalability: per-coin messages from a markets response, more partitions and transformer replicas, exporter scale-out (drop the advisory lock, an export lease per claimed part), per-partition autovacuum for the unexported-rows index, compaction to Parquet partitioned by event date, Debezium CDC instead of the exporter, Kafka Streams + Kafka Connect S3 Sink as a JVM alternative, schema registry with BACKWARD compatibility, strict window order.
- Reliability: replication factor 3, `min.insync.replicas=2`, pg_partman retention for `raw.api_calls` and claims (30 days), paid CoinGecko plan; records can outlive `retention.ms` by up to `segment.ms`, so consumer-group resets and replays belong within the claims retention.

Repository layout and tests; CoinGecko attribution.
