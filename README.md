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
    P -->|"key = coins_markets / coin id"| K[["Redpanda<br/>coingecko.raw<br/>6 partitions · TTL 7 days"]]
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
| `etl exporter` | Every ~15 s claims the next range of new rows (up to 500) and writes one raw JSONL object. | 1 (Postgres advisory lock) |
| `etl publisher` | Leases exported batches, reads the object through S3, validates and produces each line. | 1 (leases allow more) |
| `etl transformer` | Consumer group `coingecko-transformer`. Validates, cleans, deduplicates and normalizes; writes processed, rejected and dlq objects. | 2 |
| `etl healthcheck` | Exit code for container healthchecks (the distroless image has no curl). | — |

Infrastructure: Postgres 18.6, Redpanda v26.2.4, versitygw v1.8.0 (S3 gateway over plain files), Redpanda Console v3.12.0, Prometheus v3.15.0 and Grafana 13.2.3.

Why this chain and not a shorter one? Fetcher → broker directly would be simpler, but the assignment asks for raw data in Postgres first. Debezium CDC (Postgres → Kafka in commit order) is the production choice for the Postgres → broker leg, but it adds a JVM Kafka Connect stack. Kafka Streams would keep dedup state in RocksDB with a changelog, but it is JVM-only and its exactly-once stops at the Kafka boundary. All three are listed under [Productionization](#productionization).

An interactive version of this architecture, with every stage and decision explained, is in [`docs/architecture/index.html`](docs/architecture/index.html) (open the file in a browser).

### Key decisions

| Topic | Decision | Alternatives rejected |
|---|---|---|
| Object storage | versitygw: plain files behind the S3 API, write-once with `If-None-Match: *` ([ADR 0001](docs/adr/0001-object-storage-versitygw.md)) | MinIO (archived, images no longer pullable); `rclone serve s3` (ignores `If-None-Match`); direct file writes (not the production path) |
| Raw table | one row per HTTP call, failures included | a per-entity raw table: a write path nobody reads |
| Event | a source snapshot | poll time (every CDN repeat would be an event) |
| Deduplication | one step, by natural record key, in the transformer ([ADR 0002](docs/adr/0002-single-dedup-in-transformer.md)) | a second, call-level dedup by `idempotency_key`; an in-memory cache (lost on rebalance) |
| Pipeline state | Postgres schema `pipeline` ([ADR 0004](docs/adr/0004-postgres-pipeline-state.md)) | broker offsets, S3 alone, Kafka Streams state stores, goka |
| Exporter cursor | xid8 range below the oldest open transaction: stalls, never skips | an identity watermark (loses rows on out-of-order commits); an `exported` flag with `FOR UPDATE SKIP LOCKED` (updates every raw row and still needs a claimed batch for byte-identical retries, see [below](#delivery-guarantees)); Debezium CDC (production option) |
| Broker | Kafka protocol, Redpanda locally ([ADR 0003](docs/adr/0003-redpanda.md)) | see the [broker table](#why-the-kafka-protocol-and-why-redpanda) |
| Schemas | JSON Schema files in the repo, embedded in the binary | a schema registry (production note) |
| DLQ | a bucket, not a topic: two of its writers run before the broker | a dlq topic outside the batch manifest |
| Logs | one file per replica | one shared `logs/etl.log` (several writers) |
| Cleaning | reject empty records, flag questionable ones | dropping questionable rows (stale coins would vanish from the series) |
| Numbers | money and quantities as decimal strings | JSON numbers (values exceed 2^53) |

## Why the Kafka protocol, and why Redpanda

The design needs three things from the broker:

- a **replayable log**: after a schema fix, raw data is republished and reprocessed; a consumer crash resumes from a committed offset;
- **per-key ordering**: every repeat of the same data (same endpoint or coin) lands on one partition, so deduplication decisions are made sequentially and deterministically;
- **consumer groups**: transformer replicas share partitions and take over each other's work on failure.

The Kafka protocol provides all three, and it is the de-facto standard for data engineering at this scale: connectors, stream processors, lakehouse sinks and managed offerings all speak it.

Redpanda is used for the local stack because it speaks the same protocol as a single C++ binary without a JVM: it starts in about two seconds, uses about 200 MiB of memory in this stack and needs no separate controller. The Go client ([franz-go](https://github.com/twmb/franz-go)) does not know which broker it talks to, so the same code runs unchanged against Apache Kafka, Amazon MSK or Confluent Cloud in production. The main thing Redpanda lacks is the Kafka Connect ecosystem, and this project does not use Kafka Connect. Redpanda's licence (BSL 1.1) allows this use.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen for the local stack: same protocol and client code, no JVM, lighter and faster to start |
| Apache Kafka, Amazon MSK, Confluent Cloud | Drop-in for production: the code speaks the Kafka protocol |
| NATS JetStream, Apache Pulsar | Viable with caveats: a different consumer model (JetStream) or heavier operations (Pulsar, with BookKeeper), and a weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, Amazon SQS, Google Pub/Sub | Rejected: no offset-based replay or per-key ordering at scale |

## Quick start

Prerequisites: Docker with Compose v2 (Docker Desktop on macOS works), GNU make, curl. About 1.2 GB of free RAM.

```bash
make up      # creates ./data ./logs ./.s3meta, copies .env.example to .env if missing, builds and starts everything
make demo    # make up, waits 150 s, then prints object/record counts per bucket, the duplicate share and links
make down    # stops the stack; data, logs and named volumes are kept
```

`make demo-report` prints the same summary again without touching the stack. Output from a real run (the duplicate share is printed unrounded):

```
bucket      objects    records
raw              24         26
processed        38       2268
rejected          7          7
dlq               0          0
duplicate share (dropped / (dropped + written)): 0.3121098626716604
Grafana:          http://localhost:3000  (dashboard: ETL / CoinGecko ETL)
Redpanda Console: http://localhost:8081
Prometheus:       http://localhost:9090/alerts
Lake on disk:     /path/to/coingecko-etl/data/{raw,processed,rejected,dlq}
```

A duplicate share around one third to one half is expected: CoinGecko's CDN caches responses for 60 s and the fetcher polls every 30 s, so roughly every second markets response is byte-identical to the previous one.

Configuration lives in `.env` (see `.env.example`): an optional `COINGECKO_API_KEY` (sent as `x-cg-demo-api-key`; the keyless tier works at the default ~3 calls per minute), host ports, polling intervals and cleaning thresholds. The containers run as your user so that the files under `./data`, `./logs` and `./.s3meta` belong to you: `make up` exports your uid and gid; when you call `docker compose` directly on Linux, set `ETL_UID` and `ETL_GID` in `.env`.

To wipe everything and start from scratch: `docker compose down -v`, then `rm -rf data logs .s3meta`.

## Browsing the lake

The S3 gateway (versitygw, posix backend) stores every bucket as a directory and every object as a plain file under `./data`. Object metadata (content type, the `sha256` we attach) lives separately in `./.s3meta`, because Docker Desktop bind mounts do not support extended attributes.

```
data/raw/source=coingecko/ingest_date=2026-10-10/batch=00000000000000000042.jsonl
data/processed/dataset=ticker/v=1/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl
data/rejected/dataset=coin_contract/event_date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
data/dlq/stage=transform/date=2026-10-10/coingecko.raw+0+00000000000000000815.jsonl
```

Processed, rejected and transformer dlq objects are named after topic, partition and the first offset of their batch (the Confluent S3 sink convention); the exporter and publisher name their dlq objects `export+batch+<batch id>` and `publish+batch+<batch id>`.

Five ways to look at it:

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
| `/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&price_change_percentage=1h,24h,7d&sparkline=false&precision=full` | every 30 s | a flat 30-second time series of the top 100 coins |
| `/coins/{id}?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false` | every 60 s, rotating over bitcoin, ethereum, tether, usd-coin, 15 s after the markets call | deep nesting: 23 maps × 63 currencies, 100 tickers with nested objects, contracts per chain |

The keyless CoinGecko tier allows a dynamic 10–30 calls per minute per IP; we saw HTTP 429 with `Retry-After: 60` on the sixth call within 27 s. The fetcher therefore spends ~3 calls per minute (at least 15 s between calls), keeps one request in flight with a 10 s timeout per request, honours `Retry-After` (capped at 15 minutes, since the header is untrusted), backs off exponentially with jitter on 5xx and network errors, and skips ticks instead of queueing them.

### Raw zone

`raw.api_calls` in Postgres has one row per HTTP call, including failures (`outcome` is `ok`, `http_error` or `network_error`). The response body is stored in a `json` column (not `jsonb`), which keeps the exact bytes, so `payload_sha256` can be re-verified at every hop. The fetcher stores the body as received, minus surrounding whitespace; a body with line breaks between tokens is stored compacted (insignificant whitespace removed, strings and numbers untouched), so one call is always one JSONL line and one message, and `payload_sha256` is the hash of the stored bytes. Each row also carries `idempotency_key = sha256(source | endpoint | canonical params | payload_sha256)`; two polls that received the same cached response share the key. The table is partitioned by day on `fetched_at`.

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
- **Reject** to `s3://rejected` with a reason: `missing_id`, `null_price`, `nonpositive_price`, `empty_platform` (for example bitcoin's `platforms: {"": ""}`), `empty_record` (all metrics null), `snapshot_conflict` (see below; it also catches two tickers of one response that share a key with different values).
- **Flag and keep:**
  - `is_stale`: `snapshot_at − source_updated_at > 10 min`; for tickers, the upstream flag or a last trade older than 1 h;
  - `is_inactive`: zero volume;
  - `is_anomaly`: price outside `[low_24h, high_24h]`; for tickers, the upstream flag or `last_usd` more than 2% away from the coin's median over all its tickers in the snapshot. For `market_snapshot` the range is widened by `ETL_ANOMALY_TOLERANCE_PCT` (default 0.5%) on each side, `price < low_24h × (1 − t/100)` or `price > high_24h × (1 + t/100)`, because with `precision=full` the price carries more digits than the rounded 24 h bounds (USYC `1.140213` against `[1.14, 1.14]`; strict, 69 of 700 rows of a live run were flagged); `0` restores the strict check. `coin_market_by_currency` stays strict: `/coins/{id}` sends price and bounds at one precision.

The thresholds are configuration (`ETL_STALE_AFTER`, `ETL_TICKER_STALE_AFTER`, `ETL_TICKER_ANOMALY_PCT`, `ETL_ANOMALY_TOLERANCE_PCT`).

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
| fetcher, before INSERT | S1 envelope | ERROR log (a bug on our side); a persistent failure trips the stale-data alert |
| exporter, publisher | S1 | the line goes to `s3://dlq` (class `decode_error` or `schema_violation`), not downstream |
| transformer, input | S1, payload checksum, then the payload against S0 | `s3://dlq` with class `decode_error`, `schema_violation`, `integrity_error`, `source_schema_violation` or `transform_error` (a payload that passes S0 but cannot be transformed) |
| transformer, output | S2 | `s3://dlq` with class `output_schema_violation`; never written to processed |
| writing rejected / dlq records | S3 | not written; ERROR log with the original bytes in base64 |

`rejected/` is for records removed by a cleaning rule; `dlq/` means "something broke and a human is needed". After a schema fix, the raw objects are republished and reprocessed: a row that went to `dlq/` never claimed its deduplication key, so the republished copy is written, while the rows of the same message that were already written are dropped as replays.

**Schema evolution.** Because schemas ship inside the binary, a schema change is a redeploy: consumers (transformer) first, then producers. A compatible change edits `<name>.v1.json` in place. The compatibility gate is the fixture contract test (`TestSchemaFixtures`, run by CI): the valid fixtures under `schemas/testdata/` are append-only and form the corpus every in-place change must keep accepting, together with the recorded CoinGecko responses that must keep passing S0. A breaking change, one that would reject an existing valid fixture, adds `<name>.v2.json` side by side. The topic stays `coingecko.raw`: the version travels in every message (`schema_version` and the `schema` header), so a v2-aware transformer dispatches on it.

## Delivery guarantees

Every hop is at-least-once. The processed zone nevertheless contains each record exactly once, because every step can be repeated safely: deterministic object keys, ledgers in Postgres, and write-once objects (`PUT` with `If-None-Match: *`). **Deduplication happens once, in the transformer; idempotency happens at every write.**

| Hop | Mechanism | On retry |
|---|---|---|
| fetcher → `raw.api_calls` | primary key `(event_id, fetched_at)`, `ON CONFLICT DO NOTHING` | at most one missed poll |
| exporter → `s3://raw` | xid8 cursor + `pipeline.export_batches` ledger; `If-None-Match: *` | same object: 412, checksums equal |
| publisher → broker | lease from the ledger; idempotent producer, `acks=all` | copies with the same `event_id`, dropped by the transformer |
| transformer → `s3://processed` | batch manifest; write-once objects; offset commit after the write | same objects: 412, checksums equal |

**Exporter cursor.** An identity-column watermark loses rows when transactions commit out of order. The exporter reads rows by `(ingest_txid, seq)` bounded by `pg_snapshot_xmin(pg_current_snapshot())`, so it can only stall behind a long transaction, never skip a row. Each range is first claimed in `pipeline.export_batches`, which fixes the object key and the row set, so a retry after a crash writes the same bytes.

The rejected alternative is an `exported` flag on `raw.api_calls` with `SELECT … WHERE NOT exported … FOR UPDATE SKIP LOCKED LIMIT n`. It never loses a late-committing row and never stalls, but it UPDATEs every raw row (the raw table stops being append-only: dead tuples, vacuum, a partial index), and a crash between the PUT and the flag update lets the retry pick a different row set, so a byte-identical retry still needs a claimed batch: the ledger this design already has, at one row per batch instead of one update per raw row.

**Deduplication.** Every record bound for `processed/` or `rejected/` claims its natural key in `pipeline.dedup_claims` (`INSERT … ON CONFLICT DO NOTHING`); the first claimant wins:

| The claim for the key points to | Action |
|---|---|
| this record's own position (partition, offset, ordinal) | write it (first sight, or a redelivery after a crash) |
| another position, same content hash | drop as a duplicate |
| another position, different content hash | write to `rejected/` as `snapshot_conflict` |

The content hash covers business fields only, so the same record arriving in two messages matches even though its lineage columns differ. Broker offsets cannot answer "was key X seen?", S3 can only guard single objects, and in-memory state is lost on rebalance, which is why the claims live in Postgres.

**Transformer batch.** A batch is a contiguous offset range of one partition. (1) Input checks without the database. (2) A short transaction records the range in `pipeline.consumer_batches` (`planned`), validates the rows against their dataset schema (a row that fails goes to `dlq/` without claiming its key), claims the keys of the other rows, decides, and stores the object keys and checksums. (3) Objects are written with `If-None-Match: *`. (4) The batch is marked `written`. (5) The offset is committed. On partition assignment the manifest decides: `written` → skip past it, `planned` → reprocess exactly that range.

If a key already holds different bytes (412 with another checksum), or a re-planned batch would store other checksums than the manifest, the transformer halts that partition in place: it stops fetching it, commits nothing, keeps reporting its growing lag, keeps the other partitions and `/metrics` running, and `ETLObjectChecksumMismatch` fires until a human resolves the conflict and restarts the transformer.

| Crash point | Recovery | Loss | Duplicates |
|---|---|---|---|
| before the first transaction commits | the batch starts over | no | no |
| after the transaction, before any PUT | manifest `planned` → same range; claims point to the same positions → same decisions, same bytes | no | no |
| after some PUTs | remaining PUTs succeed; finished ones return 412 with an equal checksum | no | no |
| after marking `written`, before the offset commit | manifest `written` → seek past the batch, commit | no | no |
| a zombie consumer and the new owner process the same range | same range from the manifest, identical bytes; the first PUT wins, the second gets 412 with an equal checksum; the zombie's offset commit is rejected | no | no |

The naive rule "drop if the key was already claimed" would lose every record of a batch that crashed between its transaction and the PUT; claims that remember their position avoid that.

Invariants: transforms are pure (no clock, no random order); `dedup_claims` and `consumer_batches` must outlive the topic's 7-day retention, so that a replay within retention is still deduplicated (nothing in this stack deletes them; a production retention job would keep at least 30 days); Postgres runs with `idle_in_transaction_session_timeout = 60s` and `transaction_timeout = 5min`, so a stuck transaction cannot stall the exporter forever.

## Metrics

Every long-running service listens on `:8080` with `/healthz` (the process is alive), `/readyz` (its dependencies answer: Postgres for all, S3 for exporter, publisher and transformer, the broker for publisher and transformer) and `/metrics`. Prometheus discovers the containers through Docker DNS, including both transformer replicas. All pipeline metrics start with `etl_`.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `etl_api_requests_total` | counter | endpoint, status | CoinGecko calls by HTTP status, `network_error`, or `skipped`: tick skipped (limiter cooldown or a request still in flight); skipped ticks are not errors |
| `etl_api_request_duration_seconds` | histogram | endpoint | API latency, body read included |
| `etl_api_rate_limited_total` | counter | — | HTTP 429 responses |
| `etl_last_success_timestamp_seconds` | gauge | endpoint | Unix time of the last successful call; freshness = `time() - this` |
| `etl_source_data_age_seconds` | gauge | endpoint | how old CoinGecko's data was when we fetched it (`fetched_at` minus the newest `last_updated`) |
| `etl_schema_validations_total` | counter | stage, schema, result | validations per stage and schema, `result` is `pass` or `fail`; the S0 failure share is the drift signal |
| `etl_schema_violations_total` | counter | stage, schema, keyword | which JSON Schema keyword failed (`required`, `type`, …) |
| `etl_export_lag_seconds` | gauge | — | age of the oldest successful raw call not yet exported (0 when caught up); grows when a long transaction blocks the cursor |
| `etl_export_batches_total` | counter | result | exporter batches: `exported` or `error` |
| `etl_s3_put_total` | counter | result | conditional PUTs: `created`, `exists` (an idempotent retry), `mismatch` (must never happen), `error` |
| `etl_published_records_total` | counter | — | messages acknowledged by the broker |
| `etl_records_total` | counter | dataset, outcome | transformer output: `written`, `duplicate`, `rejected`, `dlq` |
| `etl_dedup_dropped_total` | counter | reason | duplicates removed: `replay` (same event redelivered), `duplicate_response` (same API response polled twice), `duplicate_record` (same record in another snapshot) |
| `etl_rejected_total` | counter | reason | records removed by cleaning rules |
| `etl_dlq_total` | counter | stage, error_class | broken records |
| `etl_consumer_lag_records` | gauge | partition | high watermark minus the next offset to process (the committed position); keeps growing while a transformer is stuck on a batch |
| `etl_batch_duration_seconds` | histogram | — | transformer batch time (transaction, PUTs, mark) |
| `etl_e2e_latency_seconds` | histogram | — | from `fetched_at` to the processed object being written |

The transformer's record, duplicate, reject, dlq, latency and batch-duration metrics are counted once per batch, when it is marked `written`, so a retried batch does not count twice; schema validation counters count every attempt. The publisher also exposes franz-go client metrics (`etl_publisher_*`, via kprom), and every service exposes the standard Go runtime and process metrics. Labels stay low-cardinality: no coin ids or offsets.

The Grafana dashboard shows records per stage, data freshness and end-to-end latency, duplicate share, rejects by reason, DLQ records, consumer lag, API status codes, export lag with S3 PUT results, and schema validations.

## Alerts

The rules are in `deploy/prometheus/alerts.yml` and have unit tests (`make alerts-test` runs `promtool`). Open <http://localhost:9090/alerts> to see their state.

Nine rules; the severity is `warning` unless marked critical.

| Alert | Fires when | Likely cause |
|---|---|---|
| `ETLDataStale` (critical) | no successful CoinGecko call on an endpoint for more than 2 minutes, or no successful markets call yet | rate limiting, network, CoinGecko outage, fetcher down |
| `ETLDeadLetterRecords` | any record reached `dlq/` in the last 10 minutes | a bug, a broken message, an integrity failure |
| `ETLSourceSchemaDrift` (critical) | more than 5% of CoinGecko responses fail the source schema over 15 minutes | CoinGecko changed its response format |
| `ETLExportLagHigh` | the exporter's lag is above 5 minutes, or the exporter reports no lag at all (only the exporter's series counts: every service exports the gauge, the others at 0) | a long Postgres transaction, S3 failures, exporter down |
| `ETLConsumerLagGrowing` | consumer lag has grown for 10 minutes | transformers too slow, a batch that keeps failing, a halted partition |
| `ETLServiceDown` (critical) | for 2 minutes a pipeline service fails its scrape (`up == 0`) or has no scrape target at all (Prometheus finds the containers through Docker DNS) | the container is stopped, crash-looping or hung |
| `ETLPipelineNotProgressing` | CoinGecko calls succeed, but for 5 minutes no transformer counted a record (written, duplicate, rejected or dlq) over the last 15 minutes | a stopped or failing publisher (for example the broker is down), a stalled exporter, transformers that cannot write |
| `ETLObjectChecksumMismatch` (critical) | an object key already holds different bytes, or a re-planned transformer batch differs from its manifest; fires at once and stays up | the lake (`./data`, `./.s3meta`) was kept while Postgres and Redpanda were reset (or the reverse), a non-deterministic transform, or a foreign writer; the transformer halts that partition, the exporter retries the batch every 15 s |
| `ETLFrequentRateLimiting` | 3 or more HTTP 429 responses in 15 minutes | another client on the same IP, or a too aggressive schedule |

Alertmanager is not part of the stack; in production the rules route to on-call through Alertmanager or Grafana alerting.

## Logs

Every service logs JSON through Go's `log/slog`, in UTC, to stdout (`docker compose logs -f fetcher`) and to one file per replica: `logs/<service>/etl.log`. The two transformer replicas share the `./logs` mount, so each writes its own file, `logs/<container id>/transformer/etl.log` (the short container id that `docker ps` shows; a recreated container starts a new directory), and no file ever has two writers. Lines carry `service`, `stage` and, where relevant, `event_id` and `batch`. They cover API success and failure, transformation errors and every object saved.

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
- **"`logs/etl.log`"**: one file per replica, `logs/<service>/etl.log`, and `logs/<container id>/transformer/etl.log` for each transformer replica, because several containers cannot safely append to one file.
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

- **More partitions, finer keys.** Today one markets response is one message, keyed `coins_markets`, so markets data goes to one partition, and the five keys land on four of the six partitions. Splitting responses into per-coin messages (100 keys) spreads the load over all partitions; the partition count can then grow beyond 6 and the transformer can run more replicas (up to one per partition).
- **Compaction to Parquet.** Small JSONL objects suit a demo; production adds a periodic compaction into partitioned Parquet (or Iceberg) for analytics.
- **Debezium CDC instead of the exporter.** Change data capture streams `raw.api_calls` to Kafka in commit order without a cursor table; the raw S3 objects would then be produced by a sink.
- **JVM alternative.** Kafka Streams (dedup state in RocksDB with `exactly_once_v2`) plus the Kafka Connect S3 Sink is the standard JVM way to build the same pipeline.
- **Schema registry.** With more producers and consumers, the embedded JSON Schemas move to a registry (AWS Glue Schema Registry or a Confluent-compatible one) with BACKWARD compatibility checks.
- **Dedup claims at volume.** One Postgres round trip per record inside a short transaction is fine here; at higher volume the claims can move to a key-value store with conditional writes (e.g. DynamoDB).

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
internal/config/        environment → Config
internal/coingecko/     HTTP client, rate limiter, Retry-After
internal/event/         raw event envelope (S1), idempotency key
internal/fetcher/ exporter/ publisher/ transformer/
internal/transform/     pure transforms: parse, normalize, clean, flag
internal/dedup/         claims and decisions
internal/validation/    JSON Schema registry
internal/storage/pg/ storage/s3/ queue/
internal/obs/           slog logger, metrics, health server
internal/xtime/         UTC timestamp formatting and parsing
internal/testutil/      testcontainers helpers (Postgres, Redpanda, versitygw)
schemas/                JSON Schemas S0–S3 and test fixtures (recorded CoinGecko responses)
migrations/             goose SQL migrations
deploy/                 Prometheus config and alerts, Grafana provisioning and dashboard, Redpanda bootstrap
docs/                   design spec, implementation plan, architecture page and decision records (docs/adr/)
```

```bash
make test-unit    # unit tests, no Docker
make test         # all tests with -race; integration tests start Postgres, Redpanda and versitygw containers
make lint         # gofmt + go vet (CI also runs golangci-lint)
make alerts-test  # promtool: Prometheus config and alert rule unit tests
```

Integration tests use testcontainers and skip themselves when Docker is unavailable (and under `-short`). They cover crashes after the manifest transaction and after partial PUTs, a zombie consumer, object storage going down mid-batch, publisher replays and out-of-order Postgres commits, and assert no loss and no duplicates in the processed zone. CI (GitHub Actions) runs lint, the full test suite with Docker, the deploy-config checks and the image build.

Architecture decisions are recorded in [`docs/adr/`](docs/adr/).

---

Data provided by [CoinGecko](https://www.coingecko.com/en/api).
