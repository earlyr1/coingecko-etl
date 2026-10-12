# CoinGecko ETL

A small but complete ETL pipeline in Go. It polls the public CoinGecko API, keeps every raw API call in Postgres, exports successful calls in 5-minute windows (JSON Lines plus a `_SUCCESS` manifest) to an S3-compatible lake, publishes each closed window to a Kafka-protocol broker (Redpanda) with a scheduled job, and writes cleaned, deduplicated and normalized JSON Lines to a processed zone. Every hop after the insert into Postgres is at-least-once; the processed zone still contains every stored record exactly once, because every write is idempotent and deduplication happens in one place, under the conditions in [Delivery guarantees](#delivery-guarantees).

Everything runs locally with `docker compose`. The lake is plain files under `./data/`, so you can `cat` any object.

Data provided by [CoinGecko](https://www.coingecko.com/en/api).

**Where each assignment requirement is met** (how open points were read: [Assumptions](#assumptions)):

| Requirement | Where |
|---|---|
| Extract from a public REST API every 30 s and store it in Postgres | `etl fetcher` → `raw.api_calls`: [Source](#source), [Raw zone](#raw-zone) |
| Export the raw data to local storage as JSON | `etl exporter` → `./data/raw`, in windows: [The raw window contract](#the-raw-window-contract) |
| Transform: drop unneeded fields, normalize, timestamps in UTC ISO 8601, a separate output | `etl transformer` → `./data/processed`: [Processed datasets](#processed-datasets), [Formats](#formats), [Cleaning rules](#cleaning-rules) |
| Raw under `data/raw/`, processed under `data/processed/`, appended instead of overwritten | write-once objects: [Browsing the lake](#browsing-the-lake), [Delivery guarantees](#delivery-guarantees) |
| Log API success and failure, transformation errors and saved data to `logs/etl.log` with Go's logging package | [Logs](#logs) |
| Health and Prometheus metrics endpoints, with the metrics explained | [Metrics](#metrics), [Alerts](#alerts) |
| A Dockerfile, the package running in a container, volume mounts for persistence | [Quick start](#quick-start), [Running the image with `docker run -v`](#running-the-image-with-docker-run--v) |
| Productionization with cloud services, scalability and reliability | [Productionization](#productionization) |

## Contents

1. [Architecture](#architecture)
2. [Why the Kafka protocol, and why Redpanda](#why-the-kafka-protocol-and-why-redpanda)
3. [Quick start](#quick-start)
4. [Scheduled jobs: publishing and maintenance](#scheduled-jobs-publishing-and-maintenance)
5. [Browsing the lake](#browsing-the-lake)
6. [Data model and time model](#data-model-and-time-model)
7. [Schemas and validation](#schemas-and-validation)
8. [Delivery guarantees](#delivery-guarantees)
9. [Metrics](#metrics)
10. [Alerts](#alerts)
11. [Logs](#logs)
12. [Running the image with `docker run -v`](#running-the-image-with-docker-run--v)
13. [Assumptions](#assumptions)
14. [Productionization](#productionization)
15. [Repository layout and tests](#repository-layout-and-tests)

## Architecture

```mermaid
flowchart LR
    CG[CoinGecko API] -->|"/coins/markets every 30 s<br/>/coins/{id} every 30 s, one coin in turn"| F[etl fetcher]
    F -->|"INSERT, one row per HTTP call"| PG[("Postgres<br/>raw.api_calls")]
    PG -->|"claim rows (FOR UPDATE SKIP LOCKED)<br/>+ pipeline.export_windows, export_batches"| E[etl exporter]
    E -->|"window parts, then _SUCCESS<br/>PUT If-None-Match: *"| RAW[("s3://raw<br/>./data/raw")]
    S[Cronicle] -. "etl publish --now, every 5 min (+1 min)" .-> P["etl publish<br/>(job)"]
    RAW -->|"parts listed in _SUCCESS"| P
    P -->|"key = idempotency_key"| K[["Redpanda<br/>coingecko.raw<br/>6 partitions · TTL 7 days"]]
    P -. push .-> PGW[Pushgateway]
    S -. "maintenance retention / vacuum, daily" .-> M[etl maintenance]
    M -. "DROP old claim partitions, VACUUM" .-> PG
    K -->|consumer group| T["etl transformer x2<br/>validate input, transform and clean,<br/>validate output, dedup"]
    T -->|claims + batch manifest| PG
    T --> PROC[("s3://processed<br/>./data/processed")]
    T --> REJ[("s3://rejected<br/>./data/rejected")]
    T --> DLQ[("s3://dlq<br/>./data/dlq")]
```

Every process is a subcommand of one Go binary, `etl`, built into one container image. The services, the one-shot setup and the healthcheck helper:

| Command | What it does | Runs as |
|---|---|---|
| `etl init` | Idempotent setup: Postgres migrations (goose) and daily partitions, the broker topic (created via `kadm`, verified if it exists), the four S3 buckets. Retries each dependency for up to 60 s. | one-shot, before the services start |
| `etl fetcher` | Pure transport. Polls CoinGecko through a rate limiter, wraps each HTTP call in an event envelope with an idempotency key and inserts it into `raw.api_calls`, failures included. | service, 1 replica |
| `etl exporter` | Closes each `ETL_WINDOW` window: claims the window's unexported rows, writes the parts and then `_SUCCESS`. Checks every `ETL_EXPORT_INTERVAL` (15 s) for a window that is due. | service, 1 replica (Postgres advisory lock) |
| `etl transformer` | Consumer group `coingecko-transformer`. Validates its input, transforms it (normalize, clean, flag), validates the output and deduplicates; writes processed, rejected and dlq objects. | service, 2 replicas |
| `etl healthcheck` | Exit code for container healthchecks (the distroless image has no curl). | helper, inside each service's healthcheck |

Jobs, run by [Cronicle](#scheduled-jobs-publishing-and-maintenance); exit code 0 is success, 1 a failure, 2 a usage error:

| Command | What it does | When |
|---|---|---|
| `etl publish --now <time>` | Publishes one closed window from S3 to the broker; no Postgres; exits non-zero when the window's `_SUCCESS` is missing | every `ETL_WINDOW`, 1 minute after the window end |
| `etl maintenance retention` | Drops dedup-claim partitions and written transformer batch rows older than `ETL_CLAIMS_RETENTION_DAYS` | daily, 11:00 |
| `etl maintenance vacuum` | `VACUUM (ANALYZE)` of the tables the pipeline updates | daily, 11:30 |

Stages meet only through what they produce: the rows of `raw.api_calls`, raw windows with their `_SUCCESS`, topic messages, and processed objects.

Infrastructure: Postgres 18.6, Redpanda v26.2.4, versitygw v1.8.0 (S3 gateway over plain files), Redpanda Console v3.12.0, Prometheus v3.15.0, Pushgateway v1.11.4, Grafana 13.2.3, and Cronicle v0.9.135 in our own image (Node 24.21.0, Cronicle built from the pinned upstream release, and the `etl` binary).

Raw data lands in Postgres before the broker because the assignment asks for it; the cost is the exporter and its claim ([ADR 0005](docs/adr/0005-exporter-skip-locked-claim.md)). JVM alternatives for the exporter and the transformer (Debezium, Kafka Connect, Kafka Streams) are under [Productionization](#productionization).

An interactive version of this architecture, with every stage and decision explained, is in [`docs/architecture/index.html`](docs/architecture/index.html) (open the file in a browser).

### Key decisions

| Topic | Decision | Cost | Alternatives rejected |
|---|---|---|---|
| Object storage | versitygw: plain files behind the S3 API, write-once with `If-None-Match: *` ([ADR 0001](docs/adr/0001-object-storage-versitygw.md)) | one more container; files must not be edited by hand (the write-once check relies on the gateway's metadata) | direct file writes (not the production path); other S3 servers (see the ADR) |
| Object names | raw by window, processed by processing time plus a hash of the batch's Kafka position; no raw database or broker counters in names ([ADR 0008](docs/adr/0008-processing-time-object-names.md)) | queries by event time scan more files | batch ids and offsets (unique only within one database and topic lifetime); event-date directories |
| File format | JSON Lines | larger files and slower scans than a columnar format | Parquet (a production compaction step) |
| Event | one CoinGecko snapshot (`snapshot_at`), not one poll: a CDN-cached repeat of a response is not a new event | the copies are stored and published, then dropped in the transformer (about a quarter of all records) | poll time (every CDN repeat would be an event) |
| Deduplication | one step, by natural record key, in the transformer ([ADR 0002](docs/adr/0002-single-dedup-in-transformer.md)) | one Postgres claim per record, kept `ETL_CLAIMS_RETENTION_DAYS` | a second, call-level dedup by `idempotency_key`; an in-memory cache (lost on rebalance) |
| Pipeline state | Postgres schema `pipeline` ([ADR 0004](docs/adr/0004-postgres-pipeline-state.md)) | Postgres is on the transformer's write path | broker offsets, S3 alone, in-memory state |
| Exporter claim | rows claimed with `FOR UPDATE SKIP LOCKED` and stamped with their batch id in the same transaction as the ledger row: never skips a late commit, never stalls, a retry rewrites the same bytes ([ADR 0005](docs/adr/0005-exporter-skip-locked-claim.md)) | one UPDATE per raw row: `raw.api_calls` is not strictly append-only | an identity watermark (loses out-of-order commits); an xid8 cursor below the oldest open transaction (Postgres internals, stalls behind long transactions); a separate insert-only `pipeline.export_claims` table (a second table for the exporter's own bookkeeping); Debezium CDC (heavier stack) |
| Raw windows | `ETL_WINDOW` (5 min) windows aligned to the UTC hour, `_SUCCESS` written last; a window is ready when its `_SUCCESS` exists ([ADR 0007](docs/adr/0007-windowed-publisher-cronicle.md)) | a row waits up to `ETL_WINDOW` for its window to close; the size is fixed for the lifetime of a stack | one object per export batch (no completeness signal); listing a prefix (cannot tell late from empty) |
| Publisher | a batch job per closed window, scheduled by Cronicle (Airflow/MWAA in production), reads only S3 ([ADR 0007](docs/adr/0007-windowed-publisher-cronicle.md)) | fetch to processed takes about `ETL_WINDOW` + 1 min; a scheduler to run | a long-running publisher with leases in Postgres; plain cron (no catch-up, history or retries); Airflow locally (heavy) |
| Message key | the event's `idempotency_key`: responses spread over all 6 partitions, copies of one response share a partition ([ADR 0003](docs/adr/0003-redpanda.md)) | no per-coin message order (nothing depends on it) | endpoint or coin id (four of six partitions used, half the traffic on one) |
| Broker | Kafka protocol, Redpanda locally ([ADR 0003](docs/adr/0003-redpanda.md)) | a stateful service to operate; locally one node without replication | NATS JetStream, Pulsar, RabbitMQ, SQS, Pub/Sub (see the ADR) |
| Schemas | JSON Schema files in the repo, embedded in the binary | a schema change is a redeploy | a schema registry (production note) |
| Cleaning | reject empty records, flag questionable ones | readers filter flagged rows themselves | dropping questionable rows (stale coins would vanish from the series) |
| Numbers | money and quantities as decimal strings | readers cast them | JSON numbers (values exceed 2^53) |
| Polling cadence | 4 calls a minute, 15 s apart: markets at :00 and :30, one coin at :15 and :45 in turn | each coin's detail is refreshed only every 2 minutes | all four coins every minute (6 calls a minute, 60% of the worst-case keyless limit of 10, little room for retries); markets every 60 s (breaks "every 30 s") |
| Maintenance | daily daytime Cronicle jobs: drop expired claim partitions, VACUUM ([ADR 0007](docs/adr/0007-windowed-publisher-cronicle.md)) | relies on the scheduler; a failing job is flagged after 26 hours | pg_cron (custom Postgres image); night runs |

Why the DLQ is a bucket and not a topic: [ADR 0006](docs/adr/0006-dlq-bucket.md).

## Why the Kafka protocol, and why Redpanda

The broker must provide a **replayable log** (republish after a schema fix, resume from a committed offset), **keyed partitions** (every copy of one API response carries the same `idempotency_key` and lands on one partition, so one transformer judges the copies in offset order; identical records from different responses, which may sit on different partitions, are settled by the claims in `pipeline.dedup_claims`, see [Delivery guarantees](#delivery-guarantees)) and **consumer groups** (transformer replicas share partitions and take over on failure). The Kafka protocol provides all three. Message order per coin is not kept, and nothing depends on it: records are independent, `snapshot_at` is part of every key, and stale and anomaly flags are computed within one response.

The publisher runs in batches, but the transformer is a consumer group: offsets make its batches replayable and crash-safe, replicas share and take over partitions, and the topic is a 7-day replay buffer independent of the publisher's schedule.

Locally that protocol is served by Redpanda: one binary, no JVM, fast to start. The Go client ([franz-go](https://github.com/twmb/franz-go)) does not know which broker it talks to, so the same code runs against Apache Kafka or Amazon MSK in production. The comparison with other brokers is in [ADR 0003](docs/adr/0003-redpanda.md).

## Quick start

Prerequisites: Docker with Compose v2 (Docker Desktop on macOS works), GNU make, curl. About 1.2 GB of free RAM: that is what the running demo stack uses by `docker stats` (Grafana about 440 MB, Redpanda about 310 MB of the 1 GB it may take).

```bash
make up      # creates ./data ./logs ./.s3meta, writes .env from .env.example if missing, builds and starts everything
make demo    # WIPES the previous stack (make clean), then make up with ETL_WINDOW=1m; waits for the first processed object (at most DEMO_TIMEOUT=300 s) and one more window, then prints the report
make down    # stops the stack; data, logs and named volumes are kept
make clean   # removes the containers and the named volumes, then ./data, ./logs and ./.s3meta; .env is kept
```

With 1-minute windows the first processed records appear about 3 minutes after the containers start: the first window closes, Cronicle publishes it a minute after its end, and the transformer writes it. `make demo-report` prints the summary again without touching the stack. Its output on a real `make demo` stack, run about 20 minutes after `make demo` started it (the duplicate share is printed unrounded):

```
bucket      objects    records
raw              19         76
processed       115       7418
rejected         26        312
dlq               0          0
duplicate share (dropped / (dropped + written)): 0.2299387522059587
raw windows closed (_SUCCESS): 20
last successful publish (s ago): 39
Grafana:          http://localhost:3000  (dashboard: ETL / CoinGecko ETL)
Cronicle:         http://localhost:3012  (jobs etl-publish, etl-retention, etl-vacuum; login admin)
Redpanda Console: http://localhost:8081
Prometheus:       http://localhost:9090/alerts
Lake on disk:     /path/to/coingecko-etl/data/{raw,processed,rejected,dlq}
```

There is one more `_SUCCESS` than raw objects because the first window of a fresh stack is closed empty (see [The raw window contract](#the-raw-window-contract)). Almost all duplicates are markets records. CoinGecko's CDN serves a cached copy for up to 60 s and the fetcher polls every 30 s, so half or more of the markets responses repeat an earlier one byte for byte (two in three on the demo stack, where cached copies were served at ages of 29 to 59 s). Per minute that is 200 markets records, about 130 of them copies, against about 350 coin-detail records (two calls of about 175 rows: the coin, 63 currencies, about 100 tickers and its contracts), which have almost none: 130 / (200 + 350) ≈ 0.24.

Configuration lives in `.env`; `.env.example` is the full reference, with a comment on every variable. The ones you are most likely to change:

| Variable | Default | Effect |
|---|---|---|
| `COINGECKO_API_KEY` | empty | optional Demo key, sent as `x-cg-demo-api-key`; the keyless tier works at the default ~4 calls per minute |
| `ETL_WINDOW` | `5m` (`make demo`: `1m`) | raw window size and publish schedule: whole minutes that divide an hour; fixed for the lifetime of a stack (below) |
| `ETL_EXPORT_INTERVAL` | `15s` | the exporter's tick; must be shorter than 1 minute |
| `ETL_CLAIMS_RETENTION_DAYS` | `8` | days of dedup claims kept; at least the topic retention in days + 1 ([Maintenance](#scheduled-jobs-publishing-and-maintenance)) |
| `ETL_SCHEDULE_TZ` | `UTC` | time zone of the daily maintenance jobs: an IANA name Cronicle knows ([Scheduled jobs](#scheduled-jobs-publishing-and-maintenance)) |
| `CRONICLE_ADMIN_PASSWORD`, `CRONICLE_API_KEY` | `admin`, `etl_local_dev_api_key_0001` | Cronicle login and API key, stored when its volume is created |
| `ETL_MARKETS_INTERVAL`, `ETL_DETAIL_INTERVAL`, `ETL_DETAIL_OFFSET`, `ETL_MIN_REQUEST_SPACING`, `ETL_WATCHLIST` | `30s`, `30s`, `15s`, `15s`, four coins | the polling schedule ([Source](#source)) |
| `ETL_STALE_AFTER`, `ETL_TICKER_STALE_AFTER`, `ETL_TICKER_ANOMALY_PCT`, `ETL_ANOMALY_TOLERANCE_PCT` | `10m`, `1h`, `2.0`, `0.5` | cleaning thresholds ([Cleaning rules](#cleaning-rules)) |
| `*_PORT` | Grafana 3000, Cronicle 3012, Postgres 5432, S3 7070, Console 8081, Prometheus 9090, Pushgateway 9091, Kafka 19092 | host ports, bound to 127.0.0.1 |
| `ETL_UID`, `ETL_GID` | your ids (`make up` writes and passes them) | owner of the files under `./data`, `./logs` and `./.s3meta` |

Each command checks only the variables it reads, with surrounding whitespace trimmed and a blank value taken as the default; an invalid value stops the commands that read it and nothing else. For example, `ETL_WINDOW=7m` stops the exporter, `etl publish` and the Cronicle provisioning, while a claims retention below the topic retention + 1 stops only `etl maintenance retention`. The error names the command: `etl publish: config: …`, `etl maintenance retention: …`.

**`ETL_WINDOW` is fixed for the lifetime of a stack.** Windows closed with one size would never be published by a schedule of another, so the exporter refuses to go on when the newest window in its ledger has another size. Every tick then fails with `export failed` and `exporter: ETL_WINDOW changed from 1m to 5m: windows closed with 1m would never be published; run make clean, or set ETL_WINDOW back to 1m`; `etl_export_lag_seconds` keeps growing and `ETLExportLagHigh` fires. With the old value back, the exporter closes the missed windows in order. To change the size, run `make clean && make up`; that is also the way back from `make demo` to the `.env` value.

**Keeping the lake.** Object names carry the window or the batch's processing time, not database or broker counters, so the lake (`./data`, `./.s3meta`) may be kept when the stack is re-created with the same `ETL_WINDOW` (`docker compose down -v` keeps the bind mounts; `make clean` wipes them). On an empty ledger the exporter reads the `_SUCCESS` at the key of its first window. If an earlier stack closed that same window, the exporter logs `raw window already in the lake; starting at the next window` and starts there, never overwriting it. If the object describes another window (a lake written with another `ETL_WINDOW`) or is not a manifest, the exporter refuses, with `the lake holds raw windows of another ETL_WINDOW: …; use a fresh lake or the same ETL_WINDOW` or `… which is not a raw manifest …; use a fresh lake`. The check reads only that one key, so a lake of another size that holds no object there goes unnoticed; its windows all end before the new stack starts, so nothing collides, but the lake then mixes two sizes. The new scheduler only runs publish times (window end + 1 min) after it started, so no window is published twice. Two consequences: windows the old stack closed but had not published yet are not published automatically (publish them by hand), and with a new Postgres volume the dedup claims start empty, so republishing a window the old stack had already published writes duplicates.

## Scheduled jobs: publishing and maintenance

**Cronicle** at <http://localhost:3012> (login `admin` / `CRONICLE_ADMIN_PASSWORD`, default `admin`) runs the batch jobs. It is the local stand-in for Airflow: one Node process with a web UI, catch-up, retries, timeouts, run history and an API, so the events are provisioned as code at every container start (`deploy/cronicle/provision.js`). Upstream Cronicle is in maintenance mode (bug fixes only; the author's successor project is xyOps), and our image is built from the pinned upstream release.

| Event | Schedule | Runs | Policy |
|---|---|---|---|
| `etl-publish` | every `ETL_WINDOW` at +1 min, UTC (5-minute windows: minutes 1, 6, …, 56; `make demo`: every minute) | `etl publish --now "$JOB_NOW"` | Catch-Up (Run All), one job at a time, 3 retries 60 s apart, timeout 5 min |
| `etl-retention` | daily 11:00 in `ETL_SCHEDULE_TZ` | `etl maintenance retention` | Catch-Up, 3 retries 60 s apart, timeout 30 min; one maintenance job at a time |
| `etl-vacuum` | daily 11:30 in `ETL_SCHEDULE_TZ` | `etl maintenance vacuum` | as above |

Maintenance runs in the daytime on purpose: the pipeline load is flat around the clock, and a failure is seen by people at work. `ETL_SCHEDULE_TZ` (default UTC) places only the maintenance events; `etl-publish` always follows the UTC window grid. `ETL_SCHEDULE_TZ` must be an IANA zone name that Cronicle's bundled tz data (2021e) knows: newer names such as `Europe/Kyiv` are refused (use `Europe/Kiev`), and so are UTC offsets such as `+03:00`. On a bad value, or an invalid `ETL_WINDOW`, the cronicle container logs `provision: FAILED: …` and restarts instead of running without its events. An optional `CRONICLE_WEB_HOOK` receives a POST at the start and end of every job and when a scheduled launch fails; the launch-failure POST carries the whole stored event, `CRONICLE_API_KEY` included, so point it only at a trusted receiver.

One job, four names: the publish event has the id `etlpublish` (API) and the title `etl-publish` (UI), pushes under the Pushgateway job `etl_publisher` and logs to `logs/publisher/etl.log`; likewise `etlretention` / `etl-retention` / `etl_retention` and `etlvacuum` / `etl-vacuum` / `etl_vacuum`, both logging to `logs/maintenance/etl.log`.

**Which window a run publishes.** The window that ends at floor(now − 1 min, `ETL_WINDOW`), where now is `JOB_NOW`, the run's scheduled time rather than the clock, so catch-up runs and reruns pick the right window: with 5-minute windows the 10:06 run publishes 10:00–10:05. A missing `_SUCCESS` means the exporter is behind or down: the run fails, Cronicle retries it three times a minute apart, and then it stays red. A run also fails when the `_SUCCESS` describes another window (the exporter and the job disagree on `ETL_WINDOW`). A red run is not re-run automatically after its retries, and no alert fires as long as later windows succeed: re-run it as below.

**Rerunning windows.**

- Finding them: failed runs are the red rows of Cronicle's **Completed** tab. To list every window the exporter closed that no run has published yet (red runs, and windows an earlier stack closed but never published when the lake was kept; the newest window may simply not be due yet):
  ```bash
  comm -23 <(jq -r 'select(.msg == "raw window closed") | .window_start' logs/exporter/etl.log | sort -u) \
           <(jq -r 'select(.msg == "window published") | .window_start' logs/publisher/etl.log | sort -u)
  ```
- One window, in the UI: open the failed job from the **Completed** tab and click **Run Again**; it reuses the job's `JOB_NOW`, so it publishes the same window.
- One window, through the API (`CRONICLE_API_KEY`, default `etl_local_dev_api_key_0001`; `now` is the window end plus 60 s, in Unix seconds; this one publishes 2026-10-12 10:00–10:05 with 5-minute windows):
  ```bash
  curl -s -X POST -H "X-API-Key: $CRONICLE_API_KEY" -H 'Content-Type: application/json' \
    -d '{"id":"etlpublish","now":1791799560}' http://localhost:3012/api/app/run_event/v1
  ```
  Both keep the one-job-at-a-time limit, so `logs/publisher/etl.log` has one writer: while a run is in progress Cronicle refuses with `Maximum of 1 job already running for event: etl-publish`; try again after it ends.
- Directly: `docker compose exec cronicle etl publish --now 2026-10-12T10:06:00Z` publishes 10:00–10:05 (5-minute windows), but bypasses that limit: use it only while no `etl-publish` job is running (Cronicle's **Home** tab shows active jobs), or two runs append to `logs/publisher/etl.log` at once and race on their Pushgateway push (the duplicate messages themselves are dropped as `replay`).
- A range: edit the `etl-publish` event and, under **Time Machine** (shown while **Catch-Up (Run All)** is on), set the event clock to the first publish time to repeat; Cronicle then runs every scheduled time from there, in order.

Reruns are safe: they produce copies with the same `event_id`, which the transformer drops as `replay`. That holds only within `ETL_CLAIMS_RETENTION_DAYS`; a rerun of an older window writes duplicates to the processed zone. After `docker kill` (SIGKILL) of the cronicle container the in-flight window stays red and must be re-run by hand; a graceful stop (`docker compose stop`, 30 s grace) aborts the job and rewinds the event, so that window runs again after the restart. The admin password and API key are stored when the `cronicle` volume is created, so editing `.env` does not change them. Change the password under **My Account** in the UI; for a new API key, recreate the volume (this also clears Cronicle's run history; the events are provisioned again at start): `docker compose rm -sf cronicle && docker volume rm coingecko-etl_cronicle && make up`. `docker compose logs cronicle` shows only Cronicle's errors and the provisioning lines; job results are in the Cronicle UI and the job logs.

**Pushgateway** at <http://localhost:9091>. Every job pushes its result when it ends, grouped by `job` (`etl_publisher`, `etl_retention`, `etl_vacuum`): `etl_job_last_run_timestamp_seconds`, `etl_job_last_success_timestamp_seconds` (pushed only on success), `etl_job_duration_seconds`, `etl_job_success` (1 or 0), and the job's own gauges ([Metrics](#metrics)). The push uses POST ("add") semantics, so a failed run does not erase the last success time; the Pushgateway persists to its `pushgateway` volume, and a failed push is logged at ERROR without changing the job's exit code. Prometheus scrapes it, and three alerts watch it: `ETLPublisherStale`, `ETLMaintenanceStale` and `ETLMaintenanceNeverSucceeded` ([Alerts](#alerts)).

**Maintenance.** `etl maintenance retention` drops the daily partitions of `pipeline.dedup_claims` older than `ETL_CLAIMS_RETENTION_DAYS` (default 8: on 2026-10-12 it keeps 2026-10-04 and later) and deletes `written` rows of `pipeline.consumer_batches` written before that date; it never touches `planned` rows, which re-planning and manual repair need. `etl maintenance retention` refuses a value below the topic retention in days + 1 (8 for the 7-day topic), because claim partitions follow the event date while the topic keeps a message for 7 days after it was published. Example: a snapshot taken at 23:59 on 2026-10-04 is exported in the window that ends at midnight and published at 00:01 on 2026-10-05, so the topic holds it until at least 00:01 on 2026-10-12, longer while its segment is not deleted yet. With 7 days, any retention run on 2026-10-12 (the cutoff is a UTC date, whatever `ETL_SCHEDULE_TZ` says) would drop the 2026-10-04 claims while the message may still be replayed, and the replay would be written again; with 8 they stay until 2026-10-13, a day of margin. Production would keep 30 days, which also covers a republish after a schema fix. Retention also bounds the dedup horizon by event time: a record that arrives again with an event time older than `ETL_CLAIMS_RETENTION_DAYS` (for example a watched coin whose `last_updated` stopped advancing) is written again once the retention run removed its claim. With the default settings this does not happen: markets records take the newest `last_updated` of all 100 coins, and the watched coins update every minute. In production such records would go to `rejected/` as too late (an allowed-lateness rule). `etl maintenance vacuum` runs `VACUUM (ANALYZE)` on `raw.api_calls` (the dead row versions the exporter's claim leaves), `pipeline.export_windows`, `pipeline.export_batches` and `pipeline.consumer_batches`, in addition to autovacuum. Publish runs log to `logs/publisher/etl.log`, maintenance runs to `logs/maintenance/etl.log`.

## Browsing the lake

The S3 gateway (versitygw, posix backend) stores every bucket as a directory and every object as a plain file under `./data`; object metadata (the `sha256` we attach) lives in `./.s3meta`.

```
data/raw/source=coingecko/ingest_date=2026-10-12/window=0410/part-00000.jsonl
data/raw/source=coingecko/ingest_date=2026-10-12/window=0410/_SUCCESS
data/processed/dataset=market_snapshot/v=1/processed_date=2026-10-12/hour=04/part-20261012T041000Z-6531919c.jsonl
data/rejected/dataset=coin_contract/processed_date=2026-10-12/hour=04/part-20261012T041000Z-a0b1e42c.jsonl
data/dlq/stage=transform/processed_date=<date>/hour=<HH>/part-<planned_at>-<id8>.jsonl
data/dlq/stage=export/ingest_date=<date>/window=<HHMM>/part-<NNNNN>.jsonl
data/dlq/stage=publish/ingest_date=<date>/window=<HHMM>/part-00000.jsonl
```

The first four keys are from a real `make demo` run, so their windows are 1 minute long (`window=0410` covers 04:10–04:11; with the default 5 minutes it would cover 04:10–04:15); the dlq lines are patterns. Raw objects and the exporter and publisher dlq objects are named by their window (the UTC date and start time; the exporter's dlq part carries the index of the raw part whose lines failed, the publisher writes one dlq object per window). Processed, rejected and transformer dlq objects are named by the batch's `planned_at`, its processing time (`processed_date`, `hour`, and `YYYYMMDDTHHMMSSZ` in the file name), plus `id8`, the first 8 hex characters of the sha256 of topic id, partition and first offset, so a retried batch writes the same key. The Kafka position enters a name only through this hash and together with the topic id, so a recreated topic, whose offsets start again at 0, cannot produce an old name. An hour directory can still receive a file after that hour: a batch planned at 10:59:59 and written after a crash lands in `hour=10`. Details: [The raw window contract](#the-raw-window-contract) and [ADR 0008](docs/adr/0008-processing-time-object-names.md).

Five ways to look at it:

- **Plain files.** `find data -name '*.jsonl'`, then `head -n1 <file> | jq .`. Do not edit or add files by hand: the gateway's write-once check relies on the metadata it stored itself.
- **Any S3 client.** Endpoint `http://localhost:7070`, access key `etl`, secret `etlsecret123`, region `us-east-1`, path-style addressing. For example with the AWS CLI:
  ```bash
  export AWS_ACCESS_KEY_ID=etl AWS_SECRET_ACCESS_KEY=etlsecret123 AWS_DEFAULT_REGION=us-east-1
  aws --endpoint-url http://localhost:7070 s3 ls s3://processed/ --recursive | head
  ```
- **Redpanda Console** at <http://localhost:8081>: topic `coingecko.raw`, messages keyed by `idempotency_key`, with their headers (`event_id`, `idempotency_key`, `schema`, `raw_object`, `line_no`), and the `coingecko-transformer` consumer group with its lag.
- **Postgres** at `localhost:5432` (user, password and database `etl`): `raw.api_calls` holds every HTTP call; `pipeline.*` holds the ledgers: `export_windows` (one row per raw window: status, row and part counts) and `export_batches` (one row per raw part) for the exporter, `consumer_batches` (transformer batch manifests) and `dedup_claims` for the transformer.
  ```sql
  SELECT outcome, http->>'status' AS status, count(*) FROM raw.api_calls GROUP BY 1, 2;
  SELECT idempotency_key, count(*) FROM raw.api_calls WHERE outcome = 'ok'
  GROUP BY 1 HAVING count(*) > 1;                          -- identical responses before dedup
  SELECT window_start, status, row_count, part_count FROM pipeline.export_windows
  ORDER BY window_start DESC LIMIT 5;                      -- the exporter's latest windows
  ```
- **Grafana** at <http://localhost:3000> (anonymous viewer): the provisioned dashboard *ETL / CoinGecko ETL*.

How long each store keeps its data:

| Store | Kept | Enforced by |
|---|---|---|
| lake objects (`./data`: raw, processed, rejected, dlq) | forever (write-once) | nothing deletes them; `make clean` wipes them |
| `raw.api_calls` and the export ledger | forever; about 33 KB per call after TOAST compression, so about 0.2 GB a day at 4 calls a minute | not implemented ([Reliability](#reliability)) |
| topic `coingecko.raw` | 7 days (longer until a segment is deleted) | the topic's `retention.ms`, set by `etl init` |
| `pipeline.dedup_claims`, `written` rows of `pipeline.consumer_batches` | `ETL_CLAIMS_RETENTION_DAYS` (8) | `etl maintenance retention`, daily |
| `planned` rows of `pipeline.consumer_batches` | until written | the transformer |
| Prometheus samples | 7 days | `--storage.tsdb.retention.time` |
| Pushgateway | the last pushed value of each metric, per job | the next push of that metric |
| logs under `./logs` | forever (appended, no rotation) | `make clean` |

## Data model and time model

### Source

| Endpoint | Schedule | Why |
|---|---|---|
| `/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&price_change_percentage=1h,24h,7d&sparkline=false&precision=full` | every 30 s | a flat 30-second time series of the top 100 coins |
| `/coins/{id}?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false` | every 30 s, between the markets calls (at :15 and :45), one coin per call, in turn: bitcoin → ethereum → tether → usd-coin → bitcoin …, so each coin is refreshed every 2 min | deep nesting: 23 maps × 63 currencies, 100 tickers with nested objects, contracts per chain |

The keyless tier allows 10–30 calls per minute per IP, so the fetcher spends 4, 15 s apart, with one request in flight; it honours `Retry-After`, backs off with jitter on 5xx and network errors, and skips ticks instead of queueing them.

### Raw zone

`raw.api_calls` in Postgres has one row per HTTP call, including failures (`outcome` is `ok`, `http_error` or `network_error`). Each call gets an `event_id`, a UUIDv7 generated by the fetcher, unique per row; the row's raw line and its broker message carry the same id, and so do the copies a rerun produces. The response body is stored as received, except that a body with line breaks is compacted to one line (one call is one JSONL line); `payload_sha256` is the hash of the stored bytes, so it can be re-verified at every hop. Each row also carries `idempotency_key = sha256(source | endpoint | canonical params | payload_sha256)`; two polls that received the same cached response share the key. The table is partitioned by day on `fetched_at`. The only column the pipeline updates is `export_batch_id`, set once when the exporter claims the row.

### The raw window contract

The exporter writes the successful calls as JSON Lines into `s3://raw`, one line per call with the stored payload embedded verbatim, grouped into windows:

- **Size.** `ETL_WINDOW`, default `5m` (`make demo`: `1m`): whole minutes that divide an hour (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30 or 60). Windows are aligned to the UTC hour. The exporter, `etl publish` and the Cronicle provisioning refuse any other value at startup. The size is fixed for the lifetime of the export ledger and the lake: the exporter refuses a ledger or a kept lake written with another size ([Quick start](#quick-start)).
- **Keys.** `raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-<NNNNN>.jsonl` (part index from `00000`, at most 500 rows per part) and `raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/_SUCCESS`; `ingest_date` and `HHMM` are the UTC date and time of the window start.
- **Content.** A window holds the rows the exporter claimed when it closed it: successful calls with `fetched_at` before the window end that no earlier window took. A row committed after its window closed goes to the next window, so nothing is lost. After downtime the missed windows are closed in order. On a fresh stack the window before the current one is closed at once (usually empty), so the first publish run finds it.
- **Readiness.** A window is ready when its `_SUCCESS` exists; it is written last. A reader reads exactly the parts it lists and checks their sha256 (part keys are relative to the `raw` bucket; `rows` counts lines). An empty window still gets a `_SUCCESS`, with `"rows":0,"parts":[]`, so a missing `_SUCCESS` always means the exporter is behind or down. Every object, `_SUCCESS` included, is written once and never overwritten.

`_SUCCESS` is compact JSON with its keys in a fixed order. Two real ones, a window with one part and the empty first window of the stack:

```json
{"window_start":"2026-10-12T04:10:00.000Z","window_end":"2026-10-12T04:11:00.000Z","rows":4,"parts":[{"key":"source=coingecko/ingest_date=2026-10-12/window=0410/part-00000.jsonl","sha256":"66d3594461a259f2c2b60cd8f5cf52394b760a7a31a7e9cf66f0004eebd6dee7","rows":4}]}
{"window_start":"2026-10-12T03:51:00.000Z","window_end":"2026-10-12T03:52:00.000Z","rows":0,"parts":[]}
```

### Processed datasets

| Dataset | Source | Natural key | Rows per snapshot |
|---|---|---|---|
| `market_snapshot` | `/coins/markets` | coin_id, snapshot_at | 100 |
| `coin_snapshot` | `/coins/{id}` scalars (rank, supply, categories, genesis date, …) | coin_id, snapshot_at | 1 |
| `coin_market_by_currency` | the 23 `market_data` maps zipped on the currency key | coin_id, currency, snapshot_at | 63 |
| `ticker` | `tickers[]`, with `market` flattened to `exchange_id`/`exchange_name` and converted values to `_usd`/`_btc`/`_eth` columns | coin_id, exchange_id, base, target, snapshot_at | 100 |
| `coin_contract` | `platforms` + `detail_platforms` zipped on the platform key | coin_id, platform_id, snapshot_at | up to 36 |

The child datasets reference `coin_snapshot` by `(coin_id, snapshot_at)`. Images, descriptions, links, sparklines and duplicated fields are dropped. Every row carries technical lineage columns: `_event_id`, `_raw_object`, `_kafka_partition`, `_kafka_offset`, `_schema_version` and `_processed_at`. `_processed_at` is the batch's `planned_at` from its manifest, never the wall clock, so a retried batch produces identical bytes; the same time names the object.

### Formats

- Timestamps: RFC 3339 in UTC with milliseconds, e.g. `2026-10-10T10:07:55.000Z`; dates as `YYYY-MM-DD`.
- Money and quantities (prices, market caps, volumes, supplies) are decimal strings in plain notation, e.g. `"43083803721443304"`, and never pass through `float64`: some values exceed 2^53. Percentages and ratios are JSON numbers.
- Symbols upper-case; currency codes and EVM addresses lower-case.
- Files are plain `.jsonl` (no compression) so they can be read with `cat`.

### Cleaning rules

- **Normalize:** `""` and `[]` become `null`; timestamps and numbers as above; `roi` is flattened to `roi_times`, `roi_currency`, `roi_pct`.
- **Reject** to `s3://rejected` with a reason: `missing_id`, `null_price`, `nonpositive_price`, `empty_platform` (for example bitcoin's `platforms: {"": ""}`), `empty_record` (all metrics null), `snapshot_conflict` (see below; it also catches two tickers of one response that share a key with different values).
- **Flag and keep:**
  - `is_stale`: `snapshot_at − source_updated_at > 10 min`; for tickers, the upstream flag or a last trade older than 1 h;
  - `is_inactive`: zero volume (`market_snapshot` and `coin_market_by_currency` only: the other datasets have no volume of their own);
  - `is_anomaly`: price outside `[low_24h, high_24h]`; for tickers, the upstream flag or `last_usd` more than 2% away from the coin's median over all its tickers in the snapshot. For `market_snapshot` the range is widened by `ETL_ANOMALY_TOLERANCE_PCT` (default 0.5%) on each side, because with `precision=full` the price carries more digits than the rounded 24 h bounds; `0` restores the strict check. `coin_market_by_currency` stays strict.

The thresholds are configuration (`ETL_STALE_AFTER`, `ETL_TICKER_STALE_AFTER`, `ETL_TICKER_ANOMALY_PCT`, `ETL_ANOMALY_TOLERANCE_PCT`).

### Time model

| Field | Source | Meaning |
|---|---|---|
| `snapshot_at` | derived: the newest `last_updated` in a markets response; the document's `last_updated` for `/coins/{id}` | when CoinGecko assembled the snapshot; event time. Its UTC date, `event_date`, is part of the dedup key and a column of rejected rows and dedup claims; processed rows have no `event_date` column (derive it from `snapshot_at`). The lake is partitioned by processing time, so queries by event time scan more files |
| `source_updated_at` | the coin's `last_updated` | when CoinGecko last recomputed this coin; lags `snapshot_at` by hours for stale coins |
| `fetched_at` | our clock | when we polled; picks the raw window, otherwise lineage only |
| `planned_at` (`_processed_at`) | the transformer batch manifest | when the batch was planned; names the processed, rejected and transformer dlq objects |
| `last_traded_at`, `last_fetch_at` (tickers) | exchange data | exchange trade time; when CoinGecko fetched the exchange |

CoinGecko recomputes all coins in one pass every 60 s and stamps them with the same `last_updated`. An event is a source snapshot: a stale coin observed in two different snapshots yields two valid rows (both flagged `is_stale`), while two polls that were served the same cached response yield one. Clocks of different systems disagree, so no pipeline decision compares them; only the `etl_source_data_age_seconds` metric does, for observation.

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
| fetcher, before INSERT | S1 envelope | the row is not inserted (the fetcher builds the envelope, so this is a bug on our side); ERROR log; a persistent failure trips the stale-data alert |
| exporter, publish job | S1 | the line goes to `s3://dlq` (class `decode_error` or `schema_violation`; the publish job also `unexpected_outcome`, a raw line whose outcome is not `ok`, which the exporter never exports), not downstream |
| transformer, input | S1, payload checksum, then the payload against S0 | `s3://dlq` with class `decode_error`, `schema_violation`, `integrity_error`, `source_schema_violation` or `transform_error` (a payload that passes S0 but cannot be transformed) |
| transformer, output | S2 | `s3://dlq` with class `output_schema_violation`; never written to processed |
| writing rejected / dlq records | S3 | not written; ERROR log with the original bytes in base64 |

`rejected/` is for records removed by a cleaning rule; `dlq/` means "something broke and a human is needed". After a schema fix, the affected windows are [published again](#scheduled-jobs-publishing-and-maintenance) and reprocessed, within the claims retention: a row that went to `dlq/` never claimed its deduplication key, so the republished copy is written, while the rows of the same message that were already written are dropped as replays.

**From a dlq record to its window.** Export and publish dlq objects are named by their window. A transform dlq record carries `origin.event_id` (and the Kafka partition and offset); look up its window in Postgres, then rerun that window as in [Rerunning windows](#scheduled-jobs-publishing-and-maintenance), with `--now` = `window_end` + 1 minute:

```sql
SELECT w.window_start, w.window_end FROM raw.api_calls a
JOIN pipeline.export_batches b ON b.batch_id = a.export_batch_id
JOIN pipeline.export_windows w USING (window_start)
WHERE a.event_id = '<origin.event_id>';
```

A message that could not be decoded has no `event_id`: open it at that partition and offset in Redpanda Console, where its `raw_object` header is the raw part, whose key names the window.

**Schema evolution.** Schemas ship inside the binary, so a schema change is a redeploy: consumers (transformer) first, then producers. A compatible change edits `<name>.v1.json` in place and must keep accepting the append-only valid fixtures under `schemas/testdata/` and the recorded CoinGecko responses (`TestSchemaFixtures`, run by CI). A breaking change adds `<name>.v2.json` side by side in the same topic; the version travels in every message.

## Delivery guarantees

Every hop after the insert into Postgres is at-least-once. The processed zone nevertheless contains each stored record exactly once, because every step can be repeated safely: deterministic object keys, ledgers in Postgres, and write-once objects (`PUT` with `If-None-Match: *`). **Deduplication happens once, in the transformer; idempotency happens at every write.**

Exactly once holds for every call stored in `raw.api_calls`, provided that:

1. every window is eventually published: a run that stays red, or a window an earlier stack closed but never published over a kept lake, is published by hand ([Rerunning windows](#scheduled-jobs-publishing-and-maintenance));
2. replays, reruns and consumer-group resets happen within `ETL_CLAIMS_RETENTION_DAYS` (default 8 days), and no record arrives again with an event time older than that ([Maintenance](#scheduled-jobs-publishing-and-maintenance));
3. no transformer batch stays `planned` longer than the topic's 7-day retention (a week-long transformer outage needs the manual repair in [ADR 0004](docs/adr/0004-postgres-pipeline-state.md)).

A halted partition (below) loses and duplicates nothing; its records wait until it is resolved. The source hop is best effort: a poll whose insert fails is lost (ERROR log), and the next poll 30 s later covers it.

| Hop | Mechanism | On retry |
|---|---|---|
| fetcher → `raw.api_calls` | primary key `(event_id, fetched_at)`, `ON CONFLICT DO NOTHING` | no retry: a failed insert loses that poll |
| exporter → `s3://raw` | rows claimed with `FOR UPDATE SKIP LOCKED` and stamped with the batch id in the ledger transaction; keys fixed at claim; `If-None-Match: *`; `_SUCCESS` last | same object: 412, checksums equal |
| `s3://raw` → broker (`etl publish`) | window from the scheduler's time; parts and sha256 from `_SUCCESS`; idempotent producer, `acks=all` | a rerun produces copies with the same `event_id`, dropped by the transformer as `replay` |
| transformer → `s3://processed` | batch manifest; write-once objects; offset commit after the write | same objects: 412, checksums equal |

**Why a `SKIP LOCKED` claim.** The assignment places raw data in Postgres first, which gives a queryable record of every API call before any deduplication, but a transactional table is not a change log, so the exporter treats it as a work queue. When a window closes, one short transaction picks up to 500 successful rows fetched before the window end that no batch owns yet (`FOR UPDATE SKIP LOCKED`), inserts the batch with its object key into `pipeline.export_batches` and stamps the rows with its id. A row whose transaction commits late is claimed by the next window, so nothing is skipped and nothing waits for a long transaction; because the rows are bound to the batch before the PUT, a retry after a crash reads them back by batch id and writes the same bytes. The cost is one UPDATE per raw row: `raw.api_calls` is therefore not strictly append-only (the recorded call never changes, but every row leaves a dead version for autovacuum and new index entries; the ~95 KB payload stays in TOAST and is not copied). One exporter runs at a time (Postgres advisory lock) and finishes any claimed part before claiming new rows. In production we would land raw events directly in a log or object storage, or stream the table with Debezium CDC ([ADR 0005](docs/adr/0005-exporter-skip-locked-claim.md), which also covers the xid8 cursor that avoids the updates).

**Deduplication.** Every record bound for `processed/` or `rejected/` claims its natural key in `pipeline.dedup_claims` (`INSERT … ON CONFLICT DO NOTHING`); the first claimant wins:

| The claim for the key points to | Action |
|---|---|
| this record's own position (partition, offset, ordinal = the record's index within the message) | write it (first sight, or a redelivery after a crash) |
| another position, same content hash | drop as a duplicate |
| another position, different content hash | write to `rejected/` as `snapshot_conflict` |

The content hash covers business fields only, so the same record arriving in two messages matches even though its lineage columns differ. A drop is one decision with three labels, chosen by comparing the claim's winner with the dropped record: `replay` when the `event_id` is the same (the same message delivered again), `duplicate_response` when the `idempotency_key` is the same (a CDN copy of the response), `duplicate_record` otherwise (the same content from another response). There is no second, call-level dedup.

**Transformer batch.** A batch is a contiguous offset range of one partition.

1. One short transaction, which commits at its end:
   1. record the range in `pipeline.consumer_batches` (`planned`, with `planned_at`, the batch's only clock);
   2. run the input checks (no database queries);
   3. transform: normalize, clean, flag;
   4. validate the rows against their dataset schema (a row that fails goes to `dlq/` without claiming its key);
   5. claim the keys of the other rows and decide;
   6. store the object keys and checksums in the manifest row.
2. After the commit, write the objects with `If-None-Match: *`.
3. Mark the batch `written`.
4. Commit the offset.

On partition assignment the manifest decides: `written` → skip past it, `planned` → reprocess exactly that range.

If a key already holds different bytes (412 with another checksum), or a re-planned batch would store other checksums than the manifest, the transformer halts that partition (no fetching, no commits, growing lag; the other partitions keep running) and `ETLObjectChecksumMismatch` fires until a human resolves the conflict and restarts it:

1. The transformer's ERROR line `partition halted` names the partition, the offset range and, in `err`, the bucket and key.
2. Decide which bytes are right. The batch's expected checksums are in its manifest row (`pipeline.consumer_batches.content_sha256`); a mismatch takes a foreign writer (a file added by hand, an old stack over a kept lake after the clock went backwards) or a non-deterministic transform, which is a bug to fix and redeploy first.
3. Delete the wrong object through the S3 API (`aws s3 rm`, never on disk, so its metadata goes too) and run `docker compose restart transformer`: the batch is re-planned from its manifest and writes its objects.

The exporter reacts to the same conflict on a raw key by retrying every 15 s (its ERROR line `export failed` names the key; no restart is needed). A raw object may hold calls no other store still has, so copy it to another key before deleting it.

| Crash point | Recovery | Loss | Duplicates |
|---|---|---|---|
| before the first transaction commits | the batch starts over | no | no |
| after the transaction, before any PUT | manifest `planned` → same range; claims point to the same positions → same decisions, same bytes | no | no |
| after some PUTs | remaining PUTs succeed; finished ones return 412 with an equal checksum | no | no |
| after marking `written`, before the offset commit | manifest `written` → seek past the batch, commit | no | no |
| a zombie consumer and the new owner process the same range | same range from the manifest, identical bytes; the first PUT wins, the second gets 412 with an equal checksum; the zombie's offset commit is rejected | no | no |

The naive rule "drop if the key was already claimed" would lose every record of a batch that crashed between its transaction and the PUT; claims that remember their position avoid that.

Invariants: transforms are pure (no clock, no random order), and `planned_at` names the batch's objects. Dedup claims and `written` manifests are kept for `ETL_CLAIMS_RETENTION_DAYS` (default 8, never less than the topic's 7 days + 1): redeliveries, topic replays and window reruns within that period are deduplicated, while a raw republish older than that may write duplicates, and so may a record that arrives again with an event time older than that. Postgres session and transaction timeouts stop a stuck transaction from holding a connection and row locks.

Known limitation: a transformer batch still `planned` when the 7-day retention deletes its records (a week-long transformer outage) needs a manual repair, described in [ADR 0004](docs/adr/0004-postgres-pipeline-state.md).

## Metrics

Every long-running service (fetcher, exporter, transformer) listens on `:8080` with `/healthz` (the process is alive), `/readyz` (its dependencies answer: Postgres for all, S3 for the exporter and transformer, the broker for the transformer) and `/metrics`. Prometheus discovers the containers through Docker DNS, including both transformer replicas, and also scrapes the Pushgateway (with `honor_labels`), which holds what the jobs pushed. All pipeline metrics start with `etl_`.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `etl_api_requests_total` | counter | endpoint, status | CoinGecko calls by HTTP status, `network_error` or `skipped` (a skipped tick, not an error) |
| `etl_api_request_duration_seconds` | histogram | endpoint | API latency, body read included |
| `etl_api_rate_limited_total` | counter | — | HTTP 429 responses |
| `etl_last_success_timestamp_seconds` | gauge | endpoint | Unix time of the last successful call; freshness = `time() - this` |
| `etl_source_data_age_seconds` | gauge | endpoint | how old CoinGecko's data was when we fetched it (`fetched_at` minus the newest `last_updated`) |
| `etl_schema_validations_total` | counter | stage, schema, result | validations, `pass` or `fail`; the S0 failure share is the drift signal |
| `etl_schema_violations_total` | counter | stage, schema, keyword | which JSON Schema keyword failed |
| `etl_export_lag_seconds` | gauge | — | seconds since the end of the oldest window the exporter has not closed yet (0 on schedule); grows while S3 fails, a claimed part is stuck, Postgres is unreachable, or the exporter is behind |
| `etl_export_batches_total` | counter | result | exporter parts: `exported` or `error`; `error` also counts every failed window step of a tick (a ledger read, a refused `ETL_WINDOW` or lake, opening, claiming, sealing, writing `_SUCCESS`) |
| `etl_s3_put_total` | counter | result | conditional PUTs: `created`, `exists` (an idempotent retry), `mismatch` (must never happen), `error` |
| `etl_records_total` | counter | dataset, outcome | transformer output: `written`, `duplicate`, `rejected`, `dlq` |
| `etl_dedup_dropped_total` | counter | reason | duplicates removed by the natural-key claim, labelled `replay` (same event redelivered), `duplicate_response` (same API response polled twice) or `duplicate_record` (the same content from another response) |
| `etl_rejected_total` | counter | reason | records removed by cleaning rules |
| `etl_dlq_total` | counter | stage, error_class | broken records in the long-running services |
| `etl_consumer_lag_records` | gauge | partition | high watermark minus the committed offset; keeps growing while a transformer is stuck on a batch |
| `etl_batch_duration_seconds` | histogram | — | transformer batch time (transaction, PUTs, mark) |
| `etl_e2e_latency_seconds` | histogram | — | from `fetched_at` to the processed object being written; includes the wait for the window to close and be published (up to `ETL_WINDOW` + about 1 min) |
| `etl_job_last_success_timestamp_seconds`, `etl_job_last_run_timestamp_seconds` | gauge | job | pushed when a Cronicle job ends; the success time only on success |
| `etl_job_duration_seconds`, `etl_job_success` | gauge | job | duration and result (1 or 0) of the job's last run |
| `etl_publish_records`, `etl_publish_dlq_lines` | gauge | job | messages produced and lines sent to the publish dlq by the last successful publish run |
| `etl_retention_partitions_dropped`, `etl_retention_batches_deleted` | gauge | job | what the last retention run removed |

Expected freshness: a record reaches `processed/` within about `ETL_WINDOW` + 1 minute of its fetch (its window closes, the publish run follows a minute later, the transformer takes seconds), so about 6 minutes with 5-minute windows; on the demo stack (1-minute windows) `etl_e2e_latency_seconds` had a median of about 90 s and a p95 of about 2 minutes. No alert checks the latency itself; `ETLPublisherStale` and `ETLPipelineNotProgressing` fire when the flow stops.

The transformer's record, duplicate, reject, dlq, latency and batch-duration metrics are counted once per batch, when it is marked `written`, so a retried batch does not count twice; schema validation counters count every attempt. Every service also exposes the standard Go runtime and process metrics. Labels stay low-cardinality: no coin ids or offsets.

The Grafana dashboard shows data freshness, duplicate share, DLQ records and end-to-end latency at the top; then records per stage, freshness and source age, end-to-end latency, duplicate share, rejects by reason, dead letters, consumer lag, API status codes, export lag with S3 writes and schema validations; and for the jobs, time since each job's last success, records published per window, job runs and retention results.

## Alerts

The rules are in `deploy/prometheus/alerts.yml` and have unit tests (`make alerts-test` runs `promtool`). Open <http://localhost:9090/alerts> to see their state.

Twelve rules; the severity is `warning` unless marked critical. The 900 s of `ETLPublisherStale` assume `ETL_WINDOW` of 5 minutes or less, the 15-minute ranges of `ETLPipelineNotProgressing` 15 minutes or less. With longer windows, raise them to about three windows: otherwise they fire between two publish runs.

| Alert | Fires when | Likely cause → first check |
|---|---|---|
| `ETLDataStale` (critical) | no successful CoinGecko call on an endpoint for more than 2 minutes, or no successful markets call yet | rate limiting, network, CoinGecko outage, fetcher down → `logs/fetcher/etl.log` (`api call` lines with their status) |
| `ETLDeadLetterRecords` | any record reached `dlq/` in the last 10 minutes, or the last successful publish run sent lines to the publish dlq (until a run sends none) | a bug, a broken message, an integrity failure → the newest objects under `data/dlq/` ([from a dlq record to its window](#schemas-and-validation)) |
| `ETLSourceSchemaDrift` (critical) | more than 5% of CoinGecko responses fail the source schema over 15 minutes | CoinGecko changed its response format → `source_schema_violation` records in `data/dlq/stage=transform/` |
| `ETLExportLagHigh` | the exporter is more than 5 minutes past the end of the oldest window it has not closed, or its lag metric is absent | S3 failures, a claimed part that keeps failing, Postgres unreachable, exporter down, an `ETL_WINDOW` changed under an existing stack or a refused kept lake → `export failed` lines in `logs/exporter/etl.log` |
| `ETLConsumerLagGrowing` | consumer lag has grown for 10 minutes | transformers too slow, a batch that keeps failing, a halted partition → the transformers' logs (`partition halted`) |
| `ETLServiceDown` (critical) | for 2 minutes a long-running service (fetcher, exporter, transformer) or the Pushgateway fails its scrape (`up == 0`), or a service has no scrape target at all (Prometheus finds the containers through Docker DNS) | the container is stopped, crash-looping or hung → `docker compose ps` and its logs |
| `ETLPipelineNotProgressing` | CoinGecko calls succeed, but for 5 minutes no transformer counted a record (written, duplicate, rejected or dlq) over the last 15 minutes; only once the oldest running transformer has been up for 15 minutes, so on a fresh stack it fires after about 21 minutes at the earliest, and with no transformer at all only `ETLServiceDown` fires | the publish job failing, a stalled exporter, the broker down, transformers that cannot write → `ETLPublisherStale`, `ETLExportLagHigh`, `ETLServiceDown` first |
| `ETLObjectChecksumMismatch` (critical) | an object key already holds different bytes, or a re-planned transformer batch differs from its manifest; fires at once and stays up | a non-deterministic transform, a foreign writer, or a clock that went backwards; the transformer halts that partition, the exporter retries every 15 s → [resolving a halted partition](#delivery-guarantees) |
| `ETLFrequentRateLimiting` | 3 or more HTTP 429 responses in 15 minutes | another client on the same IP, or a too aggressive schedule → set `COINGECKO_API_KEY` or lower the request rate |
| `ETLPublisherStale` (critical) | no successful `etl publish` for 15 minutes, or none at all once Prometheus has been up for 15 minutes; silent while any Pushgateway scrape in the last 15 minutes failed (`ETLServiceDown` names it) | the exporter behind or down (`_SUCCESS` missing), S3 or the broker down, Cronicle stopped → the red `etl-publish` runs in Cronicle and `logs/publisher/etl.log` |
| `ETLMaintenanceStale` | retention or vacuum has succeeded before, but its last success is more than 26 hours old | a failing maintenance job, Cronicle stopped → its Cronicle event and `logs/maintenance/etl.log` |
| `ETLMaintenanceNeverSucceeded` | retention or vacuum has no success recorded at all, for 26 hours in a row; silent like `ETLPublisherStale` while the Pushgateway fails its scrapes | as above; a fresh stack first runs them at 11:00 and 11:30, so this only fires after a day without one |

Alertmanager is not part of the stack; in production the rules route to on-call through Alertmanager or Grafana alerting.

## Logs

Every service and job logs JSON through Go's `log/slog`, in UTC, to stdout (`docker compose logs -f fetcher`) and to its own file, so no two processes append to one file at once:

| File | Writer |
|---|---|
| `logs/<service>/etl.log` | `init`, `fetcher`, `exporter` |
| `logs/<container id>/transformer/etl.log` | each transformer replica (the short id `docker ps` shows): compose sets `ETL_LOG_PER_HOST=true` for the transformers, which puts the container's hostname in front of the path |
| `logs/publisher/etl.log`, `logs/maintenance/etl.log` | the Cronicle jobs, appended one run at a time |

Lines carry `time`, `level`, `msg`, `service`, and where relevant `stage`, `event_id`, `batch`, `job` and the window. They cover API success and failure, transformation errors and every object saved. A publish run logs `job started`, `publish started` (with its window), `window published` (parts, lines, records, dlq lines) or `window not ready`, and `job finished` or `job failed`. Cronicle also keeps each job's output in its UI. A real line:

```json
{"time":"2026-10-12T04:47:00.201Z","level":"INFO","msg":"window published","service":"publisher","job":"etl_publisher","window_start":"2026-10-12T04:45:00.000Z","window_end":"2026-10-12T04:46:00.000Z","parts":1,"lines":4,"records":4,"dlq":0}
```

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

# One-shot setup (waits up to 60 s for each dependency), then the long-running services.
docker run --rm --network etl --user "$(id -u):$(id -g)" -v "$PWD/logs:/app/logs" "${ETL_ENV[@]}" coingecko-etl:local init
for svc in fetcher exporter transformer; do
  docker run -d --name "$svc" --network etl --user "$(id -u):$(id -g)" \
    -v "$PWD/logs:/app/logs" "${ETL_ENV[@]}" coingecko-etl:local "$svc"
done

# One publish run for the window that closed last; Cronicle runs this every window in compose.
docker run --rm --network etl --user "$(id -u):$(id -g)" -v "$PWD/logs:/app/logs" "${ETL_ENV[@]}" \
  coingecko-etl:local publish --now "$(date -u +%FT%TZ)"
```

The publish run pushes its result to `ETL_PUSHGATEWAY_URL` (default `http://pushgateway:9091`); without a Pushgateway it logs `pushgateway push failed` and still exits 0 on success.

What persists where, by how the stack was started:

| Data | `make up` (docker compose) | `docker run` (above) |
|---|---|---|
| raw, processed, rejected, dlq objects | `./data` bind mount: survives `docker compose down -v`; `make clean` removes it | `./data` bind mount: survives container and volume removal |
| object metadata (the sha256 of the write-once check) | `./.s3meta`, like `./data` | `./.s3meta`, like `./data` |
| logs | `./logs`, like `./data` | `./logs`, like `./data` |
| Postgres (raw calls, ledgers, dedup claims) | volume `coingecko-etl_pgdata`, deleted by `docker compose down -v` | volume `pgdata`, deleted only by `docker volume rm pgdata` |
| broker log and consumer offsets | volume `coingecko-etl_redpanda`, deleted by `docker compose down -v` | volume `redpanda`, deleted only by `docker volume rm redpanda` |
| Cronicle events, run history and credentials | volume `coingecko-etl_cronicle`, deleted by `docker compose down -v` | — |
| last pushed job metrics | volume `coingecko-etl_pushgateway`, deleted by `docker compose down -v` | — |

Cleanup for the `docker run` stack: `docker rm -f postgres redpanda versitygw fetcher exporter transformer`, `docker volume rm pgdata redpanda`, `docker network rm etl` and `rm -rf data logs .s3meta`. Compose and `docker run` use different volume names but the same `./data`, so run them from different checkouts.

## Assumptions

The assignment leaves some points open. This is how they were read:

- **"Fetch every 30 seconds and store to Postgres"**: every call is stored, including failures and byte-identical cached responses. Raw storage happens before deduplication; the idempotency key is just a column there.
- **"Export to local storage as raw JSON"**: raw JSON Lines objects in the `raw` bucket, which is the local directory `./data/raw`. The application only talks S3, so the same code writes to Amazon S3 in production.
- **"Append-only"**: lake objects are write-once (`If-None-Match: *`) and the pipeline never overwrites or deletes one. In Postgres the recorded call never changes (only `export_batch_id` is set once, by the claim), and the pipeline state is pruned by the retention job.
- **"Remove unnecessary fields"**: images, descriptions, links, sparklines, trade URLs and duplicated fields are dropped; everything analytical is kept.
- **"Timestamps in UTC ISO 8601"**: RFC 3339 with milliseconds and a `Z` suffix.
- **Duplicates** come in two kinds and both are removed from the processed zone: redelivered messages (same event) and true duplicates (the same response polled twice, or the same content in another response).
- **Questionable data** is kept with flags (`is_stale`, `is_inactive`, `is_anomaly`) instead of being dropped; only empty or unusable records are rejected.
- **"`logs/etl.log`"**: one `etl.log` per writer under `logs/` ([Logs](#logs)), because several containers cannot safely append to one file.
- **Scale**: the free CoinGecko tier caps the volume at a few calls per minute; the design does not depend on that (see scalability below).

## Productionization

### AWS mapping

| Local | AWS |
|---|---|
| versitygw over `./data` | Amazon S3 with raw and processed zones, lifecycle rules (raw to Glacier after N days), Parquet compaction with a Glue Data Catalog for Athena, or Apache Iceberg tables |
| Postgres container | Amazon RDS or Aurora PostgreSQL, Multi-AZ |
| Redpanda | Amazon MSK (the franz-go code is unchanged; MSK IAM auth is supported by the client) |
| long-running compose services | ECS on Fargate or EKS; the fetcher scheduled by EventBridge Scheduler if polling moves to a job model |
| Cronicle | Amazon MWAA (Airflow): a publish DAG with `schedule='1-59/5 * * * *'`, `catchup=True`, `max_active_runs=1`, an `S3KeySensor` on the window's `_SUCCESS`, `retries=3`, `retry_delay=60s`, `execution_timeout=5min`; maintenance as a daily DAG, with the claim partitions managed by pg_partman on RDS |
| Pushgateway | Airflow task metrics, or a Pushgateway scraped into Amazon Managed Service for Prometheus |
| `.env` | AWS Secrets Manager (CoinGecko key, database credentials) and SSM Parameter Store |
| Prometheus + Grafana | Amazon Managed Service for Prometheus + Amazon Managed Grafana |
| stdout logs | CloudWatch Logs (or a Fluent Bit sidecar to OpenSearch) |

### Scalability

- **Finer messages, more partitions.** Keys already spread responses over all 6 partitions, but one markets response is still one message of 100 coins. Splitting responses into per-coin messages gives finer parallelism; the partition count can then grow beyond 6 and the transformer can run more replicas (up to one per partition).
- **Exporter scale-out.** One exporter under the advisory lock is enough here. To run several: drop the lock, give each claimed part an `export_lease_until` so any instance can resume a part whose lease expired (`SKIP LOCKED` on the ledger), keep a part's export deadline shorter than the lease, store `content_sha256` with a compare-and-set, and write one log file per replica (`ETL_LOG_PER_HOST`). Claims of one window still serialize on its row and only one window is open at a time, so the parallelism comes from building and uploading the claimed parts (the claim itself is short); part order would then not follow fetch order.
- **The unexported-rows index at volume.** It behaves like a queue index (dead entries until vacuum), so at volume it needs per-partition autovacuum settings on top of the daily VACUUM.
- **Strict window order.** The publish job could write a `_published` marker per window and refuse a window until the previous one has its marker (Airflow's `depends_on_past`); a red window would then hold back later ones instead of being hidden by them.
- **Compaction to Parquet.** Small JSONL objects suit a demo; production adds a periodic compaction into partitioned Parquet (or Iceberg) for analytics, partitioned by event date.
- **Debezium CDC instead of the exporter.** Change data capture streams `raw.api_calls` to Kafka in commit order without polling or per-row updates; the raw S3 objects would then be produced by a sink.
- **JVM alternative.** Kafka Streams (dedup state in RocksDB with `exactly_once_v2`) plus the Kafka Connect S3 Sink is the standard JVM way to build the same pipeline.
- **Schema registry.** With more producers and consumers, the embedded JSON Schemas move to a registry (AWS Glue Schema Registry or a Confluent-compatible one) with BACKWARD compatibility checks.
- **Dedup claims at volume.** One Postgres round trip per record inside a short transaction is fine here; at higher volume the claims can move to a key-value store with conditional writes (e.g. DynamoDB).

### Reliability

- Broker: replication factor 3, `min.insync.replicas=2`, producers with `acks=all` (already set) across three availability zones. Kafka and Redpanda delete whole segments, so a record can outlive `retention.ms` by up to `segment.ms`: consumer-group resets and replays belong within the claims retention.
- Postgres: Multi-AZ, automated backups and PITR. Claims retention runs locally as a daily job; in production `pg_partman` manages the daily partitions of `raw.api_calls` and the claims, with claims kept 30 days. Retention of `raw.api_calls` and the export ledger is not implemented (it would delete raw rows first, then ledger rows, because of the foreign key).
- Object storage: S3 versioning and replication for the raw zone, which is the replay source.
- Source: a paid CoinGecko plan removes the 429 risk and allows higher frequency and more coins.
- Operations: alerts routed through Alertmanager to on-call; dashboards per stage; runbooks for DLQ triage and replay (rerun the windows after a schema fix).

## Repository layout and tests

```
cmd/etl/                main and subcommand wiring: services, etl publish, etl maintenance
internal/bootstrap/     etl init
internal/config/        environment → Config
internal/coingecko/     HTTP client, rate limiter, Retry-After
internal/event/         raw event envelope (S1), idempotency key
internal/window/        ETL_WINDOW rules, raw window keys, the _SUCCESS manifest
internal/fetcher/ exporter/ publisher/ transformer/
internal/maintenance/   claims retention and VACUUM
internal/transform/     pure transforms: parse, normalize, clean, flag
internal/dedup/         claims and decisions
internal/validation/    JSON Schema registry
internal/storage/pg/ storage/s3/ queue/
internal/obs/           slog logger, metrics, health server, Pushgateway push
internal/xtime/         UTC timestamp formatting and parsing
internal/testutil/      testcontainers helpers (Postgres, Redpanda, versitygw)
schemas/                JSON Schemas S0–S3 and test fixtures (recorded CoinGecko responses)
migrations/             goose SQL migrations
deploy/                 Prometheus config and alerts, Grafana provisioning and dashboard, Redpanda bootstrap
deploy/cronicle/        Cronicle image scripts: setup, event provisioning (events as code) and its tests
docs/adr/               architecture decision records
docs/architecture/      the interactive architecture page
docs/superpowers/       design spec (current) and implementation plans (historical build logs) from the AI-assisted workflow
```

`docs/superpowers/` is the development record: the spec is the current design document; the plans are the step-by-step build logs the AI agents executed, kept as written.

```bash
make test-unit      # unit tests, no Docker
make test           # all tests with -race; integration tests start Postgres, Redpanda and versitygw containers (-p 2: two packages at a time)
make lint           # gofmt + go vet (CI also runs golangci-lint)
make alerts-test    # promtool: Prometheus config and alert rule unit tests
make cronicle-test  # node --test for the Cronicle provisioning, and a check that compose gives every service the same ETL_WINDOW and starts cronicle after a healthy exporter
```

Integration tests use testcontainers and skip themselves when Docker is unavailable (and under `-short`). For the exporter they cover a late commit going to the next window, concurrent claims serializing per window, parts written before `_SUCCESS`, empty windows, missed windows closed in order after downtime, the bootstrap window, refusing an `ETL_WINDOW` change and a kept lake of another size, and keys independent of the local time zone (the exporter and transformer test packages run at UTC+05:30). For the publish job: a run before `_SUCCESS` exists fails and leaves nothing behind, a rerun is idempotent, and a run stopped by SIGTERM still reports its failure. Retention keeps `planned` batches, two transformers with overlapping key sets do not deadlock, and the transformer tests cover crashes after the manifest transaction and after partial PUTs, a zombie consumer and object storage going down mid-batch, asserting no loss and no duplicates in the processed zone. CI (GitHub Actions) runs lint, the full test suite with Docker, the deploy-config checks (compose, promtool, Cronicle provisioning, dashboard JSON) and both image builds.

Architecture decisions are recorded in [`docs/adr/`](docs/adr/).
