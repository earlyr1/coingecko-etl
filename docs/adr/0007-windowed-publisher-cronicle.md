# ADR 0007: Raw windows, a scheduled publisher job and Cronicle

- Status: accepted
- Date: 2026-10-12

## Context

Stages should meet only through the artifacts they produce: `raw.api_calls`, raw objects in S3, topic messages and processed objects. A long-running publisher that leases batches from the export ledger couples the publisher and the exporter through Postgres. A reader of the raw zone also needs to know when a time range is complete, which a growing list of objects cannot tell: a late prefix and an empty one look the same.

Batch work should behave like an Airflow schedule: every run covers a known slice of time, and a failed run is visible, retried and caught up. Postgres maintenance (dropping expired dedup claims, VACUUM) needs the same.

## Options considered

- **Keep a long-running publisher with leases in Postgres.** Lowest latency (about 20 s from fetch to processed), but it couples the publisher to the exporter's ledger, has no completeness signal for raw data, and its failures show only as a lag metric.
- **A publisher that lists S3 prefixes.** No Postgres, but it cannot tell an exporter that is behind from an empty period.
- **Schedulers:**
  - Airflow locally: the production model, but a scheduler, a webserver and a metadata database are heavy for a laptop stack.
  - cron, supercronic or Ofelia in a container: light, but no catch-up after downtime, no run history and no UI.
  - pg_cron for the maintenance jobs: needs a custom Postgres image with `shared_preload_libraries`, also in the test images, and covers only Postgres jobs.
  - Cronicle: one Node process with a web UI, Run All catch-up, retries, timeouts, run history and an API to create events as code. It publishes no official image, so we build one from the pinned upstream release. Upstream is in maintenance mode (bug fixes only); the author's successor project is xyOps.

## Decision

**Raw windows.** The exporter writes raw data in windows of `ETL_WINDOW` (default 5m; whole minutes from 1m to 60m that divide an hour), aligned to the UTC hour. When window W is due, the exporter claims the unexported `ok` rows with `fetched_at` < W.end ([ADR 0005](0005-exporter-skip-locked-claim.md)), writes them as parts, and writes `_SUCCESS` last:

```
raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-<NNNNN>.jsonl
raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/_SUCCESS
```

`ingest_date` and `HHMM` are the UTC date and time of the window start. `_SUCCESS` is compact JSON with a fixed key order, for example:

```json
{"window_start":"2026-10-12T04:09:00.000Z","window_end":"2026-10-12T04:10:00.000Z","rows":4,"parts":[{"key":"source=coingecko/ingest_date=2026-10-12/window=0409/part-00000.jsonl","sha256":"61732189b09306c562dc75cdba0a0899f6f129ab59bcab8409394becfb95a779","rows":4}]}
```

- A window is ready when its `_SUCCESS` exists. A reader reads exactly the parts it lists and checks their sha256 and line counts (`rows`).
- An empty window still gets `_SUCCESS` with `"rows":0,"parts":[]`, so a missing `_SUCCESS` always means the exporter is behind or down.
- `_SUCCESS` plays the part of the marker a Spark job leaves in a finished partition, the key an Airflow `S3KeySensor` waits for. No Spark job writes this zone, so the exporter writes it. A daily batch could read a date range after a grace period. With 5-minute windows over a live table, commit lag is comparable to the window, so the exporter claims rows ([ADR 0005](0005-exporter-skip-locked-claim.md)) instead of trusting a time range.
- A row that commits after its window closed goes into the next window. After downtime the missed windows are closed in order.
- On an empty ledger the exporter starts at the window of the oldest unexported row. On a fresh stack that is the current window, so it closes the window before the current one at once (usually empty), and the first publish run finds a `_SUCCESS`. If the lake already holds the `_SUCCESS` of the first window, written by an earlier stack with the same `ETL_WINDOW`, the exporter starts at the next window and never overwrites it; a manifest of another window there (a lake of another size) stops the exporter.

**Publish job.** `etl publish --now <unix seconds | RFC3339>` publishes the window that ends at floor(now − 1 min, `ETL_WINDOW`). The one-minute grace is a constant; the exporter ticks every `ETL_EXPORT_INTERVAL` (15 s, and config refuses 1m or more), so it closes a window well before the run that publishes it. A run:

- reads only S3 and the broker, never Postgres;
- reads `_SUCCESS` and fails with exit code 1 when it is missing (`window not ready`). It also fails when the manifest's window is not the one it computed, for example when the exporter and the job see different `ETL_WINDOW` values;
- reads every listed part and checks its sha256 and line count before producing anything;
- validates every line against S1. Invalid lines go to one deterministic DLQ object per window, `dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl`;
- produces every valid line verbatim with an idempotent producer (`acks=all`), keyed by its `idempotency_key` ([ADR 0003](0003-redpanda.md));
- exits 0 on success, 1 on failure, 2 on a usage error.

A rerun of the same window produces the same records with the same `event_id`; the transformer drops the copies as `replay`.

**Cronicle.** Our image is Node plus Cronicle from the pinned upstream release plus the `etl` binary. At every container start it creates or updates three events through the Cronicle API, idempotently:

| Event | Schedule | Command | Policy |
|---|---|---|---|
| `etl-publish` | every `ETL_WINDOW` at +1 min, UTC (minutes 1, 6, …, 56 for 5m) | `etl publish --now "$JOB_NOW"` | Run All catch-up, 1 concurrent, 3 retries 60 s apart, timeout 5 min |
| `etl-retention` | daily 11:00 in `ETL_SCHEDULE_TZ` (default UTC) | `etl maintenance retention` | catch-up, 3 retries 60 s apart, timeout 30 min |
| `etl-vacuum` | daily 11:30 in `ETL_SCHEDULE_TZ` | `etl maintenance vacuum` | same; one maintenance job at a time |

- `$JOB_NOW` is the scheduled time, not the clock, so catch-up runs and reruns publish the right window. An invalid `ETL_WINDOW` (read exactly as the Go code reads it) or an `ETL_SCHEDULE_TZ` that Cronicle's bundled tz data (2021e) does not know stops the container instead of leaving a scheduler without its events.
- Maintenance runs in the daytime on purpose: the pipeline load is flat around the clock, and a failure is seen by people at work. Retention drops `pipeline.dedup_claims` partitions older than `ETL_CLAIMS_RETENTION_DAYS` and deletes `written` rows of `pipeline.consumer_batches` older than that, never `planned` ones ([ADR 0002](0002-single-dedup-in-transformer.md)). Vacuum runs `VACUUM (ANALYZE)` on `raw.api_calls` (dead versions from the claim UPDATE), `pipeline.export_windows`, `pipeline.export_batches` and `pipeline.consumer_batches`.
- Jobs append to `logs/publisher/etl.log` and `logs/maintenance/etl.log`, one run at a time.

**Pushgateway.** Every job pushes its result once at the end, under the grouping key `job` (`etl_publisher`, `etl_retention`, `etl_vacuum`):

- `etl_job_last_run_timestamp_seconds`, `etl_job_duration_seconds` and `etl_job_success` (1 or 0) on every run;
- `etl_job_last_success_timestamp_seconds` only on success;
- the job's own gauges: `etl_publish_records` and `etl_publish_dlq_lines` on a successful publish run, `etl_retention_partitions_dropped` and `etl_retention_batches_deleted` for retention.

The push uses POST ("add") semantics, so a failed run never erases the last success. The Pushgateway persists to a volume, and Prometheus scrapes it with `honor_labels`. A failed push is logged at ERROR and does not change the job's exit code. Alerts: `ETLPublisherStale` (no successful publish in 15 minutes, or none at all after Prometheus has been up 15 minutes), `ETLMaintenanceStale` (a maintenance job's last success is older than 26 hours) and `ETLMaintenanceNeverSucceeded` (no success recorded for 26 hours). The pushed series vanish while the Pushgateway cannot be scraped, so `ETLPublisherStale` and `ETLMaintenanceNeverSucceeded` stay silent while any of its scrapes in the last 15 minutes failed, and `ETLServiceDown` names the Pushgateway instead.

## Consequences

- Stages are independent: the publisher shares no table with the exporter, and `_SUCCESS` is the whole contract between them. A reader that waits for `_SUCCESS` never sees a half-written window.
- Latency is the cost: a row waits for its window to close, then for the publish run one minute later, so with 5-minute windows a record reaches `processed/` within about 6 minutes of its fetch. `make demo` uses `ETL_WINDOW=1m` so the demo publishes within its wait.
- A broker remains between two batch steps because the transformer needs it: offsets make its batches replayable and crash-safe, the consumer group shares partitions between replicas, and the topic is a 7-day replay buffer independent of the publish schedule.
- Reruns (Cronicle's Run Again on a job, a time range through the event's Time Machine with Run All on, or `etl publish --now` by hand) are safe within the claims retention; an older rerun writes duplicates.
- Window order is not enforced. A window that stays red after its retries (the exporter late by more than about four minutes, a SIGKILL of the scheduler) is not rerun automatically, and no alert fires while later windows succeed; it is rerun by hand. A graceful stop of Cronicle aborts the running job and rewinds the event, so that window runs again after the restart. A `_published` marker per window, checked before the next one (the `depends_on_past` analogue), would enforce order and surface such a window; it is left for production.
- `ETL_WINDOW` is fixed for the lifetime of a stack and its lake: windows closed with one size would never be published by a schedule of another. The exporter refuses to go on when the newest window in its ledger, or the `_SUCCESS` of its first window in a kept lake, has another size; changing the size takes a fresh stack (`make clean`), and `make demo` starts from one.
- The publisher has no HTTP endpoint: it is not scraped by Prometheus and not covered by `ETLServiceDown`; `ETLPublisherStale` covers it.
- The shipped alert thresholds assume short windows (`ETLPublisherStale` at 15 minutes for `ETL_WINDOW` of 5 minutes or less, `ETLPipelineNotProgressing` over 15 minutes for 15 minutes or less); longer windows need both raised to about three windows.
- In production the same jobs run on Amazon MWAA (Airflow): `schedule='1-59/5 * * * *'`, `catchup=True`, `max_active_runs=1`, an `S3KeySensor` on the window's `_SUCCESS`, `retries=3`, `retry_delay=60s`, `execution_timeout=5min`; maintenance becomes a daily DAG, and pg_partman manages the claim partitions on RDS.
