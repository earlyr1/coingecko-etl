# Pipeline Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the xid8 export cursor and the long-running publisher with hour-aligned `ETL_WINDOW` raw windows (claimed with `FOR UPDATE SKIP LOCKED`, parts first, `_SUCCESS` last), a Postgres-free `etl publish` batch job and daily `etl maintenance` jobs run by Cronicle and reported through a Pushgateway, with time-based object names and nothing left over from the previous revision.

**Architecture:** One exporter (Postgres advisory lock) claims unexported ok rows with `FOR UPDATE SKIP LOCKED`, closes one UTC-hour-aligned window per `ETL_WINDOW`, and writes the window's JSONL parts and then `_SUCCESS` to `s3://raw`; window state lives in `pipeline.export_windows` and `pipeline.export_batches`. Cronicle runs `etl publish --now "$JOB_NOW"` one minute after every window end (it reads only S3 and keys messages by `idempotency_key`) and `etl maintenance retention|vacuum` once a day; every job pushes its result to a persistent Pushgateway that Prometheus scrapes. Transformers keep their Postgres claims and manifests, but name objects by the batch's `planned_at` plus a hash of its Kafka position, so the lake no longer depends on database or broker lifetimes.

**Tech Stack:** Go 1.27 (pgx v5.11.0, franz-go v1.22.1 + kadm, client_golang v1.25.0 with `prometheus/push`, prometheus/common v0.72.0 `expfmt`, aws-sdk-go-v2, testcontainers-go v0.44.0), postgres:18.6, redpandadata/redpanda:v26.2.4, versity/versitygw:v1.8.0, Cronicle v0.9.135 on node:24.21.0-trixie-slim, prom/pushgateway:v1.11.4, prom/prometheus:v3.15.0 (promtool), grafana/grafana:13.2.3, golangci-lint v2.14.0, `node --test`.

**Spec:** `TODO-refactor.md` (decisions, in Russian; untracked and owned by the user: read it, never edit, stage or commit it) and `docs/superpowers/specs/2026-10-10-coingecko-etl-design.md` (current design). Line numbers in this plan are as of commit `b883ac0`; after earlier tasks have edited a file, re-locate each anchor with `grep -n` before editing.

## Global Constraints

- ETL_WINDOW: Go duration, default 5m. Valid only if it is whole minutes, >= 1m, <= 60m and divides 60m exactly. Windows are aligned to the UTC hour. The publisher's grace after a window end is a fixed constant of 1 minute.
- Raw layout: raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-<NNNNN>.jsonl (5-digit zero-padded part index from 0), plus raw/source=coingecko/ingest_date=<d>/window=<HHMM>/_SUCCESS written LAST. ingest_date and HHMM are the UTC date and time of the window start.
- _SUCCESS is compact deterministic JSON with keys in this order: {"window_start":"<RFC3339 ms Z>","window_end":"<RFC3339 ms Z>","rows":<int>,"parts":[{"key":"<object key>","sha256":"<hex>","rows":<int>}]} with parts sorted by part index; an empty window has "rows":0,"parts":[]. Every object, _SUCCESS included, is written with If-None-Match: * and on 412 the stored sha256 is compared (existing PutIfAbsent semantics).
- Window assignment: when window W closes, the exporter claims unexported ok rows with fetched_at < W.end (FOR UPDATE SKIP LOCKED, stamping export_batch_id). A row committed after W closed goes to the next window. After downtime, missed windows are closed in order.
- Exporter DLQ: dlq/stage=export/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl (part index of the raw part whose lines failed).
- Publisher DLQ: dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl (one per window, deterministic content).
- Transformer outputs (processing-time partitions): processed/dataset=<ds>/v=1/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl; rejected/dataset=<ds>/processed_date=<d>/hour=<HH>/part-<T>-<id8>.jsonl; dlq/stage=transform/processed_date=<d>/hour=<HH>/part-<T>-<id8>.jsonl. T = the batch's planned_at from the manifest formatted YYYYMMDDTHHMMSSZ (UTC); processed_date/hour also come from planned_at; id8 = first 8 hex chars of sha256("<topic_id>|<partition>|<first_offset>"). event_date stays a column (and part of the dedup key) but is no longer a directory.
- Kafka message key: the event's idempotency_key (every ok event has one).
- CLI: "etl publish --now <unix seconds | RFC3339>" (default: current time) processes the window ending at floor(now - 1m, ETL_WINDOW); "etl maintenance retention"; "etl maintenance vacuum". Exit code 0 = success, non-zero = failure (including a missing _SUCCESS). The publisher no longer uses Postgres at all and is no longer a long-running compose service.
- Pushgateway: env ETL_PUSHGATEWAY_URL (default http://pushgateway:9091). Grouping key {job="etl_publisher"|"etl_retention"|"etl_vacuum"}. Use push "Add" (POST) semantics so a failed run does not erase the last success. Metrics: etl_job_last_run_timestamp_seconds, etl_job_last_success_timestamp_seconds (pushed only on success), etl_job_duration_seconds, etl_job_success (1 or 0); job-specific gauges etl_publish_records, etl_publish_dlq_lines, etl_retention_partitions_dropped, etl_retention_batches_deleted. A push failure is logged at ERROR and does not change the job's exit code. Pushgateway runs with persistence to a volume.
- Claims retention: ETL_CLAIMS_RETENTION_DAYS (int, default 8); config validation refuses values below the topic retention in days + 1. Retention drops pipeline.dedup_claims daily partitions older than that and deletes consumer_batches rows with status 'written' older than that; it never touches 'planned' rows.
- Cronicle (local stand-in for Airflow): our own image (Node LTS + Cronicle built from the upstream release, pinned version, + our etl binary). Events provisioned as code at startup (idempotent): "etl-publish" every ETL_WINDOW at a +1 minute offset (Run All / catch-up on, max 1 concurrent, 3 retries, 60 s retry delay, timeout 5 min, command: etl publish --now "$JOB_NOW"); "etl-retention" daily 11:00; "etl-vacuum" daily 11:30 (timezone from env ETL_SCHEDULE_TZ, default UTC). Logs: logs/publisher/etl.log and logs/maintenance/etl.log (appended; one writer at a time).
- Migrations 00001 and 00002 are edited in place (the stack is always created fresh); a later task may extend the same files.
- Git: work happens on branch refactor/pipeline-windows; one commit per task; never push, never merge, never touch main. Commit messages end with: Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
- Every task removes the code, tests, config and comments that its change makes unused (the user asked that nothing unused from the previous revision remains).
- Repository rules that still hold: timestamps are RFC 3339 UTC with milliseconds (`xtime.Format`); every key, path segment and manifest value is computed from `.UTC()` (pgx returns `timestamptz` in `time.Local`); no `time.Now()` inside transforms or serialization; never hold a Postgres transaction across S3 or broker I/O.
- Staging: never `git add -A` or `git add .`; add explicit paths. The untracked user files stay untracked: `TODO-refactor.md`, `📦Trust Wallet - Data Engineer Take Home Project.pdf`, `.DS_Store`, `docs/.DS_Store`, `.impeccable/`.
- Lint gate of every task: `make lint` (gofmt + go vet only) AND golangci-lint v2.14.0, which CI runs and `make lint` does not (command below).

## Review Focus

- Fresh stack, first publish run: after `make clean && make demo` (ETL_WINDOW=1m) or `make up` (5m), Cronicle's first `etl publish` runs in the minute after the event is created (Cronicle v0.9.135 sets a new event's cursor to its creation minute, `lib/api/event.js:165-166`, and the scheduler runs only later minutes, `lib/scheduler.js:117-131`) and targets the window ending at floor(run time − 1 min). On an empty ledger the exporter closes the window before the current one at once (an empty `_SUCCESS`), and compose starts `cronicle` only after the exporter is healthy (Task 7), so that target is never older than the exporter's first window and the first run is green, at 1m too (without the ordering, a Cronicle provisioned in an earlier minute than the exporter's first tick targets a window nobody closes). A `_SUCCESS` an earlier stack left in a kept lake is never overwritten; test `TestWindowExportBootstrapWindow` (pinned by Task 3, Step 1) and the live check in Task 9, Step 5.
- A non-UTC local time zone, including a half-hour offset (this Mac runs MSK +03:00; `Asia/Kolkata` is +05:30): pgx returns `timestamptz` in `time.Local`, yet every window bound, raw/DLQ key, `_SUCCESS` byte, `processed_date`/`hour` and part time must equal the UTC result; tests `TestLayoutIgnoresLocalZone` (pinned by Task 1, Step 1), `TestWindowExportKeysIgnoreLocalZone` (Task 3, Step 1), the subtest "object keys come from the manifest planned_at, not the clock" (Task 5, Step 1) and the `TZ=Asia/Kolkata` package run (Task 9, Step 3). `internal/exporter` and `internal/transformer` get a `TestMain` that sets `time.Local` to +05:30 before `m.Run()`, so every test of those packages runs in that zone, in CI too. Never assign `time.Local` inside a test whose package starts goroutines (pgx pools, Kafka clients, HTTP servers): `time.Now()` reads it, and `go test -race` reports the write as a data race (reproduced with Go 1.27.1). Only `internal/window`, whose tests start no goroutines, swaps it inside a test.
- `ETL_WINDOW` differs between the exporter and the publish job (a partial `docker compose up -d`, a hand-edited environment) or changes between restarts: the publisher refuses a `_SUCCESS` whose window is not its own (exit 1, `ErrIntegrity` naming ETL_WINDOW) instead of publishing a wrong span, the Cronicle schedule follows the same value, and Go and JavaScript accept exactly the same values; tests `TestCheckManifest` window-mismatch cases (pinned by Task 4, Step 1), the compose environment check in `make cronicle-test` (Task 7, Step 1), and the mirrored tables `TestValidate` (Task 1) / `windowMinutes` (Task 7).
- A window published before its `_SUCCESS` exists (exporter late, Cronicle retry) and then published again (retry, Run Again, catch-up after a graceful stop): the early run produces no record and leaves nothing behind, the later run publishes every line once, and every further run repeats the same keys and partitions, so the transformer drops the copies as `replay`; test `TestPublisherIntegration/not ready, then ready, then rerun` (pinned by Task 4, Step 1) and the live rerun check in Task 9, Step 5.
- A scheduled job stopped mid-run (Cronicle's 5-minute timeout, Abort, `docker compose stop cronicle`): the job logs `job failed`, pushes `etl_job_success 0` without touching `etl_job_last_success_timestamp_seconds`, and exits 1 well inside Cronicle's 10 s kill grace; test `TestRunJobReportsFailureOnSIGTERM` (pinned by Task 4, Step 1; Task 6 reuses the same frame).

## How to execute this plan

**Order.** T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8 → T9, strictly in sequence on branch `refactor/pipeline-windows`, one commit per task (T9 commits only if its sweep changed files). Each task consumes names produced by earlier ones (see each task's **Interfaces** block). `make test` passes at every commit and the stack starts at every commit; between T4 and T7 nothing publishes (the publisher service is gone and Cronicle does not exist yet), so do no live data checks in that range.

**Commands used by every task** (run from `/Users/ruslan/code/trust-wallet-test`):

```bash
make lint                       # gofmt -l + go vet ./...
# golangci-lint as CI runs it (not installed on the host; image present locally; ~20 s; 0 issues at b883ac0):
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod \
  -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
make test-unit                  # go test -short -count=1 ./...  (integration tests skip under -short)
go test -race -p 2 -count=1 -timeout 15m <packages>   # Docker-backed integration tests
make test                       # everything, as CI
make alerts-test                # promtool check config + test rules (Docker)
```

**Environment facts.** macOS arm64, Docker Desktop, Go 1.27.1, host time zone MSK (+03:00), so tests already run with a non-UTC `time.Local`. Present locally: `golangci/golangci-lint:v2.14.0`, `prom/prometheus:v3.15.0`, `prom/pushgateway:v1.11.4`, `node:24.21.0-trixie-slim`, `postgres:18.6`, `redpandadata/redpanda:v26.2.4`, `versity/versitygw:v1.8.0`. Set `ETL_TEST_CONTAINER_PREFIX=etlrf-` when you want to spot leftover test containers (`docker ps -a --filter name=etlrf-`). A local `.env` exists and may hold `COINGECKO_API_KEY`: never delete or overwrite it. Its `ETL_UID`/`ETL_GID` are `65532`, while `make` passes the host ids (`id -u`/`id -g` = 501/20; the shell value beats `.env` in compose interpolation). Cronicle (T7) runs as `ETL_UID:ETL_GID` and keeps its storage on the named volume `cronicle`. At startup it chmods `logs/jobs` and its queue dir there (`lib/engine.js:61-62` of v0.9.135), so a cronicle container re-created as another uid crash-loops with EPERM. Run every command that creates or re-creates a container through `make`, or prefix it with `ETL_UID=$(id -u) ETL_GID=$(id -g)` (shell state does not persist between Bash calls, so prefix every such command). Before T1 (2026-10-12) these host ports were free: 3000, 3012, 5432, 7070, 8081, 9090, 9091, 19092. Containers of other projects run on this Docker (`docker ps`): never stop or remove them.

**Prototyped drafts.** The Cronicle/compose/alert drafts of Task 7 were built and run against a copy of the repository under `/private/tmp/claude-501/-Users-ruslan-code-trust-wallet-test/627e19a9-e998-4647-b498-c775b2d81667/scratchpad/plan/repo` (unified diffs in `.../scratchpad/plan/final/*.diff`). The copies inlined in Task 7 are authoritative if the scratch directory is gone.

## Draft conflicts resolved while assembling

1. **Shared window code** lives in a new package `internal/window`, created in T1 with the whole contract API (arithmetic, keys, `_SUCCESS` manifest). Config validates with `window.Validate` (no `config.ValidateWindow`). T3 and T4 only consume it; the publisher draft's `publisher.WindowFor`/`Grace` and its assumed "lake" package (`WindowPrefix`, `PartKey`, `Manifest`, `ParseManifest`) become `window.ForPublish`, `window.PublishGrace`, `window.RawWindowPrefix`, `window.RawPartKey`, `window.Success`, `window.ParseSuccess`. `window.Containing` from the exporter draft is dropped (no production caller).
2. **Message key** changes only in T1 (`idempotency_key`, falling back to `event_id` so a key is never empty); the publisher draft's second rewrite (returning `""`) is dropped.
3. **Pushgateway helper** uses the T1 names (`obs.PushJobRun`, `obs.ReportJobRun`, `obs.JobRun{Job, Started, Finished, Err, Gauges}`); the publisher draft's assumed `obs.PushJob` is replaced.
4. **One-shot job frame** `runJob` is created in T4 (its first user) and reused by T6. The frame logs `job started` / `job finished` / `job failed` (with `"job"` on every line); the publish body adds `publish started`, `window published` and `window not ready`. The publisher draft's `publish failed` line becomes the frame's `job failed`.
5. **Job gauges on failure:** the job body decides. `etl publish` returns `etl_publish_records`/`etl_publish_dlq_lines` only on success (a failed run's counts are unreliable); retention returns what it really removed, also on failure.
6. **Export lag:** T3's schedule-based definition (seconds past the end of the oldest window not yet closed; 0 on schedule) replaces T2's `LEAST(...)` query, which is correct only before windows exist. `ETLExportLagHigh` keeps its 300 s threshold with T3's texts; the ops draft's 600 s threshold and its sawtooth test are dropped. This departs from TODO item 1's literal formula because, with windows, every row waits up to `ETL_WINDOW` by design.
7. **First window on a fresh stack** (ops draft, open question 1): on an empty ledger the exporter closes the window just before the current one at once (usually empty), unless the lake already holds that window's `_SUCCESS`, in which case it starts at the current window. `s3.ErrNotFound` therefore moves from T4 to T3. Compose starts `cronicle` only once the exporter is healthy (T7). That makes the bootstrap window enough at 1-minute windows too: Cronicle's first run comes in the minute after provisioning and so never targets a window older than the bootstrap one.
8. **Publisher leftovers in compose, Prometheus, alerts and Grafana** (the `publisher` service and scrape job, its `ETLServiceDown` entries and test series, the `etl_published_records_total` panel query) are removed in T4, whose change makes them unused, not in T7.
9. **Maintenance alerts** (`ETLMaintenanceStale`, `ETLMaintenanceNeverSucceeded`) belong to T6, with the ops draft's wording and promtool timing; T7 adds only `ETLPublisherStale` and the publish-DLQ branch.
10. **`etl_published_records_total`** (`obs.Metrics.PublishedRecords`) is deleted in T4.
11. **`.env.example`:** `ETL_WINDOW` and `ETL_CLAIMS_RETENTION_DAYS` are added once, in T1; T7 adds ports and Cronicle settings and may extend the `ETL_WINDOW` comment.
12. **Old raw-key fixture strings** in transformer, s3 and publisher tests are replaced in T3; T5 does not redo them.
13. **`ETL_SCHEDULE_TZ`** places only the daily maintenance events; `etl-publish` always runs on the UTC window grid.
14. **Lake guard (`ErrLakeFromOtherLifetime`):** T3 switches its ledger check to `pipeline.export_windows` (empty windows have no batch rows), T5 deletes the guard.
15. **`docs/superpowers/`** stays where it is (user decision, TODO-refactor.md:189); the README gets one line explaining it.
16. **TODO wording vs contract:** `part-0.jsonl` → `part-00000.jsonl`; `ETL_CLAIMS_RETENTION` → `ETL_CLAIMS_RETENTION_DAYS`.
17. **TODO item 5 code-side leftovers** (the `(Task 14)` comment, the `first_seen_at` audit comment and the `dedup_claims_position_idx` comment in `00002`) go into T1 with the fallback-key fix.
18. **Dead code already present at `b883ac0`:** `go run golang.org/x/tools/cmd/deadcode@v0.51.0 ./cmd/etl` reports `coingecko.Limiter.Wait` (only tests call it) and `dedup.Decision.String`; T9 removes both.

## Open questions carried as defaults

Each default below is already written into its task; change the task if the user decides otherwise.

- Keeping the lake across a stack re-creation (fresh volumes): nothing is published twice. The exporter never overwrites an earlier `_SUCCESS` (T3). The new Cronicle event first runs in the minute after its creation (`lib/api/event.js:165-166`, `lib/scheduler.js:117-131` of v0.9.135). So every window it publishes has its scheduled publish time (window end + 1 min) after the old stack stopped, and the old stack publishes a window only at or after that time. The new stack can therefore at most publish a window the old one closed but never published, which is correct. Default: one README sentence (T8), no waiting rule, no code. (The draft's "republishes the old stack's last window" assumed a run in the creation minute, which Cronicle does not do.)
- A publish window that stays red after its 3 retries (exporter late by more than ~4 minutes, SIGKILL of cronicle) is not re-run automatically, and no alert fires while later windows succeed. Default: runbook re-run via Cronicle (T8); the strict-order `_published` marker (`depends_on_past`) stays a Productionization note.
- Alert thresholds assume `ETL_WINDOW` <= 5m (`ETLPublisherStale` 900 s, `ETLPipelineNotProgressing` 15m); `ETLExportLagHigh` (300 s past a window end) does not depend on the window. Default: keep the contract values and document the assumption.
- Kafka/Redpanda delete whole segments, so a record can outlive `retention.ms` by up to `segment.ms`; the claims guard (topic days + 1) assumes `retention.ms`. Default: runbook note (resets and replays only within the claims window); no `segment.ms` change.
- Retention for `raw.api_calls` and the export ledger stays out of scope (Productionization; raw first, then ledger rows, because of the FK).
- Publish DLQ object already present with different bytes on a rerun (validator messages changed between builds): keep the first, log ERROR, succeed (T4).
- `etl publish --now ""` (JOB_NOW unset) is a usage error (exit 2), not "use the clock" (T4).
- `dedup_claims.first_seen_at` is kept as an audit column with a COMMENT rather than dropped (T1, TODO item 5 option 1).
- Pins: node:24.21.0-trixie-slim, Cronicle v0.9.135 (ADD --checksum), prom/pushgateway:v1.11.4 (T7).
- The Cronicle UI labels "Run Again" and "Time Machine" were not browser-tested in the prototype: T7 and T8 verify them before documenting.
- Fresh-reader check of the README (T8): the implementer spawns the agent; if it cannot, the main session runs it and commits the fixes as `docs: fresh-reader fixes`.
- The local `.env` holds `ETL_UID=65532`/`ETL_GID=65532`, but `make` runs the stack as the host user (501/20). A plain `docker compose up -d` in the morning would therefore re-create every etl container and `cronicle` as 65532, and cronicle would then crash-loop on its volume (EPERM; see Environment facts). Default: `.env` is not edited; every command in this plan that creates a container goes through `make` or passes the host ids; the hand-over (T9, Step 6) tells the user to use `make` or set the two lines to `id -u`/`id -g`.

---

### Task 1: Foundations — window contract package, window/claims/Pushgateway config, Pushgateway job helper, idempotency_key message key, unique fallback record keys

Adds the shared contract code (TODO items 3 and 7: `ETL_WINDOW`, hour-aligned windows, raw/DLQ keys, the `_SUCCESS` manifest), the shared configuration (`ETL_WINDOW`, `ETL_CLAIMS_RETENTION_DAYS`, `ETL_PUSHGATEWAY_URL`) with startup validation, one Pushgateway helper for every batch job, keys Kafka messages by `idempotency_key` (TODO item 4), and fixes the `missing_id` fallback record keys that collide across coins and responses (TODO item 5, bug). Nothing calls the window keys or the push helper yet; T3, T4 and T6 do.

**Files:**
- Create: `internal/window/window.go` — `PublishGrace`, `Window`, `Validate`, `Floor`, `Ceil`, `ForPublish`, `(Window).Date`, `(Window).HHMM`.
- Create: `internal/window/layout.go` — `RawWindowPrefix`, `RawPartKey`, `RawSuccessKey`, `ExportDLQKey`, `PublishDLQKey`, `SuccessPart`, `Success`, `NewSuccess`, `(Success).Marshal`, `ParseSuccess`.
- Create: `internal/window/window_test.go`, `internal/window/layout_test.go`.
- Modify: `internal/config/config.go` — fields in the struct (:17-48), `Load` (:52-90), new parser methods `windowSize()` and `httpURL()`, and the cross-field check of claims retention against `TopicRetention` after the struct literal (between :85 and :86).
- Modify: `internal/config/config_test.go` — `allKeys` (:10-18), `TestLoadDefaults` (:28-69), `TestLoadOverrides` (:71-151), `TestLoadInvalid` (:153-186); new `TestLoadWindow`, `TestLoadClaimsRetentionVsTopic`, `TestLoadPushgatewayURL`.
- Modify: `.env.example` — after `ETL_EXPORT_INTERVAL=15s` (:31).
- Create: `internal/obs/push.go`, `internal/obs/push_test.go`.
- Modify: `internal/queue/queue.go` — `MessageKey` (:31-46); drop the `"strings"` import (:9).
- Modify: `internal/queue/queue_test.go` — replace `TestMessageKey` (:12-31); add `TestMessageKeySpreadsResponsesAcrossPartitions`.
- Modify: `internal/queue/queue_integration_test.go` — the hand-made record key `"bitcoin"` (:80, asserted at :108) becomes an idempotency key.
- Create: `internal/transform/keys.go` (`responseID`, `fallbackKey`), `internal/transform/keys_test.go`.
- Modify: `internal/transform/markets.go:129-131`; `internal/transform/detail.go` (`detailBuilder` :182-189 gains `respKey`, set in `transformDetail` :221-227; :236; :457-461).
- Modify: `internal/transform/markets_test.go` — `TestMarketsRejects` (:207-243), expected keys at :223-224.
- Modify: `internal/transform/detail_test.go` — `TestTickerDuplicateKeysKeepOrdinals` (:220-245, `tk[3]`), `TestDetailMissingIDAndContracts` (:285-290).
- Modify: `internal/dedup/dedup_test.go` — one subtest of `TestClaimAndDecide` after the concurrent subtest (:155-204).
- Modify: `migrations/00002_pipeline.sql` — comments at :37, :39, :43 (TODO item 5).
- Modify: `go.mod`, `go.sum` — `go mod tidy`: `internal/obs/push.go` imports `github.com/prometheus/common/expfmt` and the tests import `prometheus/common/model` and `prometheus/client_model/go`, so those move from indirect to direct. No new module (`prometheus/push` ships inside client_golang v1.25.0).

**Interfaces:**
- Consumes: `event.RawEvent.IdempotencyKey *string` / `EventID string` (`internal/event/event.go:42-56`), `event.IdempotencyKey(source, endpoint string, params map[string]string, payloadSHA256Hex string) string`, `event.SHA256Hex`, `event.SourceCoinGecko`, `xtime.Format`, `xtime.FormatDate`, `xtime.Parse`; `github.com/prometheus/client_golang/prometheus/push`, `github.com/prometheus/common/expfmt`; `kgo.StickyKeyPartitioner` (tests); `testutil.StartPostgres`, `pg.Connect`/`Migrate`/`EnsurePartitions` (dedup test).
- Produces (later tasks use exactly these):
  - `package window`: `const PublishGrace = time.Minute`; `type Window struct{ Start, End time.Time }`; `func Validate(size time.Duration) error`; `func Floor(t time.Time, size time.Duration) time.Time` (UTC); `func Ceil(t time.Time, size time.Duration) time.Time` (UTC); `func ForPublish(now time.Time, size time.Duration) Window`; `func (w Window) Date() string`; `func (w Window) HHMM() string`; `func RawWindowPrefix(w Window) string`; `func RawPartKey(w Window, part int) string`; `func RawSuccessKey(w Window) string`; `func ExportDLQKey(w Window, part int) string`; `func PublishDLQKey(w Window) string`; `type SuccessPart struct{ Key, SHA256 string; Rows int }`; `type Success struct{ WindowStart, WindowEnd string; Rows int; Parts []SuccessPart }`; `func NewSuccess(w Window, parts []SuccessPart) Success`; `func (s Success) Marshal() ([]byte, error)`; `func ParseSuccess(b []byte) (Success, error)`.
  - `config.Config.Window time.Duration` (ETL_WINDOW, default 5m), `config.Config.ClaimsRetentionDays int` (ETL_CLAIMS_RETENTION_DAYS, default 8, >= topic days + 1), `config.Config.PushgatewayURL string` (ETL_PUSHGATEWAY_URL, default `http://pushgateway:9091`, no trailing slash).
  - `obs.JobPublisher = "etl_publisher"`, `obs.JobRetention = "etl_retention"`, `obs.JobVacuum = "etl_vacuum"`; `obs.MetricPublishRecords`, `obs.MetricPublishDLQLines`, `obs.MetricRetentionPartitionsDropped`, `obs.MetricRetentionBatchesDeleted`; `type obs.JobRun struct{ Job string; Started, Finished time.Time; Err error; Gauges map[string]float64 }`; `func obs.PushJobRun(ctx context.Context, baseURL string, run obs.JobRun) error`; `func obs.ReportJobRun(ctx context.Context, log *slog.Logger, baseURL string, run obs.JobRun)`.
  - `func queue.MessageKey(ev event.RawEvent) string` — the idempotency key (`EventID` only as fallback); signature unchanged, so `publisher.go:282` and `publisher_integration_test.go:205` compile unchanged.
  - `Record.Key` of a `missing_id` record: `"#<idempotency_key or event_id>|<i>|<snapshot_at>"`; id-less tickers are prefixed with `"<coin_id>|"`.

- [ ] **Step 1: Write the failing tests**

`internal/window/window_test.go`:
- `TestValidate`: for every whole minute 1..60, `Validate(m*time.Minute)` returns nil exactly for {1,2,3,4,5,6,10,12,15,20,30,60}; `0`, `-5m`, `30s`, `90s`, `1m30s`, `5m30s`, `7m`, `45m`, `61m`, `2h` are errors, and the error text lists the allowed set `1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m`.
- `TestFloorCeil`: `Floor(10:07:31Z, 5m)=10:05Z`; `Floor(10:05:00Z, 5m)=10:05Z`; `Floor(10:59:59Z, 60m)=10:00Z`; `Floor(01:30+03:00, 5m)` is `22:30Z` of the previous day; `Floor(2026-10-11T05:29:00+05:30, 60m) = 2026-10-10T23:00:00Z` (UTC hour, not the local one); `Ceil(10:05Z, 10m)=10:10Z`; `Ceil(10:10Z, 10m)=10:10Z`; every result has `Location() == time.UTC`.
- `TestForPublish` (table of size, now → [start, end)): 5m@14:06:00Z→[14:00,14:05); 5m@14:01:00→[13:55,14:00); 5m@14:05:59.999→[13:55,14:00); 5m@14:00:59→[13:50,13:55) (grace not yet elapsed); 5m@10:05:59→[09:55,10:00); 1m@10:01:00→[09:59,10:00); 15m@10:16→[10:00,10:15); 60m@10:01→[09:00,10:00); 5m@00:00:30Z→[23:50,23:55) of the previous day; 5m@2026-10-13T00:01:00Z→[2026-10-12T23:55, 2026-10-13T00:00) and `RawSuccessKey` of it contains `ingest_date=2026-10-12/window=2355`. Every result is in UTC and `End-Start == size`.
- `TestForPublishCatchUp`: `JOB_NOW` values 13:01, 13:06, …, 14:01 (as Cronicle Run All replays them at any wall time) give contiguous, non-overlapping windows that exactly cover [12:55,14:00); calling it twice gives identical results (no clock is read).
- `TestForPublishOffsetInput`: `now` parsed from `2026-10-12T17:06:00+03:00` yields the same window as `14:06:00Z`.

`internal/window/layout_test.go`:
- `TestLayoutKeys`: for window `2026-10-10T23:55Z`, part 3: `RawWindowPrefix` = `source=coingecko/ingest_date=2026-10-10/window=2355/`; `RawPartKey` = `source=coingecko/ingest_date=2026-10-10/window=2355/part-00003.jsonl`; `RawSuccessKey` = `source=coingecko/ingest_date=2026-10-10/window=2355/_SUCCESS`; `ExportDLQKey` = `stage=export/ingest_date=2026-10-10/window=2355/part-00003.jsonl`; `PublishDLQKey` = `stage=publish/ingest_date=2026-10-10/window=2355/part-00000.jsonl`.
- `TestSuccessMarshal`: an empty window marshals to exactly `{"window_start":"2026-10-10T10:00:00.000Z","window_end":"2026-10-10T10:05:00.000Z","rows":0,"parts":[]}` (also when `NewSuccess` gets `nil` parts); two parts marshal with top-level keys in the order `window_start, window_end, rows, parts`, part keys in the order `key, sha256, rows`, and `rows` = sum of the parts; marshalling twice gives equal bytes; `ParseSuccess(Marshal(s))` round-trips.
- `TestParseSuccessRejects`: an unknown field, trailing data after the object, `rows` ≠ sum of parts, unsorted or duplicate part keys, a sha256 that is uppercase or not 64 hex chars, negative part rows, `window_end` <= `window_start`, a timestamp that is not exactly `xtime.Format` output (e.g. `2026-10-10T10:00:00Z`) — each returns an error.
- `TestLayoutIgnoresLocalZone` (Review Focus 2) — not parallel, it swaps `time.Local`. That is safe only because no test of `internal/window` starts a goroutine (Review Focus 2). Never add `t.Parallel()` or goroutines to this package's tests:

```go
func TestLayoutIgnoresLocalZone(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60) // a half-hour offset: hour alignment must still be UTC
	old := time.Local
	time.Local = ist
	t.Cleanup(func() { time.Local = old })

	start := time.Date(2026, 10, 11, 5, 25, 0, 0, ist) // 2026-10-10T23:55:00Z, as pgx would return it
	w := Window{Start: start, End: start.Add(5 * time.Minute)}
	if got, want := RawPartKey(w, 3), "source=coingecko/ingest_date=2026-10-10/window=2355/part-00003.jsonl"; got != want {
		t.Fatalf("RawPartKey = %q, want %q", got, want)
	}
	b, err := NewSuccess(w, nil).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"window_start":"2026-10-10T23:55:00.000Z","window_end":"2026-10-11T00:00:00.000Z","rows":0,"parts":[]}`; string(b) != want {
		t.Fatalf("manifest %s, want %s", b, want)
	}
	if got := Floor(time.Date(2026, 10, 11, 5, 29, 0, 0, ist), time.Hour); !got.Equal(time.Date(2026, 10, 10, 23, 0, 0, 0, time.UTC)) || got.Location() != time.UTC {
		t.Fatalf("Floor = %v, want 2026-10-10T23:00:00Z in UTC", got)
	}
}
```

`internal/config/config_test.go`:
- Add `"ETL_WINDOW"`, `"ETL_CLAIMS_RETENTION_DAYS"`, `"ETL_PUSHGATEWAY_URL"` to `allKeys`.
- `TestLoadDefaults` (changed): `Window: 5*time.Minute`, `ClaimsRetentionDays: 8`, `PushgatewayURL: "http://pushgateway:9091"`.
- `TestLoadOverrides` (changed): `ETL_WINDOW=15m`, `ETL_CLAIMS_RETENTION_DAYS=30`, `ETL_PUSHGATEWAY_URL=http://pgw:9091/` give `15m`, `30` and `"http://pgw:9091"` (trailing slash trimmed).
- `TestLoadWindow` (new): every allowed value `1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m` loads; `300s` loads as `5m`.
- `TestLoadInvalid` (changed): new cases `ETL_WINDOW` in {`5`, `90s`, `7m`, `2h`, `0s`, `-5m`}, `ETL_CLAIMS_RETENTION_DAYS` in {`0`, `x`, `-1`} and `ETL_PUSHGATEWAY_URL` in {`pushgateway:9091`, `ftp://x`, `http://`} each fail and the error names the variable.
- `TestLoadClaimsRetentionVsTopic` (new): with `ETL_TOPIC_RETENTION=168h`, days `7` fails naming `ETL_CLAIMS_RETENTION_DAYS` and `8` passes; with `169h`, `8` fails and `9` passes; with `24h`, `2` passes and `1` fails; a bad `ETL_TOPIC_RETENTION` (`abc`) reports only `ETL_TOPIC_RETENTION` (exactly one error line).
- `TestLoadPushgatewayURL` (new): `http://pgw:9091/` → `http://pgw:9091`; `https://pgw.example:443/base/` → `https://pgw.example:443/base`.

`internal/obs/push_test.go` (every test against an `httptest.Server` that records method, path, `Content-Type` and body):
- `TestPushJobRunSuccess`: the server sees `POST /metrics/job/etl_retention` with a text `Content-Type`; the body, parsed with `expfmt.NewTextParser(model.UTF8Validation).TextToMetricFamilies`, has `etl_job_success=1`, `etl_job_last_run_timestamp_seconds == etl_job_last_success_timestamp_seconds == Finished` (unix seconds with ms), `etl_job_duration_seconds == Finished-Started`, and `etl_retention_partitions_dropped=3` with no labels.
- `TestPushJobRunFailureKeepsLastSuccess`: `Err != nil` → method is `POST` (not `PUT`), `etl_job_success=0`, and `etl_job_last_success_timestamp_seconds` is absent from the body.
- `TestPushJobRunRejectsUnknownGauge`: `Gauges{"etl_bogus":1}` or an empty `Job` returns an error and the server receives no request.
- `TestPushJobRunHTTPError`: the server answers 500; the error contains `500`.
- `TestReportJobRunLogsAndSurvivesCancelledContext`: with an already-cancelled ctx the server still receives the push; with a closed address, a `slog.NewJSONHandler` on a buffer gets exactly one ERROR record with `"msg":"pushgateway push failed"` and `"job":"etl_vacuum"`; nothing panics.
- `TestPushHelpStringsStable`: two pushes for different jobs carry identical `# HELP etl_job_success …` lines (the Pushgateway rejects a push whose HELP or TYPE for a metric name differs from another group's).

`internal/queue/queue_test.go`:
- `TestMessageKey` (replaced): two events with the same `IdempotencyKey` and different `EventID`/`Endpoint`/`Request.Path` get the same key, equal to the idempotency key; different idempotency keys give different keys; a nil or empty `IdempotencyKey` gives `EventID`.
- `TestMessageKeySpreadsResponsesAcrossPartitions`:

```go
func TestMessageKeySpreadsResponsesAcrossPartitions(t *testing.T) {
	part := kgo.StickyKeyPartitioner(nil).ForTopic("coingecko.raw") // murmur2, the default producer's hasher for keyed records
	seen := map[int]bool{}
	for i := 0; i < 60; i++ {
		payload := []byte(fmt.Sprintf(`[{"id":"coin-%d"}]`, i))
		k := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointMarkets, map[string]string{"vs_currency": "usd"}, event.SHA256Hex(payload))
		p := part.Partition(&kgo.Record{Key: []byte(MessageKey(event.RawEvent{EventID: fmt.Sprintf("e-%d", i), IdempotencyKey: &k}))}, 6)
		seen[p] = true
		cdnCopy := event.RawEvent{EventID: "another-poll", IdempotencyKey: &k}
		if q := part.Partition(&kgo.Record{Key: []byte(MessageKey(cdnCopy))}, 6); q != p {
			t.Fatalf("copies of one response on partitions %d and %d", p, q)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("60 responses hit %d of 6 partitions", len(seen))
	}
}
```

`internal/queue/queue_integration_test.go` (cosmetic, no old key scheme in tests): build the key at :80 as `key := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointCoinDetail, map[string]string{}, event.SHA256Hex(value))` and assert `string(got.Key) == key` at :108.

`internal/transform` tests:
- `TestMarketsRejects` (changed): the two id-less rows are keyed `"#<event id>|0|2026-10-10T10:07:55.000Z"` and `"#<event id>|1|2026-10-10T10:07:55.000Z"` (the `rawEvent` helper sets no `IdempotencyKey`, so the event_id fallback applies).
- `TestMissingIDKeysUniqueAcrossResponses` (new, `keys_test.go`): two detail payloads for coins `bitcoin` and `ethereum` share `last_updated`, have different idempotency keys and an id-less ticker at index 0 — the ticker keys differ and start with `"bitcoin|#"` and `"ethereum|#"`; the same holds for two id-less `coin_snapshot`s and for two markets payloads with an id-less row at index 0.
- `TestMissingIDKeysStableForCopiesOfOneResponse` (new): the same payload with the same `IdempotencyKey` and a different `EventID`/`FetchedAt` yields identical keys for every `missing_id` record, so dedup sees `duplicate_response`.
- `TestTickerDuplicateKeysKeepOrdinals` (changed): `tk[3].Key == "bitcoin|#<event id>|3|2026-10-10T10:01:51.000Z"`.
- `TestDetailMissingIDAndContracts` (changed): the id-less `coin_snapshot` key is `"#<event id>|0|2026-10-10T10:01:51.000Z"`.

`internal/dedup/dedup_test.go`, new subtest `TestClaimAndDecide/overlapping key sets in opposite input order do not deadlock` (integration, TODO item 4). The code is unchanged: `pickWinners` sorts keys (`dedup.go:216-236`) and `insertSQL` keeps that order (`WITH ORDINALITY` + `ORDER BY u.ord`, `:94-103`); the test pins the assumption that `INSERT … SELECT … ORDER BY` fixes the order of inserts and lock acquisition:

```go
t.Run("overlapping key sets in opposite input order do not deadlock", func(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		keys := make([]string, 100)
		for i := range keys {
			keys[i] = fmt.Sprintf("dl-%d-%03d", iter, i) // fresh keys every iteration
		}
		evA, evB := uuid.NewString(), uuid.NewString() // dedup_claims.event_id is a uuid column
		a := claimsFor(keys, 0, evA, "idem-a")            // ascending, partition 0
		b := claimsFor(reversed(keys), 1, evB, "idem-b")  // descending, partition 1, same content sha
		start := make(chan struct{})
		type res struct {
			outs []dedup.Outcome
			err  error
		}
		run := func(cs []dedup.Claim, out chan<- res) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				out <- res{err: err}
				return
			}
			defer tx.Rollback(ctx)
			<-start
			outs, err := dedup.ClaimAndDecide(ctx, tx, cs)
			if err == nil {
				time.Sleep(50 * time.Millisecond) // hold the locks
				err = tx.Commit(ctx)
			}
			out <- res{outs, err}
		}
		ra, rb := make(chan res, 1), make(chan res, 1)
		go run(a, ra)
		go run(b, rb)
		close(start)
		A, B := <-ra, <-rb
		// both commit: no SQLSTATE 40P01; per key exactly one Emit, the other Drop(duplicate_record)
		...
	}
})
```

(`claimsFor`/`reversed` are small helpers in the test file; claims use one event_date whose partition the subtest setup already creates, distinct `EventID`/`IdempotencyKey` per side and an identical `ContentSHA256`.)

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -short -count=1 ./internal/window/ ./internal/config/ ./internal/obs/ ./internal/queue/ ./internal/transform/`
Expected: FAIL — `package coingecko-etl/internal/window` has no Go files / `undefined: obs.PushJobRun`, `cfg.Window undefined`, and `TestMessageKey`/key assertions fail.
Run: `go test -race -count=1 -run 'TestClaimAndDecide' ./internal/dedup/`
Expected: the new subtest PASSES at once (it pins existing behaviour). If it fails with SQLSTATE 40P01, the insert-order assumption is broken: stop and report instead of "fixing" the test.

- [ ] **Step 3: Implement**

`internal/window/window.go`:

```go
// Package window holds the raw-zone window contract shared by the exporter
// (writer) and the publisher (reader): ETL_WINDOW validation and arithmetic
// (windows aligned to the UTC hour), the keys of raw parts, _SUCCESS and the
// window-named DLQ objects, and the _SUCCESS manifest.
package window

// PublishGrace is the fixed delay between a window's end and the publish run
// that reads it: Cronicle runs etl publish one minute after every window end.
const PublishGrace = time.Minute

// Window is the half-open interval [Start, End) in UTC.
type Window struct{ Start, End time.Time }

var errSize = errors.New("must be whole minutes from 1m to 60m that divide 60m (1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m)")

// Validate reports whether size can be ETL_WINDOW. The order of the checks
// matters: size < 1m first, so time.Hour%size never divides by zero.
func Validate(size time.Duration) error {
	if size < time.Minute || size > time.Hour || size%time.Minute != 0 || time.Hour%size != 0 {
		return errSize
	}
	return nil
}

// Floor returns the start of the size window containing t, in UTC.
func Floor(t time.Time, size time.Duration) time.Time {
	u := t.UTC()
	h := u.Truncate(time.Hour)
	return h.Add(u.Sub(h) / size * size)
}

// Ceil returns the first window boundary at or after t, in UTC.
func Ceil(t time.Time, size time.Duration) time.Time {
	f := Floor(t, size)
	if f.Equal(t) {
		return f
	}
	return f.Add(size)
}

// ForPublish is the window a publish run at now handles: it ends at
// Floor(now - PublishGrace) and lasts size. It never reads the clock, so a
// catch-up run with an old JOB_NOW takes the old window.
func ForPublish(now time.Time, size time.Duration) Window {
	end := Floor(now.Add(-PublishGrace), size)
	return Window{Start: end.Add(-size), End: end}
}

func (w Window) Date() string { return xtime.FormatDate(w.Start) }   // UTC date of the start
func (w Window) HHMM() string { return w.Start.UTC().Format("1504") } // UTC time of the start
```

`internal/window/layout.go`:

```go
// RawWindowPrefix is the prefix (in bucket raw) of every object of w.
func RawWindowPrefix(w Window) string {
	return fmt.Sprintf("source=%s/ingest_date=%s/window=%s/", event.SourceCoinGecko, w.Date(), w.HHMM())
}
func RawPartKey(w Window, part int) string { return RawWindowPrefix(w) + fmt.Sprintf("part-%05d.jsonl", part) }
func RawSuccessKey(w Window) string        { return RawWindowPrefix(w) + "_SUCCESS" }

// ExportDLQKey (bucket dlq): part is the index of the raw part whose lines failed.
func ExportDLQKey(w Window, part int) string {
	return fmt.Sprintf("stage=export/ingest_date=%s/window=%s/part-%05d.jsonl", w.Date(), w.HHMM(), part)
}

// PublishDLQKey (bucket dlq): one object per window.
func PublishDLQKey(w Window) string {
	return fmt.Sprintf("stage=publish/ingest_date=%s/window=%s/part-00000.jsonl", w.Date(), w.HHMM())
}

// SuccessPart is one raw part listed in _SUCCESS. Key is the object key inside
// bucket raw; Rows counts the JSONL lines of the part (lines the exporter sent
// to its DLQ are not in the part and not counted).
type SuccessPart struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Rows   int    `json:"rows"`
}

// Success is the _SUCCESS manifest. encoding/json writes struct fields in
// declaration order, which is the contract's key order.
type Success struct {
	WindowStart string        `json:"window_start"`
	WindowEnd   string        `json:"window_end"`
	Rows        int           `json:"rows"`
	Parts       []SuccessPart `json:"parts"`
}

// NewSuccess builds the manifest of w; parts must already be in part-index order.
func NewSuccess(w Window, parts []SuccessPart) Success {
	s := Success{WindowStart: xtime.Format(w.Start), WindowEnd: xtime.Format(w.End), Parts: []SuccessPart{}}
	for _, p := range parts {
		s.Rows += p.Rows
		s.Parts = append(s.Parts, p)
	}
	return s
}

// Marshal returns compact JSON without a trailing newline; Parts is never null.
func (s Success) Marshal() ([]byte, error) {
	if s.Parts == nil {
		s.Parts = []SuccessPart{}
	}
	return json.Marshal(s)
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ParseSuccess decodes and checks a manifest: no unknown fields and no trailing
// data; window_start/window_end are exact xtime.Format values with end > start;
// every part has a lowercase 64-hex sha256 and rows >= 0; keys strictly
// increase; rows equals the sum of the parts' rows.
func ParseSuccess(b []byte) (Success, error) { ... }
```

(For the trailing-data check decode once, then require `dec.Decode(&json.RawMessage{}) == io.EOF`. For the timestamps, `t, err := xtime.Parse(s)` and require `xtime.Format(t) == s`.)

`internal/config/config.go` (comments in the existing style):

```go
	PushgatewayURL      string        // ETL_PUSHGATEWAY_URL, default "http://pushgateway:9091" (trailing "/" trimmed)
	ClaimsRetentionDays int           // ETL_CLAIMS_RETENTION_DAYS, default 8; >= ceil(TopicRetention/24h)+1
	Window              time.Duration // ETL_WINDOW, default 5m; whole minutes in [1m,60m] dividing 60m; aligned to the UTC hour
```

Place `PushgatewayURL` after `HTTPAddr`, `ClaimsRetentionDays` after `TopicRetention`, `Window` after `ExportBatchMax`. In `Load`: `Window: p.windowSize("ETL_WINDOW", 5*time.Minute)`, `ClaimsRetentionDays: p.int("ETL_CLAIMS_RETENTION_DAYS", 8)`, `PushgatewayURL: p.httpURL("ETL_PUSHGATEWAY_URL", "http://pushgateway:9091")`. Leave `ExportInterval`, `PublishLease` and `PublishPoll` alone: T3 and T4 own them. Do not add `ETL_SCHEDULE_TZ` (only the Cronicle provisioning script reads it).

```go
// windowSize parses ETL_WINDOW (named so it does not read like the window package).
func (p *parser) windowSize(key string, def time.Duration) time.Duration {
	v, ok := p.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, v, "not a Go duration (examples: 1m, 5m, 15m)")
		return d
	}
	if err := window.Validate(d); err != nil {
		p.fail(key, v, err.Error())
	}
	return d
}

// httpURL parses an absolute http(s) URL with a host; the trailing "/" is trimmed.
func (p *parser) httpURL(key, def string) string {
	v := p.str(key, def)
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.fail(key, v, "must be an absolute http(s) URL, e.g. http://pushgateway:9091")
	}
	return strings.TrimRight(v, "/")
}
```

Cross-field check after the struct literal and before `if len(p.errs) > 0`. It runs only when both inputs parsed, so a bad input reports one error, not two:

```go
	if c.ClaimsRetentionDays > 0 && c.TopicRetention > 0 {
		const day = 24 * time.Hour
		minDays := int((c.TopicRetention+day-1)/day) + 1 // 168h -> 8, 169h -> 9, 24h -> 2
		if c.ClaimsRetentionDays < minDays {
			p.errs = append(p.errs, fmt.Errorf("ETL_CLAIMS_RETENTION_DAYS=%d: must be at least %d (ETL_TOPIC_RETENTION=%s in whole days, rounded up, plus 1)",
				c.ClaimsRetentionDays, minDays, c.TopicRetention))
		}
	}
```

Every subcommand calls `Load`, so every one refuses to start with a bad value (TODO item 2: «отказ стартовать»).

`.env.example`, after `ETL_EXPORT_INTERVAL=15s` (do not add `ETL_PUSHGATEWAY_URL`: it is compose-internal like `ETL_PG_DSN`, and the Go default already names the compose service; T7 adds `ETL_SCHEDULE_TZ`):

```
# Width of an export/publish window (raw/.../window=HHMM/). Whole minutes that
# divide an hour: 1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m.
ETL_WINDOW=5m
# Days of dedup claims (and written transformer batch rows) kept by
# `etl maintenance retention`. At least the topic retention in days + 1.
ETL_CLAIMS_RETENTION_DAYS=8
```

`internal/obs/push.go`:

```go
const (
	JobPublisher = "etl_publisher"
	JobRetention = "etl_retention"
	JobVacuum    = "etl_vacuum"

	MetricPublishRecords             = "etl_publish_records"
	MetricPublishDLQLines            = "etl_publish_dlq_lines"
	MetricRetentionPartitionsDropped = "etl_retention_partitions_dropped"
	MetricRetentionBatchesDeleted    = "etl_retention_batches_deleted"
)

// jobGaugeHelp is the only allowed set of job-specific gauges. HELP strings are
// constants: the Pushgateway rejects a push whose HELP or TYPE for a metric name
// differs from another group's.
var jobGaugeHelp = map[string]string{
	MetricPublishRecords:             "Records the last publish run sent to the broker.",
	MetricPublishDLQLines:            "Raw lines the last publish run wrote to the publish DLQ.",
	MetricRetentionPartitionsDropped: "dedup_claims partitions the last retention run dropped.",
	MetricRetentionBatchesDeleted:    "Written consumer_batches rows the last retention run deleted.",
}

// JobRun is the outcome of one batch-job run. Gauges are the job-specific
// metrics the job body returned: etl publish returns them only on success,
// retention also on failure (what it really removed).
type JobRun struct {
	Job      string             // JobPublisher | JobRetention | JobVacuum (grouping key job=...)
	Started  time.Time
	Finished time.Time
	Err      error              // nil = success
	Gauges   map[string]float64 // names above; nil = none
}

// PushJobRun sends run to the Pushgateway at baseURL with POST (push "Add"):
// only the metric names in this push are replaced in group {job=run.Job}, so a
// failed run, which omits etl_job_last_success_timestamp_seconds, keeps the
// last success. Unknown gauge names or an empty Job are rejected before any
// HTTP call.
func PushJobRun(ctx context.Context, baseURL string, run JobRun) error {
	if run.Job == "" {
		return errors.New("obs: push: empty job name")
	}
	for name := range run.Gauges {
		if _, ok := jobGaugeHelp[name]; !ok {
			return fmt.Errorf("obs: push: unknown job gauge %q", name)
		}
	}
	reg := prometheus.NewRegistry() // never the process registry
	gauge := func(name, help string, v float64) {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		g.Set(v)
		reg.MustRegister(g)
	}
	finished := float64(run.Finished.UnixMilli()) / 1000
	gauge("etl_job_last_run_timestamp_seconds", "Unix time the last run of this job finished.", finished)
	if run.Err == nil {
		gauge("etl_job_last_success_timestamp_seconds", "Unix time the last successful run of this job finished.", finished)
	}
	gauge("etl_job_duration_seconds", "Wall time of the last run of this job.", run.Finished.Sub(run.Started).Seconds())
	success := 0.0
	if run.Err == nil {
		success = 1
	}
	gauge("etl_job_success", "1 if the last run of this job succeeded, 0 if it failed.", success)
	for name, v := range run.Gauges {
		gauge(name, jobGaugeHelp[name], v)
	}
	return push.New(baseURL, run.Job).Gatherer(reg).
		Client(&http.Client{Timeout: 10 * time.Second}).
		Format(expfmt.NewFormat(expfmt.TypeTextPlain)).
		AddContext(ctx)
}

// ReportJobRun pushes run with a 10 s timeout on context.WithoutCancel(ctx), so
// a run stopped by SIGTERM still reports, and logs a failure at ERROR
// ("pushgateway push failed", job, url, error). It never changes the job's outcome.
func ReportJobRun(ctx context.Context, log *slog.Logger, baseURL string, run JobRun) {
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := PushJobRun(pctx, baseURL, run); err != nil {
		log.Error("pushgateway push failed", "job", run.Job, "url", baseURL, "error", err)
	}
}
```

The metrics carry no labels (the push package refuses a `job` label); the grouping key is the job only.

`internal/queue/queue.go:31-46` (keep the signature):

```go
// MessageKey is the partitioning key: the event's idempotency_key (sha256 of
// request + body). Copies of one response (CDN repeats, publisher re-runs)
// share it and land on one partition, so one transformer decides them in
// order. Different responses spread over all partitions (franz-go's default
// partitioner hashes keys with murmur2, like the Java client). Every ok event
// has one; the event_id fallback only keeps the key deterministic and non-empty.
func MessageKey(ev event.RawEvent) string {
	if ev.IdempotencyKey != nil && *ev.IdempotencyKey != "" {
		return *ev.IdempotencyKey
	}
	return ev.EventID
}
```

Partitioner, partition count (6) and `NewProducer` stay unchanged here (T4 changes `NewProducer`).

`internal/transform/keys.go`:

```go
// responseID identifies the response a record came from: its idempotency_key
// (the same for byte-identical copies, so replays and CDN repeats still dedup
// as replay/duplicate_response), else its event_id.
func responseID(ev event.RawEvent) string {
	if ev.IdempotencyKey != nil && *ev.IdempotencyKey != "" {
		return *ev.IdempotencyKey
	}
	return ev.EventID
}

// fallbackKey is the natural key of a record with no id of its own (rejected
// as missing_id): "#<response id>|<index in payload>|<snapshot_at>". CoinGecko
// stamps every coin of one pass with the same last_updated, so the index alone
// is unique only within one message.
func fallbackKey(respID string, i int, snapAt string) string {
	return "#" + respID + "|" + strconv.Itoa(i) + "|" + snapAt
}
```

Call sites:
- `markets.go:129-131`: `rec.Key = fallbackKey(responseID(ev), i, snapAt)`; replace the comment at :130 with `// No coin id to key on: the response id and the row's position keep the key unique.`; drop the `strconv` import (used only at :131).
- `detail.go`: `detailBuilder` gains `respKey string // responseID(ev): keys records that have no id`, set in `transformDetail` (:221-227) to `responseID(ev)`; :236 becomes `b.add(DatasetCoinSnapshot, fallbackKey(b.respKey, 0, b.snapAt), ZoneRejected, ReasonMissingID, parent)`; :460 becomes `key = b.coinID + "|" + fallbackKey(b.respKey, i, b.snapAt)` (the coin id stays first, like every other detail key). Remove the `fmt` import only if nothing else uses it.

Why not the coin id alone (TODO item 5's first suggestion): two polls of one coin with the same `last_updated` but a changed ticker list would still collide at index i (`snapshot_conflict`). Why not event_id alone: byte-identical responses would write the same `missing_id` row twice to rejected/ instead of dropping it as `duplicate_response`. Transform stays pure. `Record.Key` is used only for dedup claims and the DLQ origin's `record_key` (`transformer/batch.go:428,436`); no reader parses it.

`migrations/00002_pipeline.sql` (TODO item 5):
- :37 `idempotency_key text, -- winner's idempotency key, for the duplicate_response drop reason; NULL when absent` (the `(Task 14)` history goes).
- After the `dedup_claims` table: `COMMENT ON COLUMN pipeline.dedup_claims.first_seen_at IS 'Audit only: when the key was first claimed. No code reads it.';`
- :43 `-- Claims by Kafka position, for manual repair (ADR 0004); no pipeline code queries it.`

- [ ] **Step 4: Run the targeted tests**

Run: `go test -short -count=1 ./internal/window/ ./internal/config/ ./internal/obs/ ./internal/queue/ ./internal/transform/`
Expected: PASS.

- [ ] **Step 5: Lint and integration tests**

```bash
go mod tidy && git diff --stat go.mod go.sum   # only indirect->direct moves for prometheus/common and client_model
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
go test -race -count=1 -run 'TestClaimAndDecide' ./internal/dedup/...          # Docker
go test -race -p 2 -count=1 -timeout 15m ./internal/publisher/... ./internal/transformer/... ./internal/queue/... ./internal/storage/pg/...   # Docker; publisher_integration_test.go:205 still finds r.Key == queue.MessageKey(ev)
make test-unit
ETL_WINDOW=7m go run ./cmd/etl fetcher; echo $?                 # prints the ETL_WINDOW error, exit 1
ETL_CLAIMS_RETENTION_DAYS=7 go run ./cmd/etl fetcher; echo $?   # names ETL_CLAIMS_RETENTION_DAYS, exit 1
```

- [ ] **Step 6: Remove what became unused**

- `internal/queue/queue.go:34-46`: the path-parsing body of `MessageKey` (coins_markets / last path segment / endpoint fallback), its doc comment :31-33 and the `"strings"` import :9 (keep it only if something else still uses it: `grep -n 'strings\.' internal/queue/queue.go`).
- `internal/queue/queue_test.go:12-31`: the path-based cases (markets, detail, api prefix, trailing slash, empty path).
- `internal/transform/markets.go:130-131`: the old comment and the `"#"+strconv.Itoa(i)` key; the `strconv` import.
- `internal/transform/detail.go:236` `"#0|"+b.snapAt` and :460 `fmt.Sprintf("#%d|%s", i, b.snapAt)`.
- `internal/transform/markets_test.go:223-224`: the old expected keys.

```bash
grep -rn '"#" + strconv\|"#0|"\|#%d|%s' internal/transform; test $? -eq 1          # no matches
grep -rn 'coins_markets\|coin id\|Request.Path' internal/queue/queue.go; test $? -eq 1
```

- [ ] **Step 7: Commit**

```bash
git add internal/window internal/config/config.go internal/config/config_test.go .env.example \
  internal/obs/push.go internal/obs/push_test.go internal/queue/queue.go internal/queue/queue_test.go \
  internal/queue/queue_integration_test.go internal/transform internal/dedup/dedup_test.go \
  migrations/00002_pipeline.sql go.mod go.sum
git commit -m "refactor: add the window contract, job metrics push and idempotency_key message keys" \
  -m "internal/window holds ETL_WINDOW validation, hour-aligned window arithmetic, the raw/DLQ keys and the _SUCCESS manifest. Config gains ETL_WINDOW, ETL_CLAIMS_RETENTION_DAYS (>= topic days + 1) and ETL_PUSHGATEWAY_URL. obs pushes batch-job results with POST. Kafka messages are keyed by idempotency_key; id-less records are keyed by their response id, so keys of different coins and responses no longer collide." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** `config.Load` rejects every `ETL_WINDOW` outside {1,2,3,4,5,6,10,12,15,20,30,60} minutes and every `ETL_CLAIMS_RETENTION_DAYS` below ceil(ETL_TOPIC_RETENTION/24h)+1, naming the variable; defaults are 5m, 8, `http://pushgateway:9091`. `window.Validate` is the single window validator. `obs.PushJobRun`/`ReportJobRun` push with POST to `/metrics/job/<job>` exactly the contract's metric names; last_success only on success; a push failure is logged at ERROR and never returned by `ReportJobRun`. `queue.MessageKey` returns the idempotency key; no code derives keys from `Request.Path`. No `missing_id` key has the form `#<i>|<snapshot_at>`; id-less records of different responses never share a key and copies of one response always do. The dedup opposite-order subtest passes. Nothing unused remains (no path-based key code, dead imports or stale comments about coins_markets/coin id keys in `internal/queue`).

---

### Task 2: Exporter claim — `FOR UPDATE SKIP LOCKED` + `export_batch_id` instead of the xid8 cursor

Replaces the `(ingest_txid, seq)` cursor bounded by `pg_snapshot_xmin` with a per-row claim (TODO item 1, variant (a): one exporter under the advisory lock). One short transaction locks unexported ok rows (`FOR UPDATE SKIP LOCKED`), inserts the ledger row, then stamps `raw.api_calls.export_batch_id`. A late-committing row is picked up by a later batch, an open transaction no longer stalls the export, and a retry reads the same rows back by batch id and writes the same bytes. Object naming stays as today (`source=coingecko/ingest_date=<claim date>/batch=<%020d>.jsonl`); no windows yet (T3).

**Files:**
- Modify: `migrations/00001_raw.sql` — line 3 comment becomes `Rows are written before any deduplication; export_batch_id is the only column changed after insert (set once by the exporter).`; delete line 20 (`ingest_txid xid8 … DEFAULT pg_current_xact_id()`), line 26 (its COMMENT) and line 28 (`api_calls_export_cursor_idx`); add `export_batch_id bigint,` after `payload_sha256` (nullable, no default; the FK is declared in 00002 because the ledger table does not exist yet); add `COMMENT ON COLUMN raw.api_calls.export_batch_id IS 'pipeline.export_batches.batch_id of the batch that exported this row; NULL = not exported yet (rows with outcome <> ok stay NULL forever).';` and the two partial indexes below. Edited in place: an existing pgdata volume needs `make clean`.
- Modify: `migrations/00002_pipeline.sql` — lines 5-22 (`export_batches`): drop `from_txid`, `from_seq`, `to_txid`, `to_seq`, `watermark_xmin`, `UNIQUE (from_txid, from_seq)` and the row CHECK; add `min_fetched_at`/`max_fetched_at NOT NULL` with `CHECK (max_fetched_at >= min_fetched_at)`; keep every other column and `export_batches_unpublished_idx` (T4 removes them). Right after `CREATE TABLE export_batches` add the FK constraint. Rewrite the comment at lines 67-70. Down section (84-98): add `ALTER TABLE raw.api_calls DROP CONSTRAINT IF EXISTS api_calls_export_batch_fk;` before `DROP TABLE IF EXISTS pipeline.export_batches` (otherwise Down fails on the FK).
- Modify: `internal/exporter/exporter.go` — package doc (1-12); `batch` struct (50-63) loses `FromTxid/FromSeq/ToTxid/ToSeq`, gains `MinFetchedAt`, `MaxFetchedAt`; `claimedBatches` (192-209) new SELECT list; `claimNext` (211-281) becomes `claimNext` plus the seam `claimIn`; delete `querier` and `lastBatchEnd` (283-310); `readRange` (407-429) becomes `readBatch`; `updateLag` (501-525) becomes the LEAST query; `exportBatch` (312-387) changes only the `readRange`→`readBatch` call. `Run`, `acquireLock`, `releaseLock`, `put`, `buildLine`, `newDLQ`, `marshalDLQ`, `rowToEvent`, `joinLines`, `rawObjectKey`, `dlqObjectKey` stay unchanged.
- Modify: `internal/exporter/exporter_integration_test.go` — about 85% of the work: delete the xid8 helpers and tests, adapt helpers and existing tests, add the new tests.
- Modify: `internal/storage/pg/pg_integration_test.go` — schema assertions in subtest `MigrateTwiceIsIdempotent` (34-64).
- Modify: `internal/bootstrap/bootstrap_integration_test.go:152-154` — the hand-written ledger INSERT.
- Modify: `internal/publisher/publisher_integration_test.go:285-287` (INSERT) and :269-270 (comment).
- Modify: `deploy/prometheus/alerts.yml:62` and `deploy/prometheus/alerts_test.yml:74, 90` — `ETLExportLagHigh` description.

**Interfaces:**
- Consumes: `pg.Migrate`, `pg.EnsurePartitions` (unchanged); `s3.Client.PutIfAbsent` semantics (unchanged).
- Produces: `raw.api_calls.export_batch_id bigint REFERENCES pipeline.export_batches(batch_id)` (constraint `api_calls_export_batch_fk`, declared in 00002); indexes `api_calls_unexported_idx (seq) WHERE outcome = 'ok' AND export_batch_id IS NULL` and `api_calls_export_batch_idx (export_batch_id, seq) WHERE export_batch_id IS NOT NULL`; `pipeline.export_batches` columns `batch_id, row_count, min_fetched_at, max_fetched_at, object_key, content_sha256, status, claimed_at, exported_at, publish_lease_until, published_at`; package-internal `func claimIn(ctx context.Context, tx pgx.Tx, max int) (batch, bool, error)`, `func claimNext(ctx context.Context, pool *pgxpool.Pool, max int) (batch, bool, error)`, `func readBatch(ctx context.Context, pool *pgxpool.Pool, b batch) ([]apiRow, error)`; `exporter.ExportOnce(ctx, pool, s3c, reg, m, log, max int) (int, error)` and `exporter.Run` keep their signatures.

- [ ] **Step 1: Write the failing tests**

In `internal/exporter/exporter_integration_test.go` (Docker):
- `TestExportOnceExportsEveryRowExactlyOnce` (keep, extend): with `max=2`, `ExportOnce` returns 2, 1, 0; the 3 ids are exported once each; `s3_put{created}=2`; lag 0; the `http_error` row still has `export_batch_id IS NULL`.
- `TestExportOnceResumesAfterCrash` (adapt): after `claimNext` (crash before PUT) both rows carry `export_batch_id = b.ID`; a third row inserted afterwards is not in b's object and lands in the next batch (`ExportOnce` returns 3); resetting b to `'claimed'` (crash after PUT) gives `s3_put{exists}=1` and byte-identical object bytes.
- `TestExportOnceExportsLateCommitInLaterBatch` (replaces `TestExportOnceNeverSkipsOutOfOrderCommits`): txA inserts A and stays open; B is committed. `ExportOnce` returns 1 and batch 1 holds only B. After `commit A`, `ExportOnce` returns 1 and batch 2 holds only A. Exported ids = [B, A], then 0; nothing lost.
- `TestExportOnceNotStalledByOpenTransaction`: a transaction holding an xid (`SELECT pg_current_xact_id()`) stays open while 3 rows are committed; `ExportOnce` returns 3 and `etl_export_lag_seconds` is 0 while it is still open.
- `TestConcurrentClaimsAreDisjoint`: with 6 rows, tx1 holds `claimIn(ctx, tx1, 3)`; `claimNext(pool, 500)` under a 5 s deadline returns 3 rows within 2 s, and the two stamped id sets are disjoint; after tx1 commits, `ExportOnce` exports both batches: 6 unique ids.

```go
func TestConcurrentClaimsAreDisjoint(t *testing.T) {
	e := setup(t) // existing helper: Postgres + versitygw, migrated, partitions today-1..today+3
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	}
	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)
	b1, ok, err := claimIn(ctx, tx1, 3) // rows locked, not committed
	if err != nil || !ok || b1.RowCount != 3 {
		t.Fatalf("claimIn: %v %v %+v", err, ok, b1)
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	b2, ok, err := claimNext(dctx, e.pool, 500) // SKIP LOCKED: must not wait for tx1
	if err != nil || !ok || b2.RowCount != 3 || time.Since(start) > 2*time.Second {
		t.Fatalf("claimNext: %v %v %+v after %v", err, ok, b2, time.Since(start))
	}
	// the two stamped id sets are disjoint:
	// SELECT event_id::text, export_batch_id FROM raw.api_calls WHERE export_batch_id IN ($1, $2)
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// ExportOnce exports both batches; exportedEventIDs returns 6 unique ids
}
```

- `TestExportsRowInPartitionCreatedAfterClaim`: claim a row today; `EnsurePartitions(now+10d, 0)`; insert a row in the new partition; `ExportOnce` returns 2, both rows are exported, and the new partition has both partial indexes (`pg_indexes` count = 2 with `export_batch_id` in `indexdef`).
- `TestExportLagGrowsWhileClaimedBatchIsStuck`: a row fetched `now-10m` (the insert helpers stamp `time.Now()`, so add a `fetchedAt` variant here; T3 makes every insert helper take `fetchedAt`); `claimNext`; put other bytes at `b.ObjectKey`. `ExportOnce` errors with a checksum mismatch and lag >= 590. After a fresh row is inserted, `ExportOnce` errors again, lag is still >= 590, b stays `'claimed'` and `s3_put{mismatch}` >= 1.
- `TestExportOnceBatchesInSeqOrder` (replaces the batching half of the straddle test): 9 rows with `max=4` → `ExportOnce` returns 4, 4, 1, 0; exported ids equal the insert order; each ledger `row_count` equals `count(*) WHERE export_batch_id = batch_id`.
- `TestExportOnceSendsInvalidRowsToDLQ` (keep): unchanged assertions, plus the bad row has `export_batch_id = l[0].id` (never claimed again).
- `TestExportOnceCountsClaimFailures` (adapt): after renaming `raw.api_calls`, `ExportOnce` fails with an error containing `"pick rows"` (was `"select range"`); `export_batches_total{error}=1`.
- Helper `exportedEventIDs` (extend): for every ledger row, `row_count = SELECT count(*) FROM raw.api_calls WHERE export_batch_id = batch_id`.

Elsewhere:
- `internal/storage/pg/pg_integration_test.go` `MigrateTwiceIsIdempotent` (extend): `raw.api_calls` has `export_batch_id bigint` and no `ingest_txid`; indexes `api_calls_unexported_idx` and `api_calls_export_batch_idx` exist on the parent; constraint `api_calls_export_batch_fk` exists; `pipeline.export_batches` has `min_fetched_at` and `max_fetched_at` and no `from_txid`/`to_txid`/`watermark_xmin`.
- `internal/bootstrap/bootstrap_integration_test.go:152-154` and `internal/publisher/publisher_integration_test.go:285-287`: the hand-made INSERTs use the new column list (same assertions as before).
- `deploy/prometheus/alerts_test.yml:74, 90`: both `ETLExportLagHigh` cases expect the new description verbatim (promtool compares annotations exactly).
- `internal/exporter/exporter_test.go` stays unchanged (`TestObjectKeys`, `TestJoinLines`, `TestRowToEventKeepsPayloadVerbatim`).

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -race -count=1 -timeout 15m ./internal/exporter/ ./internal/storage/pg/`
Expected: FAIL — `undefined: claimIn`, schema assertions fail (`export_batch_id` missing).

- [ ] **Step 3: Implement**

Schema (target state after T2). `00001_raw.sql`, inside `CREATE TABLE raw.api_calls`: `export_batch_id bigint,` after `payload_sha256`; `ingest_txid` is gone. After the table:

```sql
-- Exporter claim and lag: unexported ok rows in seq order. Small in steady
-- state; behaves like a queue index (dead entries until vacuum).
CREATE INDEX api_calls_unexported_idx ON raw.api_calls (seq) WHERE outcome = 'ok' AND export_batch_id IS NULL;
-- Exporter read-back of one batch.
CREATE INDEX api_calls_export_batch_idx ON raw.api_calls (export_batch_id, seq) WHERE export_batch_id IS NOT NULL;
CREATE INDEX api_calls_idempotency_key_idx ON raw.api_calls (idempotency_key);   -- unchanged
```

Partial indexes on the partitioned parent are cloned to every partition, including partitions created later (prototype verified on 18.6). The second index also serves the FK's RI check when a ledger row is deleted.

`00002_pipeline.sql`:

```sql
CREATE TABLE pipeline.export_batches (
  batch_id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  row_count           int    NOT NULL CHECK (row_count > 0),
  -- min/max fetched_at of the claimed rows: partition pruning for the read-back,
  -- and the lag of a stuck batch.
  min_fetched_at      timestamptz NOT NULL,
  max_fetched_at      timestamptz NOT NULL,
  object_key          text   NOT NULL UNIQUE,
  content_sha256      bytea,
  status              text   NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','exported')),
  claimed_at          timestamptz NOT NULL DEFAULT now(),
  exported_at         timestamptz,
  publish_lease_until timestamptz,
  published_at        timestamptz,
  CHECK (max_fetched_at >= min_fetched_at)
);

ALTER TABLE raw.api_calls ADD CONSTRAINT api_calls_export_batch_fk
  FOREIGN KEY (export_batch_id) REFERENCES pipeline.export_batches (batch_id);
```

(`export_batches_unpublished_idx` unchanged.) New comment replacing lines 67-70 (the DO block itself stays unchanged): `Server-side guards against forgotten sessions: a session left idle in a transaction keeps its row locks and keeps VACUUM from removing dead row versions (every exported raw row leaves one behind after the exporter's claim). The exporter's SKIP LOCKED claim itself never waits for such a session. Applied to the current database whatever it is called; takes effect for new sessions. transaction_timeout exists from PostgreSQL 17 on, hence the version guard.`

Go, `batch` struct:

```go
type batch struct {
	ID                         int64
	RowCount                   int
	MinFetchedAt, MaxFetchedAt time.Time
	ObjectKey                  string
	SHA256                     []byte
	ClaimedAt                  time.Time
}
```

`claimedBatches` SQL:

```sql
SELECT batch_id, row_count, min_fetched_at, max_fetched_at, object_key, content_sha256, claimed_at
  FROM pipeline.export_batches WHERE status = 'claimed' ORDER BY batch_id
```

`claimNext(ctx, pool, max) (batch, bool, error)`: `pool.Begin`; `defer tx.Rollback`; `b, ok, err := claimIn(ctx, tx, max)`; return early if `err != nil || !ok` (the rollback releases the row locks); `tx.Commit`; `return b, true, nil`.

`claimIn(ctx context.Context, tx pgx.Tx, max int) (batch, bool, error)` is the test seam. These statements run in order, all in tx, with no S3 I/O:

1. Pick. Keep the predicates as LITERALS: with `outcome = $n` pgx's generic plan seq-scans every partition (prototype: 6,891 buffers, 20 ms). Error wrap `"exporter: pick rows: %w"`; collect `ids []string`, `ts []time.Time`.

```sql
SELECT event_id::text, fetched_at FROM raw.api_calls
 WHERE outcome = 'ok' AND export_batch_id IS NULL
 ORDER BY seq
 LIMIT $1
   FOR UPDATE SKIP LOCKED
```

2. If `len(ids) == 0` return `batch{}, false, nil` — no `nextval`, so batch ids stay dense.
3. `SELECT nextval(pg_get_serial_sequence('pipeline.export_batches', 'batch_id')), now()` → `b.ID`, `b.ClaimedAt`; `b.ObjectKey = rawObjectKey(b.ID, b.ClaimedAt)` as today; `b.MinFetchedAt`/`b.MaxFetchedAt` computed in Go over `ts`; `b.RowCount = len(ids)`.
4. Ledger first (the FK rejects a stamp that points at a missing batch):

```sql
INSERT INTO pipeline.export_batches (batch_id, row_count, min_fetched_at, max_fetched_at, object_key, claimed_at)
VALUES ($1, $2, $3, $4, $5, $6)
```

5. Stamp. Join on the PK, never on seq (seq uniqueness cannot be enforced on the partitioned table). Repeat the IS NULL guard. The redundant BETWEEN prunes partitions: this exact form was verified on PG 18.6 with `plan_cache_mode=force_generic_plan` at 32 partitions ("Subplans Removed: 31", a nested loop over `api_calls_pYYYYMMDD_pkey`, 5 ms for 300 rows); without the BETWEEN the prototype hash-joined against a seq scan of every partition (60 ms).

```sql
UPDATE raw.api_calls a
   SET export_batch_id = $1
  FROM unnest($2::text[]::uuid[], $3::timestamptz[]) AS p(event_id, fetched_at)
 WHERE a.event_id = p.event_id
   AND a.fetched_at = p.fetched_at
   AND a.export_batch_id IS NULL
   AND a.fetched_at BETWEEN $4 AND $5
```

with args `(b.ID, ids, ts, b.MinFetchedAt, b.MaxFetchedAt)`. The rows are locked by step 1, so `tag.RowsAffected()` must equal `len(ids)`; otherwise `return fmt.Errorf("exporter: batch %d: locked %d rows, stamped %d", ...)` and the transaction rolls back. Every committed ledger `row_count` therefore equals the rows actually stamped (TODO: "row_count = rows actually stamped") while the ledger row still comes first.

`readBatch(ctx, pool, b)` replaces `readRange`; the scan list is unchanged:

```sql
SELECT seq, event_id::text, idempotency_key, schema_version, source, endpoint, request::text,
       fetched_at, outcome, http::text, error, payload::text, encode(payload_sha256, 'hex')
  FROM raw.api_calls
 WHERE export_batch_id = $1
   AND fetched_at BETWEEN $2 AND $3
 ORDER BY api_calls.seq, api_calls.event_id, api_calls.fetched_at
```

with args `(b.ID, b.MinFetchedAt, b.MaxFetchedAt)`. The ORDER BY MUST name the table columns: a bare `event_id` resolves to the text output column `event_id::text` — the same trap that caused the old 999/1000 cursor bug. Qualified, the sort is (seq, then the PK), a total order. `exportBatch`'s checks stay: `len(rows) == b.RowCount`, the `content_sha256` store-or-compare, the DLQ PUT, the raw PUT and the mark `WHERE status = 'claimed'`.

`updateLag(ctx, pool, m, log)` — one query, "oldest ok row not yet exported, rows of batches still 'claimed' included" (interim: T3 replaces it with the window schedule):

```sql
SELECT COALESCE(EXTRACT(EPOCH FROM now() - LEAST(
         (SELECT min(fetched_at) FROM raw.api_calls WHERE outcome = 'ok' AND export_batch_id IS NULL),
         (SELECT min(min_fetched_at) FROM pipeline.export_batches WHERE status = 'claimed'))), 0)::float8
```

LEAST ignores NULLs; both NULL → 0. Clamp negatives to 0 (clock skew), as today. Not the bare `export_batch_id IS NULL` form (it hides a stuck claimed batch); not `LEFT JOIN … OR status='claimed'` (it seq-scans all history). Measured on 18.6: 0.47 ms at 32 partitions, using the partial index.

`ExportOnce` keeps its signature and flow (resume claimed batches in batch_id order, then `claimNext`, then `exportBatch`); `Run`, the advisory lock and the `conn.Ping` check are unchanged. New package doc: (1) a claim transaction locks unexported ok rows FOR UPDATE SKIP LOCKED, inserts the `pipeline.export_batches` row (object key, row count, fetched_at bounds) and stamps `export_batch_id` in the same transaction, before any S3 I/O, so a retry after a crash reads the same rows back (`WHERE export_batch_id = id ORDER BY seq, PK`) and rebuilds the same bytes; (2) a row that commits late is still NULL and goes into a later batch, so nothing is skipped and nothing waits for a long transaction; (3) PutIfAbsent (If-None-Match: *) as before. One line on the cost: one non-HOT UPDATE per exported row.

Hand-made INSERTs:
- `bootstrap_integration_test.go:152-154`: `INSERT INTO pipeline.export_batches (row_count, min_fetched_at, max_fetched_at, object_key) VALUES (1, now(), now(), 'source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl')`.
- `publisher_integration_test.go:285-287`: `INSERT INTO pipeline.export_batches (row_count, min_fetched_at, max_fetched_at, object_key, status, exported_at) VALUES (2, now(), now(), $1, 'exported', now()) RETURNING batch_id, exported_at`. Delete the comment at :269-270 about txid bounds confusing a later ExportOnce cursor (a ledger row without stamped rows does not affect the claim; if the subtest must still run last for another reason, say that reason instead).

`deploy/prometheus/alerts.yml:62`, `ETLExportLagHigh` description becomes exactly `"S3 is failing, a claimed batch keeps failing, rows are locked by another session, or the exporter is down."`; copy it verbatim into `alerts_test.yml:74` and `:90`.

- [ ] **Step 4: Run the targeted tests**

Run: `go test -race -count=1 -timeout 15m ./internal/exporter/ ./internal/storage/pg/ ./internal/bootstrap/ ./internal/publisher/`
Expected: PASS.

- [ ] **Step 5: Lint and the full suite**

```bash
gofmt -l . && go vet ./... && go build ./...
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
go test -short -count=1 ./...
make test
make alerts-test
```

- [ ] **Step 6: Remove what became unused**

- `migrations/00001_raw.sql:20` `ingest_txid`, :26 its COMMENT, :28 `api_calls_export_cursor_idx`.
- `migrations/00002_pipeline.sql:7-11` `from_txid/from_seq/to_txid/to_seq/watermark_xmin`, :20 `UNIQUE (from_txid, from_seq)`, :21 `CHECK ((to_txid,to_seq) > (from_txid,from_seq))`; the xid8-cursor rationale in the comment at :67-69.
- `internal/exporter/exporter.go`: the xid8 package-doc paragraph (5-8), the batch-struct comment about `$n::text::xid8` (50-52) and the txid fields, the watermark query and cursor code in `claimNext` (221-260), `type querier` and `func lastBatchEnd` (283-310), `readRange`'s xid8 bounds (412-415), `updateLag`'s `lastBatchEnd` call and xid8 predicate (507-516).
- `internal/exporter/exporter_integration_test.go`: `insertTxidSQL`, `insertWithTxid`, `burnXidsBelow`, `txidBatch`, `txidLedger`, `checkChain` (397-482); `TestExportCursorCrossesTxidPowerOfTen` (484-529); `TestExportClaimStraddlingTxidPowerOfTen` (531-568); `TestExportOnceNeverSkipsOutOfOrderCommits` and its doc comment (288-332).
- `internal/publisher/publisher_integration_test.go:269-270`: the comment about fake txid bounds.

```bash
grep -rnE 'ingest_txid|from_txid|to_txid|watermark_xmin|pg_snapshot_xmin|lastBatchEnd|readRange|xid8|from_seq|to_seq' \
  --exclude-dir=.git --exclude-dir=docs --exclude-dir=data --exclude-dir=logs --exclude-dir=.s3meta --exclude-dir=bin \
  --exclude=README.md --exclude=TODO-refactor.md .; test $? -eq 1   # README/docs belong to T8; data/logs are runtime output
```

- [ ] **Step 7: Commit**

```bash
git add migrations internal/exporter internal/storage/pg/pg_integration_test.go \
  internal/bootstrap/bootstrap_integration_test.go internal/publisher/publisher_integration_test.go \
  deploy/prometheus/alerts.yml deploy/prometheus/alerts_test.yml
git commit -m "refactor(exporter): claim rows with FOR UPDATE SKIP LOCKED instead of the xid8 cursor" \
  -m "One short transaction picks unexported ok rows FOR UPDATE SKIP LOCKED, inserts the ledger row and stamps raw.api_calls.export_batch_id before any S3 I/O. Late commits land in a later batch, open transactions no longer stall the export, and a retry reads the batch back by id. Export lag counts unexported rows and batches stuck in claimed." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** no reference to `ingest_txid`, `xid8`, `pg_snapshot_xmin`, `lastBatchEnd` or the txid ledger columns remains in code, migrations, tests or alerts. The claim is one transaction with no S3 I/O and literal predicates; `nextval` runs only when rows were picked; the ledger INSERT precedes the stamp; the stamp joins on `(event_id, fetched_at)` with IS NULL and BETWEEN; `RowsAffected == picked` or rollback. `readBatch` filters by `export_batch_id` and the ledger bounds and orders by the qualified columns. `etl_export_lag_seconds` uses the LEAST query and grows while a claimed batch is stuck. Advisory lock, `Run`, resume-first, the `content_sha256` check, If-None-Match and DLQ behaviour and object keys are unchanged.

---

### Task 3: Exporter windows — `ETL_WINDOW` raw parts, `_SUCCESS` written last, window state in Postgres

The exporter turns the stream of claimed rows into `ETL_WINDOW` windows aligned to the UTC hour, per the contract. Closing window W claims the unexported ok rows with `fetched_at < W.end` into raw parts `source=coingecko/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl`, then writes `.../_SUCCESS` last with the deterministic manifest. Empty windows get a `_SUCCESS` with no parts. A row that commits after W closed goes into the next window; after downtime, missed windows are closed in order. Window state lives in Postgres, so a restart resumes mid-window and `_SUCCESS` is written exactly once with identical bytes. The exporter DLQ is named by window and part. On a fresh stack the exporter also closes the window just before the current one at once (Review Focus 1), so the first scheduled publish run finds a `_SUCCESS`. The old PG publisher keeps working unchanged (every part is still a `pipeline.export_batches` row that ends up `'exported'`), so the stack runs at this commit.

**Files:**
- Modify: `internal/config/config.go` — cross-check after `Load`'s struct literal: `ExportInterval` must be shorter than `window.PublishGrace` (1m). Update the `ExportBatchMax` comment: maximum rows per raw part. (`Window`/`ETL_WINDOW` already exist from T1.)
- Modify: `internal/config/config_test.go` — invalid cases `ETL_EXPORT_INTERVAL=1m` and `90s`.
- Modify: `.env.example` — comment above `ETL_EXPORT_INTERVAL=15s` (:31): `# Exporter tick; must be shorter than 1m, the publisher's grace after a window end.` (`ETL_WINDOW` is already there from T1.)
- Modify: `internal/storage/s3/s3.go` — `var ErrNotFound = errors.New("s3: object not found")` and `isNotFound(err) bool` (errors.As `*types.NoSuchKey`, or `awshttp.ResponseError` with HTTP 404); `Get` (:126-138) returns `fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)` for a missing key. Verified on versitygw v1.8.0: GetObject on a missing key gives a 404 that matches both checks. (Moved here from the publisher draft: T3 is its first user.)
- Modify: `internal/storage/s3/s3_test.go` — unit `TestGetNotFound`.
- Modify: `internal/storage/s3/s3_integration_test.go` — subtest "get missing key" in `TestClientIntegration`; old-layout fixture keys at :48 and :136, and the key string at :170 inside `TestHasAnyObject` (cosmetic, so this task's grep is clean; T5 deletes that test, so leave its `MkdirAll` at :164 alone).
- Modify: `migrations/00002_pipeline.sql` — add `pipeline.export_windows` BEFORE `export_batches`; `export_batches` gains `window_start` (FK), `part_index`, `line_count`, `UNIQUE (window_start, part_index)`; Down drops `export_windows` after `export_batches`.
- Modify: `internal/exporter/exporter.go` — `ExportOnce` signature (`Settings` + `now`); the window state machine (`pendingWindow`, `openWindow`, `claimIn` with window lock and part index, `sealWindow`, `finishWindow`); `batch` gains `Window` and `PartIndex`; `exportBatch` stores `line_count` with `content_sha256` and uses window keys; `updateLag` becomes schedule-based; `Run` drops its inner drain loop (553-565); delete `rawObjectKey` and `dlqObjectKey` (:100-108); rewrite the package doc.
- Modify: `internal/exporter/exporter_test.go` — delete `TestObjectKeys` (:11-21; the layout is tested in `internal/window`); keep `TestJoinLines`, `TestRowToEventKeepsPayloadVerbatim`.
- Modify: `internal/exporter/exporter_integration_test.go` — rewrite around injected time and `Settings{Window: 5*time.Minute, PartMax: n}`; insert helpers take `fetchedAt` (and ensure its partition with `pg.EnsurePartitions(ctx, pool, fetchedAt, 0)`); `exportedEventIDs` walks `export_windows`, reads each `_SUCCESS`, then the parts it lists, and checks the key regexes `^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/window=\d{4}/part-\d{5}\.jsonl$` and `^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/window=\d{4}/_SUCCESS$`.
- Modify: `internal/obs/metrics.go:66-69` — `ExportLag` Help becomes `"Seconds since the end of the oldest ETL_WINDOW window the exporter has not closed yet (0 while on schedule)."` (the struct comment at :18 stays).
- Modify: `deploy/prometheus/alerts.yml` (`ETLExportLagHigh`, :50-62), `deploy/prometheus/alerts_test.yml` (:70-75, :86-91).
- Modify: `deploy/grafana/dashboards/etl.json:184` — panel 13 description.
- Modify: `internal/storage/pg/pg_integration_test.go` — add `"pipeline.export_windows"` to the table list at :48; assert `UNIQUE (window_start, part_index)` on `export_batches` and the partial unique index `export_windows_one_pending`.
- Modify: `internal/publisher/publisher_integration_test.go` — keep the old publisher test green until T4 rewrites it (details in Step 3).
- Modify: `internal/bootstrap/bootstrap.go:102` + `internal/bootstrap/bootstrap_integration_test.go` (:133, :152-154) — the lake guard still exists here (T5 deletes it).
- Modify (cosmetic fixture strings, opaque values): `internal/transformer/service_test.go:60,167`, `internal/transformer/helpers_test.go:75`, `internal/publisher/publisher_test.go:51`, `internal/transform/helpers_test.go:50` (`testLineage().RawObject`) together with the golden row `internal/transform/markets_test.go:95` (its `_raw_object` must stay equal to it), and the schema fixtures `schemas/testdata/pipeline/rejected_record.v1/valid/snapshot_conflict.json` and `null_price.json` (`_raw_object`) and `schemas/testdata/pipeline/dlq_record.v1/valid/decode_error_from_s3.json` (`origin.object_key`; T4 drops its `batch_id`). In each, `source=coingecko/ingest_date=2026-10-10/batch=000...N.jsonl` becomes `source=coingecko/ingest_date=2026-10-10/window=1000/part-0000N.jsonl`, with the batch number kept as the part index (`batch=00000000000000000042.jsonl` becomes `part-00042.jsonl`).
- Create: `internal/exporter/main_test.go` — `TestMain` sets `time.Local = time.FixedZone("IST", 5*3600+30*60)` and then calls `os.Exit(m.Run())`, so every exporter test runs at +05:30 (Review Focus 2). It must run before `m.Run()`, because no goroutine exists yet at that point (checked: `runtime.NumGoroutine() == 1` there with pgx, testcontainers, the AWS SDK and franz-go linked).
- Modify: `cmd/etl/main.go:44` — usage line `exporter     export raw.api_calls to s3://raw in ETL_WINDOW windows (JSONL parts + _SUCCESS)`.

**Interfaces:**
- Consumes: T1 `window.*` (all of it), `config.Config.Window`; T2 `claimIn`/`claimNext`/`readBatch`, `export_batch_id`, `min/max_fetched_at`, the FK and partial indexes; `xtime.Format`, `s3.Client.PutIfAbsent`.
- Produces: `exporter.Settings{Window time.Duration; PartMax int}`; `func exporter.ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, set exporter.Settings, now time.Time) (int, error)`; `exporter.Run` (signature unchanged); `pipeline.export_windows(window_start PK, window_end, status claiming|sealed|closed, success_key, success_sha256, row_count, part_count, opened_at, closed_at)` + `export_windows_one_pending`; `pipeline.export_batches` + `window_start` (FK), `part_index`, `line_count`, `UNIQUE (window_start, part_index)`; `var s3.ErrNotFound` (wrapped by `Client.Get` for a missing key; T4 uses it); objects in S3 per the contract (raw parts + `_SUCCESS`, exporter DLQ by window/part); `etl_export_lag_seconds` = seconds past the end of the oldest unclosed window.

- [ ] **Step 1: Write the failing tests**

Unit:
- `config` `TestLoadInvalid` (changed): `ETL_EXPORT_INTERVAL=1m` and `=90s` are reported as not shorter than the publisher's 1m grace.
- `s3` `TestGetNotFound`: against an `httptest` server answering `404` with `<Error><Code>NoSuchKey</Code></Error>`, `Client.Get` returns an error with `errors.Is(err, s3.ErrNotFound)`.

Integration (`internal/exporter/exporter_integration_test.go`, Docker; `base` is an hour-aligned UTC time whose partitions the helpers create):
- `TestWindowExportWritesPartsThenSuccess`: rows at base+1m, +2m and +6m; `ExportOnce(now=base+5m+1s)` writes `part-00000` (2 lines) and `_SUCCESS` equal to `window.NewSuccess(...)` with the part's sha256. The +6m row stays NULL and no `_SUCCESS` exists for window base+5m. The `export_windows` row `base` is `'closed'` with `success_sha256 = sha256(_SUCCESS)`.
- `TestWindowExportEmptyWindows`: after window base closes, `ExportOnce(now=base+15m+1s)` with no new rows writes `_SUCCESS` for the windows at base+5m and base+10m with exactly `rows 0` and `parts []`; no `part-*.jsonl` exists under those prefixes (checked in the versitygw data dir) and `export_batches` gains no row.
- `TestWindowExportCatchUpAfterDowntime`: rows at base+1m, +7m, +13m and +14m and a single `ExportOnce(now=base+20m+1s)`: four windows are closed in order with 1, 1, 2 and 0 rows, each part holding exactly its own rows; `batch_id` increases with `window_start`; lag is 0 afterwards.
- `TestWindowExportLateCommitGoesToNextWindow`: txA inserts a row fetched at base+2m and stays open; B fetched at base+3m is committed. `ExportOnce(base+5m+1s)`: window 0 holds only B. After `commit A`, `ExportOnce(base+10m+1s)`: window 1's part holds A, window 0's `_SUCCESS` bytes are unchanged (rows 1); nothing lost.
- `TestWindowExportCrashAfterClaim`: `openWindow` + `claimNext` for window 0, then a new row fetched at base+4m is committed. `ExportOnce` resumes part 0 (the claimed rows only), claims the new row as `part-00001`, and `_SUCCESS` lists 2 parts sorted, with their sha256.
- `TestWindowExportCrashAfterPartPutAndAfterSuccessPut`: after a full close, resetting the part to `'claimed'` and the window to `'claiming'` (success fields NULL) gives the same part and `_SUCCESS` bytes, with `s3_put{exists}` +2. Resetting only the window to `'sealed'` (`closed_at` NULL) gives a `_SUCCESS` 412 with an equal checksum (`exists` +1) and status `'closed'`.
- `TestWindowExportPartMax`: 5 rows in one window with `PartMax=2` give `part-00000/00001/00002` with 2, 2 and 1 lines; `_SUCCESS` rows=5, parts in index order; `ExportOnce` returns 5.
- `TestWindowExportStuckPartBlocksWindowAndLagGrows`: other bytes are pre-put at window 0's `part-00000` key. `ExportOnce(now=w.End+10m)` errors with a checksum mismatch: no `_SUCCESS`, window `'claiming'`, lag >= 600, `s3_put{mismatch}` >= 1. A second call with a later now opens no new window (`export_windows` count 1).
- `TestWindowExportDLQByWindow` (ported DLQ test): the bad row goes to the dlq key `window.ExportDLQKey(w, 0)`; its DLQ line validates against `dlq_record.v1` with origin `batch_id/seq/event_id`. The ledger has `row_count 2` and `line_count 1`; `_SUCCESS` has rows 1 and the part rows 1.
- `TestWindowExportBootstrapWindow` (Review Focus 1; replaces the draft's `TestWindowExportFirstWindowOnEmptyLedger`):

```go
func TestWindowExportBootstrapWindow(t *testing.T) {
	ctx := context.Background()
	set := Settings{Window: 5 * time.Minute, PartMax: 500}
	base := window.Floor(time.Now(), time.Hour).Add(-2 * time.Hour) // aligned; inside the partitions setup creates
	prev := window.Window{Start: base.Add(-5 * time.Minute), End: base}

	t.Run("empty ledger closes the window before the current one at once", func(t *testing.T) {
		e := setup(t)
		n, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, set, base.Add(2*time.Minute))
		// n == 0, err == nil; export_windows has exactly one row: prev, 'closed'
		// raw object window.RawSuccessKey(prev) == `{"window_start":"<prev.Start>","window_end":"<base>","rows":0,"parts":[]}`
		// etl_export_lag_seconds == 0
		// ExportOnce(now=base+5m+1s) then closes [base, base+5m) too (2 closed rows)
	})
	t.Run("rows in the current window wait for it", func(t *testing.T) {
		e := setup(t)
		e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
		// ExportOnce(now=base+2m): prev closed empty; the row keeps export_batch_id NULL
		// ExportOnce(now=base+5m+1s): window base holds the row (rows 1)
	})
	t.Run("a _SUCCESS already in the lake is never overwritten", func(t *testing.T) {
		e := setup(t)
		earlier := []byte(`{"left":"by an earlier stack"}`)
		if _, err := e.s3.PutIfAbsent(ctx, s3.BucketRaw, window.RawSuccessKey(prev), earlier); err != nil {
			t.Fatal(err)
		}
		// ExportOnce(now=base+2m): returns 0, nil; the object still equals earlier; export_windows stays empty;
		// s3_put{mismatch} == 0; lag == 0
		// ExportOnce(now=base+5m+1s): closes [base, base+5m)
	})
}
```

- `TestWindowExportKeysIgnoreLocalZone` (Review Focus 2) does NOT touch `time.Local`: `main_test.go`'s `TestMain` already set it to +05:30. Swapping it here would race with the pgx pool's goroutines under `-race`.

```go
func TestWindowExportKeysIgnoreLocalZone(t *testing.T) {
	if _, off := time.Now().Zone(); off != 5*3600+30*60 {
		t.Fatalf("time.Local offset %d s: TestMain must set +05:30", off)
	}
	e := setup(t) // pgx returns timestamptz in +05:30 from here on
	base := time.Date(2026, 10, 10, 18, 30, 0, 0, time.UTC) // 00:00 IST on 2026-10-11: the local date differs
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute)) // helper ensures the 2026-10-10 partition
	n, err := ExportOnce(context.Background(), e.pool, e.s3, e.reg, e.m, e.log,
		Settings{Window: 5 * time.Minute, PartMax: 500}, base.Add(5*time.Minute+time.Second))
	// n == 1, err == nil; objects exist at exactly
	//   source=coingecko/ingest_date=2026-10-10/window=1830/part-00000.jsonl
	//   source=coingecko/ingest_date=2026-10-10/window=1830/_SUCCESS
	// and _SUCCESS begins with {"window_start":"2026-10-10T18:30:00.000Z","window_end":"2026-10-10T18:35:00.000Z","rows":1,
}
```

- `TestWindowExportWindowSizeChange`: after window [base, base+5m) closes with 5m (base aligned to the hour), `ExportOnce` with `Window=10m` and `now=base+20m+1s` closes [base+10m, base+20m), which holds the rows fetched in [base+5m, base+20m); no key collides.
- `TestConcurrentClaimsSerializeOnWindow` (replaces T2's disjoint test): tx1 holds `claimIn(ctx, tx1, w, 3)` on 6 rows; a goroutine running `claimNext(w, 500)` has not returned after 300 ms; after tx1 commits it returns 3 rows with `part_index 1`, the stamped sets are disjoint, and `ExportOnce` exports all 6 exactly once.
- T2 tests ported to windows: exactly-once, seq order within a part, open-transaction-does-not-stall (now injected past the window), partition-created-after-claim, and claim-failure counting (after renaming `raw.api_calls`, the error contains `"window anchor"`, `export_batches_total{error}=1`). For partition-created-after-claim use `Settings{Window: 60*time.Minute}` and a partition at `now+4d` (beyond `EnsurePartitions`' today+3), with `now` = that row's window end + 1s, so the catch-up closes ~100 windows instead of 2,880; assert the new partition has both partial indexes.
- `TestRunClosesWindows` (replaces `TestRunExportsAndStopsOnCancel`): `cfg{ExportInterval: 50ms, ExportBatchMax: 500, Window: 1m}`; a row fetched at `now-3m` gets a `_SUCCESS` for its window within 20 s; `Run` returns nil after cancel. `TestRunFailsWhenLockHeld` is unchanged.

Elsewhere: `s3` integration subtest "get missing key" (`errors.Is(err, s3.ErrNotFound)` on versitygw); `pg` `MigrateTwiceIsIdempotent` assertions above; `publisher_integration_test` (kept green, below); `alerts_test.yml` export-lag cases expect the summary `Exporter is 6m 40s behind closing raw windows` (first case) and `The exporter reports no export lag` (second case), and the new description verbatim in both.

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -short -count=1 ./internal/config/ ./internal/storage/s3/` → FAIL (`ErrNotFound` undefined, interval case passes Load).
Run: `go test -race -count=1 -timeout 15m ./internal/exporter/` → FAIL (`Settings` undefined, wrong `ExportOnce` arity).

- [ ] **Step 3: Implement**

Schema (`00002`, replacing T2's `export_batches` DDL; the `raw.api_calls` FK and T2's indexes stay):

```sql
CREATE TABLE pipeline.export_windows (
  window_start   timestamptz PRIMARY KEY,
  window_end     timestamptz NOT NULL CHECK (window_end > window_start),
  status         text NOT NULL DEFAULT 'claiming' CHECK (status IN ('claiming','sealed','closed')),
  success_key    text NOT NULL,
  success_sha256 bytea CHECK (octet_length(success_sha256) = 32),
  row_count      int CHECK (row_count >= 0),
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

CREATE TABLE pipeline.export_batches (   -- one row per raw part
  batch_id            bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  window_start        timestamptz NOT NULL REFERENCES pipeline.export_windows (window_start),
  part_index          int NOT NULL CHECK (part_index >= 0),
  row_count           int NOT NULL CHECK (row_count > 0),                   -- rows claimed (raw lines + dlq lines)
  line_count          int CHECK (line_count >= 0 AND line_count <= row_count), -- lines in the raw part; set with content_sha256
  min_fetched_at      timestamptz NOT NULL,
  max_fetched_at      timestamptz NOT NULL,
  object_key          text NOT NULL UNIQUE,
  content_sha256      bytea,
  status              text NOT NULL DEFAULT 'claimed' CHECK (status IN ('claimed','exported')),
  claimed_at          timestamptz NOT NULL DEFAULT now(),
  exported_at         timestamptz,
  publish_lease_until timestamptz,   -- removed by T4
  published_at        timestamptz,   -- removed by T4
  UNIQUE (window_start, part_index),
  CHECK (max_fetched_at >= min_fetched_at),
  CHECK ((content_sha256 IS NULL) = (line_count IS NULL))
);
```

The `((true))` partial unique index was verified to be accepted on 18.6. Window states: `'claiming'` takes parts; `'sealed'` means the manifest is frozen and `success_sha256` stored, with the `_SUCCESS` PUT pending; `'closed'` means `_SUCCESS` is written.

Go API:

```go
type Settings struct {
	Window  time.Duration // cfg.Window
	PartMax int           // cfg.ExportBatchMax: maximum rows per raw part
}

type batch struct {
	ID                         int64
	Window                     window.Window
	PartIndex                  int
	RowCount                   int
	MinFetchedAt, MaxFetchedAt time.Time
	ObjectKey                  string
	SHA256                     []byte
	ClaimedAt                  time.Time
}
```

`Run(ctx, cfg, pool, s3c, reg, m, log)` keeps its signature: each tick (the first immediately, then every `cfg.ExportInterval`) pings the lock connection as today and calls `ExportOnce(..., Settings{cfg.Window, cfg.ExportBatchMax}, time.Now())`; on error it logs `export failed` and waits for the next tick. The inner `for n == max` drain loop (exporter.go:553-565) is deleted: `ExportOnce` drains by itself.

`ExportOnce` (one tick) returns the sum of `row_count` of the parts it exported:

```
defer updateLag(ctx, pool, m, log, set.Window, now)
0. Every failure counts once in etl_export_batches_total{result="error"}: exportBatch counts its own;
   every other failure (pendingWindow, the bootstrap check, openWindow, claim, seal, finish) is counted in ExportOnce.
1. Resume parts first: claimedBatches (status 'claimed', ORDER BY batch_id, joined with export_windows for
   window_end) -> exportBatch each (log WARN "resuming claimed part").
2. Loop:
   w, status, found, bootstrap := pendingWindow(ctx, pool, set.Window, now)
   if bootstrap {                       // empty ledger; w is the window just before the current one
       _, err := s3c.Get(ctx, s3.BucketRaw, window.RawSuccessKey(w))
       switch {
       case err == nil:                 // an earlier stack closed it (kept lake): never overwrite
           log.Warn("raw window already in the lake; starting at the next window", "window_start", xtime.Format(w.Start))
           w = window.Window{Start: w.End, End: w.End.Add(set.Window)}
       case errors.Is(err, s3.ErrNotFound): // a fresh lake: close w now (usually empty)
       default:
           return exported, fmt.Errorf("exporter: check bootstrap window: %w", err)
       }
   }
   if !found && now.Before(w.End) { return }   // next window is not due yet
   if !found { openWindow(w); status = "claiming" }
   if status == "claiming" {
       for { b, ok := claimNext(ctx, pool, w, set.PartMax); if !ok { break }; exportBatch(b); exported += b.RowCount }
       sealWindow(ctx, pool, log, w)
   }
   finishWindow(ctx, pool, s3c, m, log, w)     // PUT _SUCCESS, mark closed
   updateLag(ctx, pool, m, log, set.Window, now) // smooth lag during catch-up
   if ctx.Err() != nil { return }
```

A window is due when `now >= w.End`. `now` is frozen for the whole call; windows that fall due during a long catch-up are closed on the next tick.

`pendingWindow(ctx, q, size, now) (w window.Window, status string, found, bootstrap bool, err error)` is read-only (Postgres only):
- a) `SELECT window_start, window_end, status FROM pipeline.export_windows WHERE status <> 'closed' ORDER BY window_start LIMIT 1` → `found=true`; resume it whatever its size.
- b) else `SELECT window_end FROM pipeline.export_windows ORDER BY window_start DESC LIMIT 1` → `start = window.Ceil(lastEnd, size)`. If `ETL_WINDOW` changed between restarts, the next window starts at the first boundary of the new size at or after the last end; no key can collide, and rows between lastEnd and that boundary go into it (its claim has no lower bound).
- c) else (empty ledger) `SELECT min(fetched_at) FROM raw.api_calls WHERE outcome = 'ok' AND export_batch_id IS NULL` (partial index) → `start = window.Floor(min(now, oldest), size)` (with no rows, `window.Floor(now, size)`). If `start` equals `window.Floor(now, size)` (no row older than the current window), use the window before it: `start = start.Add(-size)`, `bootstrap = true`. That window is already due, so the first tick closes it (usually empty). Cronicle's first `etl publish` runs in the minute after it is provisioned and targets the window ending at floor(run − 1 min). Compose provisions Cronicle only after the exporter is healthy (T7), so that window is this one or a later one, and it finds a `_SUCCESS`. With older rows, the first window is the one holding the oldest unexported row (an exporter that starts late catches up from there). Error wrap `"exporter: window anchor: %w"`.
- Return `w = {start, start+size}`, `found=false`.

`openWindow`: `INSERT INTO pipeline.export_windows (window_start, window_end, success_key) VALUES ($1, $2, $3)` with `window.RawSuccessKey(w)`. A zombie racing on it fails on the PK or `export_windows_one_pending`, the tick errors, and the next tick retries.

`claimNext(ctx, pool, w, max)` → `claimIn(ctx context.Context, tx pgx.Tx, w window.Window, max int) (batch, bool, error)`, one transaction:
1. `SELECT 1 FROM pipeline.export_windows WHERE window_start = $1 AND status = 'claiming' FOR UPDATE` — no row: return false (never add a part to a sealed or closed window). The row lock serializes claims and the seal per window.
2. Pick (predicates stay literal; verified on 18.6: Merge Append over the per-partition partial `*_seq_idx` indexes under a generic plan):

```sql
SELECT event_id::text, fetched_at FROM raw.api_calls
 WHERE outcome = 'ok' AND export_batch_id IS NULL AND fetched_at < $1
 ORDER BY seq LIMIT $2 FOR UPDATE SKIP LOCKED          -- $1 = w.End
```

   None: return false (no `nextval`).
3. `SELECT COALESCE(max(part_index) + 1, 0) FROM pipeline.export_batches WHERE window_start = $1`
4. `SELECT nextval(pg_get_serial_sequence('pipeline.export_batches','batch_id')), now()`
5. `INSERT INTO pipeline.export_batches (batch_id, window_start, part_index, row_count, min_fetched_at, max_fetched_at, object_key, claimed_at) VALUES (...)` with `object_key = window.RawPartKey(w, part)`.
6. The T2 stamp UPDATE (unnest + PK join + IS NULL + BETWEEN); `RowsAffected` must equal the picked count, else roll back.

SKIP LOCKED still skips rows locked by foreign sessions (they go into a later window). Within one window, claims now serialize on the window row instead of running in parallel; this is deliberate: it stops a part being added after the manifest is sealed.

`exportBatch(b)`: unchanged flow (readBatch, len check, build lines, checksum, DLQ PUT, raw PUT, mark exported). Differences: the checksum store becomes `UPDATE pipeline.export_batches SET content_sha256 = $2, line_count = $3 WHERE batch_id = $1 AND content_sha256 IS NULL` (`line_count = len(lines)`); the DLQ key becomes `window.ExportDLQKey(b.Window, b.PartIndex)`; the DLQ origin (`kind postgres, batch_id, seq, event_id`) and `occurred_at = claimed_at` are unchanged. A part whose every row went to the DLQ is still written (empty body) and listed with rows 0, so every claimed batch has exactly one raw object.

`sealWindow(ctx, pool, log, w)`, one transaction:

```sql
SELECT status, success_sha256 FROM pipeline.export_windows WHERE window_start = $1 FOR UPDATE;
SELECT part_index, object_key, status, content_sha256, line_count
  FROM pipeline.export_batches WHERE window_start = $1 ORDER BY part_index;
```

Guard: every part has status `'exported'` and non-null `content_sha256`/`line_count`, else `"exporter: window %s: part %d not exported"` (resume-first and drain-before-claim make this unreachable). `body := window.NewSuccess(w, parts{Key: object_key, SHA256: hex(content_sha256), Rows: line_count}).Marshal()`; `sum := sha256(body)`. If status is `'claiming'`: `UPDATE pipeline.export_windows SET status='sealed', success_sha256=$2, row_count=$3, part_count=$4 WHERE window_start=$1 AND status='claiming'`. After the seal a claim for w returns false, so a row committing late with `fetched_at < w.End` goes to the next window.

`finishWindow(ctx, pool, s3c, m, log, w)`: read `status` and `success_sha256`; if `'closed'`, return. Rebuild the manifest exactly as in the seal from the immutable part rows; if `sha256(body) != success_sha256`, log ERROR `window manifest is not deterministic` and return an error. `put(ctx, s3c, m, s3.BucketRaw, window.RawSuccessKey(w), body)` via PutIfAbsent (a retry after a crash gets 412 with the same sha256, counted as `exists`). Then `UPDATE pipeline.export_windows SET status='closed', closed_at=now() WHERE window_start=$1 AND status='sealed'`. Log INFO `raw window closed` with `window_start`, `success_key`, `parts`, `rows`, `created`, `sha256`. `_SUCCESS` is therefore last: every part PUT, DLQ PUT included, finished before the seal, and the `_SUCCESS` PUT happens only after it.

Lag (replaces T2's LEAST query): `etl_export_lag_seconds = max(0, now - w.End)` where `w` comes from `pendingWindow(ctx, pool, set.Window, now)`; when `bootstrap` is true the gauge is 0 (no window has been opened yet). On schedule it is 0, or up to `ExportInterval` in the seconds between a window's end and the next tick. A stuck claimed part, a failing S3, a sealed window whose `_SUCCESS` PUT fails, or a catch-up backlog grows it linearly; an exporter that is down leaves the series absent (the `absent()` branch fires). Rows locked by another session and late rows no longer count as lag (they go to a later window). The value does not depend on `ETL_WINDOW`, so the 300 s threshold keeps meaning "5 minutes behind schedule". `updateLag` logs WARN and leaves the gauge unchanged when its query fails or ctx is done.

Config (after the struct literal, next to T1's check): `if c.ExportInterval >= window.PublishGrace { p.errs = append(p.errs, fmt.Errorf("ETL_EXPORT_INTERVAL=%s: must be shorter than the publisher's %s grace", c.ExportInterval, window.PublishGrace)) }` (only when `ExportInterval > 0`, so a bad value reports once).

`s3.go`:

```go
// ErrNotFound: the object does not exist (Get wraps it with bucket/key).
var ErrNotFound = errors.New("s3: object not found")

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
}
```

and in `Get`: `if isNotFound(err) { return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key) }` before the generic wrap.

Alerts: `ETLExportLagHigh` (:50-62) keeps its expr, `for` and the 300 s threshold. `summary: "{{ if $labels.job }}The exporter reports no export lag{{ else }}Exporter is {{ $value | humanizeDuration }} behind closing raw windows{{ end }}"`; `description: "A raw window cannot close: S3 is failing, a claimed part keeps failing (checksum or row-count mismatch), Postgres is unreachable, or the exporter is down."`; the rule comment says the gauge is the time past the end of the oldest window not yet closed. `alerts_test.yml` (:70-75, :86-91) expects these texts verbatim.

Grafana `etl.json:184` (panel 13): `"How far the exporter is behind closing raw windows (0 on schedule), and conditional S3 PUT results (exists = idempotent retry, mismatch = alert)."`

Keep the old publisher test green (`internal/publisher/publisher_integration_test.go`; T4 rewrites it): `okEvent` gains a `fetchedAt time.Time` parameter (it uses `time.Now()` at :102). `base := window.Floor(time.Now(), time.Minute).Add(-10*time.Minute)`; the 3 events are fetched at base+10s, and :165 becomes `exporter.ExportOnce(ctx, e.pool, e.s3c, e.reg, e.m, e.log, exporter.Settings{Window: time.Minute, PartMax: 500}, base.Add(time.Minute+time.Second))` returning 3 (one part). The event at :230 is fetched at base+70s, and the call at :234 uses `now = base.Add(2*time.Minute+time.Second)`, returning 1. The hand-made INSERT at :285-287 first inserts a closed window: `INSERT INTO pipeline.export_windows (window_start, window_end, status, success_key, success_sha256, row_count, part_count, closed_at) VALUES ('2000-01-01 00:00+00','2000-01-01 00:01+00','closed','manual/_SUCCESS', decode(repeat('00',32),'hex'), 2, 1, now())`; the batch INSERT then adds `window_start '2000-01-01 00:00+00'` and `part_index 0`. Leave `line_count` and `content_sha256` NULL: `CHECK ((content_sha256 IS NULL) = (line_count IS NULL))` rejects a row with only one of them, and the old publisher reads neither.

Lake guard (still present; T5 deletes it): `bootstrap.go:102` becomes `SELECT NOT EXISTS (SELECT 1 FROM pipeline.export_windows)` (empty windows write `_SUCCESS` without any `export_batches` row; every batch references a window, so this also covers batches). In `bootstrap_integration_test.go` (:133, :152-154) put the raw object at `source=coingecko/ingest_date=2026-10-10/window=1000/part-00000.jsonl` and insert one `export_windows` row (status `'claiming'`, `success_key '.../window=1000/_SUCCESS'`) instead of the batch row.

Package doc (rewrite): windows aligned to the UTC hour; the claim of T2, per window (`fetched_at < end`); parts, then the seal, then `_SUCCESS`; window state in `pipeline.export_windows`; the bootstrap window on a fresh ledger.

- [ ] **Step 4: Run the targeted tests**

Run: `go test -short -count=1 ./internal/window/ ./internal/config/ ./internal/exporter/ ./internal/storage/s3/`
Run: `go test -race -count=1 -timeout 15m ./internal/exporter/ ./internal/storage/pg/ ./internal/storage/s3/ ./internal/publisher/ ./internal/bootstrap/`
Expected: PASS.

- [ ] **Step 5: Lint and the full suite**

```bash
gofmt -l . && go vet ./... && go build ./...
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
make test
make alerts-test
docker compose config -q && jq empty deploy/grafana/dashboards/etl.json
```

Optional live smoke (no scheduler yet; the old publisher still runs): `make clean && ETL_WINDOW=1m make up`; after about 3 minutes `find data/raw -name _SUCCESS | sort | tail -3`, `cat` the last one (compact JSON with parts and sha256), `shasum -a 256` the listed part and compare; `curl -s 'http://localhost:9090/api/v1/query?query=etl_export_lag_seconds{job="exporter"}'` shows a value under 60. Then `make down`.

- [ ] **Step 6: Remove what became unused**

- `internal/exporter/exporter.go:100-108` `rawObjectKey` and `dlqObjectKey` (the `batch=<%020d>` and `export+batch+` layouts); T2's LEAST `updateLag` query; the `max int` parameter of `ExportOnce`; the inner drain loop in `Run` (553-565); package-doc text about one object per batch.
- `internal/exporter/exporter_test.go:11-21` `TestObjectKeys`.
- `internal/exporter/exporter_integration_test.go`: the old raw key regex in `exportedEventIDs`; T2 tests that assume one batch per `ExportOnce` call (rewritten above); T2's non-blocking concurrent-claims assertion.
- Old-layout fixture strings `ingest_date=2026-10-10/batch=000...N.jsonl` in transformer, transform, s3 and publisher tests and in `schemas/testdata/pipeline/` (list under **Files**).

```bash
grep -rnE 'batch=%020d|export\+batch\+|rawObjectKey|dlqObjectKey|batch=0000' \
  --exclude-dir=.git --exclude-dir=docs --exclude-dir=data --exclude-dir=logs --exclude-dir=.s3meta --exclude-dir=bin \
  --exclude=README.md --exclude=TODO-refactor.md .
# prints only dlqObjectKey in internal/publisher (publisher.go and its two test files), which T4 replaces
```

- [ ] **Step 7: Commit**

```bash
git add migrations/00002_pipeline.sql internal/config .env.example internal/storage/s3 internal/exporter \
  internal/obs/metrics.go deploy/prometheus/alerts.yml deploy/prometheus/alerts_test.yml \
  deploy/grafana/dashboards/etl.json internal/storage/pg/pg_integration_test.go \
  internal/publisher/publisher_integration_test.go internal/publisher/publisher_test.go \
  internal/bootstrap internal/transformer/service_test.go internal/transformer/helpers_test.go cmd/etl/main.go \
  internal/transform/helpers_test.go internal/transform/markets_test.go \
  schemas/testdata/pipeline/rejected_record.v1/valid schemas/testdata/pipeline/dlq_record.v1/valid/decode_error_from_s3.json
git commit -m "feat(exporter): write raw data in ETL_WINDOW windows with a _SUCCESS manifest" \
  -m "The exporter closes hour-aligned windows in order: it claims the rows fetched before each window end into part-NNNNN.jsonl objects, seals the manifest in pipeline.export_windows and writes _SUCCESS last. Empty windows get a _SUCCESS too, a fresh ledger closes the window before the current one at once, and export lag is the time past the end of the oldest window not yet closed." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** raw objects follow `source=coingecko/ingest_date=<UTC date of window start>/window=<HHMM>/part-<NNNNN>.jsonl`; `_SUCCESS` is written after every part (DLQ included) with the exact compact manifest; empty windows get `rows 0, parts []`. Window W contains exactly the rows claimed while W was `'claiming'` (`fetched_at < W.end`); a late commit lands in the next window; missed windows close strictly in order (at most one non-closed window, enforced by an index). A crash after claim, after a part PUT, after the seal and after the `_SUCCESS` PUT yields byte-identical objects on retry. On an empty ledger the window before the current one is closed at once unless the lake already holds its `_SUCCESS`. Exporter DLQ objects are `stage=export/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl`. `ETL_EXPORT_INTERVAL < 1m` is validated. `etl_export_lag_seconds` = seconds past the end of the oldest unclosed window; `ETLExportLagHigh` texts updated and promtool tests pass. No old-layout key builders or dead tests remain; the old PG publisher still works against the new parts.

---

### Task 4: Publisher job — `etl publish --now`, one closed raw window per run, no Postgres

Replaces the long-running publisher service (lease loop on `pipeline.export_batches`, HTTP health/metrics server, Postgres pool) with a short batch command `etl publish [--now <unix seconds|RFC3339>]`. Each run publishes exactly one closed window: end = floor(now - 1m, ETL_WINDOW), start = end - ETL_WINDOW (`window.ForPublish`). The run reads the window's `_SUCCESS` from s3://raw (missing: exit 1), reads and verifies every listed part (sha256 and line count) before producing anything, validates each line against S1, writes invalid lines to one deterministic publish-DLQ object for the window, and produces every valid line with key = idempotency_key (T1's `queue.MessageKey`) and the existing five headers. It waits for all acks (idempotent producer, acks=all), pushes job metrics through T1's helper, logs to `logs/publisher/etl.log` and exits 0. It also drops the kprom client metrics, removes the lease columns and index from `00002`, and removes everything the old service leaves unused in compose, Prometheus, the alerts and the dashboard (draft conflict 8). It creates the one-shot job frame `runJob` that T6 reuses.

Interpretation (unchanged from the draft): the contract's `raw/source=coingecko/...` and `dlq/stage=publish/...` start with the bucket (`s3.BucketRaw = "raw"`, `s3.BucketDLQ = "dlq"`); the object key starts at `source=` or `stage=`, and `_SUCCESS` `parts[].key` is the key without the bucket. TODO item 3's optional strict window order (`_published` marker) is not implemented.

**Files:**
- Rewrite: `internal/publisher/publisher.go` — delete lease/mark SQL (39-59), `type batch` (61-65), `leaseBatch`/`markPublished` (67-85), `dlqObjectKey` (164-169), `PublishOnce` (222-317), `Run` (319-340), `const schemaLabel` (33), `dlqOrigin.BatchID` (118), the redundant `Flush` after `ProduceSync` (306-308) and the imports `pgx`, `pgxpool`, `config`, `obs`. Keep `splitLines` (87-99), `truncate` (122-133) and the `dlqRecord` shape (101-111) without `batch_id`. Drop the `Task 3` history from its doc comment (:101-102). Add `Deps`, `Result`, `ErrNotReady`, `ErrIntegrity`, `PublishWindow`, `readManifest`, `checkManifest`, `readParts`, `classify`; re-sign `newDLQRecord` and `writeDLQ` (no batch, no `*obs.Metrics`).
- Modify: `schemas/testdata/pipeline/dlq_record.v1/valid/decode_error_from_s3.json` — the valid publish-stage example loses `"batch_id":42`, because a publish DLQ origin no longer has a batch. T3 already gave it the new part key.
- Modify: `internal/publisher/publisher_test.go` — keep `TestSplitLines` (13-35); delete `TestDLQObjectKey` (37-44); adapt `TestDLQRecordsSatisfySchema` (46-75) and `TestDLQRecordKeepsOriginal` (77-94); add `TestCheckManifest`, `TestClassify`.
- Rewrite: `internal/publisher/publisher_integration_test.go` — no Postgres and no exporter: `setup` starts Redpanda and versitygw only; a `putWindow` helper writes parts and `_SUCCESS` by hand through `window.RawPartKey`/`window.NewSuccess`; each subtest gets its own topic. Delete the lease/`published_at` subtests (183-268), the hand-made `export_batches` INSERT (283-290), the pg/exporter imports and `StartPostgres`/`Migrate`/`EnsurePartitions` (44-58).
- Modify: `internal/queue/queue.go` — `NewProducer` (48-73) loses the `reg prometheus.Registerer` parameter and the kprom block (65-71); doc comment (49-52) becomes "idempotent producer with acks=all"; drop the imports `kprom` and `prometheus`.
- Modify: `internal/queue/queue_test.go` — delete `TestNewProducerRegistersClientMetrics` (47-63) and the prometheus import; `TestNewProducerRequiresBrokers` uses the 2-arg `NewProducer`; rename `TestNewProducerNilRegistry` to `TestNewProducerBuildsClient`.
- Modify: `internal/queue/queue_integration_test.go:20` — `NewProducer(brokers, "queue-it")`.
- Create: `cmd/etl/jobs.go` — `jobFunc`, `runJob` (the frame of every scheduled one-shot job).
- Create: `cmd/etl/publish.go` — `publishTimeout`, `runPublish`, `publishWindow`, `parseNow`.
- Create: `cmd/etl/jobs_test.go` (`TestRunJobReportsFailureOnSIGTERM`), `cmd/etl/publish_test.go` (`TestParseNow`).
- Modify: `cmd/etl/main.go` — `usageText` (39-48): replace the `publisher` line with `publish      publish one closed raw window to the broker and exit (--now <unix seconds|RFC3339>)`; `run()` (65-77): `case "publish": return runPublish(args[1:], stderr)` and remove `"publisher"` from the `runService` case at :68; the `serveAndRun` comment at :133-134 drops "(publisher only) the kprom producer metrics"; delete `buildService`'s `case "publisher"` (233-244); drop the `publisher` import of `main.go` and its `queue` import if nothing else in `main.go` uses it (`publish.go` imports both itself).
- Modify: `cmd/etl/main_test.go` — `TestRunDispatch` (14-35), `TestServiceRejectsBadConfig` (:40), `TestServiceExitsWhenPostgresUnreachable` (:68); add `TestPublishExitsWhenS3Unreachable`.
- Modify: `cmd/etl/main_integration_test.go` — add `TestPublishCommand`.
- Modify: `internal/config/config.go` — delete `PublishLease`, `PublishPoll` (fields :41-42, parsing :78-79); `internal/config/config_test.go` — remove `ETL_PUBLISH_LEASE`/`ETL_PUBLISH_POLL` from `allKeys` (:16), defaults (:58-59), the env map (:96-97), checks (:138-139) and the invalid case (:158).
- Modify: `internal/obs/metrics.go` — delete `PublishedRecords` (field :21, constructor :78-81); `internal/obs/metrics_test.go` — remove lines :26 and :46; `internal/obs/server_test.go` (82-89) — use `m.APIRateLimited.Add(3)` and assert `etl_api_rate_limited_total 3`.
- Modify: `migrations/00002_pipeline.sql` — delete `publish_lease_until` and `published_at` from `export_batches` and `export_batches_unpublished_idx` with its comment. Touch nothing else in `export_batches`.
- Modify: `docker-compose.yml` — delete the `publisher` service (164-166).
- Modify: `deploy/prometheus/prometheus.yml` — delete the `publisher` scrape job (26-31).
- Modify: `deploy/prometheus/alerts.yml` — `ETLServiceDown` (77-92): regex `fetcher|exporter|transformer`, delete `or absent(up{job="publisher"})`, comment `The publisher is a batch job (etl publish), not a service.`; `ETLObjectChecksumMismatch` comment (109-114): `The exporter and the transformer count it.`
- Modify: `deploy/prometheus/alerts_test.yml` — every `job="publisher"` series and the expected publisher alert (lines 81, 173-189, 198, 218).
- Modify: `deploy/grafana/dashboards/etl.json` — panel 5: delete target refId B (`sum(rate(etl_published_records_total[5m])) * 60`, :80); description (:73) becomes `"Throughput of the long-running hops: API calls stored and processed records by outcome."`
- Modify: `go.mod`, `go.sum` — `go mod tidy` removes `github.com/twmb/franz-go/plugin/kprom` (go.mod:23, go.sum:178-179).

**Interfaces:**
- Consumes: T1 `config.Config.Window`, `config.Config.PushgatewayURL`, `window.ForPublish`, `window.RawWindowPrefix`, `window.RawSuccessKey`, `window.PublishDLQKey`, `window.Success`, `window.ParseSuccess`, `window.NewSuccess`/`RawPartKey` (tests), `obs.JobRun`, `obs.ReportJobRun`, `obs.JobPublisher`, `obs.MetricPublishRecords`, `obs.MetricPublishDLQLines`, `queue.MessageKey`; T3 `s3.ErrNotFound`, the raw window objects; existing `s3.Client.Get`/`PutIfAbsent`, `s3.ErrChecksumMismatch`, `validation.Registry.Validate` with `IDRawEvent`/`IDDLQRecord`, `event.Unmarshal`/`SHA256Hex`, `xtime.Format`, `obs.NewLogger`, `logDir` (`cmd/etl/main.go:118`), `testutil.StartRedpanda`/`StartVersityGW`/`SkipIfNoDocker`.
- Produces:
  - CLI `etl publish [--now <unix seconds|RFC3339>]`; exit 0 success, 1 runtime failure (missing `_SUCCESS`, integrity, S3/broker error, timeout, signal, bad config), 2 usage (bad flag or `--now`). `etl publisher` is an unknown command (exit 2), no alias. Log file `logs/publisher/etl.log` (service `publisher`); lines `job started`, `publish started`, then `window published` + `job finished`, or (`window not ready` +) `job failed`, every one with `"job":"etl_publisher"`. T7's Cronicle event runs `etl publish --now "$JOB_NOW"`.
  - `type jobFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) (gauges map[string]float64, err error)`; `func runJob(logService, job string, stderr io.Writer, body jobFunc) int` (package main; T6 uses it with `"maintenance"`).
  - `publisher.PublishWindow(ctx context.Context, d publisher.Deps, w window.Window) (publisher.Result, error)`; `type Deps struct{ S3 *s3.Client; Producer *kgo.Client; Reg *validation.Registry; Topic string; Log *slog.Logger }`; `type Result struct{ Window window.Window; Parts, Lines, Records, DLQLines int }`; `publisher.ErrNotReady`, `publisher.ErrIntegrity`.
  - `func queue.NewProducer(brokers []string, clientID string) (*kgo.Client, error)`.
  - Pushgateway group `job="etl_publisher"`: the four `etl_job_*` metrics plus, on success only, `etl_publish_records` and `etl_publish_dlq_lines` (T7's alerts and dashboard).
  - DLQ object `s3://dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl`: `dlq_record.v1` lines with `origin {kind:"s3", bucket:"raw", object_key:<part key>, line_no, event_id?}` and `occurred_at` = window end.
  - `migrations/00002`: `export_batches` without `publish_lease_until`, `published_at`, `export_batches_unpublished_idx`.

- [ ] **Step 1: Write the failing tests**

`cmd/etl/publish_test.go`:
- `TestParseNow`: `'1791813960'` → `2026-10-12T14:06:00Z` (UTC location); `'2026-10-12T14:06:00Z'` and `'2026-10-12T17:06:00.5+03:00'` parse to UTC; `''`, `'abc'`, `'1791813960.5'`, `'-5'`, `'+5'`, `'2026-10-12 14:06'`, `'99999999999999999999'` each return an error mentioning `--now`.

`internal/publisher/publisher_test.go` (unit):
- `TestCheckManifest` (Review Focus 3): nil for a valid 2-part manifest of window [14:00,14:05) and for `{rows:0, parts:[]}`; errors for a wrong `window_start`, a wrong `window_end` — a 10-minute manifest checked against a 5-minute window — whose message mentions `ETL_WINDOW`, a part key outside `window.RawWindowPrefix(w)`, and a key not of the form `part-NNNNN.jsonl`. (Sorting, sha256 format, negative rows and the rows sum are `window.ParseSuccess`'s checks, tested in T1.)
- `TestClassify`: an ok fixture line gives a record with `Key == idempotency_key`, `Value` byte-equal to the line, headers `[event_id, idempotency_key, schema=validation.IDRawEvent, raw_object=partKey, line_no='3']` in that order, and a nil DLQ record; `'{not json'` → `decode_error`; `'{"schema_version":1,"event_id":"<uuid>"}'` → `schema_violation` with `origin.event_id` set; an S1-valid `http_error` event → `unexpected_outcome`. Each DLQ record validates against `IDDLQRecord` and has `occurred_at` == window end.
- `TestDLQRecordsSatisfySchema` / `TestDLQRecordKeepsOriginal` (adapted): `newDLQRecord(w, partKey, lineNo, line, class, viols)` validates against `dlq_record.v1` for all four inputs; `original_base64` of `'{x'` is `'e3g='`; the JSON has no `batch_id`; two calls give byte-identical JSON.
- `TestSplitLines` unchanged.

`internal/publisher/publisher_integration_test.go` (Docker; Redpanda + versitygw; each subtest its own topic):
- `window with parts publishes every line`: window 2026-10-12 14:00-14:05; `part-00000` holds markets t0+t1 and `part-00001` holds coin bitcoin, written by `putWindow`. `PublishWindow` returns `Result{Parts:2, Lines:3, Records:3, DLQLines:0}` and nil. Consuming the topic gives exactly 3 records: `Key == *ev.IdempotencyKey`, `Value` byte-equal to the raw line selected by the `raw_object` and `line_no` headers, correct `event_id`/`idempotency_key`/`schema` headers. No object exists at `window.PublishDLQKey(w)`.
- `not ready, then ready, then rerun` (Review Focus 4; full name `TestPublisherIntegration/not ready, then ready, then rerun`): parts written, `_SUCCESS` not → `errors.Is(err, ErrNotReady)`, the error names the `_SUCCESS` key, the topic stays empty (no record after 2 s) and no DLQ object exists. Then write `_SUCCESS` → `PublishWindow` returns nil with `Records` = all lines. Run it again → nil again; the topic now holds 2 copies of every record with the same key and the same partition.
- `sha mismatch`: `_SUCCESS` lists a correct sha for `part-00000` and a wrong sha for `part-00001` → `errors.Is(err, ErrIntegrity)`, the message names `part-00001`, and ZERO records are produced (`part-00000` is not half-published).
- `row count or window mismatch`: a manifest with `rows` ≠ lines in the part, and a manifest at the right key whose `window_end` is for a 10m window — both `errors.Is(err, ErrIntegrity)`, nothing produced.
- `invalid line goes to dlq, rest produced`: one part with lines [good, S1-invalid JSON, `'{not json'`, S1-valid `http_error` event] → `Result{Records:1, DLQLines:3}`; the DLQ object `stage=publish/ingest_date=2026-10-12/window=1410/part-00000.jsonl` has 3 lines, each valid against `IDDLQRecord`, with `origin.object_key` = the part key, `line_no` 2/3/4, `error_class` `schema_violation`/`decode_error`/`unexpected_outcome` and `occurred_at` `'2026-10-12T14:15:00.000Z'`; the good line is produced once and verbatim.
- `rerun of the same window`: a second `PublishWindow` on the DLQ window returns nil (PutIfAbsent 412 with equal sha is accepted) and the DLQ object bytes are unchanged; the topic now holds 2 copies of the good event with the same key and partition.
- `empty window`: `_SUCCESS` `{rows:0, parts:[]}` → nil and `Result{Records:0}`; nothing produced, no DLQ object written.

`cmd/etl` (unit unless noted):
- `TestRunDispatch` (extended): `publisher` → exit 2 with `unknown command`; `publish --now bogus` → exit 2 with stderr containing `--now`; `publish extra` → exit 2.
- `TestServiceRejectsBadConfig` (adapted): with `ETL_TOPIC_PARTITIONS=six`, `etl publish` exits 1 and stderr names `ETL_TOPIC_PARTITIONS` (the list becomes init, fetcher, exporter, publish, transformer).
- `TestServiceExitsWhenPostgresUnreachable` (adapted): `publisher` removed from the list.
- `TestPublishExitsWhenS3Unreachable`: `ETL_S3_ENDPOINT`, `ETL_BROKERS` and `ETL_PUSHGATEWAY_URL` point at closed loopback ports; `ETL_PG_DSN` deliberately invalid. `run([publish --now 1791813960])` exits 1, and `<logdir>/publisher/etl.log` contains `"msg":"publish started"`, `"msg":"job failed"`, `"job":"etl_publisher"` and `window_start` `2026-10-12T14:00:00.000Z`.
- `TestRunJobReportsFailureOnSIGTERM` (Review Focus 5; builds the binary and runs it as a child process, no Docker):

```go
func TestRunJobReportsFailureOnSIGTERM(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "etl")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // S3 never answers: the run blocks in the _SUCCESS GET
	}))
	defer hang.Close()
	var mu sync.Mutex
	var pushes []string // "METHOD PATH\nBODY"
	pgw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		pushes = append(pushes, r.Method+" "+r.URL.Path+"\n"+string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer pgw.Close()
	logDir := t.TempDir()
	cmd := exec.Command(bin, "publish", "--now", "1791813960")
	cmd.Env = append(os.Environ(), "ETL_LOG_DIR="+logDir, "ETL_S3_ENDPOINT="+hang.URL,
		"ETL_BROKERS=127.0.0.1:1", "ETL_PUSHGATEWAY_URL="+pgw.URL, "ETL_WINDOW=5m")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(logDir, "publisher", "etl.log")
	// poll logFile (<= 5 s) until it contains "publish started", then:
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// wait <= 5 s (well inside Cronicle's 10 s kill grace): exit code 1
	// log contains `"msg":"job failed"` and `"job":"etl_publisher"`
	// exactly one push: "POST /metrics/job/etl_publisher", body has `etl_job_success 0`
	// and no `etl_job_last_success_timestamp_seconds`
}
```

- `TestPublishCommand` (integration, `main_integration_test.go`): versitygw + Redpanda + a fake Pushgateway (`httptest`, records method and path), `ETL_WINDOW=5m`. (1) No `_SUCCESS`: exit 1, the log has `"msg":"window not ready"` and `window=1400/_SUCCESS`, and the fake got one POST to `/metrics/job/etl_publisher`. (2) After putting an empty-window `_SUCCESS`: exit 0, the log has `window published`, and a second POST arrived. (3) `ETL_PUSHGATEWAY_URL` at a closed port: still exit 0, and the log has an ERROR `pushgateway push failed`.

Elsewhere: `config` tests pass without the `PublishLease`/`PublishPoll` fields and keys; `obs` `TestMetricsCatalogue` no longer lists `etl_published_records_total`; `TestMetricsEndpoint` shows `etl_api_rate_limited_total 3`; `queue` `TestNewProducerRequiresBrokers` (`NewProducer(nil, "x")` errors) and `TestNewProducerBuildsClient` (`NewProducer([]string{"127.0.0.1:1"}, "test")` returns a client without dialing); `alerts_test.yml` keeps passing without publisher series: in "export lag fires when the exporter is gone" the other job's series is `etl_export_lag_seconds{job="transformer",instance="10.0.0.7:8080"}`; in the service-down case ("a service that fails its scrape is down", :166-189) the exporter goes down instead of the publisher. Change the values of the existing `up{job="exporter"}` series (:171-172) instead of adding a second series with the same labels, and delete the publisher series (:173-174):

```yaml
      - series: 'up{job="exporter",instance="10.0.0.5:8080"}'
        values: '1 1 0 0 0 0 0 0 0 0 0'
    ...
          - exp_labels: {severity: critical, job: exporter, instance: "10.0.0.5:8080"}
            exp_annotations:
              summary: "exporter is down (10.0.0.5:8080)"
              description: "Prometheus cannot scrape it, or Docker DNS knows no running container for it. Check docker compose ps and its log."
```

and the `up{job="publisher",…}` series of the other two `ETLServiceDown` cases are deleted.

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -short -count=1 ./cmd/etl/ ./internal/publisher/ ./internal/queue/ ./internal/config/ ./internal/obs/`
Expected: FAIL — `undefined: parseNow`, `undefined: checkManifest`, `PublishWindow`, wrong `NewProducer` arity; dispatch cases fail (`publish` unknown).

- [ ] **Step 3: Implement**

`internal/publisher/publisher.go`:

```go
// Package publisher implements one run of `etl publish`: it publishes one closed
// raw window. It reads the window's _SUCCESS from s3://raw (missing: ErrNotReady),
// reads and verifies every listed part before producing anything, sends invalid
// lines to one deterministic DLQ object per window and produces every valid line
// verbatim, keyed by its idempotency_key. It never touches Postgres.
package publisher

const (
	stage            = "publish"
	errClassDecode   = "decode_error"
	errClassSchema   = "schema_violation"
	errClassOutcome  = "unexpected_outcome"
	maxMessageLength = 1024
)

var (
	ErrNotReady  = errors.New("window not closed by the exporter: no _SUCCESS")
	ErrIntegrity = errors.New("raw window does not match its _SUCCESS")
)

type Deps struct {
	S3       *s3.Client
	Producer *kgo.Client
	Reg      *validation.Registry
	Topic    string
	Log      *slog.Logger
}

type Result struct {
	Window                          window.Window
	Parts, Lines, Records, DLQLines int
}

func PublishWindow(ctx context.Context, d Deps, w window.Window) (Result, error)
```

Algorithm:
1. `readManifest`: `Get(raw, window.RawSuccessKey(w))`; `errors.Is(err, s3.ErrNotFound)` → `fmt.Errorf("%w: s3://raw/%s", ErrNotReady, key)`; any other error is returned wrapped, not as `ErrIntegrity` (transient). Then `window.ParseSuccess` (failure → `ErrIntegrity`), then `checkManifest(s, w)` (failure → `ErrIntegrity` with the reason).
2. `checkManifest(s window.Success, w window.Window) error`, pure: (a) `s.WindowStart == xtime.Format(w.Start)` and `s.WindowEnd == xtime.Format(w.End)`, otherwise `manifest is for <ws>..<we>, expected <s>..<e>; is ETL_WINDOW the same for exporter and publisher?`; (b) every part key is `window.RawWindowPrefix(w)` followed by a name matching `^part-[0-9]{5}\.jsonl$`. An empty window (`rows 0, parts []`) is valid.
3. `readParts`: read ALL parts before producing anything, so a bad part N never leaves parts 0..N-1 half-published. For each part: `Get`; `ErrNotFound` → `ErrIntegrity` (`_SUCCESS lists missing part <key>`); `event.SHA256Hex(body) != part.SHA256` → `ErrIntegrity` (`sha256 mismatch for <key>: manifest <a>, object <b>`); `len(splitLines(body)) != part.Rows` → `ErrIntegrity`. Memory is the whole window: a few MB at 5m, roughly 30 MB at 60m.
4. For each part i and line j (`lineNo = j+1`, 1-based within the part): `classify(d.Reg, d.Topic, w, part.Key, lineNo, line) (*kgo.Record, *dlqRecord)`:
   - `reg.Validate(validation.IDRawEvent, line)`: a non-nil error (not JSON) → `decode_error` with violation `{Message: err.Error()}`; violations → `schema_violation` with the violations (already sorted and capped by Validate).
   - `event.Unmarshal` error → `decode_error`.
   - `ev.Outcome != event.OutcomeOK` or `ev.IdempotencyKey == nil` → `unexpected_outcome`, violation `{Message: "outcome <x>: only ok events are published"}` (a guard: the exporter exports only ok rows).
   - Otherwise `&kgo.Record{Topic: topic, Key: []byte(queue.MessageKey(ev)), Value: line /* verbatim */, Headers: event_id=ev.EventID, idempotency_key=*ev.IdempotencyKey, schema=validation.IDRawEvent, raw_object=part.Key, line_no=strconv.Itoa(lineNo)}` (headers in this order).
5. If there are DLQ records: `writeDLQ(ctx, d.S3, d.Reg, d.Log, w, recs, originals) error`, called BEFORE producing (as today), so a run that fails in produce leaves a DLQ object that the rerun rewrites byte-identically. Each record is `json.Marshal`ed and validated against `validation.IDDLQRecord`; a record that fails is logged at ERROR with `original_base64` and left out. The kept lines are joined with `\n` (trailing newline) and written with `PutIfAbsent(dlq, window.PublishDLQKey(w), body)`. Created or exists-with-equal-sha → `log.Error("records sent to dlq", "count", n, "key", key, "created", created)`. `s3.ErrChecksumMismatch` → `log.Error("dlq object exists with different bytes; keeping the first", "key", key)` and continue (messages can differ between builds of the validator). Other errors are returned. `Result.DLQLines` counts every diverted line, including any left out because its DLQ record was invalid.
   DLQ record: `{schema_version:1, stage:"publish", error_class, violations (messages truncated to 1024 bytes on a rune boundary, [] never null), origin:{kind:"s3", bucket:"raw", object_key:<part key>, line_no:<n>, event_id:<only if the line has a valid UUID event_id>}, original_base64, occurred_at: xtime.Format(w.End)}` — no clock, no random id. `origin.batch_id` is gone (the schema has it optional with minimum 1, so emitting 0 would violate it).
6. If there are records: `d.Producer.ProduceSync(ctx, recs...).FirstErr()`; an error returns `fmt.Errorf("produce window %s: %w", xtime.Format(w.Start), err)`. No extra `Flush`: `ProduceSync` already waits for every ack. `Result.Records = len(recs)`. A rerun uses a new producer id and writes duplicates, which the transformer drops as `replay`.

`internal/queue/queue.go`: `func NewProducer(brokers []string, clientID string) (*kgo.Client, error)` — same options as today minus the kprom hooks. Rationale (kprom removed entirely): a job that lives a few seconds is never scraped, and pushing kprom's per-broker (`node_id`) client metrics would leave stale series in the Pushgateway; producer health is covered by the exit code, the log lines and the pushed `etl_job_success`/`etl_publish_records`. The publisher no longer builds `obs.Metrics`, so it no longer emits `etl_dlq_total{stage="publish"}`, `etl_schema_validations_total{stage="publish"}`, `etl_schema_violations_total{stage="publish"}` or `etl_s3_put_total`; the pushed `etl_publish_dlq_lines` replaces them (T7 adds the alert branch).

`cmd/etl/jobs.go`:

```go
// jobFunc is the body of one short batch job. gauges are its job-specific
// metrics (obs.Metric* names); a body returns them when they describe what the
// run really did (etl publish: only on success; retention: also on error).
type jobFunc func(ctx context.Context, cfg config.Config, log *slog.Logger) (gauges map[string]float64, err error)

// runJob is the frame of every scheduled one-shot job: config, logger
// (<ETL_LOG_DIR>/<logService>/etl.log, appended, plus stdout = the Cronicle job
// log), SIGINT/SIGTERM, the body, one Pushgateway report, the exit code
// (0 ok, 1 failed). A config or logger error exits 1 before any push.
func runJob(logService, job string, stderr io.Writer, body jobFunc) int {
	cfg, err := config.Load(logService)
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", logService, err)
		return 1
	}
	log, closer, err := obs.NewLogger(logService, logDir(cfg.LogDir))
	if err != nil {
		fmt.Fprintf(stderr, "etl %s: %v\n", logService, err)
		return 1
	}
	defer closer.Close()
	// SIGTERM: Cronicle's abort and timeout, docker compose stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log = log.With("job", job)
	log.Info("job started")
	start := time.Now()
	gauges, err := body(ctx, cfg, log)
	fin := time.Now()
	obs.ReportJobRun(ctx, log, cfg.PushgatewayURL, obs.JobRun{Job: job, Started: start, Finished: fin, Err: err, Gauges: gauges})
	if err != nil {
		log.Error("job failed", "error", err, "duration_ms", fin.Sub(start).Milliseconds())
		return 1
	}
	log.Info("job finished", "duration_ms", fin.Sub(start).Milliseconds())
	return 0
}
```

`cmd/etl/publish.go`:

```go
// publishTimeout stays below Cronicle's 5-minute job timeout, so a stuck S3 or
// broker still produces a "job failed" line and a success=0 push before the kill.
const publishTimeout = 4 * time.Minute

func runPublish(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nowArg := fs.String("now", "", "logical run time: unix seconds or RFC3339 (default: current time)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "etl publish: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	now := time.Now()
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == "now" })
	if set { // --now "" (JOB_NOW unset) is an error, not "use the clock"
		t, err := parseNow(*nowArg)
		if err != nil {
			fmt.Fprintf(stderr, "etl publish: %v\n", err)
			return 2
		}
		now = t
	}
	return runJob("publisher", obs.JobPublisher, stderr, func(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
		ctx, cancel := context.WithTimeout(ctx, publishTimeout)
		defer cancel()
		w := window.ForPublish(now, cfg.Window)
		log = log.With("window_start", xtime.Format(w.Start), "window_end", xtime.Format(w.End))
		log.Info("publish started", "now", xtime.Format(now), "window", cfg.Window.String())
		res, err := publishWindow(ctx, cfg, w, log)
		if errors.Is(err, publisher.ErrNotReady) {
			log.Error("window not ready", "success_key", window.RawSuccessKey(w), "error", err)
		}
		if err != nil {
			return nil, err // gauges only on success
		}
		log.Info("window published", "parts", res.Parts, "lines", res.Lines, "records", res.Records, "dlq", res.DLQLines)
		return map[string]float64{obs.MetricPublishRecords: float64(res.Records), obs.MetricPublishDLQLines: float64(res.DLQLines)}, nil
	})
}

// publishWindow builds the clients of one run (S3, schemas, producer; no Postgres) and publishes w.
func publishWindow(ctx context.Context, cfg config.Config, w window.Window, log *slog.Logger) (publisher.Result, error) {
	s3c, err := s3.New(ctx, s3.Config{Endpoint: cfg.S3Endpoint, AccessKey: cfg.S3AccessKey, SecretKey: cfg.S3SecretKey, Region: cfg.S3Region})
	if err != nil {
		return publisher.Result{}, err
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		return publisher.Result{}, fmt.Errorf("load schemas: %w", err)
	}
	prod, err := queue.NewProducer(cfg.Brokers, "etl-publisher")
	if err != nil {
		return publisher.Result{}, fmt.Errorf("producer: %w", err)
	}
	defer prod.Close()
	return publisher.PublishWindow(ctx, publisher.Deps{S3: s3c, Producer: prod, Reg: reg, Topic: cfg.Topic, Log: log}, w)
}

// parseNow accepts integer unix seconds (Cronicle's $JOB_NOW) or RFC 3339; the result is UTC.
func parseNow(s string) (time.Time, error) {
	if s != "" && strings.Trim(s, "0123456789") == "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return time.Unix(n, 0).UTC(), nil
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("--now %q: want unix seconds or RFC3339", s)
}
```

The run never connects to Postgres (`ETL_PG_DSN` may be unset or invalid). `ReportJobRun` uses `context.WithoutCancel`, so SIGTERM or the timeout still records the failure; the push result never changes the exit code.

Migration: delete `publish_lease_until timestamptz,` and `published_at timestamptz,` from `pipeline.export_batches`, and `export_batches_unpublished_idx` with its comment.

Compose, Prometheus, alerts, dashboard: as listed under **Files** (the publisher service, its scrape job, its `ETLServiceDown` entries and test series, the `ETLObjectChecksumMismatch` comment, panel 5's refId B and description). Between this commit and T7 nothing publishes; that is expected.

- [ ] **Step 4: Run the targeted tests**

Run: `go test -short -count=1 ./cmd/etl/ ./internal/publisher/ ./internal/queue/ ./internal/config/ ./internal/obs/`
Run: `go test -race -count=1 -timeout 15m ./internal/publisher/ ./internal/queue/ ./internal/storage/s3/ ./cmd/etl/`
Expected: PASS.

- [ ] **Step 5: Lint and the full suite**

```bash
go mod tidy && git diff --stat go.mod go.sum        # only the kprom lines removed
gofmt -l ./cmd ./internal                            # prints nothing
go build ./... && go vet ./...
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
go list -deps ./internal/publisher | grep -c jackc   # 0: the publisher does not link pgx
go run ./cmd/etl publish --now abc; echo $?          # 2
ETL_LOG_DIR=$(mktemp -d) ETL_S3_ENDPOINT=http://127.0.0.1:1 ETL_BROKERS=127.0.0.1:1 ETL_PUSHGATEWAY_URL=http://127.0.0.1:1 \
  go run ./cmd/etl publish --now 1791813960; echo $? # 1, with "job failed" in <dir>/publisher/etl.log
make test
make alerts-test
docker compose config -q && docker compose config --services | grep -c '^publisher$'   # 0
jq empty deploy/grafana/dashboards/etl.json
```

- [ ] **Step 6: Remove what became unused**

Everything in the **Files** deletions, then:

```bash
grep -rnE 'PublishLease|PublishPoll|PUBLISH_LEASE|PUBLISH_POLL|publish_lease_until|published_at|export_batches_unpublished_idx|leaseSQL|markSQL|markPublished|PublishOnce|PublishedRecords|etl_published_records_total|kprom|etl_publisher_|publish\+batch' \
  --include='*.go' --include='*.sql' --include='go.mod' --include='*.yml' --include='*.json' \
  --exclude-dir=.git --exclude-dir=data --exclude-dir=logs --exclude-dir=.s3meta .; test $? -eq 1
grep -rn 'job="publisher"\|names: \[publisher\]\|command: \["publisher"\]' docker-compose.yml deploy; test $? -eq 1
grep -rn '"publisher"' cmd/etl   # only runJob("publisher", …) in publish.go (the log service name) and the TestRunDispatch case expecting "publisher" to be unknown
```

- [ ] **Step 7: Commit**

```bash
git add internal/publisher internal/queue cmd/etl internal/config internal/obs/metrics.go internal/obs/metrics_test.go \
  internal/obs/server_test.go migrations/00002_pipeline.sql docker-compose.yml deploy/prometheus \
  deploy/grafana/dashboards/etl.json go.mod go.sum schemas/testdata/pipeline/dlq_record.v1/valid/decode_error_from_s3.json
git commit -m "feat(publisher): publish one closed raw window per etl publish run, without Postgres" \
  -m "etl publish --now <time> reads the window's _SUCCESS, verifies every listed part, sends invalid lines to one DLQ object per window and produces the rest keyed by idempotency_key, then pushes its result to the Pushgateway. The long-running publisher, its lease columns, kprom and etl_published_records_total are gone, and so are its compose service, scrape job, alerts and dashboard query. Nothing schedules the job until Cronicle lands." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** `etl publish --now <t>` publishes exactly [floor(t-1m, ETL_WINDOW) - ETL_WINDOW, floor(t-1m, ETL_WINDOW)) in UTC; without `--now` it uses the clock; `--now` accepts integer unix seconds or RFC3339 and rejects anything else (empty included) with exit 2. A missing `_SUCCESS` gives exit 1, an ERROR `window not ready` naming the key, no record and no DLQ object. A malformed manifest, a window mismatch, a missing part, a sha256 or row-count mismatch gives exit 1 with `ErrIntegrity` and no record for any part of the window. Every valid line is produced verbatim with key = idempotency_key and headers `event_id, idempotency_key, schema, raw_object, line_no`; exit 0 only after every ack. Invalid lines go to exactly one deterministic object per window; a rerun is accepted (412 with equal sha) and republishes the valid lines as duplicates. The publisher does not link pgx and never connects to Postgres. Each run pushes once (POST, `job="etl_publisher"`), with the two job gauges on success only; a push failure is logged at ERROR and does not change the exit code. kprom, `etl_published_records_total`, `ETL_PUBLISH_LEASE`/`ETL_PUBLISH_POLL`, the lease SQL, the publisher's compose service, scrape job and alert entries are gone; `etl publisher` is an unknown command.

---

### Task 5: Time-based object names for transformer outputs; remove the lake-lifetime guard

Names every object the transformer writes (processed, rejected, dlq) by processing time only: the manifest's `planned_at` plus `id8`, a short hash of `topic_id|partition|first_offset`. No Kafka offset or topic name appears in a key any more, so the lake no longer depends on a Postgres or Redpanda lifetime. Then removes the `etl init` lake check (`ErrLakeFromOtherLifetime`) and the code that exists only for it (TODO item 7), and removes the transformer tests' dependence on the old `coins_markets`/coin-id message key (TODO item 4, transformer side). The raw-layout fixture strings in transformer tests were already updated in T3.

**Files:**
- Modify: `internal/transformer/batch.go` — replace `ObjectKey` (:724-740) with `objectKey(zone, dataset, plannedAt, topicID, partition, firstOffset)` and add `batchID8`; `groupKey` (:206) becomes `{bucket, dataset}`; the three group sites: processed at :463, dlq at :465-468, rejected at :602; `buildObjects` (:694-722) takes `(groups, plannedAt, topicID, partition, first)` and drops its `topic` parameter; the call at :472 becomes `buildObjects(c.groups, rng.plannedAt, b.TopicID, b.Partition, rng.first)`; add the import `encoding/hex` (`strings`, `xtime` and `fmt` stay used elsewhere in the file); delete `Message.Key` (:47).
- Modify: `internal/transformer/service.go` — `toMessage` (:295-301) stops copying `rec.Key`.
- Modify: `internal/transformer/keys_test.go` — rewrite `TestObjectKey` and `TestBuildObjectsIsDeterministic`; add `TestBatchID8`, `TestObjectKeyPanics`; keep `TestUpTo`.
- Modify: `internal/transformer/batch_test.go` — subtest "snapshot dated outside the dedup partition window" (:544-573; asserts `/event_date=2026-08-01/` at :568); two new integration subtests.
- Modify: `internal/transformer/service_test.go` — `TestRunHaltsPartitionOnChecksumMismatch` (:510-516) computes the key with `ObjectKey(..., topic, 0, 0)`, which no longer exists and cannot be known before planning: pre-plan the batch with `planBatch` and corrupt one planned processed object; `TestRunEndToEnd`: rewrite the comment at :46-50 and replace the hand-picked old-scheme keys at :51 and :165. Two producers copy the removed field and stop compiling: `newTopic` (:269, `Key: m.Key`) and the `produce` closure of the halts test (:490, `Key: m.Key`). Drop the `Key` there. Both tests choose the partition themselves (a one-partition topic; `kgo.ManualPartitioner` with `Partition: p`), so a nil key changes nothing.
- Create: `internal/transformer/main_test.go` — `TestMain` sets `time.Local = time.FixedZone("IST", 5*3600+30*60)` before `os.Exit(m.Run())` (Review Focus 2; as in T3's `internal/exporter/main_test.go`), so every transformer test runs at +05:30. T9's `TZ=Asia/Kolkata` run already expects the whole package to pass in that zone.
- Modify: `internal/transformer/helpers_test.go` — `message()` (:66-77) drops the `"test-key"` key (the raw_object header at :75 already has the new layout from T3).
- Modify: `internal/bootstrap/bootstrap.go` — delete `ErrLakeFromOtherLifetime` (:34-41) and the package-doc bullet (:8-9); `initPostgres` (:74-105) returns only `error` (delete the `postgres_export_ledger` step, which T3 pointed at `export_windows`, and the `ledgerEmpty` result); `Run` (:60-69) calls `initPostgres(ctx, cfg, log)` then `initBuckets(ctx, cfg, log)`; `initBuckets` (:124-160) drops the `ledgerEmpty` parameter and the `s3_lake_check` block (:144-158) and ends after `s3_buckets`; fix its doc comment. Keep the `errors` import (`retry` uses `errors.Join`).
- Modify: `internal/bootstrap/bootstrap_integration_test.go` — delete `TestRunRefusesALakeFromAnotherLifetime` (:108-160, including the hand-written ledger INSERT); `TestRunTwiceAgainstRealDependencies` puts one raw object between run 1 and run 2; remove the `errors` import if nothing else uses it.
- Modify: `internal/storage/s3/s3.go` — delete `HasAnyObject` (:159-167; its only caller was the lake check).
- Modify: `internal/storage/s3/s3_integration_test.go` — delete `TestHasAnyObject` (:150-180) and the imports only it used (check `os`, `filepath`).
- Modify: `Makefile` — the `clean` comment (:52-55); the recipe stays.

**Interfaces:**
- Consumes: `pipeline.consumer_batches.planned_at` (timestamptz DEFAULT now(), `00002_pipeline.sql:57`, unchanged); `Batch.TopicID` from `queue.TopicID` (falls back to `ETL_PIPELINE_EPOCH`); the raw layout only as the `raw_object` lineage string; T1's message key (the transformer does not read it); `s3.Client.PutIfAbsent` (unchanged).
- Produces: the contract's transformer layout; package-internal `func objectKey(zone, dataset string, plannedAt time.Time, topicID uuid.UUID, partition int32, firstOffset int64) string`, `func batchID8(topicID uuid.UUID, partition int32, firstOffset int64) string`, `func buildObjects(groups map[groupKey][]line, plannedAt time.Time, topicID uuid.UUID, partition int32, first int64) []object`, `type groupKey struct{ bucket, dataset string }`; `bootstrap.Run` no longer touches `pipeline.export_batches`/`export_windows` or lists a bucket (`initPostgres(ctx, cfg, log) error`, `initBuckets(ctx, cfg, log) error`); `s3.Client` without `HasAnyObject`; `transformer.Message` without `Key`.

- [ ] **Step 1: Write the failing tests**

Unit (`internal/transformer/keys_test.go`; the id8 vectors were computed with `shasum`):
- `TestObjectKey`: for `plannedAt` `2026-10-10T13:02:33.123456+03:00`, topic `00000000-0000-0000-0000-000000000001`, partition 3, first offset 1200: processed `ticker` → `dataset=ticker/v=1/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl`; rejected `coin_contract` → `dataset=coin_contract/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl`; dlq → `stage=transform/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl` (covers the UTC conversion and the second truncation).
- `TestBatchID8`: `batchID8(…0001, 3, 1200) == "d4376ada"`; partition 4 → `"9b7ed59a"`; first offset 1201 → `"72621807"`; another topic id gives a value other than `d4376ada`.
- `TestObjectKeyPanics`: `objectKey` panics for zone `"raw"`, for processed or rejected with an empty dataset, and for dlq with a non-empty dataset.
- `TestBuildObjectsIsDeterministic` (changed): groups `{processed,"b"}` (same 3 lines), `{dlq,""}`, `{processed,"a"}`; `buildObjects(mk(), time.Date(2026,10,10,10,2,33,0,time.UTC), uuid.MustParse("00000000-0000-0000-0000-000000000002"), 0, 5)`; two runs give identical keys, bodies and sha256s; objects are sorted by bucket then key (`a[0].Bucket == dlq`); `a[1].Key == "dataset=a/v=1/processed_date=2026-10-10/hour=10/part-20261010T100233Z-fa6acf61.jsonl"`; rows are ordered by offset then ordinal.

Integration (`internal/transformer/batch_test.go`, `TestProcessBatch` subtests):
- `object keys come from the manifest planned_at, not the clock` (new): pre-insert a `consumer_batches` row (`topic_id = b.TopicID`, partition 0, first 100, last 104, `planned_at '2026-10-10T10:59:59.999Z'`); `planBatch` twice, then `ProcessBatch`. Every manifest key equals `bucket+"/"+objectKey(bucket, ds, 2026-10-10T10:59:59.999Z, b.TopicID, 0, 100)`, i.e. `part-20261010T105959Z-<batchID8(b.TopicID,0,100)>.jsonl` under `processed_date=2026-10-10/hour=10`; both plans return identical keys and bodies; exactly one processed object exists per dataset. Review Focus 2: `main_test.go`'s `TestMain` runs the package at +05:30, so pgx returns `planned_at` as 16:29:59.999 local, yet every key must read `processed_date=2026-10-10/hour=10/part-20261010T105959Z-…`. Assert at the start that `time.Now().Zone()` has offset 19800 s. Do not swap `time.Local` inside the test: the pool and broker goroutines read it, and `-race` fails the package.
- `a re-created topic writes new keys over a kept lake` (new): `ProcessBatch` the fixture batch with TopicID A at offsets 100..104, then `e.reset` (fresh Postgres state; the lake is kept), then `ProcessBatch` the same messages with a new TopicID B. Both runs succeed with no `ErrChecksumMismatch` and no mismatch-counter increase, and the two manifest key sets are disjoint (this replaces the protection the lake guard used to give).
- `snapshot dated outside the dedup partition window creates its partition` (changed, :544-573): claims exist for 2026-08-01; read `planned_at` for `b.TopicID`; assert a processed object exists whose key starts with `"dataset=market_snapshot/v=1/processed_date="+xtime.FormatDate(plannedAt)+"/hour="+plannedAt.UTC().Format("15")+"/"`, whose body contains `"snapshot_at":"2026-08-01T`, and that no key contains `event_date=`.
- All other `TestProcessBatch` subtests (crash after TX1, partial PUTs, mismatch, non-deterministic re-plan, zombie, boundary shift, rewind, trim, S3-down, S2 re-plan) pass unchanged; `countLines` keeps deriving the dataset from the key's first segment (`dataset=<ds>/`).

`internal/transformer/service_test.go`:
- `TestRunHaltsPartitionOnChecksumMismatch` (changed), replacing :513-516:
  1. `id, err := queue.TopicID(ctx, cl, topic, uuid.MustParse(testEpoch))`.
  2. `m := message(t, 0, 0, evs[0])`; make `produce(0, evs[0])` send exactly this `m` (refactor `produce` to take a `Message`, or build `m` first and produce it).
  3. `p, err := planBatch(ctx, e.deps, Batch{TopicID: id, Topic: topic, Partition: 0, Messages: []Message{m}})`.
  4. `o :=` the first `p.objects` entry with `Bucket == s3.BucketProcessed` (fail if none); `PutIfAbsent(ctx, o.Bucket, o.Key, []byte("{}\n"))`.
  `Run` then adopts the planned range (`fixRange` default branch, same `planned_at`), so the PUT of `o` hits the mismatch. Expected: partition 0 halts with mismatch counter == 1 and no commit; partition 1 commits 1 then 2. Update the comment at :511 ("its first object is the market_snapshot one").
- `TestRunEndToEnd` (changed): keep hand-chosen keys, because the test needs several partitions busy and the bitcoin CDN copy on another partition: `keys := []string{"k1", "k1", "k2", "k3", "k4", "k1"}`. Computed with franz-go v1.22.1's murmur2 over 6 partitions: k1 goes to 5, k2 to 3, k3 to 4 and k4 to 4. So the bitcoin copy (k3, p4) lands on another partition than the original (k2, p3), and three partitions get records, as with the old labels (0, 1, 2). Do not reuse the idempotency key here: CDN copies share it and would collapse onto one partition, defeating the cross-partition dedup check. New comment: `Production keys are the event's idempotency_key, so byte-identical copies share a partition. Here keys are chosen by hand so several partitions (and both replicas) have work, and the CDN copy of the bitcoin response lands on another partition than the original: it must still be dropped, because dedup claims are global.` At :165 use `Key: []byte(*extra.IdempotencyKey)`, matching production. Every partition commits to its end offset; processed and rejected counts equal `expected()`; the restart creates exactly one new batch.

`internal/bootstrap/bootstrap_integration_test.go`:
- `TestRunTwiceAgainstRealDependencies` (changed): after run 1, a raw object exists at `source=coingecko/ingest_date=2026-10-10/window=1000/part-00000.jsonl` while the export ledger is empty; run 2 still returns nil. The 3-partition mismatch case still fails.

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -short -count=1 ./internal/transformer/` → FAIL (`undefined: objectKey`, `batchID8`).
Run: `go test -race -count=1 -timeout 15m -run 'TestRunTwiceAgainstRealDependencies' ./internal/bootstrap/` → FAIL (init refuses: lake from another lifetime).

- [ ] **Step 3: Implement**

Key layout (keys are relative to the bucket; the manifest stores `"<bucket>/<key>"`):
- processed: `dataset=<ds>/v=1/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl`
- rejected: `dataset=<ds>/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl`
- dlq: `stage=transform/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl`

`T = plannedAt.UTC().Format("20060102T150405Z")`; `processed_date = plannedAt.UTC().Format("2006-01-02")`; `hour = plannedAt.UTC().Format("15")`. Always call `.UTC()` first: pgx decodes `timestamptz` into `time.Local`, and test hosts are not UTC. `id8` = first 8 hex chars of sha256 over the ASCII string `"<topic_id>|<partition>|<first_offset>"` (`uuid.UUID.String()` lowercase canonical form; decimal numbers). `v=1` comes from the existing `processedSchemaVersion` (batch.go:120) via `%d`, not a literal. `event_date` is no longer a path segment; it stays in `dedup_claims` (the PK) and in rejected rows (`rejectedRow.EventDate`). Processed rows have no `event_date` field (it is the UTC date of `snapshot_at`); do NOT add one: the processed schemas use `additionalProperties: false` and `v=1` must not change.

```go
const partTimeLayout = "20060102T150405Z"

// batchID8 is the first 8 hex chars of sha256("<topic_id>|<partition>|<first_offset>"):
// the batch's position, short. A re-created topic has another topic_id.
func batchID8(topicID uuid.UUID, partition int32, firstOffset int64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d|%d", topicID, partition, firstOffset))
	return hex.EncodeToString(sum[:4])
}

// objectKey returns the key, relative to bucket zone, of the object a batch writes.
// Names carry only processing time (the manifest planned_at) and the batch id8,
// so a re-plan of the same batch yields the same key and the lake does not depend on
// the lifetime of Postgres or the broker. Panics on a bug: unknown zone, empty dataset
// for processed/rejected, non-empty dataset for dlq.
func objectKey(zone, dataset string, plannedAt time.Time, topicID uuid.UUID, partition int32, firstOffset int64) string {
	at := plannedAt.UTC()
	tail := fmt.Sprintf("processed_date=%s/hour=%s/part-%s-%s.jsonl",
		at.Format("2006-01-02"), at.Format("15"), at.Format(partTimeLayout), batchID8(topicID, partition, firstOffset))
	switch {
	case zone == s3.BucketProcessed && dataset != "":
		return fmt.Sprintf("dataset=%s/v=%d/%s", dataset, processedSchemaVersion, tail)
	case zone == s3.BucketRejected && dataset != "":
		return fmt.Sprintf("dataset=%s/%s", dataset, tail)
	case zone == s3.BucketDLQ && dataset == "":
		return fmt.Sprintf("stage=%s/%s", dlqPathStage, tail)
	}
	panic(fmt.Sprintf("transformer.objectKey: bad zone %q / dataset %q", zone, dataset))
}
```

- `type groupKey struct{ bucket, dataset string }`. Processed: `c.add(groupKey{s3.BucketProcessed, r.Dataset}, ...)` at :463. Rejected: `groupKey{s3.BucketRejected, r.Dataset}` at :602. DLQ: `c.groups[groupKey{s3.BucketDLQ, ""}] = c.dlq` at :466 (the old `xtime.FormatDate(rng.plannedAt)` prefix goes; `xtime` stays imported for `ensureClaimPartitions` and `Format`).
- `buildObjects(groups map[groupKey][]line, plannedAt time.Time, topicID uuid.UUID, partition int32, first int64) []object`: line ordering and the (bucket, key) sort are unchanged; `Key = objectKey(k.bucket, k.dataset, plannedAt, topicID, partition, first)`.
- Consequence: a batch writes one processed object per dataset (not per dataset and event_date), one rejected object per dataset and at most one dlq object; all objects of a batch share one `processed_date`/`hour` and one part name.
- The function becomes unexported; nothing outside `internal/transformer` called `ObjectKey`.

Re-plan determinism (checked against the code; no change needed): `planned_at` and `first_offset` are the only key inputs that are not constant per batch, and both come from the manifest row. `fixRange` (batch.go:532-555) returns `plannedAt = row.PlannedAt` in both non-written branches; on insert it is the `RETURNING` value of `insertPlanned` (manifest.go:62-66); on re-plan with the same first offset it is the stored row (batch.go:552). The trim branch (`row.First > first`) re-inserts within the same TX, and `now()` is constant per transaction. The `IsMissingPartition` retry (batch.go:216-226) rolls TX1 back, so no keys were stored. `setObjects` (manifest.go:119-143) still compares stored keys and sha256s and returns `ErrNonDeterministic` on any difference. A `'planned'` batch left by the old build would fail that comparison: acceptable, the stack is created fresh (`make clean`). Remaining collision risk (same topic_id via the `ETL_PIPELINE_EPOCH` fallback, same partition, first offset and planned_at second) needs the clock to go back; the write-once PUT and `ETLObjectChecksumMismatch` catch it.

Bootstrap: `etl init` becomes migrate, partitions, broker topic, buckets — it no longer reads the export ledger at all:

```go
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	log = log.With("stage", "init")
	started := time.Now()
	log.Info("init started", "brokers", cfg.Brokers, "topic", cfg.Topic, "s3_endpoint", cfg.S3Endpoint)
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
```

`initPostgres(ctx, cfg, log) error` keeps the connect, migrate and partitions steps; `initBuckets(ctx, cfg, log) error` keeps the client, `s3_ping` and `s3_buckets` steps.

Message key (TODO item 4, transformer side): no production code in `internal/transformer` or `internal/transform` reads the Kafka key (`batch.go` never touches `Message.Key`; `service.go:300` only copied it), and dedup claims are global, so partition choice does not matter to the batch logic. Delete `Message.Key` and the copy.

`Makefile` `clean` comment (:52-55), recipe unchanged:

```make
# Wipes everything: the containers, the named volumes (Postgres, Redpanda,
# Prometheus, Grafana), the lake and the logs. Object names carry only time
# (windows, planned_at), so the lake may also be kept when the stack is re-created.
```

- [ ] **Step 4: Run the targeted tests**

Run: `go build ./... && go test -short -count=1 ./internal/transformer/ ./internal/bootstrap/ ./internal/storage/s3/`
Run: `go test -race -count=1 -timeout 15m ./internal/transformer/ ./internal/bootstrap/ ./internal/storage/s3/`
Expected: PASS.

- [ ] **Step 5: Lint and the full suite**

```bash
gofmt -l . && go vet ./...
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
make test-unit && make test
```

- [ ] **Step 6: Remove what became unused**

- `internal/transformer/batch.go:724-740` `ObjectKey` (its Confluent-convention doc comment and panic message); `groupKey.prefix`; the `fmt.Sprintf` prefix strings with `event_date=`/`date=` at :463, :466, :602; `buildObjects`' `topic` parameter; `Message.Key` (:47) and `service.go:300` `Key: rec.Key`.
- `internal/transformer/keys_test.go:10-27` the old `TestObjectKey` table; `service_test.go:46-50` the comment about production keys `coins_markets`/coin id; `Key: m.Key` in `service_test.go:269` and `:490`.
- While editing `batch.go`, drop the `Task 3` history from the `rejectedRow` and `dlqRow` comments (:127, :143); T9's sweep greps for it.
- `internal/bootstrap/bootstrap.go:8-9`, `:34-41`, the `postgres_export_ledger` step, `ledgerEmpty`, `:144-158`; `bootstrap_integration_test.go:108-160`.
- `internal/storage/s3/s3.go:159-167` `HasAnyObject`; `s3_integration_test.go:150-180` `TestHasAnyObject` and imports only it used.
- `Makefile:52-55` the "reset together because object keys are unique only within one lifetime" sentence.

```bash
grep -rn 'ErrLakeFromOtherLifetime\|HasAnyObject\|s3_lake_check\|postgres_export_ledger\|ledgerEmpty' --include='*.go' .; test $? -eq 1
grep -rn 'event_date=%\|event_date=20\|+%020d\|coingecko.raw+\|ObjectKey(\|"test-key"\|Key: \[\]byte("coins_markets")' internal/transformer; test $? -eq 1
grep -n 'lifetime' Makefile; test $? -eq 1
```

- [ ] **Step 7: Commit**

```bash
git add internal/transformer internal/bootstrap internal/storage/s3/s3.go internal/storage/s3/s3_integration_test.go Makefile
git commit -m "refactor(transformer): name output objects by planned_at, drop the lake-lifetime guard" \
  -m "Processed, rejected and transform-DLQ objects are named processed_date=/hour=/part-<planned_at>-<id8>.jsonl, where id8 hashes topic id, partition and first offset; a re-planned batch keeps its key and a re-created topic gets new ones. With no database or broker counters in any object name, etl init no longer refuses a lake from another lifetime." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** every transformer object follows the contract layout; its time is the manifest `planned_at` in UTC, truncated to the second; `id8` is the first 8 hex chars of sha256(`"<topic_id>|<partition>|<first_offset>"`). A re-plan stores byte-identical keys; `setObjects` still returns `ErrNonDeterministic` on any difference. No transformer key contains a Kafka offset, a topic name or `event_date`; a batch writes at most one object per (zone, dataset) and at most one dlq object. `consumer_batches.object_keys` holds `"<bucket>/<new key>"` (no schema change). `etl init` succeeds on a non-empty raw bucket with an empty ledger. `ErrLakeFromOtherLifetime`, `HasAnyObject`, their tests and the Makefile "reset together" rationale are gone; no transformer test encodes the old key scheme.

---

### Task 6: Maintenance — `etl maintenance retention` and `etl maintenance vacuum`

Adds two short scheduled jobs (TODO item 2). `retention` drops `pipeline.dedup_claims` daily partitions older than `ETL_CLAIMS_RETENTION_DAYS`, chosen by their catalog bounds, and deletes `'written'` `pipeline.consumer_batches` rows older than the same cutoff. `vacuum` runs `VACUUM (ANALYZE)` on `raw.api_calls`, `pipeline.export_batches` and `pipeline.consumer_batches` outside any transaction. Both run through T4's `runJob` frame: they log to `logs/maintenance/etl.log`, push run metrics via T1's helper, exit 0 on success and 1 on failure (2 on usage). Includes the maintenance-staleness alert rules.

Measured on postgres:18.6 (scratch container, removed afterwards):
1. `DROP TABLE` of a `dedup_claims` partition takes AccessExclusiveLock on the PARENT `pipeline.dedup_claims`. A transformer transaction that has inserted claims holds RowExclusive on the parent, so DROP waits behind it, and while DROP waits in the lock queue every new claim insert on the parent queues behind DROP. `lock_timeout` cancels the wait with SQLSTATE 55P03, and also bounds `pg_advisory_xact_lock` waits.
2. `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` does not block concurrent inserts but cannot run in a transaction block; a crash leaves a "detach pending" partition or an orphan table no longer in `pg_inherits`. Rejected for that recovery complexity; the plain DROP is held for milliseconds once granted.
3. `pg_get_expr(relpartbound)` prints the date literals in the session DateStyle (`SQL, DMY` gives `01/09/2026`), so the listing forces ISO.
4. `pool.Exec(ctx, "VACUUM (ANALYZE) raw.api_calls")` works on the partitioned parent (it covers all partitions). Two VACUUMs in one Exec string, or VACUUM inside pgx `Begin`, fail with SQLSTATE 25001.
5. The database-level `transaction_timeout = 5min` set by `migrations/00002_pipeline.sql:72-79` TERMINATES a VACUUM that runs longer (FATAL 25P04, connection closed; reproduced with 300ms on 3M rows). The vacuum session must disable it.

**Files:**
- Modify: `internal/storage/pg/pg.go` — `ClaimPartition`, `ListClaimPartitions`, `DropClaimPartition`, `IsLockTimeout` next to `EnsurePartitions` (:64-102), reusing `partitionLockKey` (:23); update the package doc (:1-2) and the `partitionLockKey` comment (:20-22) to mention retention. (`IsLockTimeout` may instead sit next to `IsMissingPartition` in `apicalls.go:95`, same style.)
- Modify: `internal/storage/pg/pg_integration_test.go` — `TestListClaimPartitions`, `TestDropClaimPartition`.
- Create: `internal/maintenance/maintenance.go` — `RetentionOptions`, `DefaultRetentionOptions`, `RetentionResult`, `Retention`, `VacuumTables`, `Vacuum`.
- Create: `internal/maintenance/maintenance_integration_test.go` — one `TestMaintenance` with subtests sharing one Postgres container.
- Modify: `cmd/etl/main.go` — `usageText` (:39-48) gains the two maintenance lines; `run()` (:65-77) gains `case "maintenance": return runMaintenance(args[1:], stderr)`.
- Modify: `cmd/etl/jobs.go` (from T4) — add `runMaintenance`, `retentionJob`, `vacuumJob` next to `runJob`.
- Modify: `cmd/etl/main_test.go` — `TestRunDispatch` (:14-35), `TestServiceRejectsBadConfig` (:37-49); add `TestMaintenanceFailsWhenPostgresUnreachable`.
- Modify: `cmd/etl/main_integration_test.go` — add `TestMaintenanceJobs`.
- Modify: `deploy/prometheus/alerts.yml` — rules `ETLMaintenanceStale` and `ETLMaintenanceNeverSucceeded` at the end of group `etl`.
- Modify: `deploy/prometheus/alerts_test.yml` — promtool tests for both.

**Interfaces:**
- Consumes: T1 `config.Config.ClaimsRetentionDays`, `config.Config.PushgatewayURL`, `obs.JobRetention`, `obs.JobVacuum`, `obs.MetricRetentionPartitionsDropped`, `obs.MetricRetentionBatchesDeleted`; T4 `runJob`, `jobFunc`; `pg.Connect`, `pg.Migrate`, `pg.EnsurePartitions`, `partitionLockKey`; `logDir` (`cmd/etl/main.go:118`), `startupTimeout` (`cmd/etl/main.go:53`); `testutil.StartPostgres` (`internal/testutil/containers.go:58`); the schema of `pipeline.consumer_batches` (`status`, `written_at`, `planned_at`; `00002_pipeline.sql:48-65`) and the `raw.api_calls` columns after T2 (`event_id, schema_version, source, endpoint, request, fetched_at, outcome, http, error`).
- Produces: CLI `etl maintenance retention | etl maintenance vacuum` (exit 0/1, 2 on usage); `type pg.ClaimPartition struct{ Schema, Name string; From, To time.Time }` with `QualifiedName() string`; `func pg.ListClaimPartitions(ctx context.Context, pool *pgxpool.Pool) (parts []pg.ClaimPartition, skipped []string, err error)`; `func pg.DropClaimPartition(ctx context.Context, pool *pgxpool.Pool, p pg.ClaimPartition, lockTimeout time.Duration) error`; `func pg.IsLockTimeout(err error) bool`; `maintenance.RetentionOptions`, `maintenance.DefaultRetentionOptions(days int)`, `maintenance.RetentionResult`, `func maintenance.Retention(ctx context.Context, pool *pgxpool.Pool, now time.Time, opt maintenance.RetentionOptions, log *slog.Logger) (maintenance.RetentionResult, error)`; `var maintenance.VacuumTables`; `func maintenance.Vacuum(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error`; Prometheus rules `ETLMaintenanceStale`, `ETLMaintenanceNeverSucceeded` (they consume `job="etl_retention"`/`"etl_vacuum"` via T7's Pushgateway scrape with `honor_labels`; T7 also schedules the jobs at 11:00 and 11:30 in one Cronicle category with max 1 job, so `logs/maintenance/etl.log` has one writer at a time).

- [ ] **Step 1: Write the failing tests**

`internal/storage/pg/pg_integration_test.go` (Docker):
- `TestListClaimPartitions`: after `Migrate` and `EnsurePartitions(2026-10-10, 1)`, plus a hand-made `pipeline.claims_legacy` partition `FOR VALUES FROM ('2026-09-01') TO ('2026-09-02')`, the result is sorted by `To`, includes `claims_legacy` with From 2026-09-01 and To 2026-09-02 UTC, and `skipped` is empty. Under `ALTER ROLE etl SET datestyle = 'SQL, DMY'` (new pool) the dates still parse.
- `TestDropClaimPartition`: dropping an existing partition removes it, and a second call is a no-op without error. While another tx holds an uncommitted claim insert, a 200ms `lockTimeout` returns an error for which `IsLockTimeout` is true, and the partition still exists.

`internal/maintenance/maintenance_integration_test.go`, `TestMaintenance` (Docker), subtests:
- `retention drops only partitions whose upper bound <= cutoff`: now=2026-10-12T11:00Z, Days 8. Partitions for 2026-10-02, 03, 04, 05 and 10-11..10-13 exist via `EnsurePartitions`, plus `claims_legacy` (2026-09-01). `Dropped == [claims_legacy, dedup_claims_p20261002, dedup_claims_p20261003]` in that order (qualified names); `p20261004` and later still exist, together with a claim row inserted for 2026-10-04.
- `retention never touches raw partitions`: `raw.api_calls_p20261002` and `p20261003` still exist afterwards.
- `retention deletes only old written batches`: `consumer_batches` rows written at cutoff-1s (deleted), exactly at cutoff (kept), at cutoff+1h (kept), and a `'planned'` row with `planned_at` 2026-09-01 (kept); `BatchesDeleted == 1`.
- `retention is idempotent`: a second run returns 0 dropped, 0 deleted, no error.
- `retention refuses days < 2`: Days 1 returns an error and drops nothing.
- `busy partition fails after retries then succeeds`: with a tx holding an uncommitted insert into today's partition, an expired partition present, and `LockTimeout 200ms, Attempts 2, RetryDelay 10ms`: `Retention` returns an error with `pg.IsLockTimeout` true, logs 2 WARN `claims partition busy, retrying` records, and drops nothing; after the tx commits the rerun drops it.
- `vacuum covers partitioned raw and pipeline tables`: insert 1000 `http_error` rows into `raw.api_calls` (`event_id gen_random_uuid()`, `schema_version 1`, `source 'coingecko'`, `endpoint 'coins_markets'`, `request '{}'`, `fetched_at` in today's partition, `outcome 'http_error'`, `http '{}'`, `error 'x'`) and delete half; insert and delete a `consumer_batches` row. `Vacuum` returns nil. Within 5 s (polling `pg_stat_user_tables`), `last_vacuum` and `last_analyze` are non-NULL for the raw partition, `pipeline.export_batches` and `pipeline.consumer_batches`.
- `vacuum survives the database transaction_timeout` (runs last): seed 300k `http_error` raw rows and delete half on the setup pool, then `ALTER DATABASE etl SET transaction_timeout = '100ms'` and open a new pool. A plain `pool.Exec("VACUUM raw.api_calls")` fails with 25P04 (the test only logs it if the machine is fast enough to finish). `Vacuum(newPool)` returns nil. Finally `ALTER DATABASE etl SET transaction_timeout = '5min'` restores the 00002 value.

`cmd/etl`:
- `TestRunDispatch` (changed): `{"maintenance"}` → exit 2 with `usage: etl`; `{"maintenance","bogus"}` → exit 2 with `unknown maintenance job "bogus"`.
- `TestServiceRejectsBadConfig` (changed): `[maintenance retention]` and `[maintenance vacuum]` with `ETL_TOPIC_PARTITIONS=six` exit 1 and name the variable; with `ETL_CLAIMS_RETENTION_DAYS=3` they exit 1 and name `ETL_CLAIMS_RETENTION_DAYS`.
- `TestMaintenanceFailsWhenPostgresUnreachable` (unit): `ETL_PG_DSN` points at a closed port with `connect_timeout=1`, `ETL_PUSHGATEWAY_URL` at an `httptest` recorder. `maintenance retention` exits 1; `<logdir>/maintenance/etl.log` contains `"msg":"job failed"` and `"job":"etl_retention"`; the recorder got `POST /metrics/job/etl_retention` with `etl_job_success 0` and no `etl_job_last_success_timestamp_seconds`.
- `TestMaintenanceJobs` (integration): against a migrated Postgres with a recorder Pushgateway: `maintenance retention` exits 0 and pushes `etl_job_success 1`, `etl_retention_partitions_dropped` and `etl_retention_batches_deleted`; `maintenance vacuum` exits 0 and pushes job `etl_vacuum`; both append to the same `logs/maintenance/etl.log`. With `ETL_PUSHGATEWAY_URL` at a closed port, vacuum still exits 0 and the log has an ERROR `pushgateway push failed`.

`deploy/prometheus/alerts_test.yml` (promtool; interval 1m and ~1700 samples — a 10m interval breaks because of the 5m staleness lookback):

```yaml
  - name: maintenance stale after 26 hours, per job
    interval: 1m
    input_series:
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_retention",instance=""}'
        values: '0x1700' # last success at time 0
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_vacuum",instance=""}'
        values: '0+60x1700' # succeeds all the time
    alert_rule_test:
      - eval_time: 25h
        alertname: ETLMaintenanceStale
        exp_alerts: []
      - eval_time: 27h
        alertname: ETLMaintenanceStale
        exp_alerts:
          - exp_labels: {severity: warning, job: etl_retention}
            exp_annotations:
              summary: "etl_retention has not succeeded for over 26 hours"
              description: "Check its Cronicle event (etl-retention or etl-vacuum) and logs/maintenance/etl.log."

  - name: no stale maintenance alert before the first run
    interval: 1m
    input_series:
      - series: 'process_start_time_seconds{job="prometheus",instance="localhost:9090"}'
        values: '0x1700'
    alert_rule_test:
      - eval_time: 27h
        alertname: ETLMaintenanceStale
        exp_alerts: []

  - name: maintenance that never succeeded
    interval: 1m
    input_series:
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_vacuum",instance=""}'
        values: '0+60x1700'
    alert_rule_test:
      - eval_time: 25h
        alertname: ETLMaintenanceNeverSucceeded
        exp_alerts: []
      - eval_time: 26h1m
        alertname: ETLMaintenanceNeverSucceeded
        exp_alerts:
          - exp_labels: {severity: warning, job: etl_retention}
            exp_annotations:
              summary: "etl_retention has never succeeded"
              description: "Check its Cronicle event (etl-retention or etl-vacuum) and logs/maintenance/etl.log."
```

- [ ] **Step 2: Run the tests and see them fail**

Run: `go test -short -count=1 ./cmd/etl/` → FAIL (`maintenance` is an unknown command).
Run: `go test -race -count=1 ./internal/storage/pg/ ./internal/maintenance/` → FAIL (`undefined: pg.ListClaimPartitions`, package maintenance missing).
Run: `make alerts-test` → FAIL (no rule `ETLMaintenanceStale`).

- [ ] **Step 3: Implement**

PG layer (`internal/storage/pg/pg.go`):

```go
// ClaimPartition is one daily partition of pipeline.dedup_claims, with its catalog bounds [From, To) as UTC dates.
type ClaimPartition struct {
	Schema, Name string
	From, To     time.Time
}

func (p ClaimPartition) QualifiedName() string { return pgx.Identifier{p.Schema, p.Name}.Sanitize() }

// ListClaimPartitions returns the partitions of pipeline.dedup_claims whose bound is a single
// FROM/TO date range, ordered by To. Others (DEFAULT, multi-column) are returned by name in
// skipped and never dropped.
func ListClaimPartitions(ctx context.Context, pool *pgxpool.Pool) (parts []ClaimPartition, skipped []string, err error)
```

SQL, in `pgx.BeginFunc`, after `tx.Exec(ctx, "SET LOCAL datestyle = 'ISO, YMD'")`:

```sql
SELECT n.nspname, c.relname, m[1], m[2]
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN LATERAL regexp_match(pg_get_expr(c.relpartbound, c.oid),
    '^FOR VALUES FROM \(''(\d{4}-\d{2}-\d{2})''\) TO \(''(\d{4}-\d{2}-\d{2})''\)$') AS m ON true
 WHERE i.inhparent = 'pipeline.dedup_claims'::regclass
 ORDER BY c.relname
```

(Verified: `claims_legacy FROM 2026-09-01 TO 2026-09-02` parses; DEFAULT gives NULL, NULL.) Scan `m[1]`, `m[2]` into `*string`; NULL goes to `skipped`; otherwise `time.Parse("2006-01-02", …)` (UTC). Sort by `To` in Go.

```go
// DropClaimPartition drops p in its own short transaction: lock_timeout, then the partition
// advisory lock (serializes with EnsurePartitions), then DROP TABLE IF EXISTS. A lock wait
// longer than lockTimeout returns the *pgconn.PgError with Code "55P03".
func DropClaimPartition(ctx context.Context, pool *pgxpool.Pool, p ClaimPartition, lockTimeout time.Duration) error
```

Body in `pgx.BeginFunc`: `tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", strconv.FormatInt(lockTimeout.Milliseconds(), 10)+"ms")` (SET LOCAL takes no bind parameters); `tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey)`; `tx.Exec(ctx, "DROP TABLE IF EXISTS "+p.QualifiedName())`.

```go
// IsLockTimeout reports SQLSTATE 55P03 (lock_not_available).
func IsLockTimeout(err error) bool // same style as IsMissingPartition (apicalls.go:95)
```

Maintenance package (`internal/maintenance/maintenance.go`):

```go
// Package maintenance holds the daily Postgres housekeeping run by the scheduler (Cronicle
// locally, Airflow/MWAA in production): claims retention and VACUUM. autovacuum keeps
// running; these complement it.
package maintenance

type RetentionOptions struct {
	Days        int           // cfg.ClaimsRetentionDays; must be >= 2 so today-1..today+3 (EnsurePartitions) is never touched
	LockTimeout time.Duration // per DROP attempt; default 3s
	Attempts    int           // DROP attempts per partition on lock timeout; default 5
	RetryDelay  time.Duration // pause between attempts; default 2s
}

func DefaultRetentionOptions(days int) RetentionOptions {
	return RetentionOptions{Days: days, LockTimeout: 3 * time.Second, Attempts: 5, RetryDelay: 2 * time.Second}
}

type RetentionResult struct {
	Cutoff            time.Time // UTC midnight of (UTC date of now) - Days
	DroppedPartitions []string  // qualified names, in drop order (oldest first)
	BatchesDeleted    int64
}

// Retention removes the dedup history the pipeline can no longer need. Partitions of
// pipeline.dedup_claims whose upper bound is <= Cutoff are dropped. Then rows of
// pipeline.consumer_batches with status 'written' and written_at < Cutoff are deleted.
// 'planned' rows are never touched (re-planning and manual repair need them).
// It is idempotent and returns the partial result with an error.
func Retention(ctx context.Context, pool *pgxpool.Pool, now time.Time, opt RetentionOptions, log *slog.Logger) (RetentionResult, error)
```

Algorithm:
- `u := now.UTC(); cutoff := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -opt.Days)`; `opt.Days < 2` → error.
- `parts, skipped, err := pg.ListClaimPartitions(...)`; each skipped name → `log.Warn("claims partition bound not understood, kept", "partition", name)`.
- For each `p` with `!p.To.After(cutoff)` (To <= cutoff), oldest first: try `pg.DropClaimPartition` up to `opt.Attempts` times; on `pg.IsLockTimeout`, `log.Warn("claims partition busy, retrying", "partition", name, "attempt", n)` and wait `opt.RetryDelay` or until ctx is done. Any other error, or the last timeout, returns the partial result. On success append `p.QualifiedName()` and `log.Info("dropped claims partition", "partition", name, "from", p.From, "to", p.To)`.
- Then `tag, err := pool.Exec(ctx, `DELETE FROM pipeline.consumer_batches WHERE status = 'written' AND written_at < $1`, cutoff)`; `res.BatchesDeleted = tag.RowsAffected()`. No index on `written_at` is needed (about one row per transformer batch, scanned once a day).
- `log.Info("retention finished", "cutoff", xtime.FormatDate(cutoff), "days", opt.Days, "partitions_dropped", n, "batches_deleted", m)`.
- Example: now 2026-10-12T11:00Z, Days 8 → cutoff 2026-10-04: partitions for event dates <= 2026-10-03 are dropped and 2026-10-04 is kept; written batches with `written_at` < 2026-10-04T00:00Z are deleted.
- `raw.api_calls` partitions and `pipeline.export_batches`/`export_windows` are NOT touched (TODO item 2 lists them as open; the `export_batch_id` FK forbids deleting ledger rows before raw).

```go
// VacuumTables are vacuumed in this order, one statement each.
var VacuumTables = []string{"raw.api_calls", "pipeline.export_batches", "pipeline.consumer_batches"}

// Vacuum runs VACUUM (ANALYZE) on VacuumTables, one autocommit statement per table on one
// dedicated connection. The connection is hijacked from the pool and closed at the end, so its
// session settings never reach the pool. On the partitioned parent raw.api_calls, VACUUM covers
// every partition, and ANALYZE also refreshes the parent-level statistics autovacuum never collects.
func Vacuum(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error
```

Body: `c, err := pool.Acquire(ctx)`; `conn := c.Hijack()`; `defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()` (`(*pgx.Conn).Close` returns an error and is not in `.golangci.yml`'s errcheck exclusions, so a bare `defer conn.Close(...)` fails golangci-lint; the exporter's `releaseLock` uses the same `_ =` form); `conn.Exec(ctx, "SET transaction_timeout = 0")` (the database default of 5min from 00002 would kill a long VACUUM with FATAL 25P04); `conn.Exec(ctx, "SET statement_timeout = 0")`; then for each table: `t0 := time.Now()`; `conn.Exec(ctx, "VACUUM (ANALYZE) "+table)` (table names are constants, never input); `log.Info("vacuumed", "table", table, "duration_ms", …)`. The first error returns `fmt.Errorf("vacuum %s: %w", table, err)`. Never use pgx `Begin`, and never join two statements in one Exec.

CLI. `usageText` gains:

```
  maintenance retention  drop expired dedup claims partitions and written batch rows (one-shot, scheduled)
  maintenance vacuum     VACUUM (ANALYZE) the updated pipeline tables (one-shot, scheduled)
```

`run()`: `case "maintenance": return runMaintenance(args[1:], stderr)`. In `cmd/etl/jobs.go`:

```go
func runMaintenance(args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "retention":
		return runJob("maintenance", obs.JobRetention, stderr, retentionJob)
	case "vacuum":
		return runJob("maintenance", obs.JobVacuum, stderr, vacuumJob)
	}
	fmt.Fprintf(stderr, "etl: unknown maintenance job %q\n%s", args[0], usageText)
	return 2
}

// retentionJob returns its gauges also on error: they count what really was removed.
func retentionJob(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
	gauges := func(r maintenance.RetentionResult) map[string]float64 {
		return map[string]float64{
			obs.MetricRetentionPartitionsDropped: float64(len(r.DroppedPartitions)),
			obs.MetricRetentionBatchesDeleted:    float64(r.BatchesDeleted),
		}
	}
	cctx, cancel := context.WithTimeout(ctx, startupTimeout)
	pool, err := pg.Connect(cctx, cfg.PostgresDSN)
	cancel()
	if err != nil {
		return gauges(maintenance.RetentionResult{}), err
	}
	defer pool.Close()
	res, err := maintenance.Retention(ctx, pool, time.Now(), maintenance.DefaultRetentionOptions(cfg.ClaimsRetentionDays), log)
	return gauges(res), err
}

func vacuumJob(ctx context.Context, cfg config.Config, log *slog.Logger) (map[string]float64, error) {
	cctx, cancel := context.WithTimeout(ctx, startupTimeout)
	pool, err := pg.Connect(cctx, cfg.PostgresDSN)
	cancel()
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	return nil, maintenance.Vacuum(ctx, pool, log)
}
```

Both write to `logs/maintenance/etl.log` (`service=maintenance`; the `job` attribute tells them apart).

Alerts (`deploy/prometheus/alerts.yml`, end of group `etl`; the Pushgateway scrape job with `honor_labels: true` arrives in T7 and keeps `job="etl_retention"/"etl_vacuum"`):

```yaml
      - alert: ETLMaintenanceStale
        # Retention and VACUUM run daily (Cronicle events etl-retention and
        # etl-vacuum). Fires only for a job that has succeeded before; a job that
        # never succeeded is ETLMaintenanceNeverSucceeded.
        expr: |
          time() - max by (job) (etl_job_last_success_timestamp_seconds{job=~"etl_retention|etl_vacuum"}) > 26 * 3600
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.job }} has not succeeded for over 26 hours"
          description: "Check its Cronicle event (etl-retention or etl-vacuum) and logs/maintenance/etl.log."

      - alert: ETLMaintenanceNeverSucceeded
        # A fresh stack runs maintenance at 11:00/11:30 only: absent for 26 hours
        # means it never succeeded.
        expr: |
          absent(etl_job_last_success_timestamp_seconds{job="etl_retention"})
          or
          absent(etl_job_last_success_timestamp_seconds{job="etl_vacuum"})
        for: 26h
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.job }} has never succeeded"
          description: "Check its Cronicle event (etl-retention or etl-vacuum) and logs/maintenance/etl.log."
```

- [ ] **Step 4: Run the targeted tests**

Run: `go test -short -count=1 ./cmd/etl/ ./internal/maintenance/ ./internal/storage/pg/`
Run: `go test -race -count=1 ./internal/maintenance/ ./internal/storage/pg/ ./cmd/etl/`   (Docker)
Run: `make alerts-test`
Expected: PASS.

- [ ] **Step 5: Lint, the full suite and a manual run**

```bash
gofmt -l internal cmd                        # empty
go vet ./...
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
make test-unit && make test
go build -o bin/etl ./cmd/etl && ./bin/etl maintenance; echo $?          # usage, 2
make up && ETL_UID=$(id -u) ETL_GID=$(id -g) docker compose run --rm --no-deps init maintenance retention; echo $?   # 0; an ERROR "pushgateway push failed" is expected (no pushgateway service until T7)
ETL_UID=$(id -u) ETL_GID=$(id -g) docker compose run --rm --no-deps init maintenance vacuum; echo $?       # 0
tail -n 20 logs/maintenance/etl.log                                      # job started/finished records for etl_retention and etl_vacuum
make down
```

- [ ] **Step 6: Remove what became unused**

Nothing in the existing code becomes unused: this task adds a subcommand. Do not add a separate `--now` flag or a `DETACH CONCURRENTLY` path, and do not add a second job frame: `runJob` from T4 is the only one (`grep -n 'func runJob' cmd/etl/*.go` prints one line).

- [ ] **Step 7: Commit**

```bash
git add internal/storage/pg internal/maintenance cmd/etl deploy/prometheus/alerts.yml deploy/prometheus/alerts_test.yml
git commit -m "feat(maintenance): add etl maintenance retention and vacuum jobs" \
  -m "Retention drops dedup_claims partitions whose catalog upper bound is older than ETL_CLAIMS_RETENTION_DAYS (lock_timeout and retries per DROP) and deletes written consumer_batches rows; planned rows and raw partitions are never touched. Vacuum runs VACUUM (ANALYZE) per table on its own connection with transaction_timeout off. Both report to the Pushgateway; ETLMaintenanceStale and ETLMaintenanceNeverSucceeded alert after 26 hours." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** `etl maintenance retention` drops exactly the `pipeline.dedup_claims` partitions whose catalog upper bound is <= UTC today - ETL_CLAIMS_RETENTION_DAYS (names are irrelevant), deletes only `'written'` `consumer_batches` rows with `written_at` < that cutoff, and never touches `'planned'` rows or `raw.api_calls` partitions. Each DROP is bounded by `lock_timeout` and retried, so claim inserts are never blocked longer than `LockTimeout` per attempt. `etl maintenance vacuum` runs `VACUUM (ANALYZE)` on the three tables as separate autocommit statements with `transaction_timeout` and `statement_timeout` disabled on its own connection. Both append to `logs/maintenance/etl.log` and push under `job="etl_retention"`/`"etl_vacuum"` (last_success only on success; a push failure is logged at ERROR and does not change the exit code); exit 0 ok, 1 failure, 2 usage. Both alert rules exist and their promtool tests pass.

---

### Task 7: Ops — Cronicle, Pushgateway, alerts, Grafana, compose, Makefile

Runs the batch jobs (`etl publish` every `ETL_WINDOW`, `etl maintenance retention` and `vacuum` daily) from a self-built Cronicle container whose events are provisioned as code at every start; collects job results in a persistent Pushgateway; alerts on a stale publisher (maintenance staleness is T6's); shows the batch jobs in Grafana; makes `make demo` work with 1-minute windows; removes what this change leaves unused. Everything below was prototyped on Docker 29.7 (arm64) with Cronicle v0.9.135, node:24.21.0-trixie-slim and prom/pushgateway:v1.11.4; validated full-file drafts are under `/private/tmp/claude-501/-Users-ruslan-code-trust-wallet-test/627e19a9-e998-4647-b498-c775b2d81667/scratchpad/plan/repo` (diffs in `.../plan/final/*.diff`). They were made against `b883ac0`: re-apply them on top of T1-T6 — the publisher service, its scrape job and alert entries are already gone (T4), `ETLMaintenanceStale` already exists (T6) and `ETLExportLagHigh` keeps T3's threshold and texts (draft conflicts 6, 8, 9). The copies below are authoritative.

Cronicle facts found by prototyping (v0.9.135 source read and run); the docs task needs them too:
1. No official image. Manual install = download the release tarball, `npm install`, `node bin/build.js dist`. We use `npm ci --omit=dev --ignore-scripts` (the postinstall hook is pixl-boot, which registers a systemd service). The build generates a random `secret_key` into `conf/config.json` and copies `sample_conf` to `conf`.
2. Foreground run: `CRONICLE_foreground=1`. Every `CRONICLE_<key>` env var overrides a top-level config key (`__` separates nested keys). `CRONICLE_echo='Error Transaction'` echoes only errors and app transactions (event create/update, job results), without the storage chatter `echo=1` prints every 2 s.
3. Cronicle normally waits `master_ping_timeout` (60 s) before becoming primary; `CRONICLE_master=1` makes a single server primary at once: provisioning finished 2 s after container start, jobs ran normally, no MASTER CONFLICT.
4. Storage: Filesystem engine, `base_dir` `data` relative to `/opt/cronicle`; logs and the pid file in `logs/`. `engine.js` chmods `logs/jobs` and `queue_dir`, so both must be created by the running uid (a root-owned `queue/` crashed with EPERM): hence `CRONICLE_queue_dir=logs/queue` and world-writable volume roots. `bin/storage-cli.js` reads `conf/config.json` WITHOUT env overrides, so storage must stay at the default path.
5. `bin/storage-cli.js setup` reads `conf/setup.json` and exits 1 if storage already exists, so a marker file (`data/.etl-setup-done`) guards it. It replaces `_HOSTNAME_` in the `maingrp` server-group regexp with the setup-time hostname; `setup-gen.js` changes that regexp to `.+`, and compose also pins `hostname: cronicle`.
6. Passwords use pixl-server-user's scheme: `bcryptjs.hashSync(password + salt)`; `setup-gen.js` uses `/opt/cronicle/node_modules/bcryptjs`. `POST /api/user/login {username, password}` gave code 0 for the right password and code `login` for a wrong one.
7. API keys are records in the list `global/api_keys` `{id, title, key (word chars), active, privileges, username, created, modified}`; one added through `setup.json` authenticated via the `X-API-Key` header.
8. `create_event` accepts a caller-chosen id (lowercased, non-word characters stripped) and does NOT check for duplicates, so provisioning calls `get_schedule` first, then `update_event` or `create_event`. `get_event` on a missing id logs an ERROR line, so it is not used. `update_event` replaces only the fields sent; we send all of ours. A new event's cursor is "now", so creation does not backfill.
9. Job environment = a copy of the Cronicle process env + `JOB_NOW` (epoch seconds of the scheduled minute, also for catch-up runs) + `JOB_*` fields + upper-cased plugin params. The Shell Plugin writes `params.script` to a temp file and runs it; the exit code decides success; stdout and stderr go to the job log, and the first stderr line is appended to the job description. SIGTERM on abort is forwarded to the child; `exec etl ...` in the script makes etl receive it directly.
10. With `catch_up=1` and `max_children=1`, a launch blocked by a running job (including one waiting for a retry) does not advance the cursor, so missed minutes run later, in order — back to back once the blocker ends (prototype jobs of 20 s ran at :00, :20, :40 of one minute, each with its own `JOB_NOW`). While blocked, Cronicle logs an ERROR `Failed to launch scheduled event ... Maximum of 1 job already running` every minute: expected noise.
11. Retries re-run the same job id with the same `JOB_NOW` after `retry_delay`; the job log shows every attempt. A timeout abort is NOT retried (`job.js` sets `retries = 0` and `no_rewind`).
12. A graceful stop (SIGTERM via tini; `docker stop` took 11 s) aborts running jobs and rewinds the cursor: after restart that window ran again (one failed run "Aborted Job: Server cronicle shut down unexpectedly" plus a green re-run). After SIGKILL the in-flight job is recorded as failed from the leftover `logs/jobs/<id>.json` at startup, but NOT re-run (the cursor was already past it); that record exists only because `logs/` is on the volume.
13. `run_event` accepts a `now` override and params/retries overrides; the prototype ran `{id:'etlpublish', now:1791760000, retries:2, retry_delay:5}` and read the result through `get_event_history`, `get_job_status` (fields `id, event, now, code, description, elapsed`) and `get_job_log` (gzip: `curl --compressed`).
14. Node as PID 1 does not reap orphans (the finished `provision.js` stayed a zombie): compose `init: true` (tini) plus a subshell double-fork fixes it, verified with `ps`.
15. `/bin/ps` is missing in slim images; Cronicle logs `Failed to exec ps` every second without `procps`.
16. A category's `max_children` limits concurrent jobs across all its events; a blocked catch-up event retries the next minute.

**Files:**
- Modify: `Dockerfile` — insert stages `cronicle-build` and `cronicle` between the Go build stage (:3-20) and the distroless runtime stage (:22-31). The runtime stage must stay LAST (default target of `docker build .`, the CI image job and every x-etl service); add `# The last stage is the default target (docker build . and the etl services).` above its `FROM`.
- Modify: `.dockerignore` — after `deploy/` (:10): `# ...except the cronicle stage's entrypoint and provisioning scripts.`, `!deploy/cronicle/`, `deploy/cronicle/*.test.js`.
- Create: `deploy/cronicle/entrypoint.sh` (mode 0755, POSIX sh), `deploy/cronicle/setup-gen.js`, `deploy/cronicle/provision.js`, `deploy/cronicle/provision.test.js`.
- Modify: `docker-compose.yml` — split the anchors (new top-level `x-etl-env`), delete `ETL_HTTP_ADDR: ":8080"` (:15; the runtime image ENV sets it, and the shared env now also feeds the cronicle jobs, which serve no HTTP), add the `pushgateway` service before `prometheus` (:111) and the `cronicle` service after `transformer` (with `depends_on` exporter healthy), add the named volumes `pushgateway` and `cronicle`. The transformer keeps `environment: <<: *etl-env` plus `ETL_LOG_PER_HOST`.
- Modify: `deploy/prometheus/prometheus.yml` — header comment (:1-3); add job `pushgateway` with `honor_labels: true`.
- Modify: `deploy/prometheus/alerts.yml` — header (:1-5); `ETLDeadLetterRecords` (:23-34) publish branch; `ETLServiceDown` comment gains `ETLPublisherStale covers it.`; `ETLPipelineNotProgressing` description (:107); `ETLObjectChecksumMismatch` comment; new rule `ETLPublisherStale`.
- Modify: `deploy/prometheus/alerts_test.yml` — the two `ETLPipelineNotProgressing` descriptions, a pushgateway series in the "not a pipeline service" case, 4 new cases.
- Modify: `deploy/grafana/dashboards/etl.json` — hand-edit, keeping the file's compact formatting (the draft file was re-indented by a script: do not copy it): descriptions of panels 3, 5, 7 and 10; new panels 15-18 at y=52 and y=60.
- Modify: `internal/obs/metrics.go:107-111` — `E2ELatency` buckets; `internal/obs/metrics_test.go` — `TestE2ELatencyBuckets`.
- Modify: `Makefile` — `DEMO_WAIT` (:16) → `CRONICLE_URL`, `DEMO_TIMEOUT ?= 300`, `NODE_IMAGE`; `cronicle-test` in `.PHONY` (:18) and as a target after `alerts-test`; `clean` comment and `down -v --remove-orphans` (a container of the removed `publisher` service, left by a pre-T4 `make up`, would otherwise survive `make clean`); `demo` with `ETL_WINDOW=1m` polling for processed output; `demo-report` additions.
- Modify: `.env.example` — `PUSHGATEWAY_PORT=9091` and `CRONICLE_PORT=3012` in the host ports block (:14-20); extend the `ETL_WINDOW` comment; add `ETL_SCHEDULE_TZ`, `CRONICLE_ADMIN_PASSWORD`, `CRONICLE_API_KEY`, `CRONICLE_WEB_HOOK`. Remove any key whose config field no longer exists in `internal/config/config.go`.
- Modify: `.github/workflows/ci.yml` — `deploy-config` job (:48-57): step `make cronicle-test` after `make alerts-test`; `image` job (:59-70): name the existing step `etl image (default target)` and add a second `docker/build-push-action` step with `target: cronicle`, `tags: coingecko-etl-cronicle:ci`, same gha cache.

**Interfaces:**
- Consumes: T4 `etl publish --now <unix seconds|RFC3339>` (exit codes, `logs/publisher/etl.log`, push to `ETL_PUSHGATEWAY_URL` with `job="etl_publisher"`, never opens `ETL_HTTP_ADDR`, never needs Postgres); T6 `etl maintenance retention|vacuum` (`logs/maintenance/etl.log`, `job="etl_retention"`/`"etl_vacuum"`, needs `ETL_PG_DSN`), T6's maintenance alert rules; T1 `ETL_WINDOW` validation (mirrored by `provision.js`), `ETL_CLAIMS_RETENTION_DAYS`; T3 raw windows with `_SUCCESS` last and the bootstrap window (so the first publish run is green); T3 `etl_export_lag_seconds` semantics (unchanged here).
- Produces: compose services `cronicle` (UI and API on `127.0.0.1:${CRONICLE_PORT:-3012}`) and `pushgateway` (`127.0.0.1:${PUSHGATEWAY_PORT:-9091}`); named volumes `cronicle`, `pushgateway`; image `coingecko-etl-cronicle:local` = Dockerfile target `cronicle`. Cronicle events `etlpublish` (title `etl-publish`), `etlretention` (`etl-retention`), `etlvacuum` (`etl-vacuum`); API key env `CRONICLE_API_KEY` (default `etl_local_dev_api_key_0001`); login `admin` / `CRONICLE_ADMIN_PASSWORD`; optional `CRONICLE_WEB_HOOK`; re-run one window with `POST /api/app/run_event/v1 {"id":"etlpublish","now":<epoch>}` or `docker compose exec cronicle etl publish --now <ts>`. Job environment: `ETL_PG_DSN`, `ETL_BROKERS`, `ETL_S3_ENDPOINT`, `ETL_LOG_DIR=/app/logs`, `ETL_WINDOW`, `ETL_PUSHGATEWAY_URL=http://pushgateway:9091`, `ETL_SCHEDULE_TZ`, everything from `.env`, plus Cronicle's `JOB_*`. `ETL_WINDOW` reaches every etl container via `${ETL_WINDOW:-5m}` (a shell variable overrides `.env`). Prometheus: the `pushgateway` scrape job (`honor_labels`), alert `ETLPublisherStale` (critical), publish DLQ lines in `ETLDeadLetterRecords` as `stage=publish`. Make: `cronicle-test`; `demo` with `ETL_WINDOW=1m` and `DEMO_TIMEOUT`; `CRONICLE_URL`.

- [ ] **Step 1: Write the failing tests**

`deploy/cronicle/provision.test.js` (`node --test`, run by `make cronicle-test`):

```js
// node --test deploy/cronicle/  (make cronicle-test runs it in the Node image)
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseGoDuration, windowMinutes, publishMinutes, timeZone, events } = require('./provision.js');

test('parseGoDuration follows Go syntax', () => {
  assert.equal(parseGoDuration('5m'), 300000);
  assert.equal(parseGoDuration('1h'), 3600000);
  assert.equal(parseGoDuration('1m30s'), 90000);
  assert.equal(parseGoDuration('300s'), 300000);
  assert.equal(parseGoDuration('+5m'), 300000); // time.ParseDuration accepts the sign too
  for (const bad of ['', '+', '5', 'm', '5 m', '5min', '-5m']) assert.throws(() => parseGoDuration(bad), bad);
});

test('windowMinutes accepts exactly the divisors of an hour', () => {
  const ok = [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60]; // the same table as window.TestValidate
  for (let m = 1; m <= 60; m++) {
    if (ok.includes(m)) assert.equal(windowMinutes(`${m}m`), m);
    else assert.throws(() => windowMinutes(`${m}m`), `${m}m`);
  }
  for (const bad of ['0s', '30s', '90s', '61m', '2h', '5m30s']) assert.throws(() => windowMinutes(bad), bad);
  assert.equal(windowMinutes('300s'), 5);
  assert.equal(windowMinutes('1h'), 60);
});

test('publishMinutes is every window end plus one minute', () => {
  assert.deepEqual(publishMinutes(5), [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56]);
  assert.deepEqual(publishMinutes(1), Array.from({ length: 60 }, (_, i) => i));
  assert.deepEqual(publishMinutes(15), [1, 16, 31, 46]);
  assert.deepEqual(publishMinutes(60), [1]);
});

test('events: publish in UTC every window, maintenance daily in ETL_SCHEDULE_TZ', () => {
  const [pub, ret, vac] = events({ ETL_WINDOW: '5m', ETL_SCHEDULE_TZ: 'Europe/Moscow' });
  assert.equal(pub.id, 'etlpublish');
  assert.equal(pub.params.script, '#!/bin/sh\nexec etl publish --now "$JOB_NOW"\n');
  assert.equal(pub.timezone, 'UTC');
  assert.deepEqual([pub.catch_up, pub.max_children, pub.retries, pub.retry_delay, pub.timeout], [1, 1, 3, 60, 300]);
  assert.deepEqual([ret.timing, ret.timezone], [{ hours: [11], minutes: [0] }, 'Europe/Moscow']);
  assert.deepEqual([vac.timing, vac.timezone], [{ hours: [11], minutes: [30] }, 'Europe/Moscow']);
  assert.equal(events({}).find((e) => e.id === 'etlretention').timezone, 'UTC');
  assert.throws(() => events({ ETL_WINDOW: '7m' }));
  assert.deepEqual([pub.category, ret.category, vac.category], ['general', 'maintenance', 'maintenance']);
});

test('timeZone accepts IANA names only', () => {
  assert.equal(timeZone('UTC'), 'UTC');
  assert.equal(timeZone('Europe/Moscow'), 'Europe/Moscow');
  assert.throws(() => timeZone('Mars/Olympus'));
  assert.throws(() => events({ ETL_SCHEDULE_TZ: 'nowhere' }));
});
```

Compose environment check (Review Focus 3), the second line of the `cronicle-test` target: `ETL_WINDOW=15m docker compose config --format json` must give `15m` to `exporter`, `transformer` and `cronicle` (`jq -e '[.services.exporter, .services.transformer, .services.cronicle] | all(.environment.ETL_WINDOW == "15m")'`).

`internal/obs/metrics_test.go` — `TestE2ELatencyBuckets`: after one `Observe`, the gathered `etl_e2e_latency_seconds` histogram has bucket upper bounds exactly `{5, 15, 30, 60, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800, 3600}`.

`deploy/prometheus/alerts_test.yml` — the two `ETLPipelineNotProgressing` cases expect the new description `"The exporter, the publisher (Cronicle job etl-publish), the broker or the transformers are stalled; check their logs, ETLServiceDown and ETLPublisherStale."`; the "not a pipeline service" `ETLServiceDown` case gains `- series: 'up{job="pushgateway",instance="pushgateway:9091"}'` with `values: '0x10' # not a pipeline service either` and still expects no alert; new cases:

```yaml
  - name: a publish run with dead-letter lines fires until a clean run
    interval: 1m
    input_series:
      - series: 'etl_publish_dlq_lines{job="etl_publisher",instance=""}'
        values: '0 0 0 0 0 2 2 2 2 2 0 0 0 0 0' # one window had 2 bad lines
    alert_rule_test:
      - eval_time: 3m
        alertname: ETLDeadLetterRecords
        exp_alerts: []
      - eval_time: 7m
        alertname: ETLDeadLetterRecords
        exp_alerts:
          - exp_labels: {severity: warning, stage: publish, error_class: "see the window's DLQ object"}
            exp_annotations:
              summary: "Records went to the dead-letter zone (publish / see the window's DLQ object)"
              description: "Inspect data/dlq/; a human is needed."
      - eval_time: 12m
        alertname: ETLDeadLetterRecords
        exp_alerts: []

  - name: publisher stale after fifteen minutes without a success
    interval: 1m
    input_series:
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_publisher",instance=""}'
        values: '60x30' # last success at minute 1, then only failures
      - series: 'process_start_time_seconds{job="prometheus",instance="localhost:9090"}'
        values: '0x30'
    alert_rule_test:
      - eval_time: 15m
        alertname: ETLPublisherStale
        exp_alerts: [] # 14 minutes old
      - eval_time: 20m
        alertname: ETLPublisherStale
        exp_alerts:
          - exp_labels: {severity: critical}
            exp_annotations:
              summary: "No successful etl publish in over 15 minutes"
              description: "Check the etl-publish job in Cronicle (red runs, their logs) and logs/publisher/etl.log; a missing raw _SUCCESS means the exporter is behind."

  - name: no publisher alert while every window publishes
    interval: 1m
    input_series:
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_publisher",instance=""}'
        values: '0 0 0 0 0 300 300 300 300 300 600 600 600 600 600 900 900 900 900 900 1200 1200 1200 1200 1200 1500 1500 1500 1500 1500 1800'
      - series: 'etl_job_last_success_timestamp_seconds{job="etl_retention",instance=""}'
        values: '0x30' # other jobs do not count
      - series: 'process_start_time_seconds{job="prometheus",instance="localhost:9090"}'
        values: '0x30'
    alert_rule_test:
      - eval_time: 30m
        alertname: ETLPublisherStale
        exp_alerts: []

  - name: a publisher that never succeeded fires after the startup grace
    interval: 1m
    input_series:
      - series: 'process_start_time_seconds{job="prometheus",instance="localhost:9090"}'
        values: '0x30'
    alert_rule_test:
      - eval_time: 10m
        alertname: ETLPublisherStale
        exp_alerts: [] # Prometheus up for 10 minutes: still in the startup grace
      - eval_time: 20m
        alertname: ETLPublisherStale
        exp_alerts:
          - exp_labels: {severity: critical, job: etl_publisher}
            exp_annotations:
              summary: "No successful etl publish in over 15 minutes"
              description: "Check the etl-publish job in Cronicle (red runs, their logs) and logs/publisher/etl.log; a missing raw _SUCCESS means the exporter is behind."
```

- [ ] **Step 2: Run the tests and see them fail**

Run: `make cronicle-test` → FAIL (no target / no `provision.js`).
Run: `make alerts-test` → FAIL (no rule `ETLPublisherStale`, old descriptions).
Run: `go test -short -count=1 -run TestE2ELatencyBuckets ./internal/obs/` → FAIL (old buckets).

- [ ] **Step 3: Implement**

`Dockerfile`, inserted after the build stage:

```dockerfile
# ---- cronicle-build: Cronicle from the pinned upstream release ----
# Cronicle publishes no official image; it is built from the release tarball.
FROM node:24.21.0-trixie-slim AS cronicle-build
ARG CRONICLE_VERSION=0.9.135
WORKDIR /opt/cronicle
ADD --checksum=sha256:96dcfebd7682df31134cfea2fa5e0f5d372fcddbfb82488e9ce0a12122367cd1 \
    https://github.com/jhuckaby/Cronicle/archive/refs/tags/v${CRONICLE_VERSION}.tar.gz /tmp/cronicle.tar.gz
# --ignore-scripts skips the postinstall hook that registers a systemd service.
RUN tar -xzf /tmp/cronicle.tar.gz --strip-components 1 \
 && rm /tmp/cronicle.tar.gz \
 && npm ci --omit=dev --ignore-scripts --no-audit --no-fund \
 && node bin/build.js dist \
 && rm -rf /root/.npm

# ---- cronicle: the scheduler of the batch jobs, with the etl binary ----
FROM node:24.21.0-trixie-slim AS cronicle
# procps: Cronicle samples the CPU and memory of running jobs with ps.
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=cronicle-build /opt/cronicle /opt/cronicle
COPY --from=build /out/etl /usr/local/bin/etl
COPY deploy/cronicle/entrypoint.sh deploy/cronicle/setup-gen.js deploy/cronicle/provision.js /opt/cronicle/etl/
# Storage (data/) and logs (logs/, including the job files Cronicle uses to
# record a job cut short by an unclean stop) live on one volume at
# /var/lib/cronicle. The container runs as the host uid, so the volume root and
# /app/logs are world-writable (Docker copies the mode into a fresh volume),
# and setup-gen.js may rewrite conf/setup.json.
RUN mkdir -p /var/lib/cronicle /app/logs \
 && chmod 1777 /var/lib/cronicle /app/logs \
 && rm -rf /opt/cronicle/data /opt/cronicle/logs \
 && ln -s /var/lib/cronicle/data /opt/cronicle/data \
 && ln -s /var/lib/cronicle/logs /opt/cronicle/logs \
 && chmod 0666 /opt/cronicle/conf/setup.json \
 && chmod 0755 /opt/cronicle/etl/entrypoint.sh
WORKDIR /opt/cronicle
# foreground: no daemon fork; echo: the error log and the transaction log
# (event changes, job starts and ends) to stdout; master: a single server
# becomes primary at once instead of after a 60 s election timeout.
ENV CRONICLE_foreground=1 \
    CRONICLE_echo="Error Transaction" \
    CRONICLE_color=0 \
    CRONICLE_master=1 \
    CRONICLE_queue_dir=logs/queue \
    CRONICLE_job_data_expire_days=30 \
    ETL_LOG_DIR=/app/logs
EXPOSE 3012
ENTRYPOINT ["/opt/cronicle/etl/entrypoint.sh"]
```

No `USER` line: compose sets `user: ETL_UID:ETL_GID` (any uid works; tested 501:20). The Go build stage's `COPY . .` now also sees `deploy/cronicle/*.js`, so editing them re-runs `go build` (cheap thanks to the cache mount; accepted). Pins: Cronicle v0.9.135 is the latest release (2026-10-01); node 24 is the current LTS (Docker tag `lts` = 24.21.0 on 2026-10-06); pushgateway v1.11.4 (2026-10-08) was tested, v1.11.3 is the fallback. If GitHub ever regenerates the archive and the checksum fails, recompute it with `curl -fsSL <url> | shasum -a 256`.

`deploy/cronicle/entrypoint.sh`:

```sh
#!/bin/sh
# Entrypoint of the cronicle container: one-time storage setup on a fresh
# volume, background provisioning of the ETL events, then Cronicle itself in the
# foreground. exec keeps this PID, so Cronicle receives SIGTERM from tini
# (compose init: true), aborts running jobs and rewinds their event cursor.
set -eu
cd /opt/cronicle
mkdir -p /var/lib/cronicle/data /var/lib/cronicle/logs

if [ ! -f data/.etl-setup-done ]; then
  node etl/setup-gen.js          # conf/setup.json: admin password, provisioning API key
  node bin/storage-cli.js setup  # users, plugins, server groups, the API key
  touch data/.etl-setup-done
fi

# A restarted container keeps the pid file of the previous run.
rm -f logs/cronicled.pid

# The subshell detaches provisioning from this PID, so tini reaps it. On
# failure it sends SIGTERM to Cronicle ($$): the container stops and the
# restart policy retries, instead of a scheduler running without its events.
( node etl/provision.js "$$" & )

exec node lib/main.js
```

`deploy/cronicle/setup-gen.js`:

```js
// Rewrites conf/setup.json, which `bin/storage-cli.js setup` reads once on a
// fresh volume: the admin password and the provisioning API key come from the
// environment, and the primary server group matches any hostname.
'use strict';
const crypto = require('crypto');
const fs = require('fs');
const bcrypt = require('/opt/cronicle/node_modules/bcryptjs');

const FILE = '/opt/cronicle/conf/setup.json';

function main() {
  const password = process.env.CRONICLE_ADMIN_PASSWORD || 'admin';
  const apiKey = process.env.CRONICLE_API_KEY || '';
  if (!/^\w{16,}$/.test(apiKey)) {
    throw new Error('CRONICLE_API_KEY must be at least 16 characters of [A-Za-z0-9_]');
  }
  const setup = JSON.parse(fs.readFileSync(FILE, 'utf8'));
  const now = Math.floor(Date.now() / 1000);
  for (const [op, key, obj] of setup.storage) {
    if (op === 'put' && key === 'users/admin') {
      obj.salt = crypto.randomBytes(32).toString('hex');
      obj.password = bcrypt.hashSync(password + obj.salt, 10); // pixl-server-user: bcrypt(password + salt)
    }
    if (op === 'listPush' && key === 'global/server_groups' && obj.id === 'maingrp') {
      obj.regexp = '.+';
    }
  }
  // Retention and VACUUM share logs/maintenance/etl.log: at most one runs at a
  // time (a blocked catch-up event retries the next minute).
  setup.storage.push(['listPush', 'global/categories', {
    id: 'maintenance',
    title: 'Maintenance',
    description: 'Postgres maintenance jobs; one at a time.',
    enabled: 1,
    max_children: 1,
    username: 'admin',
    created: now,
    modified: now,
  }]);
  setup.storage.push(['listPush', 'global/api_keys', {
    id: 'ketlprovision',
    title: 'etl-provision',
    description: 'Creates and updates the ETL events at container start.',
    key: apiKey,
    active: 1,
    username: 'admin',
    created: now,
    modified: now,
    privileges: { create_events: 1, edit_events: 1, delete_events: 0, run_events: 1, abort_events: 1, state_update: 0 },
  }]);
  fs.writeFileSync(FILE, JSON.stringify(setup, null, 2));
  console.log('setup-gen: wrote ' + FILE);
}

try { main(); } catch (e) { console.error('setup-gen: ' + e.message); process.exit(1); }
```

`deploy/cronicle/provision.js`:

```js
// Creates or updates the ETL events through the Cronicle API at every container
// start. Idempotent: fixed event ids, and every field we own is sent again.
'use strict';
const fs = require('fs');

const PORT = process.env.CRONICLE_WebServer__http_port || '3012';
const BASE = `http://127.0.0.1:${PORT}/api/app`;
const MARKER = '/tmp/etl-provisioned'; // read by the compose healthcheck
const GRACE_MINUTES = 1; // the publisher's fixed grace after a window end (window.PublishGrace)

// parseGoDuration accepts Go's time.ParseDuration syntax (5m, 1h, 90s, 1m30s)
// and returns milliseconds.
function parseGoDuration(raw) {
  const unit = { ns: 1e-6, us: 1e-3, 'µs': 1e-3, ms: 1, s: 1e3, m: 6e4, h: 3.6e6 };
  if (typeof raw !== 'string') throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  // Go accepts one leading '+'. A '-' is rejected here, which is the same
  // outcome as in Go for ETL_WINDOW: no window is negative.
  const s = raw.startsWith('+') ? raw.slice(1) : raw;
  if (s === '') throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  if (s === '0') return 0;
  const re = /(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|ms|s|m|h)/gy;
  let ms = 0, pos = 0, m;
  while ((m = re.exec(s)) !== null) { ms += parseFloat(m[1]) * unit[m[2]]; pos = re.lastIndex; }
  if (pos !== s.length) throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  return ms;
}

// windowMinutes applies the ETL_WINDOW rule of internal/window: whole minutes,
// 1m to 60m, and a divisor of 60m.
function windowMinutes(raw) {
  const min = parseGoDuration(raw) / 6e4;
  if (!Number.isInteger(min) || min < 1 || min > 60 || 60 % min !== 0) {
    throw new Error(`ETL_WINDOW=${raw}: must be whole minutes, between 1m and 60m, and divide 60m`);
  }
  return min;
}

// publishMinutes lists the minutes of the hour at which the publisher runs:
// every window end (aligned to the hour) plus the grace.
function publishMinutes(n) {
  const out = [];
  for (let m = 0; m < 60; m++) if (m % n === GRACE_MINUTES % n) out.push(m);
  return out;
}

function shell(cmd) {
  return { script: `#!/bin/sh\nexec ${cmd}\n`, annotate: 0, json: 0 };
}

// timeZone returns tz if it is an IANA time zone name the runtime knows.
function timeZone(tz) {
  try { new Intl.DateTimeFormat('en-US', { timeZone: tz }); } catch (_) {
    throw new Error(`ETL_SCHEDULE_TZ=${tz}: not an IANA time zone name`);
  }
  return tz;
}

// events returns the desired events for the given environment.
function events(env) {
  const n = windowMinutes(env.ETL_WINDOW || '5m');
  const tz = timeZone(env.ETL_SCHEDULE_TZ || 'UTC');
  const common = {
    enabled: 1, category: 'general', target: 'maingrp', plugin: 'shellplug',
    max_children: 1, catch_up: 1, multiplex: 0, detached: 0, queue: 0,
    retries: 3, retry_delay: 60,
    notify_success: '', notify_fail: '', web_hook: env.CRONICLE_WEB_HOOK || '',
  };
  return [
    { ...common, id: 'etlpublish', title: 'etl-publish',
      notes: `Publishes the ${n}-minute window that ended ${GRACE_MINUTES} minute before the job time ($JOB_NOW).`,
      params: shell('etl publish --now "$JOB_NOW"'),
      timing: { minutes: publishMinutes(n) }, timezone: 'UTC', timeout: 300 },
    { ...common, id: 'etlretention', title: 'etl-retention', category: 'maintenance',
      notes: 'Drops pipeline.dedup_claims partitions and written consumer batches older than ETL_CLAIMS_RETENTION_DAYS.',
      params: shell('etl maintenance retention'),
      timing: { hours: [11], minutes: [0] }, timezone: tz, timeout: 1800 },
    { ...common, id: 'etlvacuum', title: 'etl-vacuum', category: 'maintenance',
      notes: 'VACUUM (ANALYZE) of the tables the pipeline updates.',
      params: shell('etl maintenance vacuum'),
      timing: { hours: [11], minutes: [30] }, timezone: tz, timeout: 1800 },
  ];
}

async function api(name, body) {
  const res = await fetch(`${BASE}/${name}/v1`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': process.env.CRONICLE_API_KEY || '' },
    body: JSON.stringify(body),
  });
  return res.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// schedule waits until this server is primary and returns the existing events.
async function schedule(timeoutMs) {
  const until = Date.now() + timeoutMs;
  let last = '';
  while (Date.now() < until) {
    try {
      const r = await api('get_schedule', { offset: 0, limit: 1000 });
      if (r.code === 0) return r.rows || [];
      last = JSON.stringify(r);
    } catch (e) { last = String(e); }
    await sleep(1000);
  }
  throw new Error('Cronicle API not ready: ' + last);
}

async function main() {
  try { fs.unlinkSync(MARKER); } catch (_) { /* first start */ }
  const wanted = events(process.env); // validates ETL_WINDOW before any call
  const existing = new Set((await schedule(120000)).map((e) => e.id));
  for (const ev of wanted) {
    const verb = existing.has(ev.id) ? 'update' : 'create';
    const r = await api(`${verb}_event`, ev);
    if (r.code !== 0) throw new Error(`${verb}_event ${ev.id}: ${JSON.stringify(r)}`);
    console.log(`provision: ${verb}d ${ev.title} timing=${JSON.stringify(ev.timing)} timezone=${ev.timezone}`);
  }
  fs.writeFileSync(MARKER, new Date().toISOString() + '\n');
}

if (require.main === module) {
  main().catch((e) => {
    console.error('provision: FAILED: ' + e.message);
    const cronicle = Number(process.argv[2]);
    if (cronicle > 0) { try { process.kill(cronicle, 'SIGTERM'); } catch (_) { /* gone */ } }
    process.exit(1);
  });
}

module.exports = { parseGoDuration, windowMinutes, publishMinutes, timeZone, events };
```

The maintenance category guarantees one maintenance writer at a time (`logs/maintenance/etl.log`): if retention runs past 11:30, vacuum waits and then runs (catch_up). `etl-publish` always uses timezone UTC (windows are aligned to the UTC hour); `ETL_SCHEDULE_TZ` places only the daily events (draft conflict 13). Verified through the API: `ETL_WINDOW=5m` gave minutes `[1,6,…,56]`, `2m` gave `[1,3,…,59]`; a restart with a changed `ETL_WINDOW` logged `updated` and `get_schedule` showed the new timing; an invalid `ETL_WINDOW` makes the container exit 143 and `restart: unless-stopped` retries.

`docker-compose.yml` — top of the file:

```yaml
# Endpoints and settings every etl process needs: the long-running services
# below and the batch jobs Cronicle runs. ETL_WINDOW is interpolated, so a
# shell value overrides .env (make demo runs `make up` with ETL_WINDOW=1m).
x-etl-env: &etl-env
  ETL_PG_DSN: postgres://etl:etl@postgres:5432/etl?sslmode=disable
  ETL_BROKERS: redpanda:9092
  ETL_S3_ENDPOINT: http://versitygw:7070
  ETL_LOG_DIR: /app/logs
  ETL_WINDOW: ${ETL_WINDOW:-5m}

# Shared settings of every long-running `etl` container (one image, one
# subcommand each).
x-etl: &etl
  image: coingecko-etl:local
  build: .
  user: "${ETL_UID:-65532}:${ETL_GID:-65532}"
  env_file:
    - path: .env
      required: false
  environment: *etl-env
  # volumes, restart, depends_on, healthcheck unchanged
```

`pushgateway` (before `prometheus`):

```yaml
  # Holds the metrics the batch jobs push when they end (they are not running
  # when Prometheus scrapes). Persisted, so a restart keeps the last success.
  pushgateway:
    image: prom/pushgateway:v1.11.4
    command:
      # /pushgateway is owned by the image user (nobody); a fresh named volume
      # mounted there inherits that owner (mounting at /data fails with permission denied).
      - --persistence.file=/pushgateway/metrics.db
      - --persistence.interval=1m
    volumes:
      - pushgateway:/pushgateway
    ports:
      - "127.0.0.1:${PUSHGATEWAY_PORT:-9091}:9091"
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:9091/-/ready"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped
```

`cronicle` (after `transformer`):

```yaml
  # Scheduler of the batch jobs (local stand-in for Airflow): etl publish every
  # ETL_WINDOW, etl maintenance retention and vacuum daily. The image is the
  # Dockerfile's cronicle stage: Node, Cronicle and the same etl binary.
  # Every job inherits this container's environment.
  cronicle:
    image: coingecko-etl-cronicle:local
    build:
      context: .
      target: cronicle
    hostname: cronicle
    init: true # tini as PID 1: reaps job processes, forwards SIGTERM to Cronicle
    user: "${ETL_UID:-65532}:${ETL_GID:-65532}"
    env_file:
      - path: .env
        required: false
    environment:
      <<: *etl-env
      ETL_PUSHGATEWAY_URL: http://pushgateway:9091
      ETL_SCHEDULE_TZ: ${ETL_SCHEDULE_TZ:-UTC}
      # Stored when a fresh cronicle volume is set up (make clean resets them);
      # provision.js uses the API key at every start.
      CRONICLE_ADMIN_PASSWORD: ${CRONICLE_ADMIN_PASSWORD:-admin}
      CRONICLE_API_KEY: ${CRONICLE_API_KEY:-etl_local_dev_api_key_0001}
      # Optional: URL Cronicle POSTs to at the start and end of every job.
      CRONICLE_WEB_HOOK: ${CRONICLE_WEB_HOOK:-}
      CRONICLE_base_app_url: http://localhost:${CRONICLE_PORT:-3012}
    volumes:
      - ./logs:/app/logs
      - cronicle:/var/lib/cronicle
    ports:
      - "127.0.0.1:${CRONICLE_PORT:-3012}:3012"
    # On SIGTERM Cronicle aborts running jobs (10 s kill timeout) and rewinds
    # their event cursor, so the aborted window runs again after the restart.
    stop_grace_period: 30s
    healthcheck:
      # /tmp/etl-provisioned is written by provision.js once the events exist.
      test: ["CMD-SHELL", "test -f /tmp/etl-provisioned && node -e \"fetch('http://127.0.0.1:3012/').then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1))\""]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 60s
    depends_on:
      init:
        condition: service_completed_successfully
      pushgateway:
        condition: service_healthy
      # The exporter's first tick closes the window before the current one
      # (T3). Starting the scheduler after the exporter is healthy makes the
      # first etl-publish run target that window or a later one, so it is green
      # with 1-minute windows too.
      exporter:
        condition: service_healthy
    restart: unless-stopped
```

and `volumes:` gains `pushgateway:` and `cronicle:`. Why the exporter dependency: a new Cronicle event first runs in the minute after its creation (cursor = creation minute, `lib/api/event.js:165-166`; the scheduler steps from cursor + 60 s, `lib/scheduler.js:117-131`), and a run at `t` targets the window ending at floor(t − 1 min). The exporter turns healthy a few seconds after it starts, at its first passing healthcheck. Its first tick runs right after it takes the lock, in milliseconds on a fresh stack. So provisioning always happens after the bootstrap window is closed. Without the dependency, with `ETL_WINDOW=1m`, a Cronicle provisioned in an earlier minute than the exporter's first tick targets a window no one closes, and that run stays red after its 3 retries. Every one-container command below that recreates `cronicle` uses `--no-deps`. The defaults matter because CI runs compose without a `.env`. Do not pass `ETL_HTTP_ADDR` to cronicle. The jobs serve no HTTP (`runJob` binds no port), and the long-running services get `:8080` from the runtime image's `ENV`, which `/etl healthcheck` also reads. Measured in compose: healthy 6 s after start; the real etl binary ran as uid 501 with `ETL_PG_DSN`, `ETL_BROKERS`, `ETL_S3_ENDPOINT`, `ETL_LOG_DIR`, `ETL_WINDOW`, `ETL_PUSHGATEWAY_URL` and `ETL_SCHEDULE_TZ` in its env. A shell `ETL_WINDOW=1m docker compose up` overrides `.env` (the `environment` key beats `env_file`).

`deploy/prometheus/prometheus.yml`: header becomes

```yaml
# Scrapes every long-running etl service on :8080/metrics. dns_sd_configs
# resolves the compose service name to one A record per container, so both
# transformer replicas (deploy.replicas: 2) are discovered without listing them.
# The batch jobs (etl publish, etl maintenance) push to the Pushgateway when
# they end; honor_labels keeps the job label they push (etl_publisher, ...).
```

and, before the `prometheus` job:

```yaml
  - job_name: pushgateway
    honor_labels: true
    static_configs:
      - targets: ["pushgateway:9091"]
```

Verified: Prometheus stored `etl_job_last_success_timestamp_seconds{job="etl_publisher"}` (the empty instance label disappears), and a POST push of success=0 kept the earlier last-success series.

`deploy/prometheus/alerts.yml`:
- Header (:1-5): `Every long-running service registers the whole etl_* catalogue, so rules about one service's metric select it by job (the job names of prometheus.yml). The batch jobs push etl_job_* and etl_publish_* / etl_retention_* gauges to the Pushgateway under job="etl_publisher", "etl_retention" and "etl_vacuum" (scraped with honor_labels).`
- `ETLDeadLetterRecords`: comment `… The publisher is a batch job: its last successful run's DLQ line count is a pushed gauge, which stays up until the next successful run pushes 0.`; expr gains a third branch:

```yaml
          or
          label_replace(
            label_replace(max(etl_publish_dlq_lines{job="etl_publisher"}) > 0, "stage", "publish", "", ""),
            "error_class", "see the window's DLQ object", "", "")
```

- `ETLServiceDown` comment: `The publisher is a batch job (etl publish), not a service: ETLPublisherStale covers it.`
- `ETLPipelineNotProgressing` description: `"The exporter, the publisher (Cronicle job etl-publish), the broker or the transformers are stalled; check their logs, ETLServiceDown and ETLPublisherStale."` (keeps the 15m window and `for: 5m`: at 5m windows each 15 minutes hold at least 2 publishes, even with one window failing through all 3 retries).
- `ETLObjectChecksumMismatch` comment: `The exporter and the transformer count it; the publisher (a batch job) exits non-zero on a raw part mismatch instead, which ETLPublisherStale catches.` (expr unchanged).
- `ETLExportLagHigh`: unchanged (T3's threshold 300 and texts).
- New rule at the end of the group:

```yaml
      - alert: ETLPublisherStale
        # The publisher runs every ETL_WINDOW (5m) and pushes its last success
        # time. 15 minutes without one is three windows in a row. The second
        # branch covers a stack where no publish ever succeeded; it waits until
        # Prometheus has been up for 15 minutes (startup grace; `for` would also
        # delay the first branch). Pushgateway persistence keeps the series
        # across restarts.
        expr: |
          time() - max(etl_job_last_success_timestamp_seconds{job="etl_publisher"}) > 900
          or
          (
            absent(etl_job_last_success_timestamp_seconds{job="etl_publisher"})
            and on() (time() - max(process_start_time_seconds{job="prometheus"}) > 900)
          )
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No successful etl publish in over 15 minutes"
          description: "Check the etl-publish job in Cronicle (red runs, their logs) and logs/publisher/etl.log; a missing raw _SUCCESS means the exporter is behind."
```

`deploy/grafana/dashboards/etl.json` (hand-edit, compact formatting; copy `datasource`, `fieldConfig` and `options` blocks from the existing panels):
- Panel 5 description: `"Throughput of the continuous hops: CoinGecko calls stored and processed records by outcome. The publisher runs once per window; see Published per window."`
- Panels 3 (`DLQ records (24h)`) and 10 (`Dead-letter records`) count `etl_dlq_total`, which only the exporter and the transformers emit after T4. Keep the queries and say so in the descriptions. Panel 3: `"Records the exporter and the transformers sent to the dead-letter zone in the last 24 hours; anything above 0 needs a human. Lines the publish job sends to dlq/stage=publish are in Published per window."` Panel 10: `"Records written to the dlq zone by stage and error class (export and transform stages; the publish job's DLQ lines are in Published per window)."`
- Panel 7 description: `"From fetched_at to the processed object: includes the wait for the window to close (up to ETL_WINDOW) and the one-minute publish grace."`
- Panel 15, stat `Batch jobs: since last success`, gridPos `{"x": 0, "y": 52, "w": 8, "h": 8}`, expr `time() - max by (job) (etl_job_last_success_timestamp_seconds)`, legend `{{job}}`, unit `s`, decimals 0, `colorMode: background`, `graphMode: none`, `textMode: value_and_name`, `orientation: horizontal`; an override `byName` `etl_publisher` with thresholds green / orange 600 / red 900; description `"Seconds since each Cronicle job (etl publish, retention, vacuum) last pushed a success to the Pushgateway. ETLPublisherStale fires above 15 minutes, ETLMaintenanceStale above 26 hours."`
- Panel 16, timeseries `Published per window`, gridPos `{"x": 8, "y": 52, "w": 16, "h": 8}`, `lineInterpolation: stepAfter`, targets `max(etl_publish_records{job="etl_publisher"})` (`published records`) and `max(etl_publish_dlq_lines{job="etl_publisher"})` (`dlq lines`); description `"Records the last successful etl publish run produced to the broker and the lines it sent to dlq/stage=publish (pushed gauges: the value holds until the next successful run)."`
- Panel 17, timeseries `Batch job runs`, gridPos `{"x": 0, "y": 60, "w": 12, "h": 8}`, `stepAfter`, targets `max by (job) (etl_job_duration_seconds)` (`{{job}} duration (s)`) and `max by (job) (etl_job_success)` (`{{job}} success`); description `"Duration and result (1 = success, 0 = failure) of the last run of each Cronicle job."`
- Panel 18, timeseries `Retention`, gridPos `{"x": 12, "y": 60, "w": 12, "h": 8}`, `stepAfter`, targets `max(etl_retention_partitions_dropped{job="etl_retention"})` (`dedup claim partitions dropped`) and `max(etl_retention_batches_deleted{job="etl_retention"})` (`written consumer batches deleted`); description `"What the last etl maintenance retention run removed."`

`internal/obs/metrics.go:107-111`: `Buckets: []float64{5, 15, 30, 60, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800, 3600}` (latency now includes a whole window, so p95 must not be interpolated across 300-600 s).

`Makefile` (variables, `cronicle-test`, `clean` comment, `demo`, `demo-report`):

```make
CRONICLE_URL ?= http://localhost:3012
# Upper bound for make demo: with ETL_WINDOW=1m the first processed records
# appear about 3 minutes after the containers start.
DEMO_TIMEOUT ?= 300
NODE_IMAGE := node:24.21.0-trixie-slim

.PHONY: build test test-unit lint alerts-test cronicle-test up down clean demo demo-report

# Unit-test the Cronicle provisioning script (schedule from ETL_WINDOW), and check
# that compose hands one ETL_WINDOW to the exporter, the transformers and the scheduler.
cronicle-test:
	docker run --rm -v "$(CURDIR)/deploy/cronicle:/w:ro" -w /w $(NODE_IMAGE) node --test
	ETL_WINDOW=15m $(COMPOSE) config --format json \
	  | jq -e '[.services.exporter, .services.transformer, .services.cronicle] | all(.environment.ETL_WINDOW == "15m")' >/dev/null

# Wipes everything: the containers (also those of services this file no
# longer defines, such as the old publisher), the named volumes (Postgres,
# Redpanda, Prometheus, Grafana, Pushgateway, Cronicle), the lake and the logs.
# Object names carry only time, so the lake may also be kept when the stack is re-created.
clean:
	$(COMPOSE) down -v --remove-orphans && rm -rf data logs .s3meta

# One-minute windows, so a window is exported, published by Cronicle and
# processed within the wait. The variable reaches `up` (and compose) too.
demo: export ETL_WINDOW = 1m
demo: up
	@echo "Pipeline started with ETL_WINDOW=1m; waiting up to $(DEMO_TIMEOUT)s for processed records..."
	@for i in $$(seq $(DEMO_TIMEOUT)); do \
	  if find data/processed -type f -name '*.jsonl' 2>/dev/null | grep -q .; then break; fi; \
	  sleep 1; \
	done
	@sleep 60 # one more window, so the duplicate share has data
	@$(MAKE) --no-print-directory demo-report
```

`demo-report` (counts data objects `*.jsonl` only; raw windows are counted by `_SUCCESS`) gains, after the duplicate share:

```make
	@echo -n "raw windows closed (_SUCCESS): "; find data/raw -type f -name _SUCCESS 2>/dev/null | wc -l | tr -d ' '
	@set -o pipefail; echo -n "last successful publish (s ago): "; \
	curl -fsG "$(PROMETHEUS_URL)/api/v1/query" \
	  --data-urlencode 'query=time() - max(etl_job_last_success_timestamp_seconds{job="etl_publisher"})' \
	  | sed -nE 's/.*"value":\[[^,]*,"([^".]*)[^"]*"\].*/\1/p' | grep . || echo "n/a (no successful publish yet)"
```

and `@echo "Cronicle:         $(CRONICLE_URL)  (jobs etl-publish, etl-retention, etl-vacuum; login admin)"` after the Grafana line. The target-specific `demo: export ETL_WINDOW = 1m` was verified with GNU make 3.81 on macOS: the variable reaches the `up` prerequisite and compose; a later plain `make up` does not see it.

`.env.example`: `PUSHGATEWAY_PORT=9091` after `PROMETHEUS_PORT`, `CRONICLE_PORT=3012` after `GRAFANA_PORT`; the `ETL_WINDOW` comment from T1 gains `Cronicle runs etl publish one minute after every window end; make demo overrides it with 1m. It can be changed on a running stack (make up recreates every etl container and cronicle with the host user ids): windows switch at the next boundary of the new size, and one publish run, for the window that straddles the change, fails once and can be ignored.`; then:

```
# Cronicle (UI at http://localhost:$CRONICLE_PORT). Time zone of the daily
# maintenance jobs (retention 11:00, vacuum 11:30); etl publish always runs
# on the UTC window grid.
ETL_SCHEDULE_TZ=UTC
# Login admin / CRONICLE_ADMIN_PASSWORD, and the API key the container uses to
# create its events. Both are stored when the cronicle volume is first
# created; after changing them run make clean.
CRONICLE_ADMIN_PASSWORD=admin
CRONICLE_API_KEY=etl_local_dev_api_key_0001
# Optional job notification URL (Slack-compatible incoming web hook); empty = off.
CRONICLE_WEB_HOOK=
```

`.github/workflows/ci.yml`: in `deploy-config`, after `make alerts-test`:

```yaml
      - name: Cronicle provisioning unit tests and compose ETL_WINDOW check
        run: make cronicle-test
```

in `image`:

```yaml
      - name: etl image (default target)
        uses: docker/build-push-action@v7
        with:
          context: .
          push: false
          tags: coingecko-etl:ci
          cache-from: type=gha,scope=etl
          cache-to: type=gha,mode=max,scope=etl
      - name: cronicle image (Node, Cronicle, etl)
        uses: docker/build-push-action@v7
        with:
          context: .
          target: cronicle
          push: false
          tags: coingecko-etl-cronicle:ci
          cache-from: type=gha,scope=cronicle
          cache-to: type=gha,mode=max,scope=cronicle
```

(One scope per image: two exports to the default scope overwrite each other's cache index, so every run would rebuild one image cold.)

- [ ] **Step 4: Run the targeted tests**

```bash
make cronicle-test            # pass 5, fail 0; then the compose check exits 0
make alerts-test              # both promtool commands SUCCESS
go test -short -count=1 ./internal/obs/...
docker compose config -q && jq empty deploy/grafana/dashboards/etl.json
```

- [ ] **Step 5: Lint, images and the live stack**

```bash
make lint
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app golangci/golangci-lint:v2.14.0 golangci-lint run ./...
make test
docker build --target cronicle -t coingecko-etl-cronicle:check . && \
  docker run --rm --entrypoint sh coingecko-etl-cronicle:check -c 'etl help && ls -l /opt/cronicle/etl && readlink /opt/cronicle/data /opt/cronicle/logs'
docker build -t coingecko-etl:check . && docker image inspect coingecko-etl:check --format '{{json .Config.Entrypoint}}'   # ["/etl"]: the default target is still the runtime
make clean && make demo       # report: raw windows closed > 0, processed records > 0, last successful publish < 120 s ago
docker compose ps             # cronicle and pushgateway healthy; no publisher service
curl -s -H "X-API-Key: etl_local_dev_api_key_0001" 'http://localhost:3012/api/app/get_schedule/v1' \
  | jq -c '.rows[] | {id,timing,timezone,catch_up,max_children,retries,retry_delay,timeout,category}'
curl -s -H "X-API-Key: etl_local_dev_api_key_0001" 'http://localhost:3012/api/app/get_event_history/v1?id=etlpublish&limit=5' \
  | jq -c '.rows[] | {id,code,description,elapsed}'          # code 0 rows, the first run included (Review Focus 1)
curl -s http://localhost:9091/metrics | grep -E '^etl_job_|^etl_publish_'   # job="etl_publisher" series present
curl -s http://localhost:9090/api/v1/targets | jq -c '.data.activeTargets[] | {job: .labels.job, health}'   # fetcher, exporter, 2x transformer, pushgateway, prometheus up; no publisher
curl -s http://localhost:9090/api/v1/alerts | jq -c '.data.alerts[] | {name: .labels.alertname, state}'      # after 5 min: no ETLServiceDown, ETLPublisherStale or ETLExportLagHigh
docker compose restart cronicle && docker compose logs cronicle | grep 'provision: updated'   # idempotent re-provision, still healthy
docker compose stop cronicle   # wait 3 minutes (Monitor/until-loop, not a bare sleep), then:
docker compose start cronicle  # get_event_history + get_job_status show the missed JOB_NOW minutes, in order
# --no-deps: without it compose also recreates init and the exporter with ETL_WINDOW=7m, init fails its
# config check, and cronicle never starts, so 'provision: FAILED' would never be logged
# the host ids: a plain compose command would take 65532 from .env, and that uid cannot write the cronicle volume uid 501 created
ETL_UID=$(id -u) ETL_GID=$(id -g) ETL_WINDOW=7m docker compose up -d --no-deps cronicle; docker compose logs cronicle | grep 'provision: FAILED'
ETL_UID=$(id -u) ETL_GID=$(id -g) ETL_WINDOW=1m docker compose up -d --no-deps cronicle   # restore the demo value
```

Open http://localhost:3012 in a browser (login `admin`/`admin`) and check that the Schedule tab lists `etl-publish`, `etl-retention` and `etl-vacuum` (the UI was not browser-tested in the prototype). Leave the demo stack running: T8 captures `make demo-report` from it.

- [ ] **Step 6: Remove what became unused**

- `docker-compose.yml`: `ETL_HTTP_ADDR` in the x-etl environment (:15).
- `Makefile`: `DEMO_WAIT` (:16) and the fixed sleep in `demo` (:60-61).
- `deploy/prometheus/alerts.yml`: the remaining publisher-as-a-service wording (the `ETLObjectChecksumMismatch` comment, the `ETLPipelineNotProgressing` description).

```bash
grep -rn 'publisher' docker-compose.yml deploy Makefile .github   # only etl_publisher, publisher-as-a-job wording, the transformer lag description
grep -rn 'etl_published_records_total\|DEMO_WAIT\|job_name: publisher' --exclude-dir=.git --exclude-dir=docs --exclude=README.md .; test $? -eq 1   # docs are T8
```

- [ ] **Step 7: Commit**

```bash
git add Dockerfile .dockerignore deploy/cronicle docker-compose.yml deploy/prometheus deploy/grafana/dashboards/etl.json \
  internal/obs/metrics.go internal/obs/metrics_test.go Makefile .env.example .github/workflows/ci.yml
git commit -m "feat(ops): schedule etl publish and maintenance with Cronicle, report to a Pushgateway" \
  -m "A cronicle image (Node LTS, Cronicle v0.9.135 from the release tarball, the etl binary) provisions etl-publish (every ETL_WINDOW at +1 min, catch-up, 3 retries) and the daily maintenance jobs at every start. Jobs push to a persisted Pushgateway scraped with honor_labels; ETLPublisherStale and a publish-DLQ branch alert on them; Grafana shows the batch jobs. make demo runs with ETL_WINDOW=1m." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** `docker compose` has no publisher service; `cronicle` and `pushgateway` are healthy after `make up`, and `cronicle` starts only after the exporter is healthy; `docker build .` still produces the distroless runtime. On a fresh volume the cronicle container sets itself up without interaction and creates `etlpublish`, `etlretention`, `etlvacuum`; every later start updates them in place (no duplicates); the healthcheck turns healthy only after provisioning. `etl-publish` timing equals `publishMinutes(ETL_WINDOW)` in UTC with `catch_up 1, max_children 1, retries 3, retry_delay 60, timeout 300` and script `exec etl publish --now "$JOB_NOW"`; retention 11:00 and vacuum 11:30 in `ETL_SCHEDULE_TZ`, category `maintenance` (max 1). An invalid `ETL_WINDOW` or `ETL_SCHEDULE_TZ` stops the container with `provision: FAILED: …`. Jobs run as `ETL_UID:ETL_GID` and write `logs/publisher/etl.log` and `logs/maintenance/etl.log` on the host. Cronicle storage and job logs persist across `ETL_UID=$(id -u) ETL_GID=$(id -g) docker compose up -d --no-deps --force-recreate cronicle` (same uid as the first start); Pushgateway metrics persist across restarts; `make clean` removes both volumes. Prometheus scrapes the pushgateway with `honor_labels`; `make alerts-test` and `make cronicle-test` pass locally and in CI (including the cronicle image build). `make demo` runs with `ETL_WINDOW=1m`, waits for processed output (at most `DEMO_TIMEOUT` + 60 s) and reports raw windows, processed records, duplicate share and the last successful publish. The dashboard shows panels 15-18.

---

### Task 8: Docs — final documentation pass for the post-refactor design (TODO item 6 plus the item 5 doc fixes)

Makes `README.md`, `docs/adr/`, the design spec, `docs/architecture/index.html` and the Grafana panel descriptions describe the final design and nothing else: the SKIP LOCKED claim, `ETL_WINDOW` windows with `_SUCCESS`, the windowed `etl publish` job in Cronicle, the Pushgateway, `idempotency_key` as the message key, processing-time object names, no "reset together" rule, the maintenance jobs. Every entry must pass TODO item 6's first-time-reader filter (keep an entry only if it says what the system does / how to run it / where data lives, why a key decision was taken and its cost, what guarantee holds and when, or which assignment requirement it covers). Links, anchors and section numbers must work, and leftovers from the previous revision must be gone.

Ground rules:
- Run after T1–T7 are committed. Line numbers below are as of `b883ac0`; re-locate every anchor with `grep -n`, because earlier tasks touched `alerts.yml`, `etl.json`, the Makefile and `.env.example`.
- Describe the code as it is. Check every name against the final code first (§2): env vars, ports, services, volumes, metric names, alert names, make targets, CLI flags, test names, SQL. Where the code disagrees with this plan or the contracts, the code wins: document the code and list the disagreement in the report.
- Write no history: no "was changed", "previously", "now", "amended", task numbers or "reproduced on 18.6". Rejected options live only in ADR "Options considered" sections and in the README Key decisions "Alternatives rejected" column.
- Image and library versions appear in one place per document: the README Architecture "Infrastructure:" line, the spec §2.2 table, the architecture page's startup diagram. Image tags inside shell commands (the docker run section) stay.
- Do not renumber existing spec sections: code and config cite §3.2, §5.2, §5.3, §6.1, §6.4, §7.3, §7.4, §8.2 and §8.4 (`grep -rhoE '§ ?[0-9]+(\.[0-9]+)?' internal cmd deploy migrations .env.example | sort -u`). New material goes inside existing sections or into appended subsections (§4.5, §9.3).
- Do not edit `TODO-refactor.md`. Never `git add -A`. Do not publish the architecture artifact: the main session republishes `docs/architecture/index.html` afterwards (memory rule: the page and its artifact stay in sync).
- `docs/superpowers/` is NOT renamed or moved (user decision, TODO-refactor.md:189, 2026-10-12; the user's memory notes say the same). Add one line to the README "Repository layout" block — `docs/superpowers/  design spec (current) and implementation plans (historical build logs) from the AI-assisted workflow` — and below the code block: `docs/superpowers/ is the development record: the spec is the current design document; the plans are the step-by-step build logs the AI agents executed, kept as written.` Leave `docs/superpowers/plans/fixtures/` alone.

**Files:**
- Modify: `README.md` (448 lines) — section by section (§3).
- Rename + rewrite: `git mv docs/adr/0005-exporter-xid8-cursor.md docs/adr/0005-exporter-skip-locked-claim.md` (§4.5).
- Modify: `docs/adr/0001-object-storage-versitygw.md` (L19, L25), `0002-single-dedup-in-transformer.md` (L10, L11, L25, L31), `0003-redpanda.md` (L8, L14, L25, new consequence), `0004-postgres-pipeline-state.md` (L10, L11, L21, L25, L31, L32), `0006-dlq-bucket.md` (L17, L23).
- Create: `docs/adr/0007-windowed-publisher-cronicle.md` (§4.7), `docs/adr/0008-processing-time-object-names.md` (§4.8).
- Modify: `docs/superpowers/specs/2026-10-10-coingecko-etl-design.md` (503 lines; §5).
- Modify: `docs/superpowers/plans/2026-10-10-coingecko-etl.md` — a 2-line note after line 1 linking the spec and ADRs 0005/0007/0008; the body untouched.
- Modify: `docs/architecture/index.html` (1888 lines; §6).
- Modify: `deploy/grafana/dashboards/etl.json` — descriptions only (§7).
- Scratch only (not in the repo): `/private/tmp/claude-501/-Users-ruslan-code-trust-wallet-test/627e19a9-e998-4647-b498-c775b2d81667/scratchpad/qa/browser/t4b_interact.js` (expected counts and titles, §6.13) and a new `.../scratchpad/plan/linkcheck.py` (V3).

**Interfaces:**
- Consumes (final code of T1–T7): the DDL in `migrations/00001_raw.sql` and `00002_pipeline.sql` (`export_batch_id`, partial indexes, `export_windows`, ledger columns, the `first_seen_at` audit comment); the exporter's window/claim code, its lag definition and the bootstrap window; the `_SUCCESS` writer and format; `etl publish --now` (exit codes, publish DLQ name, Pushgateway push); `etl maintenance retention|vacuum` and their metrics; `ETL_CLAIMS_RETENTION_DAYS` and its guard; the Cronicle image and provisioning (`deploy/cronicle/*`); `queue.MessageKey`; the transformer's `objectKey`; `alerts.yml` rule names and count; `prometheus.yml`; compose services and volumes; `.env.example`; the Makefile `demo`/`clean`; test names.
- Produces: `docs/adr/0005-exporter-skip-locked-claim.md`, `0007-windowed-publisher-cronicle.md`, `0008-processing-time-object-names.md`; README anchors `#scheduled-jobs-publishing-and-maintenance` and `#the-raw-window-contract`; `docs/architecture/index.html` with node ids `cronicle` and `pushgateway` (16 nodes, 16 chips, 17 decision rows) for the main session to republish; the updated scratch harness; a final report (intentional stale-term hits, code/config leftovers not fixed, the independence audit, fresh-reader items not applied, code-vs-contract disagreements).

- [ ] **Step 1: Write the failing checks**

The tests of a docs task are the checks V1–V9 below; write the link checker now and run every check once to record the failures it must fix:
- V0 preconditions: `git status --short && git log --oneline -12` — branch `refactor/pipeline-windows`, T1–T7 commits present, no tracked changes.
- V1 stale terms (docs):

```bash
grep -rnE 'xid8|ingest_txid|pg_snapshot_xmin|watermark_xmin|publish_lease|published_at|lease batch|leases exported|batch=0|coingecko\.raw\+|export\+batch|publish\+batch|event_date=|(^|[^_a-z])date=20|ErrLakeFromOtherLifetime|lake check|reset .*together|only safe reset|coins_markets or the coin id|key = coins_markets|per-key ordering|five keys|5 keys|etl_published_records_total|etl_publisher_|0005-exporter-xid8-cursor|kprom|Task [0-9]|amended|reproduced|4\.3\.1|6th call|sixth call|same record in another snapshot|oldest unexported raw row' \
  README.md docs/adr docs/architecture/index.html docs/superpowers/specs deploy/grafana
```

  Every remaining hit must be an intentional alternative (ADR 0005 options, ADR 0008 options, the README alternatives cell); list them in the report. (`ETL_EXPORT_INTERVAL` is NOT stale: the exporter still ticks every `ETL_EXPORT_INTERVAL`.)
- V1b the same grep over `internal cmd migrations deploy Makefile .env.example docker-compose.yml Dockerfile`: expect none; fix comment-only hits yourself (then `make lint` and `make test-unit`), report any non-comment hit instead of fixing it; an `alerts.yml` annotation is edited only together with `alerts_test.yml` and `make alerts-test`.
- V2 spec section references: `for s in $(grep -rhoE '§ ?[0-9]+(\.[0-9]+)?' internal cmd deploy migrations .env.example | tr -d '§ ' | sort -u); do grep -qE "^#{2,3} ${s}[. ]" docs/superpowers/specs/2026-10-10-coingecko-etl-design.md && echo ok $s || echo MISSING $s; done` → only `ok` lines.
- V3 links: write `.../scratchpad/plan/linkcheck.py`: for each md file it collects headings, slugs them with `re.sub(r'[^\w\- ]','',h.strip().lower()).replace(' ','-')`, checks every `](target#anchor)` (relative path exists; anchor among the target's slugs), checks that the README `## Contents` list equals the H2 headings after it, and checks that every `docs/adr/*.md` is linked from the README. Run `python3 -I .../scratchpad/plan/linkcheck.py README.md docs/adr/*.md docs/superpowers/specs/2026-10-10-coingecko-etl-design.md` → 0 problems.
- V4 Grafana: `python3 -m json.tool deploy/grafana/dashboards/etl.json >/dev/null && git diff -U0 deploy/grafana/dashboards/etl.json | grep '^[-+] ' | grep -v description` → no output.
- V5 HTML build: `cd .../scratchpad/qa && python3 -I wrap.py browser/skel.html /Users/ruslan/code/trust-wallet-test/docs/architecture/index.html browser/test.html` (`wrap.py` is in `scratchpad/qa/`, not in `qa/browser`).
- V6 HTML checks: `cd .../scratchpad/qa/browser && node t1_load.js && node t8_widths.js test.html && node t4b_interact.js test.html desktop && node t4b_interact.js test.html mobile` → no FAIL lines, empty `pageerrors` and `failed`, no console errors; open the `out/` screenshots of `#pipe`, `#pg-svg` and `#ops-svg` (desktop-light and desktop-dark) with the Read tool and confirm no text overlaps.
- V9 (optional) Mermaid: render the README mermaid block in a scratch page loading `https://cdn.jsdelivr.net/npm/mermaid@11.4.0/dist/mermaid.min.js` with puppeteer (`lib.js`): an SVG, no "Syntax error".

- [ ] **Step 2: Harvest the facts from the final code (scratch notes only)**

```bash
cat migrations/00001_raw.sql migrations/00002_pipeline.sql
grep -rn 'SKIP LOCKED' internal/exporter && sed -n '1,40p' internal/exporter/exporter.go   # window loop, claim, lag, bootstrap window
go run ./cmd/etl help && go run ./cmd/etl publish -h
ls deploy deploy/cronicle && sed -n '1,80p' deploy/cronicle/provision.js
docker compose config --services && docker compose config --volumes && cat .env.example
grep -n 'alert:' deploy/prometheus/alerts.yml            # count and names (ETLPublisherStale, ETLMaintenanceStale, ETLMaintenanceNeverSucceeded)
grep -rn 'Name: *"etl_' internal/obs/*.go                # final metric names (etl_published_records_total and kprom etl_publisher_* are gone)
grep -n 'demo\|ETL_WINDOW\|DEMO_TIMEOUT' Makefile
go list ./internal/...
grep -rhn '^func Test' --include=*_test.go internal cmd | sed 's/(.*//'
grep -n 'fallbackKey\|responseID' internal/transform/*.go
sed -n '338,381p' internal/transformer/batch.go          # planBatch: TX1 begins, fixRange (2a), then input checks (1) inside TX1
```

- [ ] **Step 3: Rewrite `README.md`**

Target outline; `## Contents` must equal the H2 list: 1 Architecture · 2 Why the Kafka protocol, and why Redpanda · 3 Quick start · 4 Scheduled jobs: publishing and maintenance (NEW) · 5 Browsing the lake · 6 Data model and time model · 7 Schemas and validation · 8 Delivery guarantees · 9 Metrics · 10 Alerts · 11 Logs · 12 Running the image with `docker run -v` · 13 Assumptions · 14 Productionization · 15 Repository layout and tests.

Line-by-line inventory (KEEP / EDIT / CUT / MOVE):
- L3 EDIT the chain: `…exports successful calls in 5-minute windows (JSON Lines plus a `_SUCCESS` manifest) to an S3-compatible lake, publishes each closed window to a Kafka-protocol broker (Redpanda) with a scheduled job, and writes…`; keep the exactly-once sentence and append `under the conditions in Delivery guarantees`.
- L5 KEEP. L7 KEEP (the only CoinGecko attribution). L446-448 CUT the duplicate attribution.
- L11-24 EDIT Contents to the outline.
- L28-42 EDIT the mermaid diagram: fetcher → PG unchanged; `PG -->|"claim rows (FOR UPDATE SKIP LOCKED)<br/>+ pipeline.export_batches"| E[etl exporter]`; `E -->|"window parts, then _SUCCESS<br/>PUT If-None-Match: *"| RAW`; `S[Cronicle] -. "etl publish --now, every 5 min (+1 min)" .-> P["etl publish<br/>(job)"]`; `RAW -->|"parts listed in _SUCCESS"| P`; `P -->|"key = idempotency_key"| K`; `P -. push .-> PGW[Pushgateway]`; optional `S -. "maintenance retention / vacuum, daily" .-> M[etl maintenance] -. "DROP old claim partitions, VACUUM" .-> PG`; delete `PG -. "lease batch" .- P` (L35). Check that it renders (V9).
- L44-53 EDIT into two tables. Long-running services: init, fetcher, exporter (`Closes each ETL_WINDOW window: claims the window's unexported rows, writes the parts and then `_SUCCESS``; replicas `1 (Postgres advisory lock)`), transformer, healthcheck. Jobs run by Cronicle: `etl publish --now <time>` (`publishes one closed window from S3; no Postgres; exit code ≠ 0 when `_SUCCESS` is missing`), `etl maintenance retention`, `etl maintenance vacuum`. Delete the publisher service row (L51).
- L55 EDIT: the single version line; add Cronicle (pinned upstream version, our image) and Pushgateway; drop Redpanda Console only if it is no longer in compose (check).
- L57 KEEP as one line. L59 KEEP.
- L61-76 Key decisions table. Replace L69 with the "Exporter claim" row from the TODO appendix, adding to its alternatives cell `a separate insert-only `pipeline.export_claims` table (keeps raw append-only, but adds a second table to join for the exporter's own bookkeeping)`. Add rows:
  - `Raw windows | `ETL_WINDOW` (5 min) windows aligned to the UTC hour, `_SUCCESS` written last; ready = `_SUCCESS` exists ([ADR 0007]) | one object per export batch (no completeness signal); listing the prefix (cannot tell late from empty)`.
  - `Publisher | a batch job per closed window, scheduled by Cronicle (Airflow/MWAA in production), reads only S3 ([ADR 0007]) | a long-running publisher with leases in Postgres; plain cron (no catch-up, history or retries); Airflow locally (heavy)`.
  - `Message key | the event's `idempotency_key`: responses spread over all 6 partitions, copies of one response share a partition ([ADR 0003]) | endpoint or coin id (5 keys on 4 partitions, half the traffic on one)`.
  - `Object names | window and processing time, no Postgres or Kafka counters ([ADR 0008]) | batch ids and offsets (keys unique only within one database and topic lifetime); event_date directories`.
  - `Maintenance | daily daytime Cronicle jobs: drop expired claim partitions, VACUUM ([ADR 0007]) | pg_cron (custom Postgres image); night runs`.
  - `File format | JSON Lines | Parquet (production compaction)`.
  Shorten every "Alternatives rejected" cell to short phrases. L76 KEEP.
- L78-82 EDIT "Why the Kafka protocol": replace "per-key ordering (every repeat of the same endpoint or coin lands on one partition...)" with `keyed partitions (every copy of one API response carries the same idempotency_key and lands on one partition, so one transformer judges the copies in offset order; other overlaps are settled by the Postgres claims)`; add `The publisher runs in batches, but the transformer is a consumer group: offsets make its batches replayable and crash-safe, replicas share and take over partitions, and the topic is a 7-day replay buffer independent of the publisher's schedule.`; add one honest clause: per-coin message order is not kept, and nothing depends on it.
- L84-114 Quick start: L89-92 EDIT the make lines (`make demo` uses `ETL_WINDOW=1m` and waits up to `DEMO_TIMEOUT`; `make clean` wipes containers, volumes, the lake and logs). L95-108 EDIT: paste a REAL `make demo-report` output (Step 7) including the Cronicle line. L110 KEEP. L112 EDIT: add `ETL_WINDOW`, `ETL_CLAIMS_RETENTION_DAYS`, `ETL_SCHEDULE_TZ`, the Cronicle and Pushgateway ports (keep `ETL_EXPORT_INTERVAL`). L114 CUT the reset-together paragraph and its `ErrLakeFromOtherLifetime` sentence; replace with: `Object names carry the window or the batch's processing time, not database or broker counters, so the lake (`./data`, `./.s3meta`) may be kept when the stack is recreated. The new stack continues after the last window the old one closed: the exporter never overwrites an existing _SUCCESS, and the new scheduler only runs publish times (window end + 1 min) that come after it started, so no window is published twice.` (Checked against Cronicle v0.9.135: a new event's cursor is its creation minute and the scheduler runs only later minutes.) Add `ETL_WINDOW can be changed on a running stack: windows switch at the next boundary of the new size, and one publish run (the window straddling the change) fails once and can be ignored.`
- NEW section 4 "Scheduled jobs: publishing and maintenance":
  - (a) Cronicle UI URL (from `.env.example`) and the login (`admin` / `CRONICLE_ADMIN_PASSWORD`); one sentence: Cronicle is the local stand-in for Airflow, upstream is in maintenance mode (bug fixes only, the author's successor project is xyOps), and our image is built from the pinned upstream release.
  - (b) Events table (name | schedule | what it does | retry policy): `etl-publish` every `ETL_WINDOW` at +1 min (for 5 min: minutes 1,6,…,56), Run All catch-up, 1 concurrent, 3 retries 60 s apart, timeout 5 min, command `etl publish --now "$JOB_NOW"`; `etl-retention` daily 11:00; `etl-vacuum` daily 11:30, one maintenance job at a time. The maintenance timezone comes from `ETL_SCHEDULE_TZ` (default UTC); `etl-publish` always follows the UTC window grid. Why daytime: the pipeline load is flat, and a failure is seen by people at work.
  - (c) Which window a run publishes: end = floor(now − 1 min, ETL_WINDOW). `JOB_NOW` is the scheduled time, so catch-up and reruns pick the right window. A missing `_SUCCESS` means the exporter is behind or down: the run fails, is retried, then turns red. A red run is not re-run automatically after its retries (nothing alerts if later windows succeed): re-run it as below.
  - (d) How to rerun: one window in the UI with "Run Again" on the job, or `curl -s -X POST -H "X-API-Key: $CRONICLE_API_KEY" -H 'Content-Type: application/json' -d '{"id":"etlpublish","now":<window end epoch + 60>}' http://localhost:3012/api/app/run_event/v1`. It respects the one-job-at-a-time limit, so `logs/publisher/etl.log` keeps one writer: while a run is in progress Cronicle refuses it with `Maximum of 1 job already running`; retry after that run. `docker compose exec cronicle etl publish --now 2026-10-12T10:06:00Z` publishes 10:00–10:05 but bypasses that limit. A range: the event's Time Machine with Run All on. Verify the binary path, service name and both UI labels against the pinned Cronicle version. Reruns are safe (copies are dropped as `replay`), but only within `ETL_CLAIMS_RETENTION_DAYS`; older reruns write duplicates. After `docker kill`/SIGKILL of cronicle the in-flight window is red and must be re-run by hand; a graceful stop re-runs it automatically. Credentials are stored when the cronicle volume is created: change them with `make clean`.
  - (e) Pushgateway: URL; the four `etl_job_*` gauges with grouping key `job`; POST "add" semantics keep the last success after a failed run; persisted volume; the alerts `ETLPublisherStale`, `ETLMaintenanceStale`, `ETLMaintenanceNeverSucceeded`.
  - (f) Maintenance: retention drops `pipeline.dedup_claims` daily partitions older than `ETL_CLAIMS_RETENTION_DAYS` (default 8; config refuses less than the 7-day topic retention + 1 day, because partitions follow event_date while replays follow publish time) and deletes `written` consumer_batches older than that, never `planned` ones; production would keep 30 days. Vacuum runs `VACUUM (ANALYZE)` on `raw.api_calls` (dead versions from the claim UPDATE), `pipeline.export_batches` and `pipeline.consumer_batches`. Logs go to `logs/publisher/etl.log` and `logs/maintenance/etl.log`.
- L116-144 Browsing the lake: L118 EDIT to one clause about metadata in `./.s3meta`. L120-125 EDIT the example paths to real ones from `./data` (or illustrative, labelled): `data/raw/source=coingecko/ingest_date=2026-10-12/window=1000/part-00000.jsonl`, `data/raw/source=coingecko/ingest_date=2026-10-12/window=1000/_SUCCESS`, `data/processed/dataset=ticker/v=1/processed_date=2026-10-12/hour=10/part-20261012T100612Z-3f9a1c2e.jsonl`, `data/rejected/dataset=coin_contract/processed_date=…/hour=10/part-….jsonl`, `data/dlq/stage=transform/processed_date=…/hour=10/part-….jsonl`, `data/dlq/stage=publish/ingest_date=…/window=1000/part-00000.jsonl`. L127 REPLACE: raw and the export/publish DLQ are named by window; processed, rejected and the transform DLQ by the batch's planned_at hour plus a short hash of topic id, partition and first offset, so a retried batch writes the same key; an hour directory can still receive a batch written after a crash (a batch planned at 10:59:59 and written later lands in hour=10); link the window contract (§6) and ADR 0008. L129-144 KEEP; L137 header list unchanged, mention the message key.
- L146-202 Data model: L150-153 KEEP. L155 EDIT: cut the "200 too large for one broker message" clause if not essential; keep limits, Retry-After, backoff, skipped ticks. L159 EDIT: cut the json-vs-jsonb detail, keep `the body is stored byte-exact, so payload_sha256 can be re-verified at every hop`; add `The only column the pipeline updates is export_batch_id, set once when the exporter claims the row.` L161 REPLACE with NEW subsection `### The raw window contract`: `ETL_WINDOW` values (whole minutes, 1–60, dividing 60); hour alignment; the key format; ingest_date and HHMM are the UTC window start; a window holds the rows the exporter claimed when it closed it (`fetched_at` < window end, not yet exported), so a row committed late goes to the next window and nothing is lost; missed windows are closed in order after downtime; on a fresh stack the window before the current one is closed at once (usually empty) so the first publish run finds it; the window is ready when `_SUCCESS` exists — read exactly the parts it lists and check their sha256; an empty window still gets `_SUCCESS` with `"rows":0,"parts":[]`; show a real `_SUCCESS` example (compact JSON, key order window_start, window_end, rows, parts[{key, sha256, rows}]). L163-173 KEEP; L173: `_processed_at` = planned_at, which also names the object. L175-180 EDIT L178: cut "decoded with json.Number and a decimal type". L182-191 KEEP. L193-202 EDIT L197: `event time; its UTC date, event_date, is part of the dedup key and a column of rejected rows and dedup claims (in processed rows it is the date of snapshot_at); the lake is partitioned by processing time, so queries by event time scan more files`; EDIT L202: cut the 10:00:45 anecdote, keep "clocks of different systems disagree, so no logic compares them".
- L204-225 KEEP; L225 compress to about 3 sentences.
- L227-266 Delivery guarantees: L229 KEEP. L231-236 EDIT the hop rows: exporter `rows claimed with FOR UPDATE SKIP LOCKED and stamped with the batch id in the ledger transaction; keys fixed at claim; If-None-Match: *; `_SUCCESS` last`; publisher renamed `s3://raw → broker (etl publish)`: `window from the scheduler's time; parts and sha256 from `_SUCCESS`; idempotent producer, acks=all` | `a rerun produces copies with the same event_id, dropped as replay`; fetcher and transformer rows KEEP. L238 REPLACE with the "Why a SKIP LOCKED claim" paragraph from the TODO appendix, shortened to 6 sentences or fewer, keeping the cost sentence and the ADR 0005 link (the TOAST claim was measured in the prototype and may stay); add one sentence: the claim is per window (`fetched_at` < window end). L240-248 KEEP. L250 EDIT to match the code (TODO item 5): `(1) A short transaction first records the range in pipeline.consumer_batches (`planned`, with planned_at, the batch's only clock), then runs the input checks without querying the database, transforms, validates against S2 (a failure goes to dlq/ without claiming its key), claims the keys, decides, and stores object keys and checksums...`; renumber the remaining steps. L252-262 KEEP. L264 REWRITE the invariants: transforms are pure; planned_at names the batch's objects; claims and `written` manifests are kept `ETL_CLAIMS_RETENTION_DAYS` (default 8, never less than the topic's 7 days + 1) — redeliveries, topic replays and window reruns within that period are deduplicated, a raw republish older than that may write duplicates; Postgres session and transaction timeouts stop a stuck transaction from holding a connection and row locks; delete "nothing in this stack deletes them". L266 KEEP.
- L268-295 Metrics: L270 EDIT: long-running services only; /readyz deps: Postgres for all, S3 for exporter and transformer, broker for transformer; Prometheus also scrapes the Pushgateway with honor_labels. L272-291 EDIT and compress the label lists: L274 `calls by HTTP status, network_error or skipped (not an error)`; L281 `seconds since the end of the oldest window the exporter has not closed yet (0 on schedule); grows while S3 fails, a claimed part is stuck, Postgres is unreachable, or the exporter is behind`; L284 delete `etl_published_records_total` and add the job metrics row(s): `etl_job_last_success_timestamp_seconds`, `etl_job_last_run_timestamp_seconds`, `etl_job_duration_seconds`, `etl_job_success` (gauges, label job; pushed at the end of every Cronicle job), `etl_publish_records`, `etl_publish_dlq_lines` (last successful publish run), `etl_retention_partitions_dropped`, `etl_retention_batches_deleted`; L286 `duplicate_record` = `the same content from another response` (TODO item 5; `internal/dedup/dedup.go:61`); add any new exporter metric found in Step 2. L293 EDIT: drop the kprom `etl_publisher_*` sentence. L295 EDIT to the final dashboard panels (15-18 included).
- L297-315 Alerts: L301 EDIT the count from `alerts.yml`. Rows: L308 likely cause `S3 failures, a claimed part that keeps failing, Postgres unreachable, exporter down` (rows locked by another session are no longer a cause); L310 long-running services only, the publisher is a job; L311 causes include `the publish job failing`; L312 replace the reset cause with `a non-deterministic transform, a foreign writer, or a clock that went backwards`; add rows `ETLPublisherStale` (no successful publish in 15 min; or none at all after 15 min of Prometheus uptime), `ETLMaintenanceStale` (26 h), `ETLMaintenanceNeverSucceeded` (absent 26 h), and the publish branch of `ETLDeadLetterRecords`. Note that the shipped thresholds assume `ETL_WINDOW` ≤ 5m. L315 KEEP.
- L317-319 Logs: add `Cronicle jobs append to logs/publisher/etl.log and logs/maintenance/etl.log, one run at a time.`
- L321-366 docker run: L350-353 EDIT: the loop covers fetcher, exporter and transformer; add a one-shot `docker run --rm ... coingecko-etl:local publish --now "$(date -u +%FT%TZ)"` with the note `Cronicle runs this every window in compose`; the Pushgateway is optional (a failed push is only logged). L356-364 EDIT: rows for the Cronicle data volume and the Pushgateway volume with their compose names. L366 EDIT: cut "Reset all of it together", keep the cleanup commands, remove `publisher` from the `docker rm` list.
- L368-380 Assumptions: KEEP; L379 add the publisher and maintenance job logs; L374 stays true (lake objects are write-once); do not claim the raw table is append-only.
- L382-411 Productionization: AWS table — add a "Cronicle" row: Amazon MWAA (Airflow) with `schedule='1-59/5 * * * *'`, `catchup=True`, `max_active_runs=1`, an `S3KeySensor` on the window's `_SUCCESS`, `retries=3`, `retry_delay=60s`, `execution_timeout=5min`; maintenance as a daily DAG; pg_partman on RDS for partitions. Add a Pushgateway row (Airflow task metrics, or Pushgateway to Amazon Managed Prometheus). L391 EDIT `compose services`. Scalability: L398 REWRITE (keys already spread over all partitions; one markets response is still one message of 100 coins, so splitting it into per-coin messages gives finer parallelism; then more partitions and transformer replicas, up to one per partition); L400 EDIT `without polling or per-row updates`; add bullets: exporter scale-out, TODO item 1's variant (b) (drop the advisory lock; give each claimed part an `export_lease_until` and let any instance resume a part whose lease expired, with SKIP LOCKED on the ledger; keep a part's export deadline shorter than the lease; store `content_sha256` with a compare-and-set; one log file per replica with `ETL_LOG_PER_HOST`; claims of one window still serialize on its row and only one window is open at a time (`export_windows_one_pending`), so the parallelism comes from building and PUTting the claimed parts, since the claim itself is short; publication order then no longer follows fetch order); per-partition autovacuum for the queue-like unexported-rows index; strict window order as an option (a `_published` marker, the `depends_on_past` analogue), which would also surface a red window that later windows hide. Reliability L408: retention exists locally for claims; pg_partman for raw.api_calls and claims in production, claims kept 30 days; retention of raw.api_calls and the export ledger (raw first, then ledger rows, because of the FK) is not implemented. Note the Kafka segment caveat: records can outlive `retention.ms` by up to `segment.ms`, so consumer-group resets and replays belong within the claims window.
- L413-444 Repository layout and tests: update the package list to `go list` output (add `internal/window`, `internal/maintenance`, `deploy/cronicle`); add the `docs/superpowers` line; L442 EDIT the integration-test sentence to tests that exist (cite only what Step 2's grep finds): late commit goes to the next window, concurrent claims serialize per window, `_SUCCESS` last, empty windows, missed windows closed in order, the bootstrap window, keys independent of the local time zone, publisher missing `_SUCCESS` exits non-zero, rerun idempotent, SIGTERM still reports, retention keeps planned, overlapping key sets without deadlock. L444 KEEP.

- [ ] **Step 4: ADRs**

§4.1 `0001`: L19 `Run versitygw with the posix backend`; keep `--sidecar` as one clause: `metadata in a sidecar directory, ./.s3meta, because Docker Desktop bind mounts lack extended attributes`; L25 shorten the HeadBucket explanation to one sentence.

§4.2 `0002`: L10 `publisher reruns, consumer rebalances, crashes before an offset commit`. L11 REWRITE (TODO item 5): `True duplicates: CoinGecko's CDN caches responses for 60 s and we poll every 30 s, so about half of the markets responses are byte-identical; the same record can also arrive unchanged in another response. A stale coin repeated in two consecutive snapshots is not a duplicate: snapshot_at is part of the key, so it yields two rows, both flagged is_stale.` L25 `The fetcher's idempotency_key is the Kafka message key (ADR 0003) and labels the drop-reason metric; it is not a dedup input.` L31 REWRITE: claims are partitioned by event_date; `etl maintenance retention` drops partitions older than `ETL_CLAIMS_RETENTION_DAYS` (default 8, at least the topic retention + 1 day, because partitions follow event_date while replays follow publish time); production keeps 30 days to cover a raw republish after a schema fix; window reruns and republishes are safe only within that period. Add to the Decision: records without an id are keyed by their response's idempotency_key (event_id as fallback) plus their position, tickers prefixed with the coin id, so keys from different coins and responses never collide and copies of one response still dedup. Document `first_seen_at` as an audit-only column.

§4.3 `0003`: L8 `durable, replayable log with keyed partitions and consumer groups: replay after a schema fix, copies of one response judged in order by one consumer, replicas that share and take over partitions`. L14 drop `v26.2.4`, the `--smp` details (keep "dev-container mode") and the kprom client-metrics mention; replace the last sentence: `Messages are keyed by the event's idempotency_key, the sha256 of source, endpoint, parameters and payload hash. Responses spread over all partitions, and every copy of one response (a CDN repeat, a publisher rerun) lands on the same partition. The partitioner is franz-go's default murmur2, as in the Java client.` L25 DELETE the harness sentence, keep `integration tests run against Redpanda`. ADD consequence: `Message order per coin is not kept; nothing depends on it: records are independent, snapshot_at is in every key, and stale and anomaly flags are computed within one response. Claims are inserted in sorted key order, so two transformers with overlapping key sets cannot deadlock.`

§4.4 `0004`: L10 `which raw.api_calls rows belong to which export batch and window, and in which object (exporter)`. L11 DELETE (the publisher keeps no state). L21 `pipeline.export_windows and pipeline.export_batches: the exporter ledger (window, part index, row and line counts, fetched_at bounds, object key, checksum, status); claimed rows point to it through raw.api_calls.export_batch_id` (take the column list from the final 00002). L25 keep the timeouts as general hygiene (`so a stuck session cannot hold a connection and row locks`). L31 `The exporter claims rows with FOR UPDATE SKIP LOCKED and stamps them in the same transaction as the ledger row; alternatives in [ADR 0005](0005-exporter-skip-locked-claim.md)`. L32 `dedup_claims partitions and written consumer_batches rows are removed by the daily retention job (ADR 0007); pg_partman in production`. L33 KEEP.

§4.5 `0005` REWRITE after `git mv docs/adr/0005-exporter-xid8-cursor.md docs/adr/0005-exporter-skip-locked-claim.md`. Title `ADR 0005: Exporter claims rows with FOR UPDATE SKIP LOCKED`; Status accepted, Date 2026-10-10.
- Context: keep L8's first three sentences and L10; add that the exporter writes `ETL_WINDOW` windows (ADR 0007), so a claim also selects by `fetched_at` < window end.
- Options considered: (1) identity watermark: loses rows on out-of-order commits; (2) transaction-id (xid8) cursor below `pg_snapshot_xmin`: no row updates and raw stays append-only, but it relies on Postgres internals, has to be explained, and stalls behind any long transaction; (3) an `exported` flag set after the PUT: a crash between PUT and flag lets a retry pick a different row set (stamping the batch id in the claim transaction fixes this); (4) a separate insert-only `pipeline.export_claims` table: keeps raw append-only, but `export_batch_id` is the exporter's own synchronization column, kept on the rows it describes; the fetcher never reads or writes it, and a partial index keeps the pick cheap (this answers "why does the reader write to the writer's table"); (5) Debezium CDC: the production option, a heavier JVM Connect stack.
- Decision: one short transaction, no S3 I/O inside: `SELECT … FROM raw.api_calls WHERE outcome = 'ok' AND export_batch_id IS NULL AND fetched_at < $window_end ORDER BY seq LIMIT 500 FOR UPDATE SKIP LOCKED` with the predicates as literals; `nextval` only if rows were found; INSERT the ledger row (window, part index, object key, row count, fetched_at bounds); UPDATE the rows by primary key with the redundant fetched_at range for partition pruning. Then read the part by `export_batch_id` in (seq, event_id, fetched_at) order, validate S1, PUT with If-None-Match: * (on 412 compare the stored sha256), mark exported; when the window has no more rows, seal its manifest and write `_SUCCESS`. One exporter at a time under `pg_try_advisory_lock`; claimed parts are finished before new claims. Copy the exact SQL shape from the final code.
- Consequences: nothing is skipped and nothing waits for a long transaction; a retry rewrites the same bytes; cost: one non-HOT UPDATE per successful row (export_batch_id is in an index predicate): a dead tuple and new index entries; the ~95 KB payload stays in TOAST and is not copied; WAL about 0.4 MB per 500-row batch, under 1% of writing the rows; `raw.api_calls` is no longer strictly append-only (the lake stays write-once); a long transaction no longer stalls export but delays vacuum of claim versions (bounded by `transaction_timeout`); export lag is the time past the end of the oldest window not yet closed, so a stuck part keeps its window open and the lag grows, while rows locked by another session simply go to a later window; part order no longer follows seq across windows (a late commit lands in a later window; nothing downstream depends on it); claims of one window serialize on its `pipeline.export_windows` row (`FOR UPDATE`), so no part is added after the manifest is sealed. With one exporter under the advisory lock this costs nothing, and SKIP LOCKED still skips rows other sessions hold. This departs from the prototype's "parallel claims never block each other", which held before windows existed; at volume: per-partition autovacuum, partition retention (a claim locks every partition); scale-out recipe in one sentence (drop the lock, add an export lease); production: land events in a log or object storage directly, or Debezium.

§4.6 `0006`: L17 — transformer DLQ objects `dlq/stage=transform/processed_date=<d>/hour=<HH>/part-<planned_at>-<id8>.jsonl`, part of the batch manifest; exporter `dlq/stage=export/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl` (index of the raw part whose lines failed); publisher `dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl`, one deterministic object per window. L23 `Recovery is a replay: after a fix, the window is rerun (Cronicle or etl publish --now) and reprocessed, within the claims retention; a row that went to dlq/ never claimed its key, so the republished copy is written.`

§4.7 NEW `0007-windowed-publisher-cronicle.md`, `ADR 0007: Raw windows, a scheduled publisher job and Cronicle`, Status accepted, Date 2026-10-12.
- Context: stages should meet only through the artifacts they produce (raw.api_calls, raw windows, topic messages, processed objects); a long-running publisher leasing batches from the export ledger coupled publisher and exporter through Postgres; a reader of the raw zone needs to know when a time range is complete; batch work should look like an Airflow schedule (a failed run is visible, retried and caught up); Postgres maintenance (claim retention, VACUUM) needs the same.
- Options considered: keep the long-running publisher with leases; a publisher listing S3 prefixes (no completeness signal); schedulers: Airflow locally (scheduler, webserver, metadata DB: heavy for a laptop), cron, supercronic or Ofelia in a container (no catch-up, run history or UI), pg_cron for maintenance (custom Postgres image, shared_preload_libraries, test images), Cronicle (one Node process, web UI, Run All catch-up, retries, timeouts, run history, an API to provision events as code; no official image, so we build one from the pinned upstream release; upstream is in maintenance mode and the author's successor is xyOps).
- Decision: the window contract as in Global Constraints (ETL_WINDOW, hour alignment, closing a window with SKIP LOCKED, late rows to the next window, missed windows in order, parts, `_SUCCESS` last with its JSON format, empty windows, the bootstrap window on a fresh ledger); `etl publish --now` (end = floor(now − 1 min, ETL_WINDOW); missing `_SUCCESS` → non-zero exit; parts and sha256 from `_SUCCESS`; a DLQ object per window; no Postgres); Cronicle events (etl-publish every window at +1 min, Run All, max 1 concurrent, 3 retries 60 s apart, timeout 5 min; etl-retention 11:00 and etl-vacuum 11:30 in `ETL_SCHEDULE_TZ`, daytime on purpose; provisioned at startup, idempotently); Pushgateway (grouping key job, POST add, last_success only on success, persisted; alerts "publish older than 15 min" and "maintenance older than 26 h").
- Consequences: fetch-to-processed latency grows from about 20 s to about 6 min; `make demo` uses `ETL_WINDOW=1m`; reruns are safe within claims retention; window order is not enforced, and a window that stays red after its retries is not re-run automatically (optional `_published` marker); why a broker remains between two batch steps (offsets, consumer group, replay buffer); the publisher drops out of Prometheus scraping and `ETLServiceDown`; the alert thresholds assume `ETL_WINDOW` ≤ 5m; production mapping to MWAA with the DAG parameters from the README.

§4.8 NEW `0008-processing-time-object-names.md`, `ADR 0008: Object names from windows and processing time`, Status accepted, Date 2026-10-12.
- Context: names built from batch ids and Kafka offsets are unique only within one Postgres and Redpanda lifetime, which forced a "reset Postgres, Redpanda and the lake together" rule and an `etl init` guard.
- Options: keep counters plus the reset rule; random UUID names (a retry would write a second object, breaking write-once idempotency); event_date directories (a batch spans several event dates; late and re-planned batches write into old partitions); window or processing-time names.
- Decision: raw and export/publish DLQ use the window path; processed, rejected and transform DLQ use processed_date and hour of planned_at with `part-<planned_at YYYYMMDDTHHMMSSZ>-<id8>`, id8 = the first 8 hex characters of sha256(`"<topic_id>|<partition>|<first_offset>"`); a re-planned batch keeps its manifest planned_at, so it writes the same key; a recreated topic has a new topic_id and a new time, so there are no collisions; event_date stays a column and part of the dedup key.
- Consequences: the lake no longer depends on the database or broker lifetime and may be kept across a stack re-creation (the exporter never overwrites an earlier stack's `_SUCCESS`, and a fresh scheduler only publishes windows that end after it started, so nothing is published twice); safety nets stay (write-once PUT plus `ETLObjectChecksumMismatch`, which fires only if the clock goes backwards); queries by event time scan more files (production compaction to Parquet partitions by event_date); an hour of processed data can still receive files after a crash (a planned batch finishes later) — no hour-completeness marker yet; the optional `_SUCCESS` per hour would be written once no planned batch has planned_at in that hour; the format stays JSON Lines (arrays, null vs empty string, nested fields in rejected and dlq, self-describing lines under schema evolution).

- [ ] **Step 5: Spec, plan note, Grafana descriptions**

Spec `docs/superpowers/specs/2026-10-10-coingecko-etl-design.md` (keeps DDL and SQL as the implementation reference, but current, without history, without renumbering):
- L3-4: add `- **Revised:** 2026-10-12 (windowed export and publish, SKIP LOCKED claim, processing-time object names, maintenance jobs)`.
- L15 exporter row → windows with `_SUCCESS`; L18 logs (+ publisher and maintenance); L19 health (`every long-running service; jobs push to Pushgateway`). Drop kprom from the tech table (L83) and the metrics row (L428). L28 the chain includes the scheduled publisher job. L40-59 ASCII diagram: same flow as the README mermaid. L65-72 services plus jobs, as in the README. L74-85: add Cronicle and Pushgateway rows; L79 cut "The franz-go code was verified unchanged against this version"; L85 shorten to `MinIO and rclone rejected: ADR 0001`. L97 per-key ordering → keyed partitions (copies of a response share one).
- L106-125 decision table: L113 idempotency_key = message key and metric label; L124 → Exporter claim row; ADD rows: Raw windows; Publisher job (no Postgres); Scheduler (Cronicle locally, MWAA in production); Message key; Object names; Maintenance and claims retention (8 days default, config guard); File format JSONL; Stage independence (`stages meet only through raw.api_calls, raw windows with `_SUCCESS`, topic messages and processed objects; export_batch_id is the exporter's own column`).
- L139 cut "observed on the 6th call within 27 s". L151-173 replace the DDL with the final `00001` DDL (`export_batch_id`, two partial indexes, no `ingest_txid`); L175-180 add `export_batch_id is the only column ever updated, once, by the claim` and check the partition-maintenance sentence.
- L182-190 §4.2 REWRITE: single instance (advisory lock); the window close loop (from the final code); claim SQL and order (pick with `fetched_at < window end`, nextval only if rows, ledger INSERT then stamp by PK with the fetched_at range); read by export_batch_id; parts and DLQ names; seal, then `_SUCCESS` last, its format, the empty window; the bootstrap window on an empty ledger (and the lake check); lag = seconds past the end of the oldest window not yet closed (0 on schedule); no transaction across I/O; only ok rows.
- L192-197 §4.3 REWRITE: publisher job (window formula, `_SUCCESS` required, parts and sha256, S1 per line, DLQ per window, key idempotency_key, headers, idempotent producer, exit codes, Pushgateway push, no Postgres); keep the topic sentence.
- L241 `Event time; its date (event_date) is a dedup-key column and the partition key of pipeline.dedup_claims; lake partitions use processing time (§6.5)`. L246 cut the anecdote. L271 cut "(null in 223 of 250 rows)".
- L279-288 §6.5 REWRITE with every path from the contracts, the `_SUCCESS` format, and the id8 and T rules.
- L294-301 §7.1 table as in the README. L305-356 §7.2: paste the final `00002` DDL (`dedup_claims.first_seen_at` audit only). L358 cut "(reproduced: …)". L372 `duplicate_record` wording; one line on id-less keys.
- §7.4: L378-380 the real order, keeping the labels "1" and "2a" because code cites them: `TX1 begins and fixes the range first (2a: planned_at is the batch's only clock); the input checks (1) then run inside TX1 without database queries`. L384 2e: objects per (zone, dataset), keys from planned_at and id8.
- L406-408 §7.6 REWRITE for idempotency_key keys, 6 partitions, 2 transformers with 3 partitions each, headroom to 6. L410-414 §7.7 invariants as in the README.
- §8.1 long-running services only. §8.2 the publisher row → Pushgateway job metrics. §8.3 add job logs. §8.4 add the three new alerts, drop the publisher from "service down", update the export-lag causes. §8.5 final panels.
- §9.1 compose: infra adds pushgateway and cronicle; services fetcher, exporter, transformer×2; volumes; demo with `ETL_WINDOW=1m`; the container count from `docker compose config --services`. §9.2: add the Cronicle image (Node LTS, pinned Cronicle, etl binary). APPEND §9.3 "Scheduler": events as code, provisioning at startup, logs, rerun.
- §10 layout from `go list`. §11 L493: the exporter, publisher and maintenance tests that exist. §12 README outline mirrors the new README.

Plan note: insert after line 1 of `docs/superpowers/plans/2026-10-10-coingecko-etl.md`: `> Historical build log of the first version. The current design is the spec (docs/superpowers/specs/2026-10-10-coingecko-etl-design.md) and ADRs 0005, 0007 and 0008; the refactor is docs/superpowers/plans/2026-10-12-pipeline-refactor.md.` (two lines at most).

Grafana `deploy/grafana/dashboards/etl.json` (descriptions only; do not change queries): L73 (panel 5) matches its final queries; L117 `duplicate_record = the same content from another response`; L184 (panel 13) keeps T3's wording (check it matches the final lag definition); check the descriptions of panels 15-18.

- [ ] **Step 6: Architecture page `docs/architecture/index.html`**

- §6.1 L452 lede: `…keeps every call in Postgres, exports it in 5-minute windows to an S3 lake, publishes each window to Redpanda on a schedule, and writes each cleaned record to processed exactly once.` L454 `3 services, 3 jobs` (fetcher, exporter, transformer; publish, retention, vacuum). L457 recount containers; drop RAM unless measured with `docker stats --no-stream` on the running stack.
- §6.2 Pipeline SVG L508-672: update the `#pipe` aria-label (L508); L542 `xid8 cursor` → `SKIP LOCKED`; L543-545 move the exporter→b-raw edge to `M970 100 H1150 V496` (and `#flow-export` L526 to match), widen b-raw (L662-663) to x=830 w=330, move the `PUT / If-None-Match: *` labels to x≈1144 text-anchor end, y≈420/435, label the edge `parts, then _SUCCESS`; L558-559 delete the publisher→state `lease` edge; add `<g class="node" data-id="cronicle">` at about x=990 y=290 w=140 h=80 (title `Cronicle`, subs `publish every 5 min` / `maintenance daily`) and a dashed edge `M990 330 H974` to the publisher labelled `runs`; L637-638 publisher subs `etl publish · job` / `1 window → messages`; L548-549 label `produce · key = idempotency_key`; L666 b-raw sub `5-min windows · _SUCCESS last`; split the obs bar (L668-671): obs x=30 w≈830 plus a new node `data-id="pushgateway"` x≈870 w≈300 with text `Pushgateway · job metrics`; rewrite the obs text to fit (jobs push to Pushgateway). Check in the browser that no label overlaps.
- §6.3 Walkthrough STEPS (L1472-1513): keep 8 steps with the same titles (t4b checks `Step 2 of 8: Store`, `5. Plan`, `8. Write`), 30 words or fewer per paragraph. Export (L1483-1487): `When window 10:00–10:05 closes, the exporter claims its 4 rows (SKIP LOCKED), writes part-00000, then _SUCCESS.`; artifact: PUT `raw/…/ingest_date=2026-10-10/window=1000/part-00000.jsonl`, then PUT `…/window=1000/_SUCCESS` with the compact JSON (rows 4, one part). Publish (L1488-1492): `At 10:06 Cronicle runs etl publish for 10:00–10:05. A and B share an idempotency_key, so they land on one partition; C and the bitcoin call go elsewhere.` (illustrative offsets, e.g. p3: 1200 A, 1201 B; p5: 830 C). Plan/Dedup/Write follow the p3 batch for A and B and mention C written from its own partition. Write: key `processed/dataset=market_snapshot/v=1/processed_date=2026-10-10/hour=10/part-20261010T100612Z-<8 hex>.jsonl`. The "About this data" note (L686): `Ids, offsets and partitions are illustrative.`
- §6.4 Time model: L727 `event time · key · event_date column`; add a 4th TIME scenario "Window to processed" with marks fetched_at 10:02:33, window closes 10:05:00, `_SUCCESS` about 10:05:02, Cronicle publish 10:06:00, planned_at 10:06:12 (names the file); take: `fetched_at picks the raw window, planned_at names the processed file; snapshot_at stays the event time.`
- §6.5 Guarantees: hops-svg aria (L910); L918 label `publish job`; L922 HOP 2 `claim (SKIP LOCKED) · ledger` plus a line `window, _SUCCESS last`; L923 HOP 3 `window from JOB_NOW · _SUCCESS` / `idempotent producer, acks=all`. Partitions note L985 → `Each response has its own key, so all 6 partitions carry data; copies of one response share a key and a partition and are judged in offset order.`; PARTS (L1721-1728): each partition gets 1-2 short hash-key chips (e.g. p3 `9f3c… A + B (CDN copy)`). Lifecycle LC (L1736-1741): merge "Input checks" into the TX1 box; lines `a manifest row: planned`, `b input checks → dlq`, `c transform`, `d S2 check`, `e claims → decide`, `f objects + checksums`; re-space x so C1 still sits before TX1 commits; update the lc-svg aria-label (L998).
- §6.6 Schemas: only the publisher label `publish job` (L1027).
- §6.7 Storage tree (L1066-1108): raw `source=coingecko/ingest_date=2026-10-10/window=1000/` with `part-00000.jsonl` (`data-file="raw"`) and `_SUCCESS` (new `data-file="success"`); processed `dataset=market_snapshot/v=1/processed_date=2026-10-10/hour=10/part-20261010T100612Z-…jsonl`; rejected `dataset=coin_contract/processed_date=…/hour=10/part-…`; dlq `stage=transform/processed_date=…/hour=10/part-…`, `stage=export/` and `stage=publish/`: `ingest_date=…/window=HHMM/part-NNNNN.jsonl`. FILES (L1810-1813): raw title `raw · window 10:00, part 0`; add `FILES.success` showing the `_SUCCESS` JSON with the note `Written last; the publisher reads only the parts it lists.`
- §6.8 pg-svg (L1113-1129): update the aria-label; L1119 the exporter→raw.api_calls arrow becomes a write (class `ar`, m-mu) labelled `UPDATE … SKIP LOCKED` / `export_batch_id`; L1120 `claim part · window` / `mark exported, seal, _SUCCESS`; delete the publisher box and arrow (L1116, L1121) and put `etl maintenance` in that slot (or the bottom row); sub-labels L1124 `daily partitions · export_batch_id set once`, L1125 `export ledger: windows, parts, keys, sha256`, L1126 `kept 8 days (ETL_CLAIMS_RETENTION_DAYS)`, L1127 `written rows kept 8 days, planned kept`; L1128 bottom row `etl init creates every table and partition · etl maintenance (Cronicle, daily): drop expired claim partitions, delete old written manifests, VACUUM (ANALYZE)` (remove the lake check); L1134 "Persistence and reset": list the volumes from compose; `make clean wipes everything; object names carry no database or broker counters, so the lake may be kept.`
- §6.9 Operations: boot svg (L1145-1165) adds pushgateway to column 1, replaces `etl publisher` with `cronicle → etl publish, maintenance`, keeps the versions (the page's one place) and adds Cronicle's; update the aria-label. ops-svg (L1171-1201): the publisher column becomes `publish job · maintenance (Cronicle → Pushgateway)` with the new alert boxes and the job metrics; `ETLServiceDown` text `a long-running service…`; the rule count in the aria-label and figcaption (L1204) from `alerts.yml`; delete `etl_published_records_total`. Ports (L1206-1213): add the Cronicle UI and Pushgateway ports. Metric dl (L1216-1228): fix export_lag (time past the end of the oldest unclosed window); `duplicate_record` → `same content, another response`; add `etl_job_last_success_timestamp_seconds` and `etl_publish_records`.
- §6.10 Drawers C (L1271-1379): apicalls L1291 kicker `Postgres · schema raw`, L1293 replace the ingest_txid bullet with `export_batch_id: set once when the exporter claims the row, the only UPDATE this table gets`. exporter L1297-1305: role `Closes each 5-minute window: claims its rows, writes parts, then _SUCCESS.`; sections Claim (the `WHERE … FOR UPDATE SKIP LOCKED` predicate, one instance under the advisory lock), Windows (`fetched_at < end`, late rows go to the next window, missed windows closed in order, the bootstrap window on a fresh stack), Why a claim (raw in Postgres per the assignment, table as a work queue, cost of one UPDATE per row; production log or Debezium), Steps (claim → JSONL + S1 → PUT parts → mark exported → seal → `_SUCCESS`), Timeouts (keep). state L1310 `export_windows / export_batches: export ledger: windows, parts, keys, sha256`, chips `claims kept 8 days`. publisher L1315-1322: kicker `etl publish · Cronicle job every 5 min`; role `Publishes one closed window; one raw line becomes one message.`; flow: window formula; `_SUCCESS` required (missing → exit 1, 3 retries); parts and sha256; S1 per line → one DLQ object per window; key idempotency_key; headers; acks=all; push to Pushgateway; Rerun: `same event_ids; the transformer drops the copies as replay.`; chips `etl_job_last_success_timestamp_seconds`, `etl_publish_records`. redpanda L1324 kicker without version; role `Replayable log with consumer groups; copies of one response share a key and a partition.`; L1327 keep. transformer L1334 order `1. TX1: plan the range (planned_at), input checks, transform, S2, claims, decide.`; L1337 planned_at also names the objects. init L1344 delete step 4 (lake check). obs L1351 without versions: `Prometheus with N rules (scrapes Pushgateway too), Grafana with a provisioned dashboard.` b-raw L1355-1358 role and keys per window, add a section `_SUCCESS`. b-processed L1364 `processed_date/hour/part-<planned_at>-<id8>`; `a retried batch lands on the same object; event_date is a column`. b-dlq L1376 naming per stage. NEW cronicle drawer: Events, Rerun (Run Again / Time Machine / etl publish --now; within claims retention), Why Cronicle (one process, UI, catch-up, events as code; upstream maintenance mode; production MWAA). NEW pushgateway drawer: Metrics, Push semantics, Alerts. COMP_ORDER (L1380): insert `cronicle` before `publisher`, append `pushgateway`.
- §6.11 Decisions DEC (L1825-1837) → 17 rows: Storage (Object storage, Object names, File format); Data (Event, Cleaning, Polling cadence, Numbers); Delivery (Deduplication, Pipeline state, Exporter claim — replaces Exporter cursor, alt adds `separate export_claims table` —, Raw windows, Message key); Platform (Broker — why `replayable log, consumer groups, copies of a response on one partition` —, Schemas); Operations (Logs, Publisher job — Cronicle; alt long-running publisher with leases, cron, Airflow locally —, Maintenance — alt pg_cron, night runs). Texts as in the README Key decisions; AREAS unchanged.
- §6.12 Footer L1245: `Design details: the spec in docs/superpowers/specs/ and the ADRs in docs/adr/. Updated with every architecture change.`
- §6.13 QA harness (scratch) `t4b_interact.js`: `TITLES += {cronicle:'Cronicle', pushgateway:'Pushgateway'}`; `pipeline has 16 nodes` (`=== 16`); `16 component chips`; decisions `d.rows === 17`. If you choose other node or decision counts, set them to the real numbers and say so.

- [ ] **Step 7: Capture the demo report, run every check, sweep leftovers**

- V10 demo output: if `docker compose ps` shows the T7 demo stack running, do NOT run `make clean`: wait until Cronicle shows a green `etl-publish` run, run `make demo-report` and paste its output into the README Quick start. If the stack is not running, run `make demo` and paste its report. If neither works, keep the old sample block, mark it as an open item in the report, and invent no numbers. Use a real `_SUCCESS` and a real processed key from `./data` for the README examples if available.
- Re-run V1–V6 (and V9) until clean.
- V7 Go sanity: `make lint && make test-unit` (relevant if Go comments were touched); `make alerts-test` if an `alerts.yml` annotation was edited.
- V8 stage-independence audit (TODO item 6, read-only, report only):

```bash
for p in exporter publisher transformer fetcher maintenance; do echo "== $p"; go list -f '{{join .Imports "\n"}}' ./internal/$p | grep coingecko-etl; done
grep -rn 'pipeline\.\|raw\.api_calls' internal/{exporter,publisher,transformer,fetcher,maintenance} --include=*.go | grep -v _test
```

  Expected: no stage imports another stage's package; the publisher imports no `storage/pg` and touches no table; the transformer only `consumer_batches` and `dedup_claims`; the exporter only `raw.api_calls`, `export_windows` and `export_batches`; maintenance only `dedup_claims`, `consumer_batches` and the vacuumed tables; `etl publish` needs no `ETL_PG_DSN`. List violations in the report; do not fix code.

- [ ] **Step 8: Fresh reader**

After all edits and before committing, spawn one fresh agent (Opus, no context) if the harness allows it, with this prompt: `You are a data engineer reviewing a take-home repository for the first time. Read /Users/ruslan/code/trust-wallet-test/README.md from top to bottom, and only the files it links when you need them. Do not read code. List (1) every sentence or section that is unclear, assumes knowledge not given earlier, or contradicts another place; (2) every entry that does not answer what the system does, how to run it, where data lives, why a key decision was taken and its cost, what guarantee holds and when, or which assignment requirement it covers; (3) anything a reviewer would look for but cannot find (how to run, where data is, how to rerun a window, what each metric and alert means). Give line numbers and a one-line fix for each. Max 40 items. Change no files.` Apply every fix that passes the item-6 filter and is consistent with the code, then re-run V1–V6. In the report, list each item not applied with the reason. If you cannot spawn an agent, say so: the main session runs the reader and its fixes become a follow-up commit `docs: fresh-reader fixes`.

- [ ] **Step 9: Commit**

```bash
git add README.md docs/adr docs/superpowers/specs/2026-10-10-coingecko-etl-design.md \
  docs/superpowers/plans/2026-10-10-coingecko-etl.md docs/architecture/index.html deploy/grafana/dashboards/etl.json
git add docs/superpowers/plans/2026-10-12-pipeline-refactor.md   # this plan: the build log of the refactor, which the plan note links (skip if the main session already committed it)
# plus any comment-fixed file, named explicitly
git status --short   # TODO-refactor.md, the PDF, .DS_Store, docs/.DS_Store, .impeccable/ still untracked; git shows the ADR 0005 rename
git commit -m "docs: describe windowed export and publish, SKIP LOCKED claim and processing-time names" \
  -m "README, ADRs, spec and architecture page describe the final design: raw windows with _SUCCESS, the SKIP LOCKED claim (ADR 0005 renamed and rewritten), the Cronicle-scheduled publisher and maintenance jobs with the Pushgateway (new ADR 0007), idempotency_key message keys and object names from windows and processing time (new ADR 0008)." \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

**Acceptance:** the README follows the 15-section outline, Contents matches the headings, and it documents the window contract, the Cronicle UI and how to rerun one window and a range (with the claims-retention limit), the Pushgateway, both maintenance jobs, `ETL_WINDOW` and the claims retention, with names and values matching the final code. No document describes the xid8 cursor, publisher leases, batch- or offset-based object names, event_date directories, the reset-together rule, `ErrLakeFromOtherLifetime` or coins_markets/coin-id keys as current behaviour (only as considered alternatives). `docs/adr/0005-exporter-skip-locked-claim.md` exists (git shows a rename), lists the xid8 cursor and a separate export_claims table among the options and names the costs honestly; ADRs 0007 and 0008 exist with Context, Options considered, Decision and Consequences; 0002 L11, 0003 keying, 0004 ledger and links, and 0006 DLQ names are fixed. The TODO item 5 doc mismatches are closed (ADR 0002 stale coin; README and Grafana `duplicate_record`; README and spec §7.4 transaction order; `first_seen_at` and the id-less key rule documented). Spec sections cited by code still exist; the spec DDL equals the final migrations; the spec decision table has every new decision. The plan body is unchanged except the note. The architecture page shows Cronicle and the Pushgateway, windows with `_SUCCESS`, the idempotency_key key, processing-time names, maintenance, 17 decision rows and the new footer, and every puppeteer check passes. Grafana: only descriptions changed. `docs/superpowers/` is not renamed. Exactly one commit; untracked user files untouched; nothing pushed.

---

### Task 9: Final sweep — nothing unused from the previous revision, CI-equivalent checks, live run left running

The user asked that nothing unused from the previous revision remains, that the result is committed, brought up and left running for the morning review. This task finds and removes leftovers with machine checks (golangci-lint `unused`, x/tools `deadcode`, greps for old names), runs every check CI runs plus the Review Focus extras, commits the sweep (only if it changed files), then does a clean live run with default settings, verifies every stage end to end and leaves the stack running.

**Files:**
- Modify (known at `b883ac0`, found by `go run golang.org/x/tools/cmd/deadcode@v0.51.0 ./cmd/etl`): `internal/coingecko/limiter.go:37-43` — delete `Limiter.Wait` (only tests call it) and reword the `Limiter` doc comment, whose last sentence (:16-18) says "so Wait absorbs spacing by sleeping": the blocking call is now `acquire`. `internal/coingecko/limiter_test.go` — replace `l.Wait(ctx)` with a test helper `wait(l, ctx)` that calls the production path `l.acquire(ctx)` + `l.release()`; `internal/dedup/dedup.go:45-56` — delete `Decision.String` (never printed by production code; test failures then print the integer).
- Modify: whatever else the sweep finds (comment-only history such as `internal/transformer/batch.go:127,143` "Task 3" and `internal/transform/helpers_test.go:14` "Task 4", if T5/T8 did not already fix them).
- Keep on purpose (not dead code): columns that only the code writes and humans read, namely `dedup_claims.first_seen_at` (audit COMMENT from T1), `export_batches.exported_at`, and `export_windows.opened_at`/`closed_at` (`closed_at` is part of a CHECK); and `internal/testutil` (test infrastructure in non-test files).
- No new files.

**Interfaces:**
- Consumes: the whole branch after T1–T8.
- Produces: a clean tree (only the untracked user files remain), at most one commit `chore: remove code left unused by the refactor`, and the stack running with default settings (`ETL_WINDOW=5m`) for the morning.

- [ ] **Step 1: Static sweep (write the failing checks first)**

```bash
cd /Users/ruslan/code/trust-wallet-test
git status --short                      # only: .DS_Store, .impeccable/, TODO-refactor.md, docs/.DS_Store, the task PDF
go run golang.org/x/tools/cmd/deadcode@v0.51.0 ./cmd/etl     # baseline at b883ac0: Limiter.Wait, Decision.String
go run golang.org/x/tools/cmd/deadcode@v0.51.0 -test ./...  # functions unused even by tests
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app \
  golangci/golangci-lint:v2.14.0 golangci-lint run ./...  # standard set incl. unused (golangci's old "deadcode" linter no longer exists in v2)
```

Every function `deadcode ./cmd/etl` reports must be removed, or be test-only infrastructure that lives in a non-test file on purpose (`internal/testutil`) — justify each kept one in the commit message. Old-name greps:

```bash
# code and config: must print nothing
# data/logs/.s3meta/bin hold runtime output (raw CoinGecko text, a stale binary), not code
grep -rnE 'ingest_txid|from_txid|to_txid|from_seq|to_seq|watermark_xmin|pg_snapshot_xmin|xid8|lastBatchEnd|readRange|publish_lease_until|published_at|export_batches_unpublished_idx|PublishLease|PublishPoll|ETL_PUBLISH_LEASE|ETL_PUBLISH_POLL|leaseSQL|markSQL|markPublished|PublishOnce|PublishedRecords|etl_published_records_total|kprom|etl_publisher_[a-z]|ErrLakeFromOtherLifetime|HasAnyObject|s3_lake_check|postgres_export_ledger|ledgerEmpty|rawObjectKey|dlqObjectKey|ObjectKey\(|batch=%020d|export\+batch|publish\+batch|batch=0000|/event_date=|DEMO_WAIT|job_name: publisher|"test-key"|Task [0-9]+|\(spec [0-9]' \
  --exclude-dir=.git --exclude-dir=docs --exclude-dir=data --exclude-dir=logs --exclude-dir=.s3meta --exclude-dir=bin \
  --exclude=README.md --exclude=TODO-refactor.md --exclude='*.pdf' .
# ('/event_date=' matches the old key paths only; test messages like "ticker %s: event_date=%s" in internal/transform stay)
# coins_markets / coin id used as a Kafka key (the endpoint name, schema ids and metric labels stay); must print nothing:
grep -rnE 'Key: *\[\]byte\("|keys := \[\]string\{"coins_markets"|Production keys are "coins_markets"|partitioning key: "coins_markets"|MessageKey.*Request\.Path' internal cmd; test $? -eq 1
# docs: every hit must be an intentional "considered alternative" (ADR 0005, ADR 0008, README alternatives column)
grep -rnE 'xid8|ingest_txid|publish_lease|published_at|ErrLakeFromOtherLifetime|per-key ordering|coins_markets or the coin id|etl_published_records_total|reset .*together' README.md docs/adr docs/architecture/index.html docs/superpowers/specs
# config surface: no key that config no longer reads, no service that no longer exists
for k in $(grep -oE '^[A-Z0-9_]+' .env.example); do grep -qw "$k" internal/config/config.go deploy/cronicle/*.js docker-compose.yml Makefile 2>/dev/null || echo "unused .env.example key: $k"; done   # digits too: S3_PORT, ETL_S3_*
docker compose config --services     # postgres redpanda versitygw redpanda-console pushgateway prometheus grafana init fetcher exporter transformer cronicle; no publisher
go mod tidy && git diff --exit-code go.mod go.sum
```

(For the `.env.example` loop, keys consumed only by compose interpolation — ports, `ETL_UID`/`ETL_GID`, `GRAFANA_ADMIN_PASSWORD`, `COINGECKO_API_KEY` — show up in `docker-compose.yml`, `Makefile` or `config.go`; anything printed is a leftover.) Run each check once now and record what fails: that list is the work of Step 2.

- [ ] **Step 2: Remove every finding**

Known removals:

```go
// internal/coingecko/limiter_test.go — replaces the deleted Limiter.Wait
func wait(l *Limiter, ctx context.Context) error {
	if err := l.acquire(ctx); err != nil {
		return err
	}
	l.release()
	return nil
}
```

Replace every `l.Wait(ctx)` / `l.Wait(context.Background())` in `limiter_test.go` with `wait(l, ctx)` / `wait(l, context.Background())`, keep the test names and assertions (they now exercise the client's real path), delete `Limiter.Wait` and its doc comment from `limiter.go`, and delete `Decision.String` from `dedup.go` (drop the `fmt` import if it becomes unused). Fix every other finding from Step 1 the same way: delete dead code with its tests and comments, never silence a check. Re-run Step 1 until it is clean.

- [ ] **Step 3: Run every CI-equivalent check plus the Review Focus extras**

```bash
make lint                                                     # CI lint job: gofmt, go vet
docker run --rm -e GOFLAGS=-buildvcs=false -v "$PWD":/app -v "$(go env GOMODCACHE)":/go/pkg/mod -w /app \
  golangci/golangci-lint:v2.14.0 golangci-lint run ./...      # CI lint job: golangci-lint v2.14.0
make test                                                     # CI test job: go test -race -p 2 -count=1 -timeout 15m ./...
docker compose config -q                                      # CI deploy-config job
make alerts-test
make cronicle-test
jq empty deploy/grafana/dashboards/etl.json
docker build -t coingecko-etl:ci .                            # CI image job, default target
docker build --target cronicle -t coingecko-etl-cronicle:ci .
# Review Focus 2: the time-sensitive packages under a half-hour-offset local zone (exporter and transformer
# already force +05:30 in TestMain; TZ adds window, publisher and cmd/etl). A "DATA RACE" on time.Local means
# some test assigns time.Local outside TestMain: move it there.
TZ=Asia/Kolkata go test -race -count=1 -timeout 15m ./internal/window/ ./internal/exporter/ ./internal/publisher/ ./internal/transformer/ ./cmd/etl/
```

Expected: everything passes. A failure here is fixed in the task's code (and noted in the commit message), not by skipping the check.

- [ ] **Step 3b: Stage independence (user request, 2026-10-12: after the work, check again that the pipeline stages know nothing about each other except the artifacts they produce)**

The stages are fetcher, exporter, publisher (`etl publish`), transformer, plus the maintenance jobs. The artifacts between them are `raw.api_calls`, the raw windows with `_SUCCESS`, the topic messages and the processed, rejected and DLQ objects. Shared packages are allowed only when they define one artifact contract (`internal/window`, `internal/queue`, the schemas, the migrations) or are infrastructure without stage knowledge (`config`, `obs`, `storage/*`, `xtime`, `validation`).

```bash
# 1. Imports: no stage package depends, even transitively, on another stage's package.
for p in fetcher exporter publisher transformer maintenance; do echo "== $p"; go list -deps -f '{{.ImportPath}}' ./internal/$p | grep -E '^coingecko-etl/internal/(fetcher|exporter|publisher|transformer|maintenance)$' | grep -v "/$p\$"; done
# 2. Tables: the publisher touches none; the exporter only raw.api_calls, export_windows and export_batches;
#    the transformer only consumer_batches and dedup_claims; maintenance only claims, written manifests and VACUUM.
grep -rnoE '(raw|pipeline)\.[a-z_]+' internal/{fetcher,exporter,publisher,transformer,maintenance} --include=*.go | grep -v _test | sort -u
# 3. Config: each command loads and validates only its own settings
#    (etl publish runs without ETL_PG_DSN; the fetcher needs no S3 or broker settings; and so on).
# 4. Formats: every format crossing a stage boundary is defined once, in its contract package, and the reader
#    gets it from the artifact (the _SUCCESS manifest, message headers), never from the writer's internals or tables.
```

Fix every violation in code, with a test where behaviour changes, and re-run Step 3. If a fix is too large for the sweep, report it with a concrete proposal instead. The hand-over lists the result of all four checks.

- [ ] **Step 3c: Whole-branch review (main session)**

Before the commit, the main session runs a fresh whole-branch review of `main..HEAD` against `TODO-refactor.md` and this plan, and applies its findings in a separate fix wave. Re-run Steps 1, 3 and 3b afterwards.

- [ ] **Step 4: Commit the sweep (only if Steps 2-3 changed files)**

```bash
git status --short
git add internal/coingecko/limiter.go internal/coingecko/limiter_test.go internal/dedup/dedup.go   # plus every other file the sweep changed, named explicitly, and this plan if its checkboxes changed
git commit -m "chore: remove code left unused by the refactor" \
  -m "deadcode, golangci-lint and the old-name greps are clean: Limiter.Wait (tests now use the client's acquire/release path) and Decision.String were unreachable from the binary. <list any other removals and any function kept on purpose>" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 5: Live run with default settings, end to end**

```bash
diff <(grep -oE '^[A-Z0-9_]+=' .env | sort) <(grep -oE '^[A-Z0-9_]+=' .env.example | sort)   # digits too: S3_PORT, ETL_S3_*
# keys only in .env that no config reads any more: remove those lines by hand; never delete or regenerate .env (it may hold COINGECKO_API_KEY)
make clean
make up                                      # ETL_WINDOW defaults to 5m
docker compose ps                            # init exited 0; postgres, redpanda, versitygw, pushgateway, cronicle, fetcher, exporter, transformer x2 healthy
```

Wait with the Monitor tool or an until-loop (no bare `sleep` in this harness) until at least three windows are closed and two publish runs finished (about 12-15 minutes), e.g. `until [ "$(find data/raw -name _SUCCESS | wc -l)" -ge 3 ]; do sleep 20; done` inside Monitor. Then verify each item and note the result:

1. Windows and `_SUCCESS` (contract): `find data/raw -name _SUCCESS | sort` shows `ingest_date=<today UTC>/window=HHMM` with HHMM a multiple of 5, one per window, the first one being the bootstrap window just before the start (usually `"rows":0,"parts":[]`); `cat` the newest: compact JSON in key order `window_start, window_end, rows, parts`; for each listed part `shasum -a 256 data/raw/<key>` equals its `sha256` and `wc -l` equals its `rows`; `find data/raw -name '*.jsonl' | grep -v '/part-[0-9]\{5\}\.jsonl$'` prints nothing.
2. Cronicle jobs green (Review Focus 1): `curl -s -H "X-API-Key: etl_local_dev_api_key_0001" 'http://localhost:3012/api/app/get_event_history/v1?id=etlpublish&limit=10' | jq -c '.rows[] | {id,code,description,elapsed}'` — every run `code 0`, the first one included; `get_schedule` shows `etlpublish` minutes `[1,6,…,56]` in UTC, `etlretention` 11:00 and `etlvacuum` 11:30 in `ETL_SCHEDULE_TZ`.
3. Processed objects with the new names: `find data/processed data/rejected data/dlq -name '*.jsonl' | head` — processed keys match `^data/processed/dataset=[a-z_]+/v=1/processed_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/part-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}\.jsonl$` (check with `grep -Ev`); `find data -type d -name 'event_date=*' | wc -l` → 0; no `batch=` objects anywhere.
4. Dedup counts: `make demo-report` — processed records > 0, duplicate share roughly 0.4–0.6 (CDN 60 s vs 30 s polling); `curl -sG localhost:9090/api/v1/query --data-urlencode 'query=sum by (reason) (etl_dedup_dropped_total)'`; no duplicate business rows: `find data/processed/dataset=market_snapshot -name '*.jsonl' -exec cat {} + | jq -r '[.coin_id,.snapshot_at]|@tsv' | sort | uniq -d | wc -l` → 0.
5. Rerun of a published window (Review Focus 4). Pick a window whose `_SUCCESS` has `"rows"` > 0 and whose `etl-publish` run is green. The oldest one is the bootstrap window and usually empty, and rerunning it proves nothing. Note `sum(etl_dedup_dropped_total{reason="replay"})`, then `curl -s -X POST -H "X-API-Key: etl_local_dev_api_key_0001" -H 'Content-Type: application/json' -d '{"id":"etlpublish","now":<that window end epoch + 60>}' http://localhost:3012/api/app/run_event/v1`. Cronicle answers `Maximum of 1 job already running` while a scheduled run is in progress: retry after it. After the run is green and the transformers caught up, the replay counter grew. It counts records, not lines: each republished line drops all its records as `replay`, so the growth is many times the window's `rows`. The market_snapshot duplicate check of item 4 still prints 0.
6. Maintenance: `run_event` for `etlretention` (same curl with its Content-Type, id `etlretention`, no `now`). Poll `get_job_status` until it ends, then run `etlvacuum`: both are in the `maintenance` category, which runs one job at a time, so a second `run_event` while the first job runs is refused. Both are green; `curl -s localhost:9091/metrics | grep -E '^etl_job_last_success_timestamp_seconds|^etl_retention_'` shows `job="etl_retention"` and `job="etl_vacuum"`; `logs/maintenance/etl.log` has `job finished` for both; `logs/publisher/etl.log` has `window published` lines.
7. Prometheus targets: `curl -s localhost:9090/api/v1/targets | jq -c '.data.activeTargets[] | {job: .labels.job, health}'` — fetcher, exporter, transformer ×2, pushgateway, prometheus all `up`; no publisher.
8. Alerts: `curl -s localhost:9090/api/v1/alerts | jq -c '.data.alerts[] | {name: .labels.alertname, state}'` after at least 15 minutes — nothing firing; `ETLMaintenanceNeverSucceeded` is gone after item 6 (it is only pending before). If `ETLDataStale` or `ETLFrequentRateLimiting` fire, check `logs/fetcher/etl.log` for CoinGecko 429s (external; report, do not "fix").
9. Grafana: `curl -s -u admin:admin 'http://localhost:3000/api/search?query=CoinGecko' | jq -r '.[].title'` finds the dashboard; the PromQL of panels 13 and 15-18 return data when run against Prometheus (`/api/v1/query`).
10. Logs: `grep -h '"level":"ERROR"' logs/*/etl.log logs/*/*/etl.log | head -50` — read every ERROR; expected only CoinGecko-side errors. Anything else is a bug: stop, fix it in a follow-up commit with a test, and re-run Steps 3–5.

- [ ] **Step 6: Leave it running and hand over**

Leave the stack up (`docker compose ps` all healthy). If the Mac sleeps, Docker Desktop's VM is suspended: the morning would show a gap in the data and a burst of catch-up runs. So keep it awake for the night with `caffeinate -i` started in the background (it cannot prevent the sleep of a closed MacBook lid without an external display; say so in the hand-over). `git status --short` shows only the untracked user files; `git log --oneline main..HEAD` lists the T1–T9 commits; nothing is pushed or merged. The hand-over also tells the user to restart or change the stack with `make` (or after setting `ETL_UID`/`ETL_GID` in `.env` to `id -u`/`id -g`), not with a plain `docker compose up` (Open questions). Report back to the main session, in a few lines: the commits, the results of Step 5 items 1–10 (with numbers: windows closed, processed records, duplicate share, last successful publish age), any finding left open, and the URLs (Cronicle http://localhost:3012, Grafana http://localhost:3000, Prometheus http://localhost:9090/alerts, Pushgateway http://localhost:9091). The main session sends the morning summary to Telegram and republishes the architecture artifact from `docs/architecture/index.html`.

**Acceptance:** `deadcode ./cmd/etl` reports nothing except justified test infrastructure; golangci-lint, `make lint`, `make test`, `make alerts-test`, `make cronicle-test`, `docker compose config -q`, the dashboard JSON check, both image builds and the `TZ=Asia/Kolkata` run pass; the old-name greps print nothing in code and config and only intentional alternatives in docs. After `make clean && make up` with defaults, windows close every 5 minutes with valid `_SUCCESS` manifests, every `etl-publish` run (the first included) is green, processed objects carry the new names with no duplicate business rows, a manual rerun is dropped as `replay`, both maintenance jobs succeed and push, all Prometheus targets are up with no alert firing, Grafana shows the batch-job panels, and the stack is left running.
