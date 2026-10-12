# ADR 0008: Object names from windows and processing time

- Status: accepted
- Date: 2026-10-12

## Context

Every object in the lake is write-once (`If-None-Match: *`, [ADR 0001](0001-object-storage-versitygw.md)), and a retried write must reuse the same key. Keys built from export batch ids and Kafka offsets satisfy that, but they are unique only within one Postgres and Redpanda lifetime: batch ids restart at 1 and offsets at 0 when the database or the topic is recreated. Over a kept lake, a new batch would then reuse an old key with different bytes.

## Options considered

- **Counters, plus a rule to reset Postgres, Redpanda and the lake only together**, enforced by an `etl init` check. Works, but the lake, the part meant to outlive everything, depends on the lifetime of the other two.
- **Random names (UUIDs).** Never collide, but a retried batch would write a second object instead of meeting its first one, so write-once cannot detect retries.
- **`event_date` directories.** Good for queries by event time, but one batch spans several event dates, and late or re-planned batches write into old partitions.
- **Window and processing-time names.** Deterministic for a retry and free of raw database and broker counters: the Kafka position enters a name only hashed together with the topic id.

## Decision

Raw objects and the export and publish DLQ objects are named by their raw window ([ADR 0007](0007-windowed-publisher-cronicle.md)):

```
raw/source=coingecko/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-<NNNNN>.jsonl   (and _SUCCESS)
dlq/stage=export/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-<NNNNN>.jsonl
dlq/stage=publish/ingest_date=<YYYY-MM-DD>/window=<HHMM>/part-00000.jsonl
```

The transformer names every object of a batch by the batch's processing time:

```
processed/dataset=<ds>/v=1/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
rejected/dataset=<ds>/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
dlq/stage=transform/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
```

- `T`, `processed_date` and `hour` come from the batch's `planned_at` in its manifest (`pipeline.consumer_batches`), in UTC; `T` is formatted `YYYYMMDDTHHMMSSZ`. The same `planned_at` is every row's `_processed_at`.
- `id8` is the first 8 hex characters of sha256(`"<topic_id>|<partition>|<first_offset>"`).
- A re-planned batch keeps its manifest `planned_at` and its first offset, so it writes the same keys. A recreated topic has a new `topic_id` and a later time, so its keys never meet the old ones.
- `event_date`, the UTC date of the event time, stays part of the dedup key ([ADR 0002](0002-single-dedup-in-transformer.md)) and a column of rejected rows; processed rows have no `event_date` column (it is derived from `snapshot_at`). It is not a directory.

## Consequences

- The lake does not depend on the lifetime of Postgres or the broker and may be kept when the stack is recreated. Nothing is published twice: the exporter never overwrites an earlier stack's `_SUCCESS`, and a fresh scheduler runs only publish times (window end + 1 min) after it started, so it never repeats a run of the old stack. Windows the old stack closed but had not published are therefore published by hand, and with a new Postgres the claims start empty, so republishing a window the old stack had already published writes duplicates.
- The safety nets stay: the write-once PUT refuses different bytes under an existing key, and `ETLObjectChecksumMismatch` fires when that happens, which takes a non-deterministic transform, a foreign writer, or a clock that went backwards over a kept lake.
- Queries by event time scan more files. Production compaction to Parquet would partition by `event_date`.
- An hour of processed data can still receive files after a crash: a batch planned at 10:59:59 and written later still lands in `hour=10`. There is no hour-completeness marker; one would be a `_SUCCESS` per hour, written once no `planned` batch has `planned_at` in that hour.
- The format stays JSON Lines: it keeps arrays, the difference between null and an empty string, the nested fields of rejected and DLQ records, and self-describing lines under schema evolution. Parquet is the production compaction format.
