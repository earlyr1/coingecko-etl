# ADR 0002: One deduplication mechanism, in the transformer

- Status: accepted
- Date: 2026-10-10

## Context

Duplicates enter the pipeline in two ways:

- **Redelivery**: every hop is at-least-once, so the same event can reach the transformer more than once (publisher reruns, consumer rebalances, crashes before an offset commit).
- **True duplicates**: CoinGecko's CDN caches responses for 60 s and we poll every 30 s, so half or more of the markets responses are byte-identical (two in three on the demo stack); the same record can also arrive unchanged in another response. A stale coin repeated in two consecutive snapshots is not a duplicate: `snapshot_at` is part of the key, so it yields two rows, both flagged `is_stale`.

The assignment requires raw data in Postgres before deduplication. Deduplicating at several stages (fetcher, exporter, publisher, transformer) would mean several rules to keep consistent and several places where a crash can lose data.

An in-memory "seen keys" cache was tried in a spike and lost its state on rebalance: two records were written twice after a partition moved.

## Decision

Deduplicate exactly once, in the transformer, on the natural key of each processed record (for example `coin_id | snapshot_at` for `market_snapshot`). Each record claims its key in `pipeline.dedup_claims` with `INSERT … ON CONFLICT DO NOTHING` inside the batch transaction and reads the claim back:

- the claim points to this record's own position (topic, partition, offset, ordinal) → write it;
- another position, same content hash (business fields only) → drop as a duplicate;
- another position, different content hash → write to `rejected/` as `snapshot_conflict`.

A record without an id of its own is rejected as `missing_id` and still claims a key: its response's `idempotency_key` (the `event_id` when there is none), its index in the payload and `snapshot_at`, with the coin id in front for tickers. Keys from different coins and responses therefore never collide, and the copies of one response still deduplicate.

The fetcher's `idempotency_key` is the Kafka message key ([ADR 0003](0003-redpanda.md)) and labels the drop-reason metric (`replay`: same `event_id`; `duplicate_response`: same `idempotency_key`; `duplicate_record`: the same content from another response); it is not a dedup input. Every other hop is made idempotent instead (primary keys, ledgers, write-once objects). The claim's `first_seen_at` column is kept for audit only; no code reads it.

## Consequences

- Raw storage keeps everything, including duplicates, which makes the source behaviour observable.
- Because a claim remembers its position, reprocessing a batch after a crash reproduces the same decisions and the same bytes; the naive rule "drop if already claimed" would have lost the batch.
- Claims are partitioned by `event_date`. The daily `etl maintenance retention` job ([ADR 0007](0007-windowed-publisher-cronicle.md)) drops partitions older than `ETL_CLAIMS_RETENTION_DAYS` (default 8). Config refuses a value below the topic retention in days plus one, because partitions follow `event_date` while replays follow publish time (after an exporter outage a message published today can carry yesterday's `event_date`). Production would keep 30 days, which also covers a raw republish after a schema fix. Redeliveries, topic replays and window reruns are deduplicated only within that period; an older replay writes duplicates.
- Every record costs one Postgres round trip inside a short transaction; at higher volume the claims can move to a key-value store with conditional writes (e.g. DynamoDB).
