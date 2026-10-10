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
- The claim table grows with data volume. It is partitioned by `event_date` and must outlive the topic's 7-day retention so that replays within retention are still deduplicated; nothing in this stack deletes claims, and a production retention job (pg_partman) would keep at least 30 days.
- Every record costs one Postgres round trip inside a short transaction; at higher volume the claims can move to a key-value store with conditional writes (e.g. DynamoDB).
