# ADR 0004: Pipeline state in Postgres

- Status: accepted
- Date: 2026-10-10

## Context

Several pieces of state must survive crashes, be shared by replicas and change atomically with each other:

- which `raw.api_calls` rows belong to which export batch and window, and in which object (exporter);
- which offset ranges a transformer has planned and written, with the object keys and checksums of each batch (transformer manifest);
- which natural keys have been claimed, by which position, with which content hash (deduplication).

Alternatives: broker offsets only (cannot answer "was key X seen?"), S3 conditional writes only (guard single objects, not multi-object batches), in-memory state (lost on rebalance), Kafka Streams state stores (JVM-only) or goka (no transactions spanning state and output).

## Decision

Keep all pipeline state in the Postgres instance that already holds the raw zone, in schema `pipeline`:

- `pipeline.export_windows` and `pipeline.export_batches`: the exporter ledger. One row per window (bounds, status `claiming` → `sealed` → `closed`, the `_SUCCESS` key and sha256, row and part counts; at most one window is not closed) and one row per raw part (window, part index, claimed row count, line count, `fetched_at` bounds, object key, checksum, status `claimed` → `exported`). Claimed rows point to their part through `raw.api_calls.export_batch_id`;
- `pipeline.consumer_batches`: the transformer manifest per (topic id, partition, first offset), with an exclusion constraint that forbids overlapping ranges;
- `pipeline.dedup_claims`: the dedup claims, partitioned by `event_date`.

The publish job keeps no state of its own: it reads the raw window's `_SUCCESS` from S3 ([ADR 0007](0007-windowed-publisher-cronicle.md)).

Transactions are short and never span S3 or broker I/O. Postgres runs with `idle_in_transaction_session_timeout = 60s` and `transaction_timeout = 5min`, so a stuck session cannot hold a connection and row locks.

## Consequences

- Batch planning, claims and object checksums commit atomically, which is what makes the crash cases in the README's [Delivery guarantees](../../README.md#delivery-guarantees) hold.
- Postgres is on the critical path of the fetcher, the exporter and the transformer; in production it is RDS/Aurora Multi-AZ.
- The exporter claims rows with `FOR UPDATE SKIP LOCKED` and stamps them in the same transaction as the ledger row; the alternatives are compared in [ADR 0005](0005-exporter-skip-locked-claim.md).
- `dedup_claims` partitions and `written` `consumer_batches` rows are removed by the daily `etl maintenance retention` job ([ADR 0007](0007-windowed-publisher-cronicle.md)); pg_partman would manage the partitions in production. Retention of `raw.api_calls` and the export ledger is not implemented (raw rows would go first, then ledger rows, because of the foreign key).
- Known limitation: a transformer batch fixes its offset range in the manifest. If the topic's 7-day retention deletes the records of a batch that is still `planned` (for example after a week-long transformer outage), that batch cannot be redone. It needs a manual repair: delete the `planned` row of `pipeline.consumer_batches` and that batch's claims by position (`pipeline.dedup_claims` rows with its topic id and partition and offsets in its range; `dedup_claims_position_idx` covers that lookup).
