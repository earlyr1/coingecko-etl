# ADR 0004: Pipeline state in Postgres

- Status: accepted
- Date: 2026-10-10

## Context

Several pieces of state must survive crashes, be shared by replicas and change atomically with each other:

- which `raw.api_calls` rows have been exported, and in which object (exporter);
- which exported objects have been published, with leases so a crashed publisher's batch is picked up again (publisher);
- which offset ranges a transformer has planned and written, with the object keys and checksums of each batch (transformer manifest);
- which natural keys have been claimed, by which position, with which content hash (deduplication).

Alternatives: broker offsets only (cannot answer "was key X seen?"), S3 conditional writes only (guard single objects, not multi-object batches), in-memory state (lost on rebalance), Kafka Streams state stores (JVM-only) or goka (no transactions spanning state and output).

## Decision

Keep all pipeline state in the Postgres instance that already holds the raw zone, in schema `pipeline`:

- `pipeline.export_batches`: the exporter ledger (xid8 range, object key, checksum, status) and the publisher lease and `published_at` columns;
- `pipeline.consumer_batches`: the transformer manifest per (topic id, partition, first offset), with an exclusion constraint that forbids overlapping ranges;
- `pipeline.dedup_claims`: the dedup claims, partitioned by `event_date`.

Transactions are short and never span S3 or broker I/O. Postgres runs with `idle_in_transaction_session_timeout = 60s` and `transaction_timeout = 5min`.

## Consequences

- Batch planning, claims and object checksums commit atomically, which is what makes the crash table in the README hold.
- Postgres is on the critical path of every stage; in production it is RDS/Aurora Multi-AZ.
- The exporter uses a transaction-id (xid8) cursor rather than an identity watermark, which loses rows when transactions commit out of order. An `exported` flag on `raw.api_calls` with `FOR UPDATE SKIP LOCKED` was rejected too: it never loses a late-committing row and never stalls, but it UPDATEs every raw row (the raw table stops being append-only) and still needs a claimed batch for byte-identical retries, which `pipeline.export_batches` already provides at one row per batch.
- `dedup_claims` grows with data; daily partitions make retention a `DROP TABLE` (pg_partman in production).
