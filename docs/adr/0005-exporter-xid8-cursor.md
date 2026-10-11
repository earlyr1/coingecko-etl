# ADR 0005: Exporter cursor over transaction ids (xid8)

- Status: accepted
- Date: 2026-10-10

## Context

The assignment places raw data in Postgres first. That gives a queryable record of every API call, failures included, before any deduplication. But a transactional table is not a change log: reading "new rows" from it safely needs either change data capture or a cursor that respects commit order. We chose a cursor over `(ingest_txid, seq)` below `pg_snapshot_xmin`: it never skips a row, at the cost of depending on Postgres internals and stalling behind a long transaction (bounded by timeouts and the export-lag alert). It is a pragmatic workaround rather than an elegant one. In production we would land raw events directly in a log or object storage, or stream the table with Debezium CDC.

The exporter turns new rows of `raw.api_calls` into write-once raw JSONL objects. It must never skip a row, even one whose transaction commits late, and a retry after a crash must write the same object with the same bytes, so that the conditional PUT (`If-None-Match: *`) recognises it as a retry.

Options considered:

- **Identity (BIGSERIAL) watermark.** Export everything above the last exported id. Loses rows when transactions commit out of order: a row with a lower id that commits after the watermark has moved past it is never exported (reproduced on Postgres 18.6).
- **An `exported` flag** on `raw.api_calls` with `SELECT … WHERE NOT exported … FOR UPDATE SKIP LOCKED LIMIT n`. It never loses a late-committing row and never stalls, but it UPDATEs every raw row, so the raw table stops being append-only (dead tuples, vacuum, a partial index). A crash between the PUT and the flag update lets the retry pick a different row set, so a byte-identical retry still needs a claimed batch: a ledger anyway, on top of one update per raw row.
- **Debezium CDC.** Streams the table to Kafka in commit order without a cursor table; the production option, but it adds a JVM Kafka Connect stack (see the README's Productionization).
- **Transaction-id cursor.** Read rows by `(ingest_txid, seq)`, bounded by `pg_snapshot_xmin(pg_current_snapshot())`, the oldest transaction still open.

## Decision

Use the transaction-id cursor. Each row records `ingest_txid xid8`, the id of its inserting transaction. The exporter reads only below the oldest open transaction, so it can stall behind a long transaction but never skip a row. A single active instance holds `pg_try_advisory_lock`.

Each batch follows four steps: (1) a short transaction claims the range in `pipeline.export_batches` and derives the object key from the batch id; (2) the range is read and serialized as deterministic JSONL, each line validated against S1; (3) the object is PUT with `If-None-Match: *` and a `sha256` metadata entry, and on 412 the stored checksum is compared; (4) the batch is marked exported. No database transaction spans S3 I/O, and only `outcome = 'ok'` rows are exported.

## Consequences

- `raw.api_calls` stays append-only; the ledger costs one row per batch, not one update per raw row.
- The claimed range fixes the row set and the object key, so a retry after a crash writes the same bytes.
- This is a workaround for using a transactional table as a change stream, not an elegant design: it depends on Postgres internals and has to be explained. In production, raw events would land directly in a log or object storage, or the table would be streamed with Debezium CDC.
- A long transaction stalls the export. Postgres timeouts (`idle_in_transaction_session_timeout = 60s`, `transaction_timeout = 5min`) bound the stall, `etl_export_lag_seconds` shows it, and `ETLExportLagHigh` alerts on it.
