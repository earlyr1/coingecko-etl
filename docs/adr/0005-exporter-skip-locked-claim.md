# ADR 0005: Exporter claims rows with FOR UPDATE SKIP LOCKED

- Status: accepted
- Date: 2026-10-10

## Context

The assignment places raw data in Postgres first. That gives a queryable record of every API call, failures included, before any deduplication. But a transactional table is not a change log: reading "new rows" from it safely needs either change data capture or an explicit record of which rows have been taken.

The exporter turns new rows of `raw.api_calls` into write-once raw JSONL objects. It must never skip a row, even one whose transaction commits late, and a retry after a crash must write the same object with the same bytes, so that the conditional PUT (`If-None-Match: *`) recognises it as a retry.

The exporter writes `ETL_WINDOW` windows ([ADR 0007](0007-windowed-publisher-cronicle.md)), so a claim also selects by `fetched_at` < window end.

## Options considered

- **Identity (BIGSERIAL) watermark.** Export everything above the last exported id. Loses rows when transactions commit out of order: a row with a lower id that commits after the watermark has moved past it is never exported.
- **Transaction-id (xid8) cursor.** Each row records the id of its inserting transaction, and the exporter reads by `(transaction id, seq)` below `pg_snapshot_xmin`, the oldest transaction still open. No row is updated and the raw table stays append-only, but the design relies on Postgres internals, has to be explained to every reader, and stalls behind any long transaction.
- **An `exported` flag set after the PUT.** Never loses a late-committing row and never stalls, but a crash between the PUT and the flag update lets the retry pick a different row set, so the retried object has different bytes. Stamping the batch id in the claim transaction, before the PUT, fixes this; that is the decision below.
- **A separate insert-only `pipeline.export_claims` table.** Keeps `raw.api_calls` append-only. Rejected: `export_batch_id` is the exporter's own synchronization column, kept on the rows it describes. The fetcher never reads or writes it, and a partial index keeps the pick cheap, while a second table would add a join to every claim and every read-back. This is why the reader writes to the writer's table.
- **Debezium CDC.** Streams the table to Kafka in commit order without any claim; the production option, but it adds a JVM Kafka Connect stack (see the README's Productionization).

## Decision

One exporter runs at a time under `pg_try_advisory_lock`. On every tick it first finishes parts a crashed run left `claimed`, then closes each window that is due, in order.

A part is claimed in one short transaction with no S3 I/O inside:

```sql
-- lock the window row: claims of one window run one after another, and none follows the seal
SELECT 1 FROM pipeline.export_windows WHERE window_start = $1 AND status = 'claiming' FOR UPDATE;

SELECT event_id::text, fetched_at FROM raw.api_calls
 WHERE outcome = 'ok' AND export_batch_id IS NULL AND fetched_at < $1   -- window end
 ORDER BY seq
 LIMIT $2                                                               -- ETL_EXPORT_BATCH_MAX, default 500
   FOR UPDATE SKIP LOCKED;

-- only when rows were found: the window's next part index and nextval for batch_id, then
INSERT INTO pipeline.export_batches (batch_id, window_start, part_index, row_count,
                                     min_fetched_at, max_fetched_at, object_key, claimed_at) VALUES (...);

UPDATE raw.api_calls a
   SET export_batch_id = $1
  FROM unnest($2::text[]::uuid[], $3::timestamptz[]) AS p(event_id, fetched_at)
 WHERE a.event_id = p.event_id
   AND a.fetched_at = p.fetched_at
   AND a.export_batch_id IS NULL
   AND a.fetched_at BETWEEN $4 AND $5;   -- fetched_at bounds of the picked rows: partition pruning
```

- `outcome = 'ok'` and `export_batch_id IS NULL` are literals, so the generic plan uses the partial index `api_calls_unexported_idx`.
- `nextval` runs only when rows were found, so batch ids stay dense. The ledger row is inserted before the stamp, because the foreign key `api_calls_export_batch_fk` rejects a stamp that points at a missing batch.
- Rows are stamped by primary key `(event_id, fetched_at)`, never by `seq`, whose uniqueness cannot be enforced on the partitioned table. The transaction rolls back unless every picked row was stamped.

The part is then written outside any transaction:

1. Read the rows back with `WHERE export_batch_id = $1 AND fetched_at BETWEEN <ledger bounds> ORDER BY seq, event_id, fetched_at`, a total order.
2. Build deterministic JSONL and validate every line against S1; a row that fails goes to the part's DLQ object.
3. Store the part's sha256 and line count in the ledger, or, on a retry, check that the rebuilt bytes match them.
4. PUT the DLQ object (if any), then the part, with `If-None-Match: *`; on 412 the stored sha256 is compared and equal bytes count as success.
5. Mark the part `exported`.

When the window has no more rows to claim, the exporter seals it: under the window row lock it builds the `_SUCCESS` manifest from the exported parts, stores its sha256 and sets the window `sealed`. Then it PUTs `_SUCCESS`, the window's last object, and marks the window `closed`.

## Consequences

- Nothing is skipped and nothing waits for a long transaction: a row that commits late, or that another session holds locked, still has `export_batch_id` NULL and goes into a later window.
- The claim binds the rows, the object key and the part index before the PUT, so a retry after a crash rebuilds the same bytes and the write-once PUT accepts it.
- The cost is one non-HOT UPDATE per successful row (`export_batch_id` is in an index predicate): a dead row version for vacuum and new entries in three indexes. The ~95 KB payload is stored out of line (TOAST) and is not copied by the UPDATE; in a prototype run the WAL was about 0.4 MB per 500-row part, under 1% of writing the rows.
- `raw.api_calls` is not strictly append-only: the recorded call never changes, but `export_batch_id` is set once. The lake stays write-once.
- A long transaction does not stall the export, but it delays vacuum of the dead versions the claim leaves, bounded by `transaction_timeout`. The daily `etl maintenance vacuum` job complements autovacuum ([ADR 0007](0007-windowed-publisher-cronicle.md)).
- `etl_export_lag_seconds` is the time past the end of the oldest window not yet closed (0 on schedule). A part that keeps failing keeps its window open, so the lag grows and `ETLExportLagHigh` fires; rows locked by another session do not hold a window back.
- Parts follow `seq` within a window; across windows a late commit lands in a later window, so publication order is not strict fetch order. Nothing downstream depends on it.
- Claims of one window serialize on its `pipeline.export_windows` row, so no part can be added after the manifest is sealed. With one exporter under the advisory lock this costs nothing, and `SKIP LOCKED` still skips rows that other sessions hold.
- At volume: per-partition autovacuum settings (the unexported-rows index behaves like a queue) and partition retention (the pick has no lower `fetched_at` bound, so a claim locks every partition of `raw.api_calls`).
- Scaling out means dropping the advisory lock and giving each claimed part an export lease that another instance may take over once it expires. In production we would land raw events directly in a log or object storage, or stream the table with Debezium CDC.
