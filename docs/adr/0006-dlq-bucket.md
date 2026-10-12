# ADR 0006: The dead-letter queue is a bucket, not a topic

- Status: accepted
- Date: 2026-10-10

## Context

Three stages produce records that "broke something and need a human": the exporter and the publisher (a raw line that cannot be decoded or fails S1) and the transformer (decode, schema, integrity and transform failures). Every hop is at-least-once, so a retried stage must not add the same dead-letter entry twice.

Options considered:

- **A dlq topic.** Easy to replay with broker tooling, but two of the three writers, the exporter and the publisher, run before the broker, and transformer dlq records would sit outside the batch manifest that makes the transformer's other output repeatable.
- **A dlq bucket.** Written like every other object in the lake: deterministic keys, `If-None-Match: *`.

## Decision

Dead-letter records go to `s3://dlq` (`./data/dlq`), one shape for every stage (`pipeline/dlq_record.v1.json`: stage, error class, violations, origin, original bytes, occurred at). Keys are deterministic, so a retry writes the same key:

- transformer: `dlq/stage=transform/processed_date=<d>/hour=<HH>/part-<planned_at>-<id8>.jsonl`, named like the batch's processed objects ([ADR 0008](0008-processing-time-object-names.md)) and listed in the batch manifest;
- exporter: `dlq/stage=export/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl`, with the index of the raw part whose rows failed;
- publisher: `dlq/stage=publish/ingest_date=<d>/window=<HHMM>/part-00000.jsonl`, one object per window with deterministic content.

## Consequences

- A retried stage writes the same key and gets 412 with an equal checksum, so retries never duplicate entries.
- `ETLDeadLetterRecords` fires on any new dlq record; for the publish job it reads the DLQ line count the last successful run pushed to the Pushgateway.
- Recovery of publish and transform failures is a replay: after a fix, the window is rerun (Cronicle or `etl publish --now`) and reprocessed, within the claims retention ([ADR 0002](0002-single-dedup-in-transformer.md)); a record that went to `dlq/` never claimed its dedup key, so the republished copy is written. A row the exporter sent to `dlq/` is in no raw part (it stays in `raw.api_calls`, and its bytes are in the DLQ record), so replaying it is a manual step.
