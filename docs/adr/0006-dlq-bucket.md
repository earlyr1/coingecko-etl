# ADR 0006: The dead-letter queue is a bucket, not a topic

- Status: accepted
- Date: 2026-10-10

## Context

Three stages produce records that "broke something and need a human": the exporter and the publisher (a raw line that cannot be decoded or fails S1) and the transformer (decode, schema, integrity and transform failures). Every hop is at-least-once, so a retried stage must not add the same dead-letter entry twice.

Options considered:

- **A dlq topic.** Easy to replay with broker tooling, but two of the three writers, the exporter and the publisher, run before the broker, and transformer dlq records would sit outside the batch manifest that makes the transformer's other output repeatable.
- **A dlq bucket.** Written like every other object in the lake: deterministic keys, `If-None-Match: *`.

## Decision

Dead-letter records go to `s3://dlq` (`./data/dlq`), one shape for every stage (`pipeline/dlq_record.v1.json`: stage, error class, violations, origin, original bytes, occurred at). Transformer dlq objects are part of the batch manifest and named like processed objects, after topic, partition and the first offset of the batch (`stage=transform/date=…/coingecko.raw+<partition>+<offset>.jsonl`); the exporter and publisher name theirs `export+batch+<batch id>` and `publish+batch+<batch id>`.

## Consequences

- A retried stage writes the same key and gets 412 with an equal checksum, so retries never duplicate entries.
- `ETLDeadLetterRecords` fires on any new dlq record.
- Recovery is a replay: after a fix, the raw objects are republished and reprocessed; a row that went to `dlq/` never claimed its dedup key, so the republished copy is written.
