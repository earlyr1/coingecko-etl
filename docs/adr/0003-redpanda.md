# ADR 0003: Kafka protocol, with Redpanda as the local broker

- Status: accepted
- Date: 2026-10-10

## Context

Between the raw zone and the transformer we want a durable, replayable log with keyed partitions and consumer groups: replay after a schema fix, copies of one response judged in order by one consumer, replicas that share and take over partitions. The stack must run in docker compose on a laptop.

Candidates: Apache Kafka (KRaft), Redpanda, NATS JetStream, Apache Pulsar, RabbitMQ (classic queues or streams), Redis Streams, and cloud queues (SQS, Pub/Sub).

## Decision

Use the Kafka protocol with the franz-go client (`kgo`, `kadm`), and run Redpanda in dev-container mode locally. One topic, `coingecko.raw`, 6 partitions, retention (TTL) 7 days — messages older than a week are deleted, while `s3://raw` and `raw.api_calls` keep the history — created by `etl init`; broker-side topic auto-creation is off (set by `deploy/redpanda/bootstrap.yaml`).

Messages are keyed by the event's `idempotency_key`, the sha256 of source, endpoint, parameters and payload hash. Responses spread over all partitions, and every copy of one response (a CDN repeat, a publisher rerun) lands on the same partition. The partitioner is franz-go's default murmur2, as in the Java client.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen locally: Kafka protocol in one C++ binary, no JVM, starts in ~2 s, ~300 MiB |
| Apache Kafka, Amazon MSK, Confluent Cloud | Production targets; drop-in because the code speaks the protocol |
| NATS JetStream, Apache Pulsar | Viable with caveats: different consumer model or heavier operations; weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, SQS, Pub/Sub | Rejected: no offset replay or keyed partitions at scale |

## Consequences

- The integration tests run against Redpanda.
- Message order per coin is not kept, and nothing depends on it: records are independent, `snapshot_at` is in every key, and the stale and anomaly flags are computed within one response. Claims are inserted in sorted key order, so two transformers with overlapping key sets cannot deadlock.
- The publisher is a batch job ([ADR 0007](0007-windowed-publisher-cronicle.md)), but the transformer stays a consumer group: offsets make its batches replayable and crash-safe, replicas share and take over partitions, and the topic is a 7-day replay buffer independent of the publish schedule.
- Redpanda lacks the Kafka Connect ecosystem; the design does not use it (Debezium and the S3 sink are documented as production alternatives).
- Redpanda is licensed under BSL 1.1, which permits this use; production would use MSK.
- Broker transactions (exactly-once) are not used: the S3 sink is outside Kafka anyway, and idempotent writes plus the dedup claims give exactly-once content in the processed zone.
