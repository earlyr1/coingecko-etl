# ADR 0003: Kafka protocol, with Redpanda as the local broker

- Status: accepted
- Date: 2026-10-10

## Context

Between the raw zone and the transformer we want a durable, replayable log with per-key ordering and consumer groups: replay after a schema fix, sequential dedup decisions per key, and replicas that share and take over partitions. The stack must run in docker compose on a laptop.

Candidates: Apache Kafka 4.3 (KRaft), Redpanda, NATS JetStream, Apache Pulsar, RabbitMQ (classic queues or streams), Redis Streams, and cloud queues (SQS, Pub/Sub).

## Decision

Use the Kafka protocol with the franz-go client (`kgo`, `kadm`, `kprom`), and run Redpanda `v26.2.4` in dev-container mode (`--smp 1 --memory 1G`) locally. One topic, `coingecko.raw`, 6 partitions, 7 days retention, created by `etl init`; broker-side topic auto-creation is off (set by `deploy/redpanda/bootstrap.yaml`). Messages are keyed by `coins_markets` or the coin id.

| Broker | Verdict |
|---|---|
| Redpanda | Chosen locally: Kafka protocol in one C++ binary, no JVM, starts in ~2 s, ~200 MiB |
| Apache Kafka, Amazon MSK, Confluent Cloud | Production targets; drop-in because the code speaks the protocol |
| NATS JetStream, Apache Pulsar | Viable with caveats: different consumer model or heavier operations; weaker data-engineering ecosystem |
| RabbitMQ classic queues, Redis Streams, SQS, Pub/Sub | Rejected: no offset replay or per-key ordering at scale |

## Consequences

- The same client code was verified against both Apache Kafka 4.3.1 and Redpanda v26.2.4 during design; the integration tests run against Redpanda v26.2.4.
- Redpanda lacks the Kafka Connect ecosystem; the design does not use it (Debezium and the S3 sink are documented as production alternatives).
- Redpanda is licensed under BSL 1.1, which permits this use; production would use MSK.
- Broker transactions (exactly-once) are not used: the S3 sink is outside Kafka anyway, and idempotent writes plus the dedup claims give exactly-once content in the processed zone.
