// Package queue builds franz-go clients and manages the raw topic.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"coingecko-etl/internal/event"
)

// Record header names. Values are UTF-8 strings.
const (
	HeaderEventID        = "event_id"
	HeaderIdempotencyKey = "idempotency_key"
	HeaderSchema         = "schema"
	HeaderRawObject      = "raw_object"
	HeaderLineNo         = "line_no"
)

// MessageKey is the partitioning key: the event's idempotency_key (sha256 of
// request + body). Copies of one response (CDN repeats, publisher re-runs)
// share it and land on one partition, so one transformer decides them in
// order. Different responses spread over all partitions (franz-go's default
// partitioner hashes keys with murmur2, like the Java client). Every ok event
// has one; the event_id fallback only keeps the key deterministic and non-empty.
func MessageKey(ev event.RawEvent) string {
	if ev.IdempotencyKey != nil && *ev.IdempotencyKey != "" {
		return *ev.IdempotencyKey
	}
	return ev.EventID
}

// NewProducer returns an idempotent producer with acks=all.
// kprom client metrics are registered on reg under namespace etl_publisher
// (reg may be nil: no client metrics). kprom registers its collectors when
// the client is created and panics on duplicate registration, so build at
// most one producer per registry.
func NewProducer(brokers []string, clientID string, reg prometheus.Registerer) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("queue: no brokers configured")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotence is on by default and requires acks=all
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20 * time.Millisecond),
		kgo.RecordDeliveryTimeout(2 * time.Minute),
	}
	if reg != nil {
		kopts := []kprom.Opt{kprom.Registerer(reg)}
		if g, ok := reg.(prometheus.Gatherer); ok {
			kopts = append(kopts, kprom.Gatherer(g))
		}
		opts = append(opts, kgo.WithHooks(kprom.NewMetrics("etl_publisher", kopts...)))
	}
	return kgo.NewClient(opts...)
}

// EnsureTopic creates topic with the given partition count and retention
// (replication factor = broker default). If the topic already exists it
// verifies the partition count and retention.ms and returns an error on mismatch.
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32, retention time.Duration) error {
	adm := kadm.NewClient(cl)
	retentionMS := strconv.FormatInt(retention.Milliseconds(), 10)
	resps, err := adm.CreateTopics(ctx, partitions, -1, map[string]*string{
		"retention.ms": kadm.StringPtr(retentionMS),
	}, topic)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	resp, ok := resps[topic]
	if !ok {
		return fmt.Errorf("create topic %s: no response for topic", topic)
	}
	if resp.Err == nil {
		return nil
	}
	if !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w (%s)", topic, resp.Err, resp.ErrMessage)
	}

	// Topic exists: verify it matches what we expect.
	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe topic %s: %w", topic, err)
	}
	td, ok := details[topic]
	if !ok {
		return fmt.Errorf("describe topic %s: not in metadata", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("describe topic %s: %w", topic, td.Err)
	}
	if got := int32(len(td.Partitions)); got != partitions {
		return fmt.Errorf("topic %s has %d partitions, expected %d", topic, got, partitions)
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, err)
	}
	if rc.Err != nil {
		return fmt.Errorf("describe topic configs %s: %w", topic, rc.Err)
	}
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.MaybeValue() != retentionMS {
			return fmt.Errorf("topic %s has retention.ms=%s, expected %s", topic, c.MaybeValue(), retentionMS)
		}
	}
	return nil
}

// TopicID returns the broker-assigned topic id (the topic's incarnation).
// Brokers without topic-id support return all zeros; then fallback is returned.
func TopicID(ctx context.Context, cl *kgo.Client, topic string, fallback uuid.UUID) (uuid.UUID, error) {
	details, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return uuid.Nil, fmt.Errorf("metadata for %s: %w", topic, err)
	}
	td, ok := details[topic]
	if !ok {
		return uuid.Nil, fmt.Errorf("metadata for %s: topic not returned", topic)
	}
	if td.Err != nil {
		return uuid.Nil, fmt.Errorf("metadata for %s: %w", topic, td.Err)
	}
	if td.ID == (kadm.TopicID{}) {
		return fallback, nil
	}
	return uuid.UUID(td.ID), nil
}
