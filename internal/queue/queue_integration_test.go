package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/testutil"
)

func TestQueueIntegration(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	brokers := testutil.StartRedpanda(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	prod, err := NewProducer(brokers, "queue-it")
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	const topic = "coingecko.raw"
	const retention = 168 * time.Hour

	t.Run("EnsureTopic is idempotent", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 6, retention); err != nil {
			t.Fatalf("first EnsureTopic: %v", err)
		}
		if err := EnsureTopic(ctx, prod, topic, 6, retention); err != nil {
			t.Fatalf("second EnsureTopic: %v", err)
		}
	})

	t.Run("EnsureTopic detects partition mismatch", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 3, retention); err == nil {
			t.Fatal("expected partition mismatch error")
		}
	})

	t.Run("EnsureTopic detects retention mismatch", func(t *testing.T) {
		if err := EnsureTopic(ctx, prod, topic, 6, time.Hour); err == nil {
			t.Fatal("expected retention mismatch error")
		}
	})

	t.Run("TopicID", func(t *testing.T) {
		fallback := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		id1, err := TopicID(ctx, prod, topic, fallback)
		if err != nil {
			t.Fatal(err)
		}
		if id1 == uuid.Nil {
			t.Fatal("TopicID returned the nil uuid")
		}
		if id1 == fallback {
			t.Logf("broker returned no topic id; using fallback %s", id1)
		} else {
			t.Logf("broker topic id %s", id1)
		}
		id2, err := TopicID(ctx, prod, topic, fallback)
		if err != nil {
			t.Fatal(err)
		}
		if id1 != id2 {
			t.Fatalf("TopicID not stable: %s vs %s", id1, id2)
		}
		if _, err := TopicID(ctx, prod, "no.such.topic", fallback); err == nil {
			t.Fatal("expected error for unknown topic")
		}
	})

	t.Run("produce and consume with headers", func(t *testing.T) {
		value := []byte(`{"event_id":"x","payload":{"a":1}}`)
		key := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointCoinDetail, map[string]string{}, event.SHA256Hex(value))
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: HeaderEventID, Value: []byte("0199c8a0-0000-7000-8000-000000000001")},
				{Key: HeaderLineNo, Value: []byte("7")},
			},
		}
		if err := prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
		cons, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cons.Close()
		var got *kgo.Record
		for got == nil && ctx.Err() == nil {
			fs := cons.PollFetches(ctx)
			fs.EachError(func(_ string, _ int32, err error) { t.Logf("fetch error: %v", err) })
			fs.EachRecord(func(r *kgo.Record) { got = r })
		}
		if got == nil {
			t.Fatal("no record consumed")
		}
		if string(got.Key) != key || string(got.Value) != string(value) {
			t.Fatalf("got key=%q value=%q", got.Key, got.Value)
		}
		h := map[string]string{}
		for _, rh := range got.Headers {
			h[rh.Key] = string(rh.Value)
		}
		if h[HeaderEventID] != "0199c8a0-0000-7000-8000-000000000001" || h[HeaderLineNo] != "7" {
			t.Fatalf("headers = %v", h)
		}
	})
}
