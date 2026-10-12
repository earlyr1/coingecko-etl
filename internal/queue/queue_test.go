package queue

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/event"
)

func TestMessageKey(t *testing.T) {
	k1, k2 := "idem-1", "idem-2"
	a := event.RawEvent{EventID: "e-a", IdempotencyKey: &k1, Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/coins/bitcoin"}}
	b := event.RawEvent{EventID: "e-b", IdempotencyKey: &k1, Endpoint: event.EndpointMarkets, Request: event.Request{Path: "/coins/markets"}}
	if got := MessageKey(a); got != k1 {
		t.Fatalf("MessageKey = %q, want the idempotency key %q", got, k1)
	}
	if MessageKey(b) != MessageKey(a) {
		t.Fatalf("copies of one response got keys %q and %q", MessageKey(a), MessageKey(b))
	}
	c := a
	c.IdempotencyKey = &k2
	if MessageKey(c) == MessageKey(a) {
		t.Fatalf("different responses share key %q", MessageKey(a))
	}
	empty := ""
	for name, ev := range map[string]event.RawEvent{
		"nil idempotency key":   {EventID: "e-nil", Request: event.Request{Path: "/coins/bitcoin"}},
		"empty idempotency key": {EventID: "e-nil", IdempotencyKey: &empty, Request: event.Request{Path: "/coins/bitcoin"}},
	} {
		if got := MessageKey(ev); got != "e-nil" {
			t.Errorf("%s: MessageKey = %q, want the event id", name, got)
		}
	}
}

func TestMessageKeySpreadsResponsesAcrossPartitions(t *testing.T) {
	part := kgo.StickyKeyPartitioner(nil).ForTopic("coingecko.raw") // murmur2, the default producer's hasher for keyed records
	seen := map[int]bool{}
	for i := 0; i < 60; i++ {
		payload := []byte(fmt.Sprintf(`[{"id":"coin-%d"}]`, i))
		k := event.IdempotencyKey(event.SourceCoinGecko, event.EndpointMarkets, map[string]string{"vs_currency": "usd"}, event.SHA256Hex(payload))
		p := part.Partition(&kgo.Record{Key: []byte(MessageKey(event.RawEvent{EventID: fmt.Sprintf("e-%d", i), IdempotencyKey: &k}))}, 6)
		seen[p] = true
		cdnCopy := event.RawEvent{EventID: "another-poll", IdempotencyKey: &k}
		if q := part.Partition(&kgo.Record{Key: []byte(MessageKey(cdnCopy))}, 6); q != p {
			t.Fatalf("copies of one response on partitions %d and %d", p, q)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("60 responses hit %d of 6 partitions", len(seen))
	}
}

func TestNewProducerRequiresBrokers(t *testing.T) {
	if _, err := NewProducer(nil, "x"); err == nil {
		t.Fatal("expected error for empty broker list")
	}
}

func TestNewProducerBuildsClient(t *testing.T) {
	// No broker listens on port 1: building the client must not dial.
	cl, err := NewProducer([]string{"127.0.0.1:1"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}
