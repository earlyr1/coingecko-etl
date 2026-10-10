package queue

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"coingecko-etl/internal/event"
)

func TestMessageKey(t *testing.T) {
	cases := []struct {
		name string
		ev   event.RawEvent
		want string
	}{
		{"markets", event.RawEvent{Endpoint: event.EndpointMarkets, Request: event.Request{Path: "/coins/markets"}}, "coins_markets"},
		{"detail", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/coins/bitcoin"}}, "bitcoin"},
		{"detail with api prefix", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/api/v3/coins/usd-coin"}}, "usd-coin"},
		{"detail trailing slash", event.RawEvent{Endpoint: event.EndpointCoinDetail, Request: event.Request{Path: "/coins/ethereum/"}}, "ethereum"},
		{"empty path falls back to endpoint", event.RawEvent{Endpoint: event.EndpointCoinDetail}, "coin_detail"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := MessageKey(c.ev); got != c.want {
				t.Fatalf("MessageKey = %q, want %q", got, c.want)
			}
		})
	}
}

func TestNewProducerRequiresBrokers(t *testing.T) {
	if _, err := NewProducer(nil, "x", nil); err == nil {
		t.Fatal("expected error for empty broker list")
	}
}

func TestNewProducerNilRegistry(t *testing.T) {
	cl, err := NewProducer([]string{"127.0.0.1:1"}, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}

func TestNewProducerRegistersClientMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	cl, err := NewProducer([]string{"127.0.0.1:1"}, "test", reg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// kprom registered etl_publisher_connects_total{node_id} when the client was built.
	dup := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etl_publisher", Name: "connects_total", Help: "Total number of connections opened",
	}, []string{"node_id"})
	err = reg.Register(dup)
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		t.Fatalf("expected kprom collector to be registered, Register returned %v", err)
	}
}
