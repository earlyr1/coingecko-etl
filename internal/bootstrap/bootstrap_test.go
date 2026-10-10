package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/config"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	attempts, err := retry(context.Background(), time.Second, time.Millisecond, 4*time.Millisecond,
		func(ctx context.Context, attempt int) error {
			calls++
			if attempt != calls {
				t.Fatalf("attempt %d on call %d", attempt, calls)
			}
			if calls < 3 {
				return errors.New("not yet")
			}
			return nil
		})
	if err != nil || attempts != 3 {
		t.Fatalf("retry = (%d, %v), want (3, nil)", attempts, err)
	}
}

func TestRetryGivesUpAtTimeoutWithLastError(t *testing.T) {
	start := time.Now()
	_, err := retry(context.Background(), 50*time.Millisecond, time.Millisecond, 5*time.Millisecond,
		func(ctx context.Context, attempt int) error { return errors.New("connection refused") })
	if err == nil {
		t.Fatal("retry succeeded, want error")
	}
	if !strings.Contains(err.Error(), "connection refused") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want last error joined with DeadlineExceeded", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("retry took %v, want about 50ms", el)
	}
}

func TestRetryStopsOnParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts, err := retry(ctx, time.Minute, time.Second, time.Second,
		func(ctx context.Context, attempt int) error { return errors.New("down") })
	if attempts != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("retry = (%d, %v), want (1, Canceled)", attempts, err)
	}
}

// closedPort returns a loopback address on which nothing listens.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestRunFailsWhenPostgresUnreachable(t *testing.T) {
	old := dependencyTimeout
	dependencyTimeout = 300 * time.Millisecond
	t.Cleanup(func() { dependencyTimeout = old })

	cfg := config.Config{
		PostgresDSN: "postgres://etl:etl@" + closedPort(t) + "/etl?sslmode=disable&connect_timeout=1",
		Brokers:     []string{closedPort(t)},
		Topic:       "coingecko.raw", TopicPartitions: 6, TopicRetention: 168 * time.Hour,
		S3Endpoint: "http://" + closedPort(t), S3AccessKey: "etl", S3SecretKey: "etlsecret123", S3Region: "us-east-1",
	}
	err := Run(context.Background(), cfg, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "postgres_connect") {
		t.Fatalf("Run = %v, want postgres_connect error", err)
	}
}
