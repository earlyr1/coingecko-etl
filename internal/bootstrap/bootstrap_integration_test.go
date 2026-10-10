package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"
)

func TestRunTwiceAgainstRealDependencies(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	dsn := testutil.StartPostgres(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, dataDir := testutil.StartVersityGW(t)

	// StartVersityGW pre-creates the bucket directories; remove them so the
	// test proves that init creates the buckets itself.
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if err := os.RemoveAll(filepath.Join(dataDir, b)); err != nil {
			t.Fatal(err)
		}
	}

	cfg := config.Config{
		Service:         "init",
		PostgresDSN:     dsn,
		Brokers:         brokers,
		Topic:           "coingecko.raw",
		TopicPartitions: 6,
		TopicRetention:  168 * time.Hour,
		S3Endpoint:      endpoint,
		S3AccessKey:     testutil.VersityGWAccessKey,
		S3SecretKey:     testutil.VersityGWSecretKey,
		S3Region:        "us-east-1",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	for i := 1; i <= 2; i++ {
		if err := Run(ctx, cfg, discardLogger()); err != nil {
			t.Fatalf("run #%d: %v", i, err)
		}
	}

	// Postgres: tables exist and today's partitions were created.
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	today := time.Now().UTC().Format("20060102")
	for _, tbl := range []string{"raw.api_calls", "pipeline.export_batches", "pipeline.consumer_batches",
		"raw.api_calls_p" + today, "pipeline.dedup_claims_p" + today} {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&ok); err != nil || !ok {
			t.Errorf("relation %s missing (err=%v)", tbl, err)
		}
	}

	// Broker: the topic has 6 partitions.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(ctx, cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}
	if td, ok := topics[cfg.Topic]; !ok || td.Err != nil || len(td.Partitions) != 6 {
		t.Fatalf("topic %s: %+v", cfg.Topic, topics[cfg.Topic])
	}

	// Object storage: every bucket is a directory under the gateway root.
	for _, b := range []string{"raw", "processed", "rejected", "dlq"} {
		if fi, err := os.Stat(filepath.Join(dataDir, b)); err != nil || !fi.IsDir() {
			t.Errorf("bucket dir %s: %v", b, err)
		}
	}

	// A third run with a different partition count must fail: init verifies
	// an existing topic instead of silently accepting it.
	bad := cfg
	bad.TopicPartitions = 3
	old := dependencyTimeout
	dependencyTimeout = 2 * time.Second
	defer func() { dependencyTimeout = old }()
	err = Run(ctx, bad, discardLogger())
	if err == nil {
		t.Fatal("run with 3 partitions succeeded, want topic mismatch error")
	}
	if !strings.Contains(err.Error(), "partitions") {
		t.Fatalf("run with 3 partitions: err = %v, want a partition-count mismatch", err)
	}
}
