// Package bootstrap implements `etl init`: a one-shot, idempotent setup of
// every dependency the long-running services expect to exist.
//
//   - Postgres: goose migrations, then daily partitions [today-1, today+3].
//   - Broker: the raw topic with the configured partitions and retention
//     (an existing topic is verified, never altered).
//   - Object storage: the raw, processed, rejected and dlq buckets.
//   - The lake check: a fresh export ledger next to a raw bucket that already
//     holds objects is refused (ErrLakeFromOtherLifetime).
//
// Running it twice is safe: every step is a no-op when its target exists.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
)

// partitionDaysAhead matches the fetcher's maintenance loop: daily partitions
// [today-1, today+3].
const partitionDaysAhead = 3

// ErrLakeFromOtherLifetime: pipeline.export_batches is empty but the raw
// bucket already holds objects. Object keys are unique only within one
// Postgres and Redpanda lifetime (batch ids restart at 1, topic offsets at
// 0), so the new objects would collide with the old ones and stop the
// exporter and the transformers with checksum mismatches.
var ErrLakeFromOtherLifetime = errors.New("the raw bucket already holds objects but pipeline.export_batches is empty: " +
	"the lake (./data and ./.s3meta) belongs to a different Postgres/Redpanda lifetime. " +
	"Wipe the lake together with the volumes (make clean) or restore the matching volumes")

// Retry policy for reaching a dependency. Compose starts init only after the
// healthchecks pass, but "healthy" can precede "accepts our requests" by a
// few seconds (e.g. Redpanda's controller election), so every step retries.
var (
	dependencyTimeout = 60 * time.Second
	initialBackoff    = 500 * time.Millisecond
	maxBackoff        = 5 * time.Second
)

// Run executes every init step in order and returns the first error.
// It logs one line per completed step with its duration.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	log = log.With("stage", "init")
	started := time.Now()
	log.Info("init started",
		"brokers", cfg.Brokers, "topic", cfg.Topic, "s3_endpoint", cfg.S3Endpoint)

	ledgerEmpty, err := initPostgres(ctx, cfg, log)
	if err != nil {
		return err
	}
	if err := initBroker(ctx, cfg, log); err != nil {
		return err
	}
	if err := initBuckets(ctx, cfg, log, ledgerEmpty); err != nil {
		return err
	}
	log.Info("init finished", "duration_ms", time.Since(started).Milliseconds())
	return nil
}

// initPostgres migrates and creates partitions. It reports whether the
// export ledger is empty, for the lake check in initBuckets.
func initPostgres(ctx context.Context, cfg config.Config, log *slog.Logger) (ledgerEmpty bool, err error) {
	var pool *pgxpool.Pool
	err = step(ctx, log, "postgres_connect", func(ctx context.Context) error {
		p, err := pg.Connect(ctx, cfg.PostgresDSN)
		if err != nil {
			return err
		}
		pool = p
		return nil
	})
	if err != nil {
		return false, err
	}
	defer pool.Close()

	if err := step(ctx, log, "postgres_migrate", func(ctx context.Context) error {
		return pg.Migrate(ctx, pool)
	}); err != nil {
		return false, err
	}
	if err := step(ctx, log, "postgres_partitions", func(ctx context.Context) error {
		return pg.EnsurePartitions(ctx, pool, time.Now(), partitionDaysAhead)
	}); err != nil {
		return false, err
	}
	err = step(ctx, log, "postgres_export_ledger", func(ctx context.Context) error {
		return pool.QueryRow(ctx, `SELECT NOT EXISTS (SELECT 1 FROM pipeline.export_batches)`).Scan(&ledgerEmpty)
	})
	return ledgerEmpty, err
}

func initBroker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// A plain client is enough for admin requests; kgo.NewClient does not dial
	// until the first request, so it cannot fail on an unreachable broker.
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-init"))
	if err != nil {
		return fmt.Errorf("init: broker client: %w", err)
	}
	defer cl.Close()

	if err := step(ctx, log, "broker_ping", cl.Ping); err != nil {
		return err
	}
	return step(ctx, log, "broker_topic", func(ctx context.Context) error {
		return queue.EnsureTopic(ctx, cl, cfg.Topic, cfg.TopicPartitions, cfg.TopicRetention)
	})
}

// initBuckets creates the buckets. With an empty export ledger it also checks
// that the raw bucket holds no object (ErrLakeFromOtherLifetime).
func initBuckets(ctx context.Context, cfg config.Config, log *slog.Logger, ledgerEmpty bool) error {
	s3c, err := s3.New(ctx, s3.Config{
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		Region:    cfg.S3Region,
	})
	if err != nil {
		return fmt.Errorf("init: s3 client: %w", err)
	}
	if err := step(ctx, log, "s3_ping", s3c.Ping); err != nil {
		return err
	}
	if err := step(ctx, log, "s3_buckets", func(ctx context.Context) error {
		return s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ)
	}); err != nil {
		return err
	}
	if !ledgerEmpty {
		return nil
	}
	var rawObjects bool
	if err := step(ctx, log, "s3_lake_check", func(ctx context.Context) error {
		var err error
		rawObjects, err = s3c.HasAnyObject(ctx, s3.BucketRaw)
		return err
	}); err != nil {
		return err
	}
	if rawObjects {
		log.Error("init refused: lake from another lifetime", "step", "s3_lake_check", "error", ErrLakeFromOtherLifetime)
		return fmt.Errorf("init: %w", ErrLakeFromOtherLifetime)
	}
	return nil
}

// step runs fn with retry and logs the outcome under the given step name.
func step(ctx context.Context, log *slog.Logger, name string, fn func(context.Context) error) error {
	start := time.Now()
	attempts, err := retry(ctx, dependencyTimeout, initialBackoff, maxBackoff, func(ctx context.Context, attempt int) error {
		err := fn(ctx)
		if err != nil {
			log.Warn("init step failed, retrying", "step", name, "attempt", attempt, "error", err)
		}
		return err
	})
	if err != nil {
		log.Error("init step gave up", "step", name, "attempts", attempts, "error", err)
		return fmt.Errorf("init: %s: %w", name, err)
	}
	log.Info("init step done", "step", name, "attempts", attempts,
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// retry calls fn until it succeeds, ctx is done, or timeout elapses. The
// wait between attempts doubles from initial up to maxWait. It returns the
// number of attempts and, on failure, the last error of fn joined with the
// reason retrying stopped.
func retry(ctx context.Context, timeout, initial, maxWait time.Duration, fn func(ctx context.Context, attempt int) error) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wait := initial
	for attempt := 1; ; attempt++ {
		err := fn(ctx, attempt)
		if err == nil {
			return attempt, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempt, errors.Join(err, fmt.Errorf("gave up after %d attempts: %w", attempt, ctx.Err()))
		case <-timer.C:
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
}
