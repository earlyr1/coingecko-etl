// Package pg owns the Postgres connection pool, schema migrations and
// partition maintenance for the raw and pipeline schemas.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"coingecko-etl/migrations"
)

// partitionLockKey serializes concurrent EnsurePartitions calls across
// processes (init, fetcher maintenance loop, insert-time repair).
// Arbitrary constant: "cgetl-pt" in ASCII.
const partitionLockKey int64 = 0x6367_6574_6c2d_7074

// Connect opens a pgx pool and verifies connectivity with a ping.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// Migrate applies all embedded goose migrations. A Postgres session-level
// advisory lock makes concurrent callers wait instead of racing.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("pg: goose locker: %w", err)
	}
	// goose works on database/sql; wrap the pool. Closing db does not close pool.
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("pg: goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("pg: migrate up: %w", err)
	}
	return nil
}

// EnsurePartitions creates the daily partitions of raw.api_calls and
// pipeline.dedup_claims for every UTC day in [today-1, today+daysAhead].
// It is idempotent and safe to call concurrently.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, daysAhead int) error {
	if daysAhead < 0 {
		return errors.New("pg: daysAhead must be >= 0")
	}
	u := now.UTC()
	today := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)

	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey); err != nil {
			return fmt.Errorf("pg: partition lock: %w", err)
		}
		for d := -1; d <= daysAhead; d++ {
			for _, stmt := range partitionDDL(today.AddDate(0, 0, d)) {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("pg: create partition: %w", err)
				}
			}
		}
		return nil
	})
}

// partitionDDL returns the CREATE statements for one UTC day.
// raw.api_calls is ranged on timestamptz, so bounds carry an explicit +00
// offset; otherwise the session TimeZone would shift the day boundaries.
func partitionDDL(day time.Time) []string {
	next := day.AddDate(0, 0, 1)
	suffix := day.Format("20060102")
	from, to := day.Format("2006-01-02"), next.Format("2006-01-02")
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS raw.api_calls_p%s PARTITION OF raw.api_calls
  FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')`, suffix, from, to),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS pipeline.dedup_claims_p%s PARTITION OF pipeline.dedup_claims
  FOR VALUES FROM ('%s') TO ('%s')`, suffix, from, to),
	}
}
