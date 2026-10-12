// Package pg owns the Postgres connection pool, schema migrations and
// partition maintenance for the raw and pipeline schemas: creating daily
// partitions ahead and dropping expired dedup_claims partitions (retention).
package pg

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"coingecko-etl/migrations"
)

// partitionLockKey serializes partition DDL across processes: EnsurePartitions
// (init, fetcher maintenance loop, insert-time repair) and DropClaimPartition
// (etl maintenance retention).
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

// ClaimPartition is one daily partition of pipeline.dedup_claims, with its
// catalog bounds [From, To) as UTC dates.
type ClaimPartition struct {
	Schema, Name string
	From, To     time.Time
}

// QualifiedName returns the quoted schema.table name, safe to put into SQL.
func (p ClaimPartition) QualifiedName() string { return pgx.Identifier{p.Schema, p.Name}.Sanitize() }

// listClaimPartitionsSQL reads each partition's bound from the catalog. A
// single FROM/TO date range matches the regexp; DEFAULT (and anything else)
// gives NULL bounds.
const listClaimPartitionsSQL = `SELECT n.nspname, c.relname, m[1], m[2]
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN LATERAL regexp_match(pg_get_expr(c.relpartbound, c.oid),
    '^FOR VALUES FROM \(''(\d{4}-\d{2}-\d{2})''\) TO \(''(\d{4}-\d{2}-\d{2})''\)$') AS m ON true
 WHERE i.inhparent = 'pipeline.dedup_claims'::regclass
 ORDER BY c.relname`

// ListClaimPartitions returns the partitions of pipeline.dedup_claims whose
// bound is a single FROM/TO date range, ordered by To. Others (DEFAULT,
// multi-column) are returned as schema.name in skipped and never dropped.
// Bounds come from the catalog, so partition names do not matter.
func ListClaimPartitions(ctx context.Context, pool *pgxpool.Pool) (parts []ClaimPartition, skipped []string, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// pg_get_expr prints date literals in the session DateStyle.
		if _, err := tx.Exec(ctx, "SET LOCAL datestyle = 'ISO, YMD'"); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, listClaimPartitionsSQL)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var schema, name string
			var from, to *string
			if err := rows.Scan(&schema, &name, &from, &to); err != nil {
				return err
			}
			if from == nil || to == nil {
				skipped = append(skipped, schema+"."+name)
				continue
			}
			p := ClaimPartition{Schema: schema, Name: name}
			if p.From, err = time.Parse(time.DateOnly, *from); err != nil {
				return fmt.Errorf("partition %s.%s: %w", schema, name, err)
			}
			if p.To, err = time.Parse(time.DateOnly, *to); err != nil {
				return fmt.Errorf("partition %s.%s: %w", schema, name, err)
			}
			parts = append(parts, p)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pg: list claims partitions: %w", err)
	}
	slices.SortStableFunc(parts, func(a, b ClaimPartition) int { return a.To.Compare(b.To) })
	return parts, skipped, nil
}

// DropClaimPartition drops p in its own short transaction: lock_timeout, then
// the partition advisory lock (serializes with EnsurePartitions), then DROP
// TABLE IF EXISTS. The DROP needs an AccessExclusive lock on the parent
// pipeline.dedup_claims, and while it waits every new claim insert queues
// behind it; lock_timeout bounds that wait. A wait longer than lockTimeout
// returns an error for which IsLockTimeout is true.
func DropClaimPartition(ctx context.Context, pool *pgxpool.Pool, p ClaimPartition, lockTimeout time.Duration) error {
	if lockTimeout < time.Millisecond { // lock_timeout = 0 would wait forever
		return fmt.Errorf("pg: drop claims partition: lock timeout %v is below 1ms", lockTimeout)
	}
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// SET LOCAL takes no bind parameters; set_config(..., true) is its form that does.
		ms := strconv.FormatInt(lockTimeout.Milliseconds(), 10) + "ms"
		if _, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", ms); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", partitionLockKey); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+p.QualifiedName())
		return err
	})
	if err != nil {
		return fmt.Errorf("pg: drop claims partition %s: %w", p.QualifiedName(), err)
	}
	return nil
}

// IsLockTimeout reports SQLSTATE 55P03 (lock_not_available), which
// lock_timeout raises.
func IsLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}
