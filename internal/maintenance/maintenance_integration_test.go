package maintenance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"
)

// TestMaintenance shares one postgres:18.6 container across subtests, which
// run in order: each one builds on the state the previous ones left.
func TestMaintenance(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx := context.Background()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 10, 12, 11, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC) // now's UTC date - 8 days
	opt := RetentionOptions{Days: 8, LockTimeout: time.Second, Attempts: 2, RetryDelay: 10 * time.Millisecond}
	quiet := slog.New(slog.DiscardHandler)

	// dedup_claims and raw.api_calls partitions for 2026-10-02..05 and 2026-10-11..13.
	for _, p := range []struct {
		day   time.Time
		ahead int
	}{{time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC), 2}, {now, 1}} {
		if err := pg.EnsureClaimPartitions(ctx, pool, p.day, p.ahead); err != nil {
			t.Fatal(err)
		}
		if err := pg.EnsureRawPartitions(ctx, pool, p.day, p.ahead); err != nil {
			t.Fatal(err)
		}
	}
	exec(t, pool, `CREATE TABLE pipeline.claims_legacy PARTITION OF pipeline.dedup_claims
		FOR VALUES FROM ('2026-09-01') TO ('2026-09-02')`)
	insertClaim(t, pool, "2026-10-04")

	// consumer_batches: written just before, exactly at and after the cutoff,
	// and a planned row older than everything.
	topic := uuid.Must(uuid.NewV7())
	for i, b := range []struct {
		status    string
		plannedAt time.Time
		writtenAt *time.Time
	}{
		{"written", cutoff.Add(-time.Hour), ptr(cutoff.Add(-time.Second))},
		{"written", cutoff.Add(-time.Hour), ptr(cutoff)},
		{"written", cutoff, ptr(cutoff.Add(time.Hour))},
		{"planned", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), nil},
	} {
		exec(t, pool, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset, status, planned_at, written_at)
			VALUES ($1, 0, $2, $2, $3, $4, $5)`, topic, int64(i*10), b.status, b.plannedAt, b.writtenAt)
	}

	first, err := Retention(ctx, pool, now, opt, quiet)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}

	t.Run("retention drops only partitions whose upper bound <= cutoff", func(t *testing.T) {
		if !first.Cutoff.Equal(cutoff) {
			t.Errorf("Cutoff = %v, want %v", first.Cutoff, cutoff)
		}
		want := `"pipeline"."claims_legacy","pipeline"."dedup_claims_p20261002","pipeline"."dedup_claims_p20261003"`
		if got := strings.Join(first.DroppedPartitions, ","); got != want {
			t.Errorf("DroppedPartitions = %s, want %s", got, want)
		}
		for _, name := range []string{"claims_legacy", "dedup_claims_p20261002", "dedup_claims_p20261003"} {
			if exists(t, pool, "pipeline."+name) {
				t.Errorf("pipeline.%s still exists", name)
			}
		}
		for _, d := range []string{"20261004", "20261005", "20261011", "20261012", "20261013"} {
			if !exists(t, pool, "pipeline.dedup_claims_p"+d) {
				t.Errorf("pipeline.dedup_claims_p%s was dropped", d)
			}
		}
		if n := count(t, pool, `SELECT count(*) FROM pipeline.dedup_claims WHERE event_date = '2026-10-04'`); n != 1 {
			t.Errorf("claims on 2026-10-04 = %d, want 1", n)
		}
	})

	t.Run("retention never touches raw partitions", func(t *testing.T) {
		for _, d := range []string{"20261002", "20261003", "20261004"} {
			if !exists(t, pool, "raw.api_calls_p"+d) {
				t.Errorf("raw.api_calls_p%s was dropped", d)
			}
		}
	})

	t.Run("retention deletes only old written batches", func(t *testing.T) {
		if first.BatchesDeleted != 1 {
			t.Errorf("BatchesDeleted = %d, want 1", first.BatchesDeleted)
		}
		var offsets []int64
		rows, err := pool.Query(ctx, `SELECT first_offset FROM pipeline.consumer_batches ORDER BY first_offset`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var o int64
			if err := rows.Scan(&o); err != nil {
				t.Fatal(err)
			}
			offsets = append(offsets, o)
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		if len(offsets) != 3 || offsets[0] != 10 || offsets[1] != 20 || offsets[2] != 30 {
			t.Errorf("remaining batches (first_offset) = %v, want [10 20 30]", offsets)
		}
	})

	t.Run("retention is idempotent", func(t *testing.T) {
		res, err := Retention(ctx, pool, now, opt, quiet)
		if err != nil || len(res.DroppedPartitions) != 0 || res.BatchesDeleted != 0 {
			t.Fatalf("second run = %+v, %v; want nothing removed and no error", res, err)
		}
	})

	t.Run("retention refuses days < 2", func(t *testing.T) {
		res, err := Retention(ctx, pool, now, RetentionOptions{Days: 1, LockTimeout: time.Second, Attempts: 1}, quiet)
		if err == nil || len(res.DroppedPartitions) != 0 {
			t.Fatalf("Days 1 = %+v, %v; want an error and nothing dropped", res, err)
		}
		if !exists(t, pool, "pipeline.dedup_claims_p20261004") {
			t.Fatal("pipeline.dedup_claims_p20261004 was dropped")
		}
	})

	t.Run("busy partition fails after retries then succeeds", func(t *testing.T) {
		exec(t, pool, `CREATE TABLE pipeline.claims_busy PARTITION OF pipeline.dedup_claims
			FOR VALUES FROM ('2026-08-01') TO ('2026-08-02')`)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		insertClaim(t, tx, "2026-10-12") // RowExclusive on the parent until commit

		var buf bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&buf, nil))
		busy := RetentionOptions{Days: 8, LockTimeout: 200 * time.Millisecond, Attempts: 2, RetryDelay: 10 * time.Millisecond}
		res, err := Retention(ctx, pool, now, busy, log)
		if !pg.IsLockTimeout(err) {
			t.Fatalf("retention with a busy parent: %v, want a lock timeout", err)
		}
		if len(res.DroppedPartitions) != 0 || !exists(t, pool, "pipeline.claims_busy") {
			t.Fatalf("dropped %v despite the lock timeout", res.DroppedPartitions)
		}
		// One warning per retry: attempt 1 times out and is retried, attempt 2
		// times out and fails the run.
		if n := countLogs(t, &buf, "WARN", "claims partition busy, retrying"); n != busy.Attempts-1 {
			t.Errorf("busy warnings = %d, want %d:\n%s", n, busy.Attempts-1, buf.String())
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		res, err = Retention(ctx, pool, now, busy, quiet)
		if err != nil || strings.Join(res.DroppedPartitions, ",") != `"pipeline"."claims_busy"` {
			t.Fatalf("rerun = %+v, %v; want claims_busy dropped", res, err)
		}
	})

	today := "2026-10-12 10:00:00+00" // in raw.api_calls_p20261012

	t.Run("vacuum covers partitioned raw and pipeline tables", func(t *testing.T) {
		seedRaw(t, pool, 1000, today)
		topic := uuid.Must(uuid.NewV7())
		exec(t, pool, `INSERT INTO pipeline.consumer_batches (topic_id, kafka_partition, first_offset, last_offset)
			VALUES ($1, 0, 0, 0)`, topic)
		exec(t, pool, `DELETE FROM pipeline.consumer_batches WHERE topic_id = $1`, topic)

		var buf bytes.Buffer
		if err := Vacuum(ctx, pool, slog.New(slog.NewJSONHandler(&buf, nil))); err != nil {
			t.Fatalf("vacuum: %v", err)
		}
		if n := countLogs(t, &buf, "INFO", "vacuumed"); n != len(VacuumTables) {
			t.Errorf("vacuumed records = %d, want %d:\n%s", n, len(VacuumTables), buf.String())
		}
		// Table statistics reach pg_stat_user_tables asynchronously.
		deadline := time.Now().Add(5 * time.Second)
		for {
			missing := 0
			for _, rel := range []string{"raw.api_calls_p20261012", "pipeline.export_windows", "pipeline.export_batches", "pipeline.consumer_batches"} {
				if count(t, pool, `SELECT count(*) FROM pg_stat_user_tables
					WHERE relid = $1::regclass AND last_vacuum IS NOT NULL AND last_analyze IS NOT NULL`, rel) != 1 {
					missing++
				}
			}
			if missing == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d tables have no last_vacuum/last_analyze 5 s after Vacuum", missing)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("vacuum survives the database transaction_timeout", func(t *testing.T) {
		// A VACUUM longer than the database default transaction_timeout is
		// terminated (FATAL 25P04). Instead of seeding enough dead rows to make
		// VACUUM slow on any machine, an open transaction holds a lock VACUUM
		// waits for, and the wait counts towards transaction_timeout.
		locker, err := pool.Acquire(ctx) // a session opened under the 5min default
		if err != nil {
			t.Fatal(err)
		}
		defer locker.Release()
		exec(t, pool, `ALTER DATABASE etl SET transaction_timeout = '100ms'`)
		t.Cleanup(func() { exec(t, pool, `ALTER DATABASE etl SET transaction_timeout = '5min'`) })
		const holdLock = 500 * time.Millisecond // well past the 100ms default
		lock := func() pgx.Tx {
			t.Helper()
			tx, err := locker.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			exec(t, tx, `LOCK TABLE pipeline.consumer_batches IN SHARE UPDATE EXCLUSIVE MODE`)
			return tx
		}

		tx := lock()
		plain, err := pg.Connect(ctx, dsn) // a new session picks up the 100ms default
		if err != nil {
			t.Fatal(err)
		}
		_, err = plain.Exec(ctx, "VACUUM pipeline.consumer_batches")
		plain.Close()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25P04" {
			t.Fatalf("plain VACUUM behind a lock: %v, want SQLSTATE 25P04 (transaction_timeout)", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}

		fresh, err := pg.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer fresh.Close()
		tx = lock()
		done := make(chan error, 1)
		go func() { done <- Vacuum(ctx, fresh, quiet) }()
		// Start the hold only once Vacuum waits on the lock: the earlier
		// tables may take longer than holdLock on a slow machine.
		deadline := time.Now().Add(30 * time.Second)
		for count(t, pool, `SELECT count(*) FROM pg_stat_activity
			WHERE query = 'VACUUM (ANALYZE) pipeline.consumer_batches' AND wait_event_type = 'Lock'`) != 1 {
			select {
			case err := <-done:
				t.Fatalf("Vacuum returned %v while consumer_batches was locked", err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("Vacuum did not reach the lock on pipeline.consumer_batches within 30 s")
			}
			time.Sleep(20 * time.Millisecond)
		}
		select {
		case err := <-done:
			t.Fatalf("Vacuum returned %v while consumer_batches was locked", err)
		case <-time.After(holdLock):
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Vacuum after waiting %v under transaction_timeout = 100ms: %v", holdLock, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Vacuum did not finish within 30 s of the lock release")
		}
		// The hijacked connection never returns to the pool with its settings.
		var timeout string
		if err := fresh.QueryRow(ctx, `SHOW transaction_timeout`).Scan(&timeout); err != nil {
			t.Fatal(err)
		}
		if timeout != "100ms" {
			t.Errorf("pool session transaction_timeout = %s, want the database default 100ms", timeout)
		}
	})
}

func ptr[T any](v T) *T { return &v }

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func exec(t *testing.T, db execer, sql string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func insertClaim(t *testing.T, db execer, eventDate string) {
	t.Helper()
	exec(t, db, `INSERT INTO pipeline.dedup_claims
		(dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal, event_id, content_sha256)
		VALUES ('market_snapshot', $1, $2, $3, 0, 1, 0, $4, $5)`,
		uuid.NewString(), eventDate, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), make([]byte, 32))
}

// seedRaw inserts n http_error rows at fetchedAt and deletes half of them,
// leaving dead row versions for VACUUM.
func seedRaw(t *testing.T, pool *pgxpool.Pool, n int, fetchedAt string) {
	t.Helper()
	exec(t, pool, `INSERT INTO raw.api_calls
		(event_id, schema_version, source, endpoint, request, fetched_at, outcome, http, error)
		SELECT gen_random_uuid(), 1, 'coingecko', 'coins_markets', '{}', $2::timestamptz, 'http_error', '{}', 'x'
		  FROM generate_series(1, $1::int)`, n, fetchedAt)
	exec(t, pool, `DELETE FROM raw.api_calls WHERE seq % 2 = 0`)
}

func exists(t *testing.T, pool *pgxpool.Pool, rel string) bool {
	t.Helper()
	var ok bool
	if err := pool.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, rel).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// countLogs counts the JSON log records with this level and message.
func countLogs(t *testing.T, buf *bytes.Buffer, level, msg string) int {
	t.Helper()
	n := 0
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec struct{ Level, Msg string }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %s", line)
		}
		if rec.Level == level && rec.Msg == msg {
			n++
		}
	}
	return n
}
