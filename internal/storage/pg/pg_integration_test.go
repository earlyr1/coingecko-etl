package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/testutil"
)

// TestPostgresIntegration shares one postgres:18.6 container across subtests.
func TestPostgresIntegration(t *testing.T) {
	dsn := testutil.StartPostgres(t)
	ctx := context.Background()

	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	t.Run("MigrateTwiceIsIdempotent", func(t *testing.T) {
		for i := range 2 {
			if err := Migrate(ctx, pool); err != nil {
				t.Fatalf("migrate #%d: %v", i+1, err)
			}
		}
		var version int64
		if err := pool.QueryRow(ctx,
			`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("goose version = %d, want 2", version)
		}
		for _, tbl := range []string{"raw.api_calls", "pipeline.export_batches",
			"pipeline.dedup_claims", "pipeline.consumer_batches"} {
			var ok bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&ok); err != nil || !ok {
				t.Errorf("table %s missing (err=%v)", tbl, err)
			}
		}
		// payload must be json (byte-exact), never jsonb.
		var typ string
		if err := pool.QueryRow(ctx, `SELECT data_type FROM information_schema.columns
			WHERE table_schema='raw' AND table_name='api_calls' AND column_name='payload'`).Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "json" {
			t.Errorf("payload type = %s, want json", typ)
		}
		// Exact column sets: the exporter's claim column is export_batch_id,
		// and no column of an earlier export design is left behind.
		columns := func(schema, table string) string {
			var cols string
			if err := pool.QueryRow(ctx, `SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position)
				FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`, schema, table).Scan(&cols); err != nil {
				t.Fatal(err)
			}
			return cols
		}
		if got, want := columns("raw", "api_calls"), "seq bigint, event_id uuid, idempotency_key text, schema_version smallint, "+
			"source text, endpoint text, request jsonb, fetched_at timestamp with time zone, outcome text, http jsonb, "+
			"error text, payload json, payload_sha256 bytea, export_batch_id bigint"; got != want {
			t.Errorf("raw.api_calls columns =\n  %s\nwant\n  %s", got, want)
		}
		if got, want := columns("pipeline", "export_batches"), "batch_id bigint, row_count integer, "+
			"min_fetched_at timestamp with time zone, max_fetched_at timestamp with time zone, object_key text, "+
			"content_sha256 bytea, status text, claimed_at timestamp with time zone, exported_at timestamp with time zone, "+
			"publish_lease_until timestamp with time zone, published_at timestamp with time zone"; got != want {
			t.Errorf("pipeline.export_batches columns =\n  %s\nwant\n  %s", got, want)
		}
		for _, idx := range []string{"api_calls_unexported_idx", "api_calls_export_batch_idx"} {
			var def string
			if err := pool.QueryRow(ctx, `SELECT indexdef FROM pg_indexes
				WHERE schemaname = 'raw' AND tablename = 'api_calls' AND indexname = $1`, idx).Scan(&def); err != nil {
				t.Errorf("index %s on raw.api_calls: %v", idx, err)
			}
		}
		var fk string
		if err := pool.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conname = 'api_calls_export_batch_fk' AND conrelid = 'raw.api_calls'::regclass`).Scan(&fk); err != nil {
			t.Errorf("constraint api_calls_export_batch_fk: %v", err)
		} else if !strings.Contains(fk, "REFERENCES pipeline.export_batches(batch_id)") {
			t.Errorf("api_calls_export_batch_fk = %s, want a reference to pipeline.export_batches(batch_id)", fk)
		}
	})

	t.Run("DatabaseTimeoutsApplyToNewSessions", func(t *testing.T) {
		// ALTER DATABASE ... SET only affects sessions opened afterwards.
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		var idle, txn string
		if err := conn.QueryRow(ctx, `SHOW idle_in_transaction_session_timeout`).Scan(&idle); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SHOW transaction_timeout`).Scan(&txn); err != nil {
			t.Fatal(err)
		}
		if idle != "1min" || txn != "5min" {
			t.Errorf("idle_in_transaction_session_timeout=%s transaction_timeout=%s, want 1min/5min", idle, txn)
		}
	})

	now := time.Date(2026, 10, 10, 23, 30, 0, 0, time.UTC)

	t.Run("EnsurePartitionsCreatesDailyPartitions", func(t *testing.T) {
		for range 2 { // idempotent
			if err := EnsurePartitions(ctx, pool, now, 3); err != nil {
				t.Fatalf("ensure partitions: %v", err)
			}
		}
		want := []string{"20261009", "20261010", "20261011", "20261012", "20261013"}
		for _, parent := range []string{"raw.api_calls", "pipeline.dedup_claims"} {
			got := partitionsOf(t, pool, parent)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s partitions = %v, want %v", parent, got, want)
			}
		}
		// Day boundaries are UTC regardless of the session TimeZone.
		var bound string
		if err := pool.QueryRow(ctx, `SELECT pg_get_expr(c.relpartbound, c.oid)
			FROM pg_class c WHERE c.oid = 'raw.api_calls_p20261010'::regclass`).Scan(&bound); err != nil {
			t.Fatal(err)
		}
		if bound != "FOR VALUES FROM ('2026-10-10 00:00:00+00') TO ('2026-10-11 00:00:00+00')" {
			t.Errorf("bound = %s", bound)
		}
	})

	t.Run("InsertSameEventTwiceStoresOneRow", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2026-10-10T10:07:55.123Z"
		for range 2 {
			if err := InsertAPICall(ctx, pool, ev); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw.api_calls WHERE event_id = $1`,
			uuid.MustParse(ev.EventID)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("rows = %d, want 1", n)
		}
	})

	t.Run("PayloadStoredByteExact", func(t *testing.T) {
		raw, err := os.ReadFile("../../../schemas/testdata/coingecko/markets_top10_t0.json")
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2026-10-10T10:08:25.000Z"
		ev.PayloadSHA256 = strp(hex.EncodeToString(sum[:]))
		ev.Payload = json.RawMessage(raw)
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var text string
		var storedSHA []byte
		var fetched time.Time
		var reqPath string
		if err := pool.QueryRow(ctx, `SELECT payload::text, payload_sha256, fetched_at, request->>'path'
			FROM raw.api_calls WHERE event_id = $1`, uuid.MustParse(ev.EventID)).
			Scan(&text, &storedSHA, &fetched, &reqPath); err != nil {
			t.Fatal(err)
		}
		if text != string(raw) {
			t.Fatalf("payload not byte-exact: got %d bytes, want %d", len(text), len(raw))
		}
		if hex.EncodeToString(storedSHA) != *ev.PayloadSHA256 {
			t.Errorf("payload_sha256 = %x", storedSHA)
		}
		if !fetched.Equal(time.Date(2026, 10, 10, 10, 8, 25, 0, time.UTC)) {
			t.Errorf("fetched_at = %v", fetched)
		}
		if reqPath != "/coins/markets" {
			t.Errorf("request.path = %s", reqPath)
		}
	})

	t.Run("FailedCallStoredWithoutPayload", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.Outcome = event.OutcomeHTTPError
		ev.IdempotencyKey = nil
		ev.HTTP = &event.HTTPMeta{Status: 429, LatencyMS: 80}
		ev.Error = strp("http 429")
		ev.PayloadSHA256 = nil
		ev.Payload = nil
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var isNull bool
		var status int
		if err := pool.QueryRow(ctx, `SELECT payload IS NULL, (http->>'status')::int
			FROM raw.api_calls WHERE event_id = $1`, uuid.MustParse(ev.EventID)).Scan(&isNull, &status); err != nil {
			t.Fatal(err)
		}
		if !isNull || status != 429 {
			t.Errorf("payload null=%v status=%d", isNull, status)
		}
	})

	t.Run("CheckRejectsOKWithoutPayload", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.Payload = nil
		err := InsertAPICall(ctx, pool, ev)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "api_calls_payload_iff_ok" {
			t.Fatalf("err = %v, want CHECK api_calls_payload_iff_ok violation", err)
		}
	})

	t.Run("InsertRepairsMissingPartition", func(t *testing.T) {
		ev := okEvent()
		ev.EventID = uuid.Must(uuid.NewV7()).String()
		ev.FetchedAt = "2027-03-01T12:00:00.000Z" // far outside the ensured window
		if err := InsertAPICall(ctx, pool, ev); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('raw.api_calls_p20270301') IS NOT NULL`).Scan(&ok); err != nil || !ok {
			t.Fatalf("partition raw.api_calls_p20270301 not created (err=%v)", err)
		}
	})

	t.Run("ConsumerBatchesRejectOverlap", func(t *testing.T) {
		topic := uuid.Must(uuid.NewV7())
		if _, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 0, 100, 199)`, topic); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 0, 150, 260)`, topic)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23P01" { // exclusion_violation
			t.Fatalf("err = %v, want exclusion violation", err)
		}
		// Same range on another partition is fine.
		if _, err := pool.Exec(ctx, `INSERT INTO pipeline.consumer_batches
			(topic_id, kafka_partition, first_offset, last_offset) VALUES ($1, 1, 150, 260)`, topic); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("DedupClaimsAcceptRowsInEnsuredPartition", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO pipeline.dedup_claims
			(dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal,
			 event_id, idempotency_key, content_sha256)
			VALUES ('market_snapshot', 'bitcoin|2026-10-10T10:07:55.000Z', '2026-10-10', $1, 0, 5, 0, $2, $3, $4)`,
			uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), strings.Repeat("ab", 32), make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
	})
}

func partitionsOf(t *testing.T, pool *pgxpool.Pool, parent string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass ORDER BY c.relname`, parent)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, n[strings.LastIndex(n, "_p")+2:])
	}
	return out
}
