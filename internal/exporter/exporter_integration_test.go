package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

type env struct {
	pool    *pgxpool.Pool
	s3      *s3.Client
	reg     *validation.Registry
	m       *obs.Metrics
	log     *slog.Logger
	dataDir string
	payload []byte
}

func setup(t *testing.T) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dsn := testutil.StartPostgres(t)
	endpoint, dataDir := testutil.StartVersityGW(t)

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pg.Connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("pg.Migrate: %v", err)
	}
	if err := pg.EnsurePartitions(ctx, pool, time.Now(), 3); err != nil {
		t.Fatalf("pg.EnsurePartitions: %v", err)
	}

	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets: %v", err)
	}

	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatalf("validation.Load: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join("..", "..", "schemas", "testdata", "coingecko", "markets_top10_t0.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return &env{
		pool: pool, s3: s3c, reg: reg, dataDir: dataDir, payload: bytes.TrimSpace(payload),
		m:   obs.NewMetrics(prometheus.NewRegistry()),
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

const insertSQL = `
INSERT INTO raw.api_calls (event_id, idempotency_key, schema_version, source, endpoint, request,
                           fetched_at, outcome, http, error, payload, payload_sha256)
VALUES ($1, $2, 1, $3, $4, $5::jsonb, $6, 'ok', $7::jsonb, NULL, $8::json, decode($9, 'hex'))
RETURNING seq`

func (e *env) okArgs(source, endpoint string) (eventID string, args []any) {
	return e.okArgsAt(source, endpoint, time.Now())
}

// okArgsAt is okArgs with an explicit fetched_at.
func (e *env) okArgsAt(source, endpoint string, fetchedAt time.Time) (eventID string, args []any) {
	eventID = uuid.Must(uuid.NewV7()).String()
	sha := event.SHA256Hex(e.payload)
	params := map[string]string{"vs_currency": "usd", "page": "1"}
	req, _ := json.Marshal(event.Request{Path: "/coins/markets", Params: params})
	httpMeta, _ := json.Marshal(event.HTTPMeta{Status: 200, LatencyMS: 90})
	return eventID, []any{eventID, event.IdempotencyKey(source, endpoint, params, sha), source, endpoint,
		string(req), fetchedAt.UTC(), string(httpMeta), string(e.payload), sha}
}

func (e *env) insertCommitted(t *testing.T, source, endpoint string) (string, int64) {
	t.Helper()
	return e.insertCommittedAt(t, source, endpoint, time.Now())
}

// insertCommittedAt inserts one committed ok row fetched at fetchedAt.
func (e *env) insertCommittedAt(t *testing.T, source, endpoint string, fetchedAt time.Time) (string, int64) {
	t.Helper()
	id, args := e.okArgsAt(source, endpoint, fetchedAt)
	var seq int64
	if err := e.pool.QueryRow(context.Background(), insertSQL, args...).Scan(&seq); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id, seq
}

// insertFailedCall inserts an http_error row and returns its event id.
func (e *env) insertFailedCall(t *testing.T) string {
	t.Helper()
	id := uuid.Must(uuid.NewV7()).String()
	_, err := e.pool.Exec(context.Background(), `
INSERT INTO raw.api_calls (event_id, schema_version, source, endpoint, request, fetched_at, outcome, http, error)
VALUES ($1, 1, 'coingecko', 'coins_markets', '{"path":"/coins/markets","params":{}}', now(), 'http_error',
        '{"status":429,"latency_ms":40,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":60}', 'HTTP 429')`,
		id)
	if err != nil {
		t.Fatalf("insert failed call: %v", err)
	}
	return id
}

// batchOf returns raw.api_calls.export_batch_id of one row (nil = not exported).
func (e *env) batchOf(t *testing.T, eventID string) *int64 {
	t.Helper()
	var id *int64
	if err := e.pool.QueryRow(context.Background(),
		`SELECT export_batch_id FROM raw.api_calls WHERE event_id = $1`, eventID).Scan(&id); err != nil {
		t.Fatalf("export_batch_id of %s: %v", eventID, err)
	}
	return id
}

// objectEventIDs returns the event ids of the lines of one raw object.
func (e *env) objectEventIDs(t *testing.T, key string) []string {
	t.Helper()
	body, err := e.s3.Get(context.Background(), s3.BucketRaw, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	var ids []string
	for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
		if line == "" {
			continue
		}
		ev, err := event.Unmarshal([]byte(line))
		if err != nil {
			t.Fatalf("unmarshal line of %s: %v", key, err)
		}
		ids = append(ids, ev.EventID)
	}
	return ids
}

func (e *env) exportOnce(t *testing.T, max int) int {
	t.Helper()
	n, err := ExportOnce(context.Background(), e.pool, e.s3, e.reg, e.m, e.log, max)
	if err != nil {
		t.Fatalf("ExportOnce: %v", err)
	}
	return n
}

type ledgerRow struct {
	id     int64
	key    string
	status string
	sha    []byte
	rows   int
}

func (e *env) ledger(t *testing.T) []ledgerRow {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `SELECT batch_id, object_key, status, content_sha256, row_count FROM pipeline.export_batches ORDER BY batch_id`)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	defer rows.Close()
	var out []ledgerRow
	for rows.Next() {
		var r ledgerRow
		if err := rows.Scan(&r.id, &r.key, &r.status, &r.sha, &r.rows); err != nil {
			t.Fatalf("ledger scan: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// exportedEventIDs reads every raw object named in the ledger and returns the
// event ids of all lines in order, checking object bytes, checksums and that
// every ledger row_count equals the rows stamped with its batch id.
func (e *env) exportedEventIDs(t *testing.T) []string {
	t.Helper()
	keyRe := regexp.MustCompile(`^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/batch=\d{20}\.jsonl$`)
	var ids []string
	for _, l := range e.ledger(t) {
		if l.status != "exported" {
			t.Fatalf("batch %d status %q, want exported", l.id, l.status)
		}
		var stamped int
		if err := e.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM raw.api_calls WHERE export_batch_id = $1`, l.id).Scan(&stamped); err != nil {
			t.Fatalf("count stamped rows of batch %d: %v", l.id, err)
		}
		if stamped != l.rows {
			t.Fatalf("batch %d: row_count %d, but %d rows carry its export_batch_id", l.id, l.rows, stamped)
		}
		if !keyRe.MatchString(l.key) {
			t.Fatalf("object key %q does not match the raw key layout", l.key)
		}
		body, err := e.s3.Get(context.Background(), s3.BucketRaw, l.key)
		if err != nil {
			t.Fatalf("get %s: %v", l.key, err)
		}
		onDisk, err := os.ReadFile(filepath.Join(e.dataDir, s3.BucketRaw, filepath.FromSlash(l.key)))
		if err != nil || !bytes.Equal(onDisk, body) {
			t.Fatalf("plain file %s differs from S3 object (err=%v)", l.key, err)
		}
		if sum := sha256.Sum256(body); !bytes.Equal(sum[:], l.sha) {
			t.Fatalf("batch %d: content_sha256 does not match object bytes", l.id)
		}
		for _, line := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
			if line == "" {
				continue
			}
			if v, err := e.reg.Validate(validation.IDRawEvent, []byte(line)); err != nil || len(v) > 0 {
				t.Fatalf("exported line fails S1: err=%v violations=%v", err, v)
			}
			ev, err := event.Unmarshal([]byte(line))
			if err != nil {
				t.Fatalf("unmarshal line: %v", err)
			}
			if event.SHA256Hex(ev.Payload) != *ev.PayloadSHA256 {
				t.Fatalf("event %s: payload bytes do not match payload_sha256", ev.EventID)
			}
			ids = append(ids, ev.EventID)
		}
	}
	return ids
}

func TestExportOnceExportsEveryRowExactlyOnce(t *testing.T) {
	e := setup(t)
	want := map[string]bool{}
	for i := 0; i < 3; i++ {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
		want[id] = true
	}
	failedID := e.insertFailedCall(t) // outcome http_error: never exported

	if n := e.exportOnce(t, 2); n != 2 {
		t.Fatalf("first ExportOnce = %d, want 2", n)
	}
	if n := e.exportOnce(t, 2); n != 1 {
		t.Fatalf("second ExportOnce = %d, want 1", n)
	}
	if n := e.exportOnce(t, 2); n != 0 {
		t.Fatalf("third ExportOnce = %d, want 0", n)
	}

	ids := e.exportedEventIDs(t)
	if len(ids) != len(want) {
		t.Fatalf("exported %d lines, want %d", len(ids), len(want))
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected or duplicate event %s", id)
		}
		delete(want, id)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("created")); got != 2 {
		t.Fatalf("s3_put_total{created} = %v, want 2", got)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v after full export, want 0", got)
	}
	if b := e.batchOf(t, failedID); b != nil {
		t.Fatalf("http_error row stamped with batch %d, want NULL", *b)
	}
}

func TestExportOnceResumesAfterCrash(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	id1, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	id2, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)

	// Crash 1: the claim transaction committed, nothing was written to S3.
	b, ok, err := claimNext(ctx, e.pool, 500)
	if err != nil || !ok || b.RowCount != 2 {
		t.Fatalf("claimNext: ok=%v rows=%d err=%v", ok, b.RowCount, err)
	}
	for _, id := range []string{id1, id2} {
		if got := e.batchOf(t, id); got == nil || *got != b.ID {
			t.Fatalf("row %s export_batch_id = %v after the claim, want %d", id, got, b.ID)
		}
	}
	// A row inserted after the claim is not part of the claimed batch.
	id3, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	if n := e.exportOnce(t, 500); n != 3 {
		t.Fatalf("ExportOnce after claim-crash = %d, want 3 (resumed batch of 2 + a new batch of 1)", n)
	}
	l := e.ledger(t)
	if len(l) != 2 || l[0].id != b.ID || l[0].key != b.ObjectKey || l[0].status != "exported" || l[0].rows != 2 {
		t.Fatalf("ledger after resume = %+v, want the claimed batch %d exported with 2 rows, then one more", l, b.ID)
	}
	if got := e.objectEventIDs(t, b.ObjectKey); fmt.Sprint(got) != fmt.Sprint([]string{id1, id2}) {
		t.Fatalf("resumed batch holds %v, want [%s %s]", got, id1, id2)
	}
	if got := e.batchOf(t, id3); got == nil || *got != l[1].id {
		t.Fatalf("late row export_batch_id = %v, want the next batch %d", got, l[1].id)
	}
	first, err := e.s3.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Crash 2: the object was written but the batch was not marked exported.
	if _, err := e.pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'claimed', exported_at = NULL WHERE batch_id = $1`, b.ID); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}
	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce after put-crash = %d, want 2", n)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("exists")); got != 1 {
		t.Fatalf("s3_put_total{exists} = %v, want 1 (412 with equal checksum)", got)
	}
	again, err := e.s3.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil || !bytes.Equal(first, again) {
		t.Fatalf("object changed after retry (err=%v)", err)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 3 {
		t.Fatalf("exported %d lines, want 3", len(ids))
	}
}

// TestExportOnceExportsLateCommitInLaterBatch: transaction A inserts first
// but commits last. The claim must neither wait for A nor lose it: B goes
// into batch 1, A into batch 2 once it has committed.
func TestExportOnceExportsLateCommitInLaterBatch(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	txA, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer txA.Rollback(ctx)
	idA, argsA := e.okArgs(event.SourceCoinGecko, event.EndpointMarkets)
	var seqA int64
	if err := txA.QueryRow(ctx, insertSQL, argsA...).Scan(&seqA); err != nil {
		t.Fatalf("insert A: %v", err)
	}

	idB, seqB := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets) // tx B commits first
	if seqB <= seqA {
		t.Fatalf("seq B (%d) should be greater than seq A (%d)", seqB, seqA)
	}

	if n := e.exportOnce(t, 500); n != 1 {
		t.Fatalf("ExportOnce with A open = %d, want 1 (B only)", n)
	}
	l := e.ledger(t)
	if len(l) != 1 {
		t.Fatalf("ledger with A open = %+v, want one batch", l)
	}
	if got := e.objectEventIDs(t, l[0].key); fmt.Sprint(got) != fmt.Sprint([]string{idB}) {
		t.Fatalf("batch 1 holds %v, want only B=%s", got, idB)
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if n := e.exportOnce(t, 500); n != 1 {
		t.Fatalf("ExportOnce after A committed = %d, want 1 (A only)", n)
	}
	l = e.ledger(t)
	if len(l) != 2 {
		t.Fatalf("ledger after A committed = %+v, want two batches", l)
	}
	if got := e.objectEventIDs(t, l[1].key); fmt.Sprint(got) != fmt.Sprint([]string{idA}) {
		t.Fatalf("batch 2 holds %v, want only A=%s", got, idA)
	}
	ids := e.exportedEventIDs(t)
	if len(ids) != 2 || ids[0] != idB || ids[1] != idA {
		t.Fatalf("exported %v, want [B=%s A=%s]", ids, idB, idA)
	}
	if n := e.exportOnce(t, 500); n != 0 {
		t.Fatalf("extra ExportOnce = %d, want 0", n)
	}
}

// TestExportOnceNotStalledByOpenTransaction: a session that holds a
// transaction id open does not hold back rows committed after it started.
func TestExportOnceNotStalledByOpenTransaction(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	open, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer open.Rollback(ctx)
	if _, err := open.Exec(ctx, `SELECT pg_current_xact_id()`); err != nil {
		t.Fatalf("assign xid: %v", err)
	}

	want := map[string]bool{}
	for i := 0; i < 3; i++ {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
		want[id] = true
	}
	if n := e.exportOnce(t, 500); n != 3 {
		t.Fatalf("ExportOnce with an open transaction = %d, want 3", n)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v with the transaction still open, want 0", got)
	}
	ids := e.exportedEventIDs(t)
	if len(ids) != 3 {
		t.Fatalf("exported %v, want 3 rows", ids)
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected or duplicate event %s", id)
		}
		delete(want, id)
	}
}

// TestConcurrentClaimsAreDisjoint: a claim never waits for rows another
// claim holds (SKIP LOCKED), and the two batches share no row.
func TestConcurrentClaimsAreDisjoint(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	}
	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)
	b1, ok, err := claimIn(ctx, tx1, 3) // rows locked, not committed
	if err != nil || !ok || b1.RowCount != 3 {
		t.Fatalf("claimIn: %v %v %+v", err, ok, b1)
	}

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	b2, ok, err := claimNext(dctx, e.pool, 500) // SKIP LOCKED: must not wait for tx1
	if took := time.Since(start); err != nil || !ok || b2.RowCount != 3 || took > 2*time.Second {
		t.Fatalf("claimNext: %v %v %+v after %v", err, ok, b2, took)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := e.pool.Query(ctx,
		`SELECT event_id::text, export_batch_id FROM raw.api_calls WHERE export_batch_id IN ($1, $2)`, b1.ID, b2.ID)
	if err != nil {
		t.Fatal(err)
	}
	owner := map[string]int64{}
	perBatch := map[int64]int{}
	for rows.Next() {
		var id string
		var batchID int64
		if err := rows.Scan(&id, &batchID); err != nil {
			t.Fatal(err)
		}
		if prev, dup := owner[id]; dup {
			t.Fatalf("row %s stamped by batches %d and %d", id, prev, batchID)
		}
		owner[id] = batchID
		perBatch[batchID]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(owner) != 6 || perBatch[b1.ID] != 3 || perBatch[b2.ID] != 3 {
		t.Fatalf("stamped rows per batch = %v (%d rows), want 3 for %d and 3 for %d", perBatch, len(owner), b1.ID, b2.ID)
	}

	if n := e.exportOnce(t, 500); n != 6 {
		t.Fatalf("ExportOnce after both claims = %d, want 6 (two resumed batches)", n)
	}
	ids := e.exportedEventIDs(t)
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("event %s exported twice", id)
		}
		seen[id] = true
	}
	if len(seen) != 6 {
		t.Fatalf("exported %d unique rows, want 6", len(seen))
	}
}

// TestExportsRowInPartitionCreatedAfterClaim: the partial indexes of the
// parent are cloned into partitions created later, and the claim finds rows
// there.
func TestExportsRowInPartitionCreatedAfterClaim(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	if _, ok, err := claimNext(ctx, e.pool, 500); err != nil || !ok {
		t.Fatalf("claimNext: ok=%v err=%v", ok, err)
	}

	later := time.Now().UTC().AddDate(0, 0, 10)
	if err := pg.EnsurePartitions(ctx, e.pool, later, 0); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}
	e.insertCommittedAt(t, event.SourceCoinGecko, event.EndpointMarkets, later)
	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2 (resumed batch + the row in the new partition)", n)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 2 {
		t.Fatalf("exported %v, want 2 rows", ids)
	}

	var n int
	if err := e.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_indexes
 WHERE schemaname = 'raw' AND tablename = $1 AND indexdef LIKE '%export_batch_id%'`,
		"api_calls_p"+later.Format("20060102")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("new partition has %d indexes on export_batch_id, want 2", n)
	}
}

// TestExportLagGrowsWhileClaimedBatchIsStuck: rows of a claimed batch that
// keeps failing still count in the lag, even after newer rows arrive.
func TestExportLagGrowsWhileClaimedBatchIsStuck(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommittedAt(t, event.SourceCoinGecko, event.EndpointMarkets, time.Now().Add(-10*time.Minute))
	b, ok, err := claimNext(ctx, e.pool, 500)
	if err != nil || !ok {
		t.Fatalf("claimNext: ok=%v err=%v", ok, err)
	}
	if _, err := e.s3.PutIfAbsent(ctx, s3.BucketRaw, b.ObjectKey, []byte("other bytes\n")); err != nil {
		t.Fatalf("put other bytes: %v", err)
	}

	for i, insertFresh := range []bool{false, true} {
		if insertFresh {
			e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
		}
		_, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, 500)
		if !errors.Is(err, s3.ErrChecksumMismatch) {
			t.Fatalf("ExportOnce #%d = %v, want a checksum mismatch", i+1, err)
		}
		if got := promtest.ToFloat64(e.m.ExportLag); got < 590 {
			t.Fatalf("export lag after ExportOnce #%d = %v, want >= 590 (the stuck batch's row is 10 minutes old)", i+1, got)
		}
	}
	l := e.ledger(t)
	if len(l) != 1 || l[0].id != b.ID || l[0].status != "claimed" {
		t.Fatalf("ledger = %+v, want only batch %d, still claimed", l, b.ID)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("mismatch")); got < 1 {
		t.Fatalf("s3_put_total{mismatch} = %v, want >= 1", got)
	}
}

// TestExportOnceBatchesInSeqOrder: batches take the oldest unexported rows
// first, at most max per batch.
func TestExportOnceBatchesInSeqOrder(t *testing.T) {
	e := setup(t)
	var want []string
	for i := 0; i < 9; i++ {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
		want = append(want, id)
	}
	for i, n := range []int{4, 4, 1, 0} {
		if got := e.exportOnce(t, 4); got != n {
			t.Fatalf("ExportOnce #%d = %d, want %d", i+1, got, n)
		}
	}
	if l := e.ledger(t); len(l) != 3 {
		t.Fatalf("ledger has %d batches, want 3: %+v", len(l), l)
	}
	if got := e.exportedEventIDs(t); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("exported %v, want %v in insert order", got, want)
	}
}

func TestExportOnceSendsInvalidRowsToDLQ(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	goodID, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	// The table has no CHECK on source/endpoint, S1 does: this row is invalid.
	badID, badSeq := e.insertCommitted(t, "not-coingecko", "bogus_endpoint")

	if n := e.exportOnce(t, 500); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2", n)
	}
	ids := e.exportedEventIDs(t)
	if len(ids) != 1 || ids[0] != goodID {
		t.Fatalf("raw object holds %v, want only %s", ids, goodID)
	}

	l := e.ledger(t)
	var claimedAt time.Time
	if err := e.pool.QueryRow(ctx, `SELECT claimed_at FROM pipeline.export_batches WHERE batch_id = $1`, l[0].id).Scan(&claimedAt); err != nil {
		t.Fatalf("claimed_at: %v", err)
	}
	key := fmt.Sprintf("stage=export/date=%s/export+batch+%020d.jsonl", xtime.FormatDate(claimedAt), l[0].id)
	body, err := e.s3.Get(ctx, s3.BucketDLQ, key)
	if err != nil {
		t.Fatalf("get dlq object %s: %v", key, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("dlq object has %d lines, want 1", len(lines))
	}
	if v, err := e.reg.Validate(validation.IDDLQRecord, []byte(lines[0])); err != nil || len(v) > 0 {
		t.Fatalf("dlq line fails S3 dlq_record schema: err=%v violations=%v", err, v)
	}
	var rec struct {
		Stage      string                 `json:"stage"`
		ErrorClass string                 `json:"error_class"`
		Violations []validation.Violation `json:"violations"`
		Origin     struct {
			Kind    string `json:"kind"`
			BatchID int64  `json:"batch_id"`
			Seq     int64  `json:"seq"`
			EventID string `json:"event_id"`
		} `json:"origin"`
		OriginalBase64 string `json:"original_base64"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("dlq line: %v", err)
	}
	if rec.Stage != "export" || rec.ErrorClass != "schema_violation" || len(rec.Violations) == 0 ||
		rec.Origin.Kind != "postgres" || rec.Origin.Seq != badSeq || rec.Origin.BatchID != l[0].id || rec.Origin.EventID != badID {
		t.Fatalf("dlq record = %+v, want stage export, schema_violation, origin postgres batch %d seq %d event %s", rec, l[0].id, badSeq, badID)
	}
	original, err := base64.StdEncoding.DecodeString(rec.OriginalBase64)
	if err != nil {
		t.Fatalf("original_base64: %v", err)
	}
	if ev, err := event.Unmarshal(original); err != nil || ev.EventID != badID || ev.Source != "not-coingecko" {
		t.Fatalf("original line does not decode to the bad event: ev=%+v err=%v", ev, err)
	}
	if got := promtest.ToFloat64(e.m.DLQ.WithLabelValues("export", "schema_violation")); got != 1 {
		t.Fatalf("dlq_total = %v, want 1", got)
	}
	// The bad row belongs to the batch that sent it to the DLQ: never claimed again.
	if got := e.batchOf(t, badID); got == nil || *got != l[0].id {
		t.Fatalf("bad row export_batch_id = %v, want %d", got, l[0].id)
	}
	if n := e.exportOnce(t, 500); n != 0 {
		t.Fatalf("ExportOnce after the DLQ batch = %d, want 0", n)
	}
}

// TestExportOnceCountsClaimFailures: a claim that fails never reaches
// exportBatch's counter, so ExportOnce counts it.
func TestExportOnceCountsClaimFailures(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)
	// The ledger stays readable, the claim's row pick fails.
	if _, err := e.pool.Exec(ctx, `ALTER TABLE raw.api_calls RENAME TO api_calls_gone`); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, 500); err == nil || !strings.Contains(err.Error(), "pick rows") {
		t.Fatalf("ExportOnce without raw.api_calls = %v, want a pick rows error", err)
	}
	if got := promtest.ToFloat64(e.m.ExportBatches.WithLabelValues("error")); got != 1 {
		t.Fatalf("export_batches_total{error} = %v after a failed claim, want 1", got)
	}
}

func TestRunExportsAndStopsOnCancel(t *testing.T) {
	e := setup(t)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets)

	cfg := config.Config{ExportInterval: 50 * time.Millisecond, ExportBatchMax: 500}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		l := e.ledger(t)
		if len(l) == 1 && l[0].status == "exported" {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("Run did not export within 20s; ledger=%+v", l)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestRunFailsWhenLockHeld(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	holder, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock(hashtext('etl.exporter'))`); err != nil {
		t.Fatalf("hold lock: %v", err)
	}
	defer holder.Exec(ctx, `SELECT pg_advisory_unlock(hashtext('etl.exporter'))`)

	oldAttempts, oldDelay := lockAttempts, lockRetryDelay
	lockAttempts, lockRetryDelay = 2, 10*time.Millisecond
	defer func() { lockAttempts, lockRetryDelay = oldAttempts, oldDelay }()

	cfg := config.Config{ExportInterval: time.Second, ExportBatchMax: 500}
	err = Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log)
	if err == nil || !strings.Contains(err.Error(), "advisory lock") {
		t.Fatalf("Run with lock held = %v, want advisory lock error", err)
	}
}
