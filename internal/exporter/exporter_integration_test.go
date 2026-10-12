package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

// base is the hour-aligned UTC start of the windows most tests use; the
// insert helpers create its partitions.
var base = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

// set5 is the default window setup: 5-minute windows, parts of up to 500 rows.
var set5 = Settings{Window: 5 * time.Minute, PartMax: 500}

// win5 returns the i-th 5-minute window after base.
func win5(i int) window.Window {
	start := base.Add(time.Duration(i) * 5 * time.Minute)
	return window.Window{Start: start, End: start.Add(5 * time.Minute)}
}

var (
	partKeyRe    = regexp.MustCompile(`^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/window=\d{4}/part-\d{5}\.jsonl$`)
	successKeyRe = regexp.MustCompile(`^source=coingecko/ingest_date=\d{4}-\d{2}-\d{2}/window=\d{4}/_SUCCESS$`)
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

// okArgs returns the insert arguments of one ok row fetched at fetchedAt and
// makes sure the partition of that day exists.
func (e *env) okArgs(t *testing.T, source, endpoint string, fetchedAt time.Time) (eventID string, args []any) {
	t.Helper()
	if err := pg.EnsurePartitions(context.Background(), e.pool, fetchedAt, 0); err != nil {
		t.Fatalf("EnsurePartitions(%s): %v", fetchedAt, err)
	}
	eventID = uuid.Must(uuid.NewV7()).String()
	sha := event.SHA256Hex(e.payload)
	params := map[string]string{"vs_currency": "usd", "page": "1"}
	req, _ := json.Marshal(event.Request{Path: "/coins/markets", Params: params})
	httpMeta, _ := json.Marshal(event.HTTPMeta{Status: 200, LatencyMS: 90})
	return eventID, []any{eventID, event.IdempotencyKey(source, endpoint, params, sha), source, endpoint,
		string(req), fetchedAt.UTC(), string(httpMeta), string(e.payload), sha}
}

// insertCommitted inserts one committed ok row fetched at fetchedAt.
func (e *env) insertCommitted(t *testing.T, source, endpoint string, fetchedAt time.Time) (string, int64) {
	t.Helper()
	id, args := e.okArgs(t, source, endpoint, fetchedAt)
	var seq int64
	if err := e.pool.QueryRow(context.Background(), insertSQL, args...).Scan(&seq); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id, seq
}

// insertFailedCall inserts an http_error row fetched at fetchedAt and returns its event id.
func (e *env) insertFailedCall(t *testing.T, fetchedAt time.Time) string {
	t.Helper()
	if err := pg.EnsurePartitions(context.Background(), e.pool, fetchedAt, 0); err != nil {
		t.Fatalf("EnsurePartitions(%s): %v", fetchedAt, err)
	}
	id := uuid.Must(uuid.NewV7()).String()
	_, err := e.pool.Exec(context.Background(), `
INSERT INTO raw.api_calls (event_id, schema_version, source, endpoint, request, fetched_at, outcome, http, error)
VALUES ($1, 1, 'coingecko', 'coins_markets', '{"path":"/coins/markets","params":{}}', $2, 'http_error',
        '{"status":429,"latency_ms":40,"date":null,"age_s":null,"cf_cache_status":null,"etag":null,"retry_after_s":60}', 'HTTP 429')`,
		id, fetchedAt.UTC())
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

// get returns one object of bucket raw.
func (e *env) get(t *testing.T, key string) []byte {
	t.Helper()
	body, err := e.s3.Get(context.Background(), s3.BucketRaw, key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return body
}

// objectEventIDs returns the event ids of the lines of one raw object.
func (e *env) objectEventIDs(t *testing.T, key string) []string {
	t.Helper()
	var ids []string
	for _, line := range lines(e.get(t, key)) {
		ev, err := event.Unmarshal([]byte(line))
		if err != nil {
			t.Fatalf("unmarshal line of %s: %v", key, err)
		}
		ids = append(ids, ev.EventID)
	}
	return ids
}

func lines(body []byte) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSuffix(string(body), "\n"), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func (e *env) exportOnce(t *testing.T, set Settings, now time.Time) int {
	t.Helper()
	n, err := ExportOnce(context.Background(), e.pool, e.s3, e.reg, e.m, e.log, set, now)
	if err != nil {
		t.Fatalf("ExportOnce(now=%s): %v", xtime.Format(now), err)
	}
	return n
}

type ledgerRow struct {
	id          int64
	windowStart time.Time
	part        int
	key         string
	status      string
	sha         []byte
	rows        int
	lines       *int
}

func (e *env) ledger(t *testing.T) []ledgerRow {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `
SELECT batch_id, window_start, part_index, object_key, status, content_sha256, row_count, line_count
  FROM pipeline.export_batches ORDER BY batch_id`)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	defer rows.Close()
	var out []ledgerRow
	for rows.Next() {
		var r ledgerRow
		if err := rows.Scan(&r.id, &r.windowStart, &r.part, &r.key, &r.status, &r.sha, &r.rows, &r.lines); err != nil {
			t.Fatalf("ledger scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	return out
}

type windowRow struct {
	w          window.Window
	status     string
	successKey string
	sha        []byte
	rows       *int
	parts      *int
}

func (e *env) windows(t *testing.T) []windowRow {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), `
SELECT window_start, window_end, status, success_key, success_sha256, row_count, part_count
  FROM pipeline.export_windows ORDER BY window_start`)
	if err != nil {
		t.Fatalf("windows: %v", err)
	}
	defer rows.Close()
	var out []windowRow
	for rows.Next() {
		var r windowRow
		if err := rows.Scan(&r.w.Start, &r.w.End, &r.status, &r.successKey, &r.sha, &r.rows, &r.parts); err != nil {
			t.Fatalf("windows scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("windows: %v", err)
	}
	return out
}

// exportedEventIDs walks pipeline.export_windows in order, reads each
// window's _SUCCESS and then the parts it lists, and returns the event ids of
// all lines in order. It checks the key layout, that _SUCCESS matches the
// sealed checksum and the part rows, that every part's bytes match its
// checksum, its plain file and its line count, that every ledger row_count
// equals the rows stamped with its batch id, that every line passes S1 and
// was fetched before its window's end, and that no part is outside a closed
// window.
func (e *env) exportedEventIDs(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	byWindow := map[time.Time][]ledgerRow{}
	ledger := e.ledger(t)
	for _, l := range ledger {
		byWindow[l.windowStart.UTC()] = append(byWindow[l.windowStart.UTC()], l)
	}
	var ids []string
	listed := 0
	for _, wr := range e.windows(t) {
		w := window.Window{Start: wr.w.Start.UTC(), End: wr.w.End.UTC()}
		if wr.status != statusClosed {
			t.Fatalf("window %s status %q, want closed", xtime.Format(w.Start), wr.status)
		}
		if wr.successKey != window.RawSuccessKey(w) || !successKeyRe.MatchString(wr.successKey) {
			t.Fatalf("window %s success_key %q does not match the layout", xtime.Format(w.Start), wr.successKey)
		}
		body := e.get(t, wr.successKey)
		if sum := sha256.Sum256(body); !bytes.Equal(sum[:], wr.sha) {
			t.Fatalf("window %s: success_sha256 does not match the _SUCCESS bytes", xtime.Format(w.Start))
		}
		man, err := window.ParseSuccess(body)
		if err != nil {
			t.Fatalf("window %s: %v", xtime.Format(w.Start), err)
		}
		if man.WindowStart != xtime.Format(w.Start) || man.WindowEnd != xtime.Format(w.End) ||
			*wr.rows != man.Rows || *wr.parts != len(man.Parts) {
			t.Fatalf("window %s: _SUCCESS %s does not match the window row %+v", xtime.Format(w.Start), body, wr)
		}
		parts := byWindow[w.Start]
		if len(parts) != len(man.Parts) {
			t.Fatalf("window %s: ledger has %d parts, _SUCCESS lists %d", xtime.Format(w.Start), len(parts), len(man.Parts))
		}
		listed += len(parts)
		for i, p := range man.Parts {
			l := parts[i]
			if !partKeyRe.MatchString(p.Key) || p.Key != window.RawPartKey(w, i) || l.key != p.Key || l.part != i {
				t.Fatalf("window %s part %d: key %q (ledger %q, part %d) does not match the layout", xtime.Format(w.Start), i, p.Key, l.key, l.part)
			}
			if l.status != "exported" || l.lines == nil || *l.lines != p.Rows || hex.EncodeToString(l.sha) != p.SHA256 {
				t.Fatalf("part %s: ledger %+v does not match the manifest %+v", p.Key, l, p)
			}
			var stamped int
			if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM raw.api_calls WHERE export_batch_id = $1`, l.id).Scan(&stamped); err != nil {
				t.Fatalf("count stamped rows of batch %d: %v", l.id, err)
			}
			if stamped != l.rows {
				t.Fatalf("batch %d: row_count %d, but %d rows carry its export_batch_id", l.id, l.rows, stamped)
			}
			pbody := e.get(t, p.Key)
			onDisk, err := os.ReadFile(filepath.Join(e.dataDir, s3.BucketRaw, filepath.FromSlash(p.Key)))
			if err != nil || !bytes.Equal(onDisk, pbody) {
				t.Fatalf("plain file %s differs from S3 object (err=%v)", p.Key, err)
			}
			if sum := sha256.Sum256(pbody); hex.EncodeToString(sum[:]) != p.SHA256 {
				t.Fatalf("part %s: sha256 in _SUCCESS does not match the object bytes", p.Key)
			}
			ls := lines(pbody)
			if len(ls) != p.Rows {
				t.Fatalf("part %s has %d lines, _SUCCESS says %d", p.Key, len(ls), p.Rows)
			}
			for _, line := range ls {
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
				if at, err := xtime.Parse(ev.FetchedAt); err != nil || !at.Before(w.End) {
					t.Fatalf("event %s fetched at %s is in window %s, which ends at %s", ev.EventID, ev.FetchedAt,
						xtime.Format(w.Start), xtime.Format(w.End))
				}
				ids = append(ids, ev.EventID)
			}
		}
	}
	if listed != len(ledger) {
		t.Fatalf("ledger has %d parts, closed windows list %d", len(ledger), listed)
	}
	return ids
}

// TestWindowExportWritesPartsThenSuccess: closing a window writes its part
// and then _SUCCESS with the exact manifest; a row of the next window waits.
func TestWindowExportWritesPartsThenSuccess(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	id1, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	id2, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(2*time.Minute))
	id3, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(6*time.Minute))

	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2", n)
	}
	w0 := win5(0)
	partKey := window.RawPartKey(w0, 0)
	if got := e.objectEventIDs(t, partKey); fmt.Sprint(got) != fmt.Sprint([]string{id1, id2}) {
		t.Fatalf("part holds %v, want [%s %s]", got, id1, id2)
	}
	part := e.get(t, partKey)
	sum := sha256.Sum256(part)
	want, err := window.NewSuccess(w0, []window.SuccessPart{{Key: partKey, SHA256: hex.EncodeToString(sum[:]), Rows: 2}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	success := e.get(t, window.RawSuccessKey(w0))
	if !bytes.Equal(success, want) {
		t.Fatalf("_SUCCESS =\n  %s\nwant\n  %s", success, want)
	}
	if b := e.batchOf(t, id3); b != nil {
		t.Fatalf("row of the next window stamped with batch %d", *b)
	}
	if _, err := e.s3.Get(ctx, s3.BucketRaw, window.RawSuccessKey(win5(1))); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("_SUCCESS of the next window: err=%v, want ErrNotFound", err)
	}
	ws := e.windows(t)
	successSum := sha256.Sum256(success)
	if len(ws) != 1 || !ws[0].w.Start.Equal(w0.Start) || ws[0].status != statusClosed || !bytes.Equal(ws[0].sha, successSum[:]) {
		t.Fatalf("export_windows = %+v, want window %s closed with the _SUCCESS sha256", ws, xtime.Format(w0.Start))
	}
	if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint([]string{id1, id2}) {
		t.Fatalf("exported %v", ids)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v, want 0", got)
	}
}

// TestWindowExportEmptyWindows: windows without rows get a _SUCCESS with no
// parts and no ledger row.
func TestWindowExportEmptyWindows(t *testing.T) {
	e := setup(t)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 1 {
		t.Fatalf("ExportOnce = %d, want 1", n)
	}
	if n := e.exportOnce(t, set5, base.Add(15*time.Minute+time.Second)); n != 0 {
		t.Fatalf("ExportOnce over empty windows = %d, want 0", n)
	}
	for _, w := range []window.Window{win5(1), win5(2)} {
		want := fmt.Sprintf(`{"window_start":"%s","window_end":"%s","rows":0,"parts":[]}`, xtime.Format(w.Start), xtime.Format(w.End))
		if got := string(e.get(t, window.RawSuccessKey(w))); got != want {
			t.Fatalf("_SUCCESS of %s = %s, want %s", xtime.Format(w.Start), got, want)
		}
		dir := filepath.Join(e.dataDir, s3.BucketRaw, filepath.FromSlash(window.RawWindowPrefix(w)))
		if _, err := os.Stat(filepath.Join(dir, "_SUCCESS")); err != nil {
			t.Fatalf("plain file _SUCCESS of %s: %v", xtime.Format(w.Start), err)
		}
		if parts, err := filepath.Glob(filepath.Join(dir, "part-*.jsonl")); err != nil || len(parts) != 0 {
			t.Fatalf("empty window %s has parts %v (err=%v)", xtime.Format(w.Start), parts, err)
		}
	}
	if l := e.ledger(t); len(l) != 1 {
		t.Fatalf("ledger = %+v, want only the part of the first window", l)
	}
	if ws := e.windows(t); len(ws) != 3 {
		t.Fatalf("export_windows has %d rows, want 3", len(ws))
	}
	e.exportedEventIDs(t)
}

// TestWindowExportCatchUpAfterDowntime: one call closes every missed window
// in order, each with exactly its own rows.
func TestWindowExportCatchUpAfterDowntime(t *testing.T) {
	e := setup(t)
	a, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	b, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(7*time.Minute))
	c, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(13*time.Minute))
	d, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(14*time.Minute))

	if n := e.exportOnce(t, set5, base.Add(20*time.Minute+time.Second)); n != 4 {
		t.Fatalf("ExportOnce = %d, want 4", n)
	}
	ws := e.windows(t)
	if len(ws) != 4 {
		t.Fatalf("export_windows has %d rows, want 4", len(ws))
	}
	for i, rows := range []int{1, 1, 2, 0} {
		if !ws[i].w.Start.Equal(win5(i).Start) || ws[i].status != statusClosed || *ws[i].rows != rows {
			t.Fatalf("window %d = %+v, want %s closed with %d rows", i, ws[i], xtime.Format(win5(i).Start), rows)
		}
	}
	for i, want := range [][]string{{a}, {b}, {c, d}} {
		if got := e.objectEventIDs(t, window.RawPartKey(win5(i), 0)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("window %d part holds %v, want %v", i, got, want)
		}
	}
	l := e.ledger(t)
	for i := 1; i < len(l); i++ {
		if !l[i].windowStart.After(l[i-1].windowStart) {
			t.Fatalf("batch ids do not follow window order: %+v", l)
		}
	}
	if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint([]string{a, b, c, d}) {
		t.Fatalf("exported %v", ids)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v after the catch-up, want 0", got)
	}
}

// TestWindowExportLateCommitGoesToNextWindow: transaction A inserts first
// but commits after window 0 closed. The claim neither waits for A nor loses
// it: A goes into window 1, and window 0's _SUCCESS never changes.
func TestWindowExportLateCommitGoesToNextWindow(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	idA, argsA := e.okArgs(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(2*time.Minute))
	txA, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer txA.Rollback(ctx)
	var seqA int64
	if err := txA.QueryRow(ctx, insertSQL, argsA...).Scan(&seqA); err != nil {
		t.Fatalf("insert A: %v", err)
	}
	idB, seqB := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(3*time.Minute))
	if seqB <= seqA {
		t.Fatalf("seq B (%d) should be greater than seq A (%d)", seqB, seqA)
	}

	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 1 {
		t.Fatalf("ExportOnce with A open = %d, want 1 (B only)", n)
	}
	if got := e.objectEventIDs(t, window.RawPartKey(win5(0), 0)); fmt.Sprint(got) != fmt.Sprint([]string{idB}) {
		t.Fatalf("window 0 holds %v, want only B=%s", got, idB)
	}
	first := e.get(t, window.RawSuccessKey(win5(0)))

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if n := e.exportOnce(t, set5, base.Add(10*time.Minute+time.Second)); n != 1 {
		t.Fatalf("ExportOnce after A committed = %d, want 1 (A only)", n)
	}
	if got := e.objectEventIDs(t, window.RawPartKey(win5(1), 0)); fmt.Sprint(got) != fmt.Sprint([]string{idA}) {
		t.Fatalf("window 1 holds %v, want only A=%s", got, idA)
	}
	if again := e.get(t, window.RawSuccessKey(win5(0))); !bytes.Equal(first, again) {
		t.Fatalf("window 0 _SUCCESS changed:\n  %s\n  %s", first, again)
	}
	if man, err := window.ParseSuccess(first); err != nil || man.Rows != 1 {
		t.Fatalf("window 0 _SUCCESS rows = %d (err=%v), want 1", man.Rows, err)
	}
	if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint([]string{idB, idA}) {
		t.Fatalf("exported %v, want [B=%s A=%s]", ids, idB, idA)
	}
}

// TestWindowExportCrashAfterClaim: a part claimed before a crash is resumed
// with exactly its rows, and a row committed afterwards becomes the next part
// of the same window.
func TestWindowExportCrashAfterClaim(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	id1, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	id2, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(2*time.Minute))
	w0 := win5(0)

	if err := openWindow(ctx, e.pool, w0); err != nil {
		t.Fatalf("openWindow: %v", err)
	}
	b, ok, err := claimNext(ctx, e.pool, w0, 500)
	if err != nil || !ok || b.RowCount != 2 || b.PartIndex != 0 {
		t.Fatalf("claimNext: ok=%v batch=%+v err=%v", ok, b, err)
	}
	for _, id := range []string{id1, id2} {
		if got := e.batchOf(t, id); got == nil || *got != b.ID {
			t.Fatalf("row %s export_batch_id = %v after the claim, want %d", id, got, b.ID)
		}
	}
	id3, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(4*time.Minute))

	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 3 {
		t.Fatalf("ExportOnce after the claim-crash = %d, want 3 (resumed part of 2 + a new part of 1)", n)
	}
	l := e.ledger(t)
	if len(l) != 2 || l[0].id != b.ID || l[0].part != 0 || l[0].rows != 2 || l[1].part != 1 || l[1].rows != 1 {
		t.Fatalf("ledger = %+v, want the claimed part 0 with 2 rows, then part 1 with 1 row", l)
	}
	if got := e.objectEventIDs(t, window.RawPartKey(w0, 0)); fmt.Sprint(got) != fmt.Sprint([]string{id1, id2}) {
		t.Fatalf("resumed part holds %v, want [%s %s]", got, id1, id2)
	}
	if got := e.objectEventIDs(t, window.RawPartKey(w0, 1)); fmt.Sprint(got) != fmt.Sprint([]string{id3}) {
		t.Fatalf("part 1 holds %v, want [%s]", got, id3)
	}
	man, err := window.ParseSuccess(e.get(t, window.RawSuccessKey(w0)))
	if err != nil || len(man.Parts) != 2 || man.Parts[0].Key != window.RawPartKey(w0, 0) || man.Parts[1].Key != window.RawPartKey(w0, 1) {
		t.Fatalf("_SUCCESS = %+v (err=%v), want parts 0 and 1 in order", man, err)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 3 {
		t.Fatalf("exported %v, want 3 rows", ids)
	}
}

// TestWindowExportCrashAfterPartPutAndAfterSuccessPut: retries after a
// crash at any later step write byte-identical objects.
func TestWindowExportCrashAfterPartPutAndAfterSuccessPut(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	now := base.Add(5*time.Minute + time.Second)
	w0 := win5(0)
	if n := e.exportOnce(t, set5, now); n != 1 {
		t.Fatalf("ExportOnce = %d, want 1", n)
	}
	part, success := e.get(t, window.RawPartKey(w0, 0)), e.get(t, window.RawSuccessKey(w0))
	exists := func() float64 { return promtest.ToFloat64(e.m.S3Puts.WithLabelValues("exists")) }
	status := func() string {
		var s string
		if err := e.pool.QueryRow(ctx, `SELECT status FROM pipeline.export_windows WHERE window_start = $1`, w0.Start).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	// Crash after the part PUT, before it was marked exported (so before the seal).
	before := exists()
	if _, err := e.pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'claimed', exported_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, `UPDATE pipeline.export_windows
SET status = 'claiming', success_sha256 = NULL, row_count = NULL, part_count = NULL, closed_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if n := e.exportOnce(t, set5, now); n != 1 {
		t.Fatalf("ExportOnce after the part crash = %d, want 1 (the resumed part)", n)
	}
	if got := exists() - before; got != 2 {
		t.Fatalf("s3_put_total{exists} grew by %v, want 2 (part and _SUCCESS)", got)
	}
	if !bytes.Equal(part, e.get(t, window.RawPartKey(w0, 0))) || !bytes.Equal(success, e.get(t, window.RawSuccessKey(w0))) {
		t.Fatal("objects changed after the retry")
	}
	if s := status(); s != statusClosed {
		t.Fatalf("window status = %q, want closed", s)
	}

	// Crash after the _SUCCESS PUT, before the window was marked closed.
	before = exists()
	if _, err := e.pool.Exec(ctx, `UPDATE pipeline.export_windows SET status = 'sealed', closed_at = NULL`); err != nil {
		t.Fatal(err)
	}
	if n := e.exportOnce(t, set5, now); n != 0 {
		t.Fatalf("ExportOnce after the _SUCCESS crash = %d, want 0", n)
	}
	if got := exists() - before; got != 1 {
		t.Fatalf("s3_put_total{exists} grew by %v, want 1 (_SUCCESS)", got)
	}
	if s := status(); s != statusClosed {
		t.Fatalf("window status = %q, want closed", s)
	}
	if !bytes.Equal(success, e.get(t, window.RawSuccessKey(w0))) {
		t.Fatal("_SUCCESS changed after the retry")
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("mismatch")); got != 0 {
		t.Fatalf("s3_put_total{mismatch} = %v, want 0", got)
	}
	e.exportedEventIDs(t)
}

// TestWindowExportPartMax: a window with more rows than PartMax is split into
// parts of at most PartMax rows, listed in index order.
func TestWindowExportPartMax(t *testing.T) {
	e := setup(t)
	var want []string
	for i := range 5 {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute+time.Duration(i)*10*time.Second))
		want = append(want, id)
	}
	if n := e.exportOnce(t, Settings{Window: 5 * time.Minute, PartMax: 2}, base.Add(5*time.Minute+time.Second)); n != 5 {
		t.Fatalf("ExportOnce = %d, want 5", n)
	}
	man, err := window.ParseSuccess(e.get(t, window.RawSuccessKey(win5(0))))
	if err != nil {
		t.Fatal(err)
	}
	if man.Rows != 5 || len(man.Parts) != 3 {
		t.Fatalf("_SUCCESS = %+v, want 5 rows in 3 parts", man)
	}
	for i, rows := range []int{2, 2, 1} {
		if man.Parts[i].Key != window.RawPartKey(win5(0), i) || man.Parts[i].Rows != rows {
			t.Fatalf("part %d = %+v, want %s with %d rows", i, man.Parts[i], window.RawPartKey(win5(0), i), rows)
		}
	}
	if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("exported %v, want %v", ids, want)
	}
}

// TestWindowExportStuckPartBlocksWindowAndLagGrows: a part that keeps
// failing keeps its window open, no later window opens, and the lag grows
// with the time past the window's end.
func TestWindowExportStuckPartBlocksWindowAndLagGrows(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	w0 := win5(0)
	if _, err := e.s3.PutIfAbsent(ctx, s3.BucketRaw, window.RawPartKey(w0, 0), []byte("other bytes\n")); err != nil {
		t.Fatalf("put other bytes: %v", err)
	}

	for i, late := range []time.Duration{10 * time.Minute, 15 * time.Minute} {
		_, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, set5, w0.End.Add(late))
		if !errors.Is(err, s3.ErrChecksumMismatch) {
			t.Fatalf("ExportOnce #%d = %v, want a checksum mismatch", i+1, err)
		}
		if got := promtest.ToFloat64(e.m.ExportLag); got < late.Seconds() {
			t.Fatalf("export lag after ExportOnce #%d = %v, want >= %v", i+1, got, late.Seconds())
		}
		ws := e.windows(t)
		if len(ws) != 1 || ws[0].status != statusClaiming {
			t.Fatalf("export_windows after ExportOnce #%d = %+v, want only window 0, claiming", i+1, ws)
		}
	}
	if _, err := e.s3.Get(ctx, s3.BucketRaw, window.RawSuccessKey(w0)); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("_SUCCESS of the stuck window: err=%v, want ErrNotFound", err)
	}
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("mismatch")); got < 1 {
		t.Fatalf("s3_put_total{mismatch} = %v, want >= 1", got)
	}
}

// TestWindowExportDLQByWindow: an invalid row goes to the exporter DLQ named
// by window and part; the part and _SUCCESS count only the raw lines.
func TestWindowExportDLQByWindow(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	goodID, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	// The table has no CHECK on source/endpoint, S1 does: this row is invalid.
	badID, badSeq := e.insertCommitted(t, "not-coingecko", "bogus_endpoint", base.Add(2*time.Minute))
	w0 := win5(0)

	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2", n)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 1 || ids[0] != goodID {
		t.Fatalf("raw parts hold %v, want only %s", ids, goodID)
	}
	l := e.ledger(t)
	if len(l) != 1 || l[0].rows != 2 || l[0].lines == nil || *l[0].lines != 1 {
		t.Fatalf("ledger = %+v, want one part with row_count 2 and line_count 1", l)
	}
	man, err := window.ParseSuccess(e.get(t, window.RawSuccessKey(w0)))
	if err != nil || man.Rows != 1 || len(man.Parts) != 1 || man.Parts[0].Rows != 1 {
		t.Fatalf("_SUCCESS = %+v (err=%v), want rows 1 in one part of 1 row", man, err)
	}

	key := window.ExportDLQKey(w0, 0)
	body, err := e.s3.Get(ctx, s3.BucketDLQ, key)
	if err != nil {
		t.Fatalf("get dlq object %s: %v", key, err)
	}
	dl := lines(body)
	if len(dl) != 1 {
		t.Fatalf("dlq object has %d lines, want 1", len(dl))
	}
	if v, err := e.reg.Validate(validation.IDDLQRecord, []byte(dl[0])); err != nil || len(v) > 0 {
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
	if err := json.Unmarshal([]byte(dl[0]), &rec); err != nil {
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
	// The bad row belongs to the part that sent it to the DLQ: never claimed again.
	if got := e.batchOf(t, badID); got == nil || *got != l[0].id {
		t.Fatalf("bad row export_batch_id = %v, want %d", got, l[0].id)
	}
	if n := e.exportOnce(t, set5, base.Add(10*time.Minute+time.Second)); n != 0 {
		t.Fatalf("ExportOnce after the DLQ part = %d, want 0", n)
	}
}

// TestWindowExportBootstrapWindow: on an empty ledger the exporter closes
// the window just before the current one at once, so the first publish run
// finds a _SUCCESS, unless the lake already holds that _SUCCESS.
func TestWindowExportBootstrapWindow(t *testing.T) {
	ctx := context.Background()
	set := Settings{Window: 5 * time.Minute, PartMax: 500}
	b := window.Floor(time.Now(), time.Hour).Add(-2 * time.Hour) // aligned; inside the partitions setup creates
	prev := window.Window{Start: b.Add(-5 * time.Minute), End: b}
	emptyPrev := fmt.Sprintf(`{"window_start":"%s","window_end":"%s","rows":0,"parts":[]}`, xtime.Format(prev.Start), xtime.Format(b))

	t.Run("empty ledger closes the window before the current one at once", func(t *testing.T) {
		e := setup(t)
		if n, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, set, b.Add(2*time.Minute)); n != 0 || err != nil {
			t.Fatalf("ExportOnce = %d, %v; want 0, nil", n, err)
		}
		ws := e.windows(t)
		if len(ws) != 1 || !ws[0].w.Start.Equal(prev.Start) || ws[0].status != statusClosed {
			t.Fatalf("export_windows = %+v, want only %s, closed", ws, xtime.Format(prev.Start))
		}
		if got := string(e.get(t, window.RawSuccessKey(prev))); got != emptyPrev {
			t.Fatalf("_SUCCESS = %s, want %s", got, emptyPrev)
		}
		if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
			t.Fatalf("export lag = %v, want 0", got)
		}
		e.exportOnce(t, set, b.Add(5*time.Minute+time.Second))
		ws = e.windows(t)
		if len(ws) != 2 || !ws[1].w.Start.Equal(b) || ws[1].status != statusClosed {
			t.Fatalf("export_windows = %+v, want %s and %s closed", ws, xtime.Format(prev.Start), xtime.Format(b))
		}
	})
	t.Run("rows in the current window wait for it", func(t *testing.T) {
		e := setup(t)
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, b.Add(time.Minute))
		if n := e.exportOnce(t, set, b.Add(2*time.Minute)); n != 0 {
			t.Fatalf("ExportOnce = %d, want 0", n)
		}
		if got := string(e.get(t, window.RawSuccessKey(prev))); got != emptyPrev {
			t.Fatalf("_SUCCESS = %s, want %s", got, emptyPrev)
		}
		if got := e.batchOf(t, id); got != nil {
			t.Fatalf("row of the current window stamped with batch %d", *got)
		}
		if n := e.exportOnce(t, set, b.Add(5*time.Minute+time.Second)); n != 1 {
			t.Fatalf("ExportOnce at the window end = %d, want 1", n)
		}
		cur := window.Window{Start: b, End: b.Add(5 * time.Minute)}
		if man, err := window.ParseSuccess(e.get(t, window.RawSuccessKey(cur))); err != nil || man.Rows != 1 {
			t.Fatalf("_SUCCESS of %s = %+v (err=%v), want rows 1", xtime.Format(b), man, err)
		}
		if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint([]string{id}) {
			t.Fatalf("exported %v", ids)
		}
	})
	t.Run("a _SUCCESS already in the lake is never overwritten", func(t *testing.T) {
		e := setup(t)
		earlier := []byte(`{"left":"by an earlier stack"}`)
		if _, err := e.s3.PutIfAbsent(ctx, s3.BucketRaw, window.RawSuccessKey(prev), earlier); err != nil {
			t.Fatal(err)
		}
		if n, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, set, b.Add(2*time.Minute)); n != 0 || err != nil {
			t.Fatalf("ExportOnce = %d, %v; want 0, nil", n, err)
		}
		if got := e.get(t, window.RawSuccessKey(prev)); !bytes.Equal(got, earlier) {
			t.Fatalf("the earlier _SUCCESS was replaced by %s", got)
		}
		if ws := e.windows(t); len(ws) != 0 {
			t.Fatalf("export_windows = %+v, want empty", ws)
		}
		if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("mismatch")); got != 0 {
			t.Fatalf("s3_put_total{mismatch} = %v, want 0", got)
		}
		if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
			t.Fatalf("export lag = %v, want 0", got)
		}
		e.exportOnce(t, set, b.Add(5*time.Minute+time.Second))
		ws := e.windows(t)
		if len(ws) != 1 || !ws[0].w.Start.Equal(b) || ws[0].status != statusClosed {
			t.Fatalf("export_windows = %+v, want only %s, closed", ws, xtime.Format(b))
		}
		if got := e.get(t, window.RawSuccessKey(prev)); !bytes.Equal(got, earlier) {
			t.Fatalf("the earlier _SUCCESS was replaced by %s", got)
		}
	})
}

// TestWindowExportKeysIgnoreLocalZone: keys and manifest bytes are UTC even
// though pgx returns timestamptz in a +05:30 zone whose date differs.
// TestMain set time.Local; this test must not touch it (the pgx pool's
// goroutines read it).
func TestWindowExportKeysIgnoreLocalZone(t *testing.T) {
	if _, off := time.Now().Zone(); off != 5*3600+30*60 {
		t.Fatalf("time.Local offset %d s: TestMain must set +05:30", off)
	}
	// pgx returns timestamptz in +05:30 from here on.
	e := setup(t)
	// 00:00 IST on 2026-10-11: the local date differs from the UTC one.
	b := time.Date(2026, 10, 10, 18, 30, 0, 0, time.UTC)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, b.Add(time.Minute))
	n, err := ExportOnce(context.Background(), e.pool, e.s3, e.reg, e.m, e.log,
		Settings{Window: 5 * time.Minute, PartMax: 500}, b.Add(5*time.Minute+time.Second))
	if n != 1 || err != nil {
		t.Fatalf("ExportOnce = %d, %v; want 1, nil", n, err)
	}
	e.get(t, "source=coingecko/ingest_date=2026-10-10/window=1830/part-00000.jsonl")
	success := string(e.get(t, "source=coingecko/ingest_date=2026-10-10/window=1830/_SUCCESS"))
	if want := `{"window_start":"2026-10-10T18:30:00.000Z","window_end":"2026-10-10T18:35:00.000Z","rows":1,`; !strings.HasPrefix(success, want) {
		t.Fatalf("_SUCCESS = %s, want the prefix %s", success, want)
	}
	e.exportedEventIDs(t)
}

// TestWindowExportWindowSizeChange: after ETL_WINDOW changes, the next window
// starts at the first boundary of the new size and takes every row fetched
// before its end; no key collides.
func TestWindowExportWindowSizeChange(t *testing.T) {
	e := setup(t)
	a, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
	b, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(6*time.Minute))
	c, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(12*time.Minute))
	d, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(19*time.Minute))
	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 1 {
		t.Fatalf("ExportOnce with 5m = %d, want 1", n)
	}
	if n := e.exportOnce(t, Settings{Window: 10 * time.Minute, PartMax: 500}, base.Add(20*time.Minute+time.Second)); n != 3 {
		t.Fatalf("ExportOnce with 10m = %d, want 3", n)
	}
	ws := e.windows(t)
	w10 := window.Window{Start: base.Add(10 * time.Minute), End: base.Add(20 * time.Minute)}
	if len(ws) != 2 || !ws[1].w.Start.Equal(w10.Start) || !ws[1].w.End.Equal(w10.End) || ws[1].status != statusClosed {
		t.Fatalf("export_windows = %+v, want [10:00,10:05) and [10:10,10:20) closed", ws)
	}
	if got := e.objectEventIDs(t, window.RawPartKey(w10, 0)); fmt.Sprint(got) != fmt.Sprint([]string{b, c, d}) {
		t.Fatalf("10-minute window holds %v, want [%s %s %s]", got, b, c, d)
	}
	if ids := e.exportedEventIDs(t); fmt.Sprint(ids) != fmt.Sprint([]string{a, b, c, d}) {
		t.Fatalf("exported %v", ids)
	}
}

// TestConcurrentClaimsSerializeOnWindow: claims of one window wait for each
// other on the window row, take disjoint rows and consecutive part indexes.
func TestConcurrentClaimsSerializeOnWindow(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	for i := range 6 {
		e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute+time.Duration(i)*time.Second))
	}
	w0 := win5(0)
	if err := openWindow(ctx, e.pool, w0); err != nil {
		t.Fatal(err)
	}
	tx1, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback(ctx)
	b1, ok, err := claimIn(ctx, tx1, w0, 3) // window row and 3 rows locked, not committed
	if err != nil || !ok || b1.RowCount != 3 || b1.PartIndex != 0 {
		t.Fatalf("claimIn: %v %v %+v", err, ok, b1)
	}

	type result struct {
		b   batch
		ok  bool
		err error
	}
	done := make(chan result, 1)
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		b, ok, err := claimNext(dctx, e.pool, w0, 500)
		done <- result{b, ok, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("second claim returned while the first holds the window: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || !r.ok || r.b.RowCount != 3 || r.b.PartIndex != 1 {
		t.Fatalf("second claim = %+v, want 3 rows as part 1", r)
	}

	rows, err := e.pool.Query(ctx,
		`SELECT event_id::text, export_batch_id FROM raw.api_calls WHERE export_batch_id IN ($1, $2)`, b1.ID, r.b.ID)
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
	if len(owner) != 6 || perBatch[b1.ID] != 3 || perBatch[r.b.ID] != 3 {
		t.Fatalf("stamped rows per batch = %v (%d rows), want 3 for %d and 3 for %d", perBatch, len(owner), b1.ID, r.b.ID)
	}

	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 6 {
		t.Fatalf("ExportOnce after both claims = %d, want 6 (two resumed parts)", n)
	}
	seen := map[string]bool{}
	for _, id := range e.exportedEventIDs(t) {
		if seen[id] {
			t.Fatalf("event %s exported twice", id)
		}
		seen[id] = true
	}
	if len(seen) != 6 {
		t.Fatalf("exported %d unique rows, want 6", len(seen))
	}
}

func TestExportOnceExportsEveryRowExactlyOnce(t *testing.T) {
	e := setup(t)
	want := map[string]bool{}
	for _, at := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute, 6 * time.Minute} {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(at))
		want[id] = true
	}
	failedID := e.insertFailedCall(t, base.Add(2*time.Minute)) // outcome http_error: never exported
	set := Settings{Window: 5 * time.Minute, PartMax: 2}

	for i, c := range []struct {
		now  time.Duration
		want int
	}{{5*time.Minute + time.Second, 3}, {10*time.Minute + time.Second, 1}, {10*time.Minute + time.Second, 0}} {
		if n := e.exportOnce(t, set, base.Add(c.now)); n != c.want {
			t.Fatalf("ExportOnce #%d = %d, want %d", i+1, n, c.want)
		}
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
	// Parts: 2 + 1 in window 0, 1 in window 1; plus two _SUCCESS objects.
	if got := promtest.ToFloat64(e.m.S3Puts.WithLabelValues("created")); got != 5 {
		t.Fatalf("s3_put_total{created} = %v, want 5", got)
	}
	if got := promtest.ToFloat64(e.m.ExportLag); got != 0 {
		t.Fatalf("export lag = %v after full export, want 0", got)
	}
	if b := e.batchOf(t, failedID); b != nil {
		t.Fatalf("http_error row stamped with batch %d, want NULL", *b)
	}
}

// TestExportOnceBatchesInSeqOrder: parts take the window's rows in seq
// (commit-independent insert) order, not fetched_at order, at most PartMax
// per part.
func TestExportOnceBatchesInSeqOrder(t *testing.T) {
	e := setup(t)
	var want []string
	for i := range 9 {
		// fetched_at decreases while seq increases.
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(4*time.Minute-time.Duration(i)*10*time.Second))
		want = append(want, id)
	}
	if n := e.exportOnce(t, Settings{Window: 5 * time.Minute, PartMax: 4}, base.Add(5*time.Minute+time.Second)); n != 9 {
		t.Fatalf("ExportOnce = %d, want 9", n)
	}
	l := e.ledger(t)
	if len(l) != 3 || l[0].rows != 4 || l[1].rows != 4 || l[2].rows != 1 {
		t.Fatalf("ledger = %+v, want parts of 4, 4 and 1 rows", l)
	}
	if got := e.exportedEventIDs(t); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("exported %v, want %v in insert order", got, want)
	}
	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 0 {
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
	for i := range 3 {
		id, _ := e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute+time.Duration(i)*time.Second))
		want[id] = true
	}
	if n := e.exportOnce(t, set5, base.Add(5*time.Minute+time.Second)); n != 3 {
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

// TestExportsRowInPartitionCreatedAfterClaim: the partial indexes of the
// parent are cloned into partitions created later, and the claim finds rows
// there. Hour windows keep the catch-up to the new partition at ~100 windows.
func TestExportsRowInPartitionCreatedAfterClaim(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	set := Settings{Window: 60 * time.Minute, PartMax: 500}
	hour := window.Floor(time.Now(), time.Hour)
	w0 := window.Window{Start: hour, End: hour.Add(time.Hour)}
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, hour.Add(time.Minute))
	if err := openWindow(ctx, e.pool, w0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := claimNext(ctx, e.pool, w0, 500); err != nil || !ok {
		t.Fatalf("claimNext: ok=%v err=%v", ok, err)
	}

	later := hour.AddDate(0, 0, 4).Add(30 * time.Minute) // beyond setup's today+3
	if err := pg.EnsurePartitions(ctx, e.pool, later, 0); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, later)
	if n := e.exportOnce(t, set, window.Floor(later, time.Hour).Add(time.Hour+time.Second)); n != 2 {
		t.Fatalf("ExportOnce = %d, want 2 (resumed part + the row in the new partition)", n)
	}
	if ids := e.exportedEventIDs(t); len(ids) != 2 {
		t.Fatalf("exported %v, want 2 rows", ids)
	}

	var n int
	if err := e.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_indexes
 WHERE schemaname = 'raw' AND tablename = $1 AND indexdef LIKE '%export_batch_id%'`,
		"api_calls_p"+later.UTC().Format("20060102")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("new partition has %d indexes on export_batch_id, want 2", n)
	}
}

// TestExportOnceCountsClaimFailures: failures outside exportBatch (the window
// anchor on an empty ledger, the claim of an open window) are counted once by
// ExportOnce.
func TestExportOnceCountsClaimFailures(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name, want string
		open       bool
	}{
		{"window anchor", "window anchor", false},
		{"claim", "pick rows", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := setup(t)
			e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, base.Add(time.Minute))
			if c.open {
				if err := openWindow(ctx, e.pool, win5(0)); err != nil {
					t.Fatal(err)
				}
			}
			// The ledger stays readable; reading raw.api_calls fails.
			if _, err := e.pool.Exec(ctx, `ALTER TABLE raw.api_calls RENAME TO api_calls_gone`); err != nil {
				t.Fatalf("rename: %v", err)
			}
			_, err := ExportOnce(ctx, e.pool, e.s3, e.reg, e.m, e.log, set5, base.Add(5*time.Minute+time.Second))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("ExportOnce without raw.api_calls = %v, want a %q error", err, c.want)
			}
			if got := promtest.ToFloat64(e.m.ExportBatches.WithLabelValues("error")); got != 1 {
				t.Fatalf("export_batches_total{error} = %v after the failure, want 1", got)
			}
		})
	}
}

func TestRunClosesWindows(t *testing.T) {
	e := setup(t)
	fetched := time.Now().Add(-3 * time.Minute)
	e.insertCommitted(t, event.SourceCoinGecko, event.EndpointMarkets, fetched)
	start := window.Floor(fetched, time.Minute)
	w := window.Window{Start: start, End: start.Add(time.Minute)}

	cfg := config.Config{ExportInterval: 50 * time.Millisecond, ExportBatchMax: 500, Window: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		body, err := e.s3.Get(context.Background(), s3.BucketRaw, window.RawSuccessKey(w))
		if err == nil {
			if man, err := window.ParseSuccess(body); err != nil || man.Rows != 1 {
				t.Fatalf("_SUCCESS = %s (err=%v), want rows 1", body, err)
			}
			break
		}
		if !errors.Is(err, s3.ErrNotFound) {
			t.Fatalf("get _SUCCESS: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Run did not close window %s within 20s", xtime.Format(w.Start))
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

	cfg := config.Config{ExportInterval: time.Second, ExportBatchMax: 500, Window: 5 * time.Minute}
	err = Run(ctx, cfg, e.pool, e.s3, e.reg, e.m, e.log)
	if err == nil || !strings.Contains(err.Error(), "advisory lock") {
		t.Fatalf("Run with lock held = %v, want advisory lock error", err)
	}
}
