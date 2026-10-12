// Package exporter copies committed raw.api_calls rows (outcome ok) into
// write-once JSONL objects in the raw bucket, one ETL_WINDOW window at a time.
//
// Windows are aligned to the UTC hour and close strictly in order; their state
// lives in pipeline.export_windows (at most one window is not closed). Closing
// window W:
//   - claims the unexported ok rows with fetched_at < W.end into parts of at
//     most ETL_EXPORT_BATCH_MAX rows. A claim transaction locks W's window row
//     (claims of one window serialize, and none can follow the seal), picks
//     the rows FOR UPDATE SKIP LOCKED, inserts the pipeline.export_batches row
//     (window, part index, object key, row count, fetched_at bounds) and
//     stamps raw.api_calls.export_batch_id, all committed before any S3 I/O,
//     so a retry after a crash reads the same rows back (WHERE
//     export_batch_id = id ORDER BY seq, primary key) and rebuilds the same
//     bytes. A row that commits after W was sealed still has export_batch_id
//     NULL and goes into the next window: nothing is skipped and nothing waits
//     for a long transaction;
//   - writes each part (and its DLQ object) to
//     raw/source=coingecko/ingest_date=<d>/window=<HHMM>/part-<NNNNN>.jsonl;
//   - seals W: builds the _SUCCESS manifest from the exported parts and stores
//     its sha256, so every retry writes identical bytes;
//   - writes .../_SUCCESS last and marks W closed. An empty window gets a
//     _SUCCESS with no parts.
//
// Every PUT is PutIfAbsent (If-None-Match: *): a repeated PUT gets 412 and is
// accepted when the stored sha256 matches. After downtime the missed windows
// are closed in order. On an empty ledger the exporter closes the window just
// before the current one at once (usually empty), so the first publish run
// finds a _SUCCESS, unless the lake already holds that _SUCCESS from an
// earlier stack, which is never overwritten.
//
// The cost: one non-HOT UPDATE per exported row (a dead row version for
// vacuum and new index entries).
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
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
)

const (
	stage      = "export"
	schemaName = "raw_event.v1"
	lockName   = "etl.exporter"
)

// Lock acquisition retries; variables so tests can shorten them.
var (
	lockAttempts   = 12
	lockRetryDelay = 5 * time.Second
)

// Settings are the exporter's window parameters.
type Settings struct {
	Window  time.Duration // ETL_WINDOW
	PartMax int           // ETL_EXPORT_BATCH_MAX: maximum rows per raw part
}

// Window states in pipeline.export_windows: 'claiming' takes parts; 'sealed'
// froze the manifest (success_sha256 stored) and the _SUCCESS PUT is pending;
// 'closed' means _SUCCESS is written.
const (
	statusClaiming = "claiming"
	statusSealed   = "sealed"
	statusClosed   = "closed"
)

// batch is one row of pipeline.export_batches: one raw part of a window.
type batch struct {
	ID        int64
	Window    window.Window
	PartIndex int
	RowCount  int
	// fetched_at bounds of the claimed rows: they prune partitions when the
	// part is read back.
	MinFetchedAt, MaxFetchedAt time.Time
	ObjectKey                  string
	SHA256                     []byte // nil until the content was built once
	ClaimedAt                  time.Time
}

// apiRow is one raw.api_calls row as text/scalar columns.
type apiRow struct {
	Seq            int64
	EventID        string
	IdempotencyKey *string
	SchemaVersion  int16
	Source         string
	Endpoint       string
	Request        string // jsonb rendered as text
	FetchedAt      time.Time
	Outcome        string
	HTTP           *string // jsonb rendered as text
	Error          *string
	Payload        *string // json column: the original bytes
	PayloadSHA256  *string // hex
}

// dlqRecord is one line of a dlq/ object (schema S3 pipeline/dlq_record.v1.json).
type dlqRecord struct {
	SchemaVersion  int                    `json:"schema_version"`
	Stage          string                 `json:"stage"`
	ErrorClass     string                 `json:"error_class"`
	Violations     []validation.Violation `json:"violations"`
	Origin         dlqOrigin              `json:"origin"`
	OriginalBase64 string                 `json:"original_base64"`
	OccurredAt     string                 `json:"occurred_at"`
}

type dlqOrigin struct {
	Kind    string `json:"kind"` // "postgres"
	BatchID int64  `json:"batch_id"`
	Seq     int64  `json:"seq"`
	EventID string `json:"event_id"`
}

// joinLines builds a JSONL body: lines joined by "\n" plus a trailing newline.
// No lines → empty body.
func joinLines(lines [][]byte) []byte {
	var buf bytes.Buffer
	for _, l := range lines {
		buf.Write(l)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// rowToEvent rebuilds the S1 event from the stored columns. The payload is
// taken verbatim from payload::text, so payload_sha256 stays verifiable.
func rowToEvent(r apiRow) (event.RawEvent, error) {
	ev := event.RawEvent{
		SchemaVersion:  int(r.SchemaVersion),
		EventID:        r.EventID,
		IdempotencyKey: r.IdempotencyKey,
		Source:         r.Source,
		Endpoint:       r.Endpoint,
		FetchedAt:      xtime.Format(r.FetchedAt),
		Outcome:        r.Outcome,
		Error:          r.Error,
		PayloadSHA256:  r.PayloadSHA256,
	}
	if err := json.Unmarshal([]byte(r.Request), &ev.Request); err != nil {
		return ev, fmt.Errorf("seq %d: request column: %w", r.Seq, err)
	}
	if ev.Request.Params == nil { // S1 requires an object, never null
		ev.Request.Params = map[string]string{}
	}
	if r.HTTP != nil {
		var h event.HTTPMeta
		if err := json.Unmarshal([]byte(*r.HTTP), &h); err != nil {
			return ev, fmt.Errorf("seq %d: http column: %w", r.Seq, err)
		}
		ev.HTTP = &h
	}
	if r.Payload != nil {
		ev.Payload = json.RawMessage(*r.Payload)
	} else {
		ev.Payload = json.RawMessage("null")
	}
	return ev, nil
}

// ExportOnce is one exporter tick at time now. It first finishes parts left
// in status 'claimed' by a previous (crashed) run, then closes every window
// that is due (now >= its end) in order: claim and export its parts, seal
// its manifest, write its _SUCCESS. now is frozen for the whole call, so
// windows that fall due during a long catch-up are closed by the next tick.
// It returns the sum of row_count of the parts it exported. Every failure
// counts once in etl_export_batches_total{result="error"}: exportBatch
// counts its own, every other failure is counted here.
func ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, set Settings, now time.Time) (int, error) {
	exported := 0
	defer func() { updateLag(ctx, pool, m, log, set.Window, now) }()
	fail := func(err error) (int, error) {
		m.ExportBatches.WithLabelValues("error").Inc()
		return exported, err
	}

	pending, err := claimedBatches(ctx, pool)
	if err != nil {
		return fail(err)
	}
	for _, b := range pending {
		log.Warn("resuming claimed part", "stage", stage, "batch", b.ID, "window_start", xtime.Format(b.Window.Start),
			"part", b.PartIndex, "object_key", b.ObjectKey)
		if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
			return exported, err
		}
		exported += b.RowCount
	}

	for {
		w, status, found, bootstrap, err := pendingWindow(ctx, pool, set.Window, now)
		if err != nil {
			return fail(err)
		}
		if bootstrap {
			_, err := s3c.Get(ctx, s3.BucketRaw, window.RawSuccessKey(w))
			switch {
			case err == nil: // an earlier stack closed it (the lake was kept): never overwrite
				log.Warn("raw window already in the lake; starting at the next window", "stage", stage,
					"window_start", xtime.Format(w.Start), "success_key", window.RawSuccessKey(w))
				w = window.Window{Start: w.End, End: w.End.Add(set.Window)}
			case errors.Is(err, s3.ErrNotFound): // a fresh lake: close w now (usually empty)
			default:
				return fail(fmt.Errorf("exporter: check bootstrap window: %w", err))
			}
		}
		if !found && now.Before(w.End) {
			return exported, nil // the next window is not due yet
		}
		if !found {
			if err := openWindow(ctx, pool, w); err != nil {
				return fail(err)
			}
			status = statusClaiming
		}
		if status == statusClaiming {
			for {
				b, ok, err := claimNext(ctx, pool, w, set.PartMax)
				if err != nil {
					return fail(err)
				}
				if !ok {
					break
				}
				if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
					return exported, err
				}
				exported += b.RowCount
			}
			if err := sealWindow(ctx, pool, w); err != nil {
				return fail(err)
			}
		}
		if err := finishWindow(ctx, pool, s3c, m, log, w); err != nil {
			return fail(err)
		}
		updateLag(ctx, pool, m, log, set.Window, now) // smooth lag during a catch-up
		if ctx.Err() != nil {
			return exported, nil
		}
	}
}

// querier is what pendingWindow needs: a pool or a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// pendingWindow returns the window the exporter works on next (Postgres
// only, read-only):
//   - found: the window that is not closed yet (at most one exists), whatever
//     its size, with its status;
//   - otherwise the window after the last closed one: it starts at the first
//     boundary of size at or after the last end, so a changed ETL_WINDOW
//     switches at the next boundary of the new size and no key collides (the
//     claim has no lower bound, so rows between the last end and that
//     boundary go into it);
//   - on an empty ledger, the window holding the oldest unexported ok row,
//     or, when no row is older than the current window, the window just
//     before the current one (bootstrap): it is already due, so the first
//     tick closes it and the first publish run finds a _SUCCESS.
func pendingWindow(ctx context.Context, q querier, size time.Duration, now time.Time) (w window.Window, status string, found, bootstrap bool, err error) {
	var start, end time.Time
	err = q.QueryRow(ctx, `
SELECT window_start, window_end, status FROM pipeline.export_windows
 WHERE status <> 'closed' ORDER BY window_start LIMIT 1`).Scan(&start, &end, &status)
	switch {
	case err == nil:
		return window.Window{Start: start.UTC(), End: end.UTC()}, status, true, false, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return w, "", false, false, fmt.Errorf("exporter: pending window: %w", err)
	}

	var lastEnd *time.Time
	if err := q.QueryRow(ctx, `SELECT max(window_end) FROM pipeline.export_windows`).Scan(&lastEnd); err != nil {
		return w, "", false, false, fmt.Errorf("exporter: last window: %w", err)
	}
	if lastEnd != nil {
		start = window.Ceil(*lastEnd, size)
		return window.Window{Start: start, End: start.Add(size)}, "", false, false, nil
	}

	// Empty ledger. The predicates are literals so the partial index applies.
	var oldest *time.Time
	if err := q.QueryRow(ctx, `
SELECT min(fetched_at) FROM raw.api_calls WHERE outcome = 'ok' AND export_batch_id IS NULL`).Scan(&oldest); err != nil {
		return w, "", false, false, fmt.Errorf("exporter: window anchor: %w", err)
	}
	current := window.Floor(now, size)
	start = current
	if oldest != nil && oldest.Before(now) {
		start = window.Floor(*oldest, size)
	}
	if start.Equal(current) {
		start, bootstrap = current.Add(-size), true
	}
	return window.Window{Start: start, End: start.Add(size)}, "", false, bootstrap, nil
}

// openWindow inserts w in status 'claiming'. A second exporter racing on it
// fails on the primary key or on export_windows_one_pending; its tick errors
// and the next tick retries.
func openWindow(ctx context.Context, pool *pgxpool.Pool, w window.Window) error {
	if _, err := pool.Exec(ctx, `
INSERT INTO pipeline.export_windows (window_start, window_end, success_key) VALUES ($1, $2, $3)`,
		w.Start, w.End, window.RawSuccessKey(w)); err != nil {
		return fmt.Errorf("exporter: open window %s: %w", xtime.Format(w.Start), err)
	}
	return nil
}

// claimedBatches returns parts that were claimed but never marked exported,
// with their window.
func claimedBatches(ctx context.Context, pool *pgxpool.Pool) ([]batch, error) {
	rows, err := pool.Query(ctx, `
SELECT b.batch_id, b.window_start, w.window_end, b.part_index, b.row_count, b.min_fetched_at, b.max_fetched_at,
       b.object_key, b.content_sha256, b.claimed_at
  FROM pipeline.export_batches b JOIN pipeline.export_windows w USING (window_start)
 WHERE b.status = 'claimed' ORDER BY b.batch_id`)
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (batch, error) {
		var b batch
		err := r.Scan(&b.ID, &b.Window.Start, &b.Window.End, &b.PartIndex, &b.RowCount, &b.MinFetchedAt, &b.MaxFetchedAt,
			&b.ObjectKey, &b.SHA256, &b.ClaimedAt)
		b.Window = window.Window{Start: b.Window.Start.UTC(), End: b.Window.End.UTC()}
		return b, err
	})
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	return out, nil
}

// claimNext runs the short claim transaction for window w. It returns
// ok=false when w no longer takes parts or no unexported ok row fetched
// before w's end is free to claim.
func claimNext(ctx context.Context, pool *pgxpool.Pool, w window.Window, max int) (batch, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return batch{}, false, fmt.Errorf("exporter: begin claim: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit; otherwise releases the row locks
	b, ok, err := claimIn(ctx, tx, w, max)
	if err != nil || !ok {
		return b, ok, err
	}
	if err := tx.Commit(ctx); err != nil {
		return batch{}, false, fmt.Errorf("exporter: commit claim: %w", err)
	}
	return b, true, nil
}

// claimIn claims the next part of window w inside tx: up to max unexported
// ok rows fetched before w's end. It locks w's row first, so claims of one
// window run one after another and none follows the seal; the rows are
// locked FOR UPDATE SKIP LOCKED (rows another session holds go into a later
// window). Then it inserts the ledger row and stamps the rows with its
// batch id. No S3 I/O happens here.
func claimIn(ctx context.Context, tx pgx.Tx, w window.Window, max int) (batch, bool, error) {
	b := batch{Window: w}
	var one int
	err := tx.QueryRow(ctx, `
SELECT 1 FROM pipeline.export_windows WHERE window_start = $1 AND status = 'claiming' FOR UPDATE`, w.Start).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, false, nil // sealed or closed: never add a part to a frozen manifest
	}
	if err != nil {
		return b, false, fmt.Errorf("exporter: lock window %s: %w", xtime.Format(w.Start), err)
	}

	// The predicates are literals: with "outcome = $n" the generic plan pgx
	// switches to cannot use the partial index and scans every partition.
	rows, err := tx.Query(ctx, `
SELECT event_id::text, fetched_at FROM raw.api_calls
 WHERE outcome = 'ok' AND export_batch_id IS NULL AND fetched_at < $1
 ORDER BY seq
 LIMIT $2
   FOR UPDATE SKIP LOCKED`, w.End, max)
	if err != nil {
		return b, false, fmt.Errorf("exporter: pick rows: %w", err)
	}
	var ids []string
	var ts []time.Time
	for rows.Next() {
		var id string
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			rows.Close()
			return b, false, fmt.Errorf("exporter: pick rows: %w", err)
		}
		ids, ts = append(ids, id), append(ts, at)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, false, fmt.Errorf("exporter: pick rows: %w", err)
	}
	if len(ids) == 0 {
		return b, false, nil // no nextval: batch ids stay dense
	}

	if err := tx.QueryRow(ctx, `
SELECT COALESCE(max(part_index) + 1, 0) FROM pipeline.export_batches WHERE window_start = $1`, w.Start).Scan(&b.PartIndex); err != nil {
		return b, false, fmt.Errorf("exporter: next part index: %w", err)
	}
	// pg_get_serial_sequence also works for identity columns.
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('pipeline.export_batches', 'batch_id')), now()`).Scan(&b.ID, &b.ClaimedAt); err != nil {
		return b, false, fmt.Errorf("exporter: next batch id: %w", err)
	}
	b.ObjectKey = window.RawPartKey(w, b.PartIndex)
	b.RowCount = len(ids)
	b.MinFetchedAt, b.MaxFetchedAt = ts[0], ts[0]
	for _, at := range ts[1:] {
		if at.Before(b.MinFetchedAt) {
			b.MinFetchedAt = at
		}
		if at.After(b.MaxFetchedAt) {
			b.MaxFetchedAt = at
		}
	}

	// Ledger row first: the foreign key rejects a stamp pointing at a missing batch.
	if _, err := tx.Exec(ctx, `
INSERT INTO pipeline.export_batches (batch_id, window_start, part_index, row_count, min_fetched_at, max_fetched_at, object_key, claimed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, b.ID, w.Start, b.PartIndex, b.RowCount, b.MinFetchedAt, b.MaxFetchedAt, b.ObjectKey, b.ClaimedAt); err != nil {
		return b, false, fmt.Errorf("exporter: insert batch: %w", err)
	}

	// Stamp by primary key, never by seq (its uniqueness cannot be enforced on
	// the partitioned table). The redundant BETWEEN lets the planner prune
	// partitions; without it the join scans every partition.
	tag, err := tx.Exec(ctx, `
UPDATE raw.api_calls a
   SET export_batch_id = $1
  FROM unnest($2::text[]::uuid[], $3::timestamptz[]) AS p(event_id, fetched_at)
 WHERE a.event_id = p.event_id
   AND a.fetched_at = p.fetched_at
   AND a.export_batch_id IS NULL
   AND a.fetched_at BETWEEN $4 AND $5`, b.ID, ids, ts, b.MinFetchedAt, b.MaxFetchedAt)
	if err != nil {
		return b, false, fmt.Errorf("exporter: stamp rows: %w", err)
	}
	// The rows are locked by the pick, so every one of them must be stamped;
	// the ledger row_count is then exactly the number of stamped rows.
	if n := tag.RowsAffected(); n != int64(len(ids)) {
		return b, false, fmt.Errorf("exporter: batch %d: locked %d rows, stamped %d", b.ID, len(ids), n)
	}
	return b, true, nil
}

// manifest rebuilds the _SUCCESS bytes of w from its part rows, which are
// immutable once exported. It fails when a part is not exported yet.
func manifest(ctx context.Context, tx pgx.Tx, w window.Window) (body []byte, s window.Success, err error) {
	rows, err := tx.Query(ctx, `
SELECT part_index, object_key, status, content_sha256, line_count
  FROM pipeline.export_batches WHERE window_start = $1 ORDER BY part_index`, w.Start)
	if err != nil {
		return nil, s, fmt.Errorf("exporter: window %s parts: %w", xtime.Format(w.Start), err)
	}
	type part struct {
		index  int
		key    string
		status string
		sha    []byte
		lines  *int
	}
	ps, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (part, error) {
		var p part
		err := r.Scan(&p.index, &p.key, &p.status, &p.sha, &p.lines)
		return p, err
	})
	if err != nil {
		return nil, s, fmt.Errorf("exporter: window %s parts: %w", xtime.Format(w.Start), err)
	}
	parts := make([]window.SuccessPart, 0, len(ps))
	for _, p := range ps {
		if p.status != "exported" || p.sha == nil || p.lines == nil {
			return nil, s, fmt.Errorf("exporter: window %s: part %d not exported", xtime.Format(w.Start), p.index)
		}
		parts = append(parts, window.SuccessPart{Key: p.key, SHA256: hex.EncodeToString(p.sha), Rows: *p.lines})
	}
	s = window.NewSuccess(w, parts)
	body, err = s.Marshal()
	if err != nil {
		return nil, s, fmt.Errorf("exporter: window %s: marshal _SUCCESS: %w", xtime.Format(w.Start), err)
	}
	return body, s, nil
}

// sealWindow freezes the manifest of a 'claiming' window: it stores the
// sha256 of the _SUCCESS bytes and moves the window to 'sealed'. It holds
// the window row lock, so no claim of w runs at the same time, and a claim
// after the seal returns no part: a row committing late with fetched_at <
// w's end goes into the next window. A window that is already sealed or
// closed is left alone.
func sealWindow(ctx context.Context, pool *pgxpool.Pool, w window.Window) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM pipeline.export_windows WHERE window_start = $1 FOR UPDATE`, w.Start).Scan(&status); err != nil {
			return fmt.Errorf("exporter: seal window %s: %w", xtime.Format(w.Start), err)
		}
		if status != statusClaiming {
			return nil
		}
		body, s, err := manifest(ctx, tx, w)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		if _, err := tx.Exec(ctx, `
UPDATE pipeline.export_windows SET status = 'sealed', success_sha256 = $2, row_count = $3, part_count = $4
 WHERE window_start = $1 AND status = 'claiming'`, w.Start, sum[:], s.Rows, len(s.Parts)); err != nil {
			return fmt.Errorf("exporter: seal window %s: %w", xtime.Format(w.Start), err)
		}
		return nil
	})
}

// finishWindow writes the _SUCCESS of a sealed window and marks it closed.
// The manifest is rebuilt from the part rows and must match the sha256
// stored by the seal, so every retry writes identical bytes (a retry after a
// crash gets 412 with the same sha256, counted as exists). Every part PUT,
// DLQ included, finished before the seal, so _SUCCESS is the window's last
// object.
func finishWindow(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, m *obs.Metrics, log *slog.Logger, w window.Window) error {
	var status string
	var sealed []byte
	var body []byte
	var s window.Success
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT status, success_sha256 FROM pipeline.export_windows WHERE window_start = $1`, w.Start).Scan(&status, &sealed); err != nil {
			return fmt.Errorf("exporter: finish window %s: %w", xtime.Format(w.Start), err)
		}
		if status != statusSealed {
			return nil
		}
		var err error
		body, s, err = manifest(ctx, tx, w)
		return err
	})
	if err != nil {
		return err
	}
	switch status {
	case statusClosed:
		return nil
	case statusClaiming:
		return fmt.Errorf("exporter: finish window %s: not sealed", xtime.Format(w.Start))
	}
	sum := sha256.Sum256(body)
	if !bytes.Equal(sum[:], sealed) {
		log.Error("window manifest is not deterministic", "stage", stage, "window_start", xtime.Format(w.Start),
			"stored_sha256", hex.EncodeToString(sealed), "new_sha256", hex.EncodeToString(sum[:]))
		return fmt.Errorf("exporter: window %s: _SUCCESS checksum changed since the seal", xtime.Format(w.Start))
	}
	key := window.RawSuccessKey(w)
	created, err := put(ctx, s3c, m, s3.BucketRaw, key, body)
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `
UPDATE pipeline.export_windows SET status = 'closed', closed_at = now() WHERE window_start = $1 AND status = 'sealed'`, w.Start); err != nil {
		return fmt.Errorf("exporter: close window %s: %w", xtime.Format(w.Start), err)
	}
	log.Info("raw window closed", "stage", stage, "window_start", xtime.Format(w.Start), "bucket", s3.BucketRaw,
		"success_key", key, "parts", len(s.Parts), "rows", s.Rows, "created", created, "sha256", hex.EncodeToString(sum[:]))
	return nil
}

// exportBatch writes a claimed part: it reads the stamped rows back, builds
// the raw lines (invalid rows go to the part's DLQ object), stores the
// checksum and line count, PUTs the DLQ object and then the part, and marks
// the part exported. A part whose every row went to the DLQ is still written
// (empty body), so every claimed part has exactly one raw object. No
// transaction is open while S3 is called.
func exportBatch(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, b batch) (err error) {
	defer func() {
		if err != nil {
			m.ExportBatches.WithLabelValues("error").Inc()
		}
	}()

	rows, err := readBatch(ctx, pool, b)
	if err != nil {
		return err
	}
	if len(rows) != b.RowCount {
		return fmt.Errorf("exporter: batch %d: claimed %d rows, found %d", b.ID, b.RowCount, len(rows))
	}

	lines := make([][]byte, 0, len(rows))
	var dlqLines [][]byte
	var dlqClasses []string
	for _, r := range rows {
		line, dlq, class, err := buildLine(r, b, reg, m, log)
		if err != nil {
			return err
		}
		if dlq != nil {
			dlqLines = append(dlqLines, dlq)
			dlqClasses = append(dlqClasses, class)
			continue
		}
		lines = append(lines, line)
	}
	body := joinLines(lines)
	sum := sha256.Sum256(body)

	// Persist (or verify) the checksum before the PUT: a rebuilt part must
	// produce the same bytes as the first attempt.
	if b.SHA256 != nil {
		if !bytes.Equal(b.SHA256, sum[:]) {
			log.Error("export part is not deterministic", "stage", stage, "batch", b.ID, "object_key", b.ObjectKey,
				"stored_sha256", hex.EncodeToString(b.SHA256), "new_sha256", hex.EncodeToString(sum[:]))
			return fmt.Errorf("exporter: batch %d: content checksum changed between attempts", b.ID)
		}
	} else {
		if _, err := pool.Exec(ctx, `
UPDATE pipeline.export_batches SET content_sha256 = $2, line_count = $3 WHERE batch_id = $1 AND content_sha256 IS NULL`,
			b.ID, sum[:], len(lines)); err != nil {
			return fmt.Errorf("exporter: store checksum: %w", err)
		}
	}

	if len(dlqLines) > 0 {
		key := window.ExportDLQKey(b.Window, b.PartIndex)
		created, err := put(ctx, s3c, m, s3.BucketDLQ, key, joinLines(dlqLines))
		if err != nil {
			return err
		}
		if created { // a resumed part must not count its DLQ records twice
			for _, c := range dlqClasses {
				m.DLQ.WithLabelValues(stage, c).Inc()
			}
		}
		log.Error("raw events sent to dlq", "stage", stage, "batch", b.ID, "window_start", xtime.Format(b.Window.Start),
			"part", b.PartIndex, "bucket", s3.BucketDLQ, "object_key", key, "count", len(dlqLines))
	}

	created, err := put(ctx, s3c, m, s3.BucketRaw, b.ObjectKey, body)
	if err != nil {
		return err
	}

	if _, err := pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'exported', exported_at = now() WHERE batch_id = $1 AND status = 'claimed'`, b.ID); err != nil {
		return fmt.Errorf("exporter: mark exported: %w", err)
	}
	m.ExportBatches.WithLabelValues("exported").Inc()
	log.Info("raw part saved", "stage", stage, "batch", b.ID, "window_start", xtime.Format(b.Window.Start), "part", b.PartIndex,
		"bucket", s3.BucketRaw, "object_key", b.ObjectKey,
		"rows", len(lines), "dlq", len(dlqLines), "bytes", len(body), "created", created, "sha256", hex.EncodeToString(sum[:]))
	return nil
}

// put wraps PutIfAbsent with the etl_s3_put_total{result} metric.
func put(ctx context.Context, s3c *s3.Client, m *obs.Metrics, bucket, key string, body []byte) (bool, error) {
	created, err := s3c.PutIfAbsent(ctx, bucket, key, body)
	switch {
	case errors.Is(err, s3.ErrChecksumMismatch):
		m.S3Puts.WithLabelValues("mismatch").Inc()
		return false, fmt.Errorf("exporter: %w", err)
	case err != nil:
		m.S3Puts.WithLabelValues("error").Inc()
		return false, fmt.Errorf("exporter: %w", err)
	case created:
		m.S3Puts.WithLabelValues("created").Inc()
	default:
		m.S3Puts.WithLabelValues("exists").Inc()
	}
	return created, nil
}

// readBatch reads back the rows stamped with the batch id. The fetched_at
// bounds from the ledger prune partitions. ORDER BY names the table columns:
// a bare event_id would resolve to the text output column event_id::text.
// (seq, primary key) is a total order, so a retry rebuilds the same bytes.
func readBatch(ctx context.Context, pool *pgxpool.Pool, b batch) ([]apiRow, error) {
	rows, err := pool.Query(ctx, `
SELECT seq, event_id::text, idempotency_key, schema_version, source, endpoint, request::text,
       fetched_at, outcome, http::text, error, payload::text, encode(payload_sha256, 'hex')
  FROM raw.api_calls
 WHERE export_batch_id = $1
   AND fetched_at BETWEEN $2 AND $3
 ORDER BY api_calls.seq, api_calls.event_id, api_calls.fetched_at`, b.ID, b.MinFetchedAt, b.MaxFetchedAt)
	if err != nil {
		return nil, fmt.Errorf("exporter: read batch %d: %w", b.ID, err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (apiRow, error) {
		var a apiRow
		err := r.Scan(&a.Seq, &a.EventID, &a.IdempotencyKey, &a.SchemaVersion, &a.Source, &a.Endpoint, &a.Request,
			&a.FetchedAt, &a.Outcome, &a.HTTP, &a.Error, &a.Payload, &a.PayloadSHA256)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("exporter: read batch %d: %w", b.ID, err)
	}
	return out, nil
}

// buildLine returns either the S1 line for the raw object or a DLQ line with
// its error class.
func buildLine(r apiRow, b batch, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) (line, dlq []byte, class string, err error) {
	ev, convErr := rowToEvent(r)
	if convErr == nil {
		line, convErr = event.Marshal(ev)
	}
	if convErr != nil {
		orig := ""
		if r.Payload != nil {
			orig = *r.Payload
		}
		log.Error("raw event cannot be rebuilt", "stage", stage, "batch", b.ID, "event_id", r.EventID, "seq", r.Seq, "err", convErr)
		rec := newDLQ(r, b, "decode_error", []validation.Violation{{Message: convErr.Error()}}, []byte(orig))
		d, err := marshalDLQ(rec, reg, log)
		return nil, d, "decode_error", err
	}

	viol, verr := reg.Validate(validation.IDRawEvent, line)
	if verr != nil {
		return nil, nil, "", fmt.Errorf("exporter: validate seq %d: %w", r.Seq, verr)
	}
	if len(viol) == 0 {
		m.SchemaValidations.WithLabelValues(stage, schemaName, "pass").Inc()
		return line, nil, "", nil
	}
	m.SchemaValidations.WithLabelValues(stage, schemaName, "fail").Inc()
	for _, v := range viol {
		m.SchemaViolations.WithLabelValues(stage, schemaName, validation.Keyword(v)).Inc()
	}
	// Validate returns violations sorted and capped, so the DLQ bytes are
	// deterministic across retries of the same batch.
	log.Error("raw event failed S1 validation", "stage", stage, "batch", b.ID, "event_id", r.EventID, "seq", r.Seq, "violations", viol)
	rec := newDLQ(r, b, "schema_violation", viol, line)
	d, err := marshalDLQ(rec, reg, log)
	return nil, d, "schema_violation", err
}

func newDLQ(r apiRow, b batch, class string, viol []validation.Violation, original []byte) dlqRecord {
	if viol == nil { // the DLQ schema wants [], never null
		viol = []validation.Violation{}
	}
	return dlqRecord{
		SchemaVersion:  1,
		Stage:          stage,
		ErrorClass:     class,
		Violations:     viol,
		Origin:         dlqOrigin{Kind: "postgres", BatchID: b.ID, Seq: r.Seq, EventID: r.EventID},
		OriginalBase64: base64.StdEncoding.EncodeToString(original),
		// Claim time, not time.Now(): a retried batch must rebuild identical bytes.
		OccurredAt: xtime.Format(b.ClaimedAt),
	}
}

// marshalDLQ serializes a DLQ record and checks it against S3. A DLQ record
// that fails its own schema is still written (the data must not be lost) and
// logged at ERROR with the original bytes in base64.
func marshalDLQ(rec dlqRecord, reg *validation.Registry, log *slog.Logger) ([]byte, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("exporter: marshal dlq record: %w", err)
	}
	viol, verr := reg.Validate(validation.IDDLQRecord, b)
	if verr != nil || len(viol) > 0 {
		log.Error("dlq record failed S3 validation", "stage", stage, "err", verr, "violations", viol,
			"original_base64", rec.OriginalBase64)
	}
	return b, nil
}

// updateLag sets etl_export_lag_seconds to the seconds past the end of the
// oldest window the exporter has not closed yet: 0 on schedule (up to one
// ETL_EXPORT_INTERVAL right after a window end), growing while a part keeps
// failing, S3 or Postgres is down, or a catch-up is running. It is 0 before
// the first window (bootstrap). Rows locked by another session or committed
// late do not count: they go into a later window. On a failed query or a
// done ctx it logs and leaves the gauge unchanged.
func updateLag(ctx context.Context, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger, size time.Duration, now time.Time) {
	if ctx.Err() != nil {
		return
	}
	w, _, _, bootstrap, err := pendingWindow(ctx, pool, size, now)
	if err != nil {
		log.Warn("export lag query failed", "stage", stage, "err", err)
		return
	}
	lag := now.Sub(w.End).Seconds()
	if bootstrap || lag < 0 {
		lag = 0
	}
	m.ExportLag.Set(lag)
}

// Run holds the exporter advisory lock on a dedicated connection for the
// whole process lifetime and calls ExportOnce at once and then every
// cfg.ExportInterval; each call closes every window that is due. It returns
// nil when ctx is cancelled and an error when the lock cannot be taken or is
// lost.
func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error {
	conn, err := acquireLock(ctx, pool, log)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer releaseLock(conn, log)
	log.Info("exporter lock acquired", "stage", stage, "window", cfg.Window.String())

	set := Settings{Window: cfg.Window, PartMax: cfg.ExportBatchMax}
	ticker := time.NewTicker(cfg.ExportInterval)
	defer ticker.Stop()
	for {
		// A dead lock connection means the session lock is gone: stop so a
		// restarted process re-acquires it instead of running unprotected.
		if err := conn.Ping(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("exporter: lock connection lost: %w", err)
		}
		if _, err := ExportOnce(ctx, pool, s3c, reg, m, log, set, time.Now()); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("export failed", "stage", stage, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func acquireLock(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("exporter: acquire lock connection: %w", err)
	}
	for attempt := 1; attempt <= lockAttempts; attempt++ {
		var ok bool
		// Session-level lock: held until unlocked or the connection closes.
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockName).Scan(&ok); err != nil {
			conn.Release()
			return nil, fmt.Errorf("exporter: try advisory lock: %w", err)
		}
		if ok {
			return conn, nil
		}
		log.Warn("exporter lock held by another instance", "stage", stage, "attempt", attempt)
		if attempt == lockAttempts {
			break
		}
		select {
		case <-ctx.Done():
			conn.Release()
			return nil, ctx.Err()
		case <-time.After(lockRetryDelay):
		}
	}
	conn.Release()
	return nil, fmt.Errorf("exporter: advisory lock %q not acquired after %d attempts", lockName, lockAttempts)
}

func releaseLock(conn *pgxpool.Conn, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, lockName); err != nil {
		log.Warn("exporter unlock failed; closing connection", "stage", stage, "err", err)
		_ = conn.Conn().Close(ctx) // closing the session drops the lock; the pool discards closed conns
	}
	conn.Release()
}
