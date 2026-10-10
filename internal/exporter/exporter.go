// Package exporter copies committed raw.api_calls rows (outcome ok) into
// write-once JSONL objects in the raw bucket.
//
// Correctness rests on three pieces:
//   - an (ingest_txid, seq) cursor bounded by pg_snapshot_xmin(pg_current_snapshot()):
//     every transaction id below the snapshot xmin has finished, so no row can
//     later appear behind the cursor (an identity watermark would skip rows
//     whose transactions commit out of order);
//   - the pipeline.export_batches ledger: the row range is claimed and committed
//     before any S3 I/O, so a retry after a crash rebuilds exactly the same bytes;
//   - PutIfAbsent (If-None-Match: *): a repeated PUT gets 412 and is accepted
//     when the stored sha256 matches.
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

// batch is one row of pipeline.export_batches. Transaction ids travel as
// decimal strings and are cast in SQL with $n::text::xid8, so no pgx xid8
// codec is needed.
type batch struct {
	ID        int64
	FromTxid  string
	FromSeq   int64
	ToTxid    string
	ToSeq     int64
	RowCount  int
	ObjectKey string
	SHA256    []byte // nil until the content was built once
	ClaimedAt time.Time
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

// rawObjectKey: source=coingecko/ingest_date=<claim date>/batch=<%020d>.jsonl
func rawObjectKey(batchID int64, claimedAt time.Time) string {
	return fmt.Sprintf("source=%s/ingest_date=%s/batch=%020d.jsonl", event.SourceCoinGecko, xtime.FormatDate(claimedAt), batchID)
}

// dlqObjectKey: stage=export/date=<claim date>/export+batch+<%020d batch_id>.jsonl
func dlqObjectKey(batchID int64, claimedAt time.Time) string {
	return fmt.Sprintf("stage=%s/date=%s/export+batch+%020d.jsonl", stage, xtime.FormatDate(claimedAt), batchID)
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

// ExportOnce first finishes batches left in status 'claimed' by a previous
// (crashed) run, then claims and exports at most one new batch of up to max
// rows. It returns the number of rows covered by the batches it exported.
// Every failure counts in etl_export_batches_total{result="error"}: exportBatch
// counts its own, and a failure before any batch exists is counted here.
func ExportOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, max int) (int, error) {
	exported := 0
	defer updateLag(ctx, pool, m, log)

	pending, err := claimedBatches(ctx, pool)
	if err != nil {
		m.ExportBatches.WithLabelValues("error").Inc()
		return 0, err
	}
	for _, b := range pending {
		log.Warn("resuming claimed batch", "stage", stage, "batch", b.ID, "object_key", b.ObjectKey)
		if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
			return exported, err
		}
		exported += b.RowCount
	}

	b, ok, err := claimNext(ctx, pool, max)
	if err != nil {
		m.ExportBatches.WithLabelValues("error").Inc()
		return exported, err
	}
	if !ok {
		return exported, nil
	}
	if err := exportBatch(ctx, pool, s3c, reg, m, log, b); err != nil {
		return exported, err
	}
	return exported + b.RowCount, nil
}

// claimedBatches returns ledger rows that were claimed but never marked exported.
func claimedBatches(ctx context.Context, pool *pgxpool.Pool) ([]batch, error) {
	rows, err := pool.Query(ctx, `
SELECT batch_id, from_txid::text, from_seq, to_txid::text, to_seq, row_count, object_key, content_sha256, claimed_at
FROM pipeline.export_batches WHERE status = 'claimed' ORDER BY batch_id`)
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (batch, error) {
		var b batch
		err := r.Scan(&b.ID, &b.FromTxid, &b.FromSeq, &b.ToTxid, &b.ToSeq, &b.RowCount, &b.ObjectKey, &b.SHA256, &b.ClaimedAt)
		return b, err
	})
	if err != nil {
		return nil, fmt.Errorf("exporter: list claimed: %w", err)
	}
	return out, nil
}

// claimNext runs the short claim transaction. It returns ok=false when no
// committed row lies between the cursor and the snapshot xmin.
func claimNext(ctx context.Context, pool *pgxpool.Pool, max int) (batch, bool, error) {
	var b batch
	tx, err := pool.Begin(ctx)
	if err != nil {
		return b, false, fmt.Errorf("exporter: begin claim: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// hi: every xid below it is committed or aborted. Evaluated in its own
	// statement; the SELECT below runs on a newer snapshot (READ COMMITTED),
	// in which every transaction below hi is already resolved.
	var hi string
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT pg_snapshot_xmin(pg_current_snapshot())::text, now()`).Scan(&hi, &now); err != nil {
		return b, false, fmt.Errorf("exporter: watermark: %w", err)
	}

	b.FromTxid, b.FromSeq, err = lastBatchEnd(ctx, tx, false)
	if err != nil {
		return b, false, fmt.Errorf("exporter: cursor: %w", err)
	}

	// Sort on the xid8 column, not on its text alias (see lastBatchEnd): the
	// last row read is the batch's upper bound.
	rows, err := tx.Query(ctx, `
SELECT ingest_txid::text AS txid_s, seq FROM raw.api_calls
WHERE outcome = 'ok'
  AND (ingest_txid, seq) > ($1::text::xid8, $2::bigint)
  AND ingest_txid < $3::text::xid8
ORDER BY api_calls.ingest_txid, api_calls.seq
LIMIT $4`, b.FromTxid, b.FromSeq, hi, max)
	if err != nil {
		return b, false, fmt.Errorf("exporter: select range: %w", err)
	}
	for rows.Next() {
		if err := rows.Scan(&b.ToTxid, &b.ToSeq); err != nil {
			rows.Close()
			return b, false, fmt.Errorf("exporter: scan range: %w", err)
		}
		b.RowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, false, fmt.Errorf("exporter: select range: %w", err)
	}
	if b.RowCount == 0 {
		return b, false, nil
	}

	// Take the id first so the object key can be stored in the same INSERT.
	// pg_get_serial_sequence also works for identity columns.
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('pipeline.export_batches', 'batch_id'))`).Scan(&b.ID); err != nil {
		return b, false, fmt.Errorf("exporter: next batch id: %w", err)
	}
	b.ClaimedAt = now
	b.ObjectKey = rawObjectKey(b.ID, now)
	_, err = tx.Exec(ctx, `
INSERT INTO pipeline.export_batches
  (batch_id, from_txid, from_seq, to_txid, to_seq, watermark_xmin, row_count, object_key, claimed_at)
VALUES ($1, $2::text::xid8, $3, $4::text::xid8, $5, $6::text::xid8, $7, $8, $9)`,
		b.ID, b.FromTxid, b.FromSeq, b.ToTxid, b.ToSeq, hi, b.RowCount, b.ObjectKey, b.ClaimedAt)
	if err != nil {
		return b, false, fmt.Errorf("exporter: insert batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return b, false, fmt.Errorf("exporter: commit claim: %w", err)
	}
	return b, true, nil
}

// querier is the part of pgxpool.Pool and pgx.Tx that lastBatchEnd needs.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// lastBatchEnd returns the upper bound (to_txid, to_seq) of the highest batch
// in the ledger, or of the highest exported batch when exportedOnly is set;
// ("0", 0) when there is none.
//
// The text cast gets its own alias and ORDER BY names the table column: an
// unqualified "to_txid" would resolve to the text output column, and text
// sorts "996" above "1007", which sent the cursor backwards once txids
// passed 1000.
func lastBatchEnd(ctx context.Context, q querier, exportedOnly bool) (txid string, seq int64, err error) {
	sql := `
SELECT to_txid::text AS to_txid_s, to_seq FROM pipeline.export_batches
ORDER BY export_batches.to_txid DESC, export_batches.to_seq DESC LIMIT 1`
	if exportedOnly {
		sql = `
SELECT to_txid::text AS to_txid_s, to_seq FROM pipeline.export_batches WHERE status = 'exported'
ORDER BY export_batches.to_txid DESC, export_batches.to_seq DESC LIMIT 1`
	}
	err = q.QueryRow(ctx, sql).Scan(&txid, &seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return "0", 0, nil
	}
	return txid, seq, err
}

// exportBatch runs steps 2-4 for a claimed batch. No transaction is open
// while S3 is called.
func exportBatch(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, b batch) (err error) {
	defer func() {
		if err != nil {
			m.ExportBatches.WithLabelValues("error").Inc()
		}
	}()

	rows, err := readRange(ctx, pool, b)
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

	// Persist (or verify) the checksum before the PUT: a rebuilt batch must
	// produce the same bytes as the first attempt.
	if b.SHA256 != nil {
		if !bytes.Equal(b.SHA256, sum[:]) {
			log.Error("export batch is not deterministic", "stage", stage, "batch", b.ID,
				"stored_sha256", hex.EncodeToString(b.SHA256), "new_sha256", hex.EncodeToString(sum[:]))
			return fmt.Errorf("exporter: batch %d: content checksum changed between attempts", b.ID)
		}
	} else {
		if _, err := pool.Exec(ctx, `UPDATE pipeline.export_batches SET content_sha256 = $2 WHERE batch_id = $1 AND content_sha256 IS NULL`, b.ID, sum[:]); err != nil {
			return fmt.Errorf("exporter: store checksum: %w", err)
		}
	}

	if len(dlqLines) > 0 {
		key := dlqObjectKey(b.ID, b.ClaimedAt)
		created, err := put(ctx, s3c, m, s3.BucketDLQ, key, joinLines(dlqLines))
		if err != nil {
			return err
		}
		if created { // a resumed batch must not count its DLQ records twice
			for _, c := range dlqClasses {
				m.DLQ.WithLabelValues(stage, c).Inc()
			}
		}
		log.Error("raw events sent to dlq", "stage", stage, "batch", b.ID, "bucket", s3.BucketDLQ, "object_key", key, "count", len(dlqLines))
	}

	created, err := put(ctx, s3c, m, s3.BucketRaw, b.ObjectKey, body)
	if err != nil {
		return err
	}

	if _, err := pool.Exec(ctx, `UPDATE pipeline.export_batches SET status = 'exported', exported_at = now() WHERE batch_id = $1 AND status = 'claimed'`, b.ID); err != nil {
		return fmt.Errorf("exporter: mark exported: %w", err)
	}
	m.ExportBatches.WithLabelValues("exported").Inc()
	log.Info("raw batch saved", "stage", stage, "batch", b.ID, "bucket", s3.BucketRaw, "object_key", b.ObjectKey,
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

func readRange(ctx context.Context, pool *pgxpool.Pool, b batch) ([]apiRow, error) {
	rows, err := pool.Query(ctx, `
SELECT seq, event_id::text, idempotency_key, schema_version, source, endpoint, request::text,
       fetched_at, outcome, http::text, error, payload::text, encode(payload_sha256, 'hex')
FROM raw.api_calls
WHERE outcome = 'ok'
  AND (ingest_txid, seq) >  ($1::text::xid8, $2::bigint)
  AND (ingest_txid, seq) <= ($3::text::xid8, $4::bigint)
ORDER BY api_calls.ingest_txid, api_calls.seq`, b.FromTxid, b.FromSeq, b.ToTxid, b.ToSeq)
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

// updateLag sets etl_export_lag_seconds = now - fetched_at of the oldest ok
// row after the last exported batch (0 when everything is exported).
func updateLag(ctx context.Context, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) {
	if ctx.Err() != nil {
		return
	}
	txid, seq, err := lastBatchEnd(ctx, pool, true)
	if err != nil {
		log.Warn("export lag: cursor query failed", "stage", stage, "err", err)
		return
	}
	var lag float64
	err = pool.QueryRow(ctx, `
SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(fetched_at)), 0)::float8
FROM raw.api_calls
WHERE outcome = 'ok' AND (ingest_txid, seq) > ($1::text::xid8, $2::bigint)`, txid, seq).Scan(&lag)
	if err != nil {
		log.Warn("export lag query failed", "stage", stage, "err", err)
		return
	}
	if lag < 0 {
		lag = 0
	}
	m.ExportLag.Set(lag)
}

// Run holds the exporter advisory lock on a dedicated connection for the
// whole process lifetime and calls ExportOnce every cfg.ExportInterval,
// draining immediately while full batches keep coming. It returns nil when
// ctx is cancelled and an error when the lock cannot be taken or is lost.
func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error {
	conn, err := acquireLock(ctx, pool, log)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer releaseLock(conn, log)
	log.Info("exporter lock acquired", "stage", stage)

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
		for {
			n, err := ExportOnce(ctx, pool, s3c, reg, m, log, cfg.ExportBatchMax)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Error("export failed", "stage", stage, "err", err)
				break
			}
			if n < cfg.ExportBatchMax {
				break
			}
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
