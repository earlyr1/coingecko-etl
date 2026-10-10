// Package publisher leases exported raw batches, validates each JSONL line
// against S1 and produces it verbatim to the broker.
package publisher

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
)

const (
	stage            = "publish"
	schemaLabel      = "raw_event.v1"
	errClassDecode   = "decode_error"
	errClassSchema   = "schema_violation"
	maxMessageLength = 1024
)

// leaseSQL leases the oldest exported, unpublished batch whose lease is free
// or expired. FOR UPDATE SKIP LOCKED lets several publishers run without
// blocking each other; the single statement is its own short transaction.
const leaseSQL = `
UPDATE pipeline.export_batches
   SET publish_lease_until = now() + make_interval(secs => $1)
 WHERE batch_id = (
        SELECT batch_id
          FROM pipeline.export_batches
         WHERE status = 'exported'
           AND published_at IS NULL
           AND (publish_lease_until IS NULL OR publish_lease_until < now())
         ORDER BY batch_id
         LIMIT 1
         FOR UPDATE SKIP LOCKED)
RETURNING batch_id, object_key, COALESCE(exported_at, claimed_at)`

const markSQL = `
UPDATE pipeline.export_batches
   SET published_at = now(), publish_lease_until = NULL
 WHERE batch_id = $1 AND published_at IS NULL`

type batch struct {
	ID         int64
	ObjectKey  string
	ExportedAt time.Time // DLQ occurred_at and date: stable across retries of the batch
}

// leaseBatch returns ok=false when no batch is available.
func leaseBatch(ctx context.Context, pool *pgxpool.Pool, lease time.Duration) (batch, bool, error) {
	var b batch
	err := pool.QueryRow(ctx, leaseSQL, lease.Seconds()).Scan(&b.ID, &b.ObjectKey, &b.ExportedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return batch{}, false, nil
	}
	if err != nil {
		return batch{}, false, fmt.Errorf("lease batch: %w", err)
	}
	return b, true, nil
}

func markPublished(ctx context.Context, pool *pgxpool.Pool, batchID int64) error {
	if _, err := pool.Exec(ctx, markSQL, batchID); err != nil {
		return fmt.Errorf("mark batch %d published: %w", batchID, err)
	}
	return nil
}

// splitLines splits a JSONL object into lines. The trailing newline does not
// produce an empty last line; empty lines in the middle are kept (they fail
// validation and go to the DLQ).
func splitLines(body []byte) [][]byte {
	if len(body) == 0 {
		return nil
	}
	lines := bytes.Split(body, []byte("\n"))
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// dlqRecord is one line of a DLQ object (schema pipeline/dlq_record.v1.json,
// Task 3). The exporter and the transformer write the same shape.
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
	Kind      string `json:"kind"` // "s3"
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	LineNo    int    `json:"line_no"`
	BatchID   int64  `json:"batch_id"`
	EventID   string `json:"event_id,omitempty"`
}

// truncate caps a violation message at maxMessageLength bytes without
// splitting a UTF-8 sequence.
func truncate(s string) string {
	if len(s) <= maxMessageLength {
		return s
	}
	n := maxMessageLength
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// newDLQRecord builds a DLQ record for one bad line. Nothing in it depends on
// the clock: occurred_at is the batch's exported_at, so a retried batch
// rebuilds byte-identical DLQ lines and the write-once PUT gets 412 with an
// equal checksum. event_id is copied only when the line carries a valid UUID.
func newDLQRecord(b batch, lineNo int, line []byte, class string, viols []validation.Violation) dlqRecord {
	r := dlqRecord{
		SchemaVersion:  1,
		Stage:          stage,
		ErrorClass:     class,
		Violations:     make([]validation.Violation, 0, len(viols)), // [] not null
		Origin:         dlqOrigin{Kind: "s3", Bucket: s3.BucketRaw, ObjectKey: b.ObjectKey, LineNo: lineNo, BatchID: b.ID},
		OriginalBase64: base64.StdEncoding.EncodeToString(line),
		OccurredAt:     xtime.Format(b.ExportedAt),
	}
	for _, v := range viols {
		v.Message = truncate(v.Message)
		r.Violations = append(r.Violations, v)
	}
	var ids struct {
		EventID string `json:"event_id"`
	}
	if json.Unmarshal(line, &ids) == nil {
		if _, err := uuid.Parse(ids.EventID); err == nil {
			r.Origin.EventID = ids.EventID
		}
	}
	return r
}

// dlqObjectKey follows the DLQ layout:
// stage=publish/date=<UTC date of exported_at>/publish+batch+<%020d batch_id>.jsonl.
// The date comes from the batch, not the clock, so a retry writes the same key.
func dlqObjectKey(exportedAt time.Time, batchID int64) string {
	return fmt.Sprintf("stage=%s/date=%s/publish+batch+%020d.jsonl", stage, xtime.FormatDate(exportedAt), batchID)
}

// writeDLQ validates each DLQ record against its own schema
// (pipeline/dlq_record.v1.json) and writes the valid ones as one write-once
// object. A record that fails that schema is logged at ERROR with the original
// bytes in base64 and left out. log already carries stage and batch.
func writeDLQ(ctx context.Context, s3c *s3.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger, b batch, recs []dlqRecord, originals [][]byte) error {
	var buf bytes.Buffer
	written := 0
	for i, r := range recs {
		line, err := json.Marshal(r)
		if err == nil {
			var viols []validation.Violation
			viols, err = reg.Validate(validation.IDDLQRecord, line)
			if err == nil && len(viols) > 0 {
				err = fmt.Errorf("dlq record violates schema: %s at %s", viols[0].Message, viols[0].InstanceLocation)
			}
		}
		if err != nil {
			log.Error("dlq record invalid; original logged",
				"line_no", r.Origin.LineNo, "err", err,
				"original_base64", base64.StdEncoding.EncodeToString(originals[i]))
			continue
		}
		buf.Write(line)
		buf.WriteByte('\n')
		written++
	}
	if written == 0 {
		return nil
	}
	key := dlqObjectKey(b.ExportedAt, b.ID)
	created, err := s3c.PutIfAbsent(ctx, s3.BucketDLQ, key, buf.Bytes())
	switch {
	case errors.Is(err, s3.ErrChecksumMismatch):
		// DLQ lines are deterministic, so this only happens when the
		// validator's messages changed between attempts (a redeploy with a
		// new schema build). Keep the first object.
		m.S3Puts.WithLabelValues("mismatch").Inc()
		log.Warn("dlq object already exists from an earlier attempt", "key", key)
		return nil
	case err != nil:
		m.S3Puts.WithLabelValues("error").Inc()
		return fmt.Errorf("put dlq object %s: %w", key, err)
	case created:
		m.S3Puts.WithLabelValues("created").Inc()
	default:
		m.S3Puts.WithLabelValues("exists").Inc()
	}
	log.Error("records sent to dlq", "count", written, "key", key)
	return nil
}

// PublishOnce leases one exported batch, publishes every valid line and marks
// the batch published. It returns published=false when nothing was leased.
// Any produce error returns before the mark: the lease expires and the batch
// is republished (the transformer drops the duplicates).
func PublishOnce(ctx context.Context, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, topic string, lease time.Duration, m *obs.Metrics, log *slog.Logger) (published bool, err error) {
	b, ok, err := leaseBatch(ctx, pool, lease)
	if err != nil || !ok {
		return false, err
	}
	log = log.With("stage", stage, "batch", b.ID)

	body, err := s3c.Get(ctx, s3.BucketRaw, b.ObjectKey)
	if err != nil {
		return false, fmt.Errorf("get raw object %s: %w", b.ObjectKey, err)
	}

	var (
		recs      []*kgo.Record
		dlq       []dlqRecord
		originals [][]byte
	)
	// toDLQ counts one bad line and queues its DLQ record and original bytes.
	toDLQ := func(lineNo int, line []byte, class string, viols []validation.Violation) {
		m.DLQ.WithLabelValues(stage, class).Inc()
		dlq = append(dlq, newDLQRecord(b, lineNo, line, class, viols))
		originals = append(originals, line)
	}
	decodeViolation := func(err error) []validation.Violation {
		return []validation.Violation{{Message: err.Error()}}
	}

	for i, line := range splitLines(body) {
		lineNo := i + 1
		viols, verr := reg.Validate(validation.IDRawEvent, line)
		if verr != nil { // not JSON
			m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
			toDLQ(lineNo, line, errClassDecode, decodeViolation(verr))
			continue
		}
		if len(viols) > 0 {
			m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
			for _, v := range viols {
				m.SchemaViolations.WithLabelValues(stage, schemaLabel, validation.Keyword(v)).Inc()
			}
			toDLQ(lineNo, line, errClassSchema, viols)
			continue
		}
		m.SchemaValidations.WithLabelValues(stage, schemaLabel, "pass").Inc()

		ev, uerr := event.Unmarshal(line)
		if uerr != nil {
			toDLQ(lineNo, line, errClassDecode, decodeViolation(uerr))
			continue
		}
		idem := ""
		if ev.IdempotencyKey != nil {
			idem = *ev.IdempotencyKey
		}
		recs = append(recs, &kgo.Record{
			Topic: topic,
			Key:   []byte(queue.MessageKey(ev)),
			Value: line, // verbatim bytes of the raw JSONL line
			Headers: []kgo.RecordHeader{
				{Key: queue.HeaderEventID, Value: []byte(ev.EventID)},
				{Key: queue.HeaderIdempotencyKey, Value: []byte(idem)},
				{Key: queue.HeaderSchema, Value: []byte(validation.IDRawEvent)},
				{Key: queue.HeaderRawObject, Value: []byte(b.ObjectKey)},
				{Key: queue.HeaderLineNo, Value: []byte(strconv.Itoa(lineNo))},
			},
		})
	}

	if len(dlq) > 0 {
		if err := writeDLQ(ctx, s3c, reg, m, log, b, dlq, originals); err != nil {
			return false, err
		}
	}

	if len(recs) > 0 {
		// ProduceSync produces every record (batched by the client) and waits
		// for all acks; FirstErr is the first per-record failure.
		if err := prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			return false, fmt.Errorf("produce batch %d: %w", b.ID, err)
		}
		if err := prod.Flush(ctx); err != nil {
			return false, fmt.Errorf("flush batch %d: %w", b.ID, err)
		}
	}

	if err := markPublished(ctx, pool, b.ID); err != nil {
		return false, err
	}
	m.PublishedRecords.Add(float64(len(recs)))
	log.Info("batch published", "object_key", b.ObjectKey, "records", len(recs), "dlq", len(dlq))
	return true, nil
}

// Run publishes batches until ctx is cancelled. It drains back to back while
// batches are available; after an idle pass or an error it sleeps
// cfg.PublishPoll.
func Run(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, s3c *s3.Client, prod *kgo.Client, reg *validation.Registry, m *obs.Metrics, log *slog.Logger) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		published, err := PublishOnce(ctx, pool, s3c, prod, reg, cfg.Topic, cfg.PublishLease, m, log)
		if err != nil && ctx.Err() == nil {
			log.Error("publish failed", "stage", stage, "err", err)
		}
		if published && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.PublishPoll):
		}
	}
}
