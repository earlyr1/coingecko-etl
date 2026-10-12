// Package transformer consumes raw events from the broker and writes the
// processed, rejected and dlq zones with effectively-once content (spec §7.4).
//
// Metrics: the tally-driven metrics (etl_records_total, etl_rejected_total,
// etl_dedup_dropped_total, etl_dlq_total, etl_e2e_latency_seconds and
// etl_batch_duration_seconds) count each batch once, in the call that flips
// its manifest row to written, so a retry or a zombie consumer never counts a
// batch twice. The schema validation and S3 PUT counters
// (etl_schema_validations_total, etl_schema_violations_total and
// etl_s3_put_total) count attempts: every time a batch is planned it is
// validated again, and every PUT attempt is counted.
package transformer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"coingecko-etl/internal/dedup"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message is one broker record of the raw topic.
type Message struct {
	Partition int32
	Offset    int64
	Value     []byte
	Headers   map[string]string
}

// Batch is a contiguous offset range [F..L] of one partition.
type Batch struct {
	TopicID   uuid.UUID
	Topic     string
	Partition int32
	Messages  []Message
}

// Deps are the dependencies of the transformer.
type Deps struct {
	Pool    *pgxpool.Pool
	S3      *s3.Client
	Reg     *validation.Registry
	Metrics *obs.Metrics
	Log     *slog.Logger
	Rules   transform.Rules
}

// Result: Written is true when every offset up to LastOffset is durably in the
// lake and the manifest says so; the caller commits LastOffset+1. LastOffset can
// be lower than the last message (the manifest fixed a shorter range: process
// the rest in a new batch) or higher (a written batch already covers more:
// seek past it).
type Result struct {
	Written    bool
	LastOffset int64
}

// IncompleteBatchError: an earlier attempt planned this batch with a longer
// range than the messages given. The caller must fetch up to NeedLast and call
// ProcessBatch again with the longer batch.
type IncompleteBatchError struct {
	Partition       int32
	First, NeedLast int64
	HaveLast        int64
}

func (e *IncompleteBatchError) Error() string {
	return fmt.Sprintf("transformer: partition %d: manifest planned [%d..%d], batch ends at %d; fetch more records",
		e.Partition, e.First, e.NeedLast, e.HaveLast)
}

// RewindError: a planned (unwritten) batch starts before the first message and
// covers it. The caller must seek the partition to Offset and retry.
type RewindError struct {
	Partition int32
	Offset    int64
}

func (e *RewindError) Error() string {
	return fmt.Sprintf("transformer: partition %d: planned batch starts at %d; rewind", e.Partition, e.Offset)
}

// Stage and error-class labels for dlq records and metrics.
const (
	stageInput   = "transform_input"
	stageOutput  = "transform_output"
	dlqPathStage = "transform" // the stage=<...> segment of dlq object keys

	classDecode       = "decode_error"
	classSchema       = "schema_violation"
	classIntegrity    = "integrity_error"
	classSourceSchema = "source_schema_violation"
	classTransform    = "transform_error"
	classOutputSchema = "output_schema_violation"

	reasonSnapshotConflict = "snapshot_conflict"

	processedSchemaVersion = 1

	putAttempts = 5
	putBackoff  = 250 * time.Millisecond // doubles after every failed attempt
)

// rejectedRow is one line of a rejected/ object (schema pipeline/rejected_record.v1):
// the reject envelope, the lineage columns and the row itself.
type rejectedRow struct {
	SchemaVersion int             `json:"schema_version"`
	Dataset       string          `json:"dataset"`
	Reason        string          `json:"reason"`
	RecordKey     string          `json:"record_key"`
	EventDate     string          `json:"event_date"`
	Record        json.RawMessage `json:"record"` // transform.RowJSON: business fields + lineage
	EventID       string          `json:"_event_id"`
	RawObject     string          `json:"_raw_object"`
	Partition     int32           `json:"_kafka_partition"`
	Offset        int64           `json:"_kafka_offset"`
	ProcessedAt   string          `json:"_processed_at"`
}

// dlqRow is one line of a dlq/ object written by the transformer (schema
// pipeline/dlq_record.v1; the exporter and publisher write the same shape).
type dlqRow struct {
	SchemaVersion  int                    `json:"schema_version"`
	Stage          string                 `json:"stage"`
	ErrorClass     string                 `json:"error_class"`
	Error          string                 `json:"error"`
	Violations     []validation.Violation `json:"violations"`
	Origin         dlqOrigin              `json:"origin"`
	OriginalBase64 string                 `json:"original_base64"` // message value (input) or row (output)
	OccurredAt     string                 `json:"occurred_at"`     // manifest planned_at, never now()
}

type dlqOrigin struct {
	Kind      string `json:"kind"` // "kafka"
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	EventID   string `json:"event_id,omitempty"`
	Dataset   string `json:"dataset,omitempty"`
	RecordKey string `json:"record_key,omitempty"`
}

// tally is stored as consumer_batches.counts and drives metrics once the batch is written.
type tally struct {
	Messages      int            `json:"messages"`
	Written       map[string]int `json:"written"`        // dataset -> processed rows
	Rejected      map[string]int `json:"rejected"`       // dataset -> rejected rows
	RejectReasons map[string]int `json:"reject_reasons"` // reason -> rows
	Duplicates    map[string]int `json:"duplicates"`     // dataset -> dropped rows
	DropReasons   map[string]int `json:"drop_reasons"`   // reason -> dropped rows
	DLQ           map[string]int `json:"dlq"`            // "<stage>/<error_class>" -> records
	Unwritable    int            `json:"unwritable"`     // rejected/dlq rows failing their own schema (logged only)
}

func newTally() tally {
	return tally{Written: map[string]int{}, Rejected: map[string]int{}, RejectReasons: map[string]int{},
		Duplicates: map[string]int{}, DropReasons: map[string]int{}, DLQ: map[string]int{}}
}

// object is one write-once file of the batch.
type object struct {
	Bucket, Key string
	Body        []byte
	SHA256      [32]byte
}

// plan is the durable outcome of TX1.
type plan struct {
	first, last    int64
	alreadyWritten bool // a written batch covers [first..last]: nothing to do
	plannedAt      time.Time
	objects        []object
	counts         tally
	fetchedAt      []time.Time // of valid messages, for the e2e latency metric
}

// line is one serialized row with its sort position.
type line struct {
	offset  int64
	ordinal int
	data    []byte
}

// groupKey names one object of a batch: its bucket and, for processed and
// rejected, its dataset (empty for dlq).
type groupKey struct{ bucket, dataset string }

// ProcessBatch runs the batch algorithm of spec §7.4:
// input checks, TX1 (plan + transform + dedup + validate + build objects),
// PUT each object with If-None-Match, TX2 (mark written). It never holds a
// database transaction across S3 I/O. Calling it again for the same batch,
// concurrently or after a crash at any point, produces the same objects.
func ProcessBatch(ctx context.Context, d Deps, b Batch) (Result, error) {
	start := time.Now()
	p, err := planBatch(ctx, d, b)
	if pg.IsMissingPartition(err) {
		// A replay of old raw objects or a late snapshot can carry an
		// event_date outside the dedup_claims partitions kept by init and the
		// fetcher (today-1 .. today+3). TX1 rolled back; create the partitions
		// this batch needs, outside any transaction, and plan once more.
		if perr := ensureClaimPartitions(ctx, d, b); perr != nil {
			return Result{}, errors.Join(err, perr)
		}
		d.Log.Warn("created dedup_claims partitions for an out-of-window event_date", "stage", "transform", "partition", b.Partition)
		p, err = planBatch(ctx, d, b)
	}
	if err != nil {
		return Result{}, err
	}
	if p.alreadyWritten {
		return Result{Written: true, LastOffset: p.last}, nil
	}
	for _, o := range p.objects {
		if err := putWithRetry(ctx, d, o); err != nil {
			return Result{}, err
		}
	}
	flipped, err := markWritten(ctx, d.Pool, b.TopicID, b.Partition, p.first)
	if err != nil {
		return Result{}, err
	}
	if flipped {
		recordMetrics(d, p, start)
		d.Log.Info("batch written", "stage", "transform", "partition", b.Partition,
			"batch", fmt.Sprintf("%d-%d", p.first, p.last), "objects", len(p.objects), "counts", p.counts)
	}
	return Result{Written: true, LastOffset: p.last}, nil
}

// ensureClaimPartitions creates the daily partitions (day-1 .. day) for the
// snapshot date of every decodable ok message in the batch.
func ensureClaimPartitions(ctx context.Context, d Deps, b Batch) error {
	done := map[string]bool{}
	for _, m := range b.Messages {
		ev, err := event.Unmarshal(m.Value)
		if err != nil || ev.Outcome != event.OutcomeOK {
			continue
		}
		at, err := transform.SnapshotAt(ev)
		if err != nil || done[xtime.FormatDate(at)] {
			continue
		}
		done[xtime.FormatDate(at)] = true
		if err := pg.EnsurePartitions(ctx, d.Pool, at, 0); err != nil {
			return err
		}
	}
	return nil
}

// putWithRetry writes one object with If-None-Match: *. A 412 with an equal
// checksum counts as success. A different checksum is returned at once
// (ErrChecksumMismatch): the caller halts the partition (alert and stop).
// Transient errors are retried with backoff and finally returned, so the
// caller never commits the offset.
func putWithRetry(ctx context.Context, d Deps, o object) error {
	backoff := putBackoff
	for attempt := 1; ; attempt++ {
		created, err := d.S3.PutIfAbsent(ctx, o.Bucket, o.Key, o.Body)
		switch {
		case err == nil && created:
			d.Metrics.S3Puts.WithLabelValues("created").Inc()
			return nil
		case err == nil:
			d.Metrics.S3Puts.WithLabelValues("exists").Inc()
			return nil
		case errors.Is(err, s3.ErrChecksumMismatch):
			d.Metrics.S3Puts.WithLabelValues("mismatch").Inc()
			d.Log.Error("object exists with a different checksum", "stage", "transform", "bucket", o.Bucket, "key", o.Key)
			return fmt.Errorf("put %s/%s: %w", o.Bucket, o.Key, err)
		}
		d.Metrics.S3Puts.WithLabelValues("error").Inc()
		if attempt == putAttempts || ctx.Err() != nil {
			return fmt.Errorf("put %s/%s after %d attempts: %w", o.Bucket, o.Key, attempt, err)
		}
		d.Log.Warn("put failed, retrying", "stage", "transform", "bucket", o.Bucket, "key", o.Key, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("put %s/%s: %w", o.Bucket, o.Key, ctx.Err())
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// recordMetrics counts a written batch. Only the call that flipped the
// manifest row to written calls it, so each batch is counted once.
func recordMetrics(d Deps, p *plan, start time.Time) {
	m := d.Metrics
	for ds, n := range p.counts.Written {
		m.Records.WithLabelValues(ds, "written").Add(float64(n))
	}
	for ds, n := range p.counts.Rejected {
		m.Records.WithLabelValues(ds, "rejected").Add(float64(n))
	}
	for r, n := range p.counts.RejectReasons {
		m.Rejected.WithLabelValues(r).Add(float64(n))
	}
	for ds, n := range p.counts.Duplicates {
		m.Records.WithLabelValues(ds, "duplicate").Add(float64(n))
	}
	for r, n := range p.counts.DropReasons {
		m.DedupDropped.WithLabelValues(r).Add(float64(n))
	}
	for k, n := range p.counts.DLQ {
		stage, class, _ := strings.Cut(k, "/")
		m.DLQ.WithLabelValues(stage, class).Add(float64(n))
		m.Records.WithLabelValues("none", "dlq").Add(float64(n))
	}
	now := time.Now()
	for _, t := range p.fetchedAt {
		m.E2ELatency.Observe(now.Sub(t).Seconds())
	}
	m.BatchDuration.Observe(time.Since(start).Seconds())
}

// planBatch runs steps 1 and 2 of §7.4 and commits TX1. It does no S3 I/O.
func planBatch(ctx context.Context, d Deps, b Batch) (*plan, error) {
	if err := checkBatch(b); err != nil {
		return nil, err
	}
	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	// 2a. Fix the batch range in the manifest. It comes first because its
	// planned_at is the batch's only clock: _processed_at of every row and
	// occurred_at of every dlq record.
	rng, err := fixRange(ctx, tx, b)
	if err != nil {
		return nil, err
	}
	if rng.written {
		return &plan{first: rng.first, last: rng.last, alreadyWritten: true}, nil
	}
	msgs := upTo(b.Messages, rng.last)
	p := &plan{first: rng.first, last: rng.last, plannedAt: rng.plannedAt}
	c := &collector{d: d, topic: b.Topic, partition: b.Partition, processedAt: xtime.Format(rng.plannedAt),
		counts: newTally(), groups: map[groupKey][]line{}}
	c.counts.Messages = len(msgs)

	// 1. Input checks (no DB). Failures go to dlq; the offsets stay in the batch.
	type valid struct {
		msg Message
		ev  event.RawEvent
	}
	var good []valid
	for _, m := range msgs {
		ev, class, vs, err := checkInput(d, m.Value)
		if err != nil {
			c.toDLQ(dlqRow{Stage: stageInput, ErrorClass: class, Error: err.Error(), Violations: vs,
				Origin: dlqOrigin{EventID: ev.EventID}, OriginalBase64: base64.StdEncoding.EncodeToString(m.Value)}, m.Offset, -1)
			continue
		}
		good = append(good, valid{m, ev})
		if t, err := xtime.Parse(ev.FetchedAt); err == nil {
			p.fetchedAt = append(p.fetchedAt, t)
		}
	}

	// 2b. Transform. 2c. Validate the rows bound for processed/ against S2
	// before any claim: validation is pure, so a re-plan decides the same,
	// and a row that fails goes to dlq without claiming its key. Only rows
	// bound for processed/ or rejected/ claim keys (spec §7.3), so a
	// republish after a schema fix can still write the row.
	type candidate struct {
		msg Message
		rec transform.Record
		lin transform.Lineage
		row []byte // transform.RowJSON(rec, lin)
	}
	var cands []candidate
	var claims []dedup.Claim
	for _, g := range good {
		out, err := transform.Transform(g.ev, d.Rules)
		if err != nil {
			c.toDLQ(dlqRow{Stage: stageInput, ErrorClass: classTransform, Error: err.Error(), Origin: dlqOrigin{EventID: g.ev.EventID},
				OriginalBase64: base64.StdEncoding.EncodeToString(g.msg.Value)}, g.msg.Offset, -1)
			continue
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].Ordinal < out[j].Ordinal })
		idem := ""
		if g.ev.IdempotencyKey != nil {
			idem = *g.ev.IdempotencyKey
		}
		for _, r := range out {
			lin := transform.Lineage{
				EventID: g.ev.EventID, RawObject: g.msg.Headers[queue.HeaderRawObject],
				Partition: b.Partition, Offset: g.msg.Offset, SchemaVer: processedSchemaVersion, ProcessedAt: c.processedAt,
			}
			row, err := transform.RowJSON(r, lin)
			if err != nil {
				return nil, fmt.Errorf("serialize %s %s: %w", r.Dataset, r.Key, err)
			}
			if r.Zone == transform.ZoneProcessed {
				schemaID := transform.SchemaIDFor(r.Dataset)
				vs, err := d.Reg.Validate(schemaID, row)
				if err == nil {
					observeValidation(d.Metrics, stageOutput, r.Dataset+".v1", vs)
					if len(vs) > 0 {
						err = errors.New("processed row violates " + schemaID)
					}
				}
				if err != nil {
					c.toDLQ(dlqRow{Stage: stageOutput, ErrorClass: classOutputSchema, Error: err.Error(), Violations: vs,
						Origin:         dlqOrigin{EventID: g.ev.EventID, Dataset: r.Dataset, RecordKey: r.Key},
						OriginalBase64: base64.StdEncoding.EncodeToString(row)},
						g.msg.Offset, r.Ordinal)
					continue
				}
			}
			cands = append(cands, candidate{msg: g.msg, rec: r, lin: lin, row: row})
			claims = append(claims, dedup.Claim{
				Dataset: r.Dataset, Key: r.Key, EventDate: r.EventDate,
				Pos:     dedup.Position{TopicID: b.TopicID, Partition: b.Partition, Offset: g.msg.Offset, Ordinal: r.Ordinal},
				EventID: g.ev.EventID, IdempotencyKey: idem, ContentSHA256: r.ContentSHA256,
			})
		}
	}

	// 2d. Claim natural keys and decide.
	outcomes, err := dedup.ClaimAndDecide(ctx, tx, claims)
	if err != nil {
		return nil, err
	}
	for i, cand := range cands {
		r := cand.rec
		switch outcomes[i].Decision {
		case dedup.Drop:
			c.counts.Duplicates[r.Dataset]++
			c.counts.DropReasons[outcomes[i].DropReason]++
			continue
		case dedup.Conflict:
			r.Zone, r.Reason = transform.ZoneRejected, reasonSnapshotConflict
		}
		if r.Zone == transform.ZoneRejected {
			c.toRejected(r, cand.lin, cand.row)
			continue
		}
		c.counts.Written[r.Dataset]++
		c.add(groupKey{s3.BucketProcessed, r.Dataset}, cand.msg.Offset, r.Ordinal, cand.row)
	}
	if len(c.dlq) > 0 {
		c.groups[groupKey{s3.BucketDLQ, ""}] = c.dlq
	}
	p.counts = c.counts

	// 2e. Build deterministic objects and store keys + checksums on the manifest.
	p.objects = buildObjects(c.groups, rng.plannedAt, b.TopicID, b.Partition, rng.first)
	keys := make([]string, len(p.objects))
	shas := make([][]byte, len(p.objects))
	for i, o := range p.objects {
		keys[i] = o.Bucket + "/" + o.Key
		sum := o.SHA256
		shas[i] = sum[:]
	}
	counts, err := json.Marshal(p.counts) // map keys are sorted by encoding/json
	if err != nil {
		return nil, err
	}
	if err := setObjects(ctx, tx, b.TopicID, b.Partition, rng.first, keys, shas, counts); err != nil {
		if errors.Is(err, ErrNonDeterministic) {
			// Counted like a checksum mismatch on PUT: both mean "the same
			// key would get other bytes" and both need a human.
			d.Metrics.S3Puts.WithLabelValues("mismatch").Inc()
			d.Log.Error("non-deterministic batch", "stage", "transform", "partition", b.Partition, "first", rng.first, "err", err)
		}
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit TX1: %w", err)
	}
	return p, nil
}

// checkBatch rejects a batch that is not a non-empty run of strictly
// increasing offsets of its own partition.
func checkBatch(b Batch) error {
	if len(b.Messages) == 0 {
		return errors.New("transformer: empty batch")
	}
	for i, m := range b.Messages {
		if m.Partition != b.Partition {
			return fmt.Errorf("transformer: message at offset %d is from partition %d, batch is %d", m.Offset, m.Partition, b.Partition)
		}
		if i > 0 && m.Offset <= b.Messages[i-1].Offset {
			return fmt.Errorf("transformer: offsets not increasing at index %d", i)
		}
	}
	return nil
}

// batchRange is the range of a batch as fixed in the manifest.
type batchRange struct {
	first, last int64
	plannedAt   time.Time
	written     bool // a written batch covers [first..last]: nothing to do
}

// fixRange is step 2a of §7.4. It records the batch's range as planned, or
// adopts the range a stored batch dictates:
//   - same or earlier first offset, written: skip to its end (redelivery after
//     TX2, a zombie, or a poll that starts inside a written batch);
//   - earlier first offset, planned: *RewindError (seek back to its start);
//   - later first offset (a batch starting inside the poll): trim the poll to
//     end just before it and try again;
//   - same first offset, planned by an earlier attempt: reuse its exact range;
//     *IncompleteBatchError when the poll is shorter than that range.
func fixRange(ctx context.Context, tx pgx.Tx, b Batch) (batchRange, error) {
	msgs := b.Messages
	first, last := msgs[0].Offset, msgs[len(msgs)-1].Offset
	for {
		row, inserted, err := insertPlanned(ctx, tx, b.TopicID, b.Partition, first, last)
		switch {
		case err != nil:
			return batchRange{}, err
		case inserted:
			return batchRange{first: first, last: last, plannedAt: row.PlannedAt}, nil
		case row.Status == statusWritten && row.First <= first:
			return batchRange{first: row.First, last: row.Last, written: true}, nil
		case row.First < first:
			return batchRange{}, &RewindError{Partition: b.Partition, Offset: row.First}
		case row.First > first:
			msgs = upTo(msgs, row.First-1)
			last = msgs[len(msgs)-1].Offset
		case row.Last > last:
			return batchRange{}, &IncompleteBatchError{Partition: b.Partition, First: first, NeedLast: row.Last, HaveLast: last}
		default:
			return batchRange{first: first, last: row.Last, plannedAt: row.PlannedAt}, nil
		}
	}
}

// collector gathers the serialized lines of one batch, grouped by object.
type collector struct {
	d           Deps
	topic       string
	partition   int32
	processedAt string // manifest planned_at: _processed_at of rows, occurred_at of dlq records
	counts      tally
	groups      map[groupKey][]line
	dlq         []line // the batch's single dlq object
}

func (c *collector) add(k groupKey, offset int64, ordinal int, data []byte) {
	c.groups[k] = append(c.groups[k], line{offset: offset, ordinal: ordinal, data: data})
}

// toDLQ adds one dlq line. It fills in the envelope: schema version, the
// kafka origin and occurred_at.
func (c *collector) toDLQ(r dlqRow, offset int64, ordinal int) {
	r.SchemaVersion = 1
	r.Origin.Kind, r.Origin.Topic, r.Origin.Partition, r.Origin.Offset = "kafka", c.topic, c.partition, offset
	r.OccurredAt = c.processedAt
	if r.Violations == nil {
		r.Violations = []validation.Violation{} // the schema wants [], never null
	}
	data, err := encodeS3(c.d.Reg, validation.IDDLQRecord, r)
	if err != nil {
		c.unwritable("dlq", offset, err, r.OriginalBase64)
		return
	}
	c.counts.DLQ[r.Stage+"/"+r.ErrorClass]++
	c.dlq = append(c.dlq, line{offset: offset, ordinal: ordinal, data: data})
}

// toRejected adds one rejected line for r, whose serialized row is row.
func (c *collector) toRejected(r transform.Record, lin transform.Lineage, row []byte) {
	data, err := encodeS3(c.d.Reg, validation.IDRejectedRecord, rejectedRow{
		SchemaVersion: 1, Dataset: r.Dataset, Reason: r.Reason, RecordKey: r.Key, EventDate: r.EventDate, Record: row,
		EventID: lin.EventID, RawObject: lin.RawObject, Partition: lin.Partition, Offset: lin.Offset, ProcessedAt: lin.ProcessedAt,
	})
	if err != nil {
		c.unwritable("rejected", lin.Offset, err, base64.StdEncoding.EncodeToString(row), "dataset", r.Dataset)
		return
	}
	c.counts.Rejected[r.Dataset]++
	c.counts.RejectReasons[r.Reason]++
	c.add(groupKey{s3.BucketRejected, r.Dataset}, lin.Offset, r.Ordinal, data)
}

// unwritable handles a rejected or dlq record that fails its own schema (S3):
// it is not written, and its original bytes go to the ERROR log (spec §5.2).
func (c *collector) unwritable(kind string, offset int64, err error, originalBase64 string, attrs ...any) {
	c.counts.Unwritable++
	c.d.Log.Error("cannot write "+kind+" record", append([]any{"stage", "transform", "partition", c.partition,
		"offset", offset, "err", err, "original_base64", originalBase64}, attrs...)...)
}

// encodeS3 marshals a rejected or dlq record and validates it against its
// own schema (S3) before it may be written.
func encodeS3(reg *validation.Registry, schemaID string, rec any) ([]byte, error) {
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	vs, err := reg.Validate(schemaID, data)
	if err != nil {
		return nil, err
	}
	if len(vs) > 0 {
		return nil, fmt.Errorf("record violates %s: %s at %q (%d violations)", schemaID, vs[0].Message, vs[0].InstanceLocation, len(vs))
	}
	return data, nil
}

// observeValidation counts one schema validation (result pass or fail) and
// its violations by keyword.
func observeValidation(m *obs.Metrics, stage, schema string, vs []validation.Violation) {
	result := "pass"
	if len(vs) > 0 {
		result = "fail"
	}
	m.SchemaValidations.WithLabelValues(stage, schema, result).Inc()
	for _, v := range vs {
		m.SchemaViolations.WithLabelValues(stage, schema, validation.Keyword(v)).Inc()
	}
}

// checkInput runs the §7.4 step 1 checks on one message value. Violations
// are returned as Validate sorted them, so dlq bytes are deterministic.
func checkInput(d Deps, value []byte) (event.RawEvent, string, []validation.Violation, error) {
	// Strict JSON: valid UTF-8 and no duplicate object member names.
	if !jsontext.Value(value).IsValid() {
		return event.RawEvent{}, classDecode, nil, errors.New("message value is not valid JSON (syntax, UTF-8 or duplicate names)")
	}
	vs, err := d.Reg.Validate(validation.IDRawEvent, value)
	if err != nil {
		return event.RawEvent{}, classDecode, nil, err
	}
	observeValidation(d.Metrics, stageInput, "raw_event.v1", vs)
	if len(vs) > 0 {
		return event.RawEvent{}, classSchema, vs, errors.New("raw event violates " + validation.IDRawEvent)
	}
	ev, err := event.Unmarshal(value)
	if err != nil {
		return event.RawEvent{}, classDecode, nil, err
	}
	if ev.Outcome != event.OutcomeOK || ev.PayloadSHA256 == nil {
		return ev, classIntegrity, nil, fmt.Errorf("outcome %q has no payload to transform", ev.Outcome)
	}
	if got := event.SHA256Hex(ev.Payload); got != *ev.PayloadSHA256 {
		return ev, classIntegrity, nil, fmt.Errorf("payload sha256 %s does not match payload_sha256 %s", got, *ev.PayloadSHA256)
	}
	var schemaID, schemaName string
	switch ev.Endpoint {
	case event.EndpointMarkets:
		schemaID, schemaName = validation.IDSourceMarkets, "coins_markets.v1"
	case event.EndpointCoinDetail:
		schemaID, schemaName = validation.IDSourceCoin, "coin.v1"
	default:
		return ev, classSourceSchema, nil, fmt.Errorf("unknown endpoint %q", ev.Endpoint)
	}
	vs, err = d.Reg.Validate(schemaID, ev.Payload)
	if err != nil {
		return ev, classDecode, nil, err
	}
	observeValidation(d.Metrics, stageInput, schemaName, vs)
	if len(vs) > 0 {
		return ev, classSourceSchema, vs, errors.New("payload violates " + schemaID)
	}
	return ev, "", nil, nil
}

// upTo returns the messages with offset <= last.
func upTo(msgs []Message, last int64) []Message {
	n := sort.Search(len(msgs), func(i int) bool { return msgs[i].Offset > last })
	return msgs[:n]
}

// buildObjects serializes each group: rows ordered by offset then ordinal,
// one JSON document per line, "\n" after every line. Objects are ordered by
// bucket then key so the manifest arrays are deterministic. Every object of a
// batch is named by the batch's planned_at and position (objectKey).
func buildObjects(groups map[groupKey][]line, plannedAt time.Time, topicID uuid.UUID, partition int32, first int64) []object {
	out := make([]object, 0, len(groups))
	for k, lines := range groups {
		sort.SliceStable(lines, func(i, j int) bool {
			if lines[i].offset != lines[j].offset {
				return lines[i].offset < lines[j].offset
			}
			return lines[i].ordinal < lines[j].ordinal
		})
		var buf bytes.Buffer
		for _, l := range lines {
			buf.Write(l.data)
			buf.WriteByte('\n')
		}
		body := buf.Bytes()
		out = append(out, object{Bucket: k.bucket, Key: objectKey(k.bucket, k.dataset, plannedAt, topicID, partition, first),
			Body: body, SHA256: sha256.Sum256(body)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bucket != out[j].Bucket {
			return out[i].Bucket < out[j].Bucket
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// partTimeLayout renders a batch's planned_at in its object names.
const partTimeLayout = "20060102T150405Z"

// batchID8 is the first 8 hex chars of sha256("<topic_id>|<partition>|<first_offset>"):
// the batch's position, short. A re-created topic has another topic_id.
func batchID8(topicID uuid.UUID, partition int32, firstOffset int64) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d|%d", topicID, partition, firstOffset))
	return hex.EncodeToString(sum[:4])
}

// objectKey returns the key, relative to bucket zone, of the object a batch
// writes:
//
//	processed: dataset=<dataset>/v=1/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
//	rejected:  dataset=<dataset>/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
//	dlq:       stage=transform/processed_date=<YYYY-MM-DD>/hour=<HH>/part-<T>-<id8>.jsonl
//
// The date, hour and T come from the manifest planned_at in UTC (T truncated
// to the second), id8 from batchID8. Names carry only processing time and the
// batch id8, so a re-plan of the same batch yields the same key and the lake
// does not depend on the lifetime of Postgres or the broker. Panics on a bug:
// an unknown zone, an empty dataset for processed or rejected, a non-empty
// dataset for dlq.
func objectKey(zone, dataset string, plannedAt time.Time, topicID uuid.UUID, partition int32, firstOffset int64) string {
	at := plannedAt.UTC()
	tail := fmt.Sprintf("processed_date=%s/hour=%s/part-%s-%s.jsonl",
		at.Format("2006-01-02"), at.Format("15"), at.Format(partTimeLayout), batchID8(topicID, partition, firstOffset))
	switch {
	case zone == s3.BucketProcessed && dataset != "":
		return fmt.Sprintf("dataset=%s/v=%d/%s", dataset, processedSchemaVersion, tail)
	case zone == s3.BucketRejected && dataset != "":
		return fmt.Sprintf("dataset=%s/%s", dataset, tail)
	case zone == s3.BucketDLQ && dataset == "":
		return fmt.Sprintf("stage=%s/%s", dlqPathStage, tail)
	}
	panic(fmt.Sprintf("transformer.objectKey: bad zone %q / dataset %q", zone, dataset))
}
