// Package publisher implements one run of `etl publish`: it publishes one closed
// raw window. It reads the window's _SUCCESS from s3://raw (missing: ErrNotReady),
// reads and verifies every listed part before producing anything, sends invalid
// lines to one deterministic DLQ object per window and produces every valid line
// verbatim, keyed by its idempotency_key. It never touches Postgres.
package publisher

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
)

const (
	stage            = "publish"
	errClassDecode   = "decode_error"
	errClassSchema   = "schema_violation"
	errClassOutcome  = "unexpected_outcome"
	maxMessageLength = 1024
)

var (
	// ErrNotReady: the exporter has not closed the window (no _SUCCESS yet).
	ErrNotReady = errors.New("window not closed by the exporter: no _SUCCESS")
	// ErrIntegrity: _SUCCESS is malformed, is for another window, or does not
	// match the parts it lists. A rerun does not fix it.
	ErrIntegrity = errors.New("raw window does not match its _SUCCESS")
)

// Deps are the clients of one run.
type Deps struct {
	S3       *s3.Client
	Producer *kgo.Client
	Reg      *validation.Registry
	Topic    string
	Log      *slog.Logger
}

// Result describes one published window: Lines is every line of its parts,
// Records the lines produced, DLQLines the lines sent to the publish DLQ.
type Result struct {
	Window                          window.Window
	Parts, Lines, Records, DLQLines int
}

// part is one raw part of the window, read and verified.
type part struct {
	key   string
	lines [][]byte
}

// PublishWindow publishes w. It returns nil only after the broker acknowledged
// every record. A rerun of the same window produces the same records again
// (the transformer drops the copies as replay) and rewrites the same DLQ bytes.
func PublishWindow(ctx context.Context, d Deps, w window.Window) (Result, error) {
	res := Result{Window: w}
	man, err := readManifest(ctx, d.S3, w)
	if err != nil {
		return res, err
	}
	parts, err := readParts(ctx, d.S3, man)
	if err != nil {
		return res, err
	}
	res.Parts = len(parts)

	var (
		recs      []*kgo.Record
		dlq       []dlqRecord
		originals [][]byte
	)
	for _, p := range parts {
		for j, line := range p.lines {
			res.Lines++
			rec, bad := classify(d.Reg, d.Topic, w, p.key, j+1, line)
			if bad != nil {
				dlq = append(dlq, *bad)
				originals = append(originals, line)
				continue
			}
			recs = append(recs, rec)
		}
	}
	res.DLQLines = len(dlq)

	// The DLQ object goes first, as before: a run that then fails to produce
	// leaves an object that the rerun rewrites byte for byte.
	if len(dlq) > 0 {
		if err := writeDLQ(ctx, d.S3, d.Reg, d.Log, w, dlq, originals); err != nil {
			return res, err
		}
	}
	if len(recs) > 0 {
		// ProduceSync waits for every ack; FirstErr is the first failed record.
		if err := d.Producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			return res, fmt.Errorf("produce window %s: %w", xtime.Format(w.Start), err)
		}
	}
	res.Records = len(recs)
	return res, nil
}

// readManifest reads and checks the _SUCCESS of w. A missing object is
// ErrNotReady; any other S3 error is returned as is (transient).
func readManifest(ctx context.Context, s3c *s3.Client, w window.Window) (window.Success, error) {
	key := window.RawSuccessKey(w)
	b, err := s3c.Get(ctx, s3.BucketRaw, key)
	if errors.Is(err, s3.ErrNotFound) {
		return window.Success{}, fmt.Errorf("%w: s3://%s/%s", ErrNotReady, s3.BucketRaw, key)
	}
	if err != nil {
		return window.Success{}, fmt.Errorf("read %s: %w", key, err)
	}
	s, err := window.ParseSuccess(b)
	if err != nil {
		return window.Success{}, fmt.Errorf("%w: %s: %v", ErrIntegrity, key, err)
	}
	if err := checkManifest(s, w); err != nil {
		return window.Success{}, fmt.Errorf("%w: %s: %v", ErrIntegrity, key, err)
	}
	return s, nil
}

var partName = regexp.MustCompile(`^part-[0-9]{5}\.jsonl$`)

// checkManifest checks that a parsed manifest describes w: the same window
// bounds, and parts that are part objects of w. An empty window is valid.
func checkManifest(s window.Success, w window.Window) error {
	if start, end := xtime.Format(w.Start), xtime.Format(w.End); s.WindowStart != start || s.WindowEnd != end {
		return fmt.Errorf("manifest is for %s..%s, expected %s..%s; is ETL_WINDOW the same for exporter and publisher?",
			s.WindowStart, s.WindowEnd, start, end)
	}
	prefix := window.RawWindowPrefix(w)
	for _, p := range s.Parts {
		name, ok := strings.CutPrefix(p.Key, prefix)
		if !ok || !partName.MatchString(name) {
			return fmt.Errorf("part key %s is not %spart-NNNNN.jsonl", p.Key, prefix)
		}
	}
	return nil
}

// readParts reads every part listed in s and checks its sha256 and line
// count. It reads them all before anything is produced, so a bad part never
// leaves the parts before it half-published. Memory is the whole window: a
// few MB at 5m, about 30 MB at 60m.
func readParts(ctx context.Context, s3c *s3.Client, s window.Success) ([]part, error) {
	parts := make([]part, 0, len(s.Parts))
	for _, p := range s.Parts {
		body, err := s3c.Get(ctx, s3.BucketRaw, p.Key)
		if errors.Is(err, s3.ErrNotFound) {
			return nil, fmt.Errorf("%w: _SUCCESS lists missing part %s", ErrIntegrity, p.Key)
		}
		if err != nil {
			return nil, fmt.Errorf("read part %s: %w", p.Key, err)
		}
		if got := event.SHA256Hex(body); got != p.SHA256 {
			return nil, fmt.Errorf("%w: sha256 mismatch for %s: manifest %s, object %s", ErrIntegrity, p.Key, p.SHA256, got)
		}
		lines := splitLines(body)
		if len(lines) != p.Rows {
			return nil, fmt.Errorf("%w: %s has %d lines, manifest says %d", ErrIntegrity, p.Key, len(lines), p.Rows)
		}
		parts = append(parts, part{key: p.Key, lines: lines})
	}
	return parts, nil
}

// classify turns one raw line into either a broker record or a DLQ record.
// lineNo counts from 1 within the part.
func classify(reg *validation.Registry, topic string, w window.Window, partKey string, lineNo int, line []byte) (*kgo.Record, *dlqRecord) {
	bad := func(class string, viols []validation.Violation) (*kgo.Record, *dlqRecord) {
		r := newDLQRecord(w, partKey, lineNo, line, class, viols)
		return nil, &r
	}
	viols, err := reg.Validate(validation.IDRawEvent, line)
	if err != nil { // not JSON
		return bad(errClassDecode, []validation.Violation{{Message: err.Error()}})
	}
	if len(viols) > 0 {
		return bad(errClassSchema, viols)
	}
	ev, err := event.Unmarshal(line)
	if err != nil {
		return bad(errClassDecode, []validation.Violation{{Message: err.Error()}})
	}
	// A guard: the exporter exports only ok rows, and only they have a key.
	if ev.Outcome != event.OutcomeOK || ev.IdempotencyKey == nil {
		return bad(errClassOutcome, []validation.Violation{{Message: fmt.Sprintf("outcome %s: only ok events are published", ev.Outcome)}})
	}
	return &kgo.Record{
		Topic: topic,
		Key:   []byte(queue.MessageKey(ev)),
		Value: line, // verbatim bytes of the raw JSONL line
		Headers: []kgo.RecordHeader{
			{Key: queue.HeaderEventID, Value: []byte(ev.EventID)},
			{Key: queue.HeaderIdempotencyKey, Value: []byte(*ev.IdempotencyKey)},
			{Key: queue.HeaderSchema, Value: []byte(validation.IDRawEvent)},
			{Key: queue.HeaderRawObject, Value: []byte(partKey)},
			{Key: queue.HeaderLineNo, Value: []byte(strconv.Itoa(lineNo))},
		},
	}, nil
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

// dlqRecord is one line of a DLQ object (schema pipeline/dlq_record.v1.json).
// The exporter and the transformer write the same shape.
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

// newDLQRecord builds a DLQ record for one bad line of part partKey of w.
// Nothing in it depends on the clock: occurred_at is the window end, so a
// rerun rebuilds byte-identical DLQ lines and the write-once PUT gets 412
// with an equal checksum. event_id is copied only when the line carries a
// valid UUID.
func newDLQRecord(w window.Window, partKey string, lineNo int, line []byte, class string, viols []validation.Violation) dlqRecord {
	r := dlqRecord{
		SchemaVersion:  1,
		Stage:          stage,
		ErrorClass:     class,
		Violations:     make([]validation.Violation, 0, len(viols)), // [] not null
		Origin:         dlqOrigin{Kind: "s3", Bucket: s3.BucketRaw, ObjectKey: partKey, LineNo: lineNo},
		OriginalBase64: base64.StdEncoding.EncodeToString(line),
		OccurredAt:     xtime.Format(w.End),
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

// writeDLQ validates each DLQ record against its own schema
// (pipeline/dlq_record.v1.json) and writes the valid ones as the window's one
// write-once DLQ object. A record that fails that schema is logged at ERROR
// with the original bytes in base64 and left out.
func writeDLQ(ctx context.Context, s3c *s3.Client, reg *validation.Registry, log *slog.Logger, w window.Window, recs []dlqRecord, originals [][]byte) error {
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
				"object_key", r.Origin.ObjectKey, "line_no", r.Origin.LineNo, "error", err,
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
	key := window.PublishDLQKey(w)
	created, err := s3c.PutIfAbsent(ctx, s3.BucketDLQ, key, buf.Bytes())
	switch {
	case errors.Is(err, s3.ErrChecksumMismatch):
		// DLQ lines are deterministic, so this only happens when the
		// validator's messages changed between runs (a new build). Keep the
		// first object.
		log.Error("dlq object exists with different bytes; keeping the first", "key", key)
		return nil
	case err != nil:
		return fmt.Errorf("put dlq object %s: %w", key, err)
	}
	log.Error("records sent to dlq", "count", written, "key", key, "created", created)
	return nil
}
