package publisher

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

// w5 is the window [14:00, 14:05) of 2026-10-12 that the unit tests use.
var w5 = window.Window{
	Start: time.Date(2026, 10, 12, 14, 0, 0, 0, time.UTC),
	End:   time.Date(2026, 10, 12, 14, 5, 0, 0, time.UTC),
}

func loadReg(t *testing.T) *validation.Registry {
	t.Helper()
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// fixtureLine reads one S1 example as a JSONL line (without its newline).
func fixtureLine(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schemas/testdata/pipeline/raw_event.v1/valid/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimRight(b, "\n")
}

func TestSplitLines(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a\n", []string{"a"}},
		{"a\nb\n", []string{"a", "b"}},
		{"a\nb", []string{"a", "b"}},
		{"a\n\nb\n", []string{"a", "", "b"}},
	}
	for _, c := range cases {
		got := splitLines([]byte(c.in))
		if len(got) != len(c.want) {
			t.Fatalf("splitLines(%q) = %d lines, want %d", c.in, len(got), len(c.want))
		}
		for i := range got {
			if string(got[i]) != c.want[i] {
				t.Fatalf("splitLines(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestCheckManifest(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	twoParts := window.NewSuccess(w5, []window.SuccessPart{
		{Key: window.RawPartKey(w5, 0), SHA256: sha, Rows: 2},
		{Key: window.RawPartKey(w5, 1), SHA256: sha, Rows: 1},
	})
	if err := checkManifest(twoParts, w5); err != nil {
		t.Fatalf("valid 2-part manifest: %v", err)
	}
	if err := checkManifest(window.NewSuccess(w5, nil), w5); err != nil {
		t.Fatalf("empty window: %v", err)
	}

	withParts := func(keys ...string) window.Success {
		var parts []window.SuccessPart
		for _, k := range keys {
			parts = append(parts, window.SuccessPart{Key: k, SHA256: sha, Rows: 1})
		}
		return window.NewSuccess(w5, parts)
	}
	ten := window.Window{Start: w5.Start, End: w5.Start.Add(10 * time.Minute)}
	earlier := window.Window{Start: w5.Start.Add(-5 * time.Minute), End: w5.End}
	next := window.Window{Start: w5.End, End: w5.End.Add(5 * time.Minute)}
	prefix := window.RawWindowPrefix(w5)
	cases := []struct {
		name string
		s    window.Success
		want string // substring of the error
	}{
		{"wrong window_start", window.NewSuccess(earlier, nil), "ETL_WINDOW"},
		{"10-minute manifest checked as a 5-minute window", window.NewSuccess(ten, nil), "ETL_WINDOW"},
		{"part of another window", withParts(window.RawPartKey(next, 0)), window.RawPartKey(next, 0)},
		{"short part index", withParts(prefix + "part-1.jsonl"), "part-1.jsonl"},
		{"not a part", withParts(prefix + "_SUCCESS"), "_SUCCESS"},
		{"part in a subdirectory", withParts(prefix + "x/part-00000.jsonl"), "x/part-00000.jsonl"},
		{"suffix after the part name", withParts(prefix + "part-00000.jsonl.tmp"), "part-00000.jsonl.tmp"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkManifest(c.s, w5)
			if err == nil {
				t.Fatal("checkManifest = nil, want an error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	reg := loadReg(t)
	partKey := window.RawPartKey(w5, 0)

	ok := fixtureLine(t, "ok_markets.json")
	ev, err := event.Unmarshal(ok)
	if err != nil {
		t.Fatal(err)
	}
	rec, dlq := classify(reg, "coingecko.raw", w5, partKey, 3, ok)
	if dlq != nil || rec == nil {
		t.Fatalf("ok line: record %v, dlq %+v", rec, dlq)
	}
	if rec.Topic != "coingecko.raw" || string(rec.Key) != *ev.IdempotencyKey || !bytes.Equal(rec.Value, ok) {
		t.Fatalf("record topic %q key %q, value equal %v", rec.Topic, rec.Key, bytes.Equal(rec.Value, ok))
	}
	wantHeaders := [][2]string{
		{queue.HeaderEventID, ev.EventID},
		{queue.HeaderIdempotencyKey, *ev.IdempotencyKey},
		{queue.HeaderSchema, validation.IDRawEvent},
		{queue.HeaderRawObject, partKey},
		{queue.HeaderLineNo, "3"},
	}
	if len(rec.Headers) != len(wantHeaders) {
		t.Fatalf("headers = %v", rec.Headers)
	}
	for i, h := range rec.Headers {
		if h.Key != wantHeaders[i][0] || string(h.Value) != wantHeaders[i][1] {
			t.Fatalf("header %d = %s=%s, want %s=%s", i, h.Key, h.Value, wantHeaders[i][0], wantHeaders[i][1])
		}
	}

	id := uuid.Must(uuid.NewV7()).String()
	bad := []struct {
		name, class, eventID string
		line                 []byte
	}{
		{"not json", errClassDecode, "", []byte("{not json")},
		{"schema violation", errClassSchema, id, []byte(`{"schema_version":1,"event_id":"` + id + `"}`)},
		{"http_error event", errClassOutcome, "0199d0a6-0000-7000-8000-000000000002", fixtureLine(t, "http_error_429.json")},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			rec, d := classify(reg, "coingecko.raw", w5, partKey, 4, c.line)
			if rec != nil || d == nil {
				t.Fatalf("record %v, dlq %+v; want only a dlq record", rec, d)
			}
			if d.ErrorClass != c.class || d.Origin.EventID != c.eventID || d.Origin.ObjectKey != partKey || d.Origin.LineNo != 4 {
				t.Fatalf("dlq record = %+v", d)
			}
			if d.OccurredAt != xtime.Format(w5.End) || len(d.Violations) == 0 {
				t.Fatalf("occurred_at %q, violations %v", d.OccurredAt, d.Violations)
			}
			b, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if viols, err := reg.Validate(validation.IDDLQRecord, b); err != nil || len(viols) > 0 {
				t.Fatalf("dlq record violates its schema: %v %v\n%s", viols, err, b)
			}
		})
	}
}

func TestDLQRecordsSatisfySchema(t *testing.T) {
	reg := loadReg(t)
	partKey := window.RawPartKey(w5, 1)
	cases := map[string][]byte{
		"valid json, wrong shape": []byte(`{"event_id":"0199c8a0-0000-7000-8000-000000000001","idempotency_key":"` + strings.Repeat("0123456789abcdef", 4) + `","extra":1}`),
		"json with bad ids":       []byte(`{"event_id":"nope","idempotency_key":"short"}`),
		"not json":                []byte("{not json"),
		"empty line":              []byte(""),
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			r := newDLQRecord(w5, partKey, 3, line, errClassSchema, []validation.Violation{{InstanceLocation: "/x", KeywordLocation: "/additionalProperties", Message: "bad"}})
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			viols, err := reg.Validate(validation.IDDLQRecord, b)
			if err != nil {
				t.Fatal(err)
			}
			if len(viols) > 0 {
				t.Fatalf("dlq record violates schema: %+v\n%s", viols, b)
			}
		})
	}
}

func TestDLQRecordKeepsOriginal(t *testing.T) {
	r := newDLQRecord(w5, "k", 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}})
	if r.OriginalBase64 != "e3g=" {
		t.Fatalf("original_base64 = %q", r.OriginalBase64)
	}
	if r.Origin.Kind != "s3" || r.Origin.Bucket != "raw" || r.Origin.ObjectKey != "k" || r.Origin.LineNo != 1 ||
		r.Stage != "publish" || r.OccurredAt != "2026-10-12T14:05:00.000Z" {
		t.Fatalf("record = %+v", r)
	}
	b1, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b1, []byte("batch_id")) {
		t.Fatalf("a publish dlq record has no batch: %s", b1)
	}
	// No clock, no random id: a rerun of the window rebuilds the same bytes.
	b2, _ := json.Marshal(newDLQRecord(w5, "k", 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}}))
	if !bytes.Equal(b1, b2) {
		t.Fatalf("dlq record is not deterministic:\n%s\n%s", b1, b2)
	}
}
