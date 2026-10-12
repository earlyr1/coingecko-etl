package publisher

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"
)

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

func TestDLQObjectKey(t *testing.T) {
	exportedAt := time.Date(2026, 10, 10, 23, 59, 59, 0, time.UTC)
	got := dlqObjectKey(exportedAt, 42)
	want := "stage=publish/date=2026-10-10/publish+batch+00000000000000000042.jsonl"
	if got != want {
		t.Fatalf("dlqObjectKey = %q, want %q", got, want)
	}
}

func TestDLQRecordsSatisfySchema(t *testing.T) {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	b := batch{ID: 1, ObjectKey: "source=coingecko/ingest_date=2026-10-10/window=1000/part-00001.jsonl",
		ExportedAt: time.Date(2026, 10, 10, 10, 7, 55, 0, time.UTC)}
	cases := map[string][]byte{
		"valid json, wrong shape": []byte(`{"event_id":"0199c8a0-0000-7000-8000-000000000001","idempotency_key":"` + sixtyFourHex() + `","extra":1}`),
		"json with bad ids":       []byte(`{"event_id":"nope","idempotency_key":"short"}`),
		"not json":                []byte("{not json"),
		"empty line":              []byte(""),
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			r := newDLQRecord(b, 3, line, errClassSchema, []validation.Violation{{InstanceLocation: "/x", KeywordLocation: "/additionalProperties", Message: "bad"}})
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
	b := batch{ID: 7, ObjectKey: "k", ExportedAt: time.Date(2026, 10, 10, 23, 59, 59, 0, time.UTC)}
	r := newDLQRecord(b, 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}})
	if r.OriginalBase64 != "e3g=" {
		t.Fatalf("original_base64 = %q", r.OriginalBase64)
	}
	if r.Origin.Kind != "s3" || r.Origin.Bucket != "raw" || r.Origin.ObjectKey != "k" || r.Origin.LineNo != 1 ||
		r.Origin.BatchID != 7 || r.Stage != "publish" || r.OccurredAt != "2026-10-10T23:59:59.000Z" {
		t.Fatalf("record = %+v", r)
	}
	// No clock, no random id: a retried batch rebuilds the same bytes.
	again := newDLQRecord(b, 1, []byte("{x"), errClassDecode, []validation.Violation{{Message: "m"}})
	b1, _ := json.Marshal(r)
	b2, _ := json.Marshal(again)
	if !bytes.Equal(b1, b2) {
		t.Fatalf("dlq record is not deterministic:\n%s\n%s", b1, b2)
	}
}

func sixtyFourHex() string {
	const h = "0123456789abcdef"
	b := make([]byte, 64)
	for i := range b {
		b[i] = h[i%16]
	}
	return string(b)
}
