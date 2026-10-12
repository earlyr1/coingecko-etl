package window

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLayoutKeys(t *testing.T) {
	w := Window{Start: utc("2026-10-10T23:55:00Z"), End: utc("2026-10-11T00:00:00Z")}
	cases := []struct{ name, got, want string }{
		{"RawWindowPrefix", RawWindowPrefix(w), "source=coingecko/ingest_date=2026-10-10/window=2355/"},
		{"RawPartKey", RawPartKey(w, 3), "source=coingecko/ingest_date=2026-10-10/window=2355/part-00003.jsonl"},
		{"RawSuccessKey", RawSuccessKey(w), "source=coingecko/ingest_date=2026-10-10/window=2355/_SUCCESS"},
		{"ExportDLQKey", ExportDLQKey(w, 3), "stage=export/ingest_date=2026-10-10/window=2355/part-00003.jsonl"},
		{"PublishDLQKey", PublishDLQKey(w), "stage=publish/ingest_date=2026-10-10/window=2355/part-00000.jsonl"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func TestSuccessMarshal(t *testing.T) {
	w := Window{Start: utc("2026-10-10T10:00:00Z"), End: utc("2026-10-10T10:05:00Z")}
	const empty = `{"window_start":"2026-10-10T10:00:00.000Z","window_end":"2026-10-10T10:05:00.000Z","rows":0,"parts":[]}`
	for _, parts := range [][]SuccessPart{nil, {}} {
		b, err := NewSuccess(w, parts).Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != empty {
			t.Fatalf("empty window (parts %#v) marshals to %s, want %s", parts, b, empty)
		}
	}
	if b, err := (Success{WindowStart: "2026-10-10T10:00:00.000Z", WindowEnd: "2026-10-10T10:05:00.000Z"}).Marshal(); err != nil || string(b) != empty {
		t.Fatalf("a zero Success marshals to %s (%v), want %s", b, err, empty)
	}

	s := NewSuccess(w, []SuccessPart{
		{Key: RawPartKey(w, 0), SHA256: shaA, Rows: 3},
		{Key: RawPartKey(w, 1), SHA256: shaB, Rows: 4},
	})
	if s.Rows != 7 {
		t.Fatalf("Rows = %d, want 7", s.Rows)
	}
	b, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"window_start":"2026-10-10T10:00:00.000Z","window_end":"2026-10-10T10:05:00.000Z","rows":7,"parts":[` +
		`{"key":"source=coingecko/ingest_date=2026-10-10/window=1000/part-00000.jsonl","sha256":"` + shaA + `","rows":3},` +
		`{"key":"source=coingecko/ingest_date=2026-10-10/window=1000/part-00001.jsonl","sha256":"` + shaB + `","rows":4}]}`
	if string(b) != want {
		t.Fatalf("Marshal =\n%s\nwant\n%s", b, want)
	}
	again, err := s.Marshal()
	if err != nil || !bytes.Equal(again, b) {
		t.Fatalf("second Marshal differs: %s (%v)", again, err)
	}
	back, err := ParseSuccess(b)
	if err != nil {
		t.Fatalf("ParseSuccess: %v", err)
	}
	if !reflect.DeepEqual(back, s) {
		t.Fatalf("round trip = %+v, want %+v", back, s)
	}
	emptyBack, err := ParseSuccess([]byte(empty))
	if err != nil {
		t.Fatalf("ParseSuccess(empty): %v", err)
	}
	if !reflect.DeepEqual(emptyBack, NewSuccess(w, nil)) {
		t.Fatalf("empty round trip = %+v", emptyBack)
	}
}

func TestParseSuccessRejects(t *testing.T) {
	const (
		ws = `"window_start":"2026-10-10T10:00:00.000Z"`
		we = `"window_end":"2026-10-10T10:05:00.000Z"`
		p0 = `{"key":"source=coingecko/ingest_date=2026-10-10/window=1000/part-00000.jsonl","sha256":"` + shaA + `","rows":3}`
		p1 = `{"key":"source=coingecko/ingest_date=2026-10-10/window=1000/part-00001.jsonl","sha256":"` + shaB + `","rows":4}`
	)
	cases := map[string]string{
		"unknown field":           `{` + ws + `,` + we + `,"rows":0,"parts":[],"extra":1}`,
		"unknown part field":      `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"k","sha256":"` + shaA + `","rows":3,"x":1}]}`,
		"trailing data":           `{` + ws + `,` + we + `,"rows":0,"parts":[]}{}`,
		"trailing garbage":        `{` + ws + `,` + we + `,"rows":0,"parts":[]} x`,
		"rows not the sum":        `{` + ws + `,` + we + `,"rows":8,"parts":[` + p0 + `,` + p1 + `]}`,
		"unsorted parts":          `{` + ws + `,` + we + `,"rows":7,"parts":[` + p1 + `,` + p0 + `]}`,
		"duplicate part keys":     `{` + ws + `,` + we + `,"rows":6,"parts":[` + p0 + `,` + p0 + `]}`,
		"uppercase sha256":        `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"k","sha256":"` + strings.ToUpper(shaB) + `","rows":3}]}`,
		"short sha256":            `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"k","sha256":"abc","rows":3}]}`,
		"non-hex sha256":          `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"k","sha256":"` + strings.Repeat("g", 64) + `","rows":3}]}`,
		"negative part rows":      `{` + ws + `,` + we + `,"rows":-1,"parts":[{"key":"k","sha256":"` + shaA + `","rows":-1}]}`,
		"end equals start":        `{` + ws + `,"window_end":"2026-10-10T10:00:00.000Z","rows":0,"parts":[]}`,
		"end before start":        `{` + ws + `,"window_end":"2026-10-10T09:55:00.000Z","rows":0,"parts":[]}`,
		"start without ms":        `{"window_start":"2026-10-10T10:00:00Z",` + we + `,"rows":0,"parts":[]}`,
		"end with an offset":      `{` + ws + `,"window_end":"2026-10-10T13:05:00.000+03:00","rows":0,"parts":[]}`,
		"start not a timestamp":   `{"window_start":"soon",` + we + `,"rows":0,"parts":[]}`,
		"missing parts (null)":    `{` + ws + `,` + we + `,"rows":0,"parts":null}`,
		"not an object":           `[]`,
		"empty input":             ``,
		"empty part key":          `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"","sha256":"` + shaA + `","rows":3}]}`,
		"missing window_end":      `{` + ws + `,"rows":0,"parts":[]}`,
		"rows as a string":        `{` + ws + `,` + we + `,"rows":"0","parts":[]}`,
		"rows as a float":         `{` + ws + `,` + we + `,"rows":0.5,"parts":[]}`,
		"part rows as a fraction": `{` + ws + `,` + we + `,"rows":3,"parts":[{"key":"k","sha256":"` + shaA + `","rows":2.5}]}`,
		// Valid JSON with the right values, but not the canonical bytes Marshal writes.
		"duplicated key":         `{` + ws + `,` + we + `,"rows":5,"rows":0,"parts":[]}`,
		"keys out of order":      `{` + we + `,` + ws + `,"rows":0,"parts":[]}`,
		"space after a colon":    `{"window_start": "2026-10-10T10:00:00.000Z",` + we + `,"rows":0,"parts":[]}`,
		"space between members":  `{` + ws + `, ` + we + `,"rows":0,"parts":[]}`,
		"indented":               "{\n  " + ws + ",\n  " + we + ",\n  \"rows\":0,\n  \"parts\":[]\n}",
		"trailing newline":       `{` + ws + `,` + we + `,"rows":0,"parts":[]}` + "\n",
		"leading space":          ` {` + ws + `,` + we + `,"rows":0,"parts":[]}`,
		"escaped key character":  `{"window_` + "\\u0073" + `tart":"2026-10-10T10:00:00.000Z",` + we + `,"rows":0,"parts":[]}`,
		"part keys out of order": `{` + ws + `,` + we + `,"rows":3,"parts":[{"sha256":"` + shaA + `","key":"k","rows":3}]}`,
	}
	for name, in := range cases {
		if _, err := ParseSuccess([]byte(in)); err == nil {
			t.Errorf("%s: ParseSuccess(%s) succeeded, want an error", name, in)
		}
	}
}

func TestLayoutIgnoresLocalZone(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60) // a half-hour offset: hour alignment must still be UTC
	old := time.Local
	time.Local = ist
	t.Cleanup(func() { time.Local = old })

	start := time.Date(2026, 10, 11, 5, 25, 0, 0, ist) // 2026-10-10T23:55:00Z, as pgx would return it
	w := Window{Start: start, End: start.Add(5 * time.Minute)}
	if got, want := RawPartKey(w, 3), "source=coingecko/ingest_date=2026-10-10/window=2355/part-00003.jsonl"; got != want {
		t.Fatalf("RawPartKey = %q, want %q", got, want)
	}
	b, err := NewSuccess(w, nil).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"window_start":"2026-10-10T23:55:00.000Z","window_end":"2026-10-11T00:00:00.000Z","rows":0,"parts":[]}`; string(b) != want {
		t.Fatalf("manifest %s, want %s", b, want)
	}
	if got := Floor(time.Date(2026, 10, 11, 5, 29, 0, 0, ist), time.Hour); !got.Equal(time.Date(2026, 10, 10, 23, 0, 0, 0, time.UTC)) || got.Location() != time.UTC {
		t.Fatalf("Floor = %v, want 2026-10-10T23:00:00Z in UTC", got)
	}
}
