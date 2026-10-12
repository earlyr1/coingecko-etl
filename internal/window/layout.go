package window

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/xtime"
)

// RawWindowPrefix is the prefix (in bucket raw) of every object of w.
func RawWindowPrefix(w Window) string {
	return fmt.Sprintf("source=%s/ingest_date=%s/window=%s/", event.SourceCoinGecko, w.Date(), w.HHMM())
}

// RawPartKey is the key (in bucket raw) of part number part of w, from 0.
func RawPartKey(w Window, part int) string {
	return RawWindowPrefix(w) + fmt.Sprintf("part-%05d.jsonl", part)
}

// RawSuccessKey is the key (in bucket raw) of the manifest of w, written last.
func RawSuccessKey(w Window) string { return RawWindowPrefix(w) + "_SUCCESS" }

// ExportDLQKey (bucket dlq): part is the index of the raw part whose lines failed.
func ExportDLQKey(w Window, part int) string {
	return fmt.Sprintf("stage=export/ingest_date=%s/window=%s/part-%05d.jsonl", w.Date(), w.HHMM(), part)
}

// PublishDLQKey (bucket dlq): one object per window.
func PublishDLQKey(w Window) string {
	return fmt.Sprintf("stage=publish/ingest_date=%s/window=%s/part-00000.jsonl", w.Date(), w.HHMM())
}

// SuccessPart is one raw part listed in _SUCCESS. Key is the object key inside
// bucket raw; Rows counts the JSONL lines of the part (lines the exporter sent
// to its DLQ are not in the part and not counted).
type SuccessPart struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Rows   int    `json:"rows"`
}

// Success is the _SUCCESS manifest. encoding/json writes struct fields in
// declaration order, which is the contract's key order.
type Success struct {
	WindowStart string        `json:"window_start"`
	WindowEnd   string        `json:"window_end"`
	Rows        int           `json:"rows"`
	Parts       []SuccessPart `json:"parts"`
}

// NewSuccess builds the manifest of w; parts must already be in part-index order.
func NewSuccess(w Window, parts []SuccessPart) Success {
	s := Success{WindowStart: xtime.Format(w.Start), WindowEnd: xtime.Format(w.End), Parts: []SuccessPart{}}
	for _, p := range parts {
		s.Rows += p.Rows
		s.Parts = append(s.Parts, p)
	}
	return s
}

// Marshal returns compact JSON without a trailing newline; Parts is never null.
func (s Success) Marshal() ([]byte, error) {
	if s.Parts == nil {
		s.Parts = []SuccessPart{}
	}
	return json.Marshal(s)
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ParseSuccess decodes and checks a manifest: no unknown fields and no trailing
// data; window_start/window_end are exact xtime.Format values with end > start;
// parts is an array (never null); every part has a non-empty key, a lowercase
// 64-hex sha256 and rows >= 0; keys strictly increase; rows equals the sum of
// the parts' rows. Last, b must be exactly the bytes Marshal writes for the
// parsed manifest (compact, keys in contract order, no duplicate key, no
// trailing newline), so one window has one valid _SUCCESS spelling.
func ParseSuccess(b []byte) (Success, error) {
	var s Success
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Success{}, fmt.Errorf("window: _SUCCESS: %w", err)
	}
	if err := dec.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return Success{}, errors.New("window: _SUCCESS: trailing data after the JSON object")
	}
	start, err := exactTime("window_start", s.WindowStart)
	if err != nil {
		return Success{}, err
	}
	end, err := exactTime("window_end", s.WindowEnd)
	if err != nil {
		return Success{}, err
	}
	if !end.After(start) {
		return Success{}, fmt.Errorf("window: _SUCCESS: window_end %s is not after window_start %s", s.WindowEnd, s.WindowStart)
	}
	if s.Parts == nil {
		return Success{}, errors.New("window: _SUCCESS: parts is missing or null")
	}
	sum := 0
	for i, p := range s.Parts {
		switch {
		case p.Key == "":
			return Success{}, fmt.Errorf("window: _SUCCESS: part %d has an empty key", i)
		case !sha256Hex.MatchString(p.SHA256):
			return Success{}, fmt.Errorf("window: _SUCCESS: part %s: sha256 %q is not 64 lowercase hex characters", p.Key, p.SHA256)
		case p.Rows < 0:
			return Success{}, fmt.Errorf("window: _SUCCESS: part %s: negative rows %d", p.Key, p.Rows)
		case i > 0 && p.Key <= s.Parts[i-1].Key:
			return Success{}, fmt.Errorf("window: _SUCCESS: part keys not strictly increasing at %s", p.Key)
		}
		sum += p.Rows
	}
	if s.Rows != sum {
		return Success{}, fmt.Errorf("window: _SUCCESS: rows %d, but the parts hold %d", s.Rows, sum)
	}
	canonical, err := s.Marshal()
	if err != nil {
		return Success{}, fmt.Errorf("window: _SUCCESS: %w", err)
	}
	if !bytes.Equal(canonical, b) {
		return Success{}, errors.New("window: _SUCCESS: not in the canonical form (compact JSON, keys in contract order, no duplicate key or trailing newline)")
	}
	return s, nil
}

// exactTime parses a manifest timestamp and requires the canonical
// xtime.Format spelling, so equal windows always have equal bytes.
func exactTime(field, v string) (time.Time, error) {
	t, err := xtime.Parse(v)
	if err != nil {
		return time.Time{}, fmt.Errorf("window: _SUCCESS: %s: %w", field, err)
	}
	if xtime.Format(t) != v {
		return time.Time{}, fmt.Errorf("window: _SUCCESS: %s %q is not in the form %s", field, v, xtime.Format(t))
	}
	return t, nil
}
