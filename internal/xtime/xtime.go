// Package xtime holds the single time format used everywhere in the pipeline:
// RFC 3339, UTC, millisecond precision, e.g. 2026-10-10T10:07:55.000Z.
package xtime

import (
	"fmt"
	"time"
)

const (
	layout     = "2006-01-02T15:04:05.000Z"
	dateLayout = "2006-01-02"
)

// Format renders t in UTC with exactly three fractional digits (truncated, not rounded).
func Format(t time.Time) string {
	return t.UTC().Format(layout)
}

// Parse accepts every timestamp shape CoinGecko emits: "2026-10-10T10:02:30.123Z",
// "2026-10-10T10:02:30Z" and "2026-10-10T10:02:30+00:00" (with or without a fraction).
// The result is always in UTC.
func Parse(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("xtime: parse %q: %w", s, err)
	}
	return t.UTC(), nil
}

// FormatDate renders the UTC calendar date of t as YYYY-MM-DD.
func FormatDate(t time.Time) string {
	return t.UTC().Format(dateLayout)
}
