// Package window holds the raw-zone window contract shared by the exporter
// (writer) and the publisher (reader): ETL_WINDOW validation and arithmetic
// (windows aligned to the UTC hour), the keys of raw parts, _SUCCESS and the
// window-named DLQ objects, and the _SUCCESS manifest.
package window

import (
	"errors"
	"time"

	"coingecko-etl/internal/xtime"
)

// PublishGrace is the fixed delay between a window's end and the publish run
// that reads it: Cronicle runs etl publish one minute after every window end.
const PublishGrace = time.Minute

// Window is the half-open interval [Start, End) in UTC.
type Window struct{ Start, End time.Time }

var errSize = errors.New("must be whole minutes from 1m to 60m that divide 60m (1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m)")

// Validate reports whether size can be ETL_WINDOW. The order of the checks
// matters: size < 1m first, so time.Hour%size never divides by zero.
func Validate(size time.Duration) error {
	if size < time.Minute || size > time.Hour || size%time.Minute != 0 || time.Hour%size != 0 {
		return errSize
	}
	return nil
}

// Floor returns the start of the size window containing t, in UTC. size must
// pass Validate: windows are counted from the start of t's UTC hour.
func Floor(t time.Time, size time.Duration) time.Time {
	u := t.UTC()
	h := u.Truncate(time.Hour)
	return h.Add(u.Sub(h) / size * size)
}

// Ceil returns the first window boundary at or after t, in UTC.
func Ceil(t time.Time, size time.Duration) time.Time {
	f := Floor(t, size)
	if f.Equal(t) {
		return f
	}
	return f.Add(size)
}

// ForPublish is the window a publish run at now handles: it ends at
// Floor(now - PublishGrace) and lasts size. It never reads the clock, so a
// catch-up run with an old JOB_NOW takes the old window.
func ForPublish(now time.Time, size time.Duration) Window {
	end := Floor(now.Add(-PublishGrace), size)
	return Window{Start: end.Add(-size), End: end}
}

// Date is the UTC date of the window start (YYYY-MM-DD).
func (w Window) Date() string { return xtime.FormatDate(w.Start) }

// HHMM is the UTC time of the window start (e.g. "2355").
func (w Window) HHMM() string { return w.Start.UTC().Format("1504") }
