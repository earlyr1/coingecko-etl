package window

import (
	"strings"
	"testing"
	"time"
)

// No test of this package may call t.Parallel() or start a goroutine:
// TestLayoutIgnoresLocalZone swaps time.Local, which time.Now() reads.

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestValidate(t *testing.T) {
	allowed := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 10: true, 12: true, 15: true, 20: true, 30: true, 60: true}
	for m := 1; m <= 60; m++ {
		err := Validate(time.Duration(m) * time.Minute)
		if allowed[m] && err != nil {
			t.Errorf("Validate(%dm) = %v, want nil", m, err)
		}
		if !allowed[m] && err == nil {
			t.Errorf("Validate(%dm) = nil, want an error", m)
		}
	}
	for _, d := range []time.Duration{
		0, -5 * time.Minute, 30 * time.Second, 90 * time.Second, time.Minute + 30*time.Second,
		5*time.Minute + 30*time.Second, 7 * time.Minute, 45 * time.Minute, 61 * time.Minute, 2 * time.Hour,
	} {
		err := Validate(d)
		if err == nil {
			t.Errorf("Validate(%v) = nil, want an error", d)
			continue
		}
		if want := "1m 2m 3m 4m 5m 6m 10m 12m 15m 20m 30m 60m"; !strings.Contains(err.Error(), want) {
			t.Errorf("Validate(%v) error %q does not list %q", d, err, want)
		}
	}
}

func TestFloorCeil(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	msk := time.FixedZone("MSK", 3*3600)
	cases := []struct {
		name string
		got  time.Time
		want time.Time
	}{
		{"floor inside a window", Floor(utc("2026-10-10T10:07:31Z"), 5*time.Minute), utc("2026-10-10T10:05:00Z")},
		{"floor on a boundary", Floor(utc("2026-10-10T10:05:00Z"), 5*time.Minute), utc("2026-10-10T10:05:00Z")},
		{"floor of an hour window", Floor(utc("2026-10-10T10:59:59Z"), 60*time.Minute), utc("2026-10-10T10:00:00Z")},
		{"floor of an offset time", Floor(time.Date(2026, 10, 11, 1, 30, 0, 0, msk), 5*time.Minute), utc("2026-10-10T22:30:00Z")},
		{"floor to the UTC hour, not the local one", Floor(time.Date(2026, 10, 11, 5, 29, 0, 0, ist), 60*time.Minute), utc("2026-10-10T23:00:00Z")},
		{"ceil inside a window", Ceil(utc("2026-10-10T10:05:00Z"), 10*time.Minute), utc("2026-10-10T10:10:00Z")},
		{"ceil on a boundary", Ceil(utc("2026-10-10T10:10:00Z"), 10*time.Minute), utc("2026-10-10T10:10:00Z")},
	}
	for _, c := range cases {
		if !c.got.Equal(c.want) {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
		if c.got.Location() != time.UTC {
			t.Errorf("%s: location %v, want UTC", c.name, c.got.Location())
		}
	}
}

func TestForPublish(t *testing.T) {
	cases := []struct {
		size       time.Duration
		now        string
		start, end string
	}{
		{5 * time.Minute, "2026-10-10T14:06:00Z", "2026-10-10T14:00:00Z", "2026-10-10T14:05:00Z"},
		{5 * time.Minute, "2026-10-10T14:01:00Z", "2026-10-10T13:55:00Z", "2026-10-10T14:00:00Z"},
		{5 * time.Minute, "2026-10-10T14:05:59.999Z", "2026-10-10T13:55:00Z", "2026-10-10T14:00:00Z"},
		{5 * time.Minute, "2026-10-10T14:00:59Z", "2026-10-10T13:50:00Z", "2026-10-10T13:55:00Z"}, // grace not yet elapsed
		{5 * time.Minute, "2026-10-10T10:05:59Z", "2026-10-10T09:55:00Z", "2026-10-10T10:00:00Z"},
		{time.Minute, "2026-10-10T10:01:00Z", "2026-10-10T09:59:00Z", "2026-10-10T10:00:00Z"},
		{15 * time.Minute, "2026-10-10T10:16:00Z", "2026-10-10T10:00:00Z", "2026-10-10T10:15:00Z"},
		{60 * time.Minute, "2026-10-10T10:01:00Z", "2026-10-10T09:00:00Z", "2026-10-10T10:00:00Z"},
		{5 * time.Minute, "2026-10-10T00:00:30Z", "2026-10-09T23:50:00Z", "2026-10-09T23:55:00Z"},
		{5 * time.Minute, "2026-10-13T00:01:00Z", "2026-10-12T23:55:00Z", "2026-10-13T00:00:00Z"},
	}
	for _, c := range cases {
		w := ForPublish(utc(c.now), c.size)
		if !w.Start.Equal(utc(c.start)) || !w.End.Equal(utc(c.end)) {
			t.Errorf("ForPublish(%s, %v) = [%v, %v), want [%s, %s)", c.now, c.size, w.Start, w.End, c.start, c.end)
		}
		if w.Start.Location() != time.UTC || w.End.Location() != time.UTC {
			t.Errorf("ForPublish(%s, %v) is not in UTC: %v, %v", c.now, c.size, w.Start.Location(), w.End.Location())
		}
		if w.End.Sub(w.Start) != c.size {
			t.Errorf("ForPublish(%s, %v) lasts %v", c.now, c.size, w.End.Sub(w.Start))
		}
	}
	w := ForPublish(utc("2026-10-13T00:01:00Z"), 5*time.Minute)
	if key := RawSuccessKey(w); !strings.Contains(key, "ingest_date=2026-10-12/window=2355") {
		t.Errorf("RawSuccessKey = %q, want the 2026-10-12 23:55 window", key)
	}
}

// Cronicle's Run All replays missed runs with their scheduled JOB_NOW at any
// wall time: consecutive JOB_NOW values must cover the timeline exactly once.
func TestForPublishCatchUp(t *testing.T) {
	const size = 5 * time.Minute
	var got []Window
	for now := utc("2026-10-10T13:01:00Z"); !now.After(utc("2026-10-10T14:01:00Z")); now = now.Add(size) {
		w := ForPublish(now, size)
		if again := ForPublish(now, size); again != w {
			t.Fatalf("ForPublish(%v) is not deterministic: %v then %v", now, w, again)
		}
		got = append(got, w)
	}
	if !got[0].Start.Equal(utc("2026-10-10T12:55:00Z")) || !got[len(got)-1].End.Equal(utc("2026-10-10T14:00:00Z")) {
		t.Fatalf("windows cover [%v, %v), want [12:55, 14:00)", got[0].Start, got[len(got)-1].End)
	}
	for i := 1; i < len(got); i++ {
		if !got[i].Start.Equal(got[i-1].End) {
			t.Fatalf("window %d starts at %v, previous ends at %v", i, got[i].Start, got[i-1].End)
		}
	}
}

func TestForPublishOffsetInput(t *testing.T) {
	local, err := time.Parse(time.RFC3339, "2026-10-12T17:06:00+03:00")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ForPublish(local, 5*time.Minute), ForPublish(utc("2026-10-12T14:06:00Z"), 5*time.Minute); got != want {
		t.Fatalf("ForPublish(+03:00) = %v, want %v", got, want)
	}
}
