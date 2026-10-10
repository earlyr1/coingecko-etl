package xtime

import (
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	kyiv := time.FixedZone("UTC+3", 3*60*60)
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"utc whole second", time.Date(2026, 10, 10, 10, 7, 55, 0, time.UTC), "2026-10-10T10:07:55.000Z"},
		{"offset converted to utc", time.Date(2026, 10, 10, 13, 7, 55, 0, kyiv), "2026-10-10T10:07:55.000Z"},
		{"truncates below ms", time.Date(2026, 10, 10, 10, 7, 55, 123999999, time.UTC), "2026-10-10T10:07:55.123Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Format(c.in); got != c.want {
				t.Fatalf("Format = %q, want %q", got, c.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	want := time.Date(2026, 10, 10, 10, 2, 30, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Time
	}{
		{"2026-10-10T10:02:30.000Z", want},
		{"2026-10-10T10:02:30Z", want},
		{"2026-10-10T10:02:30+00:00", want},
		{"2026-10-10T13:02:30+03:00", want},
		{"2026-10-10T10:02:30.123Z", want.Add(123 * time.Millisecond)},
		{"2026-10-10T10:02:30.123456+00:00", want.Add(123456 * time.Microsecond)},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := Parse(c.in)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if !got.Equal(c.want) || got.Location() != time.UTC {
				t.Fatalf("Parse = %v (%v), want %v UTC", got, got.Location(), c.want)
			}
		})
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "2026-10-10", "2026-10-10 10:02:30", "yesterday"} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", in)
		}
	}
}

func TestFormatDate(t *testing.T) {
	late := time.Date(2026, 10, 10, 23, 30, 0, 0, time.FixedZone("UTC-2", -2*60*60))
	if got := FormatDate(late); got != "2026-10-11" {
		t.Fatalf("FormatDate = %q, want 2026-10-11", got)
	}
}
