package maintenance

import (
	"testing"
	"time"
)

func TestRetentionCutoffIsUTCMidnight(t *testing.T) {
	// 2026-10-12T11:00Z is already 2026-10-13 in UTC+14: the cutoff must still
	// count from the UTC date.
	now := time.Date(2026, 10, 12, 11, 0, 0, 0, time.UTC).In(time.FixedZone("LINT", 14*3600))
	got := retentionCutoff(now, 8)
	if want := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC); !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("retentionCutoff = %v, want %v in UTC", got, want)
	}
}

func TestDefaultRetentionOptions(t *testing.T) {
	got := DefaultRetentionOptions(8)
	want := RetentionOptions{Days: 8, LockTimeout: 3 * time.Second, Attempts: 5, RetryDelay: 2 * time.Second}
	if got != want {
		t.Fatalf("DefaultRetentionOptions(8) = %+v, want %+v", got, want)
	}
}
