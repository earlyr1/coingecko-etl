package maintenance

import (
	"slices"
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

// transaction_timeout is unknown before PostgreSQL 17: SET would fail there.
func TestNoTimeouts(t *testing.T) {
	for _, c := range []struct {
		version int
		want    []string
	}{
		{160004, []string{"SET statement_timeout = 0"}},
		{170000, []string{"SET transaction_timeout = 0", "SET statement_timeout = 0"}},
		{180006, []string{"SET transaction_timeout = 0", "SET statement_timeout = 0"}},
	} {
		if got := noTimeouts(c.version); !slices.Equal(got, c.want) {
			t.Errorf("noTimeouts(%d) = %q, want %q", c.version, got, c.want)
		}
	}
}

// VacuumTables lists every table the pipeline updates in place.
func TestVacuumTables(t *testing.T) {
	want := []string{"raw.api_calls", "pipeline.export_windows", "pipeline.export_batches", "pipeline.consumer_batches"}
	if !slices.Equal(VacuumTables, want) {
		t.Fatalf("VacuumTables = %q, want %q", VacuumTables, want)
	}
}
