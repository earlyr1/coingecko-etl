package exporter

import (
	"os"
	"testing"
	"time"
)

// TestMain runs every test of the package with a +05:30 local zone, so pgx
// returns timestamptz values in a zone whose date and hour differ from UTC
// and every key, bound and manifest byte must still come out in UTC. The zone
// is set before m.Run, while no goroutine exists yet: assigning time.Local
// later would race with the pgx pools' goroutines under -race.
func TestMain(m *testing.M) {
	time.Local = time.FixedZone("IST", 5*3600+30*60)
	os.Exit(m.Run())
}
