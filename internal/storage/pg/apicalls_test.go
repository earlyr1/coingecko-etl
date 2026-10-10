package pg

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"coingecko-etl/internal/event"
)

func strp(s string) *string { return &s }

func okEvent() event.RawEvent {
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        "0199cd2e-7c1a-7000-8000-000000000001",
		IdempotencyKey: strp(strings.Repeat("ab", 32)),
		Source:         event.SourceCoinGecko,
		Endpoint:       event.EndpointMarkets,
		Request:        event.Request{Path: "/coins/markets", Params: map[string]string{"vs_currency": "usd"}},
		FetchedAt:      "2026-10-10T10:07:55.123Z",
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 42},
		PayloadSHA256:  strp(strings.Repeat("0f", 32)),
		Payload:        json.RawMessage(`[{"id":"bitcoin", "current_price":82800}]`),
	}
}

func TestAPICallArgsOK(t *testing.T) {
	args, err := apiCallArgs(okEvent())
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 12 {
		t.Fatalf("got %d args, want 12", len(args))
	}
	if got := args[0].(uuid.UUID).String(); got != "0199cd2e-7c1a-7000-8000-000000000001" {
		t.Errorf("event_id = %s", got)
	}
	if got := args[2].(int16); got != 1 {
		t.Errorf("schema_version = %d", got)
	}
	if got := args[5].(string); got != `{"path":"/coins/markets","params":{"vs_currency":"usd"}}` {
		t.Errorf("request = %s", got)
	}
	want := time.Date(2026, 10, 10, 10, 7, 55, 123e6, time.UTC)
	if got := args[6].(time.Time); !got.Equal(want) || got.Location() != time.UTC {
		t.Errorf("fetched_at = %v", got)
	}
	if got := args[8].(string); !strings.Contains(got, `"status":200`) {
		t.Errorf("http = %s", got)
	}
	// payload must be a string (pgx sends text -> json column stores bytes verbatim)
	if got, ok := args[10].(string); !ok || got != `[{"id":"bitcoin", "current_price":82800}]` {
		t.Errorf("payload = %#v", args[10])
	}
	if got := args[11].([]byte); len(got) != 32 || got[0] != 0x0f {
		t.Errorf("payload_sha256 = %x", got)
	}
}

func TestAPICallArgsFailedCallHasNulls(t *testing.T) {
	ev := okEvent()
	ev.Outcome = event.OutcomeNetworkError
	ev.IdempotencyKey = nil
	ev.HTTP = nil
	ev.Error = strp("dial tcp: timeout")
	ev.PayloadSHA256 = nil
	ev.Payload = json.RawMessage("null")
	args, err := apiCallArgs(ev)
	if err != nil {
		t.Fatal(err)
	}
	if args[1].(*string) != nil {
		t.Error("idempotency_key should be nil")
	}
	if args[8] != nil {
		t.Errorf("http = %#v, want nil", args[8])
	}
	if args[10] != nil {
		t.Errorf("payload = %#v, want nil", args[10])
	}
	if args[11].([]byte) != nil {
		t.Errorf("payload_sha256 = %#v, want nil", args[11])
	}
	if *args[9].(*string) != "dial tcp: timeout" {
		t.Errorf("error = %v", args[9])
	}
}

func TestAPICallArgsRejectsBadInput(t *testing.T) {
	cases := map[string]func(*event.RawEvent){
		"bad uuid":       func(e *event.RawEvent) { e.EventID = "not-a-uuid" },
		"bad fetched_at": func(e *event.RawEvent) { e.FetchedAt = "10/10/2026" },
		"bad sha hex":    func(e *event.RawEvent) { e.PayloadSHA256 = strp("zz") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ev := okEvent()
			mutate(&ev)
			if _, err := apiCallArgs(ev); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestPartitionDDL(t *testing.T) {
	got := partitionDDL(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	if len(got) != 2 {
		t.Fatalf("got %d statements", len(got))
	}
	wantRaw := "CREATE TABLE IF NOT EXISTS raw.api_calls_p20261231 PARTITION OF raw.api_calls\n" +
		"  FOR VALUES FROM ('2026-12-31 00:00:00+00') TO ('2027-01-01 00:00:00+00')"
	if got[0] != wantRaw {
		t.Errorf("raw DDL:\n%s\nwant:\n%s", got[0], wantRaw)
	}
	wantDedup := "CREATE TABLE IF NOT EXISTS pipeline.dedup_claims_p20261231 PARTITION OF pipeline.dedup_claims\n" +
		"  FOR VALUES FROM ('2026-12-31') TO ('2027-01-01')"
	if got[1] != wantDedup {
		t.Errorf("dedup DDL:\n%s\nwant:\n%s", got[1], wantDedup)
	}
}

func TestIsMissingPartition(t *testing.T) {
	missing := &pgconn.PgError{Code: "23514", Message: `no partition of relation "api_calls" found for row`}
	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":                    {nil, false},
		"plain error":            {errors.New("boom"), false},
		"missing partition":      {missing, true},
		"wrapped missing":        {fmt.Errorf("insert: %w", missing), true},
		"check violation":        {&pgconn.PgError{Code: "23514", ConstraintName: "api_calls_payload_iff_ok"}, false},
		"other sqlstate no name": {&pgconn.PgError{Code: "23505"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := IsMissingPartition(tc.err); got != tc.want {
				t.Errorf("IsMissingPartition(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
