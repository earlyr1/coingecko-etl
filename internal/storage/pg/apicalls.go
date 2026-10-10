package pg

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"coingecko-etl/internal/event"
)

const insertAPICallSQL = `
INSERT INTO raw.api_calls (
  event_id, idempotency_key, schema_version, source, endpoint, request,
  fetched_at, outcome, http, error, payload, payload_sha256
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (event_id, fetched_at) DO NOTHING`

// apiCallArgs maps a RawEvent to the INSERT parameters, in column order.
//
// payload is passed as a Go string so pgx sends it as text into the json
// column: Postgres stores json input verbatim, so the bytes (and therefore
// payload_sha256) survive unchanged. Absent payload (nil or literal null)
// becomes SQL NULL; the table CHECK then enforces outcome='ok' <=> payload.
func apiCallArgs(ev event.RawEvent) ([]any, error) {
	id, err := uuid.Parse(ev.EventID)
	if err != nil {
		return nil, fmt.Errorf("pg: event_id: %w", err)
	}
	fetchedAt, err := time.Parse(time.RFC3339Nano, ev.FetchedAt)
	if err != nil {
		return nil, fmt.Errorf("pg: fetched_at: %w", err)
	}
	reqJSON, err := json.Marshal(ev.Request)
	if err != nil {
		return nil, fmt.Errorf("pg: request: %w", err)
	}
	var httpJSON any // nil -> SQL NULL
	if ev.HTTP != nil {
		b, err := json.Marshal(ev.HTTP)
		if err != nil {
			return nil, fmt.Errorf("pg: http: %w", err)
		}
		httpJSON = string(b)
	}
	var payload any // nil -> SQL NULL
	if p := bytes.TrimSpace(ev.Payload); len(p) > 0 && !bytes.Equal(p, []byte("null")) {
		payload = string(ev.Payload)
	}
	var sha []byte // nil -> SQL NULL
	if ev.PayloadSHA256 != nil {
		sha, err = hex.DecodeString(*ev.PayloadSHA256)
		if err != nil {
			return nil, fmt.Errorf("pg: payload_sha256: %w", err)
		}
	}
	return []any{
		id, ev.IdempotencyKey, int16(ev.SchemaVersion), ev.Source, ev.Endpoint, string(reqJSON),
		fetchedAt.UTC(), ev.Outcome, httpJSON, ev.Error, payload, sha,
	}, nil
}

// InsertAPICall stores one API call. Retrying the same event is a no-op
// (ON CONFLICT on the primary key). If the day's partition is missing
// (SQLSTATE 23514 without a constraint name) it creates it and retries once.
func InsertAPICall(ctx context.Context, pool *pgxpool.Pool, ev event.RawEvent) error {
	args, err := apiCallArgs(ev)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, insertAPICallSQL, args...)
	if IsMissingPartition(err) {
		fetchedAt := args[6].(time.Time)
		if perr := EnsurePartitions(ctx, pool, fetchedAt, 1); perr != nil {
			return fmt.Errorf("pg: insert api call: %w (partition repair: %v)", err, perr)
		}
		_, err = pool.Exec(ctx, insertAPICallSQL, args...)
	}
	if err != nil {
		return fmt.Errorf("pg: insert api call %s: %w", ev.EventID, err)
	}
	return nil
}

// IsMissingPartition reports whether err is Postgres's "no partition of
// relation ... found for row". That error and a CHECK violation share
// SQLSTATE 23514; only the CHECK violation names a constraint.
func IsMissingPartition(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514" && pgErr.ConstraintName == ""
}
