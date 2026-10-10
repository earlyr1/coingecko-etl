package transformer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Manifest statuses of pipeline.consumer_batches.
const (
	statusPlanned = "planned"
	statusWritten = "written"
)

// ErrNonDeterministic means a retry of a planned batch produced objects whose
// keys or checksums differ from the ones stored by an earlier attempt. The
// transform or serialization is not deterministic (or the code changed between
// attempts). Alert and stop: never overwrite.
var ErrNonDeterministic = errors.New("transformer: batch objects differ from the stored manifest (non-deterministic transform)")

// manifestRow is one row of pipeline.consumer_batches.
type manifestRow struct {
	First, Last int64
	Status      string
	PlannedAt   time.Time
	ObjectKeys  []string // nil until setObjects
	SHAs        [][]byte
}

const manifestCols = `first_offset, last_offset, status, planned_at, object_keys, content_sha256`

func scanManifest(row pgx.Row) (manifestRow, error) {
	var m manifestRow
	err := row.Scan(&m.First, &m.Last, &m.Status, &m.PlannedAt, &m.ObjectKeys, &m.SHAs)
	return m, err
}

// insertPlanned tries to plan [first..last] for (topicID, partition).
//
// It returns inserted=true and the new row when the range was free. Otherwise
// it returns the stored batch that blocks it: the batch with the same first
// offset (primary key conflict, handled by ON CONFLICT DO NOTHING), or the
// earliest batch overlapping the range (EXCLUDE constraint, SQLSTATE 23P01).
// The INSERT runs inside a savepoint (pgx: tx.Begin on a Tx) because an
// exclusion violation aborts the statement and would otherwise poison TX1.
//
// Both conflicts wait for a concurrent uncommitted insert of the same range to
// finish, so two consumers planning the same batch are serialized here.
func insertPlanned(ctx context.Context, tx pgx.Tx, topicID uuid.UUID, partition int32, first, last int64) (manifestRow, bool, error) {
	sp, err := tx.Begin(ctx) // SAVEPOINT
	if err != nil {
		return manifestRow{}, false, err
	}
	m, err := scanManifest(sp.QueryRow(ctx, `
INSERT INTO pipeline.consumer_batches (topic_id, kafka_partition, first_offset, last_offset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (topic_id, kafka_partition, first_offset) DO NOTHING
RETURNING `+manifestCols, topicID, partition, first, last))
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		if err := sp.Commit(ctx); err != nil { // RELEASE SAVEPOINT
			return manifestRow{}, false, err
		}
		return m, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		if err := sp.Commit(ctx); err != nil {
			return manifestRow{}, false, err
		}
	case errors.As(err, &pgErr) && pgErr.Code == "23P01": // exclusion_violation
		if err := sp.Rollback(ctx); err != nil { // ROLLBACK TO SAVEPOINT
			return manifestRow{}, false, err
		}
	default:
		_ = sp.Rollback(ctx)
		return manifestRow{}, false, fmt.Errorf("plan batch: %w", err)
	}
	m, err = scanManifest(tx.QueryRow(ctx, `
SELECT `+manifestCols+`
FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2
  AND int8range(first_offset, last_offset, '[]') && int8range($3, $4, '[]')
ORDER BY first_offset
LIMIT 1`, topicID, partition, first, last))
	if err != nil {
		return manifestRow{}, false, fmt.Errorf("load conflicting batch: %w", err)
	}
	return m, false, nil
}

// CoveringBatch returns the manifest batch whose [first..last] contains offset.
func CoveringBatch(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, offset int64) (first, last int64, status string, found bool, err error) {
	err = pool.QueryRow(ctx, `
SELECT first_offset, last_offset, status
FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2
  AND int8range(first_offset, last_offset, '[]') @> $3::bigint`,
		topicID, partition, offset).Scan(&first, &last, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, "", false, nil
	}
	if err != nil {
		return 0, 0, "", false, err
	}
	return first, last, status, true, nil
}

// setObjects stores the object keys, checksums and counts of a planned batch.
// If an earlier attempt already stored them, it compares instead and returns
// ErrNonDeterministic on any difference.
func setObjects(ctx context.Context, tx pgx.Tx, topicID uuid.UUID, partition int32, first int64, keys []string, shas [][]byte, counts []byte) error {
	tag, err := tx.Exec(ctx, `
UPDATE pipeline.consumer_batches
SET object_keys = $4, content_sha256 = $5, counts = $6
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3 AND object_keys IS NULL`,
		topicID, partition, first, keys, shas, counts)
	if err != nil {
		return fmt.Errorf("store batch objects: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var storedKeys []string
	var storedSHAs [][]byte
	if err := tx.QueryRow(ctx, `
SELECT object_keys, content_sha256 FROM pipeline.consumer_batches
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3`,
		topicID, partition, first).Scan(&storedKeys, &storedSHAs); err != nil {
		return fmt.Errorf("load stored batch objects: %w", err)
	}
	if !slices.Equal(storedKeys, keys) || !slices.EqualFunc(storedSHAs, shas, bytes.Equal) {
		return fmt.Errorf("%w: partition %d first offset %d: stored %v, now %v", ErrNonDeterministic, partition, first, storedKeys, keys)
	}
	return nil
}

// markWritten flips a planned batch to written (TX2). It reports whether this
// call did the flip, so metrics are counted once per batch even when a zombie
// consumer finishes the same batch.
func markWritten(ctx context.Context, pool *pgxpool.Pool, topicID uuid.UUID, partition int32, first int64) (bool, error) {
	tag, err := pool.Exec(ctx, `
UPDATE pipeline.consumer_batches
SET status = 'written', written_at = now()
WHERE topic_id = $1 AND kafka_partition = $2 AND first_offset = $3 AND status = 'planned'`,
		topicID, partition, first)
	if err != nil {
		return false, fmt.Errorf("mark batch written: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
