// Package dedup implements the single deduplication mechanism of the pipeline
// (spec §7.3): every record bound for processed/ or rejected/ claims its
// natural key in pipeline.dedup_claims; the first claimant wins, and every
// later claimant is compared against the stored winner.
package dedup

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Position identifies one record inside the log: topic incarnation,
// partition, offset of the message and index of the record in the message.
type Position struct {
	TopicID   uuid.UUID
	Partition int32
	Offset    int64
	Ordinal   int
}

// Claim is one record asking for its natural key.
type Claim struct {
	Dataset, Key, EventDate string // EventDate is "YYYY-MM-DD"
	Pos                     Position
	EventID                 string // uuid of the raw event (message)
	IdempotencyKey          string // drop-reason labelling only; "" is stored as NULL
	ContentSHA256           [32]byte
}

// Decision is what the caller does with a claimed record.
type Decision int

const (
	Emit     Decision = iota // the key belongs to this record's own position: write it
	Drop                     // another position won with identical content: drop
	Conflict                 // another position won with different content: reject as snapshot_conflict
)

func (d Decision) String() string {
	switch d {
	case Emit:
		return "emit"
	case Drop:
		return "drop"
	case Conflict:
		return "conflict"
	}
	return fmt.Sprintf("Decision(%d)", int(d))
}

// Drop reasons (metric label values only; they do not change behaviour).
const (
	ReasonReplay            = "replay"             // same event_id: the same message published twice
	ReasonDuplicateResponse = "duplicate_response" // same idempotency_key: byte-identical API response from another poll
	ReasonDuplicateRecord   = "duplicate_record"   // same business content from a different response
)

// Outcome is the decision for one claim.
type Outcome struct {
	Decision   Decision
	DropReason string // set only when Decision == Drop
}

type claimKey struct{ dataset, key, date string }

// stored is the winner row of one key as read back from pipeline.dedup_claims.
type stored struct {
	pos            Position
	eventID        string
	idempotencyKey string // "" when NULL
	sha            []byte
}

// less orders positions: topic id, partition, offset, ordinal.
func less(a, b Position) bool {
	if c := bytes.Compare(a.TopicID[:], b.TopicID[:]); c != 0 {
		return c < 0
	}
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	if a.Offset != b.Offset {
		return a.Offset < b.Offset
	}
	return a.Ordinal < b.Ordinal
}

const insertSQL = `
INSERT INTO pipeline.dedup_claims
  (dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal, event_id, idempotency_key, content_sha256)
SELECT u.dataset, u.record_key, u.event_date::date, u.topic_id::uuid, u.kafka_partition, u.kafka_offset, u.ordinal,
       u.event_id::uuid, NULLIF(u.idempotency_key, ''), u.content_sha256
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::bigint[], $7::int[], $8::text[], $9::text[], $10::bytea[])
     WITH ORDINALITY AS u(dataset, record_key, event_date, topic_id, kafka_partition, kafka_offset, ordinal, event_id, idempotency_key, content_sha256, ord)
ORDER BY u.ord
ON CONFLICT (dataset, record_key, event_date) DO NOTHING`

// selectSQL returns the stored winner of every requested key together with
// the key's 1-based index in the request arrays.
const selectSQL = `
SELECT k.ord, c.topic_id::text, c.kafka_partition, c.kafka_offset, c.ordinal,
       c.event_id::text, COALESCE(c.idempotency_key, ''), c.content_sha256
FROM unnest($1::text[], $2::text[], $3::text[]) WITH ORDINALITY AS k(dataset, record_key, event_date, ord)
JOIN pipeline.dedup_claims c
  ON c.dataset = k.dataset AND c.record_key = k.record_key AND c.event_date = k.event_date::date`

// ClaimAndDecide inserts claims inside the caller's transaction and returns one
// Outcome per claim, in the same order as claims.
//
// The transaction MUST use READ COMMITTED (pgx default). The INSERT and the
// SELECT are two statements on purpose: when another transaction holds an
// uncommitted claim for the same key, our INSERT waits for it; after it commits
// our INSERT does nothing, and the following SELECT (new snapshot under READ
// COMMITTED) sees the winner. A single CTE statement could not see it.
//
// Partitions of pipeline.dedup_claims for every EventDate must already exist
// (pg.EnsureClaimPartitions); otherwise Postgres returns SQLSTATE 23514, which is
// wrapped in the returned error.
func ClaimAndDecide(ctx context.Context, tx pgx.Tx, claims []Claim) ([]Outcome, error) {
	if len(claims) == 0 {
		return nil, nil
	}

	// 1. Pre-dedup in Go: one claim per key, the lowest position.
	keys, winners, err := pickWinners(claims)
	if err != nil {
		return nil, err
	}

	n := len(keys)
	var (
		datasets = make([]string, n)
		recKeys  = make([]string, n)
		dates    = make([]string, n)
		topicIDs = make([]string, n)
		parts    = make([]int32, n)
		offsets  = make([]int64, n)
		ordinals = make([]int32, n)
		eventIDs = make([]string, n)
		idemKeys = make([]string, n)
		shas     = make([][]byte, n)
	)
	for i, k := range keys {
		c := claims[winners[k]]
		datasets[i], recKeys[i], dates[i] = k.dataset, k.key, k.date
		topicIDs[i] = c.Pos.TopicID.String()
		parts[i] = c.Pos.Partition
		offsets[i] = c.Pos.Offset
		ordinals[i] = int32(c.Pos.Ordinal)
		eventIDs[i] = c.EventID
		idemKeys[i] = c.IdempotencyKey
		sha := c.ContentSHA256
		shas[i] = sha[:]
	}

	// 2. Bulk insert in sorted key order. Dates and uuids travel as text[] and
	//    are cast in SQL, so pgx needs no custom codecs; WITH ORDINALITY +
	//    ORDER BY keeps the sorted order so concurrent transactions lock keys
	//    in the same order.
	if _, err := tx.Exec(ctx, insertSQL, datasets, recKeys, dates, topicIDs, parts, offsets, ordinals, eventIDs, idemKeys, shas); err != nil {
		return nil, fmt.Errorf("dedup: insert claims: %w", err)
	}

	// 3. Read back the stored winner of every key (ours or someone else's).
	rows, err := tx.Query(ctx, selectSQL, datasets, recKeys, dates)
	if err != nil {
		return nil, fmt.Errorf("dedup: select claims: %w", err)
	}
	defer rows.Close()
	got := make(map[claimKey]stored, n)
	for rows.Next() {
		var (
			idx     int64
			s       stored
			topicID string
			ordinal int32
		)
		if err := rows.Scan(&idx, &topicID, &s.pos.Partition, &s.pos.Offset, &ordinal, &s.eventID, &s.idempotencyKey, &s.sha); err != nil {
			return nil, fmt.Errorf("dedup: scan claim: %w", err)
		}
		if idx < 1 || idx > int64(n) {
			return nil, fmt.Errorf("dedup: claim index %d out of range", idx)
		}
		if s.pos.TopicID, err = uuid.Parse(topicID); err != nil {
			return nil, fmt.Errorf("dedup: stored topic_id: %w", err)
		}
		s.pos.Ordinal = int(ordinal)
		got[keys[idx-1]] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dedup: read claims: %w", err)
	}

	// 4. Decide per claim, in input order.
	out := make([]Outcome, len(claims))
	for i, c := range claims {
		s, ok := got[claimKey{c.Dataset, c.Key, c.EventDate}]
		if !ok {
			return nil, fmt.Errorf("dedup: claim for %s/%s/%s vanished", c.Dataset, c.Key, c.EventDate)
		}
		out[i] = decide(s, c)
	}
	return out, nil
}

// pickWinners keeps, per key, only the claim with the lowest position, so the
// INSERT never carries the same key twice and the lowest position wins
// deterministically instead of by row order. The keys are returned sorted so
// that two transactions claiming overlapping key sets lock index entries in
// the same order (no deadlocks).
func pickWinners(claims []Claim) ([]claimKey, map[claimKey]int, error) {
	winners := make(map[claimKey]int, len(claims))
	for i, c := range claims {
		if c.Dataset == "" || c.EventDate == "" {
			return nil, nil, fmt.Errorf("dedup: claim %d has empty dataset or event_date", i)
		}
		if _, err := uuid.Parse(c.EventID); err != nil {
			return nil, nil, fmt.Errorf("dedup: claim %d: event_id %q: %w", i, c.EventID, err)
		}
		k := claimKey{c.Dataset, c.Key, c.EventDate}
		if j, ok := winners[k]; !ok || less(c.Pos, claims[j].Pos) {
			winners[k] = i
		}
	}
	keys := make([]claimKey, 0, len(winners))
	for k := range winners {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool {
		x, y := keys[a], keys[b]
		if x.dataset != y.dataset {
			return x.dataset < y.dataset
		}
		if x.key != y.key {
			return x.key < y.key
		}
		return x.date < y.date
	})
	return keys, winners, nil
}

// decide compares a claim with the stored winner of its key (spec §7.3).
func decide(s stored, c Claim) Outcome {
	sha := c.ContentSHA256
	switch {
	case s.pos == c.Pos:
		return Outcome{Decision: Emit}
	case bytes.Equal(s.sha, sha[:]):
		return Outcome{Decision: Drop, DropReason: dropReason(s, c)}
	default:
		return Outcome{Decision: Conflict}
	}
}

func dropReason(s stored, c Claim) string {
	switch {
	case sameUUID(s.eventID, c.EventID):
		return ReasonReplay
	case c.IdempotencyKey != "" && s.idempotencyKey == c.IdempotencyKey:
		return ReasonDuplicateResponse
	default:
		return ReasonDuplicateRecord
	}
}

// sameUUID compares two uuid strings by value, so the text form Postgres
// returns matches any spelling uuid.Parse accepts.
func sameUUID(a, b string) bool {
	ua, errA := uuid.Parse(a)
	ub, errB := uuid.Parse(b)
	if errA != nil || errB != nil {
		return strings.EqualFold(a, b)
	}
	return ua == ub
}
