package dedup_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/dedup"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/testutil"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var topicID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

const day = "2026-10-10"

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := testutil.StartPostgres(t)
	ctx := context.Background()
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// dedup_claims is partitioned by event_date: create 2026-10-09 .. 2026-10-12.
	if err := pg.EnsureClaimPartitions(ctx, pool, time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	return pool
}

func claim(key string, offset int64, ordinal int, eventID, idem, content string) dedup.Claim {
	return dedup.Claim{
		Dataset: "market_snapshot", Key: key, EventDate: day,
		Pos:     dedup.Position{TopicID: topicID, Partition: 0, Offset: offset, Ordinal: ordinal},
		EventID: eventID, IdempotencyKey: idem, ContentSHA256: sha256.Sum256([]byte(content)),
	}
}

// decide runs ClaimAndDecide in its own committed transaction.
func decide(t *testing.T, pool *pgxpool.Pool, claims ...dedup.Claim) []dedup.Outcome {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	out, err := dedup.ClaimAndDecide(ctx, tx, claims)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return out
}

func want(t *testing.T, got []dedup.Outcome, want ...dedup.Outcome) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d outcomes, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("outcome[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

var (
	emit     = dedup.Outcome{Decision: dedup.Emit}
	conflict = dedup.Outcome{Decision: dedup.Conflict}
)

func drop(reason string) dedup.Outcome {
	return dedup.Outcome{Decision: dedup.Drop, DropReason: reason}
}

func TestClaimAndDecide(t *testing.T) {
	pool := setup(t)
	e1, e2, e3 := uuid.NewString(), uuid.NewString(), uuid.NewString()

	t.Run("first claim emits and redelivery of the same position emits again", func(t *testing.T) {
		c := claim("bitcoin|2026-10-10T10:00:00.000Z", 100, 0, e1, "idem-a", "v1")
		want(t, decide(t, pool, c), emit)
		want(t, decide(t, pool, c), emit) // crash after TX1, same batch again
	})

	t.Run("other position with the same content is dropped with the right reason", func(t *testing.T) {
		k := "ethereum|2026-10-10T10:00:00.000Z"
		want(t, decide(t, pool, claim(k, 200, 3, e1, "idem-a", "v1")), emit)
		// same event_id at another offset: the publisher republished the message
		want(t, decide(t, pool, claim(k, 250, 3, e1, "idem-a", "v1")), drop(dedup.ReasonReplay))
		// other event, same idempotency_key: the CDN served the same response twice
		want(t, decide(t, pool, claim(k, 260, 3, e2, "idem-a", "v1")), drop(dedup.ReasonDuplicateResponse))
		// other event, other response, same business content
		want(t, decide(t, pool, claim(k, 270, 3, e3, "idem-b", "v1")), drop(dedup.ReasonDuplicateRecord))
	})

	t.Run("empty idempotency keys never match each other", func(t *testing.T) {
		// The winner is stored with a NULL idempotency_key; a later claim from
		// another position and event, also without a key and with the same
		// content, is a duplicate_record, not a duplicate_response.
		k := "binancecoin|2026-10-10T10:00:00.000Z"
		want(t, decide(t, pool, claim(k, 290, 0, uuid.NewString(), "", "v1")), emit)
		want(t, decide(t, pool, claim(k, 291, 0, uuid.NewString(), "", "v1")), drop(dedup.ReasonDuplicateRecord))
	})

	t.Run("other position with different content conflicts", func(t *testing.T) {
		k := "tether|2026-10-10T10:00:00.000Z"
		want(t, decide(t, pool, claim(k, 300, 0, e1, "idem-a", "v1")), emit)
		want(t, decide(t, pool, claim(k, 301, 0, e2, "idem-c", "v2")), conflict)
		// the winner itself still emits on redelivery
		want(t, decide(t, pool, claim(k, 300, 0, e1, "idem-a", "v1")), emit)
	})

	t.Run("within one call the lowest offset and ordinal wins regardless of input order", func(t *testing.T) {
		k := "solana|2026-10-10T10:00:00.000Z"
		got := decide(t, pool,
			claim(k, 412, 0, e3, "idem-z", "v1"),
			claim(k, 410, 3, e2, "idem-y", "v1"),
			claim(k, 410, 1, e1, "idem-x", "v2"),
		)
		want(t, got, conflict, conflict, emit)

		k2 := "cardano|2026-10-10T10:00:00.000Z"
		got = decide(t, pool,
			claim(k2, 501, 0, e2, "idem-q", "same"),
			claim(k2, 500, 0, e1, "idem-q", "same"),
		)
		want(t, got, drop(dedup.ReasonDuplicateResponse), emit)
	})

	t.Run("dataset and event_date are part of the key", func(t *testing.T) {
		k := "dogecoin|x"
		a := claim(k, 600, 0, e1, "i", "v1")
		b := claim(k, 601, 0, e2, "i", "v2")
		b.Dataset = "coin_snapshot"
		c := claim(k, 602, 0, e3, "i", "v3")
		c.EventDate = "2026-10-11"
		want(t, decide(t, pool, a, b, c), emit, emit, emit)
	})

	t.Run("concurrent transactions claiming the same key: exactly one emits", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("race-%d|2026-10-10T10:00:00.000Z", i)
			var wg sync.WaitGroup
			results := make([]dedup.Outcome, 2)
			errs := make([]error, 2)
			start := make(chan struct{})
			for g := 0; g < 2; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					ctx := context.Background()
					tx, err := pool.Begin(ctx)
					if err != nil {
						errs[g] = err
						return
					}
					defer tx.Rollback(ctx)
					<-start
					out, err := dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{claim(k, int64(1000+g), 0, uuid.NewString(), "", "same")})
					if err != nil {
						errs[g] = err
						return
					}
					time.Sleep(20 * time.Millisecond) // hold the claim so the other tx has to wait
					errs[g] = tx.Commit(ctx)
					results[g] = out[0]
				}(g)
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			emits := 0
			for _, r := range results {
				switch r {
				case emit:
					emits++
				case drop(dedup.ReasonDuplicateRecord):
				default:
					t.Fatalf("unexpected outcome %+v", r)
				}
			}
			if emits != 1 {
				t.Fatalf("iteration %d: %d emits, want exactly 1 (%+v)", i, emits, results)
			}
		}
	})

	// Two transformers (partitions) claiming overlapping keys in opposite input
	// orders must not deadlock: pickWinners sorts the keys and insertSQL keeps
	// that order (WITH ORDINALITY ... ORDER BY u.ord), so every transaction
	// inserts, and takes its row locks, in the same order.
	t.Run("overlapping key sets in opposite input order do not deadlock", func(t *testing.T) {
		for iter := 0; iter < 10; iter++ {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = fmt.Sprintf("dl-%d-%03d", iter, i) // fresh keys every iteration
			}
			evA, evB := uuid.NewString(), uuid.NewString()   // dedup_claims.event_id is a uuid column
			a := claimsFor(keys, 0, evA, "idem-a")           // ascending, partition 0
			b := claimsFor(reversed(keys), 1, evB, "idem-b") // descending, partition 1, same content sha
			start := make(chan struct{})
			type res struct {
				outs []dedup.Outcome
				err  error
			}
			run := func(cs []dedup.Claim, out chan<- res) {
				ctx := context.Background()
				tx, err := pool.Begin(ctx)
				if err != nil {
					out <- res{err: err}
					return
				}
				defer tx.Rollback(ctx)
				<-start
				outs, err := dedup.ClaimAndDecide(ctx, tx, cs)
				if err == nil {
					time.Sleep(50 * time.Millisecond) // hold the locks
					err = tx.Commit(ctx)
				}
				out <- res{outs, err}
			}
			ra, rb := make(chan res, 1), make(chan res, 1)
			go run(a, ra)
			go run(b, rb)
			close(start)
			A, B := <-ra, <-rb
			if A.err != nil || B.err != nil {
				t.Fatalf("iteration %d: a: %v, b: %v (SQLSTATE 40P01 means the insert order is not fixed)", iter, A.err, B.err)
			}
			if len(A.outs) != len(keys) || len(B.outs) != len(keys) {
				t.Fatalf("iteration %d: %d and %d outcomes, want %d each", iter, len(A.outs), len(B.outs), len(keys))
			}
			// Outcomes follow the input order: a[i] and b[len-1-i] carry keys[i].
			for i := range keys {
				oa, ob := A.outs[i], B.outs[len(keys)-1-i]
				switch {
				case oa == emit && ob == drop(dedup.ReasonDuplicateRecord):
				case ob == emit && oa == drop(dedup.ReasonDuplicateRecord):
				default:
					t.Fatalf("iteration %d, key %s: outcomes %+v and %+v, want one emit and one duplicate_record drop", iter, keys[i], oa, ob)
				}
			}
		}
	})

	t.Run("rolled back claims do not count", func(t *testing.T) {
		ctx := context.Background()
		k := "polkadot|2026-10-10T10:00:00.000Z"
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{claim(k, 700, 0, e1, "", "v1")}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		want(t, decide(t, pool, claim(k, 701, 0, e2, "", "v2")), emit)
	})

	t.Run("missing event_date partition is an error", func(t *testing.T) {
		ctx := context.Background()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		c := claim("x|y", 800, 0, e1, "", "v1")
		c.EventDate = "2031-01-01"
		_, err = dedup.ClaimAndDecide(ctx, tx, []dedup.Claim{c})
		if err == nil {
			t.Fatal("expected an error for a date without a partition")
		}
		// The transformer creates the partition and retries on exactly this error.
		if !pg.IsMissingPartition(err) {
			t.Fatalf("error is not a missing-partition error: %v", err)
		}
	})

	t.Run("no claims is a no-op", func(t *testing.T) {
		if out := decide(t, pool); len(out) != 0 {
			t.Fatalf("got %v", out)
		}
	})
}

// claimsFor builds the claims of one message (one position) for keys, in the
// given order; the content of a key does not depend on the message.
func claimsFor(keys []string, partition int32, eventID, idem string) []dedup.Claim {
	out := make([]dedup.Claim, len(keys))
	for i, k := range keys {
		out[i] = dedup.Claim{
			Dataset: "market_snapshot", Key: k, EventDate: day,
			Pos:     dedup.Position{TopicID: topicID, Partition: partition, Offset: 5000, Ordinal: i},
			EventID: eventID, IdempotencyKey: idem, ContentSHA256: sha256.Sum256([]byte(k)),
		}
	}
	return out
}

func reversed(keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[len(keys)-1-i] = k
	}
	return out
}
