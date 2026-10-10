package dedup

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
)

func TestPickWinnersKeepsLowestPositionPerKey(t *testing.T) {
	tid := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	ev := uuid.NewString()
	mk := func(key string, off int64, ord int) Claim {
		return Claim{Dataset: "ticker", Key: key, EventDate: "2026-10-10", EventID: ev,
			Pos: Position{TopicID: tid, Partition: 2, Offset: off, Ordinal: ord}}
	}
	claims := []Claim{mk("b", 7, 0), mk("a", 9, 4), mk("a", 9, 2), mk("b", 5, 9), mk("a", 10, 0)}
	keys, winners, err := pickWinners(claims)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].key != "a" || keys[1].key != "b" {
		t.Fatalf("keys not sorted/unique: %+v", keys)
	}
	if got := winners[keys[0]]; got != 2 {
		t.Errorf("winner for a = claim %d, want 2 (offset 9 ordinal 2)", got)
	}
	if got := winners[keys[1]]; got != 3 {
		t.Errorf("winner for b = claim %d, want 3 (offset 5)", got)
	}
}

func TestPickWinnersRejectsBadClaims(t *testing.T) {
	if _, _, err := pickWinners([]Claim{{Dataset: "x", Key: "k", EventDate: "2026-10-10", EventID: "nope"}}); err == nil {
		t.Error("expected error for a non-uuid event_id")
	}
	if _, _, err := pickWinners([]Claim{{Key: "k", EventDate: "2026-10-10", EventID: uuid.NewString()}}); err == nil {
		t.Error("expected error for an empty dataset")
	}
}

func TestDecide(t *testing.T) {
	pos := Position{TopicID: uuid.New(), Partition: 1, Offset: 10, Ordinal: 0}
	other := pos
	other.Offset = 11
	sumA, sumB := sha256.Sum256([]byte("a")), sha256.Sum256([]byte("b"))
	ev1, ev2 := uuid.NewString(), uuid.NewString()
	winner := stored{pos: pos, eventID: ev1, idempotencyKey: "k1", sha: sumA[:]}
	noKeyWinner := stored{pos: pos, eventID: ev1, idempotencyKey: "", sha: sumA[:]}

	cases := []struct {
		name string
		s    stored
		c    Claim
		want Outcome
	}{
		{"own position", winner, Claim{Pos: pos, EventID: ev1, ContentSHA256: sumA}, Outcome{Decision: Emit}},
		{"own position, content ignored", winner, Claim{Pos: pos, EventID: ev1, ContentSHA256: sumB}, Outcome{Decision: Emit}},
		{"replay", winner, Claim{Pos: other, EventID: ev1, IdempotencyKey: "k1", ContentSHA256: sumA}, Outcome{Drop, ReasonReplay}},
		{"duplicate response", winner, Claim{Pos: other, EventID: ev2, IdempotencyKey: "k1", ContentSHA256: sumA}, Outcome{Drop, ReasonDuplicateResponse}},
		{"duplicate record", winner, Claim{Pos: other, EventID: ev2, IdempotencyKey: "k2", ContentSHA256: sumA}, Outcome{Drop, ReasonDuplicateRecord}},
		{"empty idempotency keys never match", noKeyWinner, Claim{Pos: other, EventID: ev2, IdempotencyKey: "", ContentSHA256: sumA}, Outcome{Drop, ReasonDuplicateRecord}},
		{"conflict", winner, Claim{Pos: other, EventID: ev2, IdempotencyKey: "k1", ContentSHA256: sumB}, Outcome{Decision: Conflict}},
	}
	for _, tc := range cases {
		if got := decide(tc.s, tc.c); got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
