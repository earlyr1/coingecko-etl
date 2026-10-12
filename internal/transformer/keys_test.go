package transformer

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"coingecko-etl/internal/storage/s3"

	"github.com/google/uuid"
)

func TestObjectKey(t *testing.T) {
	// 13:02:33.123456 at +03:00 is 10:02:33 UTC: the key must use the UTC
	// date and hour and truncate the time to the second.
	plannedAt := time.Date(2026, 10, 10, 13, 2, 33, 123456000, time.FixedZone("MSK", 3*3600))
	topic := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	cases := []struct {
		zone, dataset string
		want          string
	}{
		{s3.BucketProcessed, "ticker",
			"dataset=ticker/v=1/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl"},
		{s3.BucketRejected, "coin_contract",
			"dataset=coin_contract/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl"},
		{s3.BucketDLQ, "",
			"stage=transform/processed_date=2026-10-10/hour=10/part-20261010T100233Z-d4376ada.jsonl"},
	}
	for _, c := range cases {
		if got := objectKey(c.zone, c.dataset, plannedAt, topic, 3, 1200); got != c.want {
			t.Errorf("objectKey(%q, %q) = %q, want %q", c.zone, c.dataset, got, c.want)
		}
	}
}

// The vectors were computed with shasum -a 256 over the same strings.
func TestBatchID8(t *testing.T) {
	topic := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	cases := []struct {
		topic     uuid.UUID
		partition int32
		first     int64
		want      string
	}{
		{topic, 3, 1200, "d4376ada"},
		{topic, 4, 1200, "9b7ed59a"},
		{topic, 3, 1201, "72621807"},
	}
	for _, c := range cases {
		if got := batchID8(c.topic, c.partition, c.first); got != c.want {
			t.Errorf("batchID8(%s, %d, %d) = %q, want %q", c.topic, c.partition, c.first, got, c.want)
		}
	}
	other := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	if got := batchID8(other, 3, 1200); got == "d4376ada" {
		t.Errorf("batchID8 of another topic id = %q, the same as topic 1", got)
	}
}

func TestObjectKeyPanics(t *testing.T) {
	at := time.Date(2026, 10, 10, 10, 2, 33, 0, time.UTC)
	topic := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	cases := []struct{ zone, dataset string }{
		{s3.BucketRaw, "ticker"},
		{s3.BucketRaw, ""},
		{s3.BucketProcessed, ""},
		{s3.BucketRejected, ""},
		{s3.BucketDLQ, "ticker"},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%q", c.zone, c.dataset), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("objectKey(%q, %q) did not panic", c.zone, c.dataset)
				}
			}()
			objectKey(c.zone, c.dataset, at, topic, 0, 0)
		})
	}
}

func TestBuildObjectsIsDeterministic(t *testing.T) {
	mk := func() map[groupKey][]line {
		return map[groupKey][]line{
			{s3.BucketProcessed, "b"}: {{offset: 7, ordinal: 1, data: []byte(`{"x":3}`)}, {offset: 5, ordinal: 2, data: []byte(`{"x":2}`)}, {offset: 5, ordinal: 0, data: []byte(`{"x":1}`)}},
			{s3.BucketDLQ, ""}:        {{offset: 6, ordinal: -1, data: []byte(`{"d":1}`)}},
			{s3.BucketProcessed, "a"}: {{offset: 5, ordinal: 0, data: []byte(`{"a":1}`)}},
		}
	}
	at := time.Date(2026, 10, 10, 10, 2, 33, 0, time.UTC)
	topic := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	a := buildObjects(mk(), at, topic, 0, 5)
	b := buildObjects(mk(), at, topic, 0, 5)
	if len(a) != 3 {
		t.Fatalf("got %d objects", len(a))
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Body, b[i].Body) || a[i].SHA256 != b[i].SHA256 {
			t.Fatalf("object %d differs between runs", i)
		}
	}
	if a[0].Bucket != s3.BucketDLQ || a[1].Key != "dataset=a/v=1/processed_date=2026-10-10/hour=10/part-20261010T100233Z-fa6acf61.jsonl" {
		t.Errorf("objects not sorted by bucket then key: %s/%s, %s/%s", a[0].Bucket, a[0].Key, a[1].Bucket, a[1].Key)
	}
	if got := string(a[2].Body); got != "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n" {
		t.Errorf("rows not ordered by offset then ordinal: %q", got)
	}
}

func TestUpTo(t *testing.T) {
	msgs := []Message{{Offset: 10}, {Offset: 11}, {Offset: 13}}
	if n := len(upTo(msgs, 12)); n != 2 {
		t.Errorf("upTo(12) kept %d, want 2", n)
	}
	if n := len(upTo(msgs, 9)); n != 0 {
		t.Errorf("upTo(9) kept %d, want 0", n)
	}
	if n := len(upTo(msgs, 13)); n != 3 {
		t.Errorf("upTo(13) kept %d, want 3", n)
	}
}
