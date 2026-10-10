package transformer

import (
	"bytes"
	"testing"

	"coingecko-etl/internal/storage/s3"
)

func TestObjectKey(t *testing.T) {
	cases := []struct {
		zone, prefix string
		want         string
	}{
		{s3.BucketProcessed, "dataset=ticker/v=1/event_date=2026-10-10",
			"dataset=ticker/v=1/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
		{s3.BucketRejected, "dataset=coin_contract/event_date=2026-10-10/",
			"dataset=coin_contract/event_date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
		{s3.BucketDLQ, "stage=transform/date=2026-10-10",
			"stage=transform/date=2026-10-10/coingecko.raw+3+00000000000000001200.jsonl"},
	}
	for _, c := range cases {
		if got := ObjectKey(c.zone, c.prefix, "coingecko.raw", 3, 1200); got != c.want {
			t.Errorf("ObjectKey(%q, %q) = %q, want %q", c.zone, c.prefix, got, c.want)
		}
	}
}

func TestBuildObjectsIsDeterministic(t *testing.T) {
	mk := func() map[groupKey][]line {
		return map[groupKey][]line{
			{s3.BucketProcessed, "dataset=b/v=1/event_date=2026-10-10"}: {{offset: 7, ordinal: 1, data: []byte(`{"x":3}`)}, {offset: 5, ordinal: 2, data: []byte(`{"x":2}`)}, {offset: 5, ordinal: 0, data: []byte(`{"x":1}`)}},
			{s3.BucketDLQ, "stage=transform/date=2026-10-10"}:           {{offset: 6, ordinal: -1, data: []byte(`{"d":1}`)}},
			{s3.BucketProcessed, "dataset=a/v=1/event_date=2026-10-10"}: {{offset: 5, ordinal: 0, data: []byte(`{"a":1}`)}},
		}
	}
	a := buildObjects(mk(), "t", 0, 5)
	b := buildObjects(mk(), "t", 0, 5)
	if len(a) != 3 {
		t.Fatalf("got %d objects", len(a))
	}
	for i := range a {
		if a[i].Key != b[i].Key || !bytes.Equal(a[i].Body, b[i].Body) || a[i].SHA256 != b[i].SHA256 {
			t.Fatalf("object %d differs between runs", i)
		}
	}
	if a[0].Bucket != s3.BucketDLQ || a[1].Key != "dataset=a/v=1/event_date=2026-10-10/t+0+00000000000000000005.jsonl" {
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
