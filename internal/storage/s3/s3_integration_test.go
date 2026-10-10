package s3_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
)

func newClient(t *testing.T) (*s3.Client, string) {
	t.Helper()
	endpoint, dataDir := testutil.StartVersityGW(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets: %v", err)
	}
	// Second call must be a no-op, not an error.
	if err := c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketDLQ); err != nil {
		t.Fatalf("EnsureBuckets again: %v", err)
	}
	return c, dataDir
}

func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	key := "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl"
	body := []byte("{\"a\":1}\n{\"b\":2}\n")

	created, err := c.PutIfAbsent(ctx, s3.BucketRaw, key, body)
	if err != nil || !created {
		t.Fatalf("first put: created=%v err=%v", created, err)
	}

	created, err = c.PutIfAbsent(ctx, s3.BucketRaw, key, append([]byte(nil), body...))
	if err != nil || created {
		t.Fatalf("same-content put: want (false,nil), got created=%v err=%v", created, err)
	}

	created, err = c.PutIfAbsent(ctx, s3.BucketRaw, key, []byte("{\"other\":true}\n"))
	if !errors.Is(err, s3.ErrChecksumMismatch) || created {
		t.Fatalf("different-content put: want ErrChecksumMismatch, got created=%v err=%v", created, err)
	}

	// The object is a plain file in the bucket directory, byte-identical,
	// and the different-content put did not overwrite it.
	onDisk, err := os.ReadFile(filepath.Join(dataDir, s3.BucketRaw, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("read plain file: %v", err)
	}
	if !bytes.Equal(onDisk, body) {
		t.Fatalf("plain file = %q, want %q", onDisk, body)
	}

	got, err := c.Get(ctx, s3.BucketRaw, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}

	if _, err := c.Get(ctx, s3.BucketRaw, "source=coingecko/missing.jsonl"); err == nil {
		t.Fatal("Get of a missing key returned no error")
	}
}

// A bucket directory created on the host (mkdir data/raw before the first
// start, or after .s3meta was wiped) must not make EnsureBuckets fail:
// versitygw answers CreateBucket on it with 500, HeadBucket with 200.
func TestEnsureBucketsAcceptsHostCreatedDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := os.MkdirAll(filepath.Join(dataDir, "handmade"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureBuckets(ctx, "handmade"); err != nil {
		t.Fatalf("EnsureBuckets on a host-created directory: %v", err)
	}
	if created, err := c.PutIfAbsent(ctx, "handmade", "k.jsonl", []byte("{}\n")); err != nil || !created {
		t.Fatalf("put into host-created bucket: created=%v err=%v", created, err)
	}
}

// `etl init` against an empty ./data takes the CreateBucket branch: HeadBucket
// fails on a bucket with no directory, so EnsureBuckets must create it through
// the gateway. A second call must be a no-op, and the new bucket must accept
// writes and reads.
func TestEnsureBucketsCreatesMissingBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const bucket = "fresh"
	if _, err := os.Stat(filepath.Join(dataDir, bucket)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("precondition: bucket directory %q must not exist, stat err=%v", bucket, err)
	}
	if err := c.EnsureBuckets(ctx, bucket); err != nil {
		t.Fatalf("EnsureBuckets on a missing bucket: %v", err)
	}
	if err := c.EnsureBuckets(ctx, bucket); err != nil {
		t.Fatalf("EnsureBuckets again on the created bucket: %v", err)
	}

	key := "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000002.jsonl"
	body := []byte("{\"fresh\":true}\n")
	if created, err := c.PutIfAbsent(ctx, bucket, key, body); err != nil || !created {
		t.Fatalf("put into created bucket: created=%v err=%v", created, err)
	}
	got, err := c.Get(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Get from created bucket: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}
}

// HasAnyObject backs the lake check of `etl init`: an empty bucket (also one
// with only empty sub-directories) holds no object, a nested key does.
func TestHasAnyObject(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker (skipped with -short)")
	}
	testutil.SkipIfNoDocker(t)
	c, dataDir := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if got, err := c.HasAnyObject(ctx, s3.BucketRaw); err != nil || got {
		t.Fatalf("empty bucket: HasAnyObject = %v, %v; want false", got, err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, s3.BucketRaw, "source=coingecko", "ingest_date=2026-10-10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got, err := c.HasAnyObject(ctx, s3.BucketRaw); err != nil || got {
		t.Fatalf("bucket with empty directories: HasAnyObject = %v, %v; want false", got, err)
	}
	key := "source=coingecko/ingest_date=2026-10-10/batch=00000000000000000001.jsonl"
	if _, err := c.PutIfAbsent(ctx, s3.BucketRaw, key, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	if got, err := c.HasAnyObject(ctx, s3.BucketRaw); err != nil || !got {
		t.Fatalf("bucket with one object: HasAnyObject = %v, %v; want true", got, err)
	}
	if got, err := c.HasAnyObject(ctx, s3.BucketDLQ); err != nil || got {
		t.Fatalf("other empty bucket: HasAnyObject = %v, %v; want false", got, err)
	}
}
