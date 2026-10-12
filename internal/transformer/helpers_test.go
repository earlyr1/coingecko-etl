package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/transform"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const fixtureDir = "../../schemas/testdata/coingecko"

// fixtureEvent wraps a recorded CoinGecko response in a raw event (S1), the
// way the fetcher would. fetchedAt must be RFC 3339 UTC with milliseconds.
func fixtureEvent(t *testing.T, file, endpoint, coinID, fetchedAt string) event.RawEvent {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, file))
	if err != nil {
		t.Fatal(err)
	}
	// encoding/json drops whitespace around a RawMessage when decoding, so the
	// payload must not carry any; otherwise payload_sha256 would not verify.
	payload := bytes.TrimSpace(b)
	req := coingecko.MarketsRequest(100)
	if endpoint == event.EndpointCoinDetail {
		req = coingecko.CoinDetailRequest(coinID)
	}
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	date, cache := "Sat, 10 Oct 2026 10:02:33 GMT", "MISS"
	age := 0
	etag := `W/"fixture"`
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        uuid.Must(uuid.NewV7()).String(),
		IdempotencyKey: &idem,
		Source:         event.SourceCoinGecko,
		Endpoint:       endpoint,
		Request:        req,
		FetchedAt:      fetchedAt,
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 120, Date: &date, AgeSeconds: &age, CFCacheStatus: &cache, ETag: &etag},
		PayloadSHA256:  &sha,
		Payload:        payload,
	}
}

func message(t *testing.T, partition int32, offset int64, ev event.RawEvent) Message {
	t.Helper()
	v, err := event.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return Message{Partition: partition, Offset: offset, Value: v, Headers: map[string]string{
		queue.HeaderEventID:   ev.EventID,
		queue.HeaderRawObject: "source=coingecko/ingest_date=2026-10-10/window=1000/part-00001.jsonl",
	}}
}

// fixtureEvents returns the standard sequence used by the tests:
//
//	0 markets t0, 1 markets t1 (byte-identical CDN copy of t0, new event_id),
//	2 bitcoin detail t0, 3 bitcoin detail t1 (byte-identical), 4 markets t2 (new snapshot)
func fixtureEvents(t *testing.T) []event.RawEvent {
	return []event.RawEvent{
		fixtureEvent(t, "markets_top10_t0.json", event.EndpointMarkets, "", "2026-10-10T10:02:32.000Z"),
		fixtureEvent(t, "markets_top10_t1.json", event.EndpointMarkets, "", "2026-10-10T10:03:07.000Z"),
		fixtureEvent(t, "coin_bitcoin_full_t0.json", event.EndpointCoinDetail, "bitcoin", "2026-10-10T10:02:38.000Z"),
		fixtureEvent(t, "coin_bitcoin_full_t1.json", event.EndpointCoinDetail, "bitcoin", "2026-10-10T10:03:12.000Z"),
		fixtureEvent(t, "markets_top10_t2.json", event.EndpointMarkets, "", "2026-10-10T10:03:42.000Z"),
	}
}

// expected computes what processed/ and rejected/ must contain for a sequence
// of valid events, applying the §7.3 rules independently of the dedup package:
// first sight of a key wins; same content later is dropped; different content
// later is a snapshot_conflict reject.
func expected(t *testing.T, evs []event.RawEvent, rules transform.Rules) (processed, rejected int) {
	t.Helper()
	first := map[string][32]byte{}
	for _, ev := range evs {
		recs, err := transform.Transform(ev, rules)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			k := r.Dataset + "\x00" + r.Key + "\x00" + r.EventDate
			sum, seen := first[k]
			switch {
			case !seen:
				first[k] = r.ContentSHA256
				if r.Zone == transform.ZoneProcessed {
					processed++
				} else {
					rejected++
				}
			case sum != r.ContentSHA256:
				rejected++
			}
		}
	}
	return processed, rejected
}

type env struct {
	deps    Deps
	dataDir string
	reg     *prometheus.Registry
}

// newEnv starts Postgres and versitygw and returns ready Deps.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	dsn := testutil.StartPostgres(t)
	endpoint, dataDir := testutil.StartVersityGW(t)
	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Fixture snapshots are dated 2026-10-10: create dedup_claims partitions around it.
	if err := pg.EnsureClaimPartitions(ctx, pool, time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC), 2); err != nil {
		t.Fatal(err)
	}
	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatal(err)
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	preg := prometheus.NewRegistry()
	return &env{
		deps: Deps{
			Pool: pool, S3: s3c, Reg: reg, Metrics: obs.NewMetrics(preg),
			Log:   slog.New(slog.DiscardHandler),
			Rules: transform.Rules{StaleAfter: 10 * time.Minute, TickerStaleAfter: time.Hour, TickerAnomalyPct: 2},
		},
		dataDir: dataDir,
		reg:     preg,
	}
}

// reset empties the pipeline state so subtests do not see each other's claims.
func (e *env) reset(t *testing.T) {
	t.Helper()
	if _, err := e.deps.Pool.Exec(context.Background(),
		`TRUNCATE pipeline.dedup_claims, pipeline.consumer_batches`); err != nil {
		t.Fatal(err)
	}
}

// objects reads every object a manifest row lists, keyed by "bucket/key".
func (e *env) objects(t *testing.T, topicID uuid.UUID, partition int32, first int64) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var status string
	if err := e.deps.Pool.QueryRow(ctx, `SELECT object_keys, status FROM pipeline.consumer_batches
		WHERE topic_id=$1 AND kafka_partition=$2 AND first_offset=$3`, topicID, partition, first).Scan(&keys, &status); err != nil {
		t.Fatal(err)
	}
	if status != statusWritten {
		t.Fatalf("batch %d status = %s, want written", first, status)
	}
	out := map[string][]byte{}
	for _, k := range keys {
		bucket, key, _ := strings.Cut(k, "/")
		b, err := e.deps.S3.Get(ctx, bucket, key)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		out[k] = b
	}
	return out
}

// countLines counts JSONL lines per bucket and validates each processed line
// against its dataset schema, each rejected line against S3 rejected_record and
// each dlq line against S3 dlq_record. It also fails on duplicate processed rows
// (same business content).
func (e *env) countLines(t *testing.T, objs map[string][]byte) map[string]int {
	t.Helper()
	counts := map[string]int{}
	seen := map[string]bool{}
	for k, body := range objs {
		bucket, key, _ := strings.Cut(k, "/")
		if len(body) == 0 || body[len(body)-1] != '\n' {
			t.Fatalf("%s: body must end with a newline", k)
		}
		for _, l := range bytes.Split(bytes.TrimSuffix(body, []byte("\n")), []byte("\n")) {
			counts[bucket]++
			var schemaID string
			switch bucket {
			case s3.BucketProcessed:
				ds := strings.TrimPrefix(strings.SplitN(key, "/", 2)[0], "dataset=")
				schemaID = transform.SchemaIDFor(ds)
				biz := businessOnly(t, l)
				if seen[ds+biz] {
					t.Errorf("%s: duplicate processed row %s", k, biz)
				}
				seen[ds+biz] = true
			case s3.BucketRejected:
				schemaID = validation.IDRejectedRecord
			case s3.BucketDLQ:
				schemaID = validation.IDDLQRecord
			}
			vs, err := e.deps.Reg.Validate(schemaID, l)
			if err != nil || len(vs) > 0 {
				t.Errorf("%s: line fails %s: %v %v", k, schemaID, err, vs)
			}
		}
	}
	return counts
}

// businessOnly drops the technical "_" columns of a processed row.
func businessOnly(t *testing.T, row []byte) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(row, &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
