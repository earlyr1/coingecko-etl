package publisher

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/exporter"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

const itTopic = "coingecko.raw"

type env struct {
	pool    *pgxpool.Pool
	s3c     *s3.Client
	prod    *kgo.Client
	reg     *validation.Registry
	m       *obs.Metrics
	log     *slog.Logger
	brokers []string
}

func setup(t *testing.T, ctx context.Context) env {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	dsn := testutil.StartPostgres(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, _ := testutil.StartVersityGW(t)

	pool, err := pg.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := pg.EnsurePartitions(ctx, pool, time.Now(), 1); err != nil {
		t.Fatal(err)
	}
	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketProcessed, s3.BucketRejected, s3.BucketDLQ); err != nil {
		t.Fatal(err)
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	prod, err := queue.NewProducer(brokers, "publisher-it", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	if err := queue.EnsureTopic(ctx, prod, itTopic, 6, 168*time.Hour); err != nil {
		t.Fatal(err)
	}
	return env{
		pool: pool, s3c: s3c, prod: prod, reg: reg,
		m:       obs.NewMetrics(prometheus.NewRegistry()),
		log:     slog.New(slog.NewTextHandler(t.Output(), nil)),
		brokers: brokers,
	}
}

// okEvent builds a successful RawEvent around a recorded CoinGecko response.
func okEvent(t *testing.T, endpoint string, req event.Request, fixture string) event.RawEvent {
	t.Helper()
	payload, err := os.ReadFile("../../schemas/testdata/coingecko/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	return event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        uuid.Must(uuid.NewV7()).String(),
		IdempotencyKey: &idem,
		Source:         event.SourceCoinGecko,
		Endpoint:       endpoint,
		Request:        req,
		FetchedAt:      xtime.Format(time.Now()),
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 120},
		PayloadSHA256:  &sha,
		Payload:        payload,
	}
}

// consumeAll reads the topic from the beginning until want records arrived
// (or the deadline), then waits briefly to catch unexpected extras.
func consumeAll(t *testing.T, ctx context.Context, brokers []string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(itTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	deadline := time.Now().Add(30 * time.Second)
	extra := time.Time{}
	for time.Now().Before(deadline) {
		if len(out) >= want {
			if extra.IsZero() {
				extra = time.Now().Add(2 * time.Second)
			} else if time.Now().After(extra) {
				break
			}
		}
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fs := cl.PollFetches(pctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func headers(r *kgo.Record) map[string]string {
	h := map[string]string{}
	for _, rh := range r.Headers {
		h[rh.Key] = string(rh.Value)
	}
	return h
}

func TestPublisherIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := setup(t, ctx)

	evs := []event.RawEvent{
		okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json"),
		okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t1.json"),
		okEvent(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json"),
	}
	for _, ev := range evs {
		if err := pg.InsertAPICall(ctx, e.pool, ev); err != nil {
			t.Fatal(err)
		}
	}
	n, err := exporter.ExportOnce(ctx, e.pool, e.s3c, e.reg, e.m, e.log, 500)
	if err != nil || n != len(evs) {
		t.Fatalf("ExportOnce = %d, %v; want %d", n, err, len(evs))
	}
	var batchID int64
	var objectKey string
	if err := e.pool.QueryRow(ctx, `SELECT batch_id, object_key FROM pipeline.export_batches ORDER BY batch_id DESC LIMIT 1`).Scan(&batchID, &objectKey); err != nil {
		t.Fatal(err)
	}
	body, err := e.s3c.Get(ctx, s3.BucketRaw, objectKey)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(body)
	if len(lines) != len(evs) {
		t.Fatalf("raw object has %d lines, want %d", len(lines), len(evs))
	}

	t.Run("publishes every line verbatim with headers", func(t *testing.T) {
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines))
		if len(recs) != len(lines) {
			t.Fatalf("consumed %d records, want %d", len(recs), len(lines))
		}
		for _, r := range recs {
			h := headers(r)
			lineNo, err := strconv.Atoi(h[queue.HeaderLineNo])
			if err != nil || lineNo < 1 || lineNo > len(lines) {
				t.Fatalf("bad line_no header %q", h[queue.HeaderLineNo])
			}
			if string(r.Value) != string(lines[lineNo-1]) {
				t.Fatalf("record value differs from raw line %d", lineNo)
			}
			ev, err := event.Unmarshal(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if string(r.Key) != queue.MessageKey(ev) {
				t.Fatalf("key = %q, want %q", r.Key, queue.MessageKey(ev))
			}
			if h[queue.HeaderEventID] != ev.EventID || h[queue.HeaderIdempotencyKey] != *ev.IdempotencyKey ||
				h[queue.HeaderSchema] != validation.IDRawEvent || h[queue.HeaderRawObject] != objectKey {
				t.Fatalf("headers = %v", h)
			}
		}
		var publishedAt *time.Time
		if err := e.pool.QueryRow(ctx, `SELECT published_at FROM pipeline.export_batches WHERE batch_id = $1`, batchID).Scan(&publishedAt); err != nil {
			t.Fatal(err)
		}
		if publishedAt == nil {
			t.Fatal("published_at not set")
		}
	})

	t.Run("second run publishes nothing", func(t *testing.T) {
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || published {
			t.Fatalf("PublishOnce = %v, %v; want false, nil", published, err)
		}
	})

	t.Run("expired lease republishes", func(t *testing.T) {
		ev := okEvent(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t2.json")
		if err := pg.InsertAPICall(ctx, e.pool, ev); err != nil {
			t.Fatal(err)
		}
		if n, err := exporter.ExportOnce(ctx, e.pool, e.s3c, e.reg, e.m, e.log, 500); err != nil || n != 1 {
			t.Fatalf("ExportOnce = %d, %v", n, err)
		}
		lease := time.Second
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		// Simulate a crash after the produce and before the mark: the batch
		// is unpublished and its lease is still held.
		if _, err := e.pool.Exec(ctx, `
UPDATE pipeline.export_batches SET published_at = NULL, publish_lease_until = now() + interval '1 second'
 WHERE batch_id = (SELECT max(batch_id) FROM pipeline.export_batches)`); err != nil {
			t.Fatal(err)
		}
		published, err = PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || published {
			t.Fatalf("PublishOnce while leased = %v, %v; want false, nil", published, err)
		}
		time.Sleep(1500 * time.Millisecond)
		published, err = PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, lease, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce after lease expiry = %v, %v", published, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines)+2)
		copies := 0
		for _, r := range recs {
			if headers(r)[queue.HeaderEventID] == ev.EventID {
				copies++
			}
		}
		if copies != 2 {
			t.Fatalf("event %s published %d times, want 2 (republish duplicates)", ev.EventID, copies)
		}
	})
	t.Run("invalid line goes to dlq, valid line is published", func(t *testing.T) {
		good := okEvent(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json")
		goodLine, err := event.Marshal(good)
		if err != nil {
			t.Fatal(err)
		}
		bad := []byte(`{"schema_version":1,"event_id":"` + uuid.Must(uuid.NewV7()).String() + `"}`)
		obj := append(append(append(goodLine, '\n'), bad...), '\n')
		key := "source=coingecko/ingest_date=2026-10-10/batch=manual-dlq.jsonl"
		if _, err := e.s3c.PutIfAbsent(ctx, s3.BucketRaw, key, obj); err != nil {
			t.Fatal(err)
		}
		var id int64
		var exportedAt time.Time
		err = e.pool.QueryRow(ctx, `
INSERT INTO pipeline.export_batches (row_count, min_fetched_at, max_fetched_at, object_key, status, exported_at)
VALUES (2, now(), now(), $1, 'exported', now()) RETURNING batch_id, exported_at`, key).Scan(&id, &exportedAt)
		if err != nil {
			t.Fatal(err)
		}
		published, err := PublishOnce(ctx, e.pool, e.s3c, e.prod, e.reg, itTopic, time.Minute, e.m, e.log)
		if err != nil || !published {
			t.Fatalf("PublishOnce = %v, %v", published, err)
		}
		dlqBody, err := e.s3c.Get(ctx, s3.BucketDLQ, dlqObjectKey(exportedAt, id))
		if err != nil {
			t.Fatalf("dlq object: %v", err)
		}
		dlqLines := splitLines(dlqBody)
		if len(dlqLines) != 1 {
			t.Fatalf("dlq has %d lines, want 1", len(dlqLines))
		}
		if viols, err := e.reg.Validate(validation.IDDLQRecord, dlqLines[0]); err != nil || len(viols) > 0 {
			t.Fatalf("dlq line invalid: %v %v", viols, err)
		}
		recs := consumeAll(t, ctx, e.brokers, len(lines)+2+1)
		found := 0
		for _, r := range recs {
			if headers(r)[queue.HeaderEventID] == good.EventID {
				found++
				if string(r.Value) != string(goodLine) {
					t.Fatal("valid line not published verbatim")
				}
			}
		}
		if found != 1 || len(recs) != len(lines)+2+1 {
			t.Fatalf("found=%d total=%d", found, len(recs))
		}
	})

}
