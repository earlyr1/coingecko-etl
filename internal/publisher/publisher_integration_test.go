package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/window"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"
)

// env holds what every subtest shares: no Postgres and no exporter, only the
// lake (versitygw) and the broker (Redpanda).
type env struct {
	s3c     *s3.Client
	prod    *kgo.Client
	reg     *validation.Registry
	log     *slog.Logger
	brokers []string
}

func setup(t *testing.T, ctx context.Context) env {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	brokers := testutil.StartRedpanda(t)
	endpoint, _ := testutil.StartVersityGW(t)

	s3c, err := s3.New(ctx, s3.Config{Endpoint: endpoint, AccessKey: testutil.VersityGWAccessKey, SecretKey: testutil.VersityGWSecretKey, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3c.EnsureBuckets(ctx, s3.BucketRaw, s3.BucketDLQ); err != nil {
		t.Fatal(err)
	}
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	prod, err := queue.NewProducer(brokers, "publisher-it")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return env{s3c: s3c, prod: prod, reg: reg, log: slog.New(slog.NewTextHandler(t.Output(), nil)), brokers: brokers}
}

// topic creates a fresh topic for one subtest, so its records count alone.
func (e env) topic(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	if err := queue.EnsureTopic(ctx, e.prod, name, 6, 168*time.Hour); err != nil {
		t.Fatal(err)
	}
	return name
}

func (e env) deps(topic string) Deps {
	return Deps{S3: e.s3c, Producer: e.prod, Reg: e.reg, Topic: topic, Log: e.log}
}

// win returns the 5-minute window of 2026-10-12 starting at hh:mm UTC.
func win(hh, mm int) window.Window {
	start := time.Date(2026, 10, 12, hh, mm, 0, 0, time.UTC)
	return window.Window{Start: start, End: start.Add(5 * time.Minute)}
}

// okLine is the JSONL line of a successful RawEvent around a recorded
// CoinGecko response.
func okLine(t *testing.T, endpoint string, req event.Request, fixture string, fetchedAt time.Time) []byte {
	t.Helper()
	payload, err := os.ReadFile("../../schemas/testdata/coingecko/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.TrimRight(payload, "\n")
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(event.SourceCoinGecko, endpoint, req.Params, sha)
	line, err := event.Marshal(event.RawEvent{
		SchemaVersion:  event.SchemaVersion,
		EventID:        uuid.Must(uuid.NewV7()).String(),
		IdempotencyKey: &idem,
		Source:         event.SourceCoinGecko,
		Endpoint:       endpoint,
		Request:        req,
		FetchedAt:      xtime.Format(fetchedAt),
		Outcome:        event.OutcomeOK,
		HTTP:           &event.HTTPMeta{Status: 200, LatencyMS: 120},
		PayloadSHA256:  &sha,
		Payload:        payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func jsonl(lines ...[]byte) []byte {
	var b bytes.Buffer
	for _, l := range lines {
		b.Write(l)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// putParts writes the raw parts of w (part i = bodies[i]) as the exporter
// would and returns their _SUCCESS entries.
func (e env) putParts(t *testing.T, ctx context.Context, w window.Window, bodies ...[]byte) []window.SuccessPart {
	t.Helper()
	parts := make([]window.SuccessPart, 0, len(bodies))
	for i, body := range bodies {
		key := window.RawPartKey(w, i)
		if _, err := e.s3c.PutIfAbsent(ctx, s3.BucketRaw, key, body); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, window.SuccessPart{Key: key, SHA256: event.SHA256Hex(body), Rows: len(splitLines(body))})
	}
	return parts
}

// putSuccess writes s as the _SUCCESS of w.
func (e env) putSuccess(t *testing.T, ctx context.Context, w window.Window, s window.Success) {
	t.Helper()
	b, err := s.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.s3c.PutIfAbsent(ctx, s3.BucketRaw, window.RawSuccessKey(w), b); err != nil {
		t.Fatal(err)
	}
}

// putWindow writes the parts of w and then its _SUCCESS.
func (e env) putWindow(t *testing.T, ctx context.Context, w window.Window, bodies ...[]byte) {
	t.Helper()
	e.putSuccess(t, ctx, w, window.NewSuccess(w, e.putParts(t, ctx, w, bodies...)))
}

// noDLQ fails if the publish DLQ object of w exists.
func (e env) noDLQ(t *testing.T, ctx context.Context, w window.Window) {
	t.Helper()
	if _, err := e.s3c.Get(ctx, s3.BucketDLQ, window.PublishDLQKey(w)); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("dlq object %s: err %v, want not found", window.PublishDLQKey(w), err)
	}
}

// consumeAll reads topic from the beginning until want records arrived (or
// the deadline), then keeps polling 2 s to catch unexpected extras.
func (e env) consumeAll(t *testing.T, ctx context.Context, topic string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(e.brokers...),
		kgo.ConsumeTopics(topic),
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

// copiesSameKeyAndPartition checks that every event in recs appears exactly n
// times, always with the same key on the same partition.
func copiesSameKeyAndPartition(t *testing.T, recs []*kgo.Record, n int) {
	t.Helper()
	byEvent := map[string][]*kgo.Record{}
	for _, r := range recs {
		id := headers(r)[queue.HeaderEventID]
		byEvent[id] = append(byEvent[id], r)
	}
	for id, rs := range byEvent {
		if len(rs) != n {
			t.Fatalf("event %s produced %d times, want %d", id, len(rs), n)
		}
		for _, r := range rs[1:] {
			if !bytes.Equal(r.Key, rs[0].Key) || r.Partition != rs[0].Partition {
				t.Fatalf("event %s: copies with key %q on partition %d and key %q on partition %d",
					id, rs[0].Key, rs[0].Partition, r.Key, r.Partition)
			}
		}
	}
}

func TestPublisherIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := setup(t, ctx)
	at := func(w window.Window) time.Time { return w.Start.Add(10 * time.Second) }

	t.Run("window with parts publishes every line", func(t *testing.T) {
		topic := e.topic(t, ctx, "pub-parts")
		w := win(14, 0)
		p0 := jsonl(
			okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json", at(w)),
			okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t1.json", at(w)),
		)
		p1 := jsonl(okLine(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json", at(w)))
		e.putWindow(t, ctx, w, p0, p1)
		raw := map[string][][]byte{window.RawPartKey(w, 0): splitLines(p0), window.RawPartKey(w, 1): splitLines(p1)}

		res, err := PublishWindow(ctx, e.deps(topic), w)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Window.Start.Equal(w.Start) || !res.Window.End.Equal(w.End) ||
			res.Parts != 2 || res.Lines != 3 || res.Records != 3 || res.DLQLines != 0 {
			t.Fatalf("Result = %+v", res)
		}
		recs := e.consumeAll(t, ctx, topic, 3)
		if len(recs) != 3 {
			t.Fatalf("consumed %d records, want 3", len(recs))
		}
		for _, r := range recs {
			h := headers(r)
			lines, ok := raw[h[queue.HeaderRawObject]]
			if !ok {
				t.Fatalf("raw_object header %q is not a part of the window", h[queue.HeaderRawObject])
			}
			lineNo, err := strconv.Atoi(h[queue.HeaderLineNo])
			if err != nil || lineNo < 1 || lineNo > len(lines) {
				t.Fatalf("bad line_no header %q", h[queue.HeaderLineNo])
			}
			if !bytes.Equal(r.Value, lines[lineNo-1]) {
				t.Fatalf("record value differs from line %d of %s", lineNo, h[queue.HeaderRawObject])
			}
			ev, err := event.Unmarshal(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if string(r.Key) != *ev.IdempotencyKey {
				t.Fatalf("key = %q, want the idempotency_key %q", r.Key, *ev.IdempotencyKey)
			}
			if h[queue.HeaderEventID] != ev.EventID || h[queue.HeaderIdempotencyKey] != *ev.IdempotencyKey ||
				h[queue.HeaderSchema] != validation.IDRawEvent {
				t.Fatalf("headers = %v", h)
			}
		}
		e.noDLQ(t, ctx, w)
	})

	t.Run("not ready, then ready, then rerun", func(t *testing.T) {
		topic := e.topic(t, ctx, "pub-not-ready")
		w := win(14, 5)
		parts := e.putParts(t, ctx, w,
			jsonl(okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json", at(w)),
				okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t2.json", at(w))))

		_, err := PublishWindow(ctx, e.deps(topic), w)
		if !errors.Is(err, ErrNotReady) || !strings.Contains(err.Error(), window.RawSuccessKey(w)) {
			t.Fatalf("PublishWindow without _SUCCESS = %v; want ErrNotReady naming %s", err, window.RawSuccessKey(w))
		}
		if recs := e.consumeAll(t, ctx, topic, 0); len(recs) != 0 {
			t.Fatalf("a window without _SUCCESS produced %d records", len(recs))
		}
		e.noDLQ(t, ctx, w)

		e.putSuccess(t, ctx, w, window.NewSuccess(w, parts))
		res, err := PublishWindow(ctx, e.deps(topic), w)
		if err != nil || res.Records != 2 {
			t.Fatalf("PublishWindow after _SUCCESS = %+v, %v; want 2 records", res, err)
		}
		if _, err := PublishWindow(ctx, e.deps(topic), w); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		recs := e.consumeAll(t, ctx, topic, 4)
		if len(recs) != 4 {
			t.Fatalf("consumed %d records, want 4 (2 lines x 2 runs)", len(recs))
		}
		copiesSameKeyAndPartition(t, recs, 2)
	})

	t.Run("sha mismatch", func(t *testing.T) {
		topic := e.topic(t, ctx, "pub-sha")
		w := win(14, 15)
		parts := e.putParts(t, ctx, w,
			jsonl(okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json", at(w))),
			jsonl(okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t1.json", at(w))))
		parts[1].SHA256 = strings.Repeat("0", 64)
		e.putSuccess(t, ctx, w, window.NewSuccess(w, parts))

		_, err := PublishWindow(ctx, e.deps(topic), w)
		if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "part-00001") {
			t.Fatalf("PublishWindow = %v; want ErrIntegrity naming part-00001", err)
		}
		if recs := e.consumeAll(t, ctx, topic, 0); len(recs) != 0 {
			t.Fatalf("produced %d records; part-00000 must not be half-published", len(recs))
		}
	})

	t.Run("row count or window mismatch", func(t *testing.T) {
		topic := e.topic(t, ctx, "pub-mismatch")
		line := okLine(t, event.EndpointMarkets, coingecko.MarketsRequest(100), "markets_top10_t0.json", at(win(14, 20)))

		rows := win(14, 20)
		parts := e.putParts(t, ctx, rows, jsonl(line))
		parts[0].Rows = 2
		e.putSuccess(t, ctx, rows, window.NewSuccess(rows, parts))
		if _, err := PublishWindow(ctx, e.deps(topic), rows); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("rows mismatch: %v; want ErrIntegrity", err)
		}

		// The exporter ran with ETL_WINDOW=10m: its _SUCCESS sits at the key
		// of the 5-minute window that starts at the same time.
		w := win(14, 25)
		ten := window.Window{Start: w.Start, End: w.Start.Add(10 * time.Minute)}
		e.putSuccess(t, ctx, w, window.NewSuccess(ten, e.putParts(t, ctx, w, jsonl(line))))
		_, err := PublishWindow(ctx, e.deps(topic), w)
		if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "ETL_WINDOW") {
			t.Fatalf("window mismatch: %v; want ErrIntegrity naming ETL_WINDOW", err)
		}
		if recs := e.consumeAll(t, ctx, topic, 0); len(recs) != 0 {
			t.Fatalf("produced %d records from mismatched windows", len(recs))
		}
	})

	dlqTopic := e.topic(t, ctx, "pub-dlq") // shared by the dlq subtest and its rerun
	dlqWin := win(14, 10)
	var good []byte
	t.Run("invalid line goes to dlq, rest produced", func(t *testing.T) {
		good = okLine(t, event.EndpointCoinDetail, coingecko.CoinDetailRequest("bitcoin"), "coin_bitcoin_full_t0.json", at(dlqWin))
		schemaBad := []byte(`{"schema_version":1,"event_id":"` + uuid.Must(uuid.NewV7()).String() + `"}`)
		httpErr, err := os.ReadFile("../../schemas/testdata/pipeline/raw_event.v1/valid/http_error_429.json")
		if err != nil {
			t.Fatal(err)
		}
		e.putWindow(t, ctx, dlqWin, jsonl(good, schemaBad, []byte("{not json"), bytes.TrimRight(httpErr, "\n")))

		res, err := PublishWindow(ctx, e.deps(dlqTopic), dlqWin)
		if err != nil || res.Records != 1 || res.DLQLines != 3 || res.Lines != 4 || res.Parts != 1 {
			t.Fatalf("PublishWindow = %+v, %v; want 1 record and 3 dlq lines", res, err)
		}
		key := window.PublishDLQKey(dlqWin)
		if key != "stage=publish/ingest_date=2026-10-12/window=1410/part-00000.jsonl" {
			t.Fatalf("dlq key = %s", key)
		}
		body, err := e.s3c.Get(ctx, s3.BucketDLQ, key)
		if err != nil {
			t.Fatal(err)
		}
		lines := splitLines(body)
		if len(lines) != 3 {
			t.Fatalf("dlq object has %d lines, want 3", len(lines))
		}
		wantClass := []string{errClassSchema, errClassDecode, errClassOutcome}
		for i, l := range lines {
			if viols, err := e.reg.Validate(validation.IDDLQRecord, l); err != nil || len(viols) > 0 {
				t.Fatalf("dlq line %d invalid: %v %v", i+1, viols, err)
			}
			var r dlqRecord
			if err := json.Unmarshal(l, &r); err != nil {
				t.Fatal(err)
			}
			if r.Origin.ObjectKey != window.RawPartKey(dlqWin, 0) || r.Origin.LineNo != i+2 ||
				r.ErrorClass != wantClass[i] || r.OccurredAt != "2026-10-12T14:15:00.000Z" {
				t.Fatalf("dlq line %d = %+v", i+1, r)
			}
		}
		recs := e.consumeAll(t, ctx, dlqTopic, 1)
		if len(recs) != 1 || !bytes.Equal(recs[0].Value, good) {
			t.Fatalf("consumed %d records; want the good line once, verbatim", len(recs))
		}
	})

	t.Run("rerun of the same window", func(t *testing.T) {
		if good == nil {
			t.Fatal("the dlq subtest did not run")
		}
		before, err := e.s3c.Get(ctx, s3.BucketDLQ, window.PublishDLQKey(dlqWin))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := PublishWindow(ctx, e.deps(dlqTopic), dlqWin); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		after, err := e.s3c.Get(ctx, s3.BucketDLQ, window.PublishDLQKey(dlqWin))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("dlq object changed on rerun (err %v)", err)
		}
		recs := e.consumeAll(t, ctx, dlqTopic, 2)
		if len(recs) != 2 {
			t.Fatalf("consumed %d records, want 2 copies of the good line", len(recs))
		}
		copiesSameKeyAndPartition(t, recs, 2)
	})

	t.Run("empty window", func(t *testing.T) {
		topic := e.topic(t, ctx, "pub-empty")
		w := win(14, 30)
		e.putWindow(t, ctx, w)
		res, err := PublishWindow(ctx, e.deps(topic), w)
		if err != nil || res.Records != 0 || res.Parts != 0 || res.Lines != 0 {
			t.Fatalf("PublishWindow = %+v, %v; want an empty result", res, err)
		}
		if recs := e.consumeAll(t, ctx, topic, 0); len(recs) != 0 {
			t.Fatalf("an empty window produced %d records", len(recs))
		}
		e.noDLQ(t, ctx, w)
	})
}
