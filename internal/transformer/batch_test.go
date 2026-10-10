package transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/event"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/xtime"

	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
)

// batchOf builds a batch on partition 0 starting at offset 100.
func batchOf(t *testing.T, topic string, evs []event.RawEvent, extra ...[]byte) Batch {
	t.Helper()
	b := Batch{TopicID: uuid.New(), Topic: topic, Partition: 0}
	off := int64(100)
	for _, ev := range evs {
		b.Messages = append(b.Messages, message(t, 0, off, ev))
		off++
	}
	for _, v := range extra {
		b.Messages = append(b.Messages, Message{Partition: 0, Offset: off, Value: v, Headers: map[string]string{}})
		off++
	}
	return b
}

// withPayload replaces the payload of ev and recomputes its checksum and
// idempotency key, so the event stays internally consistent.
func withPayload(t *testing.T, ev *event.RawEvent, payload []byte) {
	t.Helper()
	sha := event.SHA256Hex(payload)
	idem := event.IdempotencyKey(ev.Source, ev.Endpoint, ev.Request.Params, sha)
	ev.Payload, ev.PayloadSHA256, ev.IdempotencyKey = payload, &sha, &idem
}

// dlqLines decodes every line of the dlq objects in objs, ordered by offset.
func dlqLines(t *testing.T, objs map[string][]byte) []dlqRow {
	t.Helper()
	var out []dlqRow
	for k, body := range objs {
		if !strings.HasPrefix(k, s3.BucketDLQ+"/") {
			continue
		}
		for _, l := range bytes.Split(bytes.TrimSuffix(body, []byte("\n")), []byte("\n")) {
			var r dlqRow
			if err := json.Unmarshal(l, &r); err != nil {
				t.Fatalf("%s: %v", k, err)
			}
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Origin.Offset < out[j].Origin.Offset })
	return out
}

func TestProcessBatch(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := e.deps

	t.Run("happy path writes valid objects, drops CDN copies and replays, sends broken messages to dlq", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		// a replay: the publisher sent markets t0 again (same event_id) at a later offset
		withReplay := append(append([]event.RawEvent{}, evs...), evs[0])
		bad := evs[4]
		badSHA := "0000000000000000000000000000000000000000000000000000000000000000"
		bad.PayloadSHA256 = &badSHA
		badValue, err := event.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		b := batchOf(t, "test.happy", withReplay, badValue, []byte(`{"schema_version":1,"schema_version":1}`), []byte(`not json`))

		res, err := ProcessBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		lastOff := b.Messages[len(b.Messages)-1].Offset
		if !res.Written || res.LastOffset != lastOff {
			t.Fatalf("result = %+v, want written up to %d", res, lastOff)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		counts := e.countLines(t, objs)
		wantProcessed, wantRejected := expected(t, evs, d.Rules)
		if wantProcessed == 0 {
			t.Fatal("fixtures produced no processed rows")
		}
		if counts[s3.BucketProcessed] != wantProcessed {
			t.Errorf("processed rows = %d, want %d", counts[s3.BucketProcessed], wantProcessed)
		}
		if counts[s3.BucketRejected] != wantRejected {
			t.Errorf("rejected rows = %d, want %d", counts[s3.BucketRejected], wantRejected)
		}
		if counts[s3.BucketDLQ] != 3 {
			t.Errorf("dlq rows = %d, want 3 (integrity, duplicate key, not json)", counts[s3.BucketDLQ])
		}
		if got := promtest.ToFloat64(d.Metrics.DedupDropped.WithLabelValues("duplicate_response")); got == 0 {
			t.Error("CDN copies (markets t1, bitcoin t1) must be dropped as duplicate_response")
		}
		if got := promtest.ToFloat64(d.Metrics.DedupDropped.WithLabelValues("replay")); got == 0 {
			t.Error("the republished markets t0 must be dropped as replay")
		}
		if got := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageInput, classIntegrity)); got != 1 {
			t.Errorf("dlq integrity_error = %v, want 1", got)
		}
		if got := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageInput, classDecode)); got != 2 {
			t.Errorf("dlq decode_error = %v, want 2", got)
		}
		// The lake is plain files: the processed objects exist on the host.
		for k := range objs {
			if _, err := os.Stat(filepath.Join(e.dataDir, k)); err != nil {
				t.Errorf("object %s is not a plain file under the data dir: %v", k, err)
			}
		}
		// Processing the same batch again (redelivery after TX2) is a no-op.
		res2, err := ProcessBatch(ctx, d, b)
		if err != nil || res2 != res {
			t.Fatalf("second run = %+v, %v; want %+v", res2, err, res)
		}
	})

	t.Run("crash after TX1 then retry writes identical objects without duplicates", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		b := batchOf(t, "test.crash-tx1", evs)
		p, err := planBatch(ctx, d, b) // TX1 committed, process "dies" before any PUT
		if err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if res.LastOffset != 104 {
			t.Fatalf("LastOffset = %d, want 104", res.LastOffset)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		if len(objs) != len(p.objects) {
			t.Fatalf("got %d objects, planned %d", len(objs), len(p.objects))
		}
		for _, o := range p.objects {
			if !bytes.Equal(objs[o.Bucket+"/"+o.Key], o.Body) {
				t.Errorf("%s/%s differs from the first attempt", o.Bucket, o.Key)
			}
		}
		wantProcessed, _ := expected(t, evs, d.Rules)
		if got := e.countLines(t, objs)[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d (records lost or duplicated)", got, wantProcessed)
		}
	})

	t.Run("partial PUTs then retry: existing objects answer 412 with an equal checksum", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.partial", fixtureEvents(t))
		p, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.objects) < 2 {
			t.Fatalf("need at least 2 objects, got %d", len(p.objects))
		}
		if _, err := d.S3.PutIfAbsent(ctx, p.objects[0].Bucket, p.objects[0].Key, p.objects[0].Body); err != nil {
			t.Fatal(err)
		}
		before := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("exists"))
		if _, err := ProcessBatch(ctx, d, b); err != nil {
			t.Fatal(err)
		}
		if got := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("exists")) - before; got != 1 {
			t.Errorf("exists puts = %v, want 1", got)
		}
		e.objects(t, b.TopicID, 0, 100) // all objects readable, status written
	})

	t.Run("an object with different bytes under the same key stops the batch", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.mismatch", fixtureEvents(t))
		p, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.S3.PutIfAbsent(ctx, p.objects[0].Bucket, p.objects[0].Key, []byte("{}\n")); err != nil {
			t.Fatal(err)
		}
		_, err = ProcessBatch(ctx, d, b)
		if !errors.Is(err, s3.ErrChecksumMismatch) {
			t.Fatalf("err = %v, want ErrChecksumMismatch", err)
		}
		_, _, status, found, err := CoveringBatch(ctx, d.Pool, b.TopicID, 0, 100)
		if err != nil || !found || status != statusPlanned {
			t.Fatalf("batch must stay planned: found=%v status=%s err=%v", found, status, err)
		}
	})

	// A re-plan that would store other objects than an earlier attempt is
	// non-deterministic; it counts as a mismatch, like a 412 with other bytes.
	t.Run("a re-plan whose objects differ from the stored manifest is non-deterministic", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.nondeterministic", fixtureEvents(t))
		if _, err := planBatch(ctx, d, b); err != nil {
			t.Fatal(err)
		}
		// As if an earlier attempt (another build) had stored other checksums.
		if _, err := d.Pool.Exec(ctx, `UPDATE pipeline.consumer_batches SET content_sha256 = array_replace(content_sha256, content_sha256[1], sha256('other'))
			WHERE topic_id = $1`, b.TopicID); err != nil {
			t.Fatal(err)
		}
		before := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("mismatch"))
		if _, err := ProcessBatch(ctx, d, b); !errors.Is(err, ErrNonDeterministic) {
			t.Fatalf("err = %v, want ErrNonDeterministic", err)
		}
		if n := promtest.ToFloat64(d.Metrics.S3Puts.WithLabelValues("mismatch")) - before; n != 1 {
			t.Errorf("s3_put_total{mismatch} grew by %v, want 1", n)
		}
	})

	t.Run("zombie: two consumers process the same batch concurrently", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.zombie", fixtureEvents(t))
		var wg sync.WaitGroup
		results := make([]Result, 2)
		errs := make([]error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = ProcessBatch(ctx, d, b)
			}(i)
		}
		wg.Wait()
		for i := range errs {
			if errs[i] != nil {
				t.Fatalf("consumer %d: %v", i, errs[i])
			}
		}
		if results[0] != results[1] || results[0].LastOffset != 104 {
			t.Fatalf("results differ: %+v", results)
		}
		var rows int
		if err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches WHERE topic_id=$1`, b.TopicID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("manifest rows = %d, want 1", rows)
		}
		wantProcessed, _ := expected(t, fixtureEvents(t), d.Rules)
		if got := e.countLines(t, e.objects(t, b.TopicID, 0, 100))[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d", got, wantProcessed)
		}
	})

	t.Run("boundary shift: a planned [F..L] wins over a longer poll [F..L+n]", func(t *testing.T) {
		e.reset(t)
		evs := fixtureEvents(t)
		long := batchOf(t, "test.shift", evs)
		short := long
		short.Messages = long.Messages[:3] // [100..102]
		p, err := planBatch(ctx, d, short)
		if err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, long) // polled [100..104] after a restart
		if err != nil {
			t.Fatal(err)
		}
		if res.LastOffset != 102 {
			t.Fatalf("LastOffset = %d, want 102 (the planned range)", res.LastOffset)
		}
		objs := e.objects(t, long.TopicID, 0, 100)
		for _, o := range p.objects {
			if !bytes.Equal(objs[o.Bucket+"/"+o.Key], o.Body) {
				t.Errorf("%s/%s differs from the planned batch", o.Bucket, o.Key)
			}
		}
		// The caller continues with the rest as a new batch.
		rest := long
		rest.Messages = long.Messages[3:]
		res, err = ProcessBatch(ctx, d, rest)
		if err != nil || res.LastOffset != 104 {
			t.Fatalf("rest: %+v %v", res, err)
		}
		// Together the two batches hold every expected row exactly once.
		all := e.objects(t, long.TopicID, 0, 100)
		for k, v := range e.objects(t, long.TopicID, 0, 103) {
			all[k] = v
		}
		wantProcessed, _ := expected(t, evs, d.Rules)
		if got := e.countLines(t, all)[s3.BucketProcessed]; got != wantProcessed {
			t.Errorf("processed rows = %d, want %d", got, wantProcessed)
		}
	})

	t.Run("a shorter poll than the planned range asks for more records", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.incomplete", fixtureEvents(t))
		if _, err := planBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		short := long
		short.Messages = long.Messages[:2]
		_, err := ProcessBatch(ctx, d, short)
		var inc *IncompleteBatchError
		if !errors.As(err, &inc) || inc.NeedLast != 104 {
			t.Fatalf("err = %v, want IncompleteBatchError needing 104", err)
		}
	})

	t.Run("a poll starting inside a planned batch must rewind; inside a written batch it skips", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.rewind", fixtureEvents(t))
		if _, err := planBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		mid := long
		mid.Messages = long.Messages[2:]
		_, err := ProcessBatch(ctx, d, mid)
		var rw *RewindError
		if !errors.As(err, &rw) || rw.Offset != 100 {
			t.Fatalf("err = %v, want RewindError to 100", err)
		}
		if _, err := ProcessBatch(ctx, d, long); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, mid)
		if err != nil || !res.Written || res.LastOffset != 104 {
			t.Fatalf("mid after written = %+v, %v; want skip to 104", res, err)
		}
	})

	t.Run("a later planned batch inside the poll trims the poll before it", func(t *testing.T) {
		e.reset(t)
		long := batchOf(t, "test.trim", fixtureEvents(t))
		tail := long
		tail.Messages = long.Messages[3:] // [103..104]
		if _, err := planBatch(ctx, d, tail); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, long)
		if err != nil || res.LastOffset != 102 {
			t.Fatalf("got %+v, %v; want LastOffset 102", res, err)
		}
		first, last, status, found, err := CoveringBatch(ctx, d.Pool, long.TopicID, 0, 101)
		if err != nil || !found || first != 100 || last != 102 || status != statusWritten {
			t.Fatalf("covering = %d..%d %s %v %v", first, last, status, found, err)
		}
	})

	// versitygw restarted or unreachable in the middle of a batch: the PUTs
	// fail, the manifest stays planned (so the caller commits no offset), and
	// the retry after recovery writes exactly the objects TX1 planned.
	t.Run("object storage down mid-batch: batch stays planned, retry writes the planned objects", func(t *testing.T) {
		e.reset(t)
		b := batchOf(t, "test.s3down", fixtureEvents(t))
		down, err := s3.New(ctx, s3.Config{Endpoint: "http://127.0.0.1:1", AccessKey: "etl", SecretKey: "etlsecret123", Region: "us-east-1"})
		if err != nil {
			t.Fatal(err)
		}
		dd := d
		dd.S3 = down
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = ProcessBatch(tctx, dd, b)
		cancel()
		if err == nil {
			t.Fatal("ProcessBatch succeeded although object storage is unreachable")
		}
		_, _, status, found, err := CoveringBatch(ctx, d.Pool, b.TopicID, 0, 100)
		if err != nil || !found || status == statusWritten {
			t.Fatalf("after failed PUTs: status=%s found=%v err=%v, want planned", status, found, err)
		}
		var planned []string
		if err := d.Pool.QueryRow(ctx, `SELECT object_keys FROM pipeline.consumer_batches
			WHERE topic_id=$1 AND kafka_partition=0 AND first_offset=100`, b.TopicID).Scan(&planned); err != nil {
			t.Fatal(err)
		}
		res, err := ProcessBatch(ctx, d, b)
		if err != nil || !res.Written || res.LastOffset != 104 {
			t.Fatalf("retry = %+v, %v", res, err)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		if len(objs) != len(planned) {
			t.Fatalf("written %d objects, TX1 planned %d", len(objs), len(planned))
		}
		for _, k := range planned {
			if _, ok := objs[k]; !ok {
				t.Errorf("planned object %s missing after retry", k)
			}
		}
		wantProcessed, wantRejected := expected(t, fixtureEvents(t), d.Rules)
		got := e.countLines(t, objs)
		if got[s3.BucketProcessed] != wantProcessed || got[s3.BucketRejected] != wantRejected {
			t.Fatalf("lines = %v, want processed %d rejected %d", got, wantProcessed, wantRejected)
		}
	})

	t.Run("input failing S1 or S0 goes to dlq with its violations and the manifest time", func(t *testing.T) {
		e.reset(t)
		ev := fixtureEvent(t, "markets_top10_t0.json", event.EndpointMarkets, "", "2026-10-10T10:02:32.000Z")
		withPayload(t, &ev, []byte(`[{"id":"bitcoin"}]`)) // S0 wants symbol, name, current_price, last_updated
		b := batchOf(t, "test.inputschema", []event.RawEvent{ev}, []byte(`{"schema_version":1}`))
		s1Before := promtest.ToFloat64(d.Metrics.SchemaValidations.WithLabelValues(stageInput, "raw_event.v1", "fail"))
		s0Before := promtest.ToFloat64(d.Metrics.SchemaValidations.WithLabelValues(stageInput, "coins_markets.v1", "fail"))
		res, err := ProcessBatch(ctx, d, b)
		if err != nil || !res.Written || res.LastOffset != 101 {
			t.Fatalf("ProcessBatch = %+v, %v", res, err)
		}
		var plannedAt time.Time
		if err := d.Pool.QueryRow(ctx, `SELECT planned_at FROM pipeline.consumer_batches WHERE topic_id=$1`, b.TopicID).Scan(&plannedAt); err != nil {
			t.Fatal(err)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		if got := e.countLines(t, objs); got[s3.BucketDLQ] != 2 || len(objs) != 1 {
			t.Fatalf("objects = %d, lines = %v; want one dlq object with 2 lines", len(objs), got)
		}
		rows := dlqLines(t, objs)
		want := []struct {
			offset int64
			class  string
		}{{100, classSourceSchema}, {101, classSchema}}
		for i, r := range rows {
			if r.Origin.Offset != want[i].offset || r.ErrorClass != want[i].class || r.Stage != stageInput || len(r.Violations) == 0 {
				t.Errorf("dlq line %d = offset %d class %s stage %s, %d violations; want offset %d class %s with violations",
					i, r.Origin.Offset, r.ErrorClass, r.Stage, len(r.Violations), want[i].offset, want[i].class)
			}
			if r.OccurredAt != xtime.Format(plannedAt) {
				t.Errorf("dlq line %d occurred_at = %s, want the manifest planned_at %s", i, r.OccurredAt, xtime.Format(plannedAt))
			}
		}
		if rows[0].Origin.EventID != ev.EventID {
			t.Errorf("S0 failure origin event_id = %q, want %q", rows[0].Origin.EventID, ev.EventID)
		}
		if got := promtest.ToFloat64(d.Metrics.SchemaValidations.WithLabelValues(stageInput, "raw_event.v1", "fail")) - s1Before; got != 1 {
			t.Errorf("raw_event.v1 failed validations = %v, want 1", got)
		}
		if got := promtest.ToFloat64(d.Metrics.SchemaValidations.WithLabelValues(stageInput, "coins_markets.v1", "fail")) - s0Before; got != 1 {
			t.Errorf("coins_markets.v1 failed validations = %v, want 1", got)
		}
	})

	// A message without the raw_object header yields rows with an empty
	// _raw_object: processed rows fail S2 and go to dlq; a rejected row fails
	// its own schema (S3), so it is not written and only logged (spec §5.2).
	t.Run("rows failing S2 go to dlq; rejected rows failing S3 are only logged", func(t *testing.T) {
		e.reset(t)
		ev := fixtureEvent(t, "markets_top10_t0.json", event.EndpointMarkets, "", "2026-10-10T10:02:32.000Z")
		payload := bytes.Replace(ev.Payload, []byte(`"current_price":82800,`), []byte(`"current_price":null,`), 1)
		if bytes.Equal(payload, ev.Payload) {
			t.Fatal("fixture has no bitcoin current_price to null")
		}
		withPayload(t, &ev, payload) // bitcoin becomes a null_price reject
		wantProcessed, wantRejected := expected(t, []event.RawEvent{ev}, d.Rules)
		if wantProcessed == 0 || wantRejected == 0 {
			t.Fatalf("need processed and rejected rows, got %d and %d", wantProcessed, wantRejected)
		}
		b := batchOf(t, "test.outputschema", []event.RawEvent{ev})
		delete(b.Messages[0].Headers, queue.HeaderRawObject)
		before := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageOutput, classOutputSchema))
		// Validation runs before the claims and is deterministic: a re-plan
		// of the same batch stores the same objects.
		p1, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatal(err)
		}
		p2, err := planBatch(ctx, d, b)
		if err != nil {
			t.Fatalf("re-plan: %v", err)
		}
		if len(p1.objects) != len(p2.objects) {
			t.Fatalf("re-plan built %d objects, first plan %d", len(p2.objects), len(p1.objects))
		}
		for i := range p1.objects {
			if p1.objects[i].Key != p2.objects[i].Key || !bytes.Equal(p1.objects[i].Body, p2.objects[i].Body) {
				t.Fatalf("re-plan object %d differs: %s vs %s", i, p1.objects[i].Key, p2.objects[i].Key)
			}
		}
		if _, err := ProcessBatch(ctx, d, b); err != nil {
			t.Fatal(err)
		}
		objs := e.objects(t, b.TopicID, 0, 100)
		got := e.countLines(t, objs)
		if got[s3.BucketProcessed] != 0 || got[s3.BucketRejected] != 0 || got[s3.BucketDLQ] != wantProcessed {
			t.Fatalf("lines = %v, want only %d dlq lines", got, wantProcessed)
		}
		for _, r := range dlqLines(t, objs) {
			if r.Stage != stageOutput || r.ErrorClass != classOutputSchema || r.Origin.Dataset != "market_snapshot" ||
				r.Origin.RecordKey == "" || len(r.Violations) == 0 || r.Violations[0].InstanceLocation != "/_raw_object" {
				t.Errorf("unexpected dlq line %+v", r)
			}
		}
		if n := promtest.ToFloat64(d.Metrics.DLQ.WithLabelValues(stageOutput, classOutputSchema)) - before; n != float64(wantProcessed) {
			t.Errorf("dlq output_schema_violation = %v, want %d", n, wantProcessed)
		}
		var unwritable int
		if err := d.Pool.QueryRow(ctx, `SELECT (counts->>'unwritable')::int FROM pipeline.consumer_batches WHERE topic_id=$1`, b.TopicID).Scan(&unwritable); err != nil {
			t.Fatal(err)
		}
		if unwritable != wantRejected {
			t.Errorf("unwritable = %d, want %d (the rejected rows)", unwritable, wantRejected)
		}

		// Only rows bound for processed/ or rejected/ claim keys (spec §7.3):
		// the rows that failed S2 hold no claim, the cleaning-rule rejects do.
		var claimed, claimedProcessed int
		if err := d.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE dataset = 'market_snapshot' AND record_key NOT LIKE 'bitcoin|%')
			FROM pipeline.dedup_claims WHERE topic_id = $1 AND kafka_offset = 100`, b.TopicID).Scan(&claimed, &claimedProcessed); err != nil {
			t.Fatal(err)
		}
		if claimedProcessed != 0 || claimed != wantRejected {
			t.Fatalf("claims at offset 100: %d, of rows that failed S2: %d; want %d (the rejects) and 0", claimed, claimedProcessed, wantRejected)
		}

		// The documented remedy works: republishing the message (here with its
		// header restored) writes the rows to processed/ instead of dropping
		// them as replays.
		again := message(t, 0, 101, ev)
		res, err := ProcessBatch(ctx, d, Batch{TopicID: b.TopicID, Topic: b.Topic, Partition: 0, Messages: []Message{again}})
		if err != nil || !res.Written {
			t.Fatalf("republished batch = %+v, %v", res, err)
		}
		got = e.countLines(t, e.objects(t, b.TopicID, 0, 101))
		if got[s3.BucketProcessed] != wantProcessed {
			t.Fatalf("republished lines = %v, want %d processed", got, wantProcessed)
		}
	})

	// A replay of old raw objects (or a late snapshot) carries an event_date
	// outside the partition window that init and the fetcher maintain. The
	// claim INSERT fails with "no partition"; the batch must create the
	// partition and succeed instead of failing forever.
	t.Run("snapshot dated outside the dedup partition window creates its partition", func(t *testing.T) {
		e.reset(t)
		old := fixtureEvents(t)[0]
		payload := bytes.ReplaceAll(old.Payload, []byte(`"last_updated":"2026-10-10T`), []byte(`"last_updated":"2026-08-01T`))
		if bytes.Equal(payload, old.Payload) {
			t.Fatal("fixture has no 2026-10-10 last_updated to move")
		}
		sha := event.SHA256Hex(payload)
		idem := event.IdempotencyKey(old.Source, old.Endpoint, old.Request.Params, sha)
		old.Payload, old.PayloadSHA256, old.IdempotencyKey = payload, &sha, &idem
		b := batchOf(t, "test.oldpartition", []event.RawEvent{old})
		res, err := ProcessBatch(ctx, d, b)
		if err != nil || !res.Written {
			t.Fatalf("ProcessBatch = %+v, %v", res, err)
		}
		var claims int
		if err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.dedup_claims WHERE event_date = '2026-08-01'`).Scan(&claims); err != nil {
			t.Fatal(err)
		}
		if claims == 0 {
			t.Fatal("no claims stored for 2026-08-01")
		}
		found := false
		for k := range e.objects(t, b.TopicID, 0, 100) {
			found = found || (strings.HasPrefix(k, s3.BucketProcessed+"/") && strings.Contains(k, "/event_date=2026-08-01/"))
		}
		if !found {
			t.Fatal("no processed object under event_date=2026-08-01")
		}
	})
}
