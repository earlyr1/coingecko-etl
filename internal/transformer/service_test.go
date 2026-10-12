package transformer

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/s3"
	"coingecko-etl/internal/testutil"
	"coingecko-etl/internal/validation"
	"coingecko-etl/schemas"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunEndToEnd(t *testing.T) {
	e := newEnv(t)
	brokers := testutil.StartRedpanda(t)
	ctx := context.Background()
	const topic = "coingecko.raw"

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(ctx, 6, 1, map[string]*string{"retention.ms": ptr("604800000")}, topic); err != nil {
		t.Fatal(err)
	}

	evs := fixtureEvents(t)
	var recs []*kgo.Record
	// Message keys pick partitions. Production keys are "coins_markets" and the
	// coin id; here some copies get other keys on purpose so that several
	// partitions (and so both replicas) have work, and the CDN copy of the
	// bitcoin response lands on another partition than the original: it must
	// still be dropped, because dedup claims are global.
	keys := []string{"coins_markets", "coins_markets", "bitcoin", "ethereum", "tether", "coins_markets"}
	for i, ev := range append(evs, evs[0]) { // the last one is a publisher replay of markets t0
		v, err := event.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		key := keys[i]
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: v, Headers: []kgo.RecordHeader{
			{Key: queue.HeaderEventID, Value: []byte(ev.EventID)},
			{Key: queue.HeaderRawObject, Value: []byte("source=coingecko/ingest_date=2026-10-10/window=1000/part-00001.jsonl")},
		}})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: "00000000-0000-0000-0000-000000000001", TransformMaxRecords: 500}
	runFor := func(d time.Duration, done func() bool) {
		t.Helper()
		rctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := 0; i < 2; i++ { // two transformer replicas in one consumer group
			wg.Add(1)
			go func(i int) { defer wg.Done(); errs[i] = Run(rctx, cfg, e.deps) }(i)
		}
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) && !done() {
			time.Sleep(500 * time.Millisecond)
		}
		cancel()
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("Run %d: %v", i, err)
			}
		}
	}

	committedAll := func() bool {
		offs, err := adm.FetchOffsets(ctx, ConsumerGroup)
		if err != nil {
			return false
		}
		ok := true
		ends.Each(func(lo kadm.ListedOffset) {
			if lo.Offset == 0 {
				return
			}
			c, found := offs.Lookup(topic, lo.Partition)
			if !found || c.At != lo.Offset {
				ok = false
			}
		})
		return ok
	}
	runFor(60*time.Second, committedAll)
	if !committedAll() {
		t.Fatal("not every partition was committed to its end offset within 60s")
	}

	// Collect every object listed by written manifest rows.
	rows, err := e.deps.Pool.Query(ctx, `SELECT topic_id, kafka_partition, first_offset FROM pipeline.consumer_batches WHERE status = 'written'`)
	if err != nil {
		t.Fatal(err)
	}
	type bk struct {
		id    uuid.UUID
		p     int32
		first int64
	}
	var batches []bk
	for rows.Next() {
		var b bk
		if err := rows.Scan(&b.id, &b.p, &b.first); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	all := map[string][]byte{}
	for _, b := range batches {
		for k, v := range e.objects(t, b.id, b.p, b.first) {
			all[k] = v
		}
	}
	wantProcessed, wantRejected := expected(t, evs, e.deps.Rules)
	counts := e.countLines(t, all)
	t.Logf("batches=%d objects=%d lines=%v", len(batches), len(all), counts)
	if wantProcessed == 0 {
		t.Fatal("fixtures produced no processed rows")
	}
	if counts[s3.BucketProcessed] != wantProcessed || counts[s3.BucketRejected] != wantRejected {
		t.Fatalf("processed=%d rejected=%d, want %d and %d", counts[s3.BucketProcessed], counts[s3.BucketRejected], wantProcessed, wantRejected)
	}

	// A restart resumes from the committed offsets: one more message yields
	// exactly one new batch, covering only that message; nothing committed
	// is read again.
	var before int
	if err := e.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	extra := fixtureEvent(t, "markets_top10_t2.json", event.EndpointMarkets, "", "2026-10-10T10:04:17.000Z") // a CDN copy of t2
	v, err := event.Marshal(extra)
	if err != nil {
		t.Fatal(err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte("coins_markets"), Value: v, Headers: []kgo.RecordHeader{
		{Key: queue.HeaderEventID, Value: []byte(extra.EventID)},
		{Key: queue.HeaderRawObject, Value: []byte("source=coingecko/ingest_date=2026-10-10/window=1000/part-00002.jsonl")},
	}}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
	committedExtra := func() bool {
		offs, err := adm.FetchOffsets(ctx, ConsumerGroup)
		if err != nil {
			return false
		}
		c, ok := offs.Lookup(topic, rec.Partition)
		return ok && c.At == rec.Offset+1
	}
	runFor(60*time.Second, committedExtra)
	if !committedExtra() {
		t.Fatalf("partition %d not committed past the extra message at offset %d", rec.Partition, rec.Offset)
	}
	var after int
	if err := e.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("restart created %d new batches, want exactly 1", after-before)
	}
	var first, last int64
	var status string
	if err := e.deps.Pool.QueryRow(ctx, `SELECT first_offset, last_offset, status FROM pipeline.consumer_batches
		WHERE kafka_partition = $1 ORDER BY first_offset DESC LIMIT 1`, rec.Partition).Scan(&first, &last, &status); err != nil {
		t.Fatal(err)
	}
	if first != rec.Offset || last != rec.Offset || status != statusWritten {
		t.Fatalf("new batch = [%d..%d] %s, want [%d..%d] written", first, last, status, rec.Offset, rec.Offset)
	}
}

func ptr(s string) *string { return &s }

// testEpoch is the ETL_PIPELINE_EPOCH the Run tests configure.
const testEpoch = "00000000-0000-0000-0000-000000000001"

// startRun runs one transformer replica in the background. stop cancels it
// and returns Run's error; it is also called at cleanup.
func startRun(t *testing.T, cfg config.Config, d Deps) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, d) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() { cancel(); err = <-done })
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// waitFor polls cond every 250ms until it holds (true) or d elapses (false).
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(250 * time.Millisecond)
	}
	return true
}

// committedOffset returns the group's committed offset of partition 0 of
// topic, or -1 when nothing is committed.
func committedOffset(t *testing.T, adm *kadm.Client, topic string) int64 {
	t.Helper()
	offs, err := adm.FetchOffsets(context.Background(), ConsumerGroup)
	if err != nil {
		return -1
	}
	o, ok := offs.Lookup(topic, 0)
	if !ok || o.Err != nil {
		return -1
	}
	return o.At
}

// newTopic creates a one-partition topic and produces evs to it with the
// headers message() uses, so Run sees exactly the Messages that
// message(t, 0, i, evs[i]) builds. It returns those messages and the topic id.
func newTopic(t *testing.T, cl *kgo.Client, topic string, evs []event.RawEvent) ([]Message, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	var msgs []Message
	var recs []*kgo.Record
	for i, ev := range evs {
		m := message(t, 0, int64(i), ev)
		msgs = append(msgs, m)
		var hs []kgo.RecordHeader
		for k, v := range m.Headers {
			hs = append(hs, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs = append(recs, &kgo.Record{Topic: topic, Key: m.Key, Value: m.Value, Headers: hs})
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	id, err := queue.TopicID(ctx, cl, topic, uuid.MustParse(testEpoch))
	if err != nil {
		t.Fatal(err)
	}
	return msgs, id
}

// On partition assignment Run looks up the manifest batch that covers the
// committed offset (spec §7.4): a written one is skipped and its end
// committed, a planned one is reprocessed from its exact start.
func TestRunAssignmentRecovery(t *testing.T) {
	e := newEnv(t)
	brokers := testutil.StartRedpanda(t)
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	commit := func(t *testing.T, topic string, at int64) {
		t.Helper()
		var offs kadm.Offsets
		offs.AddOffset(topic, 0, at, -1)
		if err := adm.CommitAllOffsets(ctx, ConsumerGroup, offs); err != nil {
			t.Fatal(err)
		}
	}
	manifestRows := func(t *testing.T, id uuid.UUID) int {
		t.Helper()
		var n int
		if err := e.deps.Pool.QueryRow(ctx, `SELECT count(*) FROM pipeline.consumer_batches WHERE topic_id=$1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Crash after TX2, before the offset commit, at the end of the
	// partition: nothing new arrives, yet the end of the written batch must
	// be committed without processing it again.
	t.Run("a written batch covering the committed offset is skipped and committed while idle", func(t *testing.T) {
		e.reset(t)
		const topic = "test.recover-written"
		msgs, id := newTopic(t, cl, topic, fixtureEvents(t)[:3])
		res, err := ProcessBatch(ctx, e.deps, Batch{TopicID: id, Topic: topic, Partition: 0, Messages: msgs})
		if err != nil || !res.Written || res.LastOffset != 2 {
			t.Fatalf("ProcessBatch = %+v, %v", res, err)
		}
		commit(t, topic, 0)

		stop := startRun(t, config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: testEpoch, TransformMaxRecords: 500}, e.deps)
		ok := waitFor(45*time.Second, func() bool { return committedOffset(t, adm, topic) == 3 })
		if err := stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !ok {
			t.Fatalf("committed offset = %d, want 3 (past the written batch)", committedOffset(t, adm, topic))
		}
		if n := manifestRows(t, id); n != 1 {
			t.Errorf("manifest rows = %d, want 1 (no new batch)", n)
		}
	})

	// Crash after TX1, with the committed offset inside the planned range
	// (here set by hand): the batch must be redone from its first offset with
	// exactly the bytes TX1 planned.
	t.Run("a planned batch covering the committed offset is reprocessed from its start", func(t *testing.T) {
		e.reset(t)
		const topic = "test.recover-planned"
		msgs, id := newTopic(t, cl, topic, fixtureEvents(t)[:3])
		p, err := planBatch(ctx, e.deps, Batch{TopicID: id, Topic: topic, Partition: 0, Messages: msgs})
		if err != nil {
			t.Fatal(err)
		}
		if len(p.objects) == 0 {
			t.Fatal("the planned batch has no objects")
		}
		commit(t, topic, 1)

		stop := startRun(t, config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: testEpoch, TransformMaxRecords: 500}, e.deps)
		ok := waitFor(45*time.Second, func() bool { return committedOffset(t, adm, topic) == 3 })
		if err := stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !ok {
			t.Fatalf("committed offset = %d, want 3", committedOffset(t, adm, topic))
		}
		if n := manifestRows(t, id); n != 1 {
			t.Errorf("manifest rows = %d, want 1 (the planned batch)", n)
		}
		objs := e.objects(t, id, 0, 0) // fails unless the batch is written
		if len(objs) != len(p.objects) {
			t.Fatalf("written %d objects, TX1 planned %d", len(objs), len(p.objects))
		}
		for _, o := range p.objects {
			if !bytes.Equal(objs[o.Bucket+"/"+o.Key], o.Body) {
				t.Errorf("%s/%s differs from the planned bytes", o.Bucket, o.Key)
			}
		}
	})
}

// lagSeries reads etl_consumer_lag_records{partition} from reg without
// creating the series (WithLabelValues would).
func lagSeries(t *testing.T, reg *prometheus.Registry, partition string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "etl_consumer_lag_records" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "partition" && lp.GetValue() == partition {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// A replica stuck on one batch (here: Postgres unreachable, so TX1 never
// starts) commits nothing and reports the lag from the first uncommitted
// offset, so the lag grows with every produced record. Leaving the group
// removes the partition's lag series.
func TestRunReportsLagWhileBatchFails(t *testing.T) {
	brokers := testutil.StartRedpanda(t)
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	const topic = "test.stuck"
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	produce := func(n int) {
		t.Helper()
		recs := make([]*kgo.Record, n)
		for i := range recs {
			recs[i] = &kgo.Record{Topic: topic, Value: []byte(`{}`)}
		}
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce(3)

	down, err := pgxpool.New(ctx, "postgres://etl:etl@127.0.0.1:1/etl?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(down.Close)
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		t.Fatal(err)
	}
	preg := prometheus.NewRegistry()
	d := Deps{Pool: down, Reg: reg, Metrics: obs.NewMetrics(preg), Log: slog.New(slog.DiscardHandler)}
	stop := startRun(t, config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: testEpoch, TransformMaxRecords: 500}, d)

	lagIs := func(want float64) func() bool {
		return func() bool { v, ok := lagSeries(t, preg, "0"); return ok && v == want }
	}
	if !waitFor(60*time.Second, lagIs(3)) {
		v, ok := lagSeries(t, preg, "0")
		t.Fatalf("lag = %v (series present %v), want 3: the failing batch starts at offset 0", v, ok)
	}
	produce(2)
	if !waitFor(60*time.Second, lagIs(5)) {
		v, _ := lagSeries(t, preg, "0")
		t.Fatalf("lag = %v after 2 more records, want 5", v)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if at := committedOffset(t, adm, topic); at >= 0 {
		t.Errorf("committed offset %d although no batch was written", at)
	}
	if v, ok := lagSeries(t, preg, "0"); ok {
		t.Errorf("lag series still exported (%v) after the replica left the group", v)
	}
}

// A batch whose processed object already holds other bytes halts its
// partition in place (spec §7.4 "alert and stop"): Run keeps running, the
// partition is neither committed nor fetched again and keeps reporting its
// lag, the mismatch counter (ETLObjectChecksumMismatch) stays at 1, and the
// other partition carries on.
func TestRunHaltsPartitionOnChecksumMismatch(t *testing.T) {
	e := newEnv(t)
	brokers := testutil.StartRedpanda(t)
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	const topic = "test.halt"
	if _, err := adm.CreateTopic(ctx, 2, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	produce := func(p int32, ev event.RawEvent) {
		t.Helper()
		m := message(t, p, 0, ev)
		var hs []kgo.RecordHeader
		for k, v := range m.Headers {
			hs = append(hs, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Key: m.Key, Value: m.Value, Headers: hs}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	committed := func(p int32) int64 {
		offs, err := adm.FetchOffsets(ctx, ConsumerGroup)
		if err != nil {
			return -1
		}
		o, ok := offs.Lookup(topic, p)
		if !ok || o.Err != nil {
			return -1
		}
		return o.At
	}
	mismatches := func() float64 { return promtest.ToFloat64(e.deps.Metrics.S3Puts.WithLabelValues("mismatch")) }
	lagIs := func(p string, want float64) func() bool {
		return func() bool { v, ok := lagSeries(t, e.reg, p); return ok && v == want }
	}

	evs := fixtureEvents(t)
	produce(0, evs[0]) // markets t0: its first object is the market_snapshot one
	produce(1, evs[2]) // bitcoin detail t0
	key := ObjectKey(s3.BucketProcessed, "dataset=market_snapshot/v=1/event_date=2026-10-10", topic, 0, 0)
	if _, err := e.deps.S3.PutIfAbsent(ctx, s3.BucketProcessed, key, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}

	oldEvery := haltedLagEvery
	haltedLagEvery = 500 * time.Millisecond
	defer func() { haltedLagEvery = oldEvery }()
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(rctx, config.Config{Brokers: brokers, Topic: topic, PipelineEpoch: testEpoch, TransformMaxRecords: 500}, e.deps)
	}()
	running := func() {
		t.Helper()
		select {
		case err := <-done:
			t.Fatalf("Run returned %v while a partition was halted", err)
		default:
		}
	}

	if !waitFor(60*time.Second, func() bool { return committed(1) == 1 && mismatches() == 1 }) {
		t.Fatalf("committed(1)=%d mismatches=%v, want 1 and 1", committed(1), mismatches())
	}
	running()
	if !waitFor(10*time.Second, lagIs("0", 1)) {
		v, ok := lagSeries(t, e.reg, "0")
		t.Fatalf("lag of the halted partition = %v (present %v), want 1", v, ok)
	}

	// The halted partition is not fetched again: more records only raise its
	// lag. The other partition keeps going.
	produce(0, evs[1])
	produce(0, evs[4])
	produce(1, evs[3])
	if !waitFor(60*time.Second, func() bool { return committed(1) == 2 }) {
		t.Fatalf("partition 1 committed %d, want 2", committed(1))
	}
	if !waitFor(30*time.Second, lagIs("0", 3)) {
		v, _ := lagSeries(t, e.reg, "0")
		t.Fatalf("lag of the halted partition = %v after 2 more records, want 3", v)
	}
	running()
	if got := mismatches(); got != 1 {
		t.Errorf("mismatch counter = %v, want 1: a halted batch is not retried", got)
	}
	if at := committed(0); at != -1 {
		t.Errorf("halted partition committed offset %d, want none", at)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
