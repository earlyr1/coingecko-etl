package transformer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"coingecko-etl/internal/config"
	"coingecko-etl/internal/queue"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/storage/s3"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ConsumerGroup is the Kafka consumer group of all transformer replicas.
const ConsumerGroup = "coingecko-transformer"

const (
	// retryBackoff is how long a partition waits after a transient failure
	// (Postgres or S3 down) before its records are fetched again.
	retryBackoff = 2 * time.Second
	// pollWait bounds one PollRecords call so the loop also turns while no
	// record arrives: an offset moved past a written batch on assignment is
	// committed promptly even if its partition stays idle.
	pollWait = 5 * time.Second
	// defaultMaxRecords caps one poll when TransformMaxRecords is not set.
	defaultMaxRecords = 500
)

// haltedLagEvery is how often the lag of halted partitions is refreshed from
// the topic's end offsets (a variable so tests can shorten it).
var haltedLagEvery = 15 * time.Second

// Run creates the daily dedup_claims partitions [today-1, today+claimDaysAhead]
// before its first poll and then every claimPartitionsEvery, as etl init does
// once (claimPartitionsRetry after a failure: Postgres may be down while the
// broker is not, and the lag must still be reported). A batch with an
// event_date outside them creates its own (ensureClaimPartitions).
const (
	claimDaysAhead       = 3
	claimPartitionsEvery = time.Hour
	claimPartitionsRetry = time.Minute
)

// ensureClaimsAhead creates the claims partitions ahead and returns when to
// do it next.
func ensureClaimsAhead(ctx context.Context, d Deps, now time.Time) time.Time {
	if err := pg.EnsureClaimPartitions(ctx, d.Pool, now, claimDaysAhead); err != nil {
		if ctx.Err() == nil {
			d.Log.Error("ensure claims partitions failed", "stage", "transform", "retry_in", claimPartitionsRetry.String(), "err", err)
		}
		return now.Add(claimPartitionsRetry)
	}
	return now.Add(claimPartitionsEvery)
}

// runner holds the per-process consumer state.
type runner struct {
	d          Deps
	topic      string
	topicID    uuid.UUID
	maxRecords int

	mu        sync.Mutex
	pending   map[int32][]Message // records held after IncompleteBatchError
	recovered map[int32]int64     // next offsets to commit after assignment recovery
	halted    map[int32]int64     // halted partitions -> first uncommitted offset
	unhalt    []int32             // halted partitions revoked or lost: resume fetching them

	lagRefreshed time.Time // last end-offset lookup for halted partitions (poll loop only)
}

// Run consumes the raw topic until ctx is cancelled and then returns nil; it
// returns an error only when it cannot start (topic id lookup, client).
//
// It polls at most TransformMaxRecords records at a time and processes the
// records of each partition as one batch (or several, when the manifest fixed
// other boundaries), one partition after the other, so a partition never has
// two batches in flight. BlockRebalanceOnPoll keeps every polled partition
// assigned until AllowRebalance, so each batch, its offset commit and any seek
// happen while this member owns the partition.
//
// A batch that would overwrite an object with different bytes, or whose
// re-plan differs from its manifest, needs a human (spec §7.4 "alert and
// stop"): its partition halts in place (see halt) while the others carry on.
func Run(ctx context.Context, cfg config.Config, d Deps) error {
	epoch, err := uuid.Parse(cfg.PipelineEpoch)
	if err != nil {
		return fmt.Errorf("ETL_PIPELINE_EPOCH: %w", err)
	}
	topicID, err := lookupTopicID(ctx, cfg, epoch)
	if err != nil {
		return err
	}
	maxRecords := cfg.TransformMaxRecords
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	r := &runner{d: d, topic: cfg.Topic, topicID: topicID, maxRecords: maxRecords,
		pending: map[int32][]Message{}, recovered: map[int32]int64{}, halted: map[int32]int64{}}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("etl-transformer"),
		kgo.ConsumerGroup(ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // no committed offset: read everything
		kgo.DisableAutoCommit(),                           // we commit only after the objects are written
		kgo.BlockRebalanceOnPoll(),                        // no partition moves while a batch is in flight
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMinBytes(1<<20),
		kgo.FetchMaxWait(5*time.Second),
		kgo.AdjustFetchOffsetsFn(r.adjustOffsets),
		kgo.OnPartitionsRevoked(r.forget),
		kgo.OnPartitionsLost(r.forget),
	)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	d.Log.Info("transformer started", "stage", "transform", "topic", cfg.Topic, "topic_id", topicID.String(), "group", ConsumerGroup)

	var partitionsDue time.Time // zero: before the first poll
	for {
		if now := time.Now(); !now.Before(partitionsDue) {
			partitionsDue = ensureClaimsAhead(ctx, d, now)
		}
		pctx, cancel := context.WithTimeout(ctx, pollWait)
		fetches := cl.PollRecords(pctx, r.maxRecords)
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return // shutting down, or pollWait passed without records
			}
			d.Log.Warn("fetch error", "stage", "transform", "topic", topic, "partition", p, "err", err)
		})
		r.commitRecovered(ctx, cl)
		r.resumeLost(cl)

		byPartition := map[int32][]*kgo.Record{}
		highWatermark := map[int32]int64{}
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			if fp.Topic != r.topic || len(fp.Records) == 0 {
				return
			}
			byPartition[fp.Partition] = append(byPartition[fp.Partition], fp.Records...)
			highWatermark[fp.Partition] = max(highWatermark[fp.Partition], fp.HighWatermark)
		})
		for _, p := range slices.Sorted(maps.Keys(byPartition)) {
			next := r.processPartition(ctx, cl, p, byPartition[p])
			// Lag counts from the first offset no commit covers yet, so a
			// replica stuck on one batch reports a growing lag.
			lag := max(highWatermark[p]-next, 0)
			d.Metrics.ConsumerLag.WithLabelValues(strconv.Itoa(int(p))).Set(float64(lag))
		}
		r.refreshHaltedLag(ctx, cl)
		cl.AllowRebalance()
	}
}

// lookupTopicID asks the broker for the topic id with a short-lived plain
// client: AdjustFetchOffsetsFn needs it as soon as the group is joined.
func lookupTopicID(ctx context.Context, cfg config.Config, epoch uuid.UUID) (uuid.UUID, error) {
	admin, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("etl-transformer-admin"))
	if err != nil {
		return uuid.Nil, err
	}
	defer admin.Close()
	return queue.TopicID(ctx, admin, cfg.Topic, epoch)
}

// processPartition turns the polled records of one partition into one or more
// batches and commits after each written batch. It returns the first offset
// of p that no commit covers yet, the position the consumer lag is measured
// from: LastOffset+1 of the last batch when every record was handled, else
// the first offset of the batch that failed, halted the partition, was
// rewound, is incomplete or whose commit was rejected.
func (r *runner) processPartition(ctx context.Context, cl *kgo.Client, p int32, recs []*kgo.Record) int64 {
	epochs := make(map[int64]int32, len(recs))
	r.mu.Lock()
	msgs := r.pending[p]
	delete(r.pending, p)
	r.mu.Unlock()
	for _, rec := range recs {
		if n := len(msgs); n > 0 && rec.Offset <= msgs[n-1].Offset {
			continue // already held from an earlier poll
		}
		msgs = append(msgs, toMessage(rec))
		epochs[rec.Offset] = rec.LeaderEpoch
	}

	for {
		first, last := msgs[0].Offset, msgs[len(msgs)-1].Offset
		res, err := ProcessBatch(ctx, r.d, Batch{TopicID: r.topicID, Topic: r.topic, Partition: p, Messages: msgs})
		var incomplete *IncompleteBatchError
		var rewind *RewindError
		switch {
		case errors.As(err, &incomplete):
			// An earlier attempt planned a longer range: keep the records and
			// wait for the next poll to bring the rest.
			r.mu.Lock()
			r.pending[p] = msgs
			r.mu.Unlock()
			return first
		case errors.As(err, &rewind):
			r.d.Log.Warn("rewinding to the start of a planned batch", "stage", "transform", "partition", p, "offset", rewind.Offset)
			r.seek(cl, p, rewind.Offset)
			return first
		case errors.Is(err, s3.ErrChecksumMismatch), errors.Is(err, ErrNonDeterministic):
			r.halt(cl, p, first, last, err)
			return first
		case err != nil:
			if ctx.Err() != nil {
				return first
			}
			r.d.Log.Error("batch failed, will retry", "stage", "transform", "partition", p,
				"batch", fmt.Sprintf("%d-%d", first, last), "err", err)
			r.seek(cl, p, first) // fetch the same records again
			select {
			case <-ctx.Done():
			case <-time.After(retryBackoff):
			}
			return first
		}

		epoch, ok := epochs[res.LastOffset]
		if !ok {
			epoch = -1
		}
		if !r.commit(ctx, cl, p, res.LastOffset+1, epoch) {
			// Stop working on the partition: it is moving to another member, or
			// the coordinator failed. Should it stay ours, the next poll must
			// resume right after the written batch, not after the last record
			// polled, or the records in between would be skipped.
			if res.LastOffset != last {
				r.seek(cl, p, res.LastOffset+1)
			}
			return first
		}
		if res.LastOffset >= last {
			if res.LastOffset > last {
				r.seek(cl, p, res.LastOffset+1) // a written batch covered more than we polled
			}
			return res.LastOffset + 1
		}
		msgs = msgs[len(upTo(msgs, res.LastOffset)):]
	}
}

// halt stops partition p in place after the batch [first..last] met an object
// with different bytes or re-planned to different objects (spec §7.4 "alert
// and stop"). Fetching of p is paused and nothing is committed, so the batch
// stays where it is and the lag keeps counting from first; Run carries on
// with the other partitions and /metrics keeps serving. The cause already
// counted etl_s3_put_total{result="mismatch"}, which fires
// ETLObjectChecksumMismatch and, since the process keeps running, keeps it
// firing. A human resolves the conflict and restarts the transformer.
func (r *runner) halt(cl *kgo.Client, p int32, first, last int64, err error) {
	cl.PauseFetchPartitions(map[string][]int32{r.topic: {p}})
	r.mu.Lock()
	delete(r.pending, p)
	r.halted[p] = first
	r.mu.Unlock()
	r.d.Log.Error("partition halted: the batch would overwrite different bytes; resolve the conflict and restart the transformer",
		"stage", "transform", "partition", p, "batch", fmt.Sprintf("%d-%d", first, last), "err", err)
}

// refreshHaltedLag sets the lag of halted partitions from the topic's end
// offsets, at most every haltedLagEvery: they are no longer fetched, so no
// fetch reports their high watermark. It runs before AllowRebalance, while no
// partition can be revoked.
func (r *runner) refreshHaltedLag(ctx context.Context, cl *kgo.Client) {
	r.mu.Lock()
	halted := maps.Clone(r.halted)
	r.mu.Unlock()
	if len(halted) == 0 || time.Since(r.lagRefreshed) < haltedLagEvery {
		return
	}
	r.lagRefreshed = time.Now()
	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(cl).ListEndOffsets(lctx, r.topic)
	if err != nil {
		if ctx.Err() == nil {
			r.d.Log.Warn("end offsets of halted partitions", "stage", "transform", "err", err)
		}
		return
	}
	for p, first := range halted {
		if o, ok := ends.Lookup(r.topic, p); ok && o.Err == nil {
			r.d.Metrics.ConsumerLag.WithLabelValues(strconv.Itoa(int(p))).Set(float64(max(o.Offset-first, 0)))
		}
	}
}

// resumeLost resumes fetching of halted partitions this member no longer
// owns: a halt belongs to the owner. The new owner, or this member if the
// partition comes back, reads from the committed offset, meets the same
// batch and halts it again. It runs in the poll loop, not in a group callback.
func (r *runner) resumeLost(cl *kgo.Client) {
	r.mu.Lock()
	ps := r.unhalt
	r.unhalt = nil
	r.mu.Unlock()
	if len(ps) > 0 {
		cl.ResumeFetchPartitions(map[string][]int32{r.topic: ps})
	}
}

func toMessage(rec *kgo.Record) Message {
	h := make(map[string]string, len(rec.Headers))
	for _, kv := range rec.Headers {
		h[kv.Key] = string(kv.Value)
	}
	return Message{Partition: rec.Partition, Offset: rec.Offset, Value: rec.Value, Headers: h}
}

// seek moves the fetch position of one partition. Only called between
// PollRecords and AllowRebalance, so the partition cannot be revoked meanwhile.
func (r *runner) seek(cl *kgo.Client, p int32, offset int64) {
	r.mu.Lock()
	delete(r.pending, p)
	r.mu.Unlock()
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{r.topic: {p: {Epoch: -1, Offset: offset}}})
}

// commit synchronously commits next for partition p. A rejected commit means
// this member no longer owns the partition (spec §7.4 step 5).
func (r *runner) commit(ctx context.Context, cl *kgo.Client, p int32, next int64, epoch int32) bool {
	var commitErr error
	cl.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{r.topic: {p: {Epoch: epoch, Offset: next}}},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, tp := range t.Partitions {
					if err := kerr.ErrorForCode(tp.ErrorCode); err != nil {
						commitErr = err
					}
				}
			}
		})
	if commitErr != nil {
		if ctx.Err() == nil {
			r.d.Log.Warn("offset commit rejected", "stage", "transform", "partition", p, "offset", next, "err", commitErr)
		}
		return false
	}
	return true
}

// adjustOffsets runs after the group fetched committed offsets for newly
// assigned partitions and before fetching starts (franz-go AdjustFetchOffsetsFn).
// It applies the assignment rules of spec §7.4: a written manifest batch that
// covers the committed offset is skipped (and committed later in the poll
// loop); a planned one is reprocessed from its exact start.
func (r *runner) adjustOffsets(ctx context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	for p, o := range offsets[r.topic] {
		committed := o.EpochOffset().Offset
		if committed < 0 {
			continue // nothing committed yet: ConsumeResetOffset applies
		}
		first, last, status, found, err := CoveringBatch(ctx, r.d.Pool, r.topicID, p, committed)
		if err != nil {
			// Not fatal: ProcessBatch applies the same rules from the manifest.
			r.d.Log.Warn("assignment recovery lookup failed", "stage", "transform", "partition", p, "err", err)
			continue
		}
		switch {
		case found && status == statusWritten:
			offsets[r.topic][p] = kgo.NewOffset().At(last + 1)
			r.mu.Lock()
			r.recovered[p] = last + 1
			r.mu.Unlock()
			r.d.Log.Info("skipping written batch on assignment", "stage", "transform", "partition", p, "batch", fmt.Sprintf("%d-%d", first, last))
		case found && first != committed:
			offsets[r.topic][p] = kgo.NewOffset().At(first)
			r.d.Log.Info("reprocessing planned batch on assignment", "stage", "transform", "partition", p, "batch", fmt.Sprintf("%d-%d", first, last))
		}
	}
	return offsets, nil
}

// commitRecovered commits the offsets moved past written batches on
// assignment. It runs in the poll loop, never in a group callback: committing
// from inside a callback can deadlock with the join.
func (r *runner) commitRecovered(ctx context.Context, cl *kgo.Client) {
	r.mu.Lock()
	rec := r.recovered
	r.recovered = map[int32]int64{}
	r.mu.Unlock()
	for p, next := range rec {
		r.commit(ctx, cl, p, next, -1)
	}
}

// forget drops the local state and the lag series of revoked or lost
// partitions; another member reports their lag now. Offsets are not committed
// here: every written batch was committed right after it was written. A
// halted partition is queued for resumeLost.
func (r *runner) forget(_ context.Context, _ *kgo.Client, gone map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range gone[r.topic] {
		delete(r.pending, p)
		delete(r.recovered, p)
		if _, ok := r.halted[p]; ok {
			delete(r.halted, p)
			r.unhalt = append(r.unhalt, p)
		}
		r.d.Metrics.ConsumerLag.DeleteLabelValues(strconv.Itoa(int(p)))
	}
}
