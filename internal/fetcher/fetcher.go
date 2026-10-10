// Package fetcher is the `etl fetcher` service: it polls CoinGecko on two
// schedules and inserts one raw.api_calls row per call, failed calls
// included. It is pure transport: only the S1 envelope is checked here; the
// payload is validated against S0 by the transformer.
package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"coingecko-etl/internal/coingecko"
	"coingecko-etl/internal/config"
	"coingecko-etl/internal/event"
	"coingecko-etl/internal/obs"
	"coingecko-etl/internal/storage/pg"
	"coingecko-etl/internal/validation"
	"coingecko-etl/internal/xtime"
	"coingecko-etl/schemas"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	stage              = "fetch"
	schemaLabel        = "raw_event.v1"
	partitionEvery     = time.Hour
	partitionDaysAhead = 3
	// A tick that is more than lateTolerance overdue came due while a
	// request was in flight; it is skipped, not run late.
	lateTolerance = time.Second
	statusSkipped = "skipped"
)

// clock abstracts time so the scheduler can be tested with virtual time.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// job is one fixed-rate schedule. Slots are start, start+every, ...
type job struct {
	endpoint string // "" for maintenance jobs (never counted as skipped)
	every    time.Duration
	next     time.Time
	run      func(ctx context.Context, now time.Time)
}

type runner struct {
	cfg              config.Config
	clk              clock
	fetch            func(ctx context.Context, endpoint string, req event.Request) event.RawEvent
	allowed          func(now time.Time) bool
	insert           func(ctx context.Context, ev event.RawEvent) error
	ensurePartitions func(ctx context.Context, now time.Time) error
	reg              *validation.Registry
	m                *obs.Metrics
	log              *slog.Logger
	detailIdx        int
}

// Run polls until ctx is cancelled, then returns nil. It returns an error
// only for startup failures (schemas, invalid intervals, partitions).
//
// lim must be the limiter cg was built with: Run asks lim.Allowed whether a
// tick may fire and then calls cg.Fetch directly, which enforces spacing
// and single flight itself.
func Run(ctx context.Context, cfg config.Config, cg *coingecko.Client, lim *coingecko.Limiter, pool *pgxpool.Pool, m *obs.Metrics, log *slog.Logger) error {
	reg, err := validation.Load(schemas.FS)
	if err != nil {
		return fmt.Errorf("fetcher: load schemas: %w", err)
	}
	r := &runner{
		cfg:     cfg,
		clk:     realClock{},
		fetch:   cg.Fetch,
		allowed: lim.Allowed,
		insert: func(ctx context.Context, ev event.RawEvent) error {
			return pg.InsertAPICall(ctx, pool, ev)
		},
		ensurePartitions: func(ctx context.Context, now time.Time) error {
			return pg.EnsurePartitions(ctx, pool, now, partitionDaysAhead)
		},
		reg: reg,
		m:   m,
		log: log.With("stage", stage),
	}
	return r.run(ctx)
}

func (r *runner) run(ctx context.Context) error {
	if r.cfg.MarketsInterval <= 0 || r.cfg.DetailInterval <= 0 {
		return errors.New("fetcher: MarketsInterval and DetailInterval must be > 0")
	}
	start := r.clk.Now()
	if err := r.ensurePartitions(ctx, start); err != nil {
		return fmt.Errorf("fetcher: ensure partitions: %w", err)
	}
	jobs := []*job{
		{endpoint: event.EndpointMarkets, every: r.cfg.MarketsInterval, next: start, run: r.tickMarkets},
		{endpoint: "", every: partitionEvery, next: start.Add(partitionEvery), run: r.tickPartitions},
	}
	if len(r.cfg.Watchlist) > 0 {
		jobs = append(jobs, &job{endpoint: event.EndpointCoinDetail, every: r.cfg.DetailInterval,
			next: start.Add(r.cfg.DetailOffset), run: r.tickDetail})
	} else {
		r.log.Warn("watchlist is empty; coin detail polling disabled")
	}
	r.log.Info("fetcher started",
		"markets_interval", r.cfg.MarketsInterval.String(),
		"detail_interval", r.cfg.DetailInterval.String(),
		"detail_offset", r.cfg.DetailOffset.String(),
		"watchlist", r.cfg.Watchlist)

	for {
		for _, j := range jobs {
			now := r.clk.Now() // re-read: the previous job may have taken a while
			if now.Before(j.next) {
				continue
			}
			// Slots due so far: next, next+every, ... <= now.
			due := int(now.Sub(j.next)/j.every) + 1
			last := j.next.Add(time.Duration(due-1) * j.every)
			j.next = last.Add(j.every)
			skipped := due - 1
			runIt := now.Sub(last) <= lateTolerance || j.endpoint == ""
			if !runIt {
				skipped = due
			}
			if skipped > 0 && j.endpoint != "" {
				r.skip(j.endpoint, skipped, "in_flight")
			}
			if runIt {
				j.run(ctx, now)
			}
			if ctx.Err() != nil {
				r.log.Info("fetcher stopped")
				return nil
			}
		}
		earliest := jobs[0].next
		for _, j := range jobs[1:] {
			if j.next.Before(earliest) {
				earliest = j.next
			}
		}
		wait := max(earliest.Sub(r.clk.Now()), 0)
		select {
		case <-ctx.Done():
			r.log.Info("fetcher stopped")
			return nil
		case <-r.clk.After(wait):
		}
	}
}

func (r *runner) tickMarkets(ctx context.Context, now time.Time) {
	if !r.allowed(now) {
		r.skip(event.EndpointMarkets, 1, "cooldown")
		return
	}
	r.call(ctx, event.EndpointMarkets, coingecko.MarketsRequest(r.cfg.MarketsPerPage))
}

// tickDetail fetches the next coin of the watchlist (round-robin). A
// skipped tick does not advance the rotation.
func (r *runner) tickDetail(ctx context.Context, now time.Time) {
	if !r.allowed(now) {
		r.skip(event.EndpointCoinDetail, 1, "cooldown")
		return
	}
	coin := r.cfg.Watchlist[r.detailIdx%len(r.cfg.Watchlist)]
	r.detailIdx++
	r.call(ctx, event.EndpointCoinDetail, coingecko.CoinDetailRequest(coin))
}

func (r *runner) tickPartitions(ctx context.Context, now time.Time) {
	if err := r.ensurePartitions(ctx, now); err != nil && ctx.Err() == nil {
		r.log.Error("ensure partitions failed", "err", err)
	}
}

func (r *runner) skip(endpoint string, n int, reason string) {
	r.m.APIRequests.WithLabelValues(endpoint, statusSkipped).Add(float64(n))
	r.log.Debug("tick skipped", "endpoint", endpoint, "reason", reason, "ticks", n)
}

// call performs one fetch, records metrics and logs, validates the envelope
// against S1 and inserts the row.
func (r *runner) call(ctx context.Context, endpoint string, req event.Request) {
	ev := r.fetch(ctx, endpoint, req)
	if ctx.Err() != nil {
		return // shutting down: the request was cut short by us, not by CoinGecko
	}

	status := ev.Outcome
	attrs := []any{"event_id", ev.EventID, "endpoint", endpoint, "path", req.Path, "outcome", ev.Outcome}
	if ev.HTTP != nil {
		status = strconv.Itoa(ev.HTTP.Status)
		r.m.APIRequestDuration.WithLabelValues(endpoint).Observe(float64(ev.HTTP.LatencyMS) / 1000)
		attrs = append(attrs, "status", ev.HTTP.Status, "latency_ms", ev.HTTP.LatencyMS)
		if ev.HTTP.CFCacheStatus != nil {
			attrs = append(attrs, "cf_cache_status", *ev.HTTP.CFCacheStatus)
		}
		if ev.HTTP.Status == 429 {
			r.m.APIRateLimited.Inc()
			if ev.HTTP.RetryAfterS != nil {
				attrs = append(attrs, "retry_after_s", *ev.HTTP.RetryAfterS)
			}
		}
	}
	if ev.Error != nil {
		attrs = append(attrs, "error", *ev.Error)
	}
	r.m.APIRequests.WithLabelValues(endpoint, status).Inc()
	r.log.Info("api call", attrs...)

	if !r.validEnvelope(ev) {
		return
	}
	if err := r.insert(ctx, ev); err != nil {
		r.log.Error("insert api call failed", "event_id", ev.EventID, "endpoint", endpoint, "err", err)
		return
	}
	r.log.Debug("api call stored", "event_id", ev.EventID, "endpoint", endpoint)
	if ev.Outcome != event.OutcomeOK {
		return
	}
	fetchedAt, err := xtime.Parse(ev.FetchedAt)
	if err != nil {
		return
	}
	r.m.LastSuccess.WithLabelValues(endpoint).Set(float64(fetchedAt.UnixMilli()) / 1000)
	if updated, ok := sourceLastUpdated(endpoint, ev.Payload); ok {
		r.m.SourceDataAge.WithLabelValues(endpoint).Set(fetchedAt.Sub(updated).Seconds())
	}
}

// validEnvelope checks the event against S1. A violation is a bug in our
// code (the fetcher builds the envelope), so it is logged at ERROR and the
// row is not inserted.
func (r *runner) validEnvelope(ev event.RawEvent) bool {
	b, err := event.Marshal(ev)
	var vs []validation.Violation
	if err == nil {
		vs, err = r.reg.Validate(validation.IDRawEvent, b)
	}
	if err != nil || len(vs) > 0 {
		r.m.SchemaValidations.WithLabelValues(stage, schemaLabel, "fail").Inc()
		for _, v := range vs {
			r.m.SchemaViolations.WithLabelValues(stage, schemaLabel, validation.Keyword(v)).Inc()
		}
		r.log.Error("raw event violates S1; not inserted (bug)",
			"event_id", ev.EventID, "endpoint", ev.Endpoint, "err", err, "violations", vs)
		return false
	}
	r.m.SchemaValidations.WithLabelValues(stage, schemaLabel, "pass").Inc()
	return true
}

// sourceLastUpdated returns when CoinGecko computed the data: the max
// last_updated over a markets array, or the top-level last_updated of a
// /coins/{id} document. Only that one field is decoded.
func sourceLastUpdated(endpoint string, payload []byte) (time.Time, bool) {
	var stamps []*string
	switch endpoint {
	case event.EndpointMarkets:
		var rows []struct {
			LastUpdated *string `json:"last_updated"`
		}
		if json.Unmarshal(payload, &rows) != nil {
			return time.Time{}, false
		}
		for _, row := range rows {
			stamps = append(stamps, row.LastUpdated)
		}
	case event.EndpointCoinDetail:
		var doc struct {
			LastUpdated *string `json:"last_updated"`
		}
		if json.Unmarshal(payload, &doc) != nil {
			return time.Time{}, false
		}
		stamps = append(stamps, doc.LastUpdated)
	}
	var best time.Time
	for _, s := range stamps {
		if s == nil {
			continue
		}
		if t, err := xtime.Parse(*s); err == nil && t.After(best) {
			best = t
		}
	}
	return best, !best.IsZero()
}
