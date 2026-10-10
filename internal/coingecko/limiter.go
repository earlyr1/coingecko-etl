package coingecko

import (
	"context"
	"sync"
	"time"
)

// Limiter enforces the CoinGecko request budget for one process:
//   - at most one request in flight,
//   - at least minSpacing between two request starts,
//   - a cooldown (set after HTTP 429) during which no request may start.
//
// Allowed is the non-blocking check the fetcher uses to SKIP a tick: it
// reports false while a request is in flight or a cooldown is active.
// Spacing is deliberately not part of Allowed: two schedules that are
// minSpacing apart would otherwise lose ticks to scheduler jitter, so Wait
// absorbs spacing by sleeping (at most minSpacing).
type Limiter struct {
	mu            sync.Mutex
	minSpacing    time.Duration
	now           func() time.Time
	inFlight      bool
	lastStart     time.Time
	cooldownUntil time.Time
	changed       chan struct{} // closed and replaced on every state change
}

// NewLimiter returns a limiter that keeps request starts at least minSpacing apart.
func NewLimiter(minSpacing time.Duration) *Limiter {
	return &Limiter{minSpacing: minSpacing, now: time.Now, changed: make(chan struct{})}
}

// Wait blocks until a request may start and records that start (spacing is
// measured from it). It does not keep the in-flight slot. It returns
// ctx.Err() if ctx ends first.
func (l *Limiter) Wait(ctx context.Context) error {
	if err := l.acquire(ctx); err != nil {
		return err
	}
	l.release()
	return nil
}

// Cooldown forbids any request start before now+d. A shorter cooldown never
// shortens one that is already active. d comes from the client: a 429's
// Retry-After capped at 15 minutes (60 s without one), or a backoff of at
// most 60 s.
func (l *Limiter) Cooldown(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if until := l.now().Add(d); until.After(l.cooldownUntil) {
		l.cooldownUntil = until
	}
	l.broadcastLocked()
}

// Allowed reports whether a tick at now should go ahead: no request in
// flight and no active cooldown. It never blocks.
func (l *Limiter) Allowed(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.inFlight && !now.Before(l.cooldownUntil)
}

// acquire blocks until no request is in flight and both spacing and
// cooldown have passed, then takes the in-flight slot. The caller must call
// release when the request is finished.
func (l *Limiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		var delay time.Duration = -1 // -1: wait for a state change only
		if !l.inFlight {
			now := l.now()
			ready := l.lastStart.Add(l.minSpacing)
			if l.cooldownUntil.After(ready) {
				ready = l.cooldownUntil
			}
			if !now.Before(ready) {
				l.inFlight = true
				l.lastStart = now
				l.mu.Unlock()
				return nil
			}
			delay = ready.Sub(now)
		}
		changed := l.changed
		l.mu.Unlock()

		var timerC <-chan time.Time
		var timer *time.Timer
		if delay >= 0 {
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		case <-changed:
		case <-timerC:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// release frees the in-flight slot taken by acquire.
func (l *Limiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inFlight = false
	l.broadcastLocked()
}

func (l *Limiter) broadcastLocked() {
	close(l.changed)
	l.changed = make(chan struct{})
}
