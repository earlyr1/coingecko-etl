package coingecko

import (
	"context"
	"errors"
	"testing"
	"time"
)

// wait blocks until a request may start and records that start, through the
// same acquire/release path the client uses around every request.
func wait(l *Limiter, ctx context.Context) error {
	if err := l.acquire(ctx); err != nil {
		return err
	}
	l.release()
	return nil
}

func TestLimiterFirstWaitIsImmediate(t *testing.T) {
	l := NewLimiter(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := wait(l, ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("first Wait took %v, want immediate", d)
	}
}

func TestLimiterEnforcesSpacing(t *testing.T) {
	const spacing = 150 * time.Millisecond
	l := NewLimiter(spacing)
	ctx := context.Background()
	if err := wait(l, ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := wait(l, ctx); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < spacing-10*time.Millisecond {
		t.Fatalf("second Wait returned after %v, want >= %v", d, spacing)
	}
	// Spacing does not make Allowed false: ticks are only skipped for
	// in-flight requests and cooldowns.
	if !l.Allowed(time.Now()) {
		t.Fatal("Allowed = false right after a start; spacing must not skip ticks")
	}
}

func TestLimiterCooldown(t *testing.T) {
	l := NewLimiter(0)
	l.Cooldown(200 * time.Millisecond)
	now := time.Now()
	if l.Allowed(now) {
		t.Fatal("Allowed during cooldown")
	}
	if !l.Allowed(now.Add(250 * time.Millisecond)) {
		t.Fatal("not Allowed after cooldown")
	}
	// A shorter cooldown must not shorten the active one.
	l.Cooldown(time.Millisecond)
	if l.Allowed(time.Now().Add(50 * time.Millisecond)) {
		t.Fatal("shorter Cooldown shortened the active cooldown")
	}
	start := time.Now()
	if err := wait(l, context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("Wait returned after %v during a 200ms cooldown", d)
	}
}

func TestLimiterWaitHonorsContext(t *testing.T) {
	l := NewLimiter(0)
	l.Cooldown(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := wait(l, ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err = %v, want context.DeadlineExceeded", err)
	}
}

func TestLimiterSingleFlight(t *testing.T) {
	l := NewLimiter(0)
	ctx := context.Background()
	if err := l.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Allowed(time.Now()) {
		t.Fatal("Allowed while a request is in flight")
	}
	got := make(chan time.Time, 1)
	go func() {
		if err := l.acquire(ctx); err == nil {
			got <- time.Now()
			l.release()
		}
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("second acquire succeeded while the first request was in flight")
	default:
	}
	released := time.Now()
	l.release()
	select {
	case at := <-got:
		if at.Before(released) {
			t.Fatal("second acquire finished before release")
		}
	case <-time.After(time.Second):
		t.Fatal("second acquire did not proceed after release")
	}
}
