package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var (
	errDown     = errors.New("connection refused")
	errBusiness = errors.New("insufficient funds")
)

type recorder struct {
	mu       sync.Mutex
	states   []int
	rejected int
}

func (r *recorder) BreakerState(_ string, s int) {
	r.mu.Lock()
	r.states = append(r.states, s)
	r.mu.Unlock()
}

func (r *recorder) BreakerRejected(string) {
	r.mu.Lock()
	r.rejected++
	r.mu.Unlock()
}

func newTestBreaker(obs Observer) *Breaker {
	return New("postgres", Settings{
		FailureThreshold: 3,
		OpenTimeout:      100 * time.Millisecond,
		IsFailure:        func(err error) bool { return errors.Is(err, errDown) },
	}, obs, nil)
}

func TestOpensAfterConsecutiveTransientFailures(t *testing.T) {
	rec := &recorder{}
	b := newTestBreaker(rec)
	calls := 0
	fail := func() error { calls++; return errDown }
	for i := 0; i < 3; i++ {
		if err := b.Do(fail); !errors.Is(err, errDown) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if b.State() != Open || b.Allow() {
		t.Fatalf("state %s after threshold", b.State())
	}
	err := b.Do(fail)
	var open *OpenError
	if !errors.As(err, &open) || !errors.Is(err, ErrOpen) || calls != 3 {
		t.Fatalf("open breaker must fail fast without calling: err=%v calls=%d", err, calls)
	}
	if RetryAfterSeconds(err) != 1 {
		t.Fatalf("retry after %d", RetryAfterSeconds(err))
	}
	if rec.rejected != 1 || rec.states[len(rec.states)-1] != int(Open) {
		t.Fatalf("observer: %+v", rec)
	}
}

func TestBusinessErrorsAndCancellationDoNotTrip(t *testing.T) {
	b := newTestBreaker(nil)
	for i := 0; i < 10; i++ {
		_ = b.Do(func() error { return errBusiness })
		_ = b.Do(func() error { return context.Canceled })
	}
	if b.State() != Closed {
		t.Fatalf("non-dependency errors opened the breaker: %s", b.State())
	}
	// A business error between transient failures proves the dependency
	// answered, so the consecutive count restarts.
	_ = b.Do(func() error { return errDown })
	_ = b.Do(func() error { return errDown })
	_ = b.Do(func() error { return errBusiness })
	_ = b.Do(func() error { return errDown })
	if b.State() != Closed {
		t.Fatal("failures were not consecutive")
	}
}

func TestHalfOpenProbeClosesOrReopens(t *testing.T) {
	b := newTestBreaker(nil)
	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return errDown })
	}
	time.Sleep(120 * time.Millisecond)
	if b.State() != HalfOpen {
		t.Fatalf("state %s after open timeout", b.State())
	}
	_ = b.Do(func() error { return errDown }) // probe fails -> open again
	if b.State() != Open {
		t.Fatalf("failed probe must reopen: %s", b.State())
	}
	time.Sleep(120 * time.Millisecond)
	if err := b.Do(func() error { return nil }); err != nil || b.State() != Closed {
		t.Fatalf("successful probe must close: %v %s", err, b.State())
	}
}

func TestHalfOpenAllowsSingleProbe(t *testing.T) {
	b := newTestBreaker(nil)
	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return errDown })
	}
	time.Sleep(120 * time.Millisecond)
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = b.Do(func() error { close(started); <-release; return nil })
	}()
	<-started
	if err := b.Do(func() error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("second call during the probe must be rejected: %v", err)
	}
	close(release)
}

func TestGateWaitsAndProbesWithoutWork(t *testing.T) {
	b := newTestBreaker(nil)
	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return errDown })
	}
	var probes int
	var mu sync.Mutex
	healthy := false
	g := NewGate(b, func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		probes++
		if healthy {
			return nil
		}
		return errDown
	})
	g.poll = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := g.Ready(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gate must block while the dependency is down: %v", err)
	}
	mu.Lock()
	healthy = true
	mu.Unlock()
	if err := g.Ready(context.Background()); err != nil || b.State() != Closed {
		t.Fatalf("gate must open once the probe succeeds: %v %s", err, b.State())
	}
	if probes < 2 {
		t.Fatalf("expected probes while waiting, got %d", probes)
	}
}

func TestNilBreakerIsPassThrough(t *testing.T) {
	var b *Breaker
	if err := b.Do(func() error { return errDown }); !errors.Is(err, errDown) || b.State() != Closed || !b.Allow() {
		t.Fatal("nil breaker must pass calls through")
	}
	var g *Gate
	if err := g.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}
