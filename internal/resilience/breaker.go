// Package resilience provides a circuit breaker per external dependency
// (PostgreSQL pools, SQS). When a dependency keeps failing with transient
// errors the breaker opens: callers fail fast instead of piling up on
// timeouts, and background consumers pause instead of burning retries.
// After OpenTimeout a single probe is allowed (half-open); success closes it.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/sony/gobreaker/v2"
)

// ErrOpen is matched by every *OpenError.
var ErrOpen = errors.New("circuit breaker open")

// OpenError is returned without calling the dependency while the breaker is
// open (or while its single half-open probe is in flight).
type OpenError struct {
	Name       string
	RetryAfter time.Duration
}

func (e *OpenError) Error() string {
	return fmt.Sprintf("circuit breaker %q open, retry after %s", e.Name, e.RetryAfter)
}

func (e *OpenError) Is(target error) bool { return target == ErrOpen }

// State of a breaker. The numeric values are exported as a metric.
type State int

const (
	Closed   State = 0
	HalfOpen State = 1
	Open     State = 2
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	}
	return "open"
}

// Settings configure a breaker.
type Settings struct {
	// FailureThreshold consecutive transient failures open the breaker.
	FailureThreshold uint32
	// OpenTimeout is how long it stays open before allowing one probe.
	OpenTimeout time.Duration
	// IsFailure tells whether err means the dependency is unhealthy
	// (connection refused, timeout...). Business and validation errors are
	// successes: they prove the dependency answered.
	IsFailure func(err error) bool
}

// Observer receives state changes and rejections (metrics, logs).
type Observer interface {
	BreakerState(name string, state int)
	BreakerRejected(name string)
}

// Breaker guards one dependency. A nil *Breaker is valid and never trips,
// which keeps tests and tools that do not need it simple.
type Breaker struct {
	name     string
	settings Settings
	cb       *gobreaker.CircuitBreaker[struct{}]
	obs      Observer
}

func New(name string, s Settings, obs Observer, log *slog.Logger) *Breaker {
	if s.FailureThreshold == 0 {
		s.FailureThreshold = 5
	}
	if s.OpenTimeout <= 0 {
		s.OpenTimeout = 5 * time.Second
	}
	if s.IsFailure == nil {
		s.IsFailure = func(err error) bool { return err != nil }
	}
	b := &Breaker{name: name, settings: s, obs: obs}
	b.cb = gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:        name,
		MaxRequests: 1, // a single probe in half-open
		Interval:    0, // closed-state counts reset on success, not by time
		Timeout:     s.OpenTimeout,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= s.FailureThreshold },
		IsSuccessful: func(err error) bool {
			return err == nil || !s.IsFailure(err)
		},
		// A caller that gave up (client disconnected, shutdown) says nothing
		// about the dependency's health.
		IsExcluded: func(err error) bool { return errors.Is(err, context.Canceled) },
		OnStateChange: func(name string, from, to gobreaker.State) {
			st := convert(to)
			if log != nil {
				level := slog.LevelWarn
				if st == Closed {
					level = slog.LevelInfo
				}
				log.Log(context.Background(), level, "circuit breaker state changed",
					slog.String("breaker", name), slog.String("from", convert(from).String()), slog.String("to", st.String()))
			}
			if obs != nil {
				obs.BreakerState(name, int(st))
			}
		},
	})
	if obs != nil {
		obs.BreakerState(name, int(Closed))
	}
	return b
}

func convert(s gobreaker.State) State {
	switch s {
	case gobreaker.StateClosed:
		return Closed
	case gobreaker.StateHalfOpen:
		return HalfOpen
	}
	return Open
}

func (b *Breaker) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// State returns the current state (moving open -> half-open once the open
// timeout elapsed).
func (b *Breaker) State() State {
	if b == nil {
		return Closed
	}
	return convert(b.cb.State())
}

// Allow reports whether calls may go through (closed or half-open).
func (b *Breaker) Allow() bool { return b.State() != Open }

// Do runs fn through the breaker. While open it returns *OpenError without
// calling fn.
func (b *Breaker) Do(fn func() error) error {
	if b == nil {
		return fn()
	}
	_, err := b.cb.Execute(func() (struct{}, error) { return struct{}{}, fn() })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		if b.obs != nil {
			b.obs.BreakerRejected(b.name)
		}
		return &OpenError{Name: b.name, RetryAfter: b.settings.OpenTimeout}
	}
	return err
}

// RetryAfterSeconds is the Retry-After hint for an error, or 0.
func RetryAfterSeconds(err error) int {
	var open *OpenError
	if errors.As(err, &open) {
		return int(math.Ceil(open.RetryAfter.Seconds()))
	}
	return 0
}

// Gate blocks background consumers while a dependency is unhealthy. The
// half-open probe is the cheap health check (a ping), never a unit of work,
// so no message or event spends an attempt while the dependency is down.
type Gate struct {
	breaker *Breaker
	probe   func(ctx context.Context) error
	poll    time.Duration
}

func NewGate(b *Breaker, probe func(ctx context.Context) error) *Gate {
	return &Gate{breaker: b, probe: probe, poll: 250 * time.Millisecond}
}

// Ready returns nil once the breaker is closed, running the probe when it is
// half-open. It returns ctx.Err() if ctx ends first.
func (g *Gate) Ready(ctx context.Context) error {
	if g == nil || g.breaker == nil {
		return nil
	}
	for {
		switch g.breaker.State() {
		case Closed:
			return nil
		case HalfOpen:
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = g.breaker.Do(func() error { return g.probe(pctx) })
			cancel()
			if g.breaker.State() == Closed {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.poll):
		}
	}
}

// Healthy reports whether the breaker is closed (no wait needed).
func (g *Gate) Healthy() bool { return g == nil || g.breaker.State() == Closed }

// Name of the guarded dependency.
func (g *Gate) Name() string {
	if g == nil {
		return ""
	}
	return g.breaker.Name()
}

// WaitAll waits for every gate.
func WaitAll(ctx context.Context, gates ...*Gate) error {
	for _, g := range gates {
		if err := g.Ready(ctx); err != nil {
			return err
		}
	}
	return nil
}
