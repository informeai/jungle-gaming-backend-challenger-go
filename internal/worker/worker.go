// Package worker contains the background loops (outbox publisher and
// pending-reference resolver) and a small runner with observable termination.
package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Loop runs tick every interval until stopped. Start/Stop map directly to
// fx.Lifecycle hooks; Stop cancels the loop context, waits for the current
// tick to finish (bounded by the stop context) and reports termination.
type Loop struct {
	name     string
	interval time.Duration
	tick     func(ctx context.Context)
	log      *slog.Logger

	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool
}

func NewLoop(name string, interval time.Duration, log *slog.Logger, tick func(ctx context.Context)) *Loop {
	return &Loop{name: name, interval: interval, tick: tick, log: log.With(slog.String("component", name))}
}

func (l *Loop) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	l.running.Store(true)
	go func() {
		defer func() { l.running.Store(false); close(l.done) }()
		t := time.NewTicker(l.interval)
		defer t.Stop()
		for {
			l.tick(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	l.log.Info("worker started")
}

func (l *Loop) Stop(ctx context.Context) error {
	if l.cancel == nil {
		return nil
	}
	l.cancel()
	select {
	case <-l.done:
		l.log.Info("worker stopped")
		return nil
	case <-ctx.Done():
		l.log.Error("worker did not stop in time")
		return ctx.Err()
	}
}

// Running reports whether the goroutine is alive (used by tests).
func (l *Loop) Running() bool { return l.running.Load() }
