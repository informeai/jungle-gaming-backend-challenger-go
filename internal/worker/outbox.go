package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/infra/postgres"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
)

// OutboxStore is the persistence used by the publisher.
type OutboxStore interface {
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]postgres.OutboxMessage, error)
	MarkPublished(ctx context.Context, id uuid.UUID, owner string) (bool, error)
	MarkFailed(ctx context.Context, id uuid.UUID, owner string, next time.Time, cause string) error
	Lag(ctx context.Context) (int64, time.Duration, error)
}

type Publisher interface {
	Publish(ctx context.Context, m postgres.OutboxMessage) error
}

type OutboxMetrics interface {
	OutboxPublished(occurredAt time.Time)
	OutboxFailure()
	OutboxBacklog(pending int64, lag time.Duration)
}

// OutboxRelay publishes committed outbox events. Any number of relays may run:
// Claim uses FOR UPDATE SKIP LOCKED plus a lease (locked_until), so each event
// has one owner at a time and a crashed owner's events are reclaimed once the
// lease expires. Publication is at-least-once with a stable eventId.
type OutboxRelay struct {
	store   OutboxStore
	pub     Publisher
	owner   string
	cfg     config.Outbox
	metrics OutboxMetrics
	log     *slog.Logger

	// AfterPublish, when set, runs after the broker accepted the event and
	// before the outbox row is confirmed. Fault-injection hook.
	AfterPublish func(eventID uuid.UUID)

	// Gates pause claiming while the relay's database or SQS breaker is open,
	// so events are not leased (and their attempts spent) only to fail.
	Gates []*resilience.Gate

	lastLag time.Time
}

func NewOutboxRelay(store OutboxStore, pub Publisher, owner string, cfg config.Outbox, metrics OutboxMetrics, log *slog.Logger) *OutboxRelay {
	return &OutboxRelay{store: store, pub: pub, owner: owner, cfg: cfg, metrics: metrics, log: log.With(slog.String("component", "outbox-relay"))}
}

// Backoff for the given attempt number (1-based), capped at MaxBackoff.
func (r *OutboxRelay) Backoff(attempt int) time.Duration {
	d := r.cfg.BaseBackoff
	for i := 1; i < attempt && d < r.cfg.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, r.cfg.MaxBackoff)
}

// Tick claims and publishes one batch. It returns the number published.
func (r *OutboxRelay) Tick(ctx context.Context) int {
	if err := resilience.WaitAll(ctx, r.Gates...); err != nil {
		return 0 // stopping
	}
	r.reportLag(ctx)
	msgs, err := r.store.Claim(ctx, r.owner, r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("outbox claim failed", slog.String("error", err.Error()))
		}
		return 0
	}
	published := 0
	for i, m := range msgs {
		if ctx.Err() != nil {
			// Shutting down: give the remaining leases back right away.
			r.release(msgs[i:])
			return published
		}
		log := r.log.With(slog.String("eventId", m.ID.String()), slog.String("eventType", m.EventType),
			slog.String("correlationId", m.CorrelationID), slog.String("walletId", m.PartitionKey.String()))
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := r.pub.Publish(pctx, m)
		cancel()
		if errors.Is(err, resilience.ErrOpen) {
			// Broker breaker opened mid-batch: hand this and the remaining
			// leases back; the gate pauses the relay until SQS recovers.
			log.Warn("sqs circuit open; releasing claimed events")
			r.release(msgs[i:])
			return published
		}
		if err != nil {
			r.metrics.OutboxFailure()
			next := time.Now().Add(r.Backoff(m.Attempts))
			log.Warn("publish failed; will retry", slog.String("error", err.Error()),
				slog.Int("attempts", m.Attempts), slog.Time("nextAttemptAt", next))
			if err := r.markFailed(ctx, m.ID, next, err.Error()); err != nil {
				log.Warn("could not reschedule; lease expiry will release it", slog.String("error", err.Error()))
			}
			continue
		}
		if r.AfterPublish != nil {
			r.AfterPublish(m.ID)
		}
		sctx, cancel := storeContext(ctx)
		ok, err := r.store.MarkPublished(sctx, m.ID, r.owner)
		cancel()
		if err != nil || !ok {
			// The event was sent but not confirmed: it will be republished with
			// the same eventId after the lease expires (at-least-once).
			log.Warn("publication not confirmed", slog.Bool("leaseLost", !ok && err == nil))
			continue
		}
		published++
		r.metrics.OutboxPublished(m.OccurredAt)
		log.Debug("event published")
	}
	return published
}

// storeContext bounds outbox bookkeeping: it survives the loop's cancellation
// (a publication must still be confirmed during shutdown) but never waits
// forever on an unresponsive database, so the breaker can see the failure.
func storeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (r *OutboxRelay) markFailed(ctx context.Context, id uuid.UUID, next time.Time, cause string) error {
	sctx, cancel := storeContext(ctx)
	defer cancel()
	return r.store.MarkFailed(sctx, id, r.owner, next, cause)
}

func (r *OutboxRelay) release(msgs []postgres.OutboxMessage) {
	for _, m := range msgs {
		_ = r.markFailed(context.Background(), m.ID, time.Now(), "released")
	}
}

func (r *OutboxRelay) reportLag(ctx context.Context) {
	if time.Since(r.lastLag) < 5*time.Second {
		return
	}
	r.lastLag = time.Now()
	pending, lag, err := r.store.Lag(ctx)
	if err == nil {
		r.metrics.OutboxBacklog(pending, lag)
	}
}
