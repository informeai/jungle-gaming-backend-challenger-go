package worker

import (
	"context"
	"errors"
	"log/slog"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
)

// PendingResolver retries PENDING_REFERENCE operations. State lives only in
// the database (status, attempts, next_attempt_at, expires_at), so a restart
// or another instance simply continues where the last attempt stopped.
type PendingResolver struct {
	svc   *app.WageringService
	batch int
	log   *slog.Logger
	// Gate pauses the worker while the database breaker is open.
	Gate *resilience.Gate
}

func NewPendingResolver(svc *app.WageringService, batch int, log *slog.Logger) *PendingResolver {
	return &PendingResolver{svc: svc, batch: batch, log: log.With(slog.String("component", "pending-reference-worker"))}
}

// Tick evaluates every due pending operation once.
func (p *PendingResolver) Tick(ctx context.Context) {
	if err := p.Gate.Ready(ctx); err != nil {
		return // stopping
	}
	due, err := p.svc.DuePending(ctx, p.batch)
	if err != nil {
		if ctx.Err() == nil {
			p.log.Warn("listing pending references failed", slog.String("error", err.Error()))
		}
		return
	}
	for _, d := range due {
		if ctx.Err() != nil {
			return
		}
		_, err := p.svc.ResolvePending(ctx, d)
		if errors.Is(err, resilience.ErrOpen) {
			return // database unavailable: stop this tick, the gate waits
		}
		if err == nil || ctx.Err() != nil || app.IsTransient(err) || errors.Is(err, context.DeadlineExceeded) {
			if err != nil && ctx.Err() == nil {
				p.log.Warn("pending reference attempt failed (transient)", slog.String("transactionId", d.TransactionID.String()), slog.String("error", err.Error()))
			}
			continue
		}
		// Permanent processing error: record FAILED for auditing so the row
		// does not loop forever.
		p.log.Error("pending reference failed permanently", slog.String("transactionId", d.TransactionID.String()),
			slog.String("walletId", d.WalletID.String()), slog.String("error", err.Error()))
		if err := p.svc.MarkFailed(ctx, d.TransactionID); err != nil {
			p.log.Error("could not mark transaction as FAILED", slog.String("error", err.Error()))
		}
	}
}
