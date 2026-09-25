package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

const (
	SourceHTTP   = "http"
	SourceSQS    = "sqs"
	SourceWorker = "worker"
)

// SubmitResult is the (possibly replayed) outcome of a provider operation.
type SubmitResult struct {
	Tx     *wagering.Transaction
	Replay bool
}

// WageringService applies provider operations to wallets.
type WageringService struct {
	tx      TxManager
	wallets WalletRepository
	txs     TransactionRepository
	outbox  OutboxRepository
	inbox   InboxRepository
	clock   Clock
	ids     IDGenerator
	policy  wagering.RetryPolicy
	metrics Metrics
	log     *slog.Logger
}

type WageringDeps struct {
	Tx      TxManager
	Wallets WalletRepository
	Txs     TransactionRepository
	Outbox  OutboxRepository
	Inbox   InboxRepository
	Clock   Clock
	IDs     IDGenerator
	Policy  wagering.RetryPolicy
	Metrics Metrics
	Log     *slog.Logger
}

func NewWageringService(d WageringDeps) *WageringService {
	return &WageringService{
		tx: d.Tx, wallets: d.Wallets, txs: d.Txs, outbox: d.Outbox, inbox: d.Inbox,
		clock: d.Clock, ids: d.IDs, policy: d.Policy, metrics: d.Metrics, log: d.Log,
	}
}

// maxTxAttempts bounds retries of a whole database transaction after a
// concurrency conflict (serialization failure, deadlock, version mismatch).
// Unavailability (connection errors, open breaker) is not retried here: the
// caller gets a fast transient error and the circuit breaker protects the
// database from retry storms.
const maxTxAttempts = 3

func (s *WageringService) retry(ctx context.Context, fn func(ctx context.Context) error) error {
	var err error
	for attempt := 1; attempt <= maxTxAttempts; attempt++ {
		err = s.tx.WithinTx(ctx, fn)
		if err == nil || !IsRetryableConflict(err) || ctx.Err() != nil {
			return err
		}
		s.metrics.ConcurrencyConflict()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt*attempt) * 20 * time.Millisecond):
		}
	}
	return err
}

// Submit processes a provider operation received over HTTP. It is synchronous:
// the operation is created and settled in the same SQL transaction, so no
// PENDING row is ever committed.
func (s *WageringService) Submit(ctx context.Context, cmd SubmitCommand) (SubmitResult, error) {
	start := s.clock()
	var res SubmitResult
	err := s.retry(ctx, func(ctx context.Context) error {
		var err error
		res, err = s.submitInTx(ctx, cmd)
		return err
	})
	s.observe(SourceHTTP, cmd, res, err, start)
	return res, err
}

// InboxResult is the outcome of a message handled through the inbox.
type InboxResult struct {
	SubmitResult
	DuplicateMessage bool // the (consumer, messageId) was already completed
}

// SubmitMessage processes a provider operation received from the queue. The
// inbox registration, the operation, the ledger, the outbox and the inbox
// completion share a single SQL transaction.
func (s *WageringService) SubmitMessage(ctx context.Context, consumer, messageID string, cmd SubmitCommand) (InboxResult, error) {
	start := s.clock()
	var res InboxResult
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		res = InboxResult{}
		now := s.clock()
		inserted, storedHash, err := s.inbox.Register(ctx, consumer, messageID, cmd.PayloadHash, now)
		if err != nil {
			return err
		}
		if !inserted {
			if storedHash != cmd.PayloadHash {
				return ErrInboxPayloadMismatch
			}
			res.DuplicateMessage = true
			t, err := s.txs.FindByIdempotencyKey(ctx, cmd.Request.ProviderID, cmd.IdempotencyKey)
			if err != nil {
				return err
			}
			res.Tx, res.Replay = t, true
			return nil
		}
		sub, err := s.submitInTx(ctx, cmd)
		if err != nil {
			return err
		}
		res.SubmitResult = sub
		id := sub.Tx.ID()
		return s.inbox.Complete(ctx, consumer, messageID, &id, string(sub.Tx.Status()), s.clock())
	})
	if res.DuplicateMessage {
		s.metrics.Duplicate(SourceSQS)
	}
	s.observe(SourceSQS, cmd, res.SubmitResult, err, start)
	return res, err
}

func (s *WageringService) observe(source string, cmd SubmitCommand, res SubmitResult, err error, start time.Time) {
	s.metrics.ProcessingLatency(source, s.clock().Sub(start))
	attrs := []any{
		slog.String("source", source), slog.String("correlationId", cmd.CorrelationID),
		slog.String("providerId", cmd.Request.ProviderID), slog.String("walletId", cmd.Request.WalletID.String()),
		slog.String("externalTransactionId", cmd.Request.ExternalID), slog.String("kind", string(cmd.Request.Kind)),
	}
	if err != nil {
		if errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, ErrExternalIDConflict) {
			s.metrics.IdempotencyConflict()
		}
		s.log.Warn("wager operation not applied", append(attrs, slog.String("error", err.Error()))...)
		return
	}
	if res.Tx == nil {
		return
	}
	if res.Replay {
		s.metrics.Duplicate(source)
	}
	s.metrics.TransactionResult(source, string(res.Tx.Kind()), string(res.Tx.Status()), res.Replay)
	s.log.Info("wager operation handled", append(attrs,
		slog.String("transactionId", res.Tx.ID().String()), slog.String("status", string(res.Tx.Status())),
		slog.String("failureCode", string(res.Tx.FailureCode())), slog.Bool("idempotentReplay", res.Replay))...)
}

func replayOf(existing *wagering.Transaction, cmd SubmitCommand) (SubmitResult, error) {
	if existing.PayloadHash() != cmd.PayloadHash {
		return SubmitResult{}, ErrIdempotencyConflict
	}
	return SubmitResult{Tx: existing, Replay: true}, nil
}

func (s *WageringService) submitInTx(ctx context.Context, cmd SubmitCommand) (SubmitResult, error) {
	req := cmd.Request
	existing, err := s.txs.FindByIdempotencyKey(ctx, req.ProviderID, cmd.IdempotencyKey)
	if err != nil {
		return SubmitResult{}, err
	}
	if existing != nil {
		return replayOf(existing, cmd)
	}
	// Per-wallet pessimistic lock: operations of the same wallet serialise
	// here (across processes); other wallets proceed in parallel.
	w, err := s.wallets.GetForUpdate(ctx, req.WalletID)
	if err != nil {
		return SubmitResult{}, err
	}
	if w == nil {
		return SubmitResult{}, ErrWalletNotFound
	}
	t, err := wagering.NewExternal(wagering.ExternalParams{
		ID: s.ids(), ProviderID: req.ProviderID, ExternalID: req.ExternalID,
		IdempotencyKey: cmd.IdempotencyKey, PayloadHash: cmd.PayloadHash, WalletID: req.WalletID,
		PlayerID: req.PlayerID, RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind,
		Money: req.Money, ReferenceExternalID: req.ReferenceExternalID,
		CorrelationID: cmd.CorrelationID, CausationID: cmd.CausationID, Now: s.clock(),
	})
	if err != nil {
		return SubmitResult{}, err
	}
	inserted, err := s.txs.InsertIfAbsent(ctx, t)
	if err != nil {
		return SubmitResult{}, err
	}
	if !inserted {
		if existing, err = s.txs.FindByIdempotencyKey(ctx, req.ProviderID, cmd.IdempotencyKey); err != nil {
			return SubmitResult{}, err
		}
		if existing != nil {
			return replayOf(existing, cmd)
		}
		byExternal, err := s.txs.FindByExternalID(ctx, req.ProviderID, req.ExternalID)
		if err != nil {
			return SubmitResult{}, err
		}
		if byExternal != nil {
			return SubmitResult{}, ErrExternalIDConflict
		}
		return SubmitResult{}, &TransientError{Err: errors.New("idempotency row vanished during insert")}
	}
	if err := s.settle(ctx, w, t); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Tx: t}, nil
}

// settle applies t to the locked wallet and persists every resulting change
// (wallet, ledger, transaction, outbox) in the caller's SQL transaction.
func (s *WageringService) settle(ctx context.Context, w *wallet.Wallet, t *wagering.Transaction) error {
	ref := wagering.Reference{}
	if t.NeedsReference() {
		r, err := s.txs.FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
		if err != nil {
			return err
		}
		if r != nil {
			reversed, err := s.txs.HasProcessedReversal(ctx, r.ID())
			if err != nil {
				return err
			}
			ref = wagering.Reference{Tx: r, AlreadyReversed: reversed}
		}
	}
	now := s.clock()
	previousVersion := w.Version()
	st, err := wagering.Settle(wagering.Env{NewID: s.ids, Now: now, Policy: s.policy}, w, t, ref)
	if err != nil {
		return err
	}
	if st.Entry != nil {
		if err := s.wallets.Update(ctx, w, previousVersion); err != nil {
			return err
		}
		if err := s.wallets.InsertLedgerEntry(ctx, st.Entry); err != nil {
			return err
		}
	}
	if err := s.txs.Update(ctx, t); err != nil {
		return err
	}
	if err := s.outbox.Append(ctx, st.Events); err != nil {
		return err
	}
	if t.Status().IsTerminal() {
		// Operations waiting for this one become due immediately.
		return s.txs.WakeDependents(ctx, t.ProviderID(), t.ExternalID(), now)
	}
	return nil
}

// ResolvePending retries a PENDING_REFERENCE transaction. It is safe to run
// from any number of instances: the wallet lock serialises them and the
// status/due-time recheck makes a lost race a no-op.
func (s *WageringService) ResolvePending(ctx context.Context, p PendingRef) (*wagering.Transaction, error) {
	var out *wagering.Transaction
	err := s.retry(ctx, func(ctx context.Context) error {
		out = nil
		w, err := s.wallets.GetForUpdate(ctx, p.WalletID)
		if err != nil {
			return err
		}
		if w == nil {
			return fmt.Errorf("%w: %s", ErrWalletNotFound, p.WalletID)
		}
		t, err := s.txs.GetForUpdate(ctx, p.TransactionID)
		if err != nil {
			return err
		}
		if t == nil || t.Status() != wagering.StatusPendingReference {
			return nil
		}
		if next := t.NextAttemptAt(); next != nil && next.After(s.clock()) {
			return nil
		}
		if err := s.settle(ctx, w, t); err != nil {
			return err
		}
		out = t
		return nil
	})
	if err == nil && out != nil {
		if out.Status() == wagering.StatusPendingReference {
			s.metrics.ReferenceRetry()
		} else {
			s.metrics.TransactionResult(SourceWorker, string(out.Kind()), string(out.Status()), false)
		}
		s.log.Info("pending reference evaluated",
			slog.String("transactionId", out.ID().String()), slog.String("walletId", out.WalletID().String()),
			slog.String("providerId", out.ProviderID()), slog.String("correlationId", out.CorrelationID()),
			slog.String("status", string(out.Status())), slog.Int("attempts", out.Attempts()),
			slog.String("failureCode", string(out.FailureCode())))
	}
	return out, err
}

// MarkFailed records a permanent processing failure of a pending transaction.
func (s *WageringService) MarkFailed(ctx context.Context, id uuid.UUID) error {
	return s.tx.WithinTx(ctx, func(ctx context.Context) error {
		t, err := s.txs.GetForUpdate(ctx, id)
		if err != nil || t == nil || t.Status().IsTerminal() {
			return err
		}
		if err := t.MarkFailed(wagering.FailPermanentProcessingError, s.clock()); err != nil {
			return err
		}
		return s.txs.Update(ctx, t)
	})
}

// DuePending lists PENDING_REFERENCE transactions due for another attempt.
func (s *WageringService) DuePending(ctx context.Context, limit int) ([]PendingRef, error) {
	return s.txs.DuePendingReferences(ctx, s.clock(), limit)
}

// Get returns a transaction by internal id.
func (s *WageringService) Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	t, err := s.txs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// GetByExternalID returns a provider transaction.
func (s *WageringService) GetByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	t, err := s.txs.FindByExternalID(ctx, providerID, externalID)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}
