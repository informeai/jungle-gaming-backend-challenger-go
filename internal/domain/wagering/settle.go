package wagering

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/events"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

// RetryPolicy controls how long an operation waits for its reference.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	TTL         time.Duration
}

// DefaultRetryPolicy: 1s, 2s, 4s ... capped at 1m, 12 attempts or 30 minutes.
var DefaultRetryPolicy = RetryPolicy{MaxAttempts: 12, BaseDelay: time.Second, MaxDelay: time.Minute, TTL: 30 * time.Minute}

// Delay returns the exponential backoff for the given (0-based) attempt.
func (p RetryPolicy) Delay(attempt int) time.Duration {
	d := p.BaseDelay
	for i := 0; i < attempt && d < p.MaxDelay; i++ {
		d *= 2
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// Env carries the non-deterministic inputs of a settlement.
type Env struct {
	NewID  func() uuid.UUID
	Now    time.Time
	Policy RetryPolicy
}

// Reference is what the repository found for the referenced transaction,
// resolved by (providerId, referenceExternalTransactionId).
type Reference struct {
	Tx *Transaction // nil when it has not arrived yet
	// AlreadyReversed is true when a PROCESSED REFUND or ROLLBACK already
	// references Tx.
	AlreadyReversed bool
}

type Outcome string

const (
	OutcomeProcessed        Outcome = "PROCESSED"
	OutcomeRejected         Outcome = "REJECTED"
	OutcomePendingReference Outcome = "PENDING_REFERENCE"
)

// Settlement is the result of applying a transaction to a wallet. The caller
// must persist the wallet, the ledger entry, the transaction and the event
// records in the same database transaction.
type Settlement struct {
	Outcome Outcome
	Entry   *wallet.LedgerEntry
	Events  []events.Record
}

// Settle applies a PENDING or PENDING_REFERENCE transaction to its (locked)
// wallet according to the rules of each kind.
func Settle(env Env, w *wallet.Wallet, t *Transaction, ref Reference) (Settlement, error) {
	if t.status.IsTerminal() {
		return Settlement{}, fmt.Errorf("%w: %s is terminal", ErrInvalidTransition, t.status)
	}
	if w.ID() != t.walletID {
		return Settlement{}, fmt.Errorf("%w: wallet %s does not own transaction %s", ErrInvalidTransaction, w.ID(), t.id)
	}
	s := settler{env: env, w: w, t: t}
	if w.PlayerID() != t.playerID {
		return s.reject(FailWalletPlayerMismatch, false, nil)
	}
	if w.Currency() != t.money.Currency() {
		return s.reject(FailCurrencyMismatch, false, nil)
	}

	var refID *uuid.UUID
	if t.NeedsReference() {
		if ref.Tx == nil || !ref.Tx.status.IsTerminal() {
			return s.awaitReference(ref.Tx != nil)
		}
		id := ref.Tx.id
		refID = &id
		if code := s.checkReference(ref); code != "" {
			return s.reject(code, true, refID)
		}
	}

	switch t.kind {
	case KindBet:
		return s.debit(FailInsufficientFunds, nil)
	case KindWin, KindRefund:
		return s.credit(refID)
	case KindLoss:
		return s.processed(nil, nil)
	case KindRollback:
		if ref.Tx.kind == KindBet {
			return s.credit(refID)
		}
		return s.debit(FailInsufficientFundsForReversal, refID)
	}
	return Settlement{}, fmt.Errorf("%w: kind %s cannot be settled", ErrInvalidTransaction, t.kind)
}

type settler struct {
	env Env
	w   *wallet.Wallet
	t   *Transaction
}

// checkReference validates a terminal reference. It returns "" when valid.
func (s settler) checkReference(ref Reference) FailureCode {
	r, t := ref.Tx, s.t
	if r.status != StatusProcessed {
		return FailReferenceNotProcessed
	}
	switch t.kind {
	case KindWin, KindRefund:
		if r.kind != KindBet {
			return FailReferenceKindNotAllowed
		}
	case KindRollback:
		if r.kind != KindBet && r.kind != KindWin && r.kind != KindRefund {
			return FailReferenceKindNotAllowed
		}
	}
	if r.providerID != t.providerID || r.playerID != t.playerID || r.walletID != t.walletID ||
		r.money.Currency() != t.money.Currency() || r.roundID != t.roundID {
		return FailReferenceMismatch
	}
	if t.kind.IsReversal() {
		if !r.money.Equal(t.money) {
			return FailReferenceAmountMismatch
		}
		if ref.AlreadyReversed {
			return FailReferenceAlreadyReversed
		}
	}
	return ""
}

func (s settler) debit(insufficient FailureCode, refID *uuid.UUID) (Settlement, error) {
	entry, err := s.w.Debit(s.env.NewID(), s.t.id, s.t.money, s.env.Now)
	if errors.Is(err, wallet.ErrInsufficientFunds) {
		return s.reject(insufficient, true, refID)
	}
	if err != nil {
		return Settlement{}, err
	}
	return s.processed(entry, refID)
}

func (s settler) credit(refID *uuid.UUID) (Settlement, error) {
	entry, err := s.w.Credit(s.env.NewID(), s.t.id, s.t.money, s.env.Now)
	if errors.Is(err, money.ErrOverflow) {
		return s.reject(FailAmountOverflow, true, refID)
	}
	if err != nil {
		return Settlement{}, err
	}
	return s.processed(entry, refID)
}

func (s settler) meta() events.Meta {
	return events.Meta{EventID: s.env.NewID(), CorrelationID: s.t.correlationID, CausationID: s.t.causationID, OccurredAt: s.env.Now}
}

func (s settler) processed(entry *wallet.LedgerEntry, refID *uuid.UUID) (Settlement, error) {
	if err := s.t.MarkProcessed(s.w.Balance(), s.w.Version(), refID, s.env.Now); err != nil {
		return Settlement{}, err
	}
	recs, err := processedEvents(s.meta, s.t, s.w, entry)
	if err != nil {
		return Settlement{}, err
	}
	return Settlement{Outcome: OutcomeProcessed, Entry: entry, Events: recs}, nil
}

func processedEvents(meta func() events.Meta, t *Transaction, w *wallet.Wallet, entry *wallet.LedgerEntry) ([]events.Record, error) {
	processed, err := events.NewWagerTransactionProcessed(meta(), events.WagerTransactionProcessed{
		TransactionID: t.id, Origin: string(t.origin), ProviderID: t.providerID,
		ExternalTransactionID: t.externalID, WalletID: t.walletID, PlayerID: t.playerID,
		RoundID: t.roundID, GameID: t.gameID, Kind: string(t.kind), Money: events.MoneyOf(t.money),
		ReferenceTransactionID: t.referenceID, Balance: events.MoneyOf(w.Balance()),
		WalletVersion: w.Version(), ProcessedAt: events.Timestamp(t.updatedAt),
	})
	if err != nil {
		return nil, err
	}
	rec, err := processed.Record()
	if err != nil {
		return nil, err
	}
	recs := []events.Record{rec}
	if entry == nil {
		return recs, nil
	}
	changed, err := events.NewWalletBalanceChanged(meta(), events.WalletBalanceChanged{
		WalletID: w.ID(), TransactionID: t.id, Direction: string(entry.Direction()),
		Money: events.MoneyOf(entry.Amount()), BalanceBefore: events.MoneyOf(entry.BalanceBefore()),
		BalanceAfter: events.MoneyOf(entry.BalanceAfter()), WalletVersion: w.Version(),
	})
	if err != nil {
		return nil, err
	}
	rec, err = changed.Record()
	if err != nil {
		return nil, err
	}
	return append(recs, rec), nil
}

func (s settler) reject(code FailureCode, exposeBalance bool, refID *uuid.UUID) (Settlement, error) {
	var balance *money.Money
	if exposeBalance {
		b := s.w.Balance()
		balance = &b
	}
	if err := s.t.MarkRejected(code, balance, refID, s.env.Now); err != nil {
		return Settlement{}, err
	}
	env, err := events.NewWagerTransactionRejected(s.meta(), events.WagerTransactionRejected{
		TransactionID: s.t.id, ProviderID: s.t.providerID, ExternalTransactionID: s.t.externalID,
		WalletID: s.t.walletID, PlayerID: s.t.playerID, RoundID: s.t.roundID, GameID: s.t.gameID,
		Kind: string(s.t.kind), Money: events.MoneyOf(s.t.money), FailureCode: string(code),
		RejectedAt: events.Timestamp(s.env.Now),
	})
	if err != nil {
		return Settlement{}, err
	}
	rec, err := env.Record()
	if err != nil {
		return Settlement{}, err
	}
	return Settlement{Outcome: OutcomeRejected, Events: []events.Record{rec}}, nil
}

// awaitReference parks the transaction, or rejects it when the retry budget
// is exhausted: MaxAttempts evaluations (the first one included) or the TTL.
func (s settler) awaitReference(referenceExists bool) (Settlement, error) {
	t, p, now := s.t, s.env.Policy, s.env.Now
	if t.status == StatusPendingReference {
		exhausted := t.attempts+1 >= p.MaxAttempts || (t.expiresAt != nil && !now.Before(*t.expiresAt))
		if exhausted {
			code := FailReferenceNotFound
			if referenceExists {
				code = FailReferenceStillPending
			}
			return s.reject(code, true, nil)
		}
		if err := t.MarkPendingReference(now.Add(p.Delay(t.attempts)), time.Time{}, now); err != nil {
			return Settlement{}, err
		}
		return Settlement{Outcome: OutcomePendingReference}, nil
	}
	if err := t.MarkPendingReference(now.Add(p.Delay(0)), now.Add(p.TTL), now); err != nil {
		return Settlement{}, err
	}
	env, err := events.NewWagerTransactionPendingReference(s.meta(), events.WagerTransactionPendingReference{
		TransactionID: t.id, ProviderID: t.providerID, ExternalTransactionID: t.externalID,
		ReferenceExternalTransactionID: t.referenceExternalID, WalletID: t.walletID, PlayerID: t.playerID,
		Kind: string(t.kind), Money: events.MoneyOf(t.money),
		NextAttemptAt: events.Timestamp(*t.nextAttemptAt), ExpiresAt: events.Timestamp(*t.expiresAt),
	})
	if err != nil {
		return Settlement{}, err
	}
	rec, err := env.Record()
	if err != nil {
		return Settlement{}, err
	}
	return Settlement{Outcome: OutcomePendingReference, Events: []events.Record{rec}}, nil
}

// Opening is the atomic result of opening a wallet.
type Opening struct {
	Wallet *wallet.Wallet
	Tx     *Transaction        // nil for a zero initial balance
	Entry  *wallet.LedgerEntry // nil for a zero initial balance
	Events []events.Record
}

// OpenWallet opens a wallet at version 1. A positive initial balance creates
// the PROCESSED OPENING transaction, its CREDIT entry and the
// WagerTransactionProcessed + WalletBalanceChanged events; a zero balance
// creates none of them.
func OpenWallet(env Env, walletID, playerID uuid.UUID, initial money.Money, correlationID string) (Opening, error) {
	if !initial.IsInitialized() {
		return Opening{}, invalid(CodeInvalidMoney, "initialBalance", "initial balance is required")
	}
	if initial.IsNegative() {
		return Opening{}, invalid(CodeInvalidMoney, "initialBalance.amount", "initial balance cannot be negative")
	}
	if initial.IsZero() {
		o, err := wallet.Open(walletID, playerID, initial, uuid.Nil, uuid.Nil, env.Now)
		if err != nil {
			return Opening{}, err
		}
		return Opening{Wallet: o.Wallet}, nil
	}
	tx, err := NewOpening(env.NewID(), walletID, playerID, initial, correlationID, env.Now)
	if err != nil {
		return Opening{}, err
	}
	o, err := wallet.Open(walletID, playerID, initial, tx.id, env.NewID(), env.Now)
	if err != nil {
		return Opening{}, err
	}
	if err := tx.MarkProcessed(o.Wallet.Balance(), o.Wallet.Version(), nil, env.Now); err != nil {
		return Opening{}, err
	}
	meta := func() events.Meta {
		return events.Meta{EventID: env.NewID(), CorrelationID: correlationID, OccurredAt: env.Now}
	}
	recs, err := processedEvents(meta, tx, o.Wallet, o.Entry)
	if err != nil {
		return Opening{}, err
	}
	return Opening{Wallet: o.Wallet, Tx: tx, Entry: o.Entry, Events: recs}, nil
}
