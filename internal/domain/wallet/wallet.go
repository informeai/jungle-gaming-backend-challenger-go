// Package wallet contains the Wallet aggregate root and its immutable ledger
// entries. It has no knowledge of persistence, transport or DI frameworks.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

var (
	ErrInvalidWallet     = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	ErrCurrencyMismatch  = errors.New("wallet: currency mismatch")
	ErrNonPositiveAmount = errors.New("wallet: movement amount must be positive")
)

// InitialVersion is the version of a freshly opened wallet.
const InitialVersion int64 = 1

// Wallet is the financial aggregate root. Its balance only changes through
// Debit and Credit, and each change yields the matching ledger entry.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Opening is the result of opening a wallet. When the initial balance is
// positive it carries the OPENING ledger entry (the OPENING transaction is
// created by the wagering package, which owns transaction semantics).
type Opening struct {
	Wallet *Wallet
	Entry  *LedgerEntry // nil when the initial balance is zero
}

// Open creates a new wallet at version 1. A positive initial balance is booked
// as a CREDIT from 0 to the initial balance, owned by openingTxID.
func Open(id, playerID uuid.UUID, initial money.Money, openingTxID, entryID uuid.UUID, now time.Time) (Opening, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return Opening{}, fmt.Errorf("%w: id and playerId are required", ErrInvalidWallet)
	}
	if !initial.IsInitialized() {
		return Opening{}, fmt.Errorf("%w: initial balance: %w", ErrInvalidWallet, money.ErrUninitialized)
	}
	if initial.IsNegative() {
		return Opening{}, fmt.Errorf("%w: initial balance cannot be negative", ErrInvalidWallet)
	}
	now = now.UTC()
	w := &Wallet{id: id, playerID: playerID, balance: initial, version: InitialVersion, createdAt: now, updatedAt: now}
	if initial.IsZero() {
		return Opening{Wallet: w}, nil
	}
	zero, _ := money.Zero(initial.Currency())
	entry, err := NewLedgerEntry(entryID, id, openingTxID, Credit, initial, zero, initial, now)
	if err != nil {
		return Opening{}, err
	}
	return Opening{Wallet: w, Entry: entry}, nil
}

// Rehydrate rebuilds a persisted wallet without applying any movement.
func Rehydrate(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	switch {
	case id == uuid.Nil || playerID == uuid.Nil:
		return nil, fmt.Errorf("%w: id and playerId are required", ErrInvalidWallet)
	case !balance.IsInitialized():
		return nil, fmt.Errorf("%w: balance: %w", ErrInvalidWallet, money.ErrUninitialized)
	case balance.IsNegative():
		return nil, fmt.Errorf("%w: negative balance", ErrInvalidWallet)
	case version < InitialVersion:
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidWallet)
	case createdAt.IsZero() || updatedAt.IsZero():
		return nil, fmt.Errorf("%w: timestamps are required", ErrInvalidWallet)
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// CanDebit reports whether amount can be debited without a negative balance.
func (w *Wallet) CanDebit(amount money.Money) (bool, error) {
	if err := w.checkMovement(amount); err != nil {
		return false, err
	}
	cmp, err := w.balance.Cmp(amount)
	if err != nil {
		return false, err
	}
	return cmp >= 0, nil
}

// Debit subtracts amount, bumps the version and returns the ledger entry.
// It returns ErrInsufficientFunds (and leaves the wallet untouched) when the
// balance would become negative.
func (w *Wallet) Debit(entryID, txID uuid.UUID, amount money.Money, now time.Time) (*LedgerEntry, error) {
	ok, err := w.CanDebit(amount)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrInsufficientFunds
	}
	after, err := w.balance.Sub(amount)
	if err != nil {
		return nil, err
	}
	return w.apply(entryID, txID, Debit, amount, after, now)
}

// Credit adds amount, bumps the version and returns the ledger entry.
func (w *Wallet) Credit(entryID, txID uuid.UUID, amount money.Money, now time.Time) (*LedgerEntry, error) {
	if err := w.checkMovement(amount); err != nil {
		return nil, err
	}
	after, err := w.balance.Add(amount)
	if err != nil {
		return nil, err
	}
	return w.apply(entryID, txID, Credit, amount, after, now)
}

func (w *Wallet) checkMovement(amount money.Money) error {
	if !amount.IsInitialized() {
		return money.ErrUninitialized
	}
	if amount.Currency() != w.Currency() {
		return fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.Currency(), amount.Currency())
	}
	if !amount.IsPositive() {
		return ErrNonPositiveAmount
	}
	return nil
}

func (w *Wallet) apply(entryID, txID uuid.UUID, dir Direction, amount, after money.Money, now time.Time) (*LedgerEntry, error) {
	entry, err := NewLedgerEntry(entryID, w.id, txID, dir, amount, w.balance, after, now)
	if err != nil {
		return nil, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}
