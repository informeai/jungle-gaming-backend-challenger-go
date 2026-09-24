package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

var ErrInvalidLedgerEntry = errors.New("wallet: invalid ledger entry")

// Direction of a ledger entry from the wallet point of view.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func ParseDirection(s string) (Direction, error) {
	switch d := Direction(s); d {
	case Debit, Credit:
		return d, nil
	}
	return "", fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, s)
}

// LedgerEntry is an immutable, append-only record of a balance change.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount. It is also
// used for rehydration, since the invariant must hold for persisted rows too.
func NewLedgerEntry(id, walletID, txID uuid.UUID, dir Direction, amount, before, after money.Money, createdAt time.Time) (*LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || txID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids are required", ErrInvalidLedgerEntry)
	}
	if createdAt.IsZero() {
		return nil, fmt.Errorf("%w: createdAt is required", ErrInvalidLedgerEntry)
	}
	if _, err := ParseDirection(string(dir)); err != nil {
		return nil, err
	}
	if !amount.IsInitialized() || !before.IsInitialized() || !after.IsInitialized() {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLedgerEntry, money.ErrUninitialized)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidLedgerEntry)
	}
	if before.IsNegative() || after.IsNegative() {
		return nil, fmt.Errorf("%w: balances cannot be negative", ErrInvalidLedgerEntry)
	}
	var expected money.Money
	var err error
	if dir == Credit {
		expected, err = before.Add(amount)
	} else {
		expected, err = before.Sub(amount)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(after) {
		return nil, fmt.Errorf("%w: balanceAfter %s != balanceBefore %s %s %s", ErrInvalidLedgerEntry, after, before, dir, amount)
	}
	return &LedgerEntry{
		id: id, walletID: walletID, transactionID: txID, direction: dir,
		amount: amount, balanceBefore: before, balanceAfter: after, createdAt: createdAt.UTC(),
	}, nil
}

func (e *LedgerEntry) ID() uuid.UUID              { return e.id }
func (e *LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e *LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e *LedgerEntry) Direction() Direction       { return e.direction }
func (e *LedgerEntry) Amount() money.Money        { return e.amount }
func (e *LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
