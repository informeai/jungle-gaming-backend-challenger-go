// Package app holds the use cases shared by the HTTP API, the SQS consumer and
// the background workers. It depends only on the domain and on the ports
// declared here; adapters (Postgres, SQS, HTTP) implement or call them.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/events"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

// TxManager runs fn inside a single SQL transaction. Repositories called with
// the ctx passed to fn join that transaction. Nested calls reuse the outer
// transaction, so the caller that opened it owns commit/rollback.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
	// WithinSnapshot runs fn in a read-only REPEATABLE READ transaction.
	WithinSnapshot(ctx context.Context, fn func(ctx context.Context) error) error
}

type WalletRepository interface {
	// Insert fails with ErrWalletAlreadyExists on (playerId, currency) conflict.
	Insert(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// GetForUpdate locks the wallet row (SELECT ... FOR UPDATE).
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Update persists balance/version, guarded by expectedVersion.
	Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
	InsertLedgerEntry(ctx context.Context, e *wallet.LedgerEntry) error
	ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	LedgerTotals(ctx context.Context, walletID uuid.UUID, c money.Currency) (LedgerTotals, error)
}

// LedgerRow is a ledger entry plus its stable ordering key.
type LedgerRow struct {
	Seq   int64
	Entry *wallet.LedgerEntry
}

type LedgerTotals struct {
	Balance money.Money // credits - debits
	Entries int64
}

type TransactionRepository interface {
	// InsertIfAbsent inserts t unless (providerId, idempotencyKey) or
	// (providerId, externalTransactionId) already exists.
	InsertIfAbsent(ctx context.Context, t *wagering.Transaction) (bool, error)
	Insert(ctx context.Context, t *wagering.Transaction) error
	Update(ctx context.Context, t *wagering.Transaction) error
	Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error)
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error)
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// DuePendingReferences lists PENDING_REFERENCE transactions due at now.
	DuePendingReferences(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
	// WakeDependents makes operations waiting on (providerId, externalId) due now.
	WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error
}

type PendingRef struct {
	TransactionID uuid.UUID
	WalletID      uuid.UUID
}

type OutboxRepository interface {
	Append(ctx context.Context, records []events.Record) error
}

type InboxRepository interface {
	// Register records the message; it returns inserted=false and the stored
	// hash when (consumer, messageId) was already registered.
	Register(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (inserted bool, storedHash string, err error)
	Complete(ctx context.Context, consumer, messageID string, txID *uuid.UUID, outcome string, now time.Time) error
}

// Clock and IDs make time and identity injectable in tests.
type Clock func() time.Time

type IDGenerator func() uuid.UUID

// NewUUIDv7 generates time-ordered identifiers.
func NewUUIDv7() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// Metrics is the subset of instrumentation used by the use cases.
type Metrics interface {
	TransactionResult(source, kind, status string, replay bool)
	Duplicate(source string)
	IdempotencyConflict()
	ConcurrencyConflict()
	ProcessingLatency(source string, d time.Duration)
	ReferenceRetry()
	ReconciliationChecked(consistent bool)
}
