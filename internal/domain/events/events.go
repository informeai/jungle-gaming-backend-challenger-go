// Package events defines the integration events emitted through the outbox.
// Each constructor fixes the event type and schema version; payloads only use
// strings for money and RFC 3339 UTC timestamps, so a stored snapshot is
// immutable and republishing it yields the same bytes and eventId.
package events

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

const (
	AggregateWallet           = "Wallet"
	AggregateWagerTransaction = "WagerTransaction"
)

var ErrInvalidEvent = errors.New("events: invalid event")

// Timestamp serialises as RFC 3339 in UTC with millisecond precision.
type Timestamp time.Time

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02T15:04:05.000Z07:00"))
}

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*t = Timestamp(parsed.UTC())
	return nil
}

// Envelope is the common wrapper of every integration event.
type Envelope[T any] struct {
	EventID       uuid.UUID  `json:"eventId"`
	EventType     string     `json:"eventType"`
	AggregateID   uuid.UUID  `json:"aggregateId"`
	CorrelationID string     `json:"correlationId"`
	CausationID   *string    `json:"causationId,omitempty"`
	OccurredAt    Timestamp  `json:"occurredAt"`
	Version       int        `json:"version"`
	Data          T          `json:"data"`
	meta          recordMeta `json:"-"`
}

type recordMeta struct {
	aggregateType string
	partitionKey  uuid.UUID
}

// Record is the persistence-ready, immutable snapshot of an event.
type Record struct {
	EventID       uuid.UUID
	EventType     string
	Version       int
	AggregateType string
	AggregateID   uuid.UUID
	PartitionKey  uuid.UUID // wallet id: keeps per-wallet ordering in FIFO queues
	CorrelationID string
	OccurredAt    time.Time
	Payload       []byte
}

// Meta carries identifiers common to every event.
type Meta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   *string
	OccurredAt    time.Time
}

func (m Meta) validate() error {
	if m.EventID == uuid.Nil || m.OccurredAt.IsZero() || m.CorrelationID == "" {
		return ErrInvalidEvent
	}
	return nil
}

func newEnvelope[T any](m Meta, eventType, aggregateType string, aggregateID, walletID uuid.UUID, data T) (Envelope[T], error) {
	if err := m.validate(); err != nil {
		return Envelope[T]{}, err
	}
	if aggregateID == uuid.Nil || walletID == uuid.Nil {
		return Envelope[T]{}, ErrInvalidEvent
	}
	return Envelope[T]{
		EventID: m.EventID, EventType: eventType, AggregateID: aggregateID,
		CorrelationID: m.CorrelationID, CausationID: m.CausationID,
		OccurredAt: Timestamp(m.OccurredAt.UTC()), Version: 1, Data: data,
		meta: recordMeta{aggregateType: aggregateType, partitionKey: walletID},
	}, nil
}

// Record serialises the envelope into an immutable snapshot.
func (e Envelope[T]) Record() (Record, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return Record{}, err
	}
	return Record{
		EventID: e.EventID, EventType: e.EventType, Version: e.Version,
		AggregateType: e.meta.aggregateType, AggregateID: e.AggregateID,
		PartitionKey: e.meta.partitionKey, CorrelationID: e.CorrelationID,
		OccurredAt: time.Time(e.OccurredAt), Payload: payload,
	}, nil
}

// MoneyDTO is the wire form of money.
type MoneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func MoneyOf(m money.Money) MoneyDTO {
	return MoneyDTO{Amount: m.Amount(), Currency: m.Currency().Code()}
}

// WagerTransactionProcessed is emitted when an operation (including LOSS and
// the internal OPENING) completes successfully.
type WagerTransactionProcessed struct {
	TransactionID          uuid.UUID  `json:"transactionId"`
	Origin                 string     `json:"origin"`
	ProviderID             string     `json:"providerId,omitempty"`
	ExternalTransactionID  string     `json:"externalTransactionId,omitempty"`
	WalletID               uuid.UUID  `json:"walletId"`
	PlayerID               uuid.UUID  `json:"playerId"`
	RoundID                string     `json:"roundId,omitempty"`
	GameID                 string     `json:"gameId,omitempty"`
	Kind                   string     `json:"kind"`
	Money                  MoneyDTO   `json:"money"`
	ReferenceTransactionID *uuid.UUID `json:"referenceTransactionId,omitempty"`
	Balance                MoneyDTO   `json:"balance"`
	WalletVersion          int64      `json:"walletVersion"`
	ProcessedAt            Timestamp  `json:"processedAt"`
}

func NewWagerTransactionProcessed(m Meta, d WagerTransactionProcessed) (Envelope[WagerTransactionProcessed], error) {
	return newEnvelope(m, TypeWagerTransactionProcessed, AggregateWagerTransaction, d.TransactionID, d.WalletID, d)
}

// WagerTransactionRejected is emitted on a definitive business rejection.
type WagerTransactionRejected struct {
	TransactionID         uuid.UUID `json:"transactionId"`
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	WalletID              uuid.UUID `json:"walletId"`
	PlayerID              uuid.UUID `json:"playerId"`
	RoundID               string    `json:"roundId"`
	GameID                string    `json:"gameId"`
	Kind                  string    `json:"kind"`
	Money                 MoneyDTO  `json:"money"`
	FailureCode           string    `json:"failureCode"`
	RejectedAt            Timestamp `json:"rejectedAt"`
}

func NewWagerTransactionRejected(m Meta, d WagerTransactionRejected) (Envelope[WagerTransactionRejected], error) {
	if d.FailureCode == "" {
		return Envelope[WagerTransactionRejected]{}, ErrInvalidEvent
	}
	return newEnvelope(m, TypeWagerTransactionRejected, AggregateWagerTransaction, d.TransactionID, d.WalletID, d)
}

// WalletBalanceChanged is emitted for every effective balance change.
type WalletBalanceChanged struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         MoneyDTO  `json:"money"`
	BalanceBefore MoneyDTO  `json:"balanceBefore"`
	BalanceAfter  MoneyDTO  `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

func NewWalletBalanceChanged(m Meta, d WalletBalanceChanged) (Envelope[WalletBalanceChanged], error) {
	return newEnvelope(m, TypeWalletBalanceChanged, AggregateWallet, d.WalletID, d.WalletID, d)
}

// WagerTransactionPendingReference is emitted when an operation starts waiting
// for the transaction it references.
type WagerTransactionPendingReference struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	PlayerID                       uuid.UUID `json:"playerId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyDTO  `json:"money"`
	NextAttemptAt                  Timestamp `json:"nextAttemptAt"`
	ExpiresAt                      Timestamp `json:"expiresAt"`
}

func NewWagerTransactionPendingReference(m Meta, d WagerTransactionPendingReference) (Envelope[WagerTransactionPendingReference], error) {
	return newEnvelope(m, TypeWagerTransactionPendingReference, AggregateWagerTransaction, d.TransactionID, d.WalletID, d)
}
