package wagering

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

// MaxIdentifierLength bounds provider supplied identifiers.
const MaxIdentifierLength = 128

// Transaction is a wager transaction (external provider operation or the
// internal OPENING). State only changes through the Mark* methods, which
// validate the state machine:
//
//	PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
//	PENDING_REFERENCE ──► PENDING_REFERENCE (reschedule) | PROCESSED | REJECTED | FAILED
//	PROCESSED, REJECTED, FAILED: terminal
type Transaction struct {
	id                  uuid.UUID
	origin              Origin
	kind                Kind
	status              Status
	walletID            uuid.UUID
	playerID            uuid.UUID
	money               money.Money
	providerID          string
	externalID          string
	idempotencyKey      string
	payloadHash         string
	roundID             string
	gameID              string
	referenceExternalID string
	referenceID         *uuid.UUID
	failureCode         FailureCode
	resultBalance       *money.Money
	resultWalletVersion int64
	attempts            int
	nextAttemptAt       *time.Time
	expiresAt           *time.Time
	correlationID       string
	causationID         *string
	createdAt           time.Time
	updatedAt           time.Time
	completedAt         *time.Time
}

// ExternalParams are the inputs of a provider operation.
type ExternalParams struct {
	ID                  uuid.UUID
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                Kind
	Money               money.Money
	ReferenceExternalID string
	CorrelationID       string
	CausationID         *string
	Now                 time.Time
}

// NewExternal validates a provider operation and creates it as PENDING.
func NewExternal(p ExternalParams) (*Transaction, error) {
	if err := ValidateExternal(p.ProviderID, p.ExternalID, p.RoundID, p.GameID, p.Kind, p.Money, p.ReferenceExternalID); err != nil {
		return nil, err
	}
	switch {
	case p.ID == uuid.Nil:
		return nil, fmt.Errorf("%w: id is required", ErrInvalidTransaction)
	case p.WalletID == uuid.Nil:
		return nil, invalid(CodeMissingField, "walletId", "walletId is required")
	case p.PlayerID == uuid.Nil:
		return nil, invalid(CodeMissingField, "playerId", "playerId is required")
	case p.IdempotencyKey == "" || len(p.IdempotencyKey) > 2*MaxIdentifierLength+1:
		return nil, invalid(CodeMissingField, "idempotencyKey", "idempotency key is required (max 257 chars)")
	case p.PayloadHash == "":
		return nil, fmt.Errorf("%w: payload hash is required", ErrInvalidTransaction)
	case p.CorrelationID == "":
		return nil, fmt.Errorf("%w: correlation id is required", ErrInvalidTransaction)
	case p.Now.IsZero():
		return nil, fmt.Errorf("%w: timestamp is required", ErrInvalidTransaction)
	}
	now := p.Now.UTC()
	return &Transaction{
		id: p.ID, origin: OriginExternal, kind: p.Kind, status: StatusPending,
		walletID: p.WalletID, playerID: p.PlayerID, money: p.Money,
		providerID: p.ProviderID, externalID: p.ExternalID, idempotencyKey: p.IdempotencyKey,
		payloadHash: p.PayloadHash, roundID: p.RoundID, gameID: p.GameID,
		referenceExternalID: p.ReferenceExternalID, correlationID: p.CorrelationID,
		causationID: p.CausationID, createdAt: now, updatedAt: now,
	}, nil
}

// ValidateExternal applies the per-kind input rules shared by HTTP and SQS.
func ValidateExternal(providerID, externalID, roundID, gameID string, kind Kind, m money.Money, referenceExternalID string) error {
	for _, f := range []struct{ name, value string }{
		{"providerId", providerID}, {"externalTransactionId", externalID},
		{"roundId", roundID}, {"gameId", gameID},
	} {
		if f.value == "" {
			return invalid(CodeMissingField, f.name, f.name+" is required")
		}
		if len(f.value) > MaxIdentifierLength {
			return invalid(CodeInvalidField, f.name, fmt.Sprintf("%s exceeds %d characters", f.name, MaxIdentifierLength))
		}
	}
	if len(referenceExternalID) > MaxIdentifierLength {
		return invalid(CodeInvalidField, "referenceExternalTransactionId", "reference too long")
	}
	if kind == KindOpening {
		return invalid(CodeOpeningNotAllowed, "kind", "OPENING is reserved for internal wallet opening")
	}
	if _, err := ParseKind(string(kind)); err != nil {
		return err
	}
	if !m.IsInitialized() {
		return invalid(CodeInvalidMoney, "money", "money is required")
	}
	if m.IsNegative() {
		return invalid(CodeInvalidMoney, "money.amount", "amount cannot be negative")
	}
	switch kind {
	case KindLoss:
		if !m.IsZero() {
			return invalid(CodeLossAmountMustBeZero, "money.amount", `LOSS requires amount "0.00"`)
		}
	default:
		if !m.IsPositive() {
			return invalid(CodeAmountMustBePositive, "money.amount", string(kind)+" requires a positive amount")
		}
	}
	switch kind {
	case KindRefund, KindRollback:
		if referenceExternalID == "" {
			return invalid(CodeReferenceRequired, "referenceExternalTransactionId", string(kind)+" requires a reference")
		}
	case KindBet, KindLoss:
		if referenceExternalID != "" {
			return invalid(CodeReferenceNotAllowed, "referenceExternalTransactionId", string(kind)+" does not accept a reference")
		}
	}
	if referenceExternalID != "" && referenceExternalID == externalID {
		return invalid(CodeSelfReference, "referenceExternalTransactionId", "a transaction cannot reference itself")
	}
	return nil
}

// NewOpening creates the internal OPENING credit of a wallet as PENDING.
// Provider, external ids, idempotency key, hash, round, game and reference do
// not apply to this origin.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	switch {
	case id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil:
		return nil, fmt.Errorf("%w: opening requires id, walletId and playerId", ErrInvalidTransaction)
	case !amount.IsInitialized():
		return nil, fmt.Errorf("%w: %w", ErrInvalidTransaction, money.ErrUninitialized)
	case !amount.IsPositive():
		return nil, fmt.Errorf("%w: opening amount must be positive", ErrInvalidTransaction)
	case correlationID == "" || now.IsZero():
		return nil, fmt.Errorf("%w: opening requires correlation id and timestamp", ErrInvalidTransaction)
	}
	now = now.UTC()
	return &Transaction{
		id: id, origin: OriginInternal, kind: KindOpening, status: StatusPending,
		walletID: walletID, playerID: playerID, money: amount,
		correlationID: correlationID, createdAt: now, updatedAt: now,
	}, nil
}

// RehydrateParams is the persisted state of a transaction.
type RehydrateParams struct {
	ID                  uuid.UUID
	Origin              Origin
	Kind                Kind
	Status              Status
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	Money               money.Money
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	RoundID             string
	GameID              string
	ReferenceExternalID string
	ReferenceID         *uuid.UUID
	FailureCode         FailureCode
	ResultBalance       *money.Money
	ResultWalletVersion int64
	Attempts            int
	NextAttemptAt       *time.Time
	ExpiresAt           *time.Time
	CorrelationID       string
	CausationID         *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	CompletedAt         *time.Time
}

// Rehydrate rebuilds a persisted transaction without running transitions or
// emitting events. It only checks structural consistency.
func Rehydrate(p RehydrateParams) (*Transaction, error) {
	bad := func(msg string) (*Transaction, error) {
		return nil, fmt.Errorf("%w: rehydrate %s: %s", ErrInvalidTransaction, p.ID, msg)
	}
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return bad("missing ids")
	}
	if _, err := ParseKind(string(p.Kind)); err != nil {
		return bad(err.Error())
	}
	if _, err := ParseStatus(string(p.Status)); err != nil {
		return bad(err.Error())
	}
	if !p.Money.IsInitialized() || p.Money.IsNegative() {
		return bad("invalid money")
	}
	switch p.Origin {
	case OriginInternal:
		if p.Kind != KindOpening || p.ProviderID != "" || p.ExternalID != "" || p.IdempotencyKey != "" {
			return bad("internal origin must be an OPENING without external metadata")
		}
	case OriginExternal:
		if p.Kind == KindOpening || p.ProviderID == "" || p.ExternalID == "" || p.IdempotencyKey == "" || p.PayloadHash == "" {
			return bad("external origin requires provider metadata")
		}
	default:
		return bad("invalid origin")
	}
	if p.Status == StatusRejected && p.FailureCode == "" {
		return bad("rejected without failure code")
	}
	if p.Status == StatusProcessed && p.ResultBalance == nil {
		return bad("processed without result balance")
	}
	if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		return bad("missing timestamps")
	}
	return &Transaction{
		id: p.ID, origin: p.Origin, kind: p.Kind, status: p.Status, walletID: p.WalletID,
		playerID: p.PlayerID, money: p.Money, providerID: p.ProviderID, externalID: p.ExternalID,
		idempotencyKey: p.IdempotencyKey, payloadHash: p.PayloadHash, roundID: p.RoundID,
		gameID: p.GameID, referenceExternalID: p.ReferenceExternalID, referenceID: p.ReferenceID,
		failureCode: p.FailureCode, resultBalance: p.ResultBalance, resultWalletVersion: p.ResultWalletVersion,
		attempts: p.Attempts, nextAttemptAt: p.NextAttemptAt, expiresAt: p.ExpiresAt,
		correlationID: p.CorrelationID, causationID: p.CausationID,
		createdAt: p.CreatedAt.UTC(), updatedAt: p.UpdatedAt.UTC(), completedAt: p.CompletedAt,
	}, nil
}

func (t *Transaction) ID() uuid.UUID               { return t.id }
func (t *Transaction) Origin() Origin              { return t.origin }
func (t *Transaction) Kind() Kind                  { return t.kind }
func (t *Transaction) Status() Status              { return t.status }
func (t *Transaction) WalletID() uuid.UUID         { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID         { return t.playerID }
func (t *Transaction) Money() money.Money          { return t.money }
func (t *Transaction) ProviderID() string          { return t.providerID }
func (t *Transaction) ExternalID() string          { return t.externalID }
func (t *Transaction) IdempotencyKey() string      { return t.idempotencyKey }
func (t *Transaction) PayloadHash() string         { return t.payloadHash }
func (t *Transaction) RoundID() string             { return t.roundID }
func (t *Transaction) GameID() string              { return t.gameID }
func (t *Transaction) ReferenceExternalID() string { return t.referenceExternalID }
func (t *Transaction) ReferenceID() *uuid.UUID     { return t.referenceID }
func (t *Transaction) FailureCode() FailureCode    { return t.failureCode }
func (t *Transaction) ResultBalance() *money.Money { return t.resultBalance }
func (t *Transaction) ResultWalletVersion() int64  { return t.resultWalletVersion }
func (t *Transaction) Attempts() int               { return t.attempts }
func (t *Transaction) NextAttemptAt() *time.Time   { return t.nextAttemptAt }
func (t *Transaction) ExpiresAt() *time.Time       { return t.expiresAt }
func (t *Transaction) CorrelationID() string       { return t.correlationID }
func (t *Transaction) CausationID() *string        { return t.causationID }
func (t *Transaction) CreatedAt() time.Time        { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time        { return t.updatedAt }
func (t *Transaction) CompletedAt() *time.Time     { return t.completedAt }
func (t *Transaction) NeedsReference() bool        { return t.referenceExternalID != "" }

func (t *Transaction) transition(to Status, now time.Time) error {
	allowed := false
	switch t.status {
	case StatusPending:
		allowed = to != StatusPending
	case StatusPendingReference:
		allowed = to != StatusPending
	}
	if !allowed {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, to)
	}
	t.status = to
	t.updatedAt = now.UTC()
	if to.IsTerminal() {
		c := t.updatedAt
		t.completedAt = &c
		t.nextAttemptAt = nil
	}
	return nil
}

// MarkProcessed completes the operation, recording the balance returned to the
// provider (used verbatim by idempotent replays).
func (t *Transaction) MarkProcessed(balance money.Money, walletVersion int64, referenceID *uuid.UUID, now time.Time) error {
	if !balance.IsInitialized() || balance.IsNegative() {
		return fmt.Errorf("%w: invalid result balance", ErrInvalidTransaction)
	}
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	b := balance
	t.resultBalance = &b
	t.resultWalletVersion = walletVersion
	t.referenceID = referenceID
	return nil
}

// MarkRejected records a definitive business rejection. balance may be nil
// when exposing it would leak data (e.g. wallet of another player).
func (t *Transaction) MarkRejected(code FailureCode, balance *money.Money, referenceID *uuid.UUID, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code is required", ErrInvalidTransaction)
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	t.resultBalance = balance
	t.referenceID = referenceID
	return nil
}

// MarkFailed records a permanent infrastructure failure for auditing.
func (t *Transaction) MarkFailed(code FailureCode, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code is required", ErrInvalidTransaction)
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = code
	return nil
}

// MarkPendingReference parks the operation until its reference is available.
// When already waiting, it counts one more attempt and reschedules it.
func (t *Transaction) MarkPendingReference(nextAttemptAt, expiresAt time.Time, now time.Time) error {
	if !t.NeedsReference() {
		return fmt.Errorf("%w: %s has no reference to wait for", ErrInvalidTransition, t.kind)
	}
	first := t.status == StatusPending
	if err := t.transition(StatusPendingReference, now); err != nil {
		return err
	}
	if first {
		t.attempts = 1 // the synchronous evaluation counts as the first attempt
		e := expiresAt.UTC()
		t.expiresAt = &e
	} else {
		t.attempts++
	}
	n := nextAttemptAt.UTC()
	t.nextAttemptAt = &n
	return nil
}
