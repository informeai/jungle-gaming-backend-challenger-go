// Package wagering models wager transactions, their state machine and the
// settlement rules that move a wallet. It is independent of persistence,
// transport and DI frameworks.
package wagering

import (
	"errors"
	"fmt"
)

// Kind of a wager transaction.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseKind parses any kind, including the internal OPENING.
func ParseKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	}
	return "", invalid(CodeInvalidKind, "kind", fmt.Sprintf("unknown kind %q", s))
}

// ParseExternalKind parses kinds accepted from providers (HTTP and SQS).
func ParseExternalKind(s string) (Kind, error) {
	k, err := ParseKind(s)
	if err != nil {
		return "", err
	}
	if k == KindOpening {
		return "", invalid(CodeOpeningNotAllowed, "kind", "OPENING is reserved for internal wallet opening")
	}
	return k, nil
}

func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// Origin distinguishes internal (OPENING) from provider operations.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// Status of a wager transaction.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return st, nil
	}
	return "", fmt.Errorf("%w: status %q", ErrInvalidTransaction, s)
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// FailureCode is a stable, documented code for rejections and failures.
type FailureCode string

// Definitive business rejections (persisted as REJECTED; resending the same
// operation replays the rejection).
const (
	FailInsufficientFunds            FailureCode = "INSUFFICIENT_FUNDS"
	FailInsufficientFundsForReversal FailureCode = "INSUFFICIENT_FUNDS_FOR_REVERSAL"
	FailWalletPlayerMismatch         FailureCode = "WALLET_PLAYER_MISMATCH"
	FailCurrencyMismatch             FailureCode = "CURRENCY_MISMATCH"
	FailReferenceNotFound            FailureCode = "REFERENCE_NOT_FOUND"
	FailReferenceStillPending        FailureCode = "REFERENCE_STILL_PENDING"
	FailReferenceNotProcessed        FailureCode = "REFERENCE_NOT_PROCESSED"
	FailReferenceMismatch            FailureCode = "REFERENCE_MISMATCH"
	FailReferenceAmountMismatch      FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailReferenceKindNotAllowed      FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	FailReferenceAlreadyReversed     FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailAmountOverflow               FailureCode = "AMOUNT_OVERFLOW"
)

// Permanent infrastructure failure (persisted as FAILED for auditing).
const FailPermanentProcessingError FailureCode = "PERMANENT_PROCESSING_ERROR"

// Validation codes: correctable input errors, never persisted.
const (
	CodeInvalidKind          = "INVALID_KIND"
	CodeOpeningNotAllowed    = "OPENING_NOT_ALLOWED"
	CodeInvalidMoney         = "INVALID_MONEY"
	CodeAmountMustBePositive = "AMOUNT_MUST_BE_POSITIVE"
	CodeLossAmountMustBeZero = "LOSS_AMOUNT_MUST_BE_ZERO"
	CodeReferenceRequired    = "REFERENCE_REQUIRED"
	CodeReferenceNotAllowed  = "REFERENCE_NOT_ALLOWED"
	CodeSelfReference        = "SELF_REFERENCE"
	CodeMissingField         = "MISSING_FIELD"
	CodeInvalidField         = "INVALID_FIELD"
)

var (
	// ErrValidation is matched by every *ValidationError.
	ErrValidation         = errors.New("wagering: validation error")
	ErrInvalidTransaction = errors.New("wagering: invalid transaction")
	ErrInvalidTransition  = errors.New("wagering: invalid state transition")
)

// ValidationError describes a correctable input error.
type ValidationError struct {
	Code    string
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("wagering: %s (%s): %s", e.Code, e.Field, e.Message)
}

func (e *ValidationError) Is(target error) bool { return target == ErrValidation }

func invalid(code, field, msg string) error {
	return &ValidationError{Code: code, Field: field, Message: msg}
}
