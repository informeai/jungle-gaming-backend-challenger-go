package app

import "errors"

var (
	ErrWalletNotFound       = errors.New("wallet not found")
	ErrWalletAlreadyExists  = errors.New("wallet already exists for player and currency")
	ErrTransactionNotFound  = errors.New("transaction not found")
	ErrIdempotencyConflict  = errors.New("idempotency key reused with a different payload")
	ErrExternalIDConflict   = errors.New("external transaction already registered with another idempotency key")
	ErrInboxPayloadMismatch = errors.New("message redelivered with a different payload hash")
	ErrConcurrentUpdate     = errors.New("concurrent wallet update")
	ErrInvalidCursor        = errors.New("invalid cursor")
)

// TransientError marks failures worth retrying (database or broker
// unavailable, serialization failures, timeouts).
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return "transient: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// IsTransient reports whether err is (or wraps) a TransientError.
func IsTransient(err error) bool {
	var t *TransientError
	return errors.As(err, &t)
}
