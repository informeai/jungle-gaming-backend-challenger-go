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

// TransientError marks failures worth retrying later (database or broker
// unavailable, open circuit breaker, timeouts, serialization failures).
// Conflict is true only for serialization failures and deadlocks: those are
// the ones worth retrying immediately inside the same request. Retrying a
// connection failure right away would just multiply load on a dependency
// that is already struggling; the circuit breaker handles those.
type TransientError struct {
	Err      error
	Conflict bool
}

func (e *TransientError) Error() string { return "transient: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// IsTransient reports whether err is (or wraps) a TransientError.
func IsTransient(err error) bool {
	var t *TransientError
	return errors.As(err, &t)
}

// IsRetryableConflict reports whether err is a concurrency conflict worth an
// immediate retry of the whole SQL transaction.
func IsRetryableConflict(err error) bool {
	if errors.Is(err, ErrConcurrentUpdate) {
		return true
	}
	var t *TransientError
	return errors.As(err, &t) && t.Conflict
}
