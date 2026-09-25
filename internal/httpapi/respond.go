package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/contract"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/resilience"
)

// ErrorBody is the body of every non-2xx response that is not a persisted
// transaction outcome.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	Field         string `json:"field,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg, field string) {
	writeJSON(w, status, ErrorBody{Error: ErrorDetail{Code: code, Message: msg, Field: field, CorrelationID: correlationID(r.Context())}})
}

// writeAppError maps application errors to the documented HTTP contract.
func (a *API) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, contract.ErrMalformed):
		writeError(w, r, http.StatusBadRequest, "MALFORMED_REQUEST", err.Error(), "")
	case app.IsValidation(err):
		var v *wagering.ValidationError
		errors.As(err, &v)
		writeError(w, r, http.StatusBadRequest, v.Code, v.Message, v.Field)
	case errors.Is(err, app.ErrInvalidCursor):
		writeError(w, r, http.StatusBadRequest, "INVALID_CURSOR", err.Error(), "cursor")
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, r, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found", "walletId")
	case errors.Is(err, app.ErrTransactionNotFound):
		writeError(w, r, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transaction not found", "")
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeError(w, r, http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error(), "playerId")
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", err.Error(), "Idempotency-Key")
	case errors.Is(err, app.ErrExternalIDConflict):
		writeError(w, r, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", err.Error(), "externalTransactionId")
	case errors.Is(err, resilience.ErrOpen):
		// Circuit open: answered without touching the dependency.
		w.Header().Set("Retry-After", strconv.Itoa(max(1, resilience.RetryAfterSeconds(err))))
		writeError(w, r, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "dependency unavailable (circuit open), retry with the same Idempotency-Key", "")
	case app.IsTransient(err), errors.Is(err, app.ErrConcurrentUpdate):
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporarily unavailable, retry with the same Idempotency-Key", "")
	default:
		a.log.Error("unexpected error", slog.String("correlationId", correlationID(r.Context())), slog.String("error", err.Error()))
		writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error", "")
	}
}
