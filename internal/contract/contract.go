// Package contract defines the wire formats shared by the HTTP API and the
// SQS consumer, so both transports decode operations identically.
package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
)

// Money is the external money representation. Amount is *string so that a
// JSON number is a decode error instead of being coerced (never a float).
type Money struct {
	Amount   *string `json:"amount"`
	Currency string  `json:"currency"`
}

func MoneyOf(m money.Money) Money {
	a := m.Amount()
	return Money{Amount: &a, Currency: m.Currency().Code()}
}

// WagerPayload holds the business fields of a provider operation.
type WagerPayload struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Money                          *Money `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// Input converts the payload into the use-case input.
func (p WagerPayload) Input(idempotencyKey, correlationID string, causationID *string) app.SubmitInput {
	in := app.SubmitInput{
		ProviderID: p.ProviderID, ExternalTransactionID: p.ExternalTransactionID, IdempotencyKey: idempotencyKey,
		PlayerID: p.PlayerID, WalletID: p.WalletID, RoundID: p.RoundID, GameID: p.GameID, Kind: p.Kind,
		ReferenceExternalTransactionID: p.ReferenceExternalTransactionID,
		CorrelationID:                  correlationID, CausationID: causationID,
	}
	if p.Money != nil {
		in.Amount, in.Currency = p.Money.Amount, p.Money.Currency
	}
	return in
}

// MessageTypeWagerRequested is the only accepted ingress message type.
const MessageTypeWagerRequested = "WagerTransactionRequested"

// MessageData is the data of a WagerTransactionRequested message.
type MessageData struct {
	WagerPayload
	IdempotencyKey string `json:"idempotencyKey"`
}

// Envelope is the ingress SQS message body.
type Envelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurredAt"`
	Data       MessageData `json:"data"`
}

var ErrMalformed = errors.New("malformed payload")

// Decode strictly decodes a single JSON document into v (unknown fields and
// trailing data are rejected).
func Decode(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data", ErrMalformed)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing data", ErrMalformed)
	}
	return nil
}

// DecodeEnvelope decodes and validates an ingress message body.
func DecodeEnvelope(body string) (Envelope, error) {
	var e Envelope
	if err := Decode(bytes.NewBufferString(body), &e); err != nil {
		return Envelope{}, err
	}
	if e.MessageID == "" || len(e.MessageID) > 128 {
		return Envelope{}, fmt.Errorf("%w: messageId is required (max 128 chars)", ErrMalformed)
	}
	if e.Type != MessageTypeWagerRequested {
		return Envelope{}, fmt.Errorf("%w: unsupported type %q", ErrMalformed, e.Type)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err != nil {
		return Envelope{}, fmt.Errorf("%w: occurredAt must be RFC 3339", ErrMalformed)
	}
	return e, nil
}

// ValidationBody is returned for correctable input errors.
func ValidationCode(err error) (code, field string) {
	var v *wagering.ValidationError
	if errors.As(err, &v) {
		return v.Code, v.Field
	}
	return "INVALID_REQUEST", ""
}
