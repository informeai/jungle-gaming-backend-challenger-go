package app

import (
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
)

// SubmitInput is the raw provider operation as received by any transport.
// HTTP and SQS both build it and go through ParseSubmit, so validation,
// normalisation and the idempotency fingerprint are identical.
type SubmitInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         *string
	Currency                       string
	ReferenceExternalTransactionID string
	CorrelationID                  string
	CausationID                    *string
}

// SubmitCommand is a validated provider operation.
type SubmitCommand struct {
	Request        wagering.Request
	IdempotencyKey string
	PayloadHash    string
	CorrelationID  string
	CausationID    *string
}

func validation(code, field, msg string) error {
	return &wagering.ValidationError{Code: code, Field: field, Message: msg}
}

// ParseUUID accepts only the canonical 36-character textual form.
func ParseUUID(field, s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.Nil, validation(wagering.CodeMissingField, field, field+" is required")
	}
	id, err := uuid.Parse(s)
	if err != nil || len(s) != 36 || id == uuid.Nil {
		return uuid.Nil, validation(wagering.CodeInvalidField, field, field+" must be a canonical UUID")
	}
	return id, nil
}

// ParseMoneyInput parses an external, non-negative amount.
func ParseMoneyInput(field string, amount *string, currency string) (money.Money, error) {
	if amount == nil {
		return money.Money{}, validation(wagering.CodeInvalidMoney, field+".amount", "amount is required and must be a string")
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return money.Money{}, validation(wagering.CodeInvalidMoney, field+".currency", err.Error())
	}
	m, err := money.ParseNonNegative(*amount, c)
	if err != nil {
		return money.Money{}, validation(wagering.CodeInvalidMoney, field+".amount", err.Error())
	}
	return m, nil
}

// ParseSubmit validates the input and computes the payload fingerprint.
func ParseSubmit(in SubmitInput) (SubmitCommand, error) {
	key := in.IdempotencyKey
	if strings.TrimSpace(key) == "" {
		return SubmitCommand{}, validation(wagering.CodeMissingField, "idempotencyKey", "idempotency key is required")
	}
	if len(key) > 2*wagering.MaxIdentifierLength+1 {
		return SubmitCommand{}, validation(wagering.CodeInvalidField, "idempotencyKey", "idempotency key too long")
	}
	playerID, err := ParseUUID("playerId", in.PlayerID)
	if err != nil {
		return SubmitCommand{}, err
	}
	walletID, err := ParseUUID("walletId", in.WalletID)
	if err != nil {
		return SubmitCommand{}, err
	}
	if in.Kind == "" {
		return SubmitCommand{}, validation(wagering.CodeMissingField, "kind", "kind is required")
	}
	kind, err := wagering.ParseExternalKind(in.Kind)
	if err != nil {
		return SubmitCommand{}, err
	}
	m, err := ParseMoneyInput("money", in.Amount, in.Currency)
	if err != nil {
		return SubmitCommand{}, err
	}
	if err := wagering.ValidateExternal(in.ProviderID, in.ExternalTransactionID, in.RoundID, in.GameID, kind, m, in.ReferenceExternalTransactionID); err != nil {
		return SubmitCommand{}, err
	}
	req := wagering.Request{
		ProviderID: in.ProviderID, ExternalID: in.ExternalTransactionID, PlayerID: playerID,
		WalletID: walletID, RoundID: in.RoundID, GameID: in.GameID, Kind: kind, Money: m,
		ReferenceExternalID: in.ReferenceExternalTransactionID,
	}
	correlation := in.CorrelationID
	if correlation == "" {
		correlation = NewUUIDv7().String()
	}
	return SubmitCommand{
		Request: req, IdempotencyKey: key, PayloadHash: wagering.Fingerprint(req),
		CorrelationID: correlation, CausationID: in.CausationID,
	}, nil
}

// IsValidation reports whether err is a correctable input error.
func IsValidation(err error) bool { return errors.Is(err, wagering.ErrValidation) }
