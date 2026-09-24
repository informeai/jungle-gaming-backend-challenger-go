package wagering

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

// Request holds the business fields of a provider operation, already parsed.
// It is the single input of the idempotency fingerprint for HTTP and SQS.
type Request struct {
	ProviderID          string
	ExternalID          string
	PlayerID            uuid.UUID
	WalletID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                Kind
	Money               money.Money
	ReferenceExternalID string
}

// Fingerprint is the SHA-256 (hex) of the canonical JSON of the business
// fields. Canonical form: object keys sorted lexicographically (encoding/json
// sorts map keys), no insignificant whitespace, UUIDs in lower-case canonical
// form, money as {"amount":"<canonical 2-decimal string>","currency":"<ISO>"},
// referenceExternalTransactionId omitted when empty. The idempotency key and
// transport metadata (headers, SQS envelope, messageId, occurredAt) are
// excluded, so the same operation hashes identically over HTTP and SQS.
func Fingerprint(r Request) string {
	doc := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]string{
			"amount":   r.Money.Amount(),
			"currency": r.Money.Currency().Code(),
		},
	}
	if r.ReferenceExternalID != "" {
		doc["referenceExternalTransactionId"] = r.ReferenceExternalID
	}
	b, err := json.Marshal(doc)
	if err != nil { // unreachable: only strings and maps of strings
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
