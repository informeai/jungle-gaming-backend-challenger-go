//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/auth"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

func TestAuthenticationWithKeycloak(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	w := openWallet(t, inst.Base, "100.00")
	op := w.op(uniq("auth-bet"), "BET", "10.00", "")

	valid := token(t, "provider-a")
	parts := strings.Split(valid, ".")
	tampered := parts[0] + "." + parts[1] + "." + strings.Repeat("A", len(parts[2]))

	for name, tok := range map[string]string{
		"missing":        "",
		"garbage":        "not-a-jwt",
		"bad signature":  tampered,
		"other audience": fetchToken(t, "other-api-client"),
	} {
		r := do(t, http.MethodPost, inst.Base+"/wagering/transactions", tok, op.payload(), map[string]string{"Idempotency-Key": op.key()})
		if r.Status != http.StatusUnauthorized {
			t.Errorf("%s: status %d %s", name, r.Status, r.Raw)
		}
	}
	// Expired: a real Keycloak token checked by the same verifier one hour later.
	cfg := config.OIDC{Issuer: oidcIssuer, Audience: "wallet-api", ProviderClaim: "provider_id", ProviderRole: "wagering-provider", InternalRole: "wallet-internal"}
	later, err := auth.NewVerifier(cfg, auth.Options{Now: func() time.Time { return time.Now().Add(time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := later.Authenticate(context.Background(), "Bearer "+valid); !errors.Is(err, auth.ErrExpiredToken) {
		t.Fatalf("expired token err = %v", err)
	}
	now, _ := auth.NewVerifier(cfg, auth.Options{})
	p, err := now.Authenticate(context.Background(), "Bearer "+valid)
	if err != nil || p.ProviderID != "provider-a" {
		t.Fatalf("valid keycloak token: %+v %v", p, err)
	}

	// None of the rejected calls produced a financial effect.
	if s := stateOf(t, w.ID); s.Balance != 10000 || s.LedgerEntries != 1 {
		t.Fatalf("unauthorized calls changed the wallet: %+v", s)
	}
	if st, _ := txStatus(t, "provider-a", op.External); st != "" {
		t.Fatalf("unauthorized call persisted a transaction: %s", st)
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	inst := startApp(t, httpConfig(t))
	w := openWallet(t, inst.Base, "100.00")
	internal, provA, provB, noRole := token(t, "wallet-internal"), token(t, "provider-a"), token(t, "provider-b"), token(t, "reporting-no-role")

	// Wallet operations are restricted to the internal service.
	for name, tok := range map[string]string{"provider-a": provA, "no-role": noRole} {
		if r := do(t, http.MethodPost, inst.Base+"/wallets", tok, map[string]any{"playerId": w.Player,
			"initialBalance": map[string]string{"amount": "1.00", "currency": "USD"}}, nil); r.Status != http.StatusForbidden {
			t.Errorf("%s POST /wallets: %d", name, r.Status)
		}
		for _, path := range []string{"/wallets/" + w.ID, "/wallets/" + w.ID + "/ledger"} {
			if r := do(t, http.MethodGet, inst.Base+path, tok, nil, nil); r.Status != http.StatusForbidden {
				t.Errorf("%s GET %s: %d", name, path, r.Status)
			}
		}
		if r := do(t, http.MethodPost, inst.Base+"/wallets/"+w.ID+"/reconciliation", tok, nil, nil); r.Status != http.StatusForbidden {
			t.Errorf("%s reconciliation: %d", name, r.Status)
		}
	}
	// The internal service cannot submit provider operations.
	op := w.op(uniq("iso-bet"), "BET", "10.00", "")
	if r := do(t, http.MethodPost, inst.Base+"/wagering/transactions", internal, op.payload(), map[string]string{"Idempotency-Key": op.key()}); r.Status != http.StatusForbidden {
		t.Errorf("internal submit: %d", r.Status)
	}
	// provider-b cannot act as provider-a (body naming another provider).
	if r := do(t, http.MethodPost, inst.Base+"/wagering/transactions", provB, op.payload(), map[string]string{"Idempotency-Key": op.key()}); r.Status != http.StatusForbidden || r.str("error", "code") != "PROVIDER_MISMATCH" {
		t.Errorf("provider-b as provider-a: %d %s", r.Status, r.Raw)
	}
	if s := stateOf(t, w.ID); s.LedgerEntries != 1 {
		t.Fatalf("forbidden calls produced effects: %+v", s)
	}

	// provider-a processes; provider-b cannot see it by id, by path, nor replay it.
	r := submit(t, inst.Base, op)
	if r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	txID := r.str("transactionId")
	if g := do(t, http.MethodGet, inst.Base+"/wagering/transactions/"+txID, provB, nil, nil); g.Status != http.StatusNotFound || strings.Contains(g.Raw, w.ID) {
		t.Errorf("provider-b GET by id: %d %s", g.Status, g.Raw)
	}
	if g := do(t, http.MethodGet, inst.Base+"/providers/provider-a/wagering/transactions/"+op.External, provB, nil, nil); g.Status != http.StatusForbidden {
		t.Errorf("provider-b GET by provider path: %d", g.Status)
	}
	if g := do(t, http.MethodGet, inst.Base+"/wagering/transactions/"+txID, provA, nil, nil); g.Status != http.StatusOK {
		t.Errorf("provider-a GET own: %d", g.Status)
	}
	if g := do(t, http.MethodGet, inst.Base+"/wagering/transactions/"+txID, internal, nil, nil); g.Status != http.StatusOK {
		t.Errorf("internal GET: %d", g.Status)
	}
	if g := do(t, http.MethodGet, inst.Base+"/wagering/transactions/"+txID, noRole, nil, nil); g.Status != http.StatusForbidden {
		t.Errorf("no-role GET: %d", g.Status)
	}

	// Replay isolation: provider-b reusing provider-a's key and external id
	// lives in its own namespace and never receives provider-a's result.
	opB := op
	opB.Provider = "provider-b"
	rb := do(t, http.MethodPost, inst.Base+"/wagering/transactions", provB, opB.payload(), map[string]string{"Idempotency-Key": op.key()})
	if rb.Status != http.StatusOK || rb.Body["idempotentReplay"] != false || rb.str("transactionId") == txID {
		t.Fatalf("provider-b with provider-a's key: %d %s", rb.Status, rb.Raw)
	}
	if s := stateOf(t, w.ID); s.Debits != 2 || s.Balance != 8000 {
		t.Fatalf("state %+v", s)
	}
}
