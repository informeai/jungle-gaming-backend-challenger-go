package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

const issuer = "http://idp.test/realms/jungle"

var cfg = config.OIDC{Issuer: issuer, Audience: "wallet-api", ProviderClaim: "provider_id", ProviderRole: "wagering-provider", InternalRole: "wallet-internal"}

func sign(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(claims)
	obj, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	s, err := obj.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func claims(overrides map[string]any) map[string]any {
	c := map[string]any{
		"iss": issuer, "aud": "wallet-api", "sub": "svc", "azp": "provider-a",
		"exp": time.Now().Add(5 * time.Minute).Unix(), "iat": time.Now().Unix(),
		"provider_id":  "provider-a",
		"realm_access": map[string]any{"roles": []string{"wagering-provider"}},
	}
	for k, v := range overrides {
		if v == nil {
			delete(c, k)
			continue
		}
		c[k] = v
	}
	return c
}

func TestAuthenticate(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	v, err := NewVerifier(cfg, Options{KeySet: &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{key.Public()}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	p, err := v.Authenticate(ctx, "Bearer "+sign(t, key, claims(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if p.ProviderID != "provider-a" || !v.Policy.IsProvider(p) || v.Policy.IsInternal(p) {
		t.Fatalf("principal %+v", p)
	}

	cases := map[string]struct {
		header string
		want   error
	}{
		"missing":         {"", ErrMissingToken},
		"not bearer":      {"Basic abc", ErrInvalidToken},
		"garbage":         {"Bearer not.a.jwt", ErrInvalidToken},
		"expired":         {"Bearer " + sign(t, key, claims(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})), ErrExpiredToken},
		"wrong signature": {"Bearer " + sign(t, other, claims(nil)), ErrInvalidToken},
		"wrong audience":  {"Bearer " + sign(t, key, claims(map[string]any{"aud": "account"})), ErrInvalidToken},
		"wrong issuer":    {"Bearer " + sign(t, key, claims(map[string]any{"iss": "http://evil/realms/jungle"})), ErrInvalidToken},
	}
	for name, c := range cases {
		if _, err := v.Authenticate(ctx, c.header); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}

	// A provider role without the provider claim does not grant provider access.
	p, err = v.Authenticate(ctx, "Bearer "+sign(t, key, claims(map[string]any{"provider_id": nil})))
	if err != nil || v.Policy.IsProvider(p) {
		t.Fatalf("role without providerId: %+v %v", p, err)
	}
	internal, err := v.Authenticate(ctx, "Bearer "+sign(t, key, claims(map[string]any{
		"provider_id": nil, "realm_access": map[string]any{"roles": []string{"wallet-internal"}},
	})))
	if err != nil || !v.Policy.IsInternal(internal) || v.Policy.IsProvider(internal) {
		t.Fatalf("internal: %+v %v", internal, err)
	}
}
