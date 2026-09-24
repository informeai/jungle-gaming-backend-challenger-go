// Package auth validates OAuth 2.0 / OIDC access tokens issued by the
// external IdP (Keycloak) and maps them to a Principal.
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/config"
)

var (
	ErrMissingToken = errors.New("auth: missing bearer token")
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrExpiredToken = errors.New("auth: expired token")
)

// Principal is the authenticated caller.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string // bound by the IdP; empty for internal services
	Roles      []string
}

func (p Principal) HasRole(r string) bool { return slices.Contains(p.Roles, r) }

// Policy holds the role names that grant access.
type Policy struct {
	ProviderRole string
	InternalRole string
}

// IsProvider: a game provider bound to exactly one providerId.
func (p Policy) IsProvider(pr Principal) bool {
	return pr.HasRole(p.ProviderRole) && pr.ProviderID != ""
}

// IsInternal: the internal wallet service.
func (p Policy) IsInternal(pr Principal) bool { return pr.HasRole(p.InternalRole) }

// Verifier validates signature (JWKS), issuer, audience and expiry.
type Verifier struct {
	verifier      *oidc.IDTokenVerifier
	providerClaim string
	Policy        Policy
}

// Options allow tests to shift the clock or inject a static key set.
type Options struct {
	Now    func() time.Time
	KeySet oidc.KeySet
}

func NewVerifier(cfg config.OIDC, opts Options) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("auth: issuer and audience are required")
	}
	keySet := opts.KeySet
	if keySet == nil {
		jwks := cfg.JWKSURL
		if jwks == "" {
			jwks = strings.TrimSuffix(cfg.Issuer, "/") + "/protocol/openid-connect/certs"
		}
		keySet = oidc.NewRemoteKeySet(context.Background(), jwks)
	}
	v := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256, oidc.PS256},
		Now:                  opts.Now,
	})
	return &Verifier{
		verifier: v, providerClaim: cfg.ProviderClaim,
		Policy: Policy{ProviderRole: cfg.ProviderRole, InternalRole: cfg.InternalRole},
	}, nil
}

// Authenticate parses the Authorization header value and verifies the token.
func (v *Verifier) Authenticate(ctx context.Context, header string) (Principal, error) {
	scheme, raw, ok := strings.Cut(strings.TrimSpace(header), " ")
	if header == "" {
		return Principal{}, ErrMissingToken
	}
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(raw) == "" {
		return Principal{}, ErrInvalidToken
	}
	tok, err := v.verifier.Verify(ctx, strings.TrimSpace(raw))
	if err != nil {
		var expired *oidc.TokenExpiredError
		if errors.As(err, &expired) {
			return Principal{}, ErrExpiredToken
		}
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var claims map[string]any
	if err := tok.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	p := Principal{Subject: tok.Subject}
	p.ClientID, _ = claims["azp"].(string)
	p.ProviderID, _ = claims[v.providerClaim].(string)
	if ra, ok := claims["realm_access"].(map[string]any); ok {
		if roles, ok := ra["roles"].([]any); ok {
			for _, r := range roles {
				if s, ok := r.(string); ok {
					p.Roles = append(p.Roles, s)
				}
			}
		}
	}
	return p, nil
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
