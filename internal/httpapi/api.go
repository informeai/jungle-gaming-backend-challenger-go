// Package httpapi exposes the HTTP contract of the service on net/http.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/app"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/auth"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/contract"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/observability"
)

const maxBodyBytes = 64 << 10

// ReadinessCheck reports whether a dependency is usable.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

type API struct {
	wallets  *app.WalletService
	wagering *app.WageringService
	verifier *auth.Verifier
	metrics  *observability.Metrics
	checks   []ReadinessCheck
	log      *slog.Logger
	timeout  time.Duration
	draining atomic.Bool
}

func NewAPI(wallets *app.WalletService, wagering *app.WageringService, verifier *auth.Verifier,
	metrics *observability.Metrics, checks []ReadinessCheck, requestTimeout time.Duration, log *slog.Logger) *API {
	return &API{wallets: wallets, wagering: wagering, verifier: verifier, metrics: metrics,
		checks: checks, timeout: requestTimeout, log: log.With(slog.String("component", "http"))}
}

// Drain makes readiness fail so load balancers stop routing new requests.
func (a *API) Drain() { a.draining.Store(true) }

// Handler builds the router. Health and metrics are always served; business
// routes only when the API component is enabled (a verifier was provided).
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, a.observe(pattern, h))
	}
	route("GET /health/live", a.live)
	route("GET /health/ready", a.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.metrics.Registry, promhttp.HandlerOpts{}))
	if a.verifier == nil {
		// Worker-only component: no business routes and no token handling.
		return mux
	}

	route("POST /wallets", a.internalOnly(a.openWallet))
	route("GET /wallets/{walletId}", a.internalOnly(a.getWallet))
	route("GET /wallets/{walletId}/ledger", a.internalOnly(a.getLedger))
	route("POST /wallets/{walletId}/reconciliation", a.internalOnly(a.reconcile))

	route("POST /wagering/transactions", a.providerOnly(a.submit))
	route("GET /wagering/transactions/{transactionId}", a.authenticated(a.getTransaction))
	route("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", a.authenticated(a.getProviderTransaction))
	return mux
}

type ctxKey int

const correlationKey ctxKey = iota

func correlationID(ctx context.Context) string {
	s, _ := ctx.Value(correlationKey).(string)
	return s
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// observe assigns the correlation id, applies the request timeout and emits
// an access log and metrics. Bodies are never logged.
func (a *API) observe(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cid := r.Header.Get("X-Correlation-Id")
		if cid == "" || len(cid) > 128 {
			cid = app.NewUUIDv7().String()
		}
		w.Header().Set("X-Correlation-Id", cid)
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), correlationKey, cid), a.timeout)
		defer cancel()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))
		a.metrics.HTTPRequest(route, rec.status)
		if route != "GET /health/live" && route != "GET /health/ready" {
			a.log.Info("http request", slog.String("route", route), slog.Int("status", rec.status),
				slog.String("correlationId", cid), slog.Duration("duration", time.Since(start)))
		}
	})
}

func (a *API) authenticate(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	p, err := a.verifier.Authenticate(r.Context(), r.Header.Get("Authorization"))
	if err != nil {
		code := "UNAUTHENTICATED"
		if errors.Is(err, auth.ErrExpiredToken) {
			code = "TOKEN_EXPIRED"
		}
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		writeError(w, r, http.StatusUnauthorized, code, "missing, invalid or expired access token", "")
		return auth.Principal{}, false
	}
	return p, true
}

func (a *API) authenticated(next func(http.ResponseWriter, *http.Request, auth.Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, ok := a.authenticate(w, r)
		if !ok {
			return
		}
		if !a.verifier.Policy.IsInternal(p) && !a.verifier.Policy.IsProvider(p) {
			writeError(w, r, http.StatusForbidden, "FORBIDDEN", "caller has no wallet role", "")
			return
		}
		next(w, r.WithContext(auth.WithPrincipal(r.Context(), p)), p)
	}
}

func (a *API) internalOnly(next http.HandlerFunc) http.HandlerFunc {
	return a.authenticated(func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		if !a.verifier.Policy.IsInternal(p) {
			writeError(w, r, http.StatusForbidden, "FORBIDDEN", "wallet operations are restricted to the internal service", "")
			return
		}
		next(w, r)
	})
}

func (a *API) providerOnly(next func(http.ResponseWriter, *http.Request, auth.Principal)) http.HandlerFunc {
	return a.authenticated(func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		if !a.verifier.Policy.IsProvider(p) {
			writeError(w, r, http.StatusForbidden, "FORBIDDEN", "only game providers can submit wager operations", "")
			return
		}
		next(w, r, p)
	})
}

func (a *API) live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	status, code := "UP", http.StatusOK
	deps := map[string]string{}
	if a.draining.Load() {
		status, code = "DRAINING", http.StatusServiceUnavailable
	}
	for _, c := range a.checks {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := c.Check(ctx)
		cancel()
		if err != nil {
			deps[c.Name] = "DOWN"
			status, code = "DOWN", http.StatusServiceUnavailable
			continue
		}
		deps[c.Name] = "UP"
	}
	writeJSON(w, code, map[string]any{"status": status, "dependencies": deps})
}

// ---- wallets ----

type walletResponse struct {
	ID       uuid.UUID      `json:"id"`
	PlayerID uuid.UUID      `json:"playerId"`
	Balance  contract.Money `json:"balance"`
	Version  int64          `json:"version"`
}

func walletView(w *wallet.Wallet) walletResponse {
	return walletResponse{ID: w.ID(), PlayerID: w.PlayerID(), Balance: contract.MoneyOf(w.Balance()), Version: w.Version()}
}

func (a *API) openWallet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PlayerID       string          `json:"playerId"`
		InitialBalance *contract.Money `json:"initialBalance"`
	}
	if err := contract.Decode(http.MaxBytesReader(w, r.Body, maxBodyBytes), &body); err != nil {
		a.writeAppError(w, r, err)
		return
	}
	playerID, err := app.ParseUUID("playerId", body.PlayerID)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	if body.InitialBalance == nil {
		writeError(w, r, http.StatusBadRequest, wagering.CodeInvalidMoney, "initialBalance is required", "initialBalance")
		return
	}
	initial, err := app.ParseMoneyInput("initialBalance", body.InitialBalance.Amount, body.InitialBalance.Currency)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	wl, err := a.wallets.Open(r.Context(), app.OpenWalletCommand{PlayerID: playerID, Initial: initial, CorrelationID: correlationID(r.Context())})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, walletView(wl))
}

func (a *API) walletID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := app.ParseUUID("walletId", r.PathValue("walletId"))
	if err != nil {
		a.writeAppError(w, r, err)
		return uuid.Nil, false
	}
	return id, true
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	wl, err := a.wallets.Get(r.Context(), id)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, walletView(wl))
}

type ledgerEntryResponse struct {
	ID            uuid.UUID      `json:"id"`
	WalletID      uuid.UUID      `json:"walletId"`
	TransactionID uuid.UUID      `json:"transactionId"`
	Direction     string         `json:"direction"`
	Money         contract.Money `json:"money"`
	BalanceBefore contract.Money `json:"balanceBefore"`
	BalanceAfter  contract.Money `json:"balanceAfter"`
	CreatedAt     string         `json:"createdAt"`
}

func (a *API) getLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	limit := app.DefaultLedgerLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > app.MaxLedgerLimit {
			writeError(w, r, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 200", "limit")
			return
		}
		limit = n
	}
	page, err := a.wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	items := make([]ledgerEntryResponse, 0, len(page.Entries))
	for _, e := range page.Entries {
		items = append(items, ledgerEntryResponse{
			ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()),
			Money: contract.MoneyOf(e.Amount()), BalanceBefore: contract.MoneyOf(e.BalanceBefore()),
			BalanceAfter: contract.MoneyOf(e.BalanceAfter()), CreatedAt: rfc3339(e.CreatedAt()),
		})
	}
	var next *string
	if page.NextCursor != "" {
		next = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "nextCursor": next})
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	rec, err := a.wallets.Reconcile(r.Context(), id)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":          rec.WalletID,
		"storedBalance":     contract.MoneyOf(rec.StoredBalance),
		"calculatedBalance": contract.MoneyOf(rec.CalculatedBalance),
		"difference":        contract.MoneyOf(rec.Difference),
		"consistent":        rec.Consistent,
		"checkedEntries":    rec.CheckedEntries,
	})
}

// ---- wagering ----

type submitResponse struct {
	TransactionID    uuid.UUID       `json:"transactionId"`
	Status           string          `json:"status"`
	Balance          *contract.Money `json:"balance,omitempty"`
	FailureCode      string          `json:"failureCode,omitempty"`
	IdempotentReplay bool            `json:"idempotentReplay"`
}

// statusCode maps a persisted outcome to HTTP: 200 PROCESSED, 202 pending,
// 422 REJECTED/FAILED (the body carries the persisted status and code).
func statusCode(s wagering.Status) int {
	switch s {
	case wagering.StatusProcessed:
		return http.StatusOK
	case wagering.StatusPending, wagering.StatusPendingReference:
		return http.StatusAccepted
	}
	return http.StatusUnprocessableEntity
}

func (a *API) submit(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var body contract.WagerPayload
	if err := contract.Decode(http.MaxBytesReader(w, r.Body, maxBodyBytes), &body); err != nil {
		a.writeAppError(w, r, err)
		return
	}
	// The authenticated identity decides the provider: no effect is produced
	// for a body naming another provider.
	if body.ProviderID != p.ProviderID {
		writeError(w, r, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider", "providerId")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required", "Idempotency-Key")
		return
	}
	cmd, err := app.ParseSubmit(body.Input(key, correlationID(r.Context()), nil))
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	res, err := a.wagering.Submit(r.Context(), cmd)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	t := res.Tx
	out := submitResponse{TransactionID: t.ID(), Status: string(t.Status()), FailureCode: string(t.FailureCode()), IdempotentReplay: res.Replay}
	if b := t.ResultBalance(); b != nil {
		m := contract.MoneyOf(*b)
		out.Balance = &m
	}
	writeJSON(w, statusCode(t.Status()), out)
}

type transactionResponse struct {
	TransactionID                  uuid.UUID       `json:"transactionId"`
	Origin                         string          `json:"origin"`
	Kind                           string          `json:"kind"`
	Status                         string          `json:"status"`
	ProviderID                     string          `json:"providerId,omitempty"`
	ExternalTransactionID          string          `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID       `json:"walletId"`
	PlayerID                       uuid.UUID       `json:"playerId"`
	RoundID                        string          `json:"roundId,omitempty"`
	GameID                         string          `json:"gameId,omitempty"`
	Money                          contract.Money  `json:"money"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID      `json:"referenceTransactionId,omitempty"`
	FailureCode                    string          `json:"failureCode,omitempty"`
	Balance                        *contract.Money `json:"balance,omitempty"`
	Attempts                       int             `json:"attempts"`
	NextAttemptAt                  *string         `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      *string         `json:"expiresAt,omitempty"`
	CreatedAt                      string          `json:"createdAt"`
	UpdatedAt                      string          `json:"updatedAt"`
	CompletedAt                    *string         `json:"completedAt,omitempty"`
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func ts(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func transactionView(t *wagering.Transaction) transactionResponse {
	out := transactionResponse{
		TransactionID: t.ID(), Origin: string(t.Origin()), Kind: string(t.Kind()), Status: string(t.Status()),
		ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(), WalletID: t.WalletID(),
		PlayerID: t.PlayerID(), RoundID: t.RoundID(), GameID: t.GameID(), Money: contract.MoneyOf(t.Money()),
		ReferenceExternalTransactionID: t.ReferenceExternalID(), ReferenceTransactionID: t.ReferenceID(),
		FailureCode: string(t.FailureCode()), Attempts: t.Attempts(), NextAttemptAt: ts(t.NextAttemptAt()),
		ExpiresAt: ts(t.ExpiresAt()), CreatedAt: rfc3339(t.CreatedAt()), UpdatedAt: rfc3339(t.UpdatedAt()),
		CompletedAt: ts(t.CompletedAt()),
	}
	if b := t.ResultBalance(); b != nil {
		m := contract.MoneyOf(*b)
		out.Balance = &m
	}
	return out
}

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id, err := app.ParseUUID("transactionId", r.PathValue("transactionId"))
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	t, err := a.wagering.Get(r.Context(), id)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	// Providers only see their own transactions; others look nonexistent.
	if !a.verifier.Policy.IsInternal(p) && t.ProviderID() != p.ProviderID {
		a.writeAppError(w, r, app.ErrTransactionNotFound)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}

func (a *API) getProviderTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	providerID := r.PathValue("providerId")
	if !a.verifier.Policy.IsInternal(p) && providerID != p.ProviderID {
		writeError(w, r, http.StatusForbidden, "PROVIDER_MISMATCH", "providers can only read their own transactions", "providerId")
		return
	}
	t, err := a.wagering.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}
