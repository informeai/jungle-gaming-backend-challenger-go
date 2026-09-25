package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/events"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wagering"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

// In-memory fakes: enough to exercise the use-case orchestration. The real
// guarantees (locks, constraints, atomicity) are covered by the integration
// tests against PostgreSQL.

type fakeTx struct{ mu sync.Mutex }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fn(ctx)
}
func (f *fakeTx) WithinSnapshot(ctx context.Context, fn func(context.Context) error) error {
	return f.WithinTx(ctx, fn)
}

type fakeWallets struct {
	w      map[uuid.UUID]*wallet.Wallet
	ledger []*wallet.LedgerEntry
}

func (f *fakeWallets) Insert(_ context.Context, w *wallet.Wallet) error {
	for _, x := range f.w {
		if x.PlayerID() == w.PlayerID() && x.Currency() == w.Currency() {
			return ErrWalletAlreadyExists
		}
	}
	f.w[w.ID()] = w
	return nil
}
func (f *fakeWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return f.w[id], nil
}
func (f *fakeWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return f.Get(ctx, id)
}
func (f *fakeWallets) Update(context.Context, *wallet.Wallet, int64) error { return nil }
func (f *fakeWallets) InsertLedgerEntry(_ context.Context, e *wallet.LedgerEntry) error {
	f.ledger = append(f.ledger, e)
	return nil
}
func (f *fakeWallets) ListLedger(context.Context, uuid.UUID, int64, int) ([]LedgerRow, error) {
	return nil, nil
}
func (f *fakeWallets) LedgerTotals(_ context.Context, id uuid.UUID, c money.Currency) (LedgerTotals, error) {
	total, _ := money.Zero(c)
	var n int64
	for _, e := range f.ledger {
		if e.WalletID() != id {
			continue
		}
		n++
		if e.Direction() == wallet.Credit {
			total, _ = total.Add(e.Amount())
		} else {
			total, _ = total.Sub(e.Amount())
		}
	}
	return LedgerTotals{Balance: total, Entries: n}, nil
}

type fakeTxs struct {
	byID map[uuid.UUID]*wagering.Transaction
}

func (f *fakeTxs) find(match func(*wagering.Transaction) bool) *wagering.Transaction {
	for _, t := range f.byID {
		if match(t) {
			return t
		}
	}
	return nil
}
func (f *fakeTxs) InsertIfAbsent(_ context.Context, t *wagering.Transaction) (bool, error) {
	if f.find(func(x *wagering.Transaction) bool {
		return x.ProviderID() == t.ProviderID() && (x.IdempotencyKey() == t.IdempotencyKey() || x.ExternalID() == t.ExternalID())
	}) != nil {
		return false, nil
	}
	f.byID[t.ID()] = t
	return true, nil
}
func (f *fakeTxs) Insert(_ context.Context, t *wagering.Transaction) error {
	f.byID[t.ID()] = t
	return nil
}
func (f *fakeTxs) Update(context.Context, *wagering.Transaction) error { return nil }
func (f *fakeTxs) Get(_ context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return f.byID[id], nil
}
func (f *fakeTxs) GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return f.Get(ctx, id)
}
func (f *fakeTxs) FindByIdempotencyKey(_ context.Context, p, k string) (*wagering.Transaction, error) {
	return f.find(func(x *wagering.Transaction) bool { return x.ProviderID() == p && x.IdempotencyKey() == k }), nil
}
func (f *fakeTxs) FindByExternalID(_ context.Context, p, e string) (*wagering.Transaction, error) {
	return f.find(func(x *wagering.Transaction) bool { return x.ProviderID() == p && x.ExternalID() == e }), nil
}
func (f *fakeTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	return f.find(func(x *wagering.Transaction) bool {
		return x.ReferenceID() != nil && *x.ReferenceID() == ref && x.Status() == wagering.StatusProcessed && x.Kind().IsReversal()
	}) != nil, nil
}
func (f *fakeTxs) DuePendingReferences(context.Context, time.Time, int) ([]PendingRef, error) {
	return nil, nil
}
func (f *fakeTxs) WakeDependents(context.Context, string, string, time.Time) error { return nil }

type fakeOutbox struct{ records []events.Record }

func (f *fakeOutbox) Append(_ context.Context, r []events.Record) error {
	f.records = append(f.records, r...)
	return nil
}

type fakeInbox struct{ m map[string]string }

func (f *fakeInbox) Register(_ context.Context, c, id, h string, _ time.Time) (bool, string, error) {
	if stored, ok := f.m[c+"/"+id]; ok {
		return false, stored, nil
	}
	f.m[c+"/"+id] = h
	return true, "", nil
}
func (f *fakeInbox) Complete(context.Context, string, string, *uuid.UUID, string, time.Time) error {
	return nil
}

type nopMetrics struct{}

func (nopMetrics) TransactionResult(string, string, string, bool) {}
func (nopMetrics) Duplicate(string)                               {}
func (nopMetrics) IdempotencyConflict()                           {}
func (nopMetrics) ConcurrencyConflict()                           {}
func (nopMetrics) ProcessingLatency(string, time.Duration)        {}
func (nopMetrics) ReferenceRetry()                                {}
func (nopMetrics) ReconciliationChecked(bool)                     {}

type harness struct {
	wallets *WalletService
	wager   *WageringService
	repo    *fakeWallets
	outbox  *fakeOutbox
}

func newHarness() harness {
	tx, wr, tr, ob := &fakeTx{}, &fakeWallets{w: map[uuid.UUID]*wallet.Wallet{}}, &fakeTxs{byID: map[uuid.UUID]*wagering.Transaction{}}, &fakeOutbox{}
	clock := func() time.Time { return time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC) }
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return harness{
		wallets: NewWalletService(WalletDeps{Tx: tx, Wallets: wr, Txs: tr, Outbox: ob, Clock: clock, IDs: uuid.New, Metrics: nopMetrics{}, Log: log}),
		wager: NewWageringService(WageringDeps{Tx: tx, Wallets: wr, Txs: tr, Outbox: ob, Inbox: &fakeInbox{m: map[string]string{}},
			Clock: clock, IDs: uuid.New, Policy: wagering.DefaultRetryPolicy, Metrics: nopMetrics{}, Log: log}),
		repo: wr, outbox: ob,
	}
}

func ptr(s string) *string { return &s }

func input(walletID, playerID uuid.UUID, ext, kind, amount string) SubmitInput {
	return SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: "round-1", GameID: "g",
		Kind: kind, Amount: ptr(amount), Currency: "BRL",
	}
}

func TestParseSubmitValidation(t *testing.T) {
	w, p := uuid.New(), uuid.New()
	ok := input(w, p, "t1", "BET", "25.00")
	cmd, err := ParseSubmit(ok)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.PayloadHash == "" || cmd.CorrelationID == "" {
		t.Fatal("hash and correlation id must be set")
	}
	// The idempotency key and transport metadata do not affect the hash.
	other := ok
	other.IdempotencyKey, other.CorrelationID = "another-key", "c2"
	cmd2, _ := ParseSubmit(other)
	if cmd2.PayloadHash != cmd.PayloadHash {
		t.Fatal("hash must exclude idempotency key and transport metadata")
	}

	bad := map[string]func(*SubmitInput){
		"missing key":      func(i *SubmitInput) { i.IdempotencyKey = "" },
		"amount as number": func(i *SubmitInput) { i.Amount = nil },
		"scientific":       func(i *SubmitInput) { i.Amount = ptr("2.5e1") },
		"excess scale":     func(i *SubmitInput) { i.Amount = ptr("25.001") },
		"negative":         func(i *SubmitInput) { i.Amount = ptr("-25.00") },
		"NaN":              func(i *SubmitInput) { i.Amount = ptr("NaN") },
		"empty amount":     func(i *SubmitInput) { i.Amount = ptr("") },
		"bad currency":     func(i *SubmitInput) { i.Currency = "brl" },
		"opening":          func(i *SubmitInput) { i.Kind = "OPENING" },
		"unknown kind":     func(i *SubmitInput) { i.Kind = "JACKPOT" },
		"bad uuid":         func(i *SubmitInput) { i.WalletID = "not-a-uuid" },
		"braced uuid":      func(i *SubmitInput) { i.PlayerID = "{" + p.String() + "}" },
		"missing round":    func(i *SubmitInput) { i.RoundID = "" },
	}
	for name, mutate := range bad {
		in := ok
		mutate(&in)
		if _, err := ParseSubmit(in); !IsValidation(err) {
			t.Errorf("%s: err = %v, want validation error", name, err)
		}
	}
}

func TestSubmitIdempotency(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	player := uuid.New()
	w, err := h.wallets.Open(ctx, OpenWalletCommand{PlayerID: player, Initial: money.Money{}})
	if err == nil || w != nil {
		t.Fatal("uninitialized initial balance must be rejected")
	}
	initial, _ := money.Parse("100.00", money.BRL)
	w, err = h.wallets.Open(ctx, OpenWalletCommand{PlayerID: player, Initial: initial})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.wallets.Open(ctx, OpenWalletCommand{PlayerID: player, Initial: initial}); !errors.Is(err, ErrWalletAlreadyExists) {
		t.Fatalf("duplicate wallet err = %v", err)
	}

	cmd, _ := ParseSubmit(input(w.ID(), player, "t1", "BET", "25.00"))
	first, err := h.wager.Submit(ctx, cmd)
	if err != nil || first.Replay || first.Tx.Status() != wagering.StatusProcessed {
		t.Fatalf("first: %+v %v", first, err)
	}
	// The replay returns the original result even after later movements.
	later, _ := ParseSubmit(input(w.ID(), player, "t2", "WIN", "10.00"))
	if _, err := h.wager.Submit(ctx, later); err != nil {
		t.Fatal(err)
	}
	again, err := h.wager.Submit(ctx, cmd)
	if err != nil || !again.Replay || again.Tx.ID() != first.Tx.ID() || again.Tx.ResultBalance().Amount() != "75.00" {
		t.Fatalf("replay: %+v %v", again, err)
	}

	// Same key, different content -> conflict.
	changed := input(w.ID(), player, "t1", "BET", "26.00")
	cmdChanged, _ := ParseSubmit(changed)
	if _, err := h.wager.Submit(ctx, cmdChanged); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict err = %v", err)
	}
	// Same operation, another key -> not reapplied.
	otherKey := input(w.ID(), player, "t1", "BET", "25.00")
	otherKey.IdempotencyKey = "retry-with-new-key"
	cmdOther, _ := ParseSubmit(otherKey)
	if _, err := h.wager.Submit(ctx, cmdOther); !errors.Is(err, ErrExternalIDConflict) {
		t.Fatalf("external id conflict err = %v", err)
	}
	// Unknown wallet: correctable, nothing persisted.
	missing, _ := ParseSubmit(input(uuid.New(), player, "t9", "BET", "1.00"))
	if _, err := h.wager.Submit(ctx, missing); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("wallet not found err = %v", err)
	}
	if len(h.repo.ledger) != 3 { // opening + bet + win
		t.Fatalf("ledger entries = %d", len(h.repo.ledger))
	}
	rec, err := h.wallets.Reconcile(ctx, w.ID())
	if err != nil || !rec.Consistent || rec.CheckedEntries != 3 {
		t.Fatalf("reconcile %+v %v", rec, err)
	}
}

func TestSubmitMessageInbox(t *testing.T) {
	h := newHarness()
	ctx := context.Background()
	player := uuid.New()
	initial, _ := money.Parse("10.00", money.BRL)
	w, _ := h.wallets.Open(ctx, OpenWalletCommand{PlayerID: player, Initial: initial})
	cmd, _ := ParseSubmit(input(w.ID(), player, "m1", "BET", "1.00"))

	res, err := h.wager.SubmitMessage(ctx, "consumer", "msg-1", cmd)
	if err != nil || res.DuplicateMessage || res.Tx.Status() != wagering.StatusProcessed {
		t.Fatalf("first: %+v %v", res, err)
	}
	dup, err := h.wager.SubmitMessage(ctx, "consumer", "msg-1", cmd)
	if err != nil || !dup.DuplicateMessage || dup.Tx.ID() != res.Tx.ID() {
		t.Fatalf("duplicate: %+v %v", dup, err)
	}
	other, _ := ParseSubmit(input(w.ID(), player, "m2", "BET", "1.00"))
	if _, err := h.wager.SubmitMessage(ctx, "consumer", "msg-1", other); !errors.Is(err, ErrInboxPayloadMismatch) {
		t.Fatalf("hash mismatch err = %v", err)
	}
	// Same operation arriving over HTTP afterwards is a replay.
	httpRes, err := h.wager.Submit(ctx, cmd)
	if err != nil || !httpRes.Replay || httpRes.Tx.ID() != res.Tx.ID() {
		t.Fatalf("cross-transport replay: %+v %v", httpRes, err)
	}
}

func TestCursor(t *testing.T) {
	c := EncodeCursor(42)
	if seq, err := DecodeCursor(c); err != nil || seq != 42 {
		t.Fatalf("round trip: %d %v", seq, err)
	}
	if seq, err := DecodeCursor(""); err != nil || seq != 0 {
		t.Fatalf("empty cursor: %d %v", seq, err)
	}
	for _, bad := range []string{"42", "!!!", EncodeCursor(-1)[:3]} {
		if _, err := DecodeCursor(bad); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("DecodeCursor(%q) err = %v", bad, err)
		}
	}
}

// countingTx fails every transaction with a fixed error and counts attempts.
type countingTx struct {
	err   error
	calls int
}

func (c *countingTx) WithinTx(context.Context, func(context.Context) error) error {
	c.calls++
	return c.err
}
func (c *countingTx) WithinSnapshot(ctx context.Context, fn func(context.Context) error) error {
	return c.WithinTx(ctx, fn)
}

func TestRetryOnlyConcurrencyConflicts(t *testing.T) {
	cases := map[string]struct {
		err   error
		calls int
	}{
		"serialization conflict": {&TransientError{Err: errors.New("40001"), Conflict: true}, maxTxAttempts},
		"version mismatch":       {ErrConcurrentUpdate, maxTxAttempts},
		// Unavailability is not retried in-request: it would multiply load on
		// a struggling database; the circuit breaker handles it.
		"connection refused": {&TransientError{Err: errors.New("dial tcp: connection refused")}, 1},
		"business error":     {ErrWalletNotFound, 1},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			tx := &countingTx{err: c.err}
			svc := NewWageringService(WageringDeps{Tx: tx, Metrics: nopMetrics{}, Clock: time.Now,
				Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err := svc.retry(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, c.err) {
				t.Fatalf("err = %v", err)
			}
			if tx.calls != c.calls {
				t.Fatalf("attempts = %d, want %d", tx.calls, c.calls)
			}
		})
	}
}
