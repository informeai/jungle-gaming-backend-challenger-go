package wagering

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/events"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/wallet"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func env(now time.Time) Env {
	return Env{NewID: uuid.New, Now: now, Policy: RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 4 * time.Second, TTL: time.Minute}}
}

type fixture struct {
	walletID, playerID uuid.UUID
}

func newFixture() fixture { return fixture{walletID: uuid.New(), playerID: uuid.New()} }

func (f fixture) wallet(t *testing.T, balance string) *wallet.Wallet {
	t.Helper()
	w, err := wallet.Rehydrate(f.walletID, f.playerID, brl(t, balance), 1, t0, t0)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func (f fixture) tx(t *testing.T, ext string, kind Kind, amount, ref string) *Transaction {
	t.Helper()
	tx, err := NewExternal(ExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalID: ext, IdempotencyKey: "provider-a:" + ext,
		PayloadHash: "hash-" + ext, WalletID: f.walletID, PlayerID: f.playerID, RoundID: "round-1",
		GameID: "fortune-chimp", Kind: kind, Money: brl(t, amount), ReferenceExternalID: ref,
		CorrelationID: "corr-" + ext, Now: t0,
	})
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", ext, err)
	}
	return tx
}

// processed settles tx against w and asserts it was PROCESSED.
func processed(t *testing.T, w *wallet.Wallet, tx *Transaction, ref Reference) Settlement {
	t.Helper()
	s, err := Settle(env(t0), w, tx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if s.Outcome != OutcomeProcessed || tx.Status() != StatusProcessed {
		t.Fatalf("%s: outcome=%s status=%s code=%s", tx.Kind(), s.Outcome, tx.Status(), tx.FailureCode())
	}
	return s
}

func eventTypes(s Settlement) []string {
	var out []string
	for _, e := range s.Events {
		out = append(out, e.EventType)
	}
	return out
}

func TestValidationZeroPolicyAndReferences(t *testing.T) {
	cases := []struct {
		kind   Kind
		amount string
		ref    string
		code   string
	}{
		{KindBet, "0.00", "", CodeAmountMustBePositive},
		{KindWin, "0.00", "", CodeAmountMustBePositive},
		{KindRefund, "0.00", "b1", CodeAmountMustBePositive},
		{KindRollback, "0.00", "b1", CodeAmountMustBePositive},
		{KindLoss, "0.01", "", CodeLossAmountMustBeZero},
		{KindRefund, "1.00", "", CodeReferenceRequired},
		{KindRollback, "1.00", "", CodeReferenceRequired},
		{KindBet, "1.00", "b1", CodeReferenceNotAllowed},
		{KindLoss, "0.00", "b1", CodeReferenceNotAllowed},
		{KindOpening, "1.00", "", CodeOpeningNotAllowed},
		{KindBet, "-1.00", "", CodeInvalidMoney},
		{KindRefund, "1.00", "x1", CodeSelfReference},
	}
	for _, c := range cases {
		err := ValidateExternal("provider-a", "x1", "r", "g", c.kind, brl(t, c.amount), c.ref)
		var v *ValidationError
		if !errors.As(err, &v) || v.Code != c.code || !errors.Is(err, ErrValidation) {
			t.Errorf("%s %s ref=%q: err = %v, want %s", c.kind, c.amount, c.ref, err, c.code)
		}
	}
	for _, ok := range []struct {
		kind   Kind
		amount string
		ref    string
	}{{KindBet, "0.01", ""}, {KindLoss, "0.00", ""}, {KindWin, "1.00", ""}, {KindWin, "1.00", "b1"}, {KindRefund, "1.00", "b1"}, {KindRollback, "1.00", "b1"}} {
		if err := ValidateExternal("provider-a", "x1", "r", "g", ok.kind, brl(t, ok.amount), ok.ref); err != nil {
			t.Errorf("%s should be valid: %v", ok.kind, err)
		}
	}
	if err := ValidateExternal("", "x1", "r", "g", KindBet, brl(t, "1.00"), ""); err == nil {
		t.Error("missing provider must be rejected")
	}
	if err := ValidateExternal("p", "x1", "r", "g", KindBet, money.Money{}, ""); err == nil {
		t.Error("uninitialized money must be rejected")
	}
	if _, err := ParseExternalKind("OPENING"); err == nil {
		t.Error("OPENING must be rejected from external sources")
	}
	if _, err := ParseExternalKind("JACKPOT"); err == nil {
		t.Error("unknown kind must be rejected")
	}
}

func TestStateMachine(t *testing.T) {
	f := newFixture()
	tx := f.tx(t, "b1", KindBet, "1.00", "")
	if tx.Status() != StatusPending {
		t.Fatalf("initial status %s", tx.Status())
	}
	if err := tx.MarkPendingReference(t0, t0, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("BET has no reference to wait for: %v", err)
	}
	if err := tx.MarkProcessed(brl(t, "9.00"), 2, nil, t0); err != nil {
		t.Fatal(err)
	}
	if tx.CompletedAt() == nil {
		t.Fatal("completedAt must be set on terminal state")
	}
	for name, fn := range map[string]func() error{
		"processed": func() error { return tx.MarkProcessed(brl(t, "1.00"), 3, nil, t0) },
		"rejected":  func() error { return tx.MarkRejected(FailInsufficientFunds, nil, nil, t0) },
		"failed":    func() error { return tx.MarkFailed(FailPermanentProcessingError, t0) },
	} {
		if err := fn(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("terminal -> %s: err = %v", name, err)
		}
	}

	r := f.tx(t, "r1", KindRefund, "1.00", "b0")
	if err := r.MarkPendingReference(t0.Add(time.Second), t0.Add(time.Minute), t0); err != nil {
		t.Fatal(err)
	}
	if r.Attempts() != 1 || r.ExpiresAt() == nil {
		t.Fatalf("attempts=%d expires=%v", r.Attempts(), r.ExpiresAt())
	}
	if err := r.MarkPendingReference(t0.Add(2*time.Second), time.Time{}, t0); err != nil {
		t.Fatal(err)
	}
	if r.Attempts() != 2 || !r.ExpiresAt().Equal(t0.Add(time.Minute)) {
		t.Fatalf("reschedule must count an attempt and keep the TTL: %d %v", r.Attempts(), r.ExpiresAt())
	}
	if err := r.MarkRejected("", nil, nil, t0); err == nil {
		t.Fatal("rejection requires a failure code")
	}
	if err := r.MarkFailed(FailPermanentProcessingError, t0); err != nil {
		t.Fatal(err)
	}
	if r.Status() != StatusFailed || r.NextAttemptAt() != nil {
		t.Fatalf("status=%s next=%v", r.Status(), r.NextAttemptAt())
	}
}

func TestBet(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "100.00")
	s := processed(t, w, f.tx(t, "b1", KindBet, "80.00", ""), Reference{})
	if w.Balance().Amount() != "20.00" || w.Version() != 2 || s.Entry.Direction() != wallet.Debit {
		t.Fatalf("balance=%s version=%d", w.Balance(), w.Version())
	}
	if got := eventTypes(s); len(got) != 2 || got[0] != events.TypeWagerTransactionProcessed || got[1] != events.TypeWalletBalanceChanged {
		t.Fatalf("events %v", got)
	}

	second := f.tx(t, "b2", KindBet, "80.00", "")
	s2, err := Settle(env(t0), w, second, Reference{})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Outcome != OutcomeRejected || second.FailureCode() != FailInsufficientFunds || s2.Entry != nil {
		t.Fatalf("outcome=%s code=%s", s2.Outcome, second.FailureCode())
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("rejected bet must not move the wallet")
	}
	if got := eventTypes(s2); len(got) != 1 || got[0] != events.TypeWagerTransactionRejected {
		t.Fatalf("events %v", got)
	}
	if second.ResultBalance() == nil || second.ResultBalance().Amount() != "20.00" {
		t.Fatalf("rejected result balance %v", second.ResultBalance())
	}
}

func TestLossDoesNotMoveWallet(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "10.00")
	s := processed(t, w, f.tx(t, "l1", KindLoss, "0.00", ""), Reference{})
	if s.Entry != nil || w.Version() != 1 || w.Balance().Amount() != "10.00" {
		t.Fatalf("LOSS changed the wallet: %v %d", s.Entry, w.Version())
	}
	if got := eventTypes(s); len(got) != 1 || got[0] != events.TypeWagerTransactionProcessed {
		t.Fatalf("LOSS events %v", got)
	}
}

func TestWin(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "10.00")
	processed(t, w, f.tx(t, "w1", KindWin, "5.00", ""), Reference{})
	if w.Balance().Amount() != "15.00" {
		t.Fatalf("balance %s", w.Balance())
	}
	bet := f.tx(t, "b1", KindBet, "1.00", "")
	processed(t, w, bet, Reference{})
	win := f.tx(t, "w2", KindWin, "3.00", "b1")
	processed(t, w, win, Reference{Tx: bet})
	if win.ReferenceID() == nil || *win.ReferenceID() != bet.ID() {
		t.Fatal("resolved reference must be recorded")
	}
	loss := f.tx(t, "l1", KindLoss, "0.00", "")
	processed(t, w, loss, Reference{})
	badRef := f.tx(t, "w3", KindWin, "3.00", "l1")
	s, _ := Settle(env(t0), w, badRef, Reference{Tx: loss})
	if s.Outcome != OutcomeRejected || badRef.FailureCode() != FailReferenceKindNotAllowed {
		t.Fatalf("WIN referencing LOSS: %s %s", s.Outcome, badRef.FailureCode())
	}
}

func TestRefundAndRollback(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "100.00")
	bet := f.tx(t, "b1", KindBet, "30.00", "")
	processed(t, w, bet, Reference{})

	refund := f.tx(t, "r1", KindRefund, "30.00", "b1")
	processed(t, w, refund, Reference{Tx: bet})
	if w.Balance().Amount() != "100.00" {
		t.Fatalf("after refund %s", w.Balance())
	}
	// A second compensation of the same bet (REFUND or ROLLBACK) is rejected.
	for _, k := range []Kind{KindRefund, KindRollback} {
		again := f.tx(t, "again-"+string(k), k, "30.00", "b1")
		s, _ := Settle(env(t0), w, again, Reference{Tx: bet, AlreadyReversed: true})
		if s.Outcome != OutcomeRejected || again.FailureCode() != FailReferenceAlreadyReversed {
			t.Fatalf("%s twice: %s %s", k, s.Outcome, again.FailureCode())
		}
	}
	// ROLLBACK of the REFUND debits it back.
	rbRefund := f.tx(t, "rb-r1", KindRollback, "30.00", "r1")
	s := processed(t, w, rbRefund, Reference{Tx: refund})
	if s.Entry.Direction() != wallet.Debit || w.Balance().Amount() != "70.00" {
		t.Fatalf("rollback of refund: %s %s", s.Entry.Direction(), w.Balance())
	}

	// ROLLBACK of a BET credits; ROLLBACK of a WIN debits.
	bet2 := f.tx(t, "b2", KindBet, "10.00", "")
	processed(t, w, bet2, Reference{})
	processed(t, w, f.tx(t, "rb-b2", KindRollback, "10.00", "b2"), Reference{Tx: bet2})
	if w.Balance().Amount() != "70.00" {
		t.Fatalf("rollback of bet %s", w.Balance())
	}
	win := f.tx(t, "w1", KindWin, "50.00", "")
	processed(t, w, win, Reference{})
	processed(t, w, f.tx(t, "rb-w1", KindRollback, "50.00", "w1"), Reference{Tx: win})
	if w.Balance().Amount() != "70.00" {
		t.Fatalf("rollback of win %s", w.Balance())
	}
}

func TestReversalRejections(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "100.00")
	bet := f.tx(t, "b1", KindBet, "30.00", "")
	processed(t, w, bet, Reference{})

	reject := func(tx *Transaction, ref Reference, want FailureCode) {
		t.Helper()
		before := w.Balance()
		s, err := Settle(env(t0), w, tx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if s.Outcome != OutcomeRejected || tx.FailureCode() != want || !w.Balance().Equal(before) {
			t.Fatalf("%s: outcome=%s code=%s want %s", tx.ExternalID(), s.Outcome, tx.FailureCode(), want)
		}
	}
	reject(f.tx(t, "r-partial", KindRefund, "10.00", "b1"), Reference{Tx: bet}, FailReferenceAmountMismatch)

	win := f.tx(t, "w1", KindWin, "5.00", "")
	processed(t, w, win, Reference{})
	reject(f.tx(t, "r-win", KindRefund, "5.00", "w1"), Reference{Tx: win}, FailReferenceKindNotAllowed)

	rb := f.tx(t, "rb1", KindRollback, "30.00", "b1")
	processed(t, w, rb, Reference{Tx: bet})
	reject(f.tx(t, "rb-rb", KindRollback, "30.00", "rb1"), Reference{Tx: rb}, FailReferenceKindNotAllowed)

	other := newFixture()
	foreign := other.tx(t, "fb", KindBet, "30.00", "")
	_ = foreign.MarkProcessed(brl(t, "0.00"), 2, nil, t0)
	reject(f.tx(t, "r-foreign", KindRefund, "30.00", "fb"), Reference{Tx: foreign}, FailReferenceMismatch)

	rejectedBet := f.tx(t, "b-rej", KindBet, "1.00", "")
	_ = rejectedBet.MarkRejected(FailInsufficientFunds, nil, nil, t0)
	reject(f.tx(t, "r-rej", KindRefund, "1.00", "b-rej"), Reference{Tx: rejectedBet}, FailReferenceNotProcessed)

	// A ROLLBACK that would need more than the balance: distinct failure code.
	poor := f.wallet(t, "100.00")
	bigWin := f.tx(t, "bw", KindWin, "50.00", "")
	processed(t, poor, bigWin, Reference{})
	b3 := f.tx(t, "b3", KindBet, "120.00", "")
	processed(t, poor, b3, Reference{})
	rbWin := f.tx(t, "rb-bw", KindRollback, "50.00", "bw")
	s, _ := Settle(env(t0), poor, rbWin, Reference{Tx: bigWin})
	if s.Outcome != OutcomeRejected || rbWin.FailureCode() != FailInsufficientFundsForReversal || poor.Balance().Amount() != "30.00" {
		t.Fatalf("rollback without funds: %s %s %s", s.Outcome, rbWin.FailureCode(), poor.Balance())
	}
	if FailInsufficientFundsForReversal == FailInsufficientFunds {
		t.Fatal("codes must differ")
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "100.00")
	refund := f.tx(t, "r1", KindRefund, "10.00", "b1")

	s, err := Settle(env(t0), w, refund, Reference{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Outcome != OutcomePendingReference || refund.Status() != StatusPendingReference {
		t.Fatalf("outcome=%s", s.Outcome)
	}
	if got := eventTypes(s); len(got) != 1 || got[0] != events.TypeWagerTransactionPendingReference {
		t.Fatalf("events %v", got)
	}
	if !refund.NextAttemptAt().Equal(t0.Add(time.Second)) || !refund.ExpiresAt().Equal(t0.Add(time.Minute)) {
		t.Fatalf("schedule next=%v expires=%v", refund.NextAttemptAt(), refund.ExpiresAt())
	}
	// Retry still missing: rescheduled with exponential backoff, no new event.
	s, _ = Settle(env(t0.Add(time.Second)), w, refund, Reference{})
	if s.Outcome != OutcomePendingReference || len(s.Events) != 0 || refund.Attempts() != 2 ||
		!refund.NextAttemptAt().Equal(t0.Add(3*time.Second)) {
		t.Fatalf("retry: %s events=%d attempts=%d next=%v", s.Outcome, len(s.Events), refund.Attempts(), refund.NextAttemptAt())
	}
	// Reference arrives: resolved.
	bet := f.tx(t, "b1", KindBet, "10.00", "")
	processed(t, w, bet, Reference{})
	processed(t, w, refund, Reference{Tx: bet})
	if w.Balance().Amount() != "100.00" {
		t.Fatalf("balance %s", w.Balance())
	}

	// Exhausted attempts (MaxAttempts=3 evaluations): REFERENCE_NOT_FOUND
	// with a rejection event.
	lost := f.tx(t, "r2", KindRefund, "10.00", "missing")
	for i := 0; i < 2; i++ {
		if _, err := Settle(env(t0), w, lost, Reference{}); err != nil {
			t.Fatal(err)
		}
	}
	s, _ = Settle(env(t0), w, lost, Reference{})
	if s.Outcome != OutcomeRejected || lost.FailureCode() != FailReferenceNotFound {
		t.Fatalf("exhausted: %s %s", s.Outcome, lost.FailureCode())
	}
	if got := eventTypes(s); len(got) != 1 || got[0] != events.TypeWagerTransactionRejected {
		t.Fatalf("events %v", got)
	}

	// TTL elapsed while the reference exists but is itself still pending.
	waiting := f.tx(t, "w-pend", KindRefund, "10.00", "b-pend")
	_, _ = Settle(env(t0), w, waiting, Reference{})
	pendingBet := f.tx(t, "b-pend", KindBet, "10.00", "")
	s, _ = Settle(env(t0.Add(2*time.Minute)), w, waiting, Reference{Tx: pendingBet})
	if s.Outcome != OutcomeRejected || waiting.FailureCode() != FailReferenceStillPending {
		t.Fatalf("ttl: %s %s", s.Outcome, waiting.FailureCode())
	}
}

func TestSettleGuards(t *testing.T) {
	f := newFixture()
	w := f.wallet(t, "10.00")
	tx := f.tx(t, "b1", KindBet, "1.00", "")
	processed(t, w, tx, Reference{})
	if _, err := Settle(env(t0), w, tx, Reference{}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal settle err = %v", err)
	}

	stranger := fixture{walletID: f.walletID, playerID: uuid.New()}
	mismatch := stranger.tx(t, "b2", KindBet, "1.00", "")
	s, _ := Settle(env(t0), w, mismatch, Reference{})
	if s.Outcome != OutcomeRejected || mismatch.FailureCode() != FailWalletPlayerMismatch || mismatch.ResultBalance() != nil {
		t.Fatalf("player mismatch: %s %s (balance must not leak)", s.Outcome, mismatch.FailureCode())
	}

	usd, _ := money.Parse("1.00", money.MustCurrency("USD"))
	cur, err := NewExternal(ExternalParams{ID: uuid.New(), ProviderID: "p", ExternalID: "u1", IdempotencyKey: "k",
		PayloadHash: "h", WalletID: f.walletID, PlayerID: f.playerID, RoundID: "r", GameID: "g", Kind: KindBet,
		Money: usd, CorrelationID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	s, _ = Settle(env(t0), w, cur, Reference{})
	if s.Outcome != OutcomeRejected || cur.FailureCode() != FailCurrencyMismatch {
		t.Fatalf("currency mismatch: %s %s", s.Outcome, cur.FailureCode())
	}
}

func TestOpenWallet(t *testing.T) {
	player := uuid.New()
	o, err := OpenWallet(env(t0), uuid.New(), player, brl(t, "1000.00"), "corr")
	if err != nil {
		t.Fatal(err)
	}
	tx := o.Tx
	if tx.Kind() != KindOpening || tx.Origin() != OriginInternal || tx.Status() != StatusProcessed {
		t.Fatalf("opening tx %s %s %s", tx.Kind(), tx.Origin(), tx.Status())
	}
	if tx.ProviderID() != "" || tx.ExternalID() != "" || tx.IdempotencyKey() != "" || tx.PayloadHash() != "" || tx.RoundID() != "" || tx.GameID() != "" {
		t.Fatal("external metadata must not be set on OPENING")
	}
	if o.Wallet.Version() != 1 || o.Entry == nil || o.Entry.TransactionID() != tx.ID() {
		t.Fatalf("wallet version %d entry %v", o.Wallet.Version(), o.Entry)
	}
	if got := eventTypes(Settlement{Events: o.Events}); len(got) != 2 || got[0] != events.TypeWagerTransactionProcessed || got[1] != events.TypeWalletBalanceChanged {
		t.Fatalf("events %v", got)
	}
	var changed events.Envelope[events.WalletBalanceChanged]
	if err := json.Unmarshal(o.Events[1].Payload, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Version != 1 || changed.Data.WalletVersion != 1 || changed.Data.BalanceAfter.Amount != "1000.00" ||
		changed.Data.BalanceBefore.Amount != "0.00" || changed.Data.Direction != "CREDIT" || changed.CorrelationID != "corr" {
		t.Fatalf("payload %+v", changed)
	}

	zero, err := OpenWallet(env(t0), uuid.New(), player, brl(t, "0.00"), "corr")
	if err != nil || zero.Tx != nil || zero.Entry != nil || len(zero.Events) != 0 {
		t.Fatalf("zero opening must create no OPENING/ledger/events: %+v %v", zero, err)
	}
	if _, err := OpenWallet(env(t0), uuid.New(), player, brl(t, "-1.00"), "corr"); !errors.Is(err, ErrValidation) {
		t.Fatalf("negative opening err = %v", err)
	}
}

func TestRehydrate(t *testing.T) {
	f := newFixture()
	orig := f.tx(t, "b1", KindBet, "5.00", "")
	w := f.wallet(t, "10.00")
	processed(t, w, orig, Reference{})
	p := RehydrateParams{
		ID: orig.ID(), Origin: orig.Origin(), Kind: orig.Kind(), Status: orig.Status(), WalletID: orig.WalletID(),
		PlayerID: orig.PlayerID(), Money: orig.Money(), ProviderID: orig.ProviderID(), ExternalID: orig.ExternalID(),
		IdempotencyKey: orig.IdempotencyKey(), PayloadHash: orig.PayloadHash(), RoundID: orig.RoundID(), GameID: orig.GameID(),
		ResultBalance: orig.ResultBalance(), CorrelationID: orig.CorrelationID(), CreatedAt: orig.CreatedAt(), UpdatedAt: orig.UpdatedAt(),
	}
	back, err := Rehydrate(p)
	if err != nil || back.Status() != StatusProcessed || back.ResultBalance().Amount() != "5.00" {
		t.Fatalf("rehydrate: %v", err)
	}
	p.Origin = OriginInternal
	if _, err := Rehydrate(p); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("internal origin with provider metadata must fail: %v", err)
	}
	p.Origin, p.ResultBalance = OriginExternal, nil
	if _, err := Rehydrate(p); !errors.Is(err, ErrInvalidTransaction) {
		t.Fatalf("processed without result must fail: %v", err)
	}
}

func TestFingerprint(t *testing.T) {
	base := Request{
		ProviderID: "provider-a", ExternalID: "transaction-123",
		PlayerID: uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"), WalletID: uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37"),
		RoundID: "round-987", GameID: "fortune-chimp", Kind: KindBet, Money: brl(t, "25.00"),
	}
	h := Fingerprint(base)
	if len(h) != 64 || Fingerprint(base) != h {
		t.Fatal("fingerprint must be a deterministic sha256 hex")
	}
	// Canonical JSON: sorted keys, no whitespace.
	const want = `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	if h != sha256hex(want) {
		t.Fatalf("fingerprint does not match the documented canonical form")
	}
	changed := base
	changed.Money = brl(t, "25.01")
	if Fingerprint(changed) == h {
		t.Fatal("amount must change the fingerprint")
	}
	withRef := base
	withRef.ReferenceExternalID = "b0"
	if Fingerprint(withRef) == h {
		t.Fatal("reference must change the fingerprint")
	}
}
