package wallet

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/informeai/jungle-gaming-backend-challenger-go/internal/domain/money"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func open(t *testing.T, amount string) *Wallet {
	t.Helper()
	o, err := Open(uuid.New(), uuid.New(), brl(t, amount), uuid.New(), uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	return o.Wallet
}

func TestOpen(t *testing.T) {
	txID := uuid.New()
	o, err := Open(uuid.New(), uuid.New(), brl(t, "100.00"), txID, uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	if o.Wallet.Version() != InitialVersion || o.Wallet.Balance().Amount() != "100.00" {
		t.Fatalf("version=%d balance=%s", o.Wallet.Version(), o.Wallet.Balance())
	}
	e := o.Entry
	if e == nil || e.Direction() != Credit || e.BalanceBefore().Amount() != "0.00" || e.BalanceAfter().Amount() != "100.00" || e.TransactionID() != txID {
		t.Fatalf("opening entry = %+v", e)
	}

	zero, err := Open(uuid.New(), uuid.New(), brl(t, "0.00"), uuid.Nil, uuid.Nil, now)
	if err != nil || zero.Entry != nil || zero.Wallet.Version() != 1 {
		t.Fatalf("zero opening: %+v %v", zero, err)
	}
	if _, err := Open(uuid.New(), uuid.New(), brl(t, "-1.00"), uuid.New(), uuid.New(), now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("negative opening err = %v", err)
	}
	if _, err := Open(uuid.Nil, uuid.New(), brl(t, "1.00"), uuid.New(), uuid.New(), now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("nil id err = %v", err)
	}
	if _, err := Open(uuid.New(), uuid.New(), money.Money{}, uuid.New(), uuid.New(), now); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("uninitialized balance err = %v", err)
	}
}

func TestDebitCredit(t *testing.T) {
	w := open(t, "100.00")
	e, err := w.Debit(uuid.New(), uuid.New(), brl(t, "80.00"), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("after debit balance=%s version=%d", w.Balance(), w.Version())
	}
	if e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" || e.Direction() != Debit {
		t.Fatalf("entry %+v", e)
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(t, "80.00"), now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("insufficient err = %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("rejected debit must not change the wallet")
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(t, "20.00"), now); err != nil {
		t.Fatalf("debit to exactly zero: %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl(t, "5.50"), now); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "5.50" || w.Version() != 4 {
		t.Fatalf("balance=%s version=%d", w.Balance(), w.Version())
	}
}

func TestMovementValidation(t *testing.T) {
	w := open(t, "10.00")
	usd, _ := money.Parse("1.00", money.MustCurrency("USD"))
	if _, err := w.Credit(uuid.New(), uuid.New(), usd, now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency err = %v", err)
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(t, "0.00"), now); !errors.Is(err, ErrNonPositiveAmount) {
		t.Fatalf("zero err = %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl(t, "-1.00"), now); !errors.Is(err, ErrNonPositiveAmount) {
		t.Fatalf("negative err = %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), money.Money{}, now); !errors.Is(err, money.ErrUninitialized) {
		t.Fatalf("uninitialized err = %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("invalid movements must not bump the version")
	}
}

func TestCreditOverflow(t *testing.T) {
	huge, _ := money.FromMinor(1<<62, money.BRL)
	o, err := Open(uuid.New(), uuid.New(), huge, uuid.New(), uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Wallet.Credit(uuid.New(), uuid.New(), huge, now); !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("overflow err = %v", err)
	}
}

func TestRehydrateDoesNotApplyMovements(t *testing.T) {
	created := now.Add(-time.Hour)
	w, err := Rehydrate(uuid.New(), uuid.New(), brl(t, "42.00"), 7, created, now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "42.00" || w.Version() != 7 || !w.CreatedAt().Equal(created) {
		t.Fatalf("rehydrated %+v", w)
	}
	bad := []struct {
		name    string
		balance money.Money
		version int64
	}{
		{"negative", brl(t, "-1.00"), 1},
		{"version zero", brl(t, "1.00"), 0},
		{"uninitialized", money.Money{}, 1},
	}
	for _, b := range bad {
		if _, err := Rehydrate(uuid.New(), uuid.New(), b.balance, b.version, created, now); !errors.Is(err, ErrInvalidWallet) {
			t.Errorf("%s: err = %v", b.name, err)
		}
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	id, w, tx := uuid.New(), uuid.New(), uuid.New()
	if _, err := NewLedgerEntry(id, w, tx, Credit, brl(t, "10.00"), brl(t, "5.00"), brl(t, "15.00"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry(id, w, tx, Debit, brl(t, "10.00"), brl(t, "15.00"), brl(t, "5.00"), now); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][3]string{
		"wrong credit":   {"10.00", "5.00", "14.99"},
		"negative after": {"10.00", "5.00", "-5.00"},
		"zero amount":    {"0.00", "5.00", "5.00"},
	} {
		dir := Credit
		if name == "negative after" {
			dir = Debit
		}
		if _, err := NewLedgerEntry(id, w, tx, dir, brl(t, args[0]), brl(t, args[1]), brl(t, args[2]), now); !errors.Is(err, ErrInvalidLedgerEntry) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := NewLedgerEntry(id, w, tx, Direction("SIDEWAYS"), brl(t, "1.00"), brl(t, "1.00"), brl(t, "2.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("bad direction err = %v", err)
	}
	if _, err := NewLedgerEntry(uuid.Nil, w, tx, Credit, brl(t, "1.00"), brl(t, "1.00"), brl(t, "2.00"), now); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("nil id err = %v", err)
	}
}
