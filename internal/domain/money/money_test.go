package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		in    string
		minor int64
		err   error
	}{
		{"0.00", 0, nil},
		{"25.00", 2500, nil},
		{"0.01", 1, nil},
		{"1000.10", 100010, nil},
		{"-3.10", -310, nil},
		{"92233720368547758.07", math.MaxInt64, nil},
		{"-92233720368547758.08", math.MinInt64, nil},
		{"92233720368547758.08", 0, ErrOverflow},
		{"-92233720368547758.09", 0, ErrOverflow},
		{"999999999999999999999.00", 0, ErrOverflow},
		{"", 0, ErrInvalidAmount},
		{" 1.00", 0, ErrInvalidAmount},
		{"1.00 ", 0, ErrInvalidAmount},
		{"+1.00", 0, ErrInvalidAmount},
		{"1", 0, ErrInvalidAmount},
		{"1.0", 0, ErrInvalidAmount},
		{"1.000", 0, ErrInvalidAmount},
		{".50", 0, ErrInvalidAmount},
		{"1.", 0, ErrInvalidAmount},
		{"01.00", 0, ErrInvalidAmount},
		{"1,00", 0, ErrInvalidAmount},
		{"1e2", 0, ErrInvalidAmount},
		{"1.00e2", 0, ErrInvalidAmount},
		{"NaN", 0, ErrInvalidAmount},
		{"Infinity", 0, ErrInvalidAmount},
		{"-Infinity", 0, ErrInvalidAmount},
		{"0x10.00", 0, ErrInvalidAmount},
		{"1_000.00", 0, ErrInvalidAmount},
		{"--1.00", 0, ErrInvalidAmount},
		{"-", 0, ErrInvalidAmount},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			m, err := Parse(c.in, BRL)
			if c.err != nil {
				if !errors.Is(err, c.err) {
					t.Fatalf("Parse(%q) err = %v, want %v", c.in, err, c.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error %v", c.in, err)
			}
			if m.MinorUnits() != c.minor {
				t.Fatalf("Parse(%q) = %d, want %d", c.in, m.MinorUnits(), c.minor)
			}
			if m.Amount() != c.in {
				t.Fatalf("round trip %q -> %q", c.in, m.Amount())
			}
		})
	}
}

func TestParseNonNegativeRejectsNegative(t *testing.T) {
	if _, err := ParseNonNegative("-1.00", BRL); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("err = %v", err)
	}
	if _, err := ParseNonNegative("-0.00", BRL); !errors.Is(err, ErrNegativeAmount) {
		t.Fatalf("-0.00 must be rejected, err = %v", err)
	}
	if m, err := ParseNonNegative("0.00", BRL); err != nil || !m.IsZero() {
		t.Fatalf("0.00: %v %v", m, err)
	}
}

func TestCurrency(t *testing.T) {
	for _, bad := range []string{"", "brl", "BR", "BRLL", "B1L", "XXX", "JPY"} {
		if _, err := ParseCurrency(bad); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) err = %v", bad, err)
		}
	}
	if c, err := ParseCurrency("USD"); err != nil || c.Code() != "USD" {
		t.Fatalf("USD: %v %v", c, err)
	}
}

func TestUninitialized(t *testing.T) {
	var zero Money
	if zero.IsInitialized() {
		t.Fatal("zero value must be uninitialized")
	}
	one, _ := Parse("1.00", BRL)
	if _, err := zero.Add(one); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Add err = %v", err)
	}
	if _, err := one.Sub(zero); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Sub err = %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Neg err = %v", err)
	}
	if _, err := zero.MarshalJSON(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Marshal err = %v", err)
	}
	if _, err := Parse("1.00", Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Parse with zero currency err = %v", err)
	}
	if _, err := Zero(Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Zero err = %v", err)
	}
}

func mustParse(t *testing.T, s string, c Currency) Money {
	t.Helper()
	m, err := Parse(s, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestArithmetic(t *testing.T) {
	a, b := mustParse(t, "100.00", BRL), mustParse(t, "80.00", BRL)
	sum, _ := a.Add(b)
	diff, _ := b.Sub(a)
	neg, _ := a.Neg()
	if sum.Amount() != "180.00" || diff.Amount() != "-20.00" || neg.Amount() != "-100.00" {
		t.Fatalf("sum=%s diff=%s neg=%s", sum, diff, neg)
	}
	if !diff.IsNegative() || a.IsNegative() || !a.IsPositive() {
		t.Fatal("sign helpers")
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("cmp = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Fatalf("cmp = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Fatalf("cmp = %d", c)
	}
	// Immutability: operands are unchanged.
	if a.Amount() != "100.00" || b.Amount() != "80.00" {
		t.Fatal("operands mutated")
	}
	z, _ := Zero(BRL)
	if !z.IsZero() || z.Amount() != "0.00" {
		t.Fatal("zero")
	}
}

func TestOverflow(t *testing.T) {
	maxM, _ := FromMinor(math.MaxInt64, BRL)
	minM, _ := FromMinor(math.MinInt64, BRL)
	one, _ := FromMinor(1, BRL)
	if _, err := maxM.Add(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("add overflow err = %v", err)
	}
	if _, err := minM.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("sub overflow err = %v", err)
	}
	negOne, _ := one.Neg()
	if _, err := maxM.Sub(negOne); !errors.Is(err, ErrOverflow) {
		t.Fatalf("sub negative overflow err = %v", err)
	}
	if _, err := minM.Add(negOne); !errors.Is(err, ErrOverflow) {
		t.Fatalf("add negative overflow err = %v", err)
	}
	if _, err := minM.Neg(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("neg overflow err = %v", err)
	}
	if minM.Amount() != "-92233720368547758.08" {
		t.Fatalf("format MinInt64 = %s", minM.Amount())
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl, usd := mustParse(t, "1.00", BRL), mustParse(t, "1.00", MustCurrency("USD"))
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Add err = %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Sub err = %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Cmp err = %v", err)
	}
	if brl.Equal(usd) {
		t.Fatal("different currencies must not be equal")
	}
}

func TestJSON(t *testing.T) {
	m := mustParse(t, "25.00", BRL)
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("marshal = %s %v", b, err)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("unmarshal = %v %v", back, err)
	}
	for _, bad := range []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25","currency":"BRL"}`,
		`{"amount":"25.00","currency":"brl"}`,
		`{"amount":"1e3","currency":"BRL"}`,
		`{"currency":"BRL"}`,
		`{"amount":null,"currency":"BRL"}`,
	} {
		var x Money
		if err := json.Unmarshal([]byte(bad), &x); err == nil {
			t.Errorf("Unmarshal(%s) should fail", bad)
		}
	}
}
