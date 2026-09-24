// Package money implements the Money value object.
//
// Representation: amounts are int64 counts of minor units (cents) with a fixed
// scale of two decimal places. The supported range is therefore
// [-9223372036854775808, 9223372036854775807] minor units, i.e. roughly
// ±92 233 720 368 547 758.07 in major units. Every arithmetic operation is
// overflow checked and returns ErrOverflow instead of wrapping around.
//
// No float32/float64 value is ever used: parsing is done digit by digit and
// formatting uses integer division.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale is the fixed number of decimal places of every supported currency.
const Scale = 2

const minorPerMajor = 100

var (
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: amount overflow")
	ErrNegativeAmount   = errors.New("money: negative amount")
	ErrUninitialized    = errors.New("money: uninitialized value")
)

// Currency is an ISO 4217 currency code whose minor unit is 2.
type Currency struct {
	code string
}

// supportedCurrencies lists the ISO 4217 codes accepted by the service. Only
// currencies with exactly two decimal places are listed, so the fixed scale is
// always correct.
var supportedCurrencies = map[string]struct{}{
	"BRL": {},
	"USD": {},
	"EUR": {},
	"GBP": {},
	"MXN": {},
	"ARS": {},
}

// ParseCurrency validates an ISO 4217 code. Codes must be three upper-case
// letters and belong to the supported set; no case normalisation is applied.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}
	}
	if _, ok := supportedCurrencies[code]; !ok {
		return Currency{}, fmt.Errorf("%w: unsupported %q", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

// MustCurrency is a helper for constants and tests.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// BRL is the main currency of the challenge.
var BRL = MustCurrency("BRL")

func (c Currency) Code() string   { return c.code }
func (c Currency) String() string { return c.code }
func (c Currency) IsZero() bool   { return c.code == "" }

// Money is an immutable amount of a currency.
type Money struct {
	minor    int64
	currency Currency
}

// Zero returns zero units of the currency.
func Zero(c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{currency: c}, nil
}

// FromMinor builds Money from minor units (used by persistence rehydration).
func FromMinor(minor int64, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse parses a signed decimal string with exactly two decimal places, e.g.
// "25.00" or "-3.10". Accepted grammar: -?(0|[1-9][0-9]*)\.[0-9]{2}.
// Empty strings, whitespace, "+", NaN, Infinity, exponents, missing or excess
// scale and leading zeros are rejected; nothing is rounded.
func Parse(amount string, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrUninitialized
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

// ParseNonNegative is Parse for external financial inputs, where negative
// values are forbidden.
func ParseNonNegative(amount string, c Currency) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	return Parse(amount, c)
}

// ParseWithCode parses both the amount and the currency code.
func ParseWithCode(amount, code string) (Money, error) {
	c, err := ParseCurrency(code)
	if err != nil {
		return Money{}, err
	}
	return Parse(amount, c)
}

func parseMinor(s string) (int64, error) {
	invalid := func() (int64, error) { return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s) }
	if s == "" {
		return invalid()
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	dot := strings.IndexByte(s, '.')
	if dot <= 0 || len(s)-dot-1 != Scale {
		return invalid()
	}
	intPart, fracPart := s[:dot], s[dot+1:]
	if len(intPart) > 1 && intPart[0] == '0' {
		return invalid()
	}
	// Accumulate as a negative number so that math.MinInt64 is representable.
	var acc int64
	for _, part := range []string{intPart, fracPart} {
		for i := 0; i < len(part); i++ {
			ch := part[i]
			if ch < '0' || ch > '9' {
				return invalid()
			}
			d := int64(ch - '0')
			if acc < (math.MinInt64+d)/10 {
				return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
			}
			acc = acc*10 - d
		}
	}
	if neg {
		return acc, nil
	}
	if acc == math.MinInt64 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	return -acc, nil
}

// MinorUnits returns the amount in minor units.
func (m Money) MinorUnits() int64   { return m.minor }
func (m Money) Currency() Currency  { return m.currency }
func (m Money) IsInitialized() bool { return !m.currency.IsZero() }
func (m Money) IsZero() bool        { return m.minor == 0 }
func (m Money) IsNegative() bool    { return m.minor < 0 }
func (m Money) IsPositive() bool    { return m.minor > 0 }

// Amount returns the canonical decimal representation, e.g. "25.00".
func (m Money) Amount() string {
	var abs uint64
	sign := ""
	if m.minor < 0 {
		sign = "-"
		abs = uint64(-(m.minor + 1)) + 1 // safe for MinInt64
	} else {
		abs = uint64(m.minor)
	}
	frac := abs % minorPerMajor
	return sign + strconv.FormatUint(abs/minorPerMajor, 10) + "." + fmt.Sprintf("%02d", frac)
}

func (m Money) String() string { return m.Amount() + " " + m.currency.code }

func (m Money) compatible(o Money) error {
	if !m.IsInitialized() || !o.IsInitialized() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Neg returns -m.
func (m Money) Neg() (Money, error) {
	if !m.IsInitialized() {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Equal reports whether both values have the same amount and currency.
func (m Money) Equal(o Money) bool { return m.minor == o.minor && m.currency == o.currency }

type jsonMoney struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON renders {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsInitialized() {
		return nil, ErrUninitialized
	}
	return json.Marshal(jsonMoney{Amount: m.Amount(), Currency: m.currency.code})
}

// UnmarshalJSON accepts only the string form of the amount; JSON numbers are
// rejected so that no decoder can route the value through a float.
func (m *Money) UnmarshalJSON(b []byte) error {
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	if len(raw.Amount) == 0 || raw.Amount[0] != '"' {
		return fmt.Errorf("%w: amount must be a JSON string", ErrInvalidAmount)
	}
	var amount string
	if err := json.Unmarshal(raw.Amount, &amount); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	parsed, err := ParseWithCode(amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

// FromMinorCode builds Money from minor units and a currency code.
func FromMinorCode(minor int64, code string) (Money, error) {
	c, err := ParseCurrency(code)
	if err != nil {
		return Money{}, err
	}
	return FromMinor(minor, c)
}
