package ledger

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// ParseAmount validates and parses a positive decimal amount string.
// Returns an error if the value is empty, malformed, zero, or negative.
func ParseAmount(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Decimal{}, errors.New("amount is empty")
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid amount %q: %w", s, err)
	}
	if !d.IsPositive() {
		return decimal.Decimal{}, fmt.Errorf("amount must be > 0, got %s", s)
	}
	return d, nil
}
