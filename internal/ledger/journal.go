package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

func (d Direction) Valid() bool { return d == DirectionDebit || d == DirectionCredit }

type Entry struct {
	AccountID string
	Currency  string
	Direction Direction
	Amount    decimal.Decimal
}

type Journal struct {
	ID            string
	TenantID      string
	FlowRunID     string
	EventID       string
	SourceService string
	SourceType    string
	ActorID       string
	Metadata      map[string]any
	Entries       []Entry
	CreatedAt     time.Time
}

// Validate enforces the core accounting rule: per-currency debit/credit equality.
// It also rejects empty journals and malformed entries.
func (j *Journal) Validate() error {
	if j.EventID == "" {
		return errors.New("journal: event_id is required")
	}
	if len(j.Entries) == 0 {
		return errors.New("journal: at least one entry required")
	}
	sums := map[string]decimal.Decimal{}
	for i, e := range j.Entries {
		if e.AccountID == "" {
			return fmt.Errorf("journal: entry[%d]: account_id required", i)
		}
		if e.Currency == "" {
			return fmt.Errorf("journal: entry[%d]: currency required", i)
		}
		if !e.Direction.Valid() {
			return fmt.Errorf("journal: entry[%d]: invalid direction %q", i, e.Direction)
		}
		if !e.Amount.IsPositive() {
			return fmt.Errorf("journal: entry[%d]: amount must be > 0", i)
		}
		signed := e.Amount
		if e.Direction == DirectionCredit {
			signed = signed.Neg()
		}
		sums[e.Currency] = sums[e.Currency].Add(signed)
	}
	for ccy, sum := range sums {
		if !sum.IsZero() {
			return fmt.Errorf("journal: unbalanced %s: debits-credits=%s", ccy, sum)
		}
	}
	return nil
}
