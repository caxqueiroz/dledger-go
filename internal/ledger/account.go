package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

type NormalBalance string

const (
	NormalDebit  NormalBalance = "DEBIT"
	NormalCredit NormalBalance = "CREDIT"
)

func (n NormalBalance) Valid() bool {
	return n == NormalDebit || n == NormalCredit
}

type AccountStatus string

const (
	AccountActive AccountStatus = "ACTIVE"
	AccountFrozen AccountStatus = "FROZEN"
	AccountClosed AccountStatus = "CLOSED"
)

func (s AccountStatus) Valid() bool {
	switch s {
	case AccountActive, AccountFrozen, AccountClosed:
		return true
	}
	return false
}

// Account is the canonical ledger account record.
type Account struct {
	ID            string
	TenantID      string
	OwnerType     string
	OwnerID       string
	AccountType   string
	Currency      string
	NormalBalance NormalBalance
	AllowNegative bool
	Status        AccountStatus
	CreatedAt     time.Time
}

// NormalizedBalance returns posted_debits - posted_credits for debit-normal
// accounts, and the inverse for credit-normal accounts.
func NormalizedBalance(nb NormalBalance, postedDebits, postedCredits decimal.Decimal) decimal.Decimal {
	switch nb {
	case NormalCredit:
		return postedCredits.Sub(postedDebits)
	default:
		return postedDebits.Sub(postedCredits)
	}
}
