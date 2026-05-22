// internal/ledger/snapshot.go
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// BalanceSnapshot captures account_balances at a logical point in time.
type BalanceSnapshot struct {
	ID            string
	TenantID      string
	AccountID     string
	Currency      string
	PostedDebits  decimal.Decimal
	PostedCredits decimal.Decimal
	Version       int64
	SnapshotAt    time.Time
	CreatedAt     time.Time
}
