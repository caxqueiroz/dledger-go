// fx.go declares the FXRate domain type.
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// FXRate is a tenant-scoped historical foreign-exchange rate row.
type FXRate struct {
	ID            string
	TenantID      string
	BaseCurrency  string
	QuoteCurrency string
	Rate          decimal.Decimal
	Source        string
	EffectiveAt   time.Time
	CreatedAt     time.Time
}
