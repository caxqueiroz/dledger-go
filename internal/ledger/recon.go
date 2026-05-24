// recon.go declares the reconciliation domain types.
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

type ExternalRecordStatus string

const (
	ExternalUnmatched  ExternalRecordStatus = "UNMATCHED"
	ExternalMatched    ExternalRecordStatus = "MATCHED"
	ExternalMismatched ExternalRecordStatus = "MISMATCHED"
)

type ExternalRecord struct {
	ID               string
	TenantID         string
	Source           string
	ExternalRef      string
	Amount           decimal.Decimal
	Currency         string
	OccurredAt       time.Time
	AccountID        string
	RawPayload       map[string]any
	MatchStatus      ExternalRecordStatus
	MatchedJournalID string
	CreatedAt        time.Time
}

type BatchStatus string

const (
	BatchRunning   BatchStatus = "RUNNING"
	BatchCompleted BatchStatus = "COMPLETED"
	BatchFailed    BatchStatus = "FAILED"
)

type ReconciliationBatch struct {
	ID                     string
	TenantID               string
	IdempotencyKey         string
	Source                 string
	WindowStart            time.Time
	WindowEnd              time.Time
	Status                 BatchStatus
	IngestedCount          int32
	MatchedCount           int32
	MismatchedCount        int32
	MissingInLedgerCount   int32
	MissingInExternalCount int32
	StartedAt              time.Time
	CompletedAt            time.Time
	ActorID                string
}

type DiscrepancyType string

const (
	DiscrepancyMissingInLedger   DiscrepancyType = "MISSING_IN_LEDGER"
	DiscrepancyMissingInExternal DiscrepancyType = "MISSING_IN_EXTERNAL"
	DiscrepancyAmountMismatch    DiscrepancyType = "AMOUNT_MISMATCH"
)

type DiscrepancyStatus string

const (
	DiscrepancyOpen     DiscrepancyStatus = "OPEN"
	DiscrepancyResolved DiscrepancyStatus = "RESOLVED"
	DiscrepancyIgnored  DiscrepancyStatus = "IGNORED"
)

// Closed reports whether s is a terminal status.
func (s DiscrepancyStatus) Closed() bool {
	return s == DiscrepancyResolved || s == DiscrepancyIgnored
}

type Discrepancy struct {
	ID                  string
	TenantID            string
	BatchID             string
	Type                DiscrepancyType
	ExternalRecordID    string
	JournalID           string
	Status              DiscrepancyStatus
	ResolutionJournalID string
	ResolutionNote      string
	ResolvedBy          string
	ResolvedAt          time.Time
	CreatedAt           time.Time
}
