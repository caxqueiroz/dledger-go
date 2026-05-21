package sqlite

import (
	"encoding/json"
	"time"

	sqlitestore "github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// parseTime parses an RFC3339Nano timestamp string, returning zero time on failure.
func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// rowToAccount converts a sqlitestore.Account to a ledger.Account.
func rowToAccount(r sqlitestore.Account) *ledger.Account {
	return &ledger.Account{
		ID:            r.ID,
		TenantID:      r.TenantID,
		OwnerType:     r.OwnerType,
		OwnerID:       r.OwnerID,
		AccountType:   r.AccountType,
		Currency:      r.Currency,
		NormalBalance: ledger.NormalBalance(r.NormalBalance),
		AllowNegative: r.AllowNegative != 0,
		Status:        ledger.AccountStatus(r.Status),
		CreatedAt:     parseTime(r.CreatedAt),
	}
}

// rowToFlowRun converts a sqlitestore.FlowRun to a ledger.FlowRun.
func rowToFlowRun(r sqlitestore.FlowRun) *ledger.FlowRun {
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	f := &ledger.FlowRun{
		ID:             r.ID,
		TenantID:       r.TenantID,
		FlowType:       r.FlowType,
		IdempotencyKey: r.IdempotencyKey,
		SourceService:  r.SourceService,
		ActorID:        r.ActorID,
		Status:         ledger.FlowStatus(r.Status),
		Metadata:       meta,
		CreatedAt:      parseTime(r.CreatedAt),
	}
	if r.CompletedAt != nil {
		t := parseTime(*r.CompletedAt)
		f.CompletedAt = &t
	}
	if r.FailedAt != nil {
		t := parseTime(*r.FailedAt)
		f.FailedAt = &t
	}
	return f
}

// rowToFlowStep converts a sqlitestore.FlowStep to a ledger.FlowStep.
// JournalID and ErrorCode are nullable *string in the generated model.
func rowToFlowStep(r sqlitestore.FlowStep) *ledger.FlowStep {
	s := &ledger.FlowStep{
		ID:        r.ID,
		TenantID:  r.TenantID,
		FlowRunID: r.FlowRunID,
		StepID:    r.StepID,
		Status:    ledger.StepStatus(r.Status),
		CreatedAt: parseTime(r.CreatedAt),
	}
	if r.JournalID != nil {
		s.JournalID = *r.JournalID
	}
	if r.ErrorCode != nil {
		s.ErrorCode = *r.ErrorCode
	}
	return s
}
