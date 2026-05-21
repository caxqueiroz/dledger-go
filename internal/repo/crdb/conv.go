package crdb

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	crdbstore "github.com/caxqueiroz/doubleledger/gen/crdb"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// timestamptzToTime converts a pgtype.Timestamptz to time.Time.
// Returns zero time if the value is not valid.
func timestamptzToTime(ts pgtype.Timestamptz) time.Time {
	if !ts.Valid {
		return time.Time{}
	}
	return ts.Time
}

// timestamptzToTimePtr converts a pgtype.Timestamptz to *time.Time.
// Returns nil if not valid.
func timestamptzToTimePtr(ts pgtype.Timestamptz) *time.Time {
	if !ts.Valid {
		return nil
	}
	t := ts.Time
	return &t
}

func rowToAccount(r crdbstore.Account) *ledger.Account {
	return &ledger.Account{
		ID:            r.ID,
		TenantID:      r.TenantID,
		OwnerType:     r.OwnerType,
		OwnerID:       r.OwnerID,
		AccountType:   r.AccountType,
		Currency:      r.Currency,
		NormalBalance: ledger.NormalBalance(r.NormalBalance),
		AllowNegative: r.AllowNegative,
		Status:        ledger.AccountStatus(r.Status),
		CreatedAt:     timestamptzToTime(r.CreatedAt),
	}
}

func rowToFlowRun(r crdbstore.FlowRun) *ledger.FlowRun {
	meta := map[string]any{}
	if len(r.Metadata) > 0 {
		_ = json.Unmarshal(r.Metadata, &meta)
	}
	return &ledger.FlowRun{
		ID:             r.ID,
		TenantID:       r.TenantID,
		FlowType:       r.FlowType,
		IdempotencyKey: r.IdempotencyKey,
		SourceService:  r.SourceService,
		ActorID:        r.ActorID,
		Status:         ledger.FlowStatus(r.Status),
		Metadata:       meta,
		CreatedAt:      timestamptzToTime(r.CreatedAt),
		CompletedAt:    timestamptzToTimePtr(r.CompletedAt),
		FailedAt:       timestamptzToTimePtr(r.FailedAt),
	}
}

func rowToFlowStep(r crdbstore.FlowStep) *ledger.FlowStep {
	s := &ledger.FlowStep{
		ID:        r.ID,
		TenantID:  r.TenantID,
		FlowRunID: r.FlowRunID,
		StepID:    r.StepID,
		Status:    ledger.StepStatus(r.Status),
		CreatedAt: timestamptzToTime(r.CreatedAt),
	}
	if r.JournalID != nil {
		s.JournalID = *r.JournalID
	}
	if r.ErrorCode != nil {
		s.ErrorCode = *r.ErrorCode
	}
	return s
}
