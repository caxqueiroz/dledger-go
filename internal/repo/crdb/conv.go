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

// rowToSnapshot converts a crdbstore.BalanceSnapshot to a ledger.BalanceSnapshot.
func rowToSnapshot(r crdbstore.BalanceSnapshot) *ledger.BalanceSnapshot {
	s := &ledger.BalanceSnapshot{
		ID:            r.ID,
		TenantID:      r.TenantID,
		AccountID:     r.AccountID,
		Currency:      r.Currency,
		PostedDebits:  r.PostedDebits,
		PostedCredits: r.PostedCredits,
		Version:       r.Version,
	}
	if r.SnapshotAt.Valid {
		s.SnapshotAt = r.SnapshotAt.Time
	}
	if r.CreatedAt.Valid {
		s.CreatedAt = r.CreatedAt.Time
	}
	return s
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

// rowToReservation converts a crdbstore.Reservation to a ledger.Reservation.
func rowToReservation(r crdbstore.Reservation) *ledger.Reservation {
	meta := map[string]any{}
	if len(r.Metadata) > 0 {
		_ = json.Unmarshal(r.Metadata, &meta)
	}
	res := &ledger.Reservation{
		ID:                r.ID,
		TenantID:          r.TenantID,
		IdempotencyKey:    r.IdempotencyKey,
		SourceAccountID:   r.SourceAccountID,
		ReservedAccountID: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount,
		OutstandingAmount: r.OutstandingAmount,
		CommittedAmount:   r.CommittedAmount,
		ReleasedAmount:    r.ReleasedAmount,
		Status:            ledger.ReservationStatus(r.Status),
		FlowRunID:         r.FlowRunID,
		Metadata:          meta,
	}
	if r.ExpiresAt.Valid {
		t := r.ExpiresAt.Time
		res.ExpiresAt = &t
	}
	if r.CreatedAt.Valid {
		res.CreatedAt = r.CreatedAt.Time
	}
	if r.UpdatedAt.Valid {
		res.UpdatedAt = r.UpdatedAt.Time
	}
	return res
}
