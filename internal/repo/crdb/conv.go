package crdb

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/dledger-go/gen/crdb"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
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

// rowToFXRate converts a crdbstore.FxRate to a ledger.FXRate.
func rowToFXRate(r crdbstore.FxRate) *ledger.FXRate {
	res := &ledger.FXRate{
		ID: r.ID, TenantID: r.TenantID,
		BaseCurrency: r.BaseCurrency, QuoteCurrency: r.QuoteCurrency,
		Rate: r.Rate, Source: r.Source,
	}
	if r.EffectiveAt.Valid {
		res.EffectiveAt = r.EffectiveAt.Time
	}
	if r.CreatedAt.Valid {
		res.CreatedAt = r.CreatedAt.Time
	}
	return res
}

// rowToExternalRecord converts a crdbstore.ExternalRecord to a ledger.ExternalRecord.
func rowToExternalRecord(r crdbstore.ExternalRecord) *ledger.ExternalRecord {
	meta := map[string]any{}
	if len(r.RawPayload) > 0 {
		_ = json.Unmarshal(r.RawPayload, &meta)
	}
	res := &ledger.ExternalRecord{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount, Currency: r.Currency,
		RawPayload:  meta,
		MatchStatus: ledger.ExternalRecordStatus(r.MatchStatus),
	}
	if r.OccurredAt.Valid {
		res.OccurredAt = r.OccurredAt.Time
	}
	if r.CreatedAt.Valid {
		res.CreatedAt = r.CreatedAt.Time
	}
	if r.AccountID != nil {
		res.AccountID = *r.AccountID
	}
	if r.MatchedJournalID != nil {
		res.MatchedJournalID = *r.MatchedJournalID
	}
	return res
}

// rowToReconBatch converts a crdbstore.ReconciliationBatch to a ledger.ReconciliationBatch.
func rowToReconBatch(r crdbstore.ReconciliationBatch) *ledger.ReconciliationBatch {
	b := &ledger.ReconciliationBatch{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		Source:                 r.Source,
		Status:                 ledger.BatchStatus(r.Status),
		IngestedCount:          int32(r.IngestedCount),
		MatchedCount:           int32(r.MatchedCount),
		MismatchedCount:        int32(r.MismatchedCount),
		MissingInLedgerCount:   int32(r.MissingInLedgerCount),
		MissingInExternalCount: int32(r.MissingInExternalCount),
		ActorID:                r.ActorID,
	}
	if r.WindowStart.Valid {
		b.WindowStart = r.WindowStart.Time
	}
	if r.WindowEnd.Valid {
		b.WindowEnd = r.WindowEnd.Time
	}
	if r.StartedAt.Valid {
		b.StartedAt = r.StartedAt.Time
	}
	if r.CompletedAt.Valid {
		b.CompletedAt = r.CompletedAt.Time
	}
	return b
}

// rowToDiscrepancy converts a crdbstore.Discrepancy to a ledger.Discrepancy.
func rowToDiscrepancy(r crdbstore.Discrepancy) *ledger.Discrepancy {
	d := &ledger.Discrepancy{
		ID: r.ID, TenantID: r.TenantID, BatchID: r.BatchID,
		Type:           ledger.DiscrepancyType(r.Type),
		Status:         ledger.DiscrepancyStatus(r.Status),
		ResolutionNote: r.ResolutionNote,
		ResolvedBy:     r.ResolvedBy,
	}
	if r.ExternalRecordID != nil {
		d.ExternalRecordID = *r.ExternalRecordID
	}
	if r.JournalID != nil {
		d.JournalID = *r.JournalID
	}
	if r.ResolutionJournalID != nil {
		d.ResolutionJournalID = *r.ResolutionJournalID
	}
	if r.ResolvedAt.Valid {
		d.ResolvedAt = r.ResolvedAt.Time
	}
	if r.CreatedAt.Valid {
		d.CreatedAt = r.CreatedAt.Time
	}
	return d
}

// rowToJournal converts a crdbstore.LedgerJournal to a ledger.Journal.
func rowToJournal(r crdbstore.LedgerJournal) *ledger.Journal {
	meta := map[string]any{}
	if len(r.Metadata) > 0 {
		_ = json.Unmarshal(r.Metadata, &meta)
	}
	j := &ledger.Journal{
		ID: r.ID, TenantID: r.TenantID,
		EventID: r.EventID, SourceService: r.SourceService, SourceType: r.SourceType,
		ActorID: r.ActorID, Metadata: meta,
	}
	if r.FlowRunID != nil {
		j.FlowRunID = *r.FlowRunID
	}
	if r.CreatedAt.Valid {
		j.CreatedAt = r.CreatedAt.Time
	}
	return j
}

// ensure decimal import is used (referenced by rowToExternalRecord via r.Amount).
var _ = decimal.Zero

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
