package sqlite

import (
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"

	sqlitestore "github.com/caxqueiroz/dledger-go/gen/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func rowToExternalRecord(r sqlitestore.ExternalRecord) *ledger.ExternalRecord {
	amt, _ := decimal.NewFromString(r.Amount)
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.RawPayload), &meta)
	res := &ledger.ExternalRecord{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: amt, Currency: r.Currency,
		OccurredAt:  parseTime(r.OccurredAt),
		RawPayload:  meta,
		MatchStatus: ledger.ExternalRecordStatus(r.MatchStatus),
		CreatedAt:   parseTime(r.CreatedAt),
	}
	if r.AccountID != nil {
		res.AccountID = *r.AccountID
	}
	if r.MatchedJournalID != nil {
		res.MatchedJournalID = *r.MatchedJournalID
	}
	return res
}

func rowToReconBatch(r sqlitestore.ReconciliationBatch) *ledger.ReconciliationBatch {
	b := &ledger.ReconciliationBatch{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		Source:                 r.Source,
		WindowStart:            parseTime(r.WindowStart),
		WindowEnd:              parseTime(r.WindowEnd),
		Status:                 ledger.BatchStatus(r.Status),
		IngestedCount:          int32(r.IngestedCount),
		MatchedCount:           int32(r.MatchedCount),
		MismatchedCount:        int32(r.MismatchedCount),
		MissingInLedgerCount:   int32(r.MissingInLedgerCount),
		MissingInExternalCount: int32(r.MissingInExternalCount),
		StartedAt:              parseTime(r.StartedAt),
		ActorID:                r.ActorID,
	}
	if r.CompletedAt != nil {
		b.CompletedAt = parseTime(*r.CompletedAt)
	}
	return b
}

func rowToDiscrepancy(r sqlitestore.Discrepancy) *ledger.Discrepancy {
	d := &ledger.Discrepancy{
		ID: r.ID, TenantID: r.TenantID, BatchID: r.BatchID,
		Type:           ledger.DiscrepancyType(r.Type),
		Status:         ledger.DiscrepancyStatus(r.Status),
		ResolutionNote: r.ResolutionNote,
		ResolvedBy:     r.ResolvedBy,
		CreatedAt:      parseTime(r.CreatedAt),
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
	if r.ResolvedAt != nil {
		d.ResolvedAt = parseTime(*r.ResolvedAt)
	}
	return d
}

func rowToJournal(r sqlitestore.LedgerJournal) *ledger.Journal {
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	j := &ledger.Journal{
		ID: r.ID, TenantID: r.TenantID,
		EventID: r.EventID, SourceService: r.SourceService, SourceType: r.SourceType,
		ActorID: r.ActorID, Metadata: meta,
		CreatedAt: parseTime(r.CreatedAt),
	}
	if r.FlowRunID != nil {
		j.FlowRunID = *r.FlowRunID
	}
	return j
}

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

// rowToSnapshot converts a sqlitestore.BalanceSnapshot to a ledger.BalanceSnapshot.
func rowToSnapshot(r sqlitestore.BalanceSnapshot) *ledger.BalanceSnapshot {
	d, _ := decimal.NewFromString(r.PostedDebits)
	c, _ := decimal.NewFromString(r.PostedCredits)
	return &ledger.BalanceSnapshot{
		ID:            r.ID,
		TenantID:      r.TenantID,
		AccountID:     r.AccountID,
		Currency:      r.Currency,
		PostedDebits:  d,
		PostedCredits: c,
		Version:       r.Version,
		SnapshotAt:    parseTime(r.SnapshotAt),
		CreatedAt:     parseTime(r.CreatedAt),
	}
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

// rowToFXRate converts a sqlitestore.FxRate to a ledger.FXRate.
func rowToFXRate(r sqlitestore.FxRate) *ledger.FXRate {
	rate, _ := decimal.NewFromString(r.Rate)
	return &ledger.FXRate{
		ID: r.ID, TenantID: r.TenantID,
		BaseCurrency: r.BaseCurrency, QuoteCurrency: r.QuoteCurrency,
		Rate: rate, Source: r.Source,
		EffectiveAt: parseTime(r.EffectiveAt),
		CreatedAt:   parseTime(r.CreatedAt),
	}
}

// rowToReservation converts a sqlitestore.Reservation to a ledger.Reservation.
func rowToReservation(r sqlitestore.Reservation) *ledger.Reservation {
	orig, _ := decimal.NewFromString(r.OriginalAmount)
	out, _ := decimal.NewFromString(r.OutstandingAmount)
	com, _ := decimal.NewFromString(r.CommittedAmount)
	rel, _ := decimal.NewFromString(r.ReleasedAmount)
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	res := &ledger.Reservation{
		ID:                r.ID,
		TenantID:          r.TenantID,
		IdempotencyKey:    r.IdempotencyKey,
		SourceAccountID:   r.SourceAccountID,
		ReservedAccountID: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    orig,
		OutstandingAmount: out,
		CommittedAmount:   com,
		ReleasedAmount:    rel,
		Status:            ledger.ReservationStatus(r.Status),
		FlowRunID:         r.FlowRunID,
		Metadata:          meta,
		CreatedAt:         parseTime(r.CreatedAt),
		UpdatedAt:         parseTime(r.UpdatedAt),
	}
	if r.ExpiresAt != nil {
		t := parseTime(*r.ExpiresAt)
		res.ExpiresAt = &t
	}
	return res
}
