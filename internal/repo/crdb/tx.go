package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/dledger-go/gen/crdb"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// Tx wraps a pgx.Tx and implements repo.Tx for CockroachDB.
type Tx struct {
	tx pgx.Tx
	q  *crdbstore.Queries
}

// Commit commits the transaction.
func (t *Tx) Commit() error { return t.tx.Commit(context.Background()) }

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error { return t.tx.Rollback(context.Background()) }

// GetFlowByIdempotency looks up an existing FlowRun by idempotency key within
// the transaction (FOR UPDATE in the underlying query). Returns nil if not found.
func (t *Tx) GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error) {
	row, err := t.q.GetFlowByIdempotency(ctx, crdbstore.GetFlowByIdempotencyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFlowRun(row), nil
}

// GetAccount fetches an account within the transaction context.
func (t *Tx) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := t.q.GetAccount(ctx, crdbstore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

// EnsureBalanceRow upserts a zero balance row, doing nothing if one exists.
func (t *Tx) EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error {
	return t.q.UpsertBalanceZero(ctx, crdbstore.UpsertBalanceZeroParams{
		TenantID:  tenantID,
		AccountID: accountID,
		Currency:  currency,
	})
}

// LockBalance ensures the balance row exists and acquires a FOR UPDATE lock on it.
func (t *Tx) LockBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	if err := t.EnsureBalanceRow(ctx, tenantID, accountID, currency); err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	row, err := t.q.LockBalance(ctx, crdbstore.LockBalanceParams{
		TenantID:  tenantID,
		AccountID: accountID,
		Currency:  currency,
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	return row.PostedDebits, row.PostedCredits, row.Version, nil
}

// UpdateBalance overwrites the posted_debits and posted_credits columns and
// increments the version.
func (t *Tx) UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error {
	return t.q.UpdateBalance(ctx, crdbstore.UpdateBalanceParams{
		TenantID:      tenantID,
		AccountID:     accountID,
		Currency:      currency,
		PostedDebits:  postedDebits,
		PostedCredits: postedCredits,
	})
}

// InsertAccount inserts a new account row.
func (t *Tx) InsertAccount(ctx context.Context, a ledger.Account) error {
	return t.q.InsertAccount(ctx, crdbstore.InsertAccountParams{
		ID:            a.ID,
		TenantID:      a.TenantID,
		OwnerType:     a.OwnerType,
		OwnerID:       a.OwnerID,
		AccountType:   a.AccountType,
		Currency:      a.Currency,
		NormalBalance: string(a.NormalBalance),
		AllowNegative: a.AllowNegative,
		Status:        string(a.Status),
	})
}

// InsertFlowRun inserts a new flow_run row in RUNNING state.
func (t *Tx) InsertFlowRun(ctx context.Context, f ledger.FlowRun) error {
	meta, _ := json.Marshal(f.Metadata)
	return t.q.InsertFlowRun(ctx, crdbstore.InsertFlowRunParams{
		ID:             f.ID,
		TenantID:       f.TenantID,
		FlowType:       f.FlowType,
		IdempotencyKey: f.IdempotencyKey,
		SourceService:  f.SourceService,
		ActorID:        f.ActorID,
		Metadata:       meta,
	})
}

// CompleteFlowRun marks a flow run as COMPLETED and sets completed_at.
func (t *Tx) CompleteFlowRun(ctx context.Context, tenantID, flowRunID string) error {
	return t.q.CompleteFlowRun(ctx, crdbstore.CompleteFlowRunParams{
		ID:       flowRunID,
		TenantID: tenantID,
	})
}

// InsertJournal inserts a new ledger journal.
func (t *Tx) InsertJournal(ctx context.Context, j ledger.Journal) error {
	meta, _ := json.Marshal(j.Metadata)
	return t.q.InsertJournal(ctx, crdbstore.InsertJournalParams{
		ID:            j.ID,
		TenantID:      j.TenantID,
		FlowRunID:     nullString(j.FlowRunID),
		EventID:       j.EventID,
		SourceService: j.SourceService,
		SourceType:    j.SourceType,
		ActorID:       j.ActorID,
		Metadata:      meta,
	})
}

// InsertEntry inserts a single ledger entry.
// entryID must be a valid UUID string (e.g. from github.com/google/uuid).
func (t *Tx) InsertEntry(ctx context.Context, tenantID, entryID, journalID, accountID, currency string, dir ledger.Direction, amount decimal.Decimal) error {
	uid, err := parseUUID(entryID)
	if err != nil {
		return fmt.Errorf("insert entry: invalid uuid %q: %w", entryID, err)
	}
	return t.q.InsertEntry(ctx, crdbstore.InsertEntryParams{
		ID:        uid,
		TenantID:  tenantID,
		JournalID: journalID,
		AccountID: accountID,
		Currency:  currency,
		Direction: string(dir),
		Amount:    amount,
	})
}

// InsertFlowStep inserts a flow step record.
func (t *Tx) InsertFlowStep(ctx context.Context, s ledger.FlowStep) error {
	return t.q.InsertFlowStep(ctx, crdbstore.InsertFlowStepParams{
		ID:        s.ID,
		TenantID:  s.TenantID,
		FlowRunID: s.FlowRunID,
		StepID:    s.StepID,
		Status:    string(s.Status),
		JournalID: nullString(s.JournalID),
		ErrorCode: nullString(s.ErrorCode),
	})
}

// InsertOutbox inserts an outbox event in PENDING state.
func (t *Tx) InsertOutbox(ctx context.Context, e repo.OutboxEvent) error {
	return t.q.InsertOutbox(ctx, crdbstore.InsertOutboxParams{
		ID:             e.ID,
		TenantID:       e.TenantID,
		AggregateID:    e.AggregateID,
		EventType:      e.EventType,
		IdempotencyKey: e.IdempotencyKey,
		Payload:        e.Payload,
	})
}

// GetFlowSteps returns all steps for a given flow run in ascending order.
func (t *Tx) GetFlowSteps(ctx context.Context, tenantID, flowRunID string) ([]ledger.FlowStep, error) {
	rows, err := t.q.GetFlowSteps(ctx, crdbstore.GetFlowStepsParams{
		TenantID:  tenantID,
		FlowRunID: flowRunID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.FlowStep, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToFlowStep(r))
	}
	return out, nil
}

// InsertReservation inserts a new reservation row.
func (t *Tx) InsertReservation(ctx context.Context, r ledger.Reservation) error {
	metaBytes, _ := json.Marshal(r.Metadata)
	expires := pgtype.Timestamptz{}
	if r.ExpiresAt != nil {
		expires = pgtype.Timestamptz{Time: r.ExpiresAt.UTC(), Valid: true}
	}
	return t.q.InsertReservation(ctx, crdbstore.InsertReservationParams{
		ID:                r.ID,
		TenantID:          r.TenantID,
		IdempotencyKey:    r.IdempotencyKey,
		SourceAccountID:   r.SourceAccountID,
		ReservedAccountID: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount,
		OutstandingAmount: r.OutstandingAmount,
		Status:            string(r.Status),
		ExpiresAt:         expires,
		FlowRunID:         r.FlowRunID,
		Metadata:          metaBytes,
	})
}

// LockReservation fetches a reservation with a FOR UPDATE lock within the transaction.
func (t *Tx) LockReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	row, err := t.q.LockReservation(ctx, crdbstore.LockReservationParams{TenantID: tenantID, ID: reservationID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

// GetReservationByIdempotency looks up a reservation by idempotency key.
// Returns nil, nil if not found.
func (t *Tx) GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error) {
	row, err := t.q.GetReservationByIdempotency(ctx, crdbstore.GetReservationByIdempotencyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

// UpdateReservationAmounts updates the amounts and status of a reservation.
func (t *Tx) UpdateReservationAmounts(ctx context.Context, tenantID, reservationID string, outstanding, committed, released decimal.Decimal, status ledger.ReservationStatus) error {
	return t.q.UpdateReservationAmounts(ctx, crdbstore.UpdateReservationAmountsParams{
		OutstandingAmount: outstanding,
		CommittedAmount:   committed,
		ReleasedAmount:    released,
		Status:            string(status),
		TenantID:          tenantID,
		ID:                reservationID,
	})
}

// nullString converts an empty string to nil, otherwise returns a pointer to the value.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	v := s
	return &v
}

// ListExternalRecordsForRecon returns UNMATCHED external records for the given
// source within the time window, executed inside the current transaction.
func (t *Tx) ListExternalRecordsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.ExternalRecord, error) {
	rows, err := t.q.ListExternalRecordsForRecon(ctx, crdbstore.ListExternalRecordsForReconParams{
		TenantID:     tenantID,
		Source:       source,
		OccurredAt:   pgtype.Timestamptz{Time: windowStart.UTC(), Valid: true},
		OccurredAt_2: pgtype.Timestamptz{Time: windowEnd.UTC(), Valid: true},
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.ExternalRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToExternalRecord(r))
	}
	return out, nil
}

// ListJournalsForRecon returns ledger journals for the given source_service within
// the time window, executed inside the current transaction.
func (t *Tx) ListJournalsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.Journal, error) {
	rows, err := t.q.ListJournalsForRecon(ctx, crdbstore.ListJournalsForReconParams{
		TenantID:      tenantID,
		SourceService: source,
		CreatedAt:     pgtype.Timestamptz{Time: windowStart.UTC(), Valid: true},
		CreatedAt_2:   pgtype.Timestamptz{Time: windowEnd.UTC(), Valid: true},
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Journal, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToJournal(r))
	}
	return out, nil
}

// GetReconBatchByIdempotency looks up a reconciliation batch by idempotency key within
// the transaction (FOR UPDATE in the underlying query). Returns nil if not found.
func (t *Tx) GetReconBatchByIdempotency(ctx context.Context, tenantID, key string) (*ledger.ReconciliationBatch, error) {
	row, err := t.q.GetReconBatchByIdempotency(ctx, crdbstore.GetReconBatchByIdempotencyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReconBatch(row), nil
}

// InsertReconBatch inserts a new reconciliation batch in RUNNING state.
func (t *Tx) InsertReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error {
	return t.q.InsertReconBatch(ctx, crdbstore.InsertReconBatchParams{
		ID:             b.ID,
		TenantID:       b.TenantID,
		IdempotencyKey: b.IdempotencyKey,
		Source:         b.Source,
		WindowStart:    pgtype.Timestamptz{Time: b.WindowStart.UTC(), Valid: true},
		WindowEnd:      pgtype.Timestamptz{Time: b.WindowEnd.UTC(), Valid: true},
		ActorID:        b.ActorID,
	})
}

// CompleteReconBatch marks a batch as COMPLETED and writes the final counts.
func (t *Tx) CompleteReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error {
	return t.q.CompleteReconBatch(ctx, crdbstore.CompleteReconBatchParams{
		IngestedCount:          int64(b.IngestedCount),
		MatchedCount:           int64(b.MatchedCount),
		MismatchedCount:        int64(b.MismatchedCount),
		MissingInLedgerCount:   int64(b.MissingInLedgerCount),
		MissingInExternalCount: int64(b.MissingInExternalCount),
		ID:                     b.ID,
		TenantID:               b.TenantID,
	})
}

// UpdateExternalRecordMatch updates the match status (and optional journal ID) for an external record.
func (t *Tx) UpdateExternalRecordMatch(ctx context.Context, tenantID, id string, status ledger.ExternalRecordStatus, journalID string) error {
	return t.q.UpdateExternalRecordMatch(ctx, crdbstore.UpdateExternalRecordMatchParams{
		MatchStatus:      string(status),
		MatchedJournalID: nullString(journalID),
		ID:               id,
		TenantID:         tenantID,
	})
}

// SumJournalEntries returns the total debits and credits for a journal scoped by account and currency.
func (t *Tx) SumJournalEntries(ctx context.Context, tenantID, journalID, accountID, currency string) (decimal.Decimal, decimal.Decimal, error) {
	row, err := t.q.SumJournalEntries(ctx, crdbstore.SumJournalEntriesParams{
		TenantID:  tenantID,
		JournalID: journalID,
		AccountID: accountID,
		Currency:  currency,
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	return row.Debits, row.Credits, nil
}

// InsertDiscrepancy inserts a new discrepancy row.
func (t *Tx) InsertDiscrepancy(ctx context.Context, d ledger.Discrepancy) error {
	return t.q.InsertDiscrepancy(ctx, crdbstore.InsertDiscrepancyParams{
		ID:               d.ID,
		TenantID:         d.TenantID,
		BatchID:          d.BatchID,
		Type:             string(d.Type),
		ExternalRecordID: nullString(d.ExternalRecordID),
		JournalID:        nullString(d.JournalID),
	})
}

// LockDiscrepancy fetches a discrepancy with a FOR UPDATE lock within the transaction.
func (t *Tx) LockDiscrepancy(ctx context.Context, tenantID, id string) (*ledger.Discrepancy, error) {
	row, err := t.q.LockDiscrepancy(ctx, crdbstore.LockDiscrepancyParams{TenantID: tenantID, ID: id})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeDiscrepancyNotFound, id)
		}
		return nil, err
	}
	return rowToDiscrepancy(row), nil
}

// ResolveDiscrepancyRow updates a discrepancy to a resolved/ignored state.
func (t *Tx) ResolveDiscrepancyRow(ctx context.Context, d ledger.Discrepancy) error {
	return t.q.ResolveDiscrepancyRow(ctx, crdbstore.ResolveDiscrepancyRowParams{
		Status:              string(d.Status),
		ResolutionJournalID: nullString(d.ResolutionJournalID),
		ResolutionNote:      d.ResolutionNote,
		ResolvedBy:          d.ResolvedBy,
		ID:                  d.ID,
		TenantID:            d.TenantID,
	})
}

// parseUUID parses a UUID string (with or without hyphens) into pgtype.UUID.
func parseUUID(s string) (pgtype.UUID, error) {
	// Remove hyphens to get 32 hex chars.
	hex := ""
	for _, c := range s {
		if c != '-' {
			hex += string(c)
		}
	}
	if len(hex) != 32 {
		return pgtype.UUID{}, fmt.Errorf("expected 32 hex chars, got %d", len(hex))
	}
	var b [16]byte
	for i := range 16 {
		hi := hexVal(hex[i*2])
		lo := hexVal(hex[i*2+1])
		if hi < 0 || lo < 0 {
			return pgtype.UUID{}, fmt.Errorf("invalid hex at position %d", i*2)
		}
		b[i] = byte(hi<<4 | lo)
	}
	return pgtype.UUID{Bytes: b, Valid: true}, nil
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}
