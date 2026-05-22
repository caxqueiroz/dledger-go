package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"

	sqlitestore "github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

// Tx is a SQLite write transaction backed by a dedicated *sql.Conn.
type Tx struct {
	db   *sql.DB
	conn *sql.Conn
	q    *sqlitestore.Queries
	done bool
}

func (t *Tx) finalize(stmt string) error {
	if t.done {
		return nil
	}
	t.done = true
	_, err := t.conn.ExecContext(context.Background(), stmt)
	closeErr := t.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// Commit commits the transaction.
func (t *Tx) Commit() error { return t.finalize("COMMIT") }

// Rollback rolls back the transaction.
func (t *Tx) Rollback() error { return t.finalize("ROLLBACK") }

// GetFlowByIdempotency looks up an existing FlowRun by idempotency key.
// Returns nil if not found.
func (t *Tx) GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error) {
	row, err := t.q.GetFlowByIdempotency(ctx, sqlitestore.GetFlowByIdempotencyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFlowRun(row), nil
}

// GetAccount fetches an account within the transaction context.
func (t *Tx) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := t.q.GetAccount(ctx, sqlitestore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

// EnsureBalanceRow upserts a zero balance row, doing nothing if one exists.
func (t *Tx) EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error {
	return t.q.UpsertBalanceZero(ctx, sqlitestore.UpsertBalanceZeroParams{
		TenantID:  tenantID,
		AccountID: accountID,
		Currency:  currency,
	})
}

// LockBalance ensures the balance row exists and reads it within the transaction,
// effectively locking it for the duration of the BEGIN IMMEDIATE transaction.
func (t *Tx) LockBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	if err := t.EnsureBalanceRow(ctx, tenantID, accountID, currency); err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	row, err := t.q.GetBalance(ctx, sqlitestore.GetBalanceParams{
		TenantID:  tenantID,
		AccountID: accountID,
		Currency:  currency,
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	d, err := decimal.NewFromString(row.PostedDebits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_debits: %w", err)
	}
	c, err := decimal.NewFromString(row.PostedCredits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_credits: %w", err)
	}
	return d, c, row.Version, nil
}

// UpdateBalance overwrites the posted_debits and posted_credits columns and
// increments the version.
func (t *Tx) UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error {
	return t.q.UpdateBalance(ctx, sqlitestore.UpdateBalanceParams{
		PostedDebits:  postedDebits.String(),
		PostedCredits: postedCredits.String(),
		TenantID:      tenantID,
		AccountID:     accountID,
		Currency:      currency,
	})
}

// InsertAccount inserts a new account row.
func (t *Tx) InsertAccount(ctx context.Context, a ledger.Account) error {
	allow := int64(0)
	if a.AllowNegative {
		allow = 1
	}
	return t.q.InsertAccount(ctx, sqlitestore.InsertAccountParams{
		ID:            a.ID,
		TenantID:      a.TenantID,
		OwnerType:     a.OwnerType,
		OwnerID:       a.OwnerID,
		AccountType:   a.AccountType,
		Currency:      a.Currency,
		NormalBalance: string(a.NormalBalance),
		AllowNegative: allow,
		Status:        string(a.Status),
	})
}

// InsertFlowRun inserts a new flow_run row in RUNNING state.
func (t *Tx) InsertFlowRun(ctx context.Context, f ledger.FlowRun) error {
	meta, _ := json.Marshal(f.Metadata)
	return t.q.InsertFlowRun(ctx, sqlitestore.InsertFlowRunParams{
		ID:             f.ID,
		TenantID:       f.TenantID,
		FlowType:       f.FlowType,
		IdempotencyKey: f.IdempotencyKey,
		SourceService:  f.SourceService,
		ActorID:        f.ActorID,
		Metadata:       string(meta),
	})
}

// CompleteFlowRun marks a flow run as COMPLETED and sets completed_at.
func (t *Tx) CompleteFlowRun(ctx context.Context, tenantID, flowRunID string) error {
	return t.q.CompleteFlowRun(ctx, sqlitestore.CompleteFlowRunParams{
		ID:       flowRunID,
		TenantID: tenantID,
	})
}

// InsertJournal inserts a new ledger journal.
func (t *Tx) InsertJournal(ctx context.Context, j ledger.Journal) error {
	meta, _ := json.Marshal(j.Metadata)
	return t.q.InsertJournal(ctx, sqlitestore.InsertJournalParams{
		ID:            j.ID,
		TenantID:      j.TenantID,
		FlowRunID:     nullString(j.FlowRunID),
		EventID:       j.EventID,
		SourceService: j.SourceService,
		SourceType:    j.SourceType,
		ActorID:       j.ActorID,
		Metadata:      string(meta),
	})
}

// InsertEntry inserts a single ledger entry.
func (t *Tx) InsertEntry(ctx context.Context, tenantID, entryID, journalID, accountID, currency string, dir ledger.Direction, amount decimal.Decimal) error {
	return t.q.InsertEntry(ctx, sqlitestore.InsertEntryParams{
		ID:        entryID,
		TenantID:  tenantID,
		JournalID: journalID,
		AccountID: accountID,
		Currency:  currency,
		Direction: string(dir),
		Amount:    amount.String(),
	})
}

// InsertFlowStep inserts a flow step record.
func (t *Tx) InsertFlowStep(ctx context.Context, s ledger.FlowStep) error {
	return t.q.InsertFlowStep(ctx, sqlitestore.InsertFlowStepParams{
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
	return t.q.InsertOutbox(ctx, sqlitestore.InsertOutboxParams{
		ID:             e.ID,
		TenantID:       e.TenantID,
		AggregateID:    e.AggregateID,
		EventType:      e.EventType,
		IdempotencyKey: e.IdempotencyKey,
		Payload:        string(e.Payload),
	})
}

// GetFlowSteps returns all steps for a given flow run in ascending order.
func (t *Tx) GetFlowSteps(ctx context.Context, tenantID, flowRunID string) ([]ledger.FlowStep, error) {
	rows, err := t.q.GetFlowSteps(ctx, sqlitestore.GetFlowStepsParams{
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
	var expires *string
	if r.ExpiresAt != nil {
		v := r.ExpiresAt.UTC().Format(sqliteTimeFormat)
		expires = &v
	}
	return t.q.InsertReservation(ctx, sqlitestore.InsertReservationParams{
		ID:                r.ID,
		TenantID:          r.TenantID,
		IdempotencyKey:    r.IdempotencyKey,
		SourceAccountID:   r.SourceAccountID,
		ReservedAccountID: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount.String(),
		OutstandingAmount: r.OutstandingAmount.String(),
		Status:            string(r.Status),
		ExpiresAt:         expires,
		FlowRunID:         r.FlowRunID,
		Metadata:          string(metaBytes),
	})
}

// LockReservation fetches a reservation within the transaction.
// SQLite: BEGIN IMMEDIATE serializes writes; a plain SELECT suffices.
func (t *Tx) LockReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	row, err := t.q.GetReservation(ctx, sqlitestore.GetReservationParams{TenantID: tenantID, ID: reservationID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

// GetReservationByIdempotency looks up a reservation by idempotency key.
// Returns nil, nil if not found.
func (t *Tx) GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error) {
	row, err := t.q.GetReservationByIdempotency(ctx, sqlitestore.GetReservationByIdempotencyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

// UpdateReservationAmounts updates the amounts and status of a reservation.
func (t *Tx) UpdateReservationAmounts(ctx context.Context, tenantID, reservationID string, outstanding, committed, released decimal.Decimal, status ledger.ReservationStatus) error {
	return t.q.UpdateReservationAmounts(ctx, sqlitestore.UpdateReservationAmountsParams{
		OutstandingAmount: outstanding.String(),
		CommittedAmount:   committed.String(),
		ReleasedAmount:    released.String(),
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
