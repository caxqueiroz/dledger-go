package crdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/dledger-go/gen/crdb"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// Store wraps a pgxpool.Pool and implements repo.Store for CockroachDB.
type Store struct {
	pool *pgxpool.Pool
	q    *crdbstore.Queries
}

// Open parses dsn, connects to CockroachDB, and returns a ready Store.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Store{pool: pool, q: crdbstore.New(pool)}, nil
}

// Close closes the underlying connection pool.
func (s *Store) Close() error { s.pool.Close(); return nil }

// Pool returns the underlying *pgxpool.Pool, used by migration and test code.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// BeginFlowTx opens a SERIALIZABLE transaction and returns a Tx.
func (s *Store) BeginFlowTx(ctx context.Context) (repo.Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, q: crdbstore.New(tx)}, nil
}

// GetAccount fetches a single account by tenant and account ID.
func (s *Store) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := s.q.GetAccount(ctx, crdbstore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, "account "+accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

// GetBalance returns postedDebits, postedCredits, and version for a balance row.
// Returns zero values (not an error) when no row exists yet.
func (s *Store) GetBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	row, err := s.q.GetBalance(ctx, crdbstore.GetBalanceParams{TenantID: tenantID, AccountID: accountID, Currency: currency})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return decimal.Zero, decimal.Zero, 0, nil
		}
		return decimal.Zero, decimal.Zero, 0, err
	}
	return row.PostedDebits, row.PostedCredits, row.Version, nil
}

// GetFlow fetches a FlowRun with its steps by ID. Returns nil if not found.
func (s *Store) GetFlow(ctx context.Context, tenantID, flowRunID string) (*ledger.FlowRun, error) {
	row, err := s.q.GetFlowByID(ctx, crdbstore.GetFlowByIDParams{TenantID: tenantID, ID: flowRunID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	f := rowToFlowRun(row)
	steps, err := s.q.GetFlowSteps(ctx, crdbstore.GetFlowStepsParams{TenantID: tenantID, FlowRunID: flowRunID})
	if err != nil {
		return nil, err
	}
	for _, st := range steps {
		f.Steps = append(f.Steps, *rowToFlowStep(st))
	}
	return f, nil
}

// ListAccountActivity returns ledger entries for an account filtered by time range.
// Since/Until are encoded as pgtype.Timestamptz; Valid=false means "no filter".
func (s *Store) ListAccountActivity(ctx context.Context, in repo.ListActivityInput) ([]repo.ActivityRow, error) {
	limit := int32(in.Limit)
	if limit <= 0 {
		limit = 100
	}

	// Column4 = Since filter, Column5 = Until filter.
	// Valid=false causes the IS NULL check in the SQL to pass (no filter applied).
	var since, until pgtype.Timestamptz
	if in.Since != nil {
		since = pgtype.Timestamptz{Time: *in.Since, Valid: true}
	}
	if in.Until != nil {
		until = pgtype.Timestamptz{Time: *in.Until, Valid: true}
	}

	params := crdbstore.ListAccountActivityParams{
		TenantID:  in.TenantID,
		AccountID: in.AccountID,
		Currency:  in.Currency,
		Column4:   since,
		Column5:   until,
		Limit:     limit,
	}
	rows, err := s.q.ListAccountActivity(ctx, params)
	if err != nil {
		return nil, err
	}
	out := make([]repo.ActivityRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.ActivityRow{
			JournalID: r.JournalID,
			EntryID:   fmt.Sprintf("%x", r.ID.Bytes),
			Currency:  r.Currency,
			Direction: ledger.Direction(r.Direction),
			Amount:    r.Amount,
			CreatedAt: timestamptzToTime(r.CreatedAt),
		})
	}
	return out, nil
}

// PendingOutbox returns up to limit outbox events in PENDING state.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]repo.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.ListPendingOutbox(ctx, int32(limit))
	if err != nil {
		return nil, err
	}
	out := make([]repo.OutboxEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.OutboxEvent{
			ID:             r.ID,
			TenantID:       r.TenantID,
			AggregateID:    r.AggregateID,
			EventType:      r.EventType,
			IdempotencyKey: r.IdempotencyKey,
			Payload:        r.Payload,
			CreatedAt:      timestamptzToTime(r.CreatedAt),
		})
	}
	return out, nil
}

// MarkOutboxPublished marks an outbox event as PUBLISHED.
func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	return s.q.MarkOutboxPublished(ctx, id)
}

// IncrementOutboxAttempts increments the attempt count for an outbox event.
func (s *Store) IncrementOutboxAttempts(ctx context.Context, id string) error {
	return s.q.IncrementOutboxAttempts(ctx, id)
}

// InsertSnapshot persists a BalanceSnapshot.
func (s *Store) InsertSnapshot(ctx context.Context, snap ledger.BalanceSnapshot) error {
	return s.q.InsertSnapshot(ctx, crdbstore.InsertSnapshotParams{
		ID:            snap.ID,
		TenantID:      snap.TenantID,
		AccountID:     snap.AccountID,
		Currency:      snap.Currency,
		PostedDebits:  snap.PostedDebits,
		PostedCredits: snap.PostedCredits,
		Version:       snap.Version,
		SnapshotAt:    pgtype.Timestamptz{Time: snap.SnapshotAt.UTC(), Valid: true},
	})
}

// GetSnapshotBefore returns the latest snapshot for an account at or before at.
// Returns nil, nil when no snapshot exists.
func (s *Store) GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error) {
	row, err := s.q.GetLatestSnapshotBefore(ctx, crdbstore.GetLatestSnapshotBeforeParams{
		TenantID:   tenantID,
		AccountID:  accountID,
		Currency:   currency,
		SnapshotAt: pgtype.Timestamptz{Time: at.UTC(), Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToSnapshot(row), nil
}

// SumEntriesBetween sums debit and credit entries for an account in (after, until].
func (s *Store) SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (decimal.Decimal, decimal.Decimal, error) {
	row, err := s.q.SumEntriesBetween(ctx, crdbstore.SumEntriesBetweenParams{
		TenantID:    tenantID,
		AccountID:   accountID,
		Currency:    currency,
		CreatedAt:   pgtype.Timestamptz{Time: after.UTC(), Valid: true},
		CreatedAt_2: pgtype.Timestamptz{Time: until.UTC(), Valid: true},
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	return row.Debits, row.Credits, nil
}

// ListTenantBalances returns all balance rows for a tenant.
func (s *Store) ListTenantBalances(ctx context.Context, tenantID string) ([]repo.TenantBalanceRow, error) {
	rows, err := s.q.ListAllBalancesForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]repo.TenantBalanceRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.TenantBalanceRow{
			AccountID:     r.AccountID,
			Currency:      r.Currency,
			PostedDebits:  r.PostedDebits,
			PostedCredits: r.PostedCredits,
			Version:       r.Version,
		})
	}
	return out, nil
}

// ListTenantsDueForSnapshot returns up to limit tenant IDs that have no
// balance_snapshots row newer than cutoff.
func (s *Store) ListTenantsDueForSnapshot(ctx context.Context, cutoff time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.q.ListTenantsDueForSnapshot(ctx, crdbstore.ListTenantsDueForSnapshotParams{
		SnapshotAt: pgtype.Timestamptz{Time: cutoff.UTC(), Valid: true},
		Limit:      int32(limit),
	})
}

// GetReservation fetches a single reservation by tenant and reservation ID.
func (s *Store) GetReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	row, err := s.q.GetReservation(ctx, crdbstore.GetReservationParams{TenantID: tenantID, ID: reservationID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

// ListExpiredReservations returns up to limit reservations past their expiry time.
func (s *Store) ListExpiredReservations(ctx context.Context, now time.Time, limit int) ([]repo.ExpiredReservation, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.ListExpiredReservations(ctx, crdbstore.ListExpiredReservationsParams{
		ExpiresAt: pgtype.Timestamptz{Time: now.UTC(), Valid: true},
		Limit:     int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]repo.ExpiredReservation, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.ExpiredReservation{ID: r.ID, TenantID: r.TenantID})
	}
	return out, nil
}

// ListReservations returns reservations filtered by tenant and optional
// owner/status/limit.
func (s *Store) ListReservations(ctx context.Context, in repo.ListReservationsInput) ([]ledger.Reservation, error) {
	limit := int32(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.q.ListReservations(ctx, crdbstore.ListReservationsParams{
		TenantID:  in.TenantID,
		OwnerType: in.OwnerType,
		OwnerID:   in.OwnerID,
		Status:    in.Status,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Reservation, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToReservation(r))
	}
	return out, nil
}

// UpsertFXRate inserts or updates an FX rate row by the unique tuple.
func (s *Store) UpsertFXRate(ctx context.Context, r ledger.FXRate) (*ledger.FXRate, error) {
	row, err := s.q.UpsertFXRate(ctx, crdbstore.UpsertFXRateParams{
		ID: r.ID, TenantID: r.TenantID,
		BaseCurrency: r.BaseCurrency, QuoteCurrency: r.QuoteCurrency,
		Rate: r.Rate, Source: r.Source,
		EffectiveAt: pgtype.Timestamptz{Time: r.EffectiveAt.UTC(), Valid: true},
	})
	if err != nil {
		return nil, err
	}
	return rowToFXRate(row), nil
}

// GetFXRateAt returns the most recent rate with effective_at <= at, or nil if none.
func (s *Store) GetFXRateAt(ctx context.Context, tenantID, base, quote string, at time.Time) (*ledger.FXRate, error) {
	row, err := s.q.GetFXRateAt(ctx, crdbstore.GetFXRateAtParams{
		TenantID: tenantID, BaseCurrency: base, QuoteCurrency: quote,
		EffectiveAt: pgtype.Timestamptz{Time: at.UTC(), Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFXRate(row), nil
}

// ListFXRates returns FX rate rows filtered by base/quote/time-range.
func (s *Store) ListFXRates(ctx context.Context, in repo.ListFXRatesInput) ([]ledger.FXRate, error) {
	limit := int32(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	since := pgtype.Timestamptz{}
	if in.Since != nil {
		since = pgtype.Timestamptz{Time: in.Since.UTC(), Valid: true}
	}
	until := pgtype.Timestamptz{}
	if in.Until != nil {
		until = pgtype.Timestamptz{Time: in.Until.UTC(), Valid: true}
	}
	rows, err := s.q.ListFXRates(ctx, crdbstore.ListFXRatesParams{
		TenantID: in.TenantID,
		Column2:  in.BaseCurrency,
		Column3:  in.QuoteCurrency,
		Column4:  since,
		Column5:  until,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.FXRate, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToFXRate(r))
	}
	return out, nil
}

// PruneSnapshotsOlderThan deletes up to limit snapshots with snapshot_at < cutoff,
// preserving the most-recent snapshot per (tenant, account, currency).
// Returns the number of rows deleted.
func (s *Store) PruneSnapshotsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.q.PruneSnapshotsOlderThan(ctx, crdbstore.PruneSnapshotsOlderThanParams{
		SnapshotAt: pgtype.Timestamptz{Time: cutoff.UTC(), Valid: true},
		Limit:      int32(limit),
	})
}

// InsertExternalRecord inserts a record; returns (true, nil) when the row was
// newly inserted, (false, nil) on UNIQUE conflict (already exists).
func (s *Store) InsertExternalRecord(ctx context.Context, r ledger.ExternalRecord) (bool, error) {
	payload, _ := json.Marshal(r.RawPayload)
	var acct *string
	if r.AccountID != "" {
		v := r.AccountID
		acct = &v
	}
	n, err := s.q.InsertExternalRecord(ctx, crdbstore.InsertExternalRecordParams{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount, Currency: r.Currency,
		OccurredAt: pgtype.Timestamptz{Time: r.OccurredAt.UTC(), Valid: true},
		AccountID:  acct,
		RawPayload: payload,
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ListExternalRecordsForRecon returns UNMATCHED external records for the given
// source within the time window.
func (s *Store) ListExternalRecordsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.ExternalRecord, error) {
	rows, err := s.q.ListExternalRecordsForRecon(ctx, crdbstore.ListExternalRecordsForReconParams{
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
// the time window.
func (s *Store) ListJournalsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.Journal, error) {
	rows, err := s.q.ListJournalsForRecon(ctx, crdbstore.ListJournalsForReconParams{
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

// GetReconBatch fetches a reconciliation batch by ID.
func (s *Store) GetReconBatch(ctx context.Context, tenantID, batchID string) (*ledger.ReconciliationBatch, error) {
	row, err := s.q.GetReconBatch(ctx, crdbstore.GetReconBatchParams{TenantID: tenantID, ID: batchID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReconBatchNotFound, batchID)
		}
		return nil, err
	}
	return rowToReconBatch(row), nil
}

// ListDiscrepancies returns discrepancies optionally filtered by batch_id and status.
func (s *Store) ListDiscrepancies(ctx context.Context, in repo.ListDiscrepanciesInput) ([]ledger.Discrepancy, error) {
	limit := int32(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.q.ListDiscrepancies(ctx, crdbstore.ListDiscrepanciesParams{
		TenantID: in.TenantID,
		Column2:  in.BatchID,
		Column3:  in.Status,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Discrepancy, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToDiscrepancy(r))
	}
	return out, nil
}

// GetDiscrepancy fetches a single discrepancy by ID.
func (s *Store) GetDiscrepancy(ctx context.Context, tenantID, discrepancyID string) (*ledger.Discrepancy, error) {
	row, err := s.q.GetDiscrepancy(ctx, crdbstore.GetDiscrepancyParams{TenantID: tenantID, ID: discrepancyID})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeDiscrepancyNotFound, discrepancyID)
		}
		return nil, err
	}
	return rowToDiscrepancy(row), nil
}
