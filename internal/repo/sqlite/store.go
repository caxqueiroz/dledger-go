package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	// blank-import registers the modernc SQLite driver with database/sql.
	_ "modernc.org/sqlite"

	sqlitestore "github.com/caxqueiroz/dledger-go/gen/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// sqliteTimeFormat matches SQLite's strftime('%Y-%m-%dT%H:%M:%fZ','now') output
// (millisecond precision). All time comparisons against ledger_entries.created_at
// and balance_snapshots.snapshot_at must use this format for correct string ordering.
const sqliteTimeFormat = "2006-01-02T15:04:05.000Z"

// Store wraps a single *sql.DB and implements repo.Store for SQLite.
type Store struct {
	db *sql.DB
	q  *sqlitestore.Queries
}

// Open opens a SQLite database at dsn with WAL mode and a single writer connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // single-writer for BEGIN IMMEDIATE
	return &Store{db: db, q: sqlitestore.New(db)}, nil
}

// DB returns the underlying *sql.DB, used by migration and test code.
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// BeginFlowTx acquires a dedicated connection, issues BEGIN IMMEDIATE, and
// returns a Tx bound to that connection.
func (s *Store) BeginFlowTx(ctx context.Context) (repo.Tx, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("begin immediate: %w", err)
	}
	return &Tx{db: s.db, conn: conn, q: sqlitestore.New(conn)}, nil
}

// GetAccount fetches a single account by tenant and account ID.
func (s *Store) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := s.q.GetAccount(ctx, sqlitestore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, "account "+accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

// GetBalance returns postedDebits, postedCredits, and version for a balance row.
// Returns zero values (not an error) when no row exists yet.
func (s *Store) GetBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	row, err := s.q.GetBalance(ctx, sqlitestore.GetBalanceParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decimal.Zero, decimal.Zero, 0, nil
		}
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

// GetFlow fetches a FlowRun with its steps by ID. Returns nil if not found.
func (s *Store) GetFlow(ctx context.Context, tenantID, flowRunID string) (*ledger.FlowRun, error) {
	row, err := s.q.GetFlowByID(ctx, sqlitestore.GetFlowByIDParams{TenantID: tenantID, ID: flowRunID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	f := rowToFlowRun(row)
	steps, err := s.q.GetFlowSteps(ctx, sqlitestore.GetFlowStepsParams{TenantID: tenantID, FlowRunID: flowRunID})
	if err != nil {
		return nil, err
	}
	for _, st := range steps {
		f.Steps = append(f.Steps, *rowToFlowStep(st))
	}
	return f, nil
}

// ListAccountActivity returns ledger entries for an account filtered by time range.
func (s *Store) ListAccountActivity(ctx context.Context, in repo.ListActivityInput) ([]repo.ActivityRow, error) {
	since, until := "", ""
	if in.Since != nil {
		since = in.Since.UTC().Format(sqliteTimeFormat)
	}
	if in.Until != nil {
		until = in.Until.UTC().Format(sqliteTimeFormat)
	}
	limit := int64(in.Limit)
	if limit <= 0 {
		limit = 100
	}
	// ListAccountActivityParams uses dual-bind pattern:
	//   Column5 mirrors CreatedAt (the "OR ? = ''" bind), Column7 mirrors CreatedAt_2.
	rows, err := s.q.ListAccountActivity(ctx, sqlitestore.ListAccountActivityParams{
		TenantID:    in.TenantID,
		AccountID:   in.AccountID,
		Currency:    in.Currency,
		CreatedAt:   since,
		Column5:     since,
		CreatedAt_2: until,
		Column7:     until,
		Limit:       limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]repo.ActivityRow, 0, len(rows))
	for _, r := range rows {
		amt, _ := decimal.NewFromString(r.Amount)
		out = append(out, repo.ActivityRow{
			JournalID: r.JournalID,
			EntryID:   r.ID,
			Currency:  r.Currency,
			Direction: ledger.Direction(r.Direction),
			Amount:    amt,
			CreatedAt: parseTime(r.CreatedAt),
		})
	}
	return out, nil
}

// PendingOutbox returns up to limit outbox events in PENDING state.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]repo.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.ListPendingOutbox(ctx, int64(limit))
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
			Payload:        []byte(r.Payload),
			CreatedAt:      parseTime(r.CreatedAt),
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
	return s.q.InsertSnapshot(ctx, sqlitestore.InsertSnapshotParams{
		ID:            snap.ID,
		TenantID:      snap.TenantID,
		AccountID:     snap.AccountID,
		Currency:      snap.Currency,
		PostedDebits:  snap.PostedDebits.String(),
		PostedCredits: snap.PostedCredits.String(),
		Version:       snap.Version,
		SnapshotAt:    snap.SnapshotAt.UTC().Format(sqliteTimeFormat),
	})
}

// GetSnapshotBefore returns the latest snapshot for an account at or before at.
// Returns nil, nil when no snapshot exists.
func (s *Store) GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error) {
	row, err := s.q.GetLatestSnapshotBefore(ctx, sqlitestore.GetLatestSnapshotBeforeParams{
		TenantID:   tenantID,
		AccountID:  accountID,
		Currency:   currency,
		SnapshotAt: at.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToSnapshot(row), nil
}

// SumEntriesBetween sums debit and credit entries for an account in (after, until].
func (s *Store) SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (decimal.Decimal, decimal.Decimal, error) {
	row, err := s.q.SumEntriesBetween(ctx, sqlitestore.SumEntriesBetweenParams{
		TenantID:    tenantID,
		AccountID:   accountID,
		Currency:    currency,
		CreatedAt:   after.UTC().Format(sqliteTimeFormat),
		CreatedAt_2: until.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	d := decimal.NewFromFloat(row.Debits)
	c := decimal.NewFromFloat(row.Credits)
	return d, c, nil
}

// ListTenantBalances returns all balance rows for a tenant.
func (s *Store) ListTenantBalances(ctx context.Context, tenantID string) ([]repo.TenantBalanceRow, error) {
	rows, err := s.q.ListAllBalancesForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]repo.TenantBalanceRow, 0, len(rows))
	for _, r := range rows {
		d, _ := decimal.NewFromString(r.PostedDebits)
		c, _ := decimal.NewFromString(r.PostedCredits)
		out = append(out, repo.TenantBalanceRow{
			AccountID:     r.AccountID,
			Currency:      r.Currency,
			PostedDebits:  d,
			PostedCredits: c,
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
	return s.q.ListTenantsDueForSnapshot(ctx, sqlitestore.ListTenantsDueForSnapshotParams{
		SnapshotAt: cutoff.UTC().Format(sqliteTimeFormat),
		Limit:      int64(limit),
	})
}

// GetReservation fetches a single reservation by tenant and reservation ID.
func (s *Store) GetReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	row, err := s.q.GetReservation(ctx, sqlitestore.GetReservationParams{TenantID: tenantID, ID: reservationID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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
	ts := now.UTC().Format(sqliteTimeFormat)
	rows, err := s.q.ListExpiredReservations(ctx, sqlitestore.ListExpiredReservationsParams{
		ExpiresAt: &ts,
		Limit:     int64(limit),
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

// UpsertFXRate inserts or updates an FX rate row by the unique tuple.
func (s *Store) UpsertFXRate(ctx context.Context, r ledger.FXRate) (*ledger.FXRate, error) {
	row, err := s.q.UpsertFXRate(ctx, sqlitestore.UpsertFXRateParams{
		ID: r.ID, TenantID: r.TenantID,
		BaseCurrency: r.BaseCurrency, QuoteCurrency: r.QuoteCurrency,
		Rate: r.Rate.String(), Source: r.Source,
		EffectiveAt: r.EffectiveAt.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		return nil, err
	}
	return rowToFXRate(row), nil
}

// GetFXRateAt returns the most recent rate with effective_at <= at, or nil if none.
func (s *Store) GetFXRateAt(ctx context.Context, tenantID, base, quote string, at time.Time) (*ledger.FXRate, error) {
	row, err := s.q.GetFXRateAt(ctx, sqlitestore.GetFXRateAtParams{
		TenantID: tenantID, BaseCurrency: base, QuoteCurrency: quote,
		EffectiveAt: at.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFXRate(row), nil
}

// ListFXRates returns FX rate rows filtered by base/quote/time-range.
func (s *Store) ListFXRates(ctx context.Context, in repo.ListFXRatesInput) ([]ledger.FXRate, error) {
	limit := int64(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	since, until := "", ""
	if in.Since != nil {
		since = in.Since.UTC().Format(sqliteTimeFormat)
	}
	if in.Until != nil {
		until = in.Until.UTC().Format(sqliteTimeFormat)
	}
	// The dual-bind ? = '' pattern produces Column3/5/7/9 as opaque sides.
	// Pass the same value for both binds; the empty-string sentinel disables
	// the filter at the SQL level.
	rows, err := s.q.ListFXRates(ctx, sqlitestore.ListFXRatesParams{
		TenantID:      in.TenantID,
		BaseCurrency:  in.BaseCurrency,
		Column3:       in.BaseCurrency,
		QuoteCurrency: in.QuoteCurrency,
		Column5:       in.QuoteCurrency,
		EffectiveAt:   since,
		Column7:       since,
		EffectiveAt_2: until,
		Column9:       until,
		Limit:         limit,
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
	return s.q.PruneSnapshotsOlderThan(ctx, sqlitestore.PruneSnapshotsOlderThanParams{
		SnapshotAt: cutoff.UTC().Format(sqliteTimeFormat),
		Limit:      int64(limit),
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
	n, err := s.q.InsertExternalRecord(ctx, sqlitestore.InsertExternalRecordParams{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount.String(), Currency: r.Currency,
		OccurredAt: r.OccurredAt.UTC().Format(sqliteTimeFormat),
		AccountID:  acct,
		RawPayload: string(payload),
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ListExternalRecordsForRecon returns UNMATCHED external records for the given
// source within the time window.
func (s *Store) ListExternalRecordsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.ExternalRecord, error) {
	rows, err := s.q.ListExternalRecordsForRecon(ctx, sqlitestore.ListExternalRecordsForReconParams{
		TenantID:     tenantID,
		Source:       source,
		OccurredAt:   windowStart.UTC().Format(sqliteTimeFormat),
		OccurredAt_2: windowEnd.UTC().Format(sqliteTimeFormat),
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
	rows, err := s.q.ListJournalsForRecon(ctx, sqlitestore.ListJournalsForReconParams{
		TenantID:      tenantID,
		SourceService: source,
		CreatedAt:     windowStart.UTC().Format(sqliteTimeFormat),
		CreatedAt_2:   windowEnd.UTC().Format(sqliteTimeFormat),
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
	row, err := s.q.GetReconBatch(ctx, sqlitestore.GetReconBatchParams{TenantID: tenantID, ID: batchID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReconBatchNotFound, batchID)
		}
		return nil, err
	}
	return rowToReconBatch(row), nil
}

// ListDiscrepancies returns discrepancies optionally filtered by batch_id and status.
func (s *Store) ListDiscrepancies(ctx context.Context, in repo.ListDiscrepanciesInput) ([]ledger.Discrepancy, error) {
	limit := int64(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.q.ListDiscrepancies(ctx, sqlitestore.ListDiscrepanciesParams{
		TenantID: in.TenantID,
		BatchID:  in.BatchID,
		Column3:  in.BatchID,
		Status:   in.Status,
		Column5:  in.Status,
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
	row, err := s.q.GetDiscrepancy(ctx, sqlitestore.GetDiscrepancyParams{TenantID: tenantID, ID: discrepancyID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeDiscrepancyNotFound, discrepancyID)
		}
		return nil, err
	}
	return rowToDiscrepancy(row), nil
}
