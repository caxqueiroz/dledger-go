package crdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/doubleledger/gen/crdb"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
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
