package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"

	sqlitestore "github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

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
		since = in.Since.UTC().Format(time.RFC3339Nano)
	}
	if in.Until != nil {
		until = in.Until.UTC().Format(time.RFC3339Nano)
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
