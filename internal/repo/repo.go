package repo

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// Store opens transactions and executes read-only queries.
type Store interface {
	// BeginFlowTx opens a write transaction with the strongest isolation the
	// backend supports (CRDB: SERIALIZABLE; SQLite: BEGIN IMMEDIATE).
	BeginFlowTx(ctx context.Context) (Tx, error)

	// Read-only verbs (auto-committed).
	GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error)
	GetBalance(ctx context.Context, tenantID, accountID, currency string) (postedDebits, postedCredits decimal.Decimal, version int64, err error)
	GetFlow(ctx context.Context, tenantID, flowRunID string) (*ledger.FlowRun, error)
	ListAccountActivity(ctx context.Context, in ListActivityInput) ([]ActivityRow, error)

	// PendingOutbox returns up to limit events in PENDING state.
	PendingOutbox(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, id string) error
	IncrementOutboxAttempts(ctx context.Context, id string) error

	// Snapshots
	InsertSnapshot(ctx context.Context, s ledger.BalanceSnapshot) error
	GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error)
	SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (debits, credits decimal.Decimal, err error)
	ListTenantBalances(ctx context.Context, tenantID string) ([]TenantBalanceRow, error)

	Close() error
}

// Tx is the per-flow transactional surface.
type Tx interface {
	// Idempotency lookups
	GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error)

	// Account fetch + balance locking
	GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error)
	LockBalance(ctx context.Context, tenantID, accountID, currency string) (postedDebits, postedCredits decimal.Decimal, version int64, err error)
	EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error
	UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error

	// Writes
	InsertAccount(ctx context.Context, a ledger.Account) error
	InsertFlowRun(ctx context.Context, f ledger.FlowRun) error
	CompleteFlowRun(ctx context.Context, tenantID, flowRunID string) error
	InsertJournal(ctx context.Context, j ledger.Journal) error
	InsertEntry(ctx context.Context, tenantID, entryID, journalID, accountID, currency string, direction ledger.Direction, amount decimal.Decimal) error
	InsertFlowStep(ctx context.Context, s ledger.FlowStep) error
	InsertOutbox(ctx context.Context, e OutboxEvent) error

	// Replay
	GetFlowSteps(ctx context.Context, tenantID, flowRunID string) ([]ledger.FlowStep, error)

	Commit() error
	Rollback() error
}

type OutboxEvent struct {
	ID             string
	TenantID       string
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}

type ListActivityInput struct {
	TenantID  string
	AccountID string
	Currency  string
	Since     *time.Time
	Until     *time.Time
	Limit     int
}

type ActivityRow struct {
	JournalID     string
	EntryID       string
	Currency      string
	Direction     ledger.Direction
	Amount        decimal.Decimal
	CreatedAt     time.Time
	SourceService string
}

type TenantBalanceRow struct {
	AccountID     string
	Currency      string
	PostedDebits  decimal.Decimal
	PostedCredits decimal.Decimal
	Version       int64
}
