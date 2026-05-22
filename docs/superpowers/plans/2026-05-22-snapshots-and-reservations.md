# Snapshots and Reservations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add point-in-time balance snapshots and a full Reservation lifecycle (HELD → PARTIAL → COMMITTED|RELEASED|EXPIRED, with partial commits/releases and auto-expiry) to the existing ledger service, with both features driven by a shared in-process scheduler.

**Architecture:** Two stacked phases. Phase A adds the `balance_snapshots` table, `TakeBalanceSnapshot` RPC, `as_of` parameter on `GetBalance`, and a snapshot scheduler tick. Phase B adds the `reservations` table, four reservation RPCs, an extracted `executeFlowInTx` helper so reservation handlers can run an internal flow plus reservation-row writes in one tx, an internal expiry helper, and a reservation-expiry scheduler tick. All money movement still flows through the existing `ExecuteFlow` orchestrator.

**Tech Stack:** Go 1.26, Connect-RPC, sqlc (per-dialect), goose, shopspring/decimal, SQLite (`modernc.org/sqlite`), CockroachDB (via pgx), OpenTelemetry, slog.

**Design doc:** `docs/superpowers/specs/2026-05-22-snapshots-and-reservations-design.md`

---

## File map (what gets created / modified)

```
sql/migrations/sqlite/0002_balance_snapshots.sql        NEW
sql/migrations/crdb/0002_balance_snapshots.sql          NEW
sql/migrations/sqlite/0003_reservations.sql             NEW
sql/migrations/crdb/0003_reservations.sql               NEW

sql/queries/sqlite/snapshots.sql                        NEW
sql/queries/crdb/snapshots.sql                          NEW
sql/queries/sqlite/reservations.sql                     NEW
sql/queries/crdb/reservations.sql                       NEW

gen/sqlite/...                                           REGEN
gen/crdb/...                                             REGEN

proto/ledger/v1/ledger.proto                            MODIFY (new RPCs + as_of)
gen/proto/...                                            REGEN

internal/ledger/snapshot.go                             NEW
internal/ledger/reservation.go                          NEW
internal/ledger/errors.go                               MODIFY (4 new codes)

internal/repo/repo.go                                   MODIFY (Store/Tx extensions)
internal/repo/sqlite/store.go                           MODIFY
internal/repo/sqlite/tx.go                              MODIFY
internal/repo/sqlite/conv.go                            MODIFY
internal/repo/crdb/store.go                             MODIFY
internal/repo/crdb/tx.go                                MODIFY
internal/repo/crdb/conv.go                              MODIFY

internal/service/errors.go                              MODIFY (map new codes)
internal/service/execute_flow.go                        MODIFY (extract executeFlowInTx)
internal/service/get_balance.go                         MODIFY (as_of branch)
internal/service/take_snapshot.go                       NEW
internal/service/create_reservation.go                  NEW
internal/service/commit_reservation.go                  NEW
internal/service/release_reservation.go                 NEW
internal/service/get_reservation.go                     NEW
internal/service/expire_reservation.go                  NEW (internal helper)
internal/service/reservation_helpers.go                 NEW (shared transition logic)

internal/scheduler/scheduler.go                         NEW

cmd/server/main.go                                      MODIFY (start scheduler)

internal/service/snapshots_test.go                      NEW
internal/service/reservations_test.go                   NEW
internal/scheduler/scheduler_test.go                    NEW
```

---

# Phase A — Balance Snapshots

## Task 1: Domain type for BalanceSnapshot

**Files:**
- Create: `internal/ledger/snapshot.go`

- [ ] **Step 1: Write the type**

```go
// internal/ledger/snapshot.go
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// BalanceSnapshot captures account_balances at a logical point in time.
type BalanceSnapshot struct {
	ID            string
	TenantID      string
	AccountID     string
	Currency      string
	PostedDebits  decimal.Decimal
	PostedCredits decimal.Decimal
	Version       int64
	SnapshotAt    time.Time
	CreatedAt     time.Time
}
```

- [ ] **Step 2: Build**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./internal/ledger/
```

- [ ] **Step 3: Commit**

```bash
git add internal/ledger/snapshot.go
git commit -m "feat(ledger): add BalanceSnapshot domain type"
```

---

## Task 2: SQLite + CRDB snapshot migrations

**Files:**
- Create: `sql/migrations/sqlite/0002_balance_snapshots.sql`
- Create: `sql/migrations/crdb/0002_balance_snapshots.sql`

- [ ] **Step 1: SQLite migration**

```sql
-- sql/migrations/sqlite/0002_balance_snapshots.sql
-- +goose Up
CREATE TABLE balance_snapshots (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    posted_debits   TEXT NOT NULL,
    posted_credits  TEXT NOT NULL,
    version         INTEGER NOT NULL,
    snapshot_at     TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX balance_snapshots_lookup_idx
    ON balance_snapshots (tenant_id, account_id, currency, snapshot_at DESC);

-- +goose Down
DROP TABLE balance_snapshots;
```

- [ ] **Step 2: CRDB migration**

```sql
-- sql/migrations/crdb/0002_balance_snapshots.sql
-- +goose Up
CREATE TABLE balance_snapshots (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    posted_debits   DECIMAL(38, 18) NOT NULL,
    posted_credits  DECIMAL(38, 18) NOT NULL,
    version         INT8 NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX balance_snapshots_lookup_idx
    ON balance_snapshots (tenant_id, account_id, currency, snapshot_at DESC);

-- +goose Down
DROP TABLE balance_snapshots;
```

- [ ] **Step 3: Smoke against SQLite**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
mkdir -p bin
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/snap-mig.db
./bin/migrate --backend=sqlite --dsn=/tmp/snap-mig.db up
sqlite3 /tmp/snap-mig.db ".tables" | tr '\n ' ' '
echo
./bin/migrate --backend=sqlite --dsn=/tmp/snap-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/snap-mig.db down  # back to 0
rm -f /tmp/snap-mig.db bin/migrate
```

Expected `.tables` output includes `balance_snapshots`.

- [ ] **Step 4: Commit**

```bash
git add sql/migrations/
git commit -m "feat(db): add balance_snapshots tables"
```

---

## Task 3: sqlc snapshot queries

**Files:**
- Create: `sql/queries/sqlite/snapshots.sql`
- Create: `sql/queries/crdb/snapshots.sql`
- Regenerate: `gen/sqlite`, `gen/crdb`

- [ ] **Step 1: SQLite queries**

```sql
-- sql/queries/sqlite/snapshots.sql

-- name: InsertSnapshot :exec
INSERT INTO balance_snapshots (id, tenant_id, account_id, currency, posted_debits, posted_credits, version, snapshot_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetLatestSnapshotBefore :one
SELECT * FROM balance_snapshots
WHERE tenant_id = ? AND account_id = ? AND currency = ? AND snapshot_at <= ?
ORDER BY snapshot_at DESC, id DESC
LIMIT 1;

-- name: SumEntriesBetween :one
SELECT
    COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN CAST(amount AS REAL) ELSE 0 END), 0) AS debits,
    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN CAST(amount AS REAL) ELSE 0 END), 0) AS credits
FROM ledger_entries
WHERE tenant_id = ? AND account_id = ? AND currency = ?
  AND created_at > ? AND created_at <= ?;

-- name: ListAllBalancesForTenant :many
SELECT account_id, currency, posted_debits, posted_credits, version
FROM account_balances
WHERE tenant_id = ?
ORDER BY account_id, currency;
```

Note: `SumEntriesBetween` returns `REAL` (float). The repo layer will treat amounts as text and use shopspring/decimal arithmetic — this query gets used only when there's no snapshot yet AND the caller asks for `as_of`. For SQLite local dev the precision tradeoff is acceptable. CRDB uses `DECIMAL` natively.

- [ ] **Step 2: CRDB queries**

```sql
-- sql/queries/crdb/snapshots.sql

-- name: InsertSnapshot :exec
INSERT INTO balance_snapshots (id, tenant_id, account_id, currency, posted_debits, posted_credits, version, snapshot_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetLatestSnapshotBefore :one
SELECT * FROM balance_snapshots
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3 AND snapshot_at <= $4
ORDER BY snapshot_at DESC, id DESC
LIMIT 1;

-- name: SumEntriesBetween :one
SELECT
    COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END), 0) AS debits,
    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0) AS credits
FROM ledger_entries
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
  AND created_at > $4 AND created_at <= $5;

-- name: ListAllBalancesForTenant :many
SELECT account_id, currency, posted_debits, posted_credits, version
FROM account_balances
WHERE tenant_id = $1
ORDER BY account_id, currency;
```

- [ ] **Step 3: Regenerate sqlc**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" sqlc generate
go build ./gen/...
```

- [ ] **Step 4: Commit**

```bash
git add sql/queries/ gen/sqlite/ gen/crdb/
git commit -m "feat(db): add sqlc queries for snapshots and per-tenant balances"
```

---

## Task 4: Repository extension for snapshots

**Files:**
- Modify: `internal/repo/repo.go`
- Modify: `internal/repo/sqlite/store.go`
- Modify: `internal/repo/sqlite/conv.go`
- Modify: `internal/repo/crdb/store.go`
- Modify: `internal/repo/crdb/conv.go`

- [ ] **Step 1: Extend `Store` interface in `internal/repo/repo.go`**

Add to the `Store` interface, right after `IncrementOutboxAttempts`:

```go
	// Snapshots
	InsertSnapshot(ctx context.Context, s ledger.BalanceSnapshot) error
	GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error)
	SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (debits, credits decimal.Decimal, err error)
	ListTenantBalances(ctx context.Context, tenantID string) ([]TenantBalanceRow, error)
```

And add the helper type right above `OutboxEvent`:

```go
type TenantBalanceRow struct {
	AccountID     string
	Currency      string
	PostedDebits  decimal.Decimal
	PostedCredits decimal.Decimal
	Version       int64
}
```

- [ ] **Step 2: Implement on SQLite Store (`internal/repo/sqlite/store.go`)**

Append after `IncrementOutboxAttempts`:

```go
func (s *Store) InsertSnapshot(ctx context.Context, snap ledger.BalanceSnapshot) error {
	return s.q.InsertSnapshot(ctx, sqlitestore.InsertSnapshotParams{
		ID: snap.ID, TenantID: snap.TenantID, AccountID: snap.AccountID, Currency: snap.Currency,
		PostedDebits: snap.PostedDebits.String(), PostedCredits: snap.PostedCredits.String(),
		Version: snap.Version, SnapshotAt: snap.SnapshotAt.UTC().Format(time.RFC3339Nano),
	})
}

func (s *Store) GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error) {
	row, err := s.q.GetLatestSnapshotBefore(ctx, sqlitestore.GetLatestSnapshotBeforeParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
		SnapshotAt: at.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToSnapshot(row), nil
}

func (s *Store) SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (decimal.Decimal, decimal.Decimal, error) {
	row, err := s.q.SumEntriesBetween(ctx, sqlitestore.SumEntriesBetweenParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
		CreatedAt:   after.UTC().Format(time.RFC3339Nano),
		CreatedAt_2: until.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	d := decimal.NewFromFloat(row.Debits)
	c := decimal.NewFromFloat(row.Credits)
	return d, c, nil
}

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
			AccountID: r.AccountID, Currency: r.Currency,
			PostedDebits: d, PostedCredits: c, Version: r.Version,
		})
	}
	return out, nil
}
```

If the sqlc-generated field for the second `CreatedAt` is named differently (e.g. `Column5`), inspect `gen/sqlite/snapshots.sql.go` and adjust.

- [ ] **Step 3: Add `rowToSnapshot` in `internal/repo/sqlite/conv.go`**

```go
func rowToSnapshot(r sqlitestore.BalanceSnapshot) *ledger.BalanceSnapshot {
	d, _ := decimal.NewFromString(r.PostedDebits)
	c, _ := decimal.NewFromString(r.PostedCredits)
	return &ledger.BalanceSnapshot{
		ID: r.ID, TenantID: r.TenantID, AccountID: r.AccountID, Currency: r.Currency,
		PostedDebits: d, PostedCredits: c, Version: r.Version,
		SnapshotAt: parseTime(r.SnapshotAt), CreatedAt: parseTime(r.CreatedAt),
	}
}
```

Add the import `"github.com/shopspring/decimal"` if not already present.

- [ ] **Step 4: Implement on CRDB Store (`internal/repo/crdb/store.go`)**

Append:

```go
func (s *Store) InsertSnapshot(ctx context.Context, snap ledger.BalanceSnapshot) error {
	return s.q.InsertSnapshot(ctx, crdbstore.InsertSnapshotParams{
		ID: snap.ID, TenantID: snap.TenantID, AccountID: snap.AccountID, Currency: snap.Currency,
		PostedDebits: snap.PostedDebits, PostedCredits: snap.PostedCredits,
		Version: snap.Version,
		SnapshotAt: pgtype.Timestamptz{Time: snap.SnapshotAt.UTC(), Valid: true},
	})
}

func (s *Store) GetSnapshotBefore(ctx context.Context, tenantID, accountID, currency string, at time.Time) (*ledger.BalanceSnapshot, error) {
	row, err := s.q.GetLatestSnapshotBefore(ctx, crdbstore.GetLatestSnapshotBeforeParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
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

func (s *Store) SumEntriesBetween(ctx context.Context, tenantID, accountID, currency string, after, until time.Time) (decimal.Decimal, decimal.Decimal, error) {
	row, err := s.q.SumEntriesBetween(ctx, crdbstore.SumEntriesBetweenParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
		CreatedAt:   pgtype.Timestamptz{Time: after.UTC(), Valid: true},
		CreatedAt_2: pgtype.Timestamptz{Time: until.UTC(), Valid: true},
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	// sqlc will produce row.Debits / row.Credits as decimal.Decimal because the
	// decimal override is in sqlc.yaml.
	return row.Debits, row.Credits, nil
}

func (s *Store) ListTenantBalances(ctx context.Context, tenantID string) ([]repo.TenantBalanceRow, error) {
	rows, err := s.q.ListAllBalancesForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := make([]repo.TenantBalanceRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.TenantBalanceRow{
			AccountID: r.AccountID, Currency: r.Currency,
			PostedDebits: r.PostedDebits, PostedCredits: r.PostedCredits, Version: r.Version,
		})
	}
	return out, nil
}
```

If the sum query's param names differ (likely `Column4` / `Column5`), inspect `gen/crdb/snapshots.sql.go` and adjust.

- [ ] **Step 5: Add `rowToSnapshot` in `internal/repo/crdb/conv.go`**

```go
func rowToSnapshot(r crdbstore.BalanceSnapshot) *ledger.BalanceSnapshot {
	s := &ledger.BalanceSnapshot{
		ID: r.ID, TenantID: r.TenantID, AccountID: r.AccountID, Currency: r.Currency,
		PostedDebits: r.PostedDebits, PostedCredits: r.PostedCredits, Version: r.Version,
	}
	if r.SnapshotAt.Valid {
		s.SnapshotAt = r.SnapshotAt.Time
	}
	if r.CreatedAt.Valid {
		s.CreatedAt = r.CreatedAt.Time
	}
	return s
}
```

- [ ] **Step 6: Build, run smoke**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go vet ./...
go test ./internal/repo/sqlite/ -v
```

- [ ] **Step 7: Commit**

```bash
git add internal/repo/
git commit -m "feat(repo): snapshot insert/lookup + tenant-balance listing"
```

---

## Task 5: Proto additions for snapshots and as_of

**Files:**
- Modify: `proto/ledger/v1/ledger.proto`
- Regenerate: `gen/proto/...`

- [ ] **Step 1: Add the RPC and messages to `proto/ledger/v1/ledger.proto`**

Inside the `service LedgerService { ... }` block, add after `ListAccountActivity`:

```proto
  rpc TakeBalanceSnapshot(TakeBalanceSnapshotRequest) returns (TakeBalanceSnapshotResponse);
```

After the existing message definitions, add:

```proto
message TakeBalanceSnapshotRequest {
  string tenant_id  = 1 [(buf.validate.field).string.min_len = 1];
  // If account_id+currency are set, snapshot one row. Otherwise snapshot
  // every account_balances row in the tenant.
  string account_id = 2;
  string currency   = 3;
}
message TakeBalanceSnapshotResponse {
  int32 snapshots_taken = 1;
}
```

Modify `GetBalanceRequest` to add the optional `as_of`:

```proto
message GetBalanceRequest {
  string tenant_id  = 1;
  string account_id = 2;
  string currency   = 3;
  google.protobuf.Timestamp as_of = 4;
}
```

- [ ] **Step 2: Regenerate**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" buf generate
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add proto/ gen/proto/
git commit -m "feat(proto): add TakeBalanceSnapshot RPC and GetBalance.as_of"
```

---

## Task 6: `TakeBalanceSnapshot` handler

**Files:**
- Create: `internal/service/take_snapshot.go`

- [ ] **Step 1: Implement the handler**

```go
// internal/service/take_snapshot.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) TakeBalanceSnapshot(ctx context.Context, req *connect.Request[ledgerv1.TakeBalanceSnapshotRequest]) (*connect.Response[ledgerv1.TakeBalanceSnapshotResponse], error) {
	r := req.Msg
	tenant := r.GetTenantId()
	if tenant == "" {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch, "tenant_id required"))
	}

	now := s.Now()

	// Single-row variant
	if r.GetAccountId() != "" && r.GetCurrency() != "" {
		d, c, ver, err := s.Store.GetBalance(ctx, tenant, r.GetAccountId(), r.GetCurrency())
		if err != nil {
			return nil, ToConnectError(err)
		}
		snap := ledger.BalanceSnapshot{
			ID: s.NewID(), TenantID: tenant,
			AccountID: r.GetAccountId(), Currency: r.GetCurrency(),
			PostedDebits: d, PostedCredits: c, Version: ver,
			SnapshotAt: now,
		}
		if err := s.Store.InsertSnapshot(ctx, snap); err != nil {
			return nil, ToConnectError(err)
		}
		return connect.NewResponse(&ledgerv1.TakeBalanceSnapshotResponse{SnapshotsTaken: 1}), nil
	}

	// Bulk variant
	rows, err := s.Store.ListTenantBalances(ctx, tenant)
	if err != nil {
		return nil, ToConnectError(err)
	}
	taken := int32(0)
	for _, b := range rows {
		snap := ledger.BalanceSnapshot{
			ID: s.NewID(), TenantID: tenant,
			AccountID: b.AccountID, Currency: b.Currency,
			PostedDebits: b.PostedDebits, PostedCredits: b.PostedCredits, Version: b.Version,
			SnapshotAt: now,
		}
		if err := s.Store.InsertSnapshot(ctx, snap); err != nil {
			return nil, ToConnectError(err)
		}
		taken++
	}
	return connect.NewResponse(&ledgerv1.TakeBalanceSnapshotResponse{SnapshotsTaken: taken}), nil
}
```

- [ ] **Step 2: Build**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add internal/service/take_snapshot.go
git commit -m "feat(service): implement TakeBalanceSnapshot (single + bulk)"
```

---

## Task 7: Extend `GetBalance` with `as_of`

**Files:**
- Modify: `internal/service/get_balance.go`

- [ ] **Step 1: Rewrite `GetBalance` to branch on `as_of`**

```go
// internal/service/get_balance.go
package service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) GetBalance(ctx context.Context, req *connect.Request[ledgerv1.GetBalanceRequest]) (*connect.Response[ledgerv1.GetBalanceResponse], error) {
	r := req.Msg
	a, err := s.Store.GetAccount(ctx, r.GetTenantId(), r.GetAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if a.Currency != r.GetCurrency() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
			"account "+a.ID+" currency="+a.Currency+" req="+r.GetCurrency()))
	}

	var d, c decimal.Decimal
	var ver int64

	if r.GetAsOf() != nil {
		at := r.GetAsOf().AsTime()

		snap, err := s.Store.GetSnapshotBefore(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency(), at)
		if err != nil {
			return nil, ToConnectError(err)
		}
		var snapAt = epochStart()
		if snap != nil {
			d = snap.PostedDebits
			c = snap.PostedCredits
			ver = snap.Version
			snapAt = snap.SnapshotAt
		}
		addD, addC, err := s.Store.SumEntriesBetween(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency(), snapAt, at)
		if err != nil {
			return nil, ToConnectError(err)
		}
		d = d.Add(addD)
		c = c.Add(addC)
	} else {
		d, c, ver, err = s.Store.GetBalance(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency())
		if err != nil {
			return nil, ToConnectError(err)
		}
	}

	norm := ledger.NormalizedBalance(a.NormalBalance, d, c)
	return connect.NewResponse(&ledgerv1.GetBalanceResponse{
		Balance: &ledgerv1.Balance{
			AccountId: a.ID, Currency: r.GetCurrency(),
			PostedDebits: d.String(), PostedCredits: c.String(),
			Normalized: norm.String(), Version: ver,
		},
	}), nil
}

// epochStart returns a time before any real entry could exist.
func epochStart() (t time.Time) { return }
```

Note: add the `"time"` import at top.

- [ ] **Step 2: Build and run service tests**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go test ./internal/service/ -v -run "GetBalance|GetFlow|CreateAndGetAccount"
```

- [ ] **Step 3: Commit**

```bash
git add internal/service/get_balance.go
git commit -m "feat(service): extend GetBalance with as_of reconstruction"
```

---

## Task 8: Snapshot end-to-end test

**Files:**
- Create: `internal/service/snapshots_test.go`

- [ ] **Step 1: Write the test**

```go
// internal/service/snapshots_test.go
package service_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func TestTakeBalanceSnapshot_SingleRow(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-seed", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-seed", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "300"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "300"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, err := srv.TakeBalanceSnapshot(context.Background(), connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if resp.Msg.GetSnapshotsTaken() != 1 {
		t.Fatalf("want 1 snapshot, got %d", resp.Msg.GetSnapshotsTaken())
	}
}

func TestGetBalance_AsOfHistoricalPoint(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	// Deposit 100 at t=0.
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-1", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-1", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("first deposit: %v", err)
	}

	// Capture a snapshot.
	if _, err := srv.TakeBalanceSnapshot(context.Background(), connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	})); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	asOf := time.Now()

	// Wait briefly so subsequent entry has a strictly later created_at.
	time.Sleep(20 * time.Millisecond)

	// Deposit another 250.
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-2", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-2", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "250"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "250"},
		}},
	})); err != nil {
		t.Fatalf("second deposit: %v", err)
	}

	// Current balance should be 350.
	now, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if got := now.Msg.GetBalance().GetNormalized(); got != "350" {
		t.Fatalf("current: want 350, got %s", got)
	}

	// As-of asOf, balance should be 100.
	historical, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
		AsOf: timestamppb.New(asOf),
	}))
	if err != nil {
		t.Fatalf("get as_of: %v", err)
	}
	if got := historical.Msg.GetBalance().GetNormalized(); got != "100" {
		t.Fatalf("as_of: want 100, got %s", got)
	}
}
```

- [ ] **Step 2: Run, verify pass**

```bash
go test ./internal/service/ -v -run "Snapshot|AsOf"
```

- [ ] **Step 3: Commit**

```bash
git add internal/service/snapshots_test.go
git commit -m "test(service): snapshot capture and as_of historical balance"
```

---

## Task 9: Scheduler package skeleton (snapshot tick)

**Files:**
- Create: `internal/scheduler/scheduler.go`

- [ ] **Step 1: Implement the package**

```go
// internal/scheduler/scheduler.go
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

type Snapshotter interface {
	TakeBalanceSnapshot(ctx context.Context, req *connect.Request[ledgerv1.TakeBalanceSnapshotRequest]) (*connect.Response[ledgerv1.TakeBalanceSnapshotResponse], error)
}

type Reservations interface {
	ExpireReservation(ctx context.Context, tenantID, reservationID string) error
}

type Config struct {
	SnapshotTick     time.Duration // default 5m
	SnapshotInterval time.Duration // default 24h
	ExpiryTick       time.Duration // default 30s
	BatchN           int           // default 100
}

type Scheduler struct {
	Store        repo.Store
	Snapshotter  Snapshotter
	Reservations Reservations // optional; nil disables reservation expiry tick
	Cfg          Config
	Log          *slog.Logger
}

func New(store repo.Store, srv *service.Server) *Scheduler {
	return &Scheduler{
		Store:        store,
		Snapshotter:  srv,
		Reservations: srv, // becomes useful once Phase B adds ExpireReservation
		Cfg: Config{
			SnapshotTick:     5 * time.Minute,
			SnapshotInterval: 24 * time.Hour,
			ExpiryTick:       30 * time.Second,
			BatchN:           100,
		},
		Log: slog.Default(),
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	snapT := time.NewTicker(s.Cfg.SnapshotTick)
	defer snapT.Stop()
	expT := time.NewTicker(s.Cfg.ExpiryTick)
	defer expT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-snapT.C:
			s.snapshotTick(ctx)
		case <-expT.C:
			s.expiryTick(ctx)
		}
	}
}

func (s *Scheduler) snapshotTick(ctx context.Context) {
	// Phase A: no tenant discovery yet; relies on operator triggering manually
	// or via integration test override. This stub keeps the goroutine alive and
	// will be expanded once we add a TenantList query.
}

func (s *Scheduler) expiryTick(ctx context.Context) {
	if s.Reservations == nil {
		return
	}
	// Phase A: no reservations yet, so this is a no-op until Phase B wires it.
}
```

Note: `*service.Server` must implement both `Snapshotter` and `Reservations`. Phase A only needs `Snapshotter`; the `ExpireReservation` method is added in Phase B Task 21. To keep this file compiling in Phase A, the `Reservations` interface is satisfied by `*Server` only after Phase B lands. Workaround: cast at runtime.

Replace `Reservations: srv` with:
```go
	var resv Reservations
	if r, ok := any(srv).(Reservations); ok {
		resv = r
	}
	return &Scheduler{
		// ...
		Reservations: resv,
		// ...
	}
```

- [ ] **Step 2: Build**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add internal/scheduler/
git commit -m "feat(scheduler): add package skeleton with snapshot + expiry ticks"
```

---

## Task 10: Wire scheduler into `cmd/server`

**Files:**
- Modify: `cmd/server/main.go`

- [ ] **Step 1: Add scheduler startup**

Add the import `"github.com/caxqueiroz/doubleledger/internal/scheduler"`.

After the existing `go disp.Run(ctx)` line, add:

```go
	sched := scheduler.New(store, srv)
	go sched.Run(ctx)
```

- [ ] **Step 2: Build + smoke**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/sched-smoke.db
./bin/migrate --backend=sqlite --dsn=/tmp/sched-smoke.db up
./bin/server --backend=sqlite --dsn=/tmp/sched-smoke.db --addr=127.0.0.1:18092 &
PID=$!
sleep 1
curl -fsS http://127.0.0.1:18092/healthz
kill $PID 2>/dev/null
wait $PID 2>/dev/null
rm -rf /tmp/sched-smoke.db bin
```

Expected: healthz 200, clean shutdown, no scheduler-related log errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/server/main.go
git commit -m "feat(server): launch scheduler alongside outbox dispatcher"
```

---

# Phase B — Reservations

## Task 11: Reservation domain types + error codes

**Files:**
- Create: `internal/ledger/reservation.go`
- Modify: `internal/ledger/errors.go`

- [ ] **Step 1: Add error codes to `internal/ledger/errors.go`**

Append to the `const ( ... )` block:

```go
	CodeReservationNotFound          DomainCode = "RESERVATION_NOT_FOUND"
	CodeReservationClosed            DomainCode = "RESERVATION_CLOSED"
	CodeReservationAmountExceeds     DomainCode = "RESERVATION_AMOUNT_EXCEEDS"
	CodeReservationCurrencyMismatch  DomainCode = "RESERVATION_CURRENCY_MISMATCH"
```

- [ ] **Step 2: Create `internal/ledger/reservation.go`**

```go
// internal/ledger/reservation.go
package ledger

import (
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

type ReservationStatus string

const (
	ReservationHeld      ReservationStatus = "HELD"
	ReservationPartial   ReservationStatus = "PARTIAL"
	ReservationCommitted ReservationStatus = "COMMITTED"
	ReservationReleased  ReservationStatus = "RELEASED"
	ReservationExpired   ReservationStatus = "EXPIRED"
)

func (s ReservationStatus) Closed() bool {
	switch s {
	case ReservationCommitted, ReservationReleased, ReservationExpired:
		return true
	}
	return false
}

type Reservation struct {
	ID                string
	TenantID          string
	IdempotencyKey    string
	SourceAccountID   string
	ReservedAccountID string
	Currency          string
	OriginalAmount    decimal.Decimal
	OutstandingAmount decimal.Decimal
	CommittedAmount   decimal.Decimal
	ReleasedAmount    decimal.Decimal
	Status            ReservationStatus
	ExpiresAt         *time.Time
	FlowRunID         string
	Metadata          map[string]any
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Validate enforces the conservation invariant.
func (r *Reservation) Validate() error {
	sum := r.OutstandingAmount.Add(r.CommittedAmount).Add(r.ReleasedAmount)
	if !sum.Equal(r.OriginalAmount) {
		return errors.New("reservation: outstanding+committed+released != original")
	}
	return nil
}
```

- [ ] **Step 3: Build + test**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go test ./internal/ledger/
```

- [ ] **Step 4: Commit**

```bash
git add internal/ledger/
git commit -m "feat(ledger): add Reservation type and reservation error codes"
```

---

## Task 12: Reservation migrations

**Files:**
- Create: `sql/migrations/sqlite/0003_reservations.sql`
- Create: `sql/migrations/crdb/0003_reservations.sql`

- [ ] **Step 1: SQLite**

```sql
-- sql/migrations/sqlite/0003_reservations.sql
-- +goose Up
CREATE TABLE reservations (
    id                    TEXT PRIMARY KEY,
    tenant_id             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL UNIQUE,
    source_account_id     TEXT NOT NULL REFERENCES accounts(id),
    reserved_account_id   TEXT NOT NULL REFERENCES accounts(id),
    currency              TEXT NOT NULL,
    original_amount       TEXT NOT NULL,
    outstanding_amount    TEXT NOT NULL,
    committed_amount      TEXT NOT NULL DEFAULT '0',
    released_amount       TEXT NOT NULL DEFAULT '0',
    status                TEXT NOT NULL CHECK (status IN ('HELD','PARTIAL','COMMITTED','RELEASED','EXPIRED')),
    expires_at            TEXT,
    flow_run_id           TEXT NOT NULL REFERENCES flow_runs(id),
    metadata              TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX reservations_expiry_idx ON reservations (tenant_id, status, expires_at);

-- +goose Down
DROP TABLE reservations;
```

- [ ] **Step 2: CRDB**

```sql
-- sql/migrations/crdb/0003_reservations.sql
-- +goose Up
CREATE TABLE reservations (
    id                    STRING PRIMARY KEY,
    tenant_id             STRING NOT NULL,
    idempotency_key       STRING NOT NULL UNIQUE,
    source_account_id     STRING NOT NULL REFERENCES accounts(id),
    reserved_account_id   STRING NOT NULL REFERENCES accounts(id),
    currency              STRING NOT NULL,
    original_amount       DECIMAL(38, 18) NOT NULL,
    outstanding_amount    DECIMAL(38, 18) NOT NULL,
    committed_amount      DECIMAL(38, 18) NOT NULL DEFAULT 0,
    released_amount       DECIMAL(38, 18) NOT NULL DEFAULT 0,
    status                STRING NOT NULL CHECK (status IN ('HELD','PARTIAL','COMMITTED','RELEASED','EXPIRED')),
    expires_at            TIMESTAMPTZ,
    flow_run_id           STRING NOT NULL REFERENCES flow_runs(id),
    metadata              JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX reservations_expiry_idx ON reservations (tenant_id, status, expires_at);

-- +goose Down
DROP TABLE reservations;
```

- [ ] **Step 3: Smoke**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
mkdir -p bin && go build -o bin/migrate ./cmd/migrate
rm -f /tmp/resv-mig.db
./bin/migrate --backend=sqlite --dsn=/tmp/resv-mig.db up
sqlite3 /tmp/resv-mig.db ".tables" | tr '\n ' ' '; echo
./bin/migrate --backend=sqlite --dsn=/tmp/resv-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/resv-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/resv-mig.db down
rm -rf /tmp/resv-mig.db bin
```

Expected `.tables` includes `reservations`.

- [ ] **Step 4: Commit**

```bash
git add sql/migrations/
git commit -m "feat(db): add reservations tables"
```

---

## Task 13: Reservation sqlc queries

**Files:**
- Create: `sql/queries/sqlite/reservations.sql`
- Create: `sql/queries/crdb/reservations.sql`
- Regenerate `gen/sqlite`, `gen/crdb`

- [ ] **Step 1: SQLite queries**

```sql
-- sql/queries/sqlite/reservations.sql

-- name: InsertReservation :exec
INSERT INTO reservations (
    id, tenant_id, idempotency_key, source_account_id, reserved_account_id,
    currency, original_amount, outstanding_amount, status, expires_at,
    flow_run_id, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetReservation :one
SELECT * FROM reservations WHERE tenant_id = ? AND id = ?;

-- name: GetReservationByIdempotency :one
SELECT * FROM reservations WHERE tenant_id = ? AND idempotency_key = ?;

-- name: UpdateReservationAmounts :exec
UPDATE reservations
SET outstanding_amount = ?, committed_amount = ?, released_amount = ?,
    status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE tenant_id = ? AND id = ?;

-- name: ListExpiredReservations :many
SELECT id, tenant_id FROM reservations
WHERE status IN ('HELD', 'PARTIAL')
  AND expires_at IS NOT NULL
  AND expires_at <= ?
ORDER BY expires_at ASC
LIMIT ?;
```

- [ ] **Step 2: CRDB queries**

```sql
-- sql/queries/crdb/reservations.sql

-- name: InsertReservation :exec
INSERT INTO reservations (
    id, tenant_id, idempotency_key, source_account_id, reserved_account_id,
    currency, original_amount, outstanding_amount, status, expires_at,
    flow_run_id, metadata
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- name: GetReservation :one
SELECT * FROM reservations WHERE tenant_id = $1 AND id = $2;

-- name: LockReservation :one
SELECT * FROM reservations WHERE tenant_id = $1 AND id = $2 FOR UPDATE;

-- name: GetReservationByIdempotency :one
SELECT * FROM reservations WHERE tenant_id = $1 AND idempotency_key = $2;

-- name: UpdateReservationAmounts :exec
UPDATE reservations
SET outstanding_amount = $1, committed_amount = $2, released_amount = $3,
    status = $4, updated_at = now()
WHERE tenant_id = $5 AND id = $6;

-- name: ListExpiredReservations :many
SELECT id, tenant_id FROM reservations
WHERE status IN ('HELD', 'PARTIAL')
  AND expires_at IS NOT NULL
  AND expires_at <= $1
ORDER BY expires_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;
```

- [ ] **Step 3: Regenerate and build**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" sqlc generate
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git add sql/queries/ gen/sqlite/ gen/crdb/
git commit -m "feat(db): add sqlc queries for reservations"
```

---

## Task 14: Repository extension for reservations

**Files:**
- Modify: `internal/repo/repo.go`
- Modify: `internal/repo/sqlite/{store.go,tx.go,conv.go}`
- Modify: `internal/repo/crdb/{store.go,tx.go,conv.go}`

- [ ] **Step 1: Extend interfaces in `internal/repo/repo.go`**

Add to `Store`:

```go
	// Reservations (read-only)
	GetReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error)
	ListExpiredReservations(ctx context.Context, now time.Time, limit int) ([]ExpiredReservation, error)
```

Add to `Tx`:

```go
	// Reservations
	InsertReservation(ctx context.Context, r ledger.Reservation) error
	LockReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error)
	GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error)
	UpdateReservationAmounts(ctx context.Context, tenantID, reservationID string, outstanding, committed, released decimal.Decimal, status ledger.ReservationStatus) error
```

Add the helper type:

```go
type ExpiredReservation struct {
	ID       string
	TenantID string
}
```

- [ ] **Step 2: SQLite implementation**

Add to `internal/repo/sqlite/store.go`:

```go
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

func (s *Store) ListExpiredReservations(ctx context.Context, now time.Time, limit int) ([]repo.ExpiredReservation, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.ListExpiredReservations(ctx, sqlitestore.ListExpiredReservationsParams{
		ExpiresAt: now.UTC().Format(time.RFC3339Nano),
		Limit:     int64(limit),
	})
	if err != nil {
		return nil, err
	}
	// Note: sqlc may name the nullable param differently; inspect generated code
	// and adjust the field name above.
	out := make([]repo.ExpiredReservation, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.ExpiredReservation{ID: r.ID, TenantID: r.TenantID})
	}
	return out, nil
}
```

Add to `internal/repo/sqlite/tx.go`:

```go
func (t *Tx) InsertReservation(ctx context.Context, r ledger.Reservation) error {
	metaBytes, _ := json.Marshal(r.Metadata)
	var expires *string
	if r.ExpiresAt != nil {
		s := r.ExpiresAt.UTC().Format(time.RFC3339Nano)
		expires = &s
	}
	return t.q.InsertReservation(ctx, sqlitestore.InsertReservationParams{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		SourceAccountID: r.SourceAccountID, ReservedAccountID: r.ReservedAccountID,
		Currency: r.Currency,
		OriginalAmount:    r.OriginalAmount.String(),
		OutstandingAmount: r.OutstandingAmount.String(),
		Status:            string(r.Status),
		ExpiresAt:         expires,
		FlowRunID:         r.FlowRunID,
		Metadata:          string(metaBytes),
	})
}

func (t *Tx) LockReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	// SQLite path: BEGIN IMMEDIATE serializes writes; a plain SELECT is enough.
	row, err := t.q.GetReservation(ctx, sqlitestore.GetReservationParams{TenantID: tenantID, ID: reservationID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

func (t *Tx) GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error) {
	row, err := t.q.GetReservationByIdempotency(ctx, sqlitestore.GetReservationByIdempotencyParams{TenantID: tenantID, IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

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
```

Add to `internal/repo/sqlite/conv.go`:

```go
func rowToReservation(r sqlitestore.Reservation) *ledger.Reservation {
	orig, _ := decimal.NewFromString(r.OriginalAmount)
	out, _ := decimal.NewFromString(r.OutstandingAmount)
	com, _ := decimal.NewFromString(r.CommittedAmount)
	rel, _ := decimal.NewFromString(r.ReleasedAmount)
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	res := &ledger.Reservation{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		SourceAccountID: r.SourceAccountID, ReservedAccountID: r.ReservedAccountID,
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
```

(Add `"encoding/json"` and `"github.com/shopspring/decimal"` imports if not already present in conv.go.)

- [ ] **Step 3: CRDB implementation**

Add equivalents to `internal/repo/crdb/store.go`, `tx.go`, `conv.go`. Key differences vs SQLite:

- `LockReservation` calls `t.q.LockReservation(ctx, ...)` (the FOR UPDATE variant).
- Decimals are `decimal.Decimal` directly (no String() round-trip).
- `metadata` is `[]byte` (JSONB) — `json.Marshal(r.Metadata)` directly.
- `expires_at` uses `pgtype.Timestamptz{Time: ..., Valid: r.ExpiresAt != nil}`.
- `ListExpiredReservations` uses `pgtype.Timestamptz` for the cutoff arg.
- `Reservation.UpdatedAt` and `CreatedAt` come from `pgtype.Timestamptz.Time`.

`internal/repo/crdb/store.go`:

```go
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
```

`internal/repo/crdb/tx.go`:

```go
func (t *Tx) InsertReservation(ctx context.Context, r ledger.Reservation) error {
	metaBytes, _ := json.Marshal(r.Metadata)
	expires := pgtype.Timestamptz{}
	if r.ExpiresAt != nil {
		expires = pgtype.Timestamptz{Time: r.ExpiresAt.UTC(), Valid: true}
	}
	return t.q.InsertReservation(ctx, crdbstore.InsertReservationParams{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		SourceAccountID: r.SourceAccountID, ReservedAccountID: r.ReservedAccountID,
		Currency: r.Currency,
		OriginalAmount:    r.OriginalAmount,
		OutstandingAmount: r.OutstandingAmount,
		Status:            string(r.Status),
		ExpiresAt:         expires,
		FlowRunID:         r.FlowRunID,
		Metadata:          metaBytes,
	})
}

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

func (t *Tx) GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error) {
	row, err := t.q.GetReservationByIdempotency(ctx, crdbstore.GetReservationByIdempotencyParams{TenantID: tenantID, IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReservation(row), nil
}

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
```

`internal/repo/crdb/conv.go`:

```go
func rowToReservation(r crdbstore.Reservation) *ledger.Reservation {
	meta := map[string]any{}
	if len(r.Metadata) > 0 {
		_ = json.Unmarshal(r.Metadata, &meta)
	}
	res := &ledger.Reservation{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		SourceAccountID: r.SourceAccountID, ReservedAccountID: r.ReservedAccountID,
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
```

- [ ] **Step 4: Build + test**

```bash
go build ./...
go vet ./...
go test ./internal/repo/sqlite/ -v
```

- [ ] **Step 5: Commit**

```bash
git add internal/repo/
git commit -m "feat(repo): reservation insert/lock/lookup/update for both backends"
```

---

## Task 15: Refactor `ExecuteFlow` to expose `executeFlowInTx`

**Files:**
- Modify: `internal/service/execute_flow.go`

- [ ] **Step 1: Extract the orchestrator body**

Open `internal/service/execute_flow.go`. The current `ExecuteFlow` function opens a tx, runs orchestration, commits, returns. Split it: the new `executeFlowInTx(ctx, tx, req)` performs the orchestration body (idempotency check, validations, locks, writes, outbox) using an existing tx. The public `ExecuteFlow` becomes a thin wrapper that opens the tx, calls `executeFlowInTx`, then commits.

Concrete shape:

```go
func (s *Server) ExecuteFlow(ctx context.Context, req *connect.Request[ledgerv1.ExecuteFlowRequest]) (*connect.Response[ledgerv1.ExecuteFlowResponse], error) {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	resp, err := s.executeFlowInTx(ctx, tx, req.Msg)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if err := tx.CommitTx(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(resp), nil
}

// executeFlowInTx runs the orchestrator against an existing tx. The caller
// owns Begin / Commit / Rollback. Returns the domain (non-Connect) error so
// callers can inspect it.
func (s *Server) executeFlowInTx(ctx context.Context, tx repo.Tx, r *ledgerv1.ExecuteFlowRequest) (*ledgerv1.ExecuteFlowResponse, error) {
    // ... move all existing orchestration body here, returning the domain
    //     error directly (without ToConnectError wrapping). Replace the
    //     previous tx.Commit() with a direct return of the response value.
}
```

Important: the existing `tx.Commit()` calls inside the body must be removed — the outer caller commits. The replay early-return path must also leave commit to the caller.

If the existing `Tx` interface uses `Commit()` (not `CommitTx()`), keep the name. Don't rename methods.

The orchestrator body retains the deferred-rollback pattern *only in the public ExecuteFlow*. The `executeFlowInTx` itself never rolls back; it just returns errors.

- [ ] **Step 2: Run existing tests**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go test ./internal/service/ -v
```

All existing ExecuteFlow / PostJournal / GetFlow tests must still pass. If any fail, the refactor introduced a regression — bisect by reverting and re-applying carefully.

- [ ] **Step 3: Commit**

```bash
git add internal/service/execute_flow.go
git commit -m "refactor(service): extract executeFlowInTx for reuse"
```

---

## Task 16: Reservation proto additions

**Files:**
- Modify: `proto/ledger/v1/ledger.proto`
- Regenerate `gen/proto/...`

- [ ] **Step 1: Add RPCs to the service block**

```proto
  rpc CreateReservation(CreateReservationRequest) returns (CreateReservationResponse);
  rpc CommitReservation(CommitReservationRequest) returns (CommitReservationResponse);
  rpc ReleaseReservation(ReleaseReservationRequest) returns (ReleaseReservationResponse);
  rpc GetReservation(GetReservationRequest) returns (GetReservationResponse);
```

- [ ] **Step 2: Add messages at the end of the file**

```proto
message Reservation {
  string id                       = 1;
  string tenant_id                = 2;
  string status                   = 3;
  string source_account_id        = 4;
  string reserved_account_id      = 5;
  string currency                 = 6;
  string original_amount          = 7;
  string outstanding_amount       = 8;
  string committed_amount         = 9;
  string released_amount          = 10;
  google.protobuf.Timestamp expires_at  = 11;
  google.protobuf.Timestamp created_at  = 12;
  google.protobuf.Timestamp updated_at  = 13;
  string flow_run_id              = 14;
}

message CreateReservationRequest {
  string tenant_id            = 1 [(buf.validate.field).string.min_len = 1];
  string idempotency_key      = 2 [(buf.validate.field).string.min_len = 1];
  string source_account_id    = 3 [(buf.validate.field).string.min_len = 1];
  string reserved_account_id  = 4 [(buf.validate.field).string.min_len = 1];
  string currency             = 5 [(buf.validate.field).string.min_len = 3];
  string amount               = 6 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
  google.protobuf.Timestamp expires_at = 7;
  string source_service       = 8;
  string actor_id             = 9;
  google.protobuf.Struct metadata = 10;
}
message CreateReservationResponse { Reservation reservation = 1; }

message CommitReservationRequest {
  string tenant_id              = 1;
  string reservation_id         = 2;
  string destination_account_id = 3;
  string amount                 = 4;
  string idempotency_key        = 5;
  string source_service         = 6;
  string actor_id               = 7;
}
message CommitReservationResponse { Reservation reservation = 1; }

message ReleaseReservationRequest {
  string tenant_id       = 1;
  string reservation_id  = 2;
  string amount          = 3;
  string idempotency_key = 4;
  string source_service  = 5;
  string actor_id        = 6;
}
message ReleaseReservationResponse { Reservation reservation = 1; }

message GetReservationRequest  { string tenant_id = 1; string reservation_id = 2; }
message GetReservationResponse { Reservation reservation = 1; }
```

- [ ] **Step 3: Regenerate and build**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" buf generate
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git add proto/ gen/proto/
git commit -m "feat(proto): add reservation RPCs and messages"
```

---

## Task 17: Reservation error mapping + shared transition helpers

**Files:**
- Modify: `internal/service/errors.go`
- Create: `internal/service/reservation_helpers.go`

- [ ] **Step 1: Map new domain codes**

In `internal/service/errors.go`, extend the switch:

```go
		case ledger.CodeInsufficientFunds, ledger.CodeInvalidAccountStatus, ledger.CodeReservationClosed:
			code = connect.CodeFailedPrecondition
		case ledger.CodeAccountNotFound, ledger.CodeReservationNotFound:
			code = connect.CodeNotFound
		case ledger.CodeAccountCurrencyMismatch, ledger.CodeUnbalancedJournal,
			ledger.CodeReservationAmountExceeds, ledger.CodeReservationCurrencyMismatch:
			code = connect.CodeInvalidArgument
```

- [ ] **Step 2: Add transition helper for converting Reservation to proto**

```go
// internal/service/reservation_helpers.go
package service

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func reservationToProto(r *ledger.Reservation) *ledgerv1.Reservation {
	p := &ledgerv1.Reservation{
		Id: r.ID, TenantId: r.TenantID, Status: string(r.Status),
		SourceAccountId: r.SourceAccountID, ReservedAccountId: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount.String(),
		OutstandingAmount: r.OutstandingAmount.String(),
		CommittedAmount:   r.CommittedAmount.String(),
		ReleasedAmount:    r.ReleasedAmount.String(),
		FlowRunId:         r.FlowRunID,
		CreatedAt:         timestamppb.New(r.CreatedAt),
		UpdatedAt:         timestamppb.New(r.UpdatedAt),
	}
	if r.ExpiresAt != nil {
		p.ExpiresAt = timestamppb.New(*r.ExpiresAt)
	}
	return p
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
go test ./internal/service/ -v -run TestToConnectError
```

- [ ] **Step 4: Commit**

```bash
git add internal/service/errors.go internal/service/reservation_helpers.go
git commit -m "feat(service): map reservation domain codes; reservation->proto helper"
```

---

## Task 18: `CreateReservation` handler

**Files:**
- Create: `internal/service/create_reservation.go`

- [ ] **Step 1: Implement**

```go
// internal/service/create_reservation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/structpb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

func (s *Server) CreateReservation(ctx context.Context, req *connect.Request[ledgerv1.CreateReservationRequest]) (*connect.Response[ledgerv1.CreateReservationResponse], error) {
	r := req.Msg
	amount, err := ledger.ParseAmount(r.GetAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds, err.Error()))
	}

	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Idempotent replay: same idempotency_key returns existing.
	existing, err := tx.GetReservationByIdempotency(ctx, r.GetTenantId(), r.GetIdempotencyKey())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if existing != nil {
		_ = tx.Commit()
		committed = true
		return connect.NewResponse(&ledgerv1.CreateReservationResponse{Reservation: reservationToProto(existing)}), nil
	}

	// Run the inner ExecuteFlow that moves funds source -> reserved.
	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "CREATE_RESERVATION",
		IdempotencyKey: r.GetIdempotencyKey() + ":create",
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{{
			StepId: "reserve",
			Journal: &ledgerv1.Journal{
				EventId: r.GetIdempotencyKey() + ":reserve",
				Entries: []*ledgerv1.Entry{
					{AccountId: r.GetReservedAccountId(), Currency: r.GetCurrency(), Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: r.GetAmount()},
					{AccountId: r.GetSourceAccountId(), Currency: r.GetCurrency(), Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: r.GetAmount()},
				},
			},
		}},
	}
	flowResp, err := s.executeFlowInTx(ctx, tx, flowReq)
	if err != nil {
		return nil, ToConnectError(err)
	}

	// Insert the reservation row.
	res := ledger.Reservation{
		ID:                s.NewID(),
		TenantID:          r.GetTenantId(),
		IdempotencyKey:    r.GetIdempotencyKey(),
		SourceAccountID:   r.GetSourceAccountId(),
		ReservedAccountID: r.GetReservedAccountId(),
		Currency:          r.GetCurrency(),
		OriginalAmount:    amount,
		OutstandingAmount: amount,
		CommittedAmount:   decimal.Zero,
		ReleasedAmount:    decimal.Zero,
		Status:            ledger.ReservationHeld,
		FlowRunID:         flowResp.GetFlowRunId(),
		Metadata:          mustStructToMap(r.GetMetadata()),
		CreatedAt:         s.Now(),
		UpdatedAt:         s.Now(),
	}
	if r.GetExpiresAt() != nil {
		t := r.GetExpiresAt().AsTime()
		res.ExpiresAt = &t
	}
	if err := tx.InsertReservation(ctx, res); err != nil {
		return nil, ToConnectError(err)
	}

	// Reservation-level outbox event.
	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID, "amount": amount.String(), "currency": res.Currency,
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType: "RESERVATION_CREATED", IdempotencyKey: res.ID + ":created", Payload: payload, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.CreateReservationResponse{Reservation: reservationToProto(&res)}), nil
}

func mustStructToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	return s.AsMap()
}
```

- [ ] **Step 2: Build**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add internal/service/create_reservation.go
git commit -m "feat(service): implement CreateReservation"
```

---

## Task 19: `CommitReservation` and `ReleaseReservation` handlers

**Files:**
- Create: `internal/service/commit_reservation.go`
- Create: `internal/service/release_reservation.go`

Both transitions follow the same shape. Pattern:
1. Open tx.
2. LockReservation.
3. Validate: not closed, amount ≤ outstanding, currency match (for commit's destination only).
4. Run `executeFlowInTx` with the appropriate journal entries.
5. Compute new amounts and status.
6. UpdateReservationAmounts.
7. Insert outbox event.
8. Commit.

- [ ] **Step 1: `CommitReservation`**

```go
// internal/service/commit_reservation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

func (s *Server) CommitReservation(ctx context.Context, req *connect.Request[ledgerv1.CommitReservationRequest]) (*connect.Response[ledgerv1.CommitReservationResponse], error) {
	r := req.Msg
	amount, err := ledger.ParseAmount(r.GetAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds, err.Error()))
	}

	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.LockReservation(ctx, r.GetTenantId(), r.GetReservationId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if res.Status.Closed() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationClosed, "status="+string(res.Status)))
	}
	if amount.GreaterThan(res.OutstandingAmount) {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds,
			"amount="+amount.String()+" outstanding="+res.OutstandingAmount.String()))
	}

	// Verify destination account exists with matching currency.
	dst, err := tx.GetAccount(ctx, r.GetTenantId(), r.GetDestinationAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if dst.Currency != res.Currency {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationCurrencyMismatch,
			"destination "+dst.ID+" currency="+dst.Currency+" reservation="+res.Currency))
	}

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "COMMIT_RESERVATION",
		IdempotencyKey: res.ID + ":commit:" + r.GetIdempotencyKey(),
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{{
			StepId: "commit",
			Journal: &ledgerv1.Journal{
				EventId: res.ID + ":commit:" + r.GetIdempotencyKey(),
				Entries: []*ledgerv1.Entry{
					{AccountId: dst.ID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: amount.String()},
					{AccountId: res.ReservedAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: amount.String()},
				},
			},
		}},
	}
	if _, err := s.executeFlowInTx(ctx, tx, flowReq); err != nil {
		return nil, ToConnectError(err)
	}

	res.OutstandingAmount = res.OutstandingAmount.Sub(amount)
	res.CommittedAmount = res.CommittedAmount.Add(amount)
	res.UpdatedAt = s.Now()
	switch {
	case res.OutstandingAmount.IsZero():
		res.Status = ledger.ReservationCommitted
	default:
		res.Status = ledger.ReservationPartial
	}

	if err := tx.UpdateReservationAmounts(ctx, r.GetTenantId(), res.ID,
		res.OutstandingAmount, res.CommittedAmount, res.ReleasedAmount, res.Status); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID, "amount": amount.String(),
		"outstanding": res.OutstandingAmount.String(), "status": string(res.Status),
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType: "RESERVATION_COMMITTED",
		IdempotencyKey: res.ID + ":committed:" + r.GetIdempotencyKey(),
		Payload: payload, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.CommitReservationResponse{Reservation: reservationToProto(res)}), nil
}
```

- [ ] **Step 2: `ReleaseReservation`**

```go
// internal/service/release_reservation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

func (s *Server) ReleaseReservation(ctx context.Context, req *connect.Request[ledgerv1.ReleaseReservationRequest]) (*connect.Response[ledgerv1.ReleaseReservationResponse], error) {
	r := req.Msg
	amount, err := ledger.ParseAmount(r.GetAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds, err.Error()))
	}

	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.LockReservation(ctx, r.GetTenantId(), r.GetReservationId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if res.Status.Closed() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationClosed, "status="+string(res.Status)))
	}
	if amount.GreaterThan(res.OutstandingAmount) {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds,
			"amount="+amount.String()+" outstanding="+res.OutstandingAmount.String()))
	}

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "RELEASE_RESERVATION",
		IdempotencyKey: res.ID + ":release:" + r.GetIdempotencyKey(),
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{{
			StepId: "release",
			Journal: &ledgerv1.Journal{
				EventId: res.ID + ":release:" + r.GetIdempotencyKey(),
				Entries: []*ledgerv1.Entry{
					{AccountId: res.SourceAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: amount.String()},
					{AccountId: res.ReservedAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: amount.String()},
				},
			},
		}},
	}
	if _, err := s.executeFlowInTx(ctx, tx, flowReq); err != nil {
		return nil, ToConnectError(err)
	}

	res.OutstandingAmount = res.OutstandingAmount.Sub(amount)
	res.ReleasedAmount = res.ReleasedAmount.Add(amount)
	res.UpdatedAt = s.Now()
	switch {
	case res.OutstandingAmount.IsZero():
		res.Status = ledger.ReservationReleased
	default:
		res.Status = ledger.ReservationPartial
	}

	if err := tx.UpdateReservationAmounts(ctx, r.GetTenantId(), res.ID,
		res.OutstandingAmount, res.CommittedAmount, res.ReleasedAmount, res.Status); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID, "amount": amount.String(),
		"outstanding": res.OutstandingAmount.String(), "status": string(res.Status),
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType: "RESERVATION_RELEASED",
		IdempotencyKey: res.ID + ":released:" + r.GetIdempotencyKey(),
		Payload: payload, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.ReleaseReservationResponse{Reservation: reservationToProto(res)}), nil
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git add internal/service/commit_reservation.go internal/service/release_reservation.go
git commit -m "feat(service): implement CommitReservation and ReleaseReservation"
```

---

## Task 20: `GetReservation` handler

**Files:**
- Create: `internal/service/get_reservation.go`

- [ ] **Step 1: Implement**

```go
// internal/service/get_reservation.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) GetReservation(ctx context.Context, req *connect.Request[ledgerv1.GetReservationRequest]) (*connect.Response[ledgerv1.GetReservationResponse], error) {
	res, err := s.Store.GetReservation(ctx, req.Msg.GetTenantId(), req.Msg.GetReservationId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetReservationResponse{Reservation: reservationToProto(res)}), nil
}
```

- [ ] **Step 2: Build + commit**

```bash
go build ./...
git add internal/service/get_reservation.go
git commit -m "feat(service): implement GetReservation"
```

---

## Task 21: `ExpireReservation` internal helper

**Files:**
- Create: `internal/service/expire_reservation.go`

- [ ] **Step 1: Implement**

```go
// internal/service/expire_reservation.go
package service

import (
	"context"
	"encoding/json"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

// ExpireReservation is called by the scheduler. It mirrors ReleaseReservation
// but marks the final status as EXPIRED. Not exposed as a public RPC.
func (s *Server) ExpireReservation(ctx context.Context, tenantID, reservationID string) error {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.LockReservation(ctx, tenantID, reservationID)
	if err != nil {
		return err
	}
	if res.Status.Closed() {
		// Another transition got here first; this is fine.
		_ = tx.Commit()
		committed = true
		return nil
	}
	amount := res.OutstandingAmount
	if amount.IsZero() {
		_ = tx.Commit()
		committed = true
		return nil
	}

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       tenantID,
		FlowType:       "EXPIRE_RESERVATION",
		IdempotencyKey: res.ID + ":expire",
		SourceService:  "scheduler",
		ActorId:        "system",
		Steps: []*ledgerv1.Step{{
			StepId: "expire",
			Journal: &ledgerv1.Journal{
				EventId: res.ID + ":expire",
				Entries: []*ledgerv1.Entry{
					{AccountId: res.SourceAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: amount.String()},
					{AccountId: res.ReservedAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: amount.String()},
				},
			},
		}},
	}
	if _, err := s.executeFlowInTx(ctx, tx, flowReq); err != nil {
		return err
	}

	res.ReleasedAmount = res.ReleasedAmount.Add(amount)
	res.OutstandingAmount = res.OutstandingAmount.Sub(amount)
	res.Status = ledger.ReservationExpired
	res.UpdatedAt = s.Now()

	if err := tx.UpdateReservationAmounts(ctx, tenantID, res.ID,
		res.OutstandingAmount, res.CommittedAmount, res.ReleasedAmount, res.Status); err != nil {
		return err
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID, "amount": amount.String(), "status": "EXPIRED",
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType: "RESERVATION_EXPIRED", IdempotencyKey: res.ID + ":expired",
		Payload: payload, CreatedAt: s.Now(),
	}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
```

- [ ] **Step 2: Build + commit**

```bash
go build ./...
git add internal/service/expire_reservation.go
git commit -m "feat(service): add internal ExpireReservation helper"
```

---

## Task 22: Wire reservation tick into scheduler

**Files:**
- Modify: `internal/scheduler/scheduler.go`

- [ ] **Step 1: Implement `expiryTick`**

Replace the existing `expiryTick` body:

```go
func (s *Scheduler) expiryTick(ctx context.Context) {
	if s.Reservations == nil {
		return
	}
	now := time.Now()
	rows, err := s.Store.ListExpiredReservations(ctx, now, s.Cfg.BatchN)
	if err != nil {
		s.Log.WarnContext(ctx, "scheduler.list_expired", "err", err)
		return
	}
	for _, r := range rows {
		if err := s.Reservations.ExpireReservation(ctx, r.TenantID, r.ID); err != nil {
			s.Log.WarnContext(ctx, "scheduler.expire", "tenant_id", r.TenantID, "id", r.ID, "err", err)
		}
	}
}
```

- [ ] **Step 2: Build**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add internal/scheduler/scheduler.go
git commit -m "feat(scheduler): expire reservations whose expires_at has passed"
```

---

## Task 23: Reservation end-to-end tests

**Files:**
- Create: `internal/service/reservations_test.go`

- [ ] **Step 1: Write the tests**

```go
// internal/service/reservations_test.go
package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func seedAndReserve(t *testing.T, srv server, amount string) (resvID, sourceID, reservedID, destID string) {
	t.Helper()
	src := seedSource(t, srv)
	sourceID = mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	reservedID = mustCreateAccount(t, srv, "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	destID = mustCreateAccount(t, srv, "2", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "res-seed-" + t.Name(), SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "res-seed-" + t.Name(), Entries: []*ledgerv1.Entry{
			{AccountId: sourceID, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r, err := srv.CreateReservation(context.Background(), connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-" + t.Name(),
		SourceAccountId: sourceID, ReservedAccountId: reservedID,
		Currency: "USD", Amount: amount, SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}
	return r.Msg.GetReservation().GetId(), sourceID, reservedID, destID
}

type server = interface {
	CreateAccount(context.Context, *connect.Request[ledgerv1.CreateAccountRequest]) (*connect.Response[ledgerv1.CreateAccountResponse], error)
	PostJournal(context.Context, *connect.Request[ledgerv1.PostJournalRequest]) (*connect.Response[ledgerv1.PostJournalResponse], error)
	CreateReservation(context.Context, *connect.Request[ledgerv1.CreateReservationRequest]) (*connect.Response[ledgerv1.CreateReservationResponse], error)
	CommitReservation(context.Context, *connect.Request[ledgerv1.CommitReservationRequest]) (*connect.Response[ledgerv1.CommitReservationResponse], error)
	ReleaseReservation(context.Context, *connect.Request[ledgerv1.ReleaseReservationRequest]) (*connect.Response[ledgerv1.ReleaseReservationResponse], error)
	GetReservation(context.Context, *connect.Request[ledgerv1.GetReservationRequest]) (*connect.Response[ledgerv1.GetReservationResponse], error)
	GetBalance(context.Context, *connect.Request[ledgerv1.GetBalanceRequest]) (*connect.Response[ledgerv1.GetBalanceResponse], error)
}

func TestCreateReservation_HoldsFunds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	_, sourceID, reservedID, _ := seedAndReserve(t, srv, "300")
	bal := func(acct string) string {
		r, _ := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
			TenantId: "t1", AccountId: acct, Currency: "USD",
		}))
		return r.Msg.GetBalance().GetNormalized()
	}
	if bal(sourceID) != "700" {
		t.Fatalf("source: want 700, got %s", bal(sourceID))
	}
	if bal(reservedID) != "300" {
		t.Fatalf("reserved: want 300, got %s", bal(reservedID))
	}
}

func TestReservation_PartialCommitThenFinalCommit(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "300")

	// Partial commit of 100.
	r1, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "100", IdempotencyKey: "c1",
	}))
	if err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	if r1.Msg.GetReservation().GetStatus() != "PARTIAL" {
		t.Fatalf("after partial commit: want PARTIAL, got %s", r1.Msg.GetReservation().GetStatus())
	}
	if r1.Msg.GetReservation().GetOutstandingAmount() != "200" {
		t.Fatalf("outstanding: want 200, got %s", r1.Msg.GetReservation().GetOutstandingAmount())
	}

	// Final commit of 200.
	r2, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "200", IdempotencyKey: "c2",
	}))
	if err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if r2.Msg.GetReservation().GetStatus() != "COMMITTED" {
		t.Fatalf("final commit: want COMMITTED, got %s", r2.Msg.GetReservation().GetStatus())
	}
	if r2.Msg.GetReservation().GetOutstandingAmount() != "0" {
		t.Fatalf("outstanding: want 0, got %s", r2.Msg.GetReservation().GetOutstandingAmount())
	}
}

func TestReservation_PartialReleaseThenCommit(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "300")

	// Release 100.
	if _, err := srv.ReleaseReservation(context.Background(), connect.NewRequest(&ledgerv1.ReleaseReservationRequest{
		TenantId: "t1", ReservationId: resvID, Amount: "100", IdempotencyKey: "r1",
	})); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Commit remaining 200 — should mark COMMITTED.
	r, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "200", IdempotencyKey: "c1",
	}))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if r.Msg.GetReservation().GetStatus() != "COMMITTED" {
		t.Fatalf("status: want COMMITTED, got %s", r.Msg.GetReservation().GetStatus())
	}
}

func TestReservation_CommitClosed(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "100")

	if _, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "100", IdempotencyKey: "c1",
	})); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Try to commit again — should be FailedPrecondition.
	_, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "1", IdempotencyKey: "c2",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition CodeReservationClosed, got %v", err)
	}
}

func TestReservation_AmountExceeds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "100")

	_, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "101", IdempotencyKey: "x1",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument CodeReservationAmountExceeds, got %v", err)
	}
}

func TestReservation_IdempotentCreate(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, _ := seedAndReserve(t, srv, "100")

	// Replay using same idempotency key
	r, err := srv.CreateReservation(context.Background(), connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-" + t.Name(),
		SourceAccountId: "ignored", ReservedAccountId: "ignored",
		Currency: "USD", Amount: "100",
	}))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if r.Msg.GetReservation().GetId() != resvID {
		t.Fatalf("replay returned different id: %s vs %s", r.Msg.GetReservation().GetId(), resvID)
	}
}

func TestReservation_Expiry(t *testing.T) {
	// This test relies on the scheduler. We call ExpireReservation directly via
	// the Server (it's a public method on *service.Server even though it has
	// no RPC). The scheduler integration test belongs in the scheduler package.
	t.Skip("expire path covered in scheduler tests")
}

func _useTimestamppb() { _ = timestamppb.Now() } // ensure imports remain
func _useTime() time.Duration                    { return time.Second }
```

Note the dummy functions at the bottom keep the `timestamppb` and `time` imports used. Remove them if you've added real uses above.

- [ ] **Step 2: Run, verify pass**

```bash
go test ./internal/service/ -v -run TestReservation -count=1
```

- [ ] **Step 3: Commit**

```bash
git add internal/service/reservations_test.go
git commit -m "test(service): reservation lifecycle scenarios"
```

---

## Task 24: Scheduler expiry test

**Files:**
- Create: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: Test**

```go
// internal/scheduler/scheduler_test.go
package scheduler_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/scheduler"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

func setup(t *testing.T) (*service.Server, *sqlite.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 1; i <= 3; i++ {
		mig, err := os.ReadFile(filepath.Join("../../sql/migrations/sqlite", []string{
			"0001_init.sql", "0002_balance_snapshots.sql", "0003_reservations.sql",
		}[i-1]))
		if err != nil {
			t.Fatalf("read migration %d: %v", i, err)
		}
		if _, err := st.DB().Exec(sqlite.StripGoose(string(mig))); err != nil {
			t.Fatalf("apply migration %d: %v", i, err)
		}
	}
	srv := service.New(st)
	return srv, st, func() { _ = st.Close() }
}

func TestScheduler_ExpiresOverdueReservation(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	// Create accounts and seed.
	mk := func(ownerID, kind string, nb ledgerv1.NormalBalance, allowNeg bool) string {
		r, err := srv.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
			TenantId: "t1", OwnerType: "user", OwnerId: ownerID, AccountType: kind,
			Currency: "USD", NormalBalance: nb, AllowNegative: allowNeg,
		}))
		if err != nil {
			t.Fatalf("create %s: %v", kind, err)
		}
		return r.Msg.GetAccount().GetId()
	}
	source := mk("1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	reserved := mk("1", "cash_reserved", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := mk("0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	if _, err := srv.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-exp", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-exp", Entries: []*ledgerv1.Entry{
			{AccountId: source, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "200"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "200"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Reservation that expires immediately.
	past := time.Now().Add(-1 * time.Second)
	pbTime := func(t time.Time) *connect.Request[ledgerv1.CreateReservationRequest] {
		return connect.NewRequest(&ledgerv1.CreateReservationRequest{
			TenantId: "t1", IdempotencyKey: "exp-res",
			SourceAccountId: source, ReservedAccountId: reserved,
			Currency: "USD", Amount: "150",
			ExpiresAt: timestampOf(t),
		})
	}
	r, err := srv.CreateReservation(ctx, pbTime(past))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	resID := r.Msg.GetReservation().GetId()

	// Run scheduler with a short tick so it fires fast.
	sched := scheduler.New(store, srv)
	sched.Cfg.ExpiryTick = 50 * time.Millisecond
	sched.Cfg.SnapshotTick = time.Hour // disable in this test
	ctx2, cancel := context.WithCancel(ctx)
	go sched.Run(ctx2)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := srv.GetReservation(ctx, connect.NewRequest(&ledgerv1.GetReservationRequest{
			TenantId: "t1", ReservationId: resID,
		}))
		if err == nil && got.Msg.GetReservation().GetStatus() == "EXPIRED" {
			cancel()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	t.Fatalf("reservation did not transition to EXPIRED in time")
}

func timestampOf(t time.Time) *struct{} { _ = t; return nil } // placeholder
```

The `timestampOf` placeholder is wrong — replace it with the real timestamppb import:

```go
import "google.golang.org/protobuf/types/known/timestamppb"

// ...

func timestampOf(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}
```

And remove the trailing placeholder definition.

- [ ] **Step 2: Run, verify pass**

```bash
go test ./internal/scheduler/ -v -timeout 30s
```

- [ ] **Step 3: Commit**

```bash
git add internal/scheduler/scheduler_test.go
git commit -m "test(scheduler): expire overdue reservations via tick"
```

---

## Task 25: Final wiring check

- [ ] **Step 1: Run the full suite**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go vet ./...
go test ./...
```

- [ ] **Step 2: End-to-end smoke against running server**

```bash
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/final.db
./bin/migrate --backend=sqlite --dsn=/tmp/final.db up

./bin/server --backend=sqlite --dsn=/tmp/final.db --addr=127.0.0.1:18093 &
PID=$!
sleep 1

# create accounts
curl -fsS -X POST http://127.0.0.1:18093/ledger.v1.LedgerService/CreateAccount \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: t1' \
  -d '{"tenant_id":"t1","owner_type":"user","owner_id":"1","account_type":"cash_available","currency":"USD","normal_balance":"NORMAL_BALANCE_DEBIT"}' >/dev/null

# take a snapshot (bulk)
curl -fsS -X POST http://127.0.0.1:18093/ledger.v1.LedgerService/TakeBalanceSnapshot \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: t1' \
  -d '{"tenant_id":"t1"}'
echo

kill $PID 2>/dev/null; wait $PID 2>/dev/null
rm -rf /tmp/final.db bin
```

Expected: TakeBalanceSnapshot returns `{"snapshotsTaken":1}` (one balance row exists for the newly created account once it gets touched — note that bulk snapshot only captures rows already in `account_balances`; if `snapshotsTaken=0`, post a tiny journal first to materialize the row).

- [ ] **Step 3: Commit (only if something changed)**

```bash
git status
# If clean, you're done.
```

---

## Self-review notes

- Spec sections 1–13 all map to tasks above. Section 11 "two stacked phases" is reflected: Tasks 1–10 are Phase A, Tasks 11–24 are Phase B.
- The `executeFlowInTx` refactor (Task 15) is the load-bearing change in Phase B — all reservation handlers depend on it.
- Reservation idempotency lives at two layers: the reservation row's `idempotency_key UNIQUE` and the per-transition flow keys constructed as `<reservation_id>:commit:<key>` etc.
- Mixed terminal states: the status assignment in commit/release handlers reaches `COMMITTED` when commit drove outstanding to 0, and `RELEASED` when release did. Spec section 4 "Mixed terminal states" is honored.
- All money movement goes through `ExecuteFlow` (now via `executeFlowInTx`). No raw `tx.InsertEntry` calls outside the orchestrator.
- Outbox events emitted: `RESERVATION_CREATED`, `RESERVATION_COMMITTED`, `RESERVATION_RELEASED`, `RESERVATION_EXPIRED`. Snapshot doesn't write an outbox row (deferred, snapshots are read models). If the spec strictly requires `BALANCE_SNAPSHOT_TAKEN`, add an InsertOutbox in Task 6 — left out for YAGNI.
- The CRDB integration test for concurrent transitions on the same reservation is NOT in this plan. It's a small follow-up gated by `//go:build integration` mirroring Task 24. Acceptable to ship without it given the SQLite-side coverage and existing CRDB concurrent reservation test.
