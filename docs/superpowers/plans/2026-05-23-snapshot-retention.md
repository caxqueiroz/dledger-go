# Snapshot Retention / Pruning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a periodic retention tick to the scheduler that deletes `balance_snapshots` rows older than `RetentionAge`, while always preserving the most-recent snapshot per `(tenant, account, currency)`.

**Architecture:** One new sqlc query per backend executes a single safety-preserving `DELETE`. A new `Store.PruneSnapshotsOlderThan` method wraps it. The scheduler gets a third ticker (`retentionTick`) alongside the snapshot and reservation-expiry ticks. No new RPC, no proto changes, no new domain types.

**Tech Stack:** Go 1.26, sqlc per-dialect, SQLite + CockroachDB.

**Design doc:** `docs/superpowers/specs/2026-05-23-snapshot-retention-design.md`

---

## File map

```
sql/queries/sqlite/snapshots.sql                         MODIFY (append PruneSnapshotsOlderThan)
sql/queries/crdb/snapshots.sql                           MODIFY (append PruneSnapshotsOlderThan)
gen/sqlite/snapshots.sql.go                              REGEN
gen/crdb/snapshots.sql.go                                REGEN

internal/repo/repo.go                                    MODIFY (add Store method)
internal/repo/sqlite/store.go                            MODIFY (impl)
internal/repo/crdb/store.go                              MODIFY (impl)

internal/scheduler/scheduler.go                          MODIFY (Config fields, retentionTick, Run)
internal/scheduler/scheduler_test.go                     MODIFY (3 new tests)
```

---

## Task 1: sqlc queries

**Files:**
- Modify: `sql/queries/sqlite/snapshots.sql` (append)
- Modify: `sql/queries/crdb/snapshots.sql` (append)
- Regenerate: `gen/sqlite/snapshots.sql.go`, `gen/crdb/snapshots.sql.go`

- [ ] **Step 1: Append to `sql/queries/sqlite/snapshots.sql`**

```sql

-- name: PruneSnapshotsOlderThan :execrows
DELETE FROM balance_snapshots
WHERE id IN (
  SELECT bs.id FROM balance_snapshots bs
  WHERE bs.snapshot_at < ?
    AND EXISTS (
      SELECT 1 FROM balance_snapshots bs2
      WHERE bs2.tenant_id = bs.tenant_id
        AND bs2.account_id = bs.account_id
        AND bs2.currency = bs.currency
        AND bs2.snapshot_at > bs.snapshot_at
    )
  LIMIT ?
);
```

- [ ] **Step 2: Append to `sql/queries/crdb/snapshots.sql`**

```sql

-- name: PruneSnapshotsOlderThan :execrows
DELETE FROM balance_snapshots
WHERE id IN (
  SELECT bs.id FROM balance_snapshots bs
  WHERE bs.snapshot_at < $1
    AND EXISTS (
      SELECT 1 FROM balance_snapshots bs2
      WHERE bs2.tenant_id = bs.tenant_id
        AND bs2.account_id = bs.account_id
        AND bs2.currency = bs.currency
        AND bs2.snapshot_at > bs.snapshot_at
    )
  LIMIT $2
);
```

- [ ] **Step 3: Regenerate**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" sqlc generate
```

- [ ] **Step 4: Inspect generated signatures**

```bash
grep -A 5 "PruneSnapshotsOlderThan" gen/sqlite/snapshots.sql.go gen/crdb/snapshots.sql.go
```

Expected signatures (the SnapshotAt/Limit types and param-struct field names may differ — record what sqlc actually generated):

- SQLite: likely `PruneSnapshotsOlderThan(ctx, arg PruneSnapshotsOlderThanParams) (int64, error)` with `SnapshotAt string`, `Limit int64`.
- CRDB: likely `PruneSnapshotsOlderThan(ctx, arg PruneSnapshotsOlderThanParams) (int64, error)` with `SnapshotAt pgtype.Timestamptz`, `Limit int32`.

- [ ] **Step 5: Build clean**

```bash
go build ./...
```

- [ ] **Step 6: Commit**

```bash
git add sql/queries/ gen/sqlite/ gen/crdb/
git commit -m "feat(db): add PruneSnapshotsOlderThan query"
```

---

## Task 2: Repository extension

**Files:**
- Modify: `internal/repo/repo.go` (interface)
- Modify: `internal/repo/sqlite/store.go` (impl)
- Modify: `internal/repo/crdb/store.go` (impl)

- [ ] **Step 1: Add to `Store` interface in `internal/repo/repo.go`**

Insert after the existing `ListTenantsDueForSnapshot` line in the snapshots block:

```go
	PruneSnapshotsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
```

- [ ] **Step 2: SQLite impl**

Append to `internal/repo/sqlite/store.go`:

```go
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
```

If sqlc generated different field names in `PruneSnapshotsOlderThanParams`, adjust to match.

- [ ] **Step 3: CRDB impl**

Append to `internal/repo/crdb/store.go`:

```go
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
```

- [ ] **Step 4: Build + test**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go vet ./...
go test ./internal/repo/sqlite/ -v
go test ./internal/service/ -v
go test ./internal/scheduler/ -v
```

All existing tests must still pass.

- [ ] **Step 5: Commit**

```bash
git add internal/repo/
git commit -m "feat(repo): PruneSnapshotsOlderThan for both backends"
```

---

## Task 3: Scheduler config + retentionTick

**Files:**
- Modify: `internal/scheduler/scheduler.go`

- [ ] **Step 1: Add Config fields**

Read the existing `Config` struct first:
```bash
grep -A 10 "type Config struct" internal/scheduler/scheduler.go
```

Add two new fields:

```go
type Config struct {
	SnapshotTick     time.Duration // default 5m
	SnapshotInterval time.Duration // default 24h
	ExpiryTick       time.Duration // default 30s
	RetentionTick    time.Duration // default 1h
	RetentionAge     time.Duration // default 90 * 24h
	BatchN           int           // default 100
}
```

- [ ] **Step 2: Update `New` to set defaults**

Locate the `Cfg: Config{...}` literal in `New`, add:

```go
		Cfg: Config{
			SnapshotTick:     5 * time.Minute,
			SnapshotInterval: 24 * time.Hour,
			ExpiryTick:       30 * time.Second,
			RetentionTick:    1 * time.Hour,
			RetentionAge:     90 * 24 * time.Hour,
			BatchN:           100,
		},
```

- [ ] **Step 3: Add a third ticker arm in `Run`**

The current `Run` has two ticker arms (snapshot + expiry). Add the third:

```go
func (s *Scheduler) Run(ctx context.Context) {
	snapT := time.NewTicker(s.Cfg.SnapshotTick)
	defer snapT.Stop()
	expT := time.NewTicker(s.Cfg.ExpiryTick)
	defer expT.Stop()
	retT := time.NewTicker(s.Cfg.RetentionTick)
	defer retT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-snapT.C:
			s.snapshotTick(ctx)
		case <-expT.C:
			s.expiryTick(ctx)
		case <-retT.C:
			s.retentionTick(ctx)
		}
	}
}
```

- [ ] **Step 4: Implement `retentionTick`**

Add at the bottom of `scheduler.go`:

```go
func (s *Scheduler) retentionTick(ctx context.Context) {
	cutoff := time.Now().Add(-s.Cfg.RetentionAge)
	n, err := s.Store.PruneSnapshotsOlderThan(ctx, cutoff, s.Cfg.BatchN)
	if err != nil {
		s.Log.WarnContext(ctx, "scheduler.retention.error", "err", err)
		return
	}
	if n > 0 {
		s.Log.InfoContext(ctx, "scheduler.retention.deleted",
			"count", n, "cutoff", cutoff.Format(time.RFC3339))
	}
}
```

- [ ] **Step 5: Build + existing tests**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go vet ./...
go test ./internal/scheduler/ -v
```

All existing scheduler tests must still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/scheduler/scheduler.go
git commit -m "feat(scheduler): retentionTick prunes old snapshots"
```

---

## Task 4: Tests

**Files:**
- Modify: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: Add the three tests**

Append at the end of `internal/scheduler/scheduler_test.go`. Each test inserts snapshots by calling the store's `InsertSnapshot` directly (the public RPC sets `snapshot_at = s.Now()`, so we can't control the timestamp from outside without a helper — direct insert is simpler).

```go
func TestRetention_DeletesOldNonLatestSnapshot(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	// Create an account so the FK constraint on balance_snapshots holds.
	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	old := ledger.BalanceSnapshot{
		ID: "snap-old", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 0,
		SnapshotAt: time.Now().Add(-48 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, old); err != nil {
		t.Fatalf("insert old: %v", err)
	}
	newer := ledger.BalanceSnapshot{
		ID: "snap-new", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 1,
		SnapshotAt: time.Now().Add(-1 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, newer); err != nil {
		t.Fatalf("insert new: %v", err)
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionTick = 50 * time.Millisecond
	sched.Cfg.RetentionAge = 24 * time.Hour
	sched.Cfg.SnapshotTick = time.Hour
	sched.Cfg.ExpiryTick = time.Hour
	runCtx, cancel := context.WithCancel(ctx)
	go sched.Run(runCtx)
	defer cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now().Add(-25*time.Hour))
		if err != nil {
			t.Fatalf("get old: %v", err)
		}
		if got == nil {
			// Old snapshot is gone. Verify the new one survives.
			newest, nerr := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now())
			if nerr != nil {
				t.Fatalf("get newest: %v", nerr)
			}
			if newest == nil || newest.ID != "snap-new" {
				t.Fatalf("newest snapshot missing; want snap-new")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("old snapshot was not deleted in time")
}

func TestRetention_PreservesMostRecentEvenWhenOlder(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	// One snapshot, far in the past — must be preserved because nothing newer exists.
	only := ledger.BalanceSnapshot{
		ID: "snap-only", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 0,
		SnapshotAt: time.Now().Add(-365 * 24 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, only); err != nil {
		t.Fatalf("insert only: %v", err)
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionTick = 50 * time.Millisecond
	sched.Cfg.RetentionAge = 30 * 24 * time.Hour
	sched.Cfg.SnapshotTick = time.Hour
	sched.Cfg.ExpiryTick = time.Hour
	runCtx, cancel := context.WithCancel(ctx)
	go sched.Run(runCtx)
	defer cancel()

	// Wait for at least one retention tick to fire.
	time.Sleep(300 * time.Millisecond)

	got, err := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil || got.ID != "snap-only" {
		t.Fatalf("only snapshot was deleted despite being the most-recent for its key")
	}
}

func TestRetention_RespectsBatchLimit(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	// Insert 6 snapshots: 1 fresh (most-recent, will be preserved) and 5 olds
	// (all eligible because the fresh one is newer than all of them).
	fresh := ledger.BalanceSnapshot{
		ID: "snap-fresh", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 999,
		SnapshotAt: time.Now().Add(-1 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, fresh); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}
	for i := 0; i < 5; i++ {
		old := ledger.BalanceSnapshot{
			ID:           fmt.Sprintf("snap-old-%d", i),
			TenantID:     "t1",
			AccountID:    acct,
			Currency:     "USD",
			PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: int64(i),
			SnapshotAt: time.Now().Add(time.Duration(-(48 + i)) * time.Hour),
		}
		if err := store.InsertSnapshot(ctx, old); err != nil {
			t.Fatalf("insert old %d: %v", i, err)
		}
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionTick = 200 * time.Millisecond
	sched.Cfg.RetentionAge = 24 * time.Hour
	sched.Cfg.BatchN = 2
	sched.Cfg.SnapshotTick = time.Hour
	sched.Cfg.ExpiryTick = time.Hour

	// Manually call retentionTick once (rather than racing the scheduler) so
	// we can assert the exact count after one batch.
	sched.RetentionTickForTest(ctx)

	remaining := countSnapshots(t, store, acct)
	// Started with 6, batch of 2 removed → 4 remain.
	if remaining != 4 {
		t.Fatalf("after one tick: want 4 snapshots remaining, got %d", remaining)
	}

	// Second tick: 4 → 2.
	sched.RetentionTickForTest(ctx)
	remaining = countSnapshots(t, store, acct)
	if remaining != 2 {
		t.Fatalf("after two ticks: want 2 remaining, got %d", remaining)
	}
}

func countSnapshots(t *testing.T, store *sqlite.Store, accountID string) int {
	t.Helper()
	row := store.DB().QueryRow(
		"SELECT COUNT(*) FROM balance_snapshots WHERE tenant_id = ? AND account_id = ?",
		"t1", accountID,
	)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
```

The third test calls `RetentionTickForTest` directly. Expose it from the scheduler:

- [ ] **Step 2: Expose `RetentionTickForTest`**

Append to `internal/scheduler/scheduler.go`:

```go
// RetentionTickForTest exposes retentionTick for deterministic test invocations.
// Production code uses the scheduler's Run loop.
func (s *Scheduler) RetentionTickForTest(ctx context.Context) {
	s.retentionTick(ctx)
}
```

- [ ] **Step 3: Add imports to scheduler_test.go**

Make sure the file imports:

```go
import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/scheduler"
	"github.com/caxqueiroz/dledger-go/internal/service"
)
```

If `fmt`, `decimal`, or `ledger` aren't already imported, add them.

- [ ] **Step 4: Run**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go test ./internal/scheduler/ -v -run Retention
```

All three tests must pass.

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/
git commit -m "test(scheduler): retention deletes old, preserves latest, respects batch"
```

---

## Task 5: Final wiring check

- [ ] **Step 1: Full suite**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go vet ./...
go test ./...
golangci-lint run ./...
```

All three must be clean.

- [ ] **Step 2: Smoke against a running server**

```bash
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/ret-e2e.db
./bin/migrate --backend=sqlite --dsn=/tmp/ret-e2e.db up
./bin/server --backend=sqlite --dsn=/tmp/ret-e2e.db --addr=127.0.0.1:18095 &
PID=$!
sleep 1
curl -fsS http://127.0.0.1:18095/healthz; echo
kill $PID 2>/dev/null; wait $PID 2>/dev/null
rm -rf /tmp/ret-e2e.db bin
```

Expected: server starts cleanly, scheduler logs nothing concerning, healthz 200.

- [ ] **Step 3: Commit if anything moved; otherwise done**

```bash
git status
```

---

## Self-review notes

- **Spec coverage**: §2 algorithm → Task 1; §3 repo method → Task 2; §4 scheduler config + tick → Task 3; §5 tests → Task 4. §6 concurrency is documented behavior, no implementation; §7 observability is in the tick body (Task 3). All defaults from §8 acceptance criteria are set in Task 3 step 2.
- **No placeholders**. The `RetentionTickForTest` helper is concrete (Task 4 step 2).
- **Type consistency**: `PruneSnapshotsOlderThan` signature `(ctx, cutoff time.Time, limit int) (int64, error)` is the same in interface + both impls + scheduler call. `Cfg.RetentionAge`, `Cfg.RetentionTick`, `Cfg.BatchN` are used in `retentionTick` and configured in tests.
