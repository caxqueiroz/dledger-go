# Snapshot Retention / Pruning — Design

Date: 2026-05-23
Module: `github.com/caxqueiroz/dledger-go`

## 1. Purpose and scope

Cap unbounded growth of the `balance_snapshots` table by deleting rows older than a configurable cutoff. The most-recent snapshot per `(tenant, account, currency)` is always preserved, so historical reconstruction via `GetBalance(as_of=T)` continues to work for accounts that haven't been touched recently.

In scope:

- One new repository method `PruneSnapshotsOlderThan`.
- One new sqlc query per backend.
- A new `retentionTick` in `internal/scheduler` (default hourly).
- Two new config knobs: `RetentionTick` (interval) and `RetentionAge` (cutoff).
- Three SQLite-backed tests (delete old, preserve most-recent, respect batch limit).

Out of scope:

- Admin RPC. The tick is enough for routine cleanup; one-off purges can use direct SQL.
- Count-based retention (keep last N) — time-window is enough.
- Per-tenant retention policies.
- Closed-account special-case deletion (kept simple per design call).

## 2. Algorithm

A single SQL statement per backend deletes a batch of "safely deletable" snapshots — those older than the cutoff for which a newer snapshot exists for the same `(tenant, account, currency)`:

```sql
DELETE FROM balance_snapshots
WHERE id IN (
  SELECT bs.id FROM balance_snapshots bs
  WHERE bs.snapshot_at < ?               -- cutoff
    AND EXISTS (
      SELECT 1 FROM balance_snapshots bs2
      WHERE bs2.tenant_id   = bs.tenant_id
        AND bs2.account_id  = bs.account_id
        AND bs2.currency    = bs.currency
        AND bs2.snapshot_at > bs.snapshot_at
    )
  LIMIT ?                                 -- batch
);
```

The `EXISTS` clause is the safety: if no newer snapshot exists for the same key, the row is preserved regardless of age. This guarantees `GetBalance(as_of=T)` can always start from some snapshot (or zero, if none ever existed) for active accounts.

The query is identical in shape for SQLite and CRDB. Both backends already have the supporting index `(tenant_id, account_id, currency, snapshot_at DESC)`, which makes the `EXISTS` lookup cheap.

## 3. Repository extension

New `Store` method:

```go
PruneSnapshotsOlderThan(ctx context.Context, cutoff time.Time, limit int) (deleted int64, err error)
```

- Returns the number of rows actually deleted in this call.
- Returns 0 (and `nil` error) when nothing matches — perfectly fine for the tick to no-op.
- The `limit` caps work per call so a backlog doesn't stall the scheduler goroutine.

New sqlc queries:

- `sql/queries/sqlite/snapshots.sql` — append:
  ```sql
  -- name: PruneSnapshotsOlderThan :execrows
  DELETE FROM balance_snapshots ...
  ```
- `sql/queries/crdb/snapshots.sql` — same with `$1`/`$2` placeholders.

`:execrows` returns the affected count, which the Store method surfaces.

## 4. Scheduler integration

Add to `internal/scheduler.Config`:

```go
RetentionTick time.Duration  // default 1 * time.Hour
RetentionAge  time.Duration  // default 90 * 24 * time.Hour
```

`Run` adds a third ticker and `select` arm:

```go
retT := time.NewTicker(s.Cfg.RetentionTick)
defer retT.Stop()
// ...
case <-retT.C:
    s.retentionTick(ctx)
```

`retentionTick`:

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

Errors are logged and the next tick retries. There's no compensation needed — the same query runs again next hour and picks up where it left off.

## 5. Tests

`internal/scheduler/scheduler_test.go` (SQLite-backed) gets three new tests:

1. **`TestRetention_DeletesOldNonLatestSnapshot`** — seed two snapshots for the same account at different times. Run the tick with `RetentionAge` set short enough to make the older one eligible. Assert the old snapshot is gone, the new one survives.
2. **`TestRetention_PreservesMostRecentEvenWhenOlder`** — seed one snapshot whose `snapshot_at` is older than the cutoff. Run the tick. Assert the snapshot still exists (it's the most-recent for its key).
3. **`TestRetention_RespectsBatchLimit`** — seed 5 deletable rows for the same account (one fresh + 5 olds where each has a newer one above it). Configure `BatchN=2`. Run the tick once. Assert exactly 2 deletions; the remaining 3 stay until the next tick.

The existing scheduler tests already validate the dispatcher startup; no scheduler-Run wiring tests need re-running.

## 6. Concurrency

The prune query runs outside any flow transaction — it's a single autonomous `DELETE`. Concurrent writers (snapshot inserts, balance updates) are unaffected; the `EXISTS` predicate sees a consistent snapshot of the table because the statement is one SQL operation. No FOR UPDATE needed because we're not locking rows for downstream use.

If a snapshot is inserted between the inner SELECT and the outer DELETE (CRDB SI), the worst case is we delete a row that's about to become non-most-recent — the new insert is unaffected, and the next tick will clean up the now-stale rows. No correctness issue.

## 7. Observability

- `slog.Info` on every tick that actually deleted rows: `count`, `cutoff`.
- `slog.Warn` on errors with the wrapped error.
- No new metric counter for now; can be added later if operators need it.

## 8. Acceptance criteria

- A snapshot whose `snapshot_at` is older than `now - RetentionAge` AND has a newer sibling is deleted by the retention tick.
- The most-recent snapshot per `(tenant, account, currency)` is preserved regardless of age.
- The tick deletes at most `BatchN` rows per fire; a larger backlog drains over multiple ticks.
- Defaults (1h tick, 90d age, 100 batch) ship in `scheduler.New`.

## 9. Out of scope

- Admin RPC for on-demand purges.
- Count-based retention.
- Per-tenant policy overrides.
- Closed-account fast-path deletion.
- Snapshot compaction (delta snapshots).
- Reconciliation (separate design doc later).
