# Balance Snapshots and Reservations — Design

Date: 2026-05-22
Module: `github.com/caxqueiroz/doubleledger`

## 1. Purpose

Two new product features on top of the MVP ledger:

1. **Balance snapshots** — point-in-time captures of `account_balances`, taken on a schedule or on demand, used to answer "what was the balance at time T?" without scanning the full entry history for active accounts.
2. **Reservations** — first-class held-fund objects with a state machine (`HELD → PARTIAL → COMMITTED|RELEASED|EXPIRED`), partial commit/release support, and auto-expiry driven by an in-process scheduler.

Both features layer on the existing `ExecuteFlow` orchestrator. All money movement still goes through `ExecuteFlow` so atomicity, idempotency, locking, and outbox semantics are preserved.

## 2. Architecture

No new top-level packages beyond `internal/scheduler`. New files only.

```
internal/ledger/
    reservation.go            # Reservation + ReservationStatus
    snapshot.go               # BalanceSnapshot
internal/repo/
    repo.go                   # extend Store/Tx with new verbs
internal/repo/sqlite/        # impl
internal/repo/crdb/          # impl
internal/service/
    create_reservation.go
    commit_reservation.go
    release_reservation.go
    expire_reservation.go     # internal helper, not an exposed RPC
    take_snapshot.go
    get_balance.go            # extended for as_of
internal/scheduler/
    scheduler.go              # two tickers: reservation expiry + snapshot
sql/migrations/{sqlite,crdb}/
    0002_reservations_snapshots.sql
proto/ledger/v1/ledger.proto # add 4 RPCs + extend GetBalance
```

`cmd/server` launches the scheduler alongside the outbox dispatcher; both share the server's signal-cancelled context.

## 3. Data model

### `reservations`

| Column | Type | Notes |
|---|---|---|
| `id` | string PK | UUID, server-generated |
| `tenant_id` | string | scopes every query |
| `idempotency_key` | string UNIQUE | required at create time |
| `source_account_id` | string FK accounts | the account being held *from* |
| `reserved_account_id` | string FK accounts | the destination "hold" account |
| `currency` | string | must match both accounts' currencies |
| `original_amount` | DECIMAL(38,18) | amount when the reservation was created |
| `outstanding_amount` | DECIMAL(38,18) | what's still held; decreases on partial commit/release |
| `committed_amount` | DECIMAL(38,18) | running total committed |
| `released_amount` | DECIMAL(38,18) | running total released (caller-initiated + expiry) |
| `status` | string CHECK | `HELD`, `PARTIAL`, `COMMITTED`, `RELEASED`, `EXPIRED` |
| `expires_at` | timestamp NULL | NULL = no auto-expiry; scheduler scans non-null values |
| `flow_run_id` | string FK flow_runs | the ExecuteFlow run that created the hold |
| `metadata` | JSON | caller-supplied; opaque to the engine |
| `created_at`, `updated_at` | timestamp | |

Invariant (enforced in service-layer transitions):
```
original_amount == outstanding_amount + committed_amount + released_amount
```

Index: `(tenant_id, status, expires_at)` — drives scheduler scans.

### `balance_snapshots`

| Column | Type | Notes |
|---|---|---|
| `id` | string PK | UUID, server-generated |
| `tenant_id` | string | |
| `account_id` | string FK accounts | |
| `currency` | string | |
| `posted_debits`, `posted_credits` | DECIMAL(38,18) | captured at snapshot time |
| `version` | int8 | balance row's version at capture |
| `snapshot_at` | timestamp | logical "as of" time |
| `created_at` | timestamp | wall-clock insert |

Index: `(tenant_id, account_id, currency, snapshot_at DESC)` — drives point-in-time lookup.

## 4. Reservation lifecycle

```
            CreateReservation(amount=A)
                       │
                       ▼
                    HELD ────── partial commit(x) ─────▶ PARTIAL ─── final commit ─▶ COMMITTED
                     │                                      │
                     ├── release ──▶ RELEASED               ├── release ──▶ RELEASED
                     │                                      ├── expire  ──▶ EXPIRED
                     ├── expire  ──▶ EXPIRED                └── partial commit/release ─▶ stays PARTIAL
                     │
                     └── full commit ──▶ COMMITTED
```

Each transition runs an internal `ExecuteFlow`. Concurrency: every transition holds `SELECT … FOR UPDATE` on the reservation row first, then runs the flow.

### Transitions

| Transition | Trigger | Journal entries | Resulting status |
|---|---|---|---|
| **Create** | `CreateReservation` RPC | Debit `reserved_account`, Credit `source_account` for `original_amount` | `HELD` |
| **Commit (full)** | `CommitReservation` with `amount = outstanding` | Debit caller-supplied `destination_account`, Credit `reserved_account` for that amount | `COMMITTED` |
| **Commit (partial)** | `CommitReservation` with `amount < outstanding` | same shape, smaller amount | `PARTIAL` |
| **Release (full)** | `ReleaseReservation` with `amount = outstanding` | Debit `source_account`, Credit `reserved_account` for that amount | `RELEASED` |
| **Release (partial)** | `ReleaseReservation` with `amount < outstanding` | same shape, smaller amount | `PARTIAL` |
| **Expire** | Scheduler | Same as release (full of remaining `outstanding_amount`) | `EXPIRED` |

After every transition the invariant is re-checked. Updates to `outstanding_amount`, `committed_amount`, `released_amount`, `status`, `updated_at` happen in the same DB transaction as the flow.

### Status semantics

- `HELD` — nothing committed or released yet.
- `PARTIAL` — at least one partial op applied; still some `outstanding > 0`.
- `COMMITTED` — `outstanding == 0` and all closed via commit (or mix that reached zero via commits + releases — see "Mixed terminal states" below).
- `RELEASED` — closed by caller-initiated release(s) with `committed_amount == 0`.
- `EXPIRED` — closed by scheduler-driven expire with `committed_amount == 0`.

**Mixed terminal states.** If a reservation receives partial commits AND partial releases, the final status is set by which path drove `outstanding` to zero:
- Last transition was commit → `COMMITTED`.
- Last transition was release → `RELEASED`.
- Last transition was expire → `EXPIRED`.

`committed_amount` and `released_amount` remain visible on the row so callers can audit the breakdown.

### Idempotency

Reservation-level: `reservations.idempotency_key UNIQUE`. Replays of `CreateReservation` with the same key return the existing reservation without modifying it.

Transition-level: each commit/release call carries its own `idempotency_key`. The underlying `ExecuteFlow` uses `<reservation_id>:commit:<key>` / `<reservation_id>:release:<key>` as its flow-level idempotency key, so retries are safe.

Scheduler-driven expiry uses `<reservation_id>:expire` as its key — a reservation can be expired at most once.

## 5. Scheduler

A new `internal/scheduler` package exposes:

```go
type Scheduler struct {
    Store      repo.Store
    Server     *service.Server     // for calling internal helpers
    ExpiryTick time.Duration       // default 30s
    Snapshot   SnapshotConfig
    Log        *slog.Logger
}

type SnapshotConfig struct {
    Tick     time.Duration // default 5 min
    Interval time.Duration // default 24 h — minimum age before re-snapshotting a tenant
    BatchN   int           // accounts processed per tick
}

func (s *Scheduler) Run(ctx context.Context) // blocks until ctx canceled
```

Two independent goroutines:

1. **Reservation expiry tick** — every `ExpiryTick`:
   ```sql
   SELECT id FROM reservations
   WHERE status IN ('HELD','PARTIAL')
     AND expires_at IS NOT NULL
     AND expires_at <= now()
   LIMIT N FOR UPDATE SKIP LOCKED;
   ```
   For each, call `Server.ExpireReservation(ctx, tenantID, id)` (non-exported helper, not a public RPC). Errors are logged; the next tick retries.

2. **Snapshot tick** — every `SnapshotConfig.Tick`:
   - For each tenant, find accounts whose newest snapshot is older than `SnapshotConfig.Interval`.
   - Take a snapshot for those accounts (batch up to `BatchN` per tick).
   - Tenants are discovered by scanning distinct tenant IDs from `accounts` (cheap, indexed-friendly).

The scheduler is launched in `cmd/server`:
```go
sched := scheduler.New(...)
go sched.Run(ctx)
```

Same lifecycle as the outbox dispatcher. Both stop on signal-cancelled context.

## 6. RPC surface (additions)

```proto
service LedgerService {
  // ...existing 7...
  rpc CreateReservation(CreateReservationRequest) returns (CreateReservationResponse);
  rpc CommitReservation(CommitReservationRequest) returns (CommitReservationResponse);
  rpc ReleaseReservation(ReleaseReservationRequest) returns (ReleaseReservationResponse);
  rpc GetReservation(GetReservationRequest) returns (GetReservationResponse);
  rpc TakeBalanceSnapshot(TakeBalanceSnapshotRequest) returns (TakeBalanceSnapshotResponse);
}

message CreateReservationRequest {
  string tenant_id = 1;
  string idempotency_key = 2;
  string source_account_id = 3;
  string reserved_account_id = 4;
  string currency = 5;
  string amount = 6;                                    // decimal string
  google.protobuf.Timestamp expires_at = 7;             // optional
  string source_service = 8;
  string actor_id = 9;
  google.protobuf.Struct metadata = 10;
}

message Reservation {
  string id = 1;
  string tenant_id = 2;
  string status = 3;                                    // HELD/PARTIAL/...
  string source_account_id = 4;
  string reserved_account_id = 5;
  string currency = 6;
  string original_amount = 7;
  string outstanding_amount = 8;
  string committed_amount = 9;
  string released_amount = 10;
  google.protobuf.Timestamp expires_at = 11;
  google.protobuf.Timestamp created_at = 12;
  google.protobuf.Timestamp updated_at = 13;
  string flow_run_id = 14;
}
message CreateReservationResponse { Reservation reservation = 1; }

message CommitReservationRequest {
  string tenant_id = 1;
  string reservation_id = 2;
  string destination_account_id = 3;
  string amount = 4;                                    // ≤ outstanding
  string idempotency_key = 5;
  string source_service = 6;
  string actor_id = 7;
}
message CommitReservationResponse { Reservation reservation = 1; }

message ReleaseReservationRequest {
  string tenant_id = 1;
  string reservation_id = 2;
  string amount = 3;                                    // ≤ outstanding
  string idempotency_key = 4;
  string source_service = 5;
  string actor_id = 6;
}
message ReleaseReservationResponse { Reservation reservation = 1; }

message GetReservationRequest { string tenant_id = 1; string reservation_id = 2; }
message GetReservationResponse { Reservation reservation = 1; }

message TakeBalanceSnapshotRequest {
  string tenant_id = 1;
  // If account_id+currency are set, snapshot one row. If both empty, snapshot
  // the entire tenant.
  string account_id = 2;
  string currency = 3;
}
message TakeBalanceSnapshotResponse {
  int32 snapshots_taken = 1;
}

// Extend GetBalance:
message GetBalanceRequest {
  string tenant_id = 1;
  string account_id = 2;
  string currency = 3;
  google.protobuf.Timestamp as_of = 4;                  // new
}
```

`GetBalance` semantics with `as_of`:
1. Find the latest snapshot row with `snapshot_at ≤ as_of`. If none, treat as `posted_debits=0, posted_credits=0`.
2. Sum all `ledger_entries` for that `(tenant_id, account_id, currency)` with `created_at > snapshot.snapshot_at AND created_at <= as_of`.
3. Add those sums to the snapshot's posted-debits / posted-credits.
4. Normalize via the account's `normal_balance` and return.

Without `as_of`, `GetBalance` returns the current row from `account_balances` exactly as today.

## 7. Error mapping

| Domain code | Connect code | Scenario |
|---|---|---|
| `CodeReservationNotFound` | `NotFound` | reservation_id doesn't exist |
| `CodeReservationClosed` | `FailedPrecondition` | status in (COMMITTED, RELEASED, EXPIRED) |
| `CodeReservationAmountExceeds` | `InvalidArgument` | commit/release amount > outstanding |
| `CodeReservationCurrencyMismatch` | `InvalidArgument` | destination account currency ≠ reservation currency |
| `CodeSnapshotNoData` | `NotFound` | tenant has no accounts (TakeBalanceSnapshot bulk variant) |

Add these to `internal/ledger/errors.go` and `internal/service/errors.go`.

## 8. Concurrency

- `CreateReservation`: opens a flow tx; inserts the reservation row in the same tx as the flow's journal/entries/balances. The unique idempotency_key handles concurrent duplicate creates.
- `CommitReservation` / `ReleaseReservation`: open flow tx; `SELECT … FOR UPDATE` the reservation row before reading current state; abort with `FailedPrecondition` if closed. Then run the flow and update the reservation row in the same tx.
- Scheduler `Expire`: uses `FOR UPDATE SKIP LOCKED` to claim a batch — multiple scheduler replicas (future) won't race on the same row.
- SQLite path: relies on `BEGIN IMMEDIATE` as today; `FOR UPDATE` is treated as a no-op (the whole tx is exclusive).

## 9. Outbox events

New event types written inside the same tx:

- `RESERVATION_CREATED`
- `RESERVATION_COMMITTED` (per commit; payload includes `amount`, remaining `outstanding`)
- `RESERVATION_RELEASED` (per release)
- `RESERVATION_EXPIRED` (scheduler-driven)
- `BALANCE_SNAPSHOT_TAKEN` (per snapshot row)

Idempotency keys mirror the flow keys.

## 10. Testing

### Unit (domain)

- Invariant check helper: `Reservation.Validate()` enforces `original == outstanding + committed + released`.
- Status transitions: table-driven test of all legal transitions.

### Service-level (SQLite-backed; reuse `newServer`/`newServerWithStore`)

- `CreateReservation` happy path: balances move from source → reserved; reservation row present with `status=HELD`.
- Partial commit → status `PARTIAL`; balances correct.
- Full commit on `PARTIAL` → status `COMMITTED`; outstanding == 0.
- Partial release → status `PARTIAL`.
- Mixed commits + releases reaching zero via commit → `COMMITTED`; via release → `RELEASED`; via expire → `EXPIRED`.
- Commit on closed reservation → `FailedPrecondition`.
- Commit/release amount > outstanding → `InvalidArgument`.
- Idempotent replay of `CreateReservation` with same key.
- Idempotent replay of `CommitReservation` and `ReleaseReservation` (same `idempotency_key`).

### Scheduler

- Expiry: insert a reservation with `expires_at` 1s in the past; run one scheduler tick (or call the helper directly); reservation should be `EXPIRED` and balances moved back to source.
- Concurrent expire + caller release: only one transition succeeds; the other returns `FailedPrecondition`.

### Snapshots

- `TakeBalanceSnapshot` creates a row matching current `account_balances`.
- `GetBalance(..., as_of=T)` where T is between two entries returns the correct reconstructed balance.
- `GetBalance(..., as_of=T)` with no snapshot present sums entries from inception.

### CRDB integration (build tag `integration`)

- Concurrent commit attempts on the same reservation → exactly one succeeds (others get `FailedPrecondition`); 40001 retries handled.

## 11. Migration plan

This delivery is split into **two stacked phases**:

1. **Phase A — Snapshots.** Migration `0002` adds only `balance_snapshots`. New RPCs: `TakeBalanceSnapshot`. `GetBalance` extended with `as_of`. Snapshot tick added to scheduler skeleton.
2. **Phase B — Reservations.** Migration `0003` adds `reservations`. New RPCs and reservation-expiry tick. Reservation-related error codes.

Each phase is one mergeable PR. Phase B depends on Phase A only for the shared scheduler skeleton.

## 12. Acceptance criteria

- `TakeBalanceSnapshot` persists a row equal to the current balance.
- `GetBalance(as_of=T)` returns the exact balance that existed at time T (verified by comparing against running totals from entry history in tests).
- A reservation can be created, partially committed, partially released, and finally closed; balances and reservation totals reconcile at every step.
- An expired reservation has its outstanding amount returned to the source account.
- Concurrent transitions on the same reservation are serialised; exactly one wins.
- All money movement still flows through `ExecuteFlow` — no direct repo writes to `account_balances` in the reservation/snapshot code paths.

## 13. Out of scope

- Cross-tenant reservations.
- Snapshot retention / pruning (future cleanup job).
- Snapshot compaction (e.g. delta snapshots) — full row each time.
- FX-driven reservations.
- Webhook-style pre-transaction hooks (Blnk has these; we still don't).
- Restoring scheduler state after crash beyond the natural "scan again next tick" recovery.
