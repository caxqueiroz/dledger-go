# Architecture

`dledger-go` is a product-neutral double-entry ledger service in Go. Product engines submit accounting intents; they do not mutate balances directly. Every balance change is the result of a balanced journal posted inside a single transaction.

## At a glance

```
                                          ┌───────────────────────┐
        Connect-RPC / HTTP                 │   cmd/server          │
   gRPC, gRPC-Web, HTTP+JSON ─────────────▶│   - interceptors      │
        on one port :8080                  │   - outbox dispatcher │
                                          │   - scheduler         │
                                          └──────────┬────────────┘
                                                     │
                  ┌──────────────────────────────────┼───────────────────┐
                  ▼                                  ▼                   ▼
        ┌─────────────────┐               ┌──────────────────┐   ┌────────────────┐
        │ internal/service│               │ internal/outbox  │   │ internal/      │
        │  RPC handlers + │               │  Sink, polling   │   │ scheduler      │
        │  ExecuteFlow    │               │  Dispatcher      │   │  expiry +      │
        │  orchestrator   │               │                  │   │  snapshot ticks│
        └────────┬────────┘               └──────────────────┘   └────────┬───────┘
                 │                                                        │
                 ▼                                                        │
        ┌─────────────────┐                                               │
        │ internal/repo   │◀──────────────────────────────────────────────┘
        │   Store + Tx    │
        │   interfaces    │
        └────────┬────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
 ┌─────────────┐   ┌─────────────┐
 │ repo/sqlite │   │ repo/crdb   │
 │  BEGIN      │   │  SERIALIZABLE
 │  IMMEDIATE  │   │  + 40001    │
 │             │   │  retry      │
 └──────┬──────┘   └──────┬──────┘
        │                 │
        ▼                 ▼
 ┌─────────────┐   ┌─────────────────┐
 │ SQLite      │   │ CockroachDB     │
 │ (modernc.org│   │ (pgx)           │
 │  /sqlite)   │   │                 │
 └─────────────┘   └─────────────────┘

Domain types (internal/ledger) — pure Go, no DB or transport:
  Account, Journal, Entry, FlowRun, FlowStep, Reservation, BalanceSnapshot,
  DomainError, DomainCode
```

## Layers

| Layer | Path | Responsibility |
|---|---|---|
| Domain | `internal/ledger` | Money parsing, account/journal/flow/reservation types, per-currency balance validation, normal-balance arithmetic, domain error codes. No DB, no proto. |
| Repository | `internal/repo` + `internal/repo/{sqlite,crdb}` | `Store` + `Tx` interfaces. Two backends. CRDB does row-level `FOR UPDATE`; SQLite serializes writes via `BEGIN IMMEDIATE` + single-writer pool. |
| Service | `internal/service` | Connect-RPC handlers. `ExecuteFlow` orchestrator (atomic multi-step). Reservation lifecycle handlers. Snapshot read-side. |
| Scheduler | `internal/scheduler` | Polling goroutine: expires overdue reservations; future hook for periodic snapshots. |
| Outbox | `internal/outbox` | `Sink` interface, polling `Dispatcher`, `RepoAdapter` bridging `repo.Store`. |
| Observability | `internal/observability` | OTel `TracerProvider` setup; OTLP/HTTP exporter when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. |
| Entry points | `cmd/server`, `cmd/migrate` | HTTP server + goose-driven migration runner. |

## Data model

Nine tables, all tenant-scoped. Each row has `tenant_id` and queries filter on it.

| Table | Purpose | Key constraints |
|---|---|---|
| `accounts` | Ledger accounts | UNIQUE `(tenant_id, owner_type, owner_id, account_type, currency)`. `normal_balance` ∈ {DEBIT,CREDIT}. `status` ∈ {ACTIVE,FROZEN,CLOSED}. |
| `ledger_journals` | One per accounting event | `event_id` UNIQUE. Links to `flow_runs` via `flow_run_id`. |
| `ledger_entries` | Debit/credit lines | `amount > 0`, `direction` ∈ {DEBIT,CREDIT}. Index `(tenant_id, account_id, currency, created_at)`. |
| `account_balances` | Per `(account, currency)` running totals | PK `(tenant_id, account_id, currency)`. `posted_debits` / `posted_credits` decimal. `version` increments on every update. |
| `flow_runs` | One per atomic multi-step operation | `idempotency_key` UNIQUE. `status` ∈ {RUNNING,COMPLETED,FAILED}. |
| `flow_steps` | One per step inside a flow | UNIQUE `(tenant_id, flow_run_id, step_id)`. |
| `outbox_events` | Transactional event queue | `idempotency_key` UNIQUE. Index on `(publish_state, created_at)` for the dispatcher. |
| `balance_snapshots` | Point-in-time `account_balances` captures | Index on `(tenant_id, account_id, currency, snapshot_at DESC)` for `as_of` queries. |
| `reservations` | Held-fund objects | `idempotency_key` UNIQUE. `status` ∈ {HELD,PARTIAL,COMMITTED,RELEASED,EXPIRED}. Index on `(tenant_id, status, expires_at)` for expiry scans. |
| `fx_rates` | FX rate history for ExecuteExchange | UNIQUE `(tenant_id, base_currency, quote_currency, effective_at, source)`; index on `(tenant_id, base, quote, effective_at DESC)`. |
| `external_records` | Ingested external transaction records | UNIQUE `(tenant_id, source, external_ref)`; indexes on `(tenant, source, occurred_at)` and `(tenant, source, match_status)`. |
| `reconciliation_batches` | One row per `RunReconciliation` call | UNIQUE `idempotency_key`. `status` ∈ {RUNNING, COMPLETED, FAILED}. Summary counts populated on completion. |
| `discrepancies` | Surfaces of unmatched/mismatched pairs | `type` ∈ {MISSING_IN_LEDGER, MISSING_IN_EXTERNAL, AMOUNT_MISMATCH}; `status` ∈ {OPEN, RESOLVED, IGNORED}. Index on `(tenant, status, batch_id)`. |

## Key flows

### `ExecuteFlow` — atomic multi-step orchestrator

```
                              ┌─────────────────────────────────────────┐
                              │             ExecuteFlow                 │
                              │           (single DB tx)                │
                              │                                         │
  client request              │  1. Idempotency lookup                  │
  (tenant_id,                 │     ├─ already completed → replay        │
   flow_type,                 │     │   existing flow_run + steps        │
   idempotency_key,           │     └─ in-flight → FLOW_CONFLICT         │
   steps[N])                  │                                         │
  ───────────────────────────▶│  2. Insert flow_run (RUNNING)            │
                              │  3. Collect unique (account, currency)  │
                              │     keys, sort deterministically        │
                              │  4. SELECT … FOR UPDATE balances        │
                              │     (deadlock-free via stable order)    │
                              │  5. Per step:                           │
                              │     ├─ Validate journal (per-currency   │
                              │     │   debit/credit equality)          │
                              │     ├─ Insert journal + entries         │
                              │     ├─ Accumulate balance deltas        │
                              │     ├─ Insert flow_step (COMPLETED)     │
                              │     └─ Insert outbox event              │
                              │  6. INSUFFICIENT_FUNDS check (post-     │
                              │     apply, on non-overdraft accts)     │
                              │  7. UPDATE balances                     │
                              │  8. Complete flow_run                   │
                              │  9. COMMIT                              │
                              └──────────────┬──────────────────────────┘
                                             ▼
                                  Outbox dispatcher (async)
                                  picks up events post-commit
```

Any error before commit triggers full rollback; the outbox writes never escape.

CRDB-only: the whole orchestrator body is retried on SQLSTATE `40001` (`SerializationFailure`) up to a capped backoff. Final exhaustion returns `SERIALIZATION_RETRY_EXHAUSTED`.

### Reservations — state machine

```
                   CreateReservation(amount=A)
                            │
                            ▼
                         HELD ─── commit(x) ────▶ PARTIAL ─── final commit ──▶ COMMITTED
                          │                          │
                          ├─ release ──▶ RELEASED    ├─ release ────▶ RELEASED
                          │                          ├─ expire   ────▶ EXPIRED
                          ├─ expire  ──▶ EXPIRED     └─ partial cmd ─▶ stays PARTIAL
                          │
                          └─ full commit ──▶ COMMITTED
```

Every transition runs an internal `ExecuteFlow` against an existing tx (`executeFlowInTx`), then updates the reservation row in the same tx. Conservation invariant: `outstanding + committed + released == original`.

Mixed terminal-state rule: when commits + releases together drive `outstanding → 0`, the status reflects the transition that finished it (`COMMITTED` if the last move was a commit; `RELEASED` for release; `EXPIRED` for scheduler-driven). `committed_amount` and `released_amount` remain on the row so callers can audit the breakdown.

### FX — exchange and rate management

`ExecuteExchange` runs a two-currency exchange as a single atomic flow:

1. Read `from_account` and `to_account`, derive currencies.
2. Either use the caller-supplied `rate` or look one up from `fx_rates` (most recent row with `effective_at ≤ now()`).
3. Validate `from_amount × rate == to_amount`.
4. Build a 4-entry journal — debit `from_counter` / credit `from_account` on the source side; debit `to_account` / credit `to_counter` on the target side. Each currency self-balances; the existing per-currency validator passes unchanged.
5. Run the inner ExecuteFlow against the same tx — atomicity, locking, idempotency, and outbox are inherited.

The `fx_rates` table is admin data with `UNIQUE (tenant, base, quote, effective_at, source)`. `PutFXRate` is upsert-on-conflict so re-posting the same rate is a no-op.

**P&L pattern (documented, no enforcement)**: callers needing to record gain/loss — exchange-with-residual, end-of-day mark-to-market — use raw `ExecuteFlow` with an `fx_pnl:<currency>` account that absorbs the per-currency imbalance. The existing validator accepts any N-entry journal as long as each currency nets to zero. See `examples/go/fx_revaluation/` for both shapes.

### Reconciliation — match, surface, resolve

`IngestExternalRecords` writes external-source transaction records (idempotent on `(tenant, source, external_ref)`). `RunReconciliation` opens one flow tx and:

1. Loads external records and ledger journals for `(tenant, source, window)` via the transactional `Tx` interface (avoids deadlocking against the open SQLite tx).
2. Indexes journals by `event_id`.
3. For each external record: if a journal with `event_id == external_ref` exists, optionally verifies the amount against `sum(debit-credit) on entries(journal) WHERE account_id = external.account_id` (when an anchor account was supplied). Match → `MATCHED`. Amount diverges → `MISMATCHED` + `AMOUNT_MISMATCH` discrepancy. No journal → `MISSING_IN_LEDGER` discrepancy.
4. Journals not seen in step 3 become `MISSING_IN_EXTERNAL` discrepancies.
5. Writes the batch summary, all discrepancy rows, and outbox events in the same tx → commit.

`ResolveDiscrepancy` takes a discrepancy from `OPEN` to `RESOLVED` or `IGNORED`. When `RESOLVED` with an embedded `ExecuteFlowRequest`, the adjustment flow runs in the same tx via `executeFlowInTx`, and its `journal_id` is linked into `resolution_journal_id`.

All money movement still flows through `ExecuteFlow` — recon never writes ledger entries directly.

### Balance snapshots

`GetBalance(account, currency, as_of=T)` reconstructs the balance as of time `T`:

1. Find the latest snapshot with `snapshot_at ≤ T`. If none, start from zero.
2. Sum entries posted in `(snapshot_at, T]`.
3. Add the sums to the snapshot's `posted_debits` / `posted_credits`.
4. Normalize via the account's `normal_balance`.

`TakeBalanceSnapshot` either snapshots a single `(account, currency)` row or every balance row for a tenant. The scheduler's snapshot tick is a stub today — operators trigger snapshots manually or via cron-like jobs.

## Concurrency

### CockroachDB

- `BeginTx(IsoLevel: pgx.Serializable)`.
- `SELECT … FOR UPDATE` on `account_balances` rows touched by the flow, taken in deterministic order to avoid deadlocks.
- `SELECT … FOR UPDATE` on `flow_runs` for idempotency lookups.
- `SELECT … FOR UPDATE SKIP LOCKED` on `reservations` during scheduler scans (safe for multiple scheduler replicas in the future).
- SQLSTATE `40001` retry helper: capped exponential backoff with jitter, up to 5 attempts; exhausts to `SERIALIZATION_RETRY_EXHAUSTED`.

### SQLite

- `BEGIN IMMEDIATE` for every write tx — claims the DB write lock immediately, fails fast on contention.
- `_journal_mode=WAL`, `_busy_timeout=5000`, `_foreign_keys=on`.
- `db.SetMaxOpenConns(1)` so the connection pool can't multiplex writes. Sufficient for local dev.

## Idempotency

Three levels:

1. **Flow level**: `flow_runs.idempotency_key` UNIQUE. Replays of `ExecuteFlow` / `PostJournal` return the same `flow_run_id` and step list without inserting new rows.
2. **Journal level**: `ledger_journals.event_id` UNIQUE. Duplicate event IDs inside a flow reject with `DUPLICATE_IDEMPOTENCY_KEY`.
3. **Reservation level**: `reservations.idempotency_key` UNIQUE; per-transition flow keys are derived as `<reservation_id>:commit:<key>` / `:release:<key>` / `:expire`.
4. **Outbox level**: `outbox_events.idempotency_key` UNIQUE. Consumers must dedupe — delivery is at-least-once.

## Outbox

The transactional outbox is the only way events leave the ledger. Inside the same tx as every state change, one row per logical event:

| Event type | Emitted by | Notes |
|---|---|---|
| `<flow_type>.<step_id>` | `ExecuteFlow` / `PostJournal` | One per step. |
| `RESERVATION_CREATED` | `CreateReservation` | In addition to the inner flow's step event. |
| `RESERVATION_COMMITTED` | `CommitReservation` | Per partial or final commit. |
| `RESERVATION_RELEASED` | `ReleaseReservation` | Per partial or final release. |
| `RESERVATION_EXPIRED` | scheduler → `ExpireReservation` | At most one per reservation. |

The `Dispatcher` polls `WHERE publish_state='PENDING' ORDER BY created_at LIMIT N`, hands each event to a `Sink.Publish(ctx, Event)`, marks `PUBLISHED` on success, increments `attempts` on failure. Default sink is `LogSink` (structured slog). Real adapters (Dapr, Kafka, NATS, webhooks) implement `Sink`.

## Multi-tenancy

`tenant_id` is on every table and indexed. The `TenantIDInterceptor` extracts `X-Tenant-Id` from request headers and rejects requests without one (`InvalidArgument`). Every store query filters on `tenant_id`. There is no cross-tenant view today.

## Error model

Domain code → Connect code mapping (in `internal/service/errors.go`):

| Domain code | Connect code | When |
|---|---|---|
| `INSUFFICIENT_FUNDS` | `FailedPrecondition` | Non-overdraft account would go negative |
| `INVALID_ACCOUNT_STATUS` | `FailedPrecondition` | Account not `ACTIVE` |
| `RESERVATION_CLOSED` | `FailedPrecondition` | Reservation already terminal |
| `ACCOUNT_NOT_FOUND` | `NotFound` | |
| `RESERVATION_NOT_FOUND` | `NotFound` | |
| `ACCOUNT_CURRENCY_MISMATCH` | `InvalidArgument` | Account currency ≠ requested currency |
| `UNBALANCED_JOURNAL` | `InvalidArgument` | Per-currency debit/credit mismatch |
| `RESERVATION_AMOUNT_EXCEEDS` | `InvalidArgument` | Commit/release amount > outstanding |
| `RESERVATION_CURRENCY_MISMATCH` | `InvalidArgument` | Commit destination currency ≠ reservation currency |
| `DUPLICATE_IDEMPOTENCY_KEY` | `AlreadyExists` | |
| `FLOW_ALREADY_COMPLETED` | `AlreadyExists` | |
| `FLOW_CONFLICT` | `Aborted` | Idempotency key reused while original still RUNNING |
| `SERIALIZATION_RETRY_EXHAUSTED` | `Aborted` | CRDB 40001 retried N times |
| `FX_RATE_NOT_FOUND` | `NotFound` | `GetFXRate` or `ExecuteExchange` can't resolve a rate |
| `FX_AMOUNT_MISMATCH` | `InvalidArgument` | `to_amount` differs from `from_amount × rate` |
| `DISCREPANCY_NOT_FOUND` | `NotFound` | `ResolveDiscrepancy` on unknown id |
| `DISCREPANCY_CLOSED` | `FailedPrecondition` | Resolving an already-closed discrepancy |
| `RECON_BATCH_NOT_FOUND` | `NotFound` | `GetReconciliationBatch` on unknown id |

The Connect response includes a `ledger-error-code` header carrying the domain code, so clients can branch programmatically.

## Observability

- **slog** with JSON handler at startup. The logging interceptor records procedure, tenant, and duration per RPC.
- **OpenTelemetry** `TracerProvider`. When `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) is set, an OTLP/HTTP exporter is wired. `OTEL_EXPORTER_OTLP_INSECURE=true` disables TLS.
- The `service.name` resource attribute is set from the `Setup` arg (`"ledger-service"` in `cmd/server`).

## Tooling

| Tool | Role |
|---|---|
| `buf` | Proto code generation (Go + TypeScript) |
| `sqlc` | Typed query bindings per dialect |
| `goose` | Migrations |
| `pgx/v5` | CRDB driver |
| `modernc.org/sqlite` | Pure-Go SQLite driver |
| `shopspring/decimal` | Arbitrary-precision money math |
| `testcontainers-go` | CRDB integration tests behind `//go:build integration` |
| `protoc-gen-es` / `protoc-gen-connect-es` | TS client codegen for the React example |

## Extension points

- **New backends**: implement `repo.Store` and `repo.Tx`. The orchestrator and handlers are backend-agnostic.
- **New event sinks**: implement `outbox.Sink` and pass it to `outbox.NewDispatcher` in `cmd/server`.
- **New flow types**: callers submit `ExecuteFlowRequest` with arbitrary `flow_type`. Outbox `event_type` is `<flow_type>.<step_id>` — downstream consumers can dispatch on this prefix.
- **Custom domain errors**: add a code to `internal/ledger/errors.go`, map it in `internal/service/errors.go`. The header surface lets clients see new codes without breaking older clients.

## Non-goals

The service deliberately does **not** do:

- Payment-rail integration. The outbox is the integration point.
- Identity / KYC / PII tokenization.
- Synchronous pre-transaction hooks (post-transaction hooks live in the outbox).

See `docs/superpowers/specs/` for the original design documents.
