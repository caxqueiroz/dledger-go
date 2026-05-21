# Multi-Currency Ledger and Atomic Flow Service — Design

Date: 2026-05-21
Module: `github.com/caxqueiroz/doubleledger`

## 1. Purpose and scope

A product-neutral ledger service that owns double-entry accounting, multi-currency balances, reservations, idempotency, and atomic flow execution. Product engines submit accounting intents; they do not mutate balances directly.

In scope (MVP, single delivery):

- All seven Connect-RPC methods: `CreateAccount`, `GetAccount`, `GetBalance`, `PostJournal`, `ExecuteFlow`, `GetFlow`, `ListAccountActivity`.
- Two storage backends: CockroachDB (production) and SQLite via `modernc.org/sqlite` (local).
- Transactional outbox with in-process polling dispatcher and a pluggable `Sink` interface (default: slog).
- OTel tracing + metrics + structured logging.
- Example Go client and React example skeleton.

Out of scope (deferred):

- FX conversion flows.
- Manual adjustment approval workflow.
- Statement reconciliation.
- Payment-rail adapters.
- Hierarchical account structures.
- Real pub/sub adapters (Dapr, Kafka). The `Sink` interface accommodates them without changing the core.

## 2. Architecture

Three internal layers plus orthogonal infrastructure:

```
proto/ledger/v1            -> RPC contracts
gen/{proto,sqlite,crdb}    -> generated code (do not edit)

internal/ledger            -> domain types, rules, errors (pure Go)
internal/repo              -> Repository interface + tx abstraction
internal/repo/sqlite       -> SQLite implementation (sqlc-generated queries)
internal/repo/crdb         -> CockroachDB implementation (sqlc-generated queries)
internal/service           -> Connect-RPC handlers (the LedgerService)
internal/outbox            -> background dispatcher + Sink interface
internal/observability     -> OTel + slog wiring

cmd/server                 -> server entry point
cmd/migrate                -> goose-based migration runner

sql/migrations/{sqlite,crdb}
sql/queries/{sqlite,crdb}
examples/{go,react}
docs/superpowers/specs
```

### 2.1 Layer responsibilities

**`internal/ledger`** — pure Go. Defines `Account`, `Journal`, `Entry`, `Flow`, `Balance`; the per-currency journal-balance check; normal-balance arithmetic; domain error codes. No DB, no proto. Fully unit-testable.

**`internal/repo`** — `Repository` and `Tx` interfaces. Verbs:

- `BeginFlowTx(ctx) (Tx, error)`
- On `Tx`:
  - `LookupFlowByIdempotency(key)` → existing run or not-found
  - `InsertFlowRun`, `UpdateFlowRunStatus`
  - `GetAccount`, `LoadBalancesForUpdate(accountIDs, currency)` (CRDB: `SELECT ... FOR UPDATE`; SQLite: relies on `BEGIN IMMEDIATE`)
  - `InsertJournal`, `InsertEntries`
  - `ApplyBalanceDelta(accountID, currency, debit, credit)` (idempotent per-call, with version increment)
  - `InsertFlowStep`
  - `InsertOutboxEvent`
  - `Commit`, `Rollback`
- Read-only verbs outside flows: `ListAccountActivity`, `GetFlow`, `GetBalance`, `GetAccount`.

**`internal/service`** — Connect-RPC handler. Implements the orchestration in section 5. Backend-agnostic; takes a `Repository` plus a `clock`/`uuid` source for testability.

**`internal/outbox`** — `Dispatcher` polls `outbox_events WHERE publish_state='PENDING'`, hands each to a `Sink.Publish(ctx, Event) error`. On success, marks `PUBLISHED` with `published_at`. Crash-safe via at-least-once delivery + idempotency keys on the event itself.

### 2.2 Cross-cutting

- Multi-tenancy: every table has `tenant_id`; every query filters by it. A Connect `TenantIDInterceptor` extracts `X-Tenant-Id` from headers and injects into the request context.
- protovalidate `validate.NewInterceptor()` runs after the tenant interceptor.
- OTel: tracer + meter created at startup; one root span per RPC; spans for each significant operation (`db.tx`, `db.lock`, `journal.insert`, `outbox.publish`).

## 3. Data model

The schema follows the spec verbatim for CockroachDB. The SQLite variant adapts types:

| Column kind | CRDB | SQLite |
|---|---|---|
| Identifier | `STRING` | `TEXT` |
| Money | `DECIMAL(38,18)` | `TEXT` (parsed via shopspring/decimal) |
| Timestamp | `TIMESTAMPTZ` | `TEXT` (RFC3339Nano UTC) |
| JSON | `JSONB` | `TEXT` (JSON-encoded) |
| UUID default | `gen_random_uuid()` | application-generated UUIDv7 |
| Booleans | `BOOL` | `INTEGER` (0/1) |

Indexes mirror the spec: `ledger_entries (tenant_id, account_id, currency, created_at)`, unique `(tenant_id, owner_type, owner_id, account_type, currency)` on accounts, unique `event_id` on journals, unique `idempotency_key` on flow runs and on outbox events, unique `(tenant_id, flow_run_id, step_id)` on flow steps.

Additional design choices:

- Account `id` is caller-supplied (e.g. `"user:123:cash_available:USD"`) and used directly as PK. The service does not parse the string.
- Money columns store positive amounts only; sign is determined by `direction`.
- `account_balances` rows are upserted lazily on first entry into the account.

## 4. Money and decimals

- All money values flow as `shopspring/decimal.Decimal` in Go. Wire format is the decimal-string proto field (`"100.00"`).
- Parsing rejects negative amounts at the proto-validation layer (`amount` regex / `gt: 0` constraint).
- All arithmetic is performed in decimal; no float anywhere on the money path.
- sqlc type overrides map the DB column to `decimal.Decimal` for CRDB and to a custom `Decimal` wrapper for SQLite (text-encoded).

## 5. Flow execution algorithm

`ExecuteFlow` and `PostJournal` share the same core. The handler:

```
1.  begin Tx (CRDB: SERIALIZABLE)
2.  lookup flow_runs by idempotency_key (FOR UPDATE on CRDB)
    - if found and status=COMPLETED: load and return existing response
    - if found and status=RUNNING/FAILED: return FLOW_CONFLICT
3.  insert flow_run(status=RUNNING)
4.  for each step:
    a. validate journal balances by currency (sum debits == sum credits per currency)
    b. resolve accounts; verify existence, tenant scope, status=ACTIVE, currency match
5.  collect all (account_id, currency) pairs; LoadBalancesForUpdate
    - CRDB: SELECT ... FOR UPDATE
    - SQLite: rows are already write-locked via BEGIN IMMEDIATE
6.  apply per-account balance deltas in memory; verify each non-overdraft account
    still has non-negative normalized balance; otherwise abort with INSUFFICIENT_FUNDS
7.  insert journals, insert entries, upsert balances (with version++)
8.  insert flow_steps (COMPLETED)
9.  for each step: insert one outbox_event (event_type derived from flow_type/step_id;
    idempotency_key = "<flow_run_id>:<step_id>")
10. update flow_run(status=COMPLETED, completed_at=now)
11. commit
```

If any step fails before commit, the tx rolls back; on retryable CRDB errors (SQLSTATE 40001), retry with capped exponential backoff up to N attempts. After N: `SERIALIZATION_RETRY_EXHAUSTED`.

`PostJournal` is `ExecuteFlow` with a single synthetic step.

## 6. Idempotency

Two layers:

- **Flow level**: `flow_runs.idempotency_key` UNIQUE. Replays return the original response by re-loading the run, its steps, and the linked journals/entries — no new rows.
- **Journal level**: `ledger_journals.event_id` UNIQUE. Within a flow, a duplicate `event_id` is rejected with `DUPLICATE_IDEMPOTENCY_KEY`.

The "completed-replay" path must be deterministic: the response is reconstructed from persisted state, not from re-running the steps.

## 7. Concurrency model

### 7.1 CockroachDB

- `SERIALIZABLE` isolation (default).
- `SELECT ... FOR UPDATE` on `account_balances` rows that the flow touches, in deterministic ID order to avoid deadlocks.
- Retry on `40001` with exponential backoff (e.g. 5ms, 10ms, 20ms, 40ms, 80ms, cap at 5 retries). After exhaustion: `SERIALIZATION_RETRY_EXHAUSTED` returned as `Aborted`.
- A test forces a 40001 by running two concurrent reservations against the same account.

### 7.2 SQLite

- `BEGIN IMMEDIATE` for any write tx; readers use deferred. `_journal_mode=WAL`, `_synchronous=NORMAL`, `_busy_timeout=5000`.
- Single-writer semantics are sufficient for dev. Concurrent writers serialize naturally; no explicit row locks.
- The same concurrency test is exercised against SQLite to confirm correctness under contention.

## 8. Outbox

- Outbox events are written in the same tx as the journals/balances. There is no second write path that bypasses the outbox.
- The `Dispatcher` runs as a goroutine started by `cmd/server`:
  - `tick` interval (default 250ms) or push-trigger on commit.
  - Batch size (default 100).
  - Per-event call `Sink.Publish`; on success update `publish_state='PUBLISHED', published_at=now`.
  - On failure: leave `PENDING`, increment an attempts counter (added column `attempts INT DEFAULT 0`), apply backoff.
- `Sink` interface:
  ```go
  type Sink interface {
      Publish(ctx context.Context, e Event) error
  }
  ```
- Default sink is `LogSink` (structured slog). The interface is suitable for Dapr/Kafka adapters but those are deferred.
- Subscribers downstream must be idempotent on the event's `idempotency_key` (we deliver at-least-once).

## 9. RPC surface

All seven RPCs are defined in `proto/ledger/v1/ledger.proto`. Error mapping:

| Domain code | Connect code |
|---|---|
| `INSUFFICIENT_FUNDS` | `FailedPrecondition` |
| `ACCOUNT_NOT_FOUND` | `NotFound` |
| `ACCOUNT_CURRENCY_MISMATCH` | `InvalidArgument` |
| `UNBALANCED_JOURNAL` | `InvalidArgument` |
| `DUPLICATE_IDEMPOTENCY_KEY` | `AlreadyExists` |
| `FLOW_ALREADY_COMPLETED` | `AlreadyExists` (with stored payload) |
| `FLOW_CONFLICT` | `Aborted` |
| `INVALID_ACCOUNT_STATUS` | `FailedPrecondition` |
| `SERIALIZATION_RETRY_EXHAUSTED` | `Aborted` |

Domain error codes are surfaced as an `error_code` string in error details (`google.rpc.ErrorInfo` or a custom Connect detail message) so callers can branch programmatically.

Interceptor order (matches the org's Connect convention):

1. `TenantIDInterceptor` — extracts `X-Tenant-Id`.
2. `protovalidate` interceptor.
3. OTel interceptor (tracing).
4. Logging interceptor.

## 10. Migrations and tooling

- **Migrations**: `goose` per backend, separate dirs. `cmd/migrate up|down|status|reset --backend=crdb|sqlite`.
- **Proto codegen**: `buf` with `protoc-gen-go`, `protoc-gen-connect-go`. Validation via `buf.build/go/protovalidate` (runtime CEL-based validator with `(buf.validate.field)` options in the proto). Generated code under `gen/proto/ledger/v1`.
- **SQL codegen**: `sqlc` with two packages targeting the two dialect dirs. `gen/sqlite` and `gen/crdb`.
- **Mocks**: `mockery` for the `Repository` interface so the service layer can be unit-tested without a DB.
- **Makefile**: `tools`, `proto`, `sqlc`, `generate` (proto + sqlc + mocks), `migrate-up`, `migrate-down`, `serve`, `test`, `test-integration`, `lint`.

## 11. Testing strategy

### 11.1 Layers

- **Domain (`internal/ledger`)**: pure unit tests. Includes `TestJournal_BalancesPerCurrency` with table-driven mixed-currency cases.
- **Service (`internal/service`)**: against both backends through a shared test harness. The harness takes a `Repository` factory and runs the same suite against each backend.
- **Repo**: each backend gets a thin smoke suite covering all `Repository` verbs plus error mapping.
- **Outbox**: dispatcher tested with an in-memory `Sink` (records publishes, can be set to fail) against SQLite.

### 11.2 Required scenarios (per spec)

- Per-currency journal balancing — multiple currencies in one journal, each must balance independently.
- Idempotent replay — same key returns identical response, no new rows.
- Concurrent reservation — two flows reserving the same account; exactly one succeeds with sufficient funds, the other gets `INSUFFICIENT_FUNDS`.
- Insufficient balance.
- Multi-step rollback — second step fails; first step's writes must not persist.
- Outbox-after-commit — failing the tx must leave no outbox rows.
- Multi-currency invalid balancing — `UNBALANCED_JOURNAL`.
- CRDB serializable retry — forced 40001 with a barrier; verifies retry path and final outcome.
- SQLite persistence — close DB, reopen, balances match.

### 11.3 Infrastructure

- `testcontainers-go` starts a CRDB single-node container per integration package; behind `//go:build integration`. CI runs both `go test ./...` (unit) and `go test -tags integration ./...`.
- SQLite tests use `modernc.org/sqlite` against a temp file or `file::memory:?cache=shared`.

## 12. Observability

- OTel SDK initialized in `cmd/server` with exporter selected by env (`OTEL_EXPORTER_OTLP_ENDPOINT` etc.). Defaults to no-op for tests.
- Spans: `ledger.execute_flow`, `ledger.post_journal`, `repo.lock_balances`, `repo.insert_journal`, `outbox.publish`.
- Required span attributes per the org convention: `tenant_id`, `actor_id`, `flow_run_id`, `flow_type`.
- Metrics:
  - Counter `ledger_flows_total{flow_type,status}`
  - Histogram `ledger_flow_duration_seconds{flow_type}`
  - Counter `ledger_outbox_events_total{event_type,result}`
  - Gauge `ledger_outbox_pending`
- Logging: `slog` with JSON handler, `trace_id`, `tenant_id`, `actor_id` injected from context.

## 13. Examples

- `examples/go/client/main.go` — creates accounts, posts a journal, executes a `PLACE_ORDER` flow, reads balance.
- `examples/react/` — minimal hook that calls `GetBalance` via `@connectrpc/connect-web`. README only; not a real build target.

## 14. Repository layout (final)

```
.
├── Makefile
├── buf.yaml
├── buf.gen.yaml
├── sqlc.yaml
├── go.mod
├── cmd/
│   ├── server/main.go
│   └── migrate/main.go
├── proto/ledger/v1/ledger.proto
├── gen/
│   ├── proto/ledger/v1/
│   ├── sqlite/
│   └── crdb/
├── sql/
│   ├── migrations/{sqlite,crdb}/
│   └── queries/{sqlite,crdb}/
├── internal/
│   ├── ledger/        # domain
│   ├── repo/          # Repository interface + tx abstraction
│   ├── repo/sqlite/
│   ├── repo/crdb/
│   ├── service/       # Connect handlers
│   ├── outbox/
│   └── observability/
├── examples/
│   ├── go/client/
│   └── react/
└── docs/
    └── superpowers/specs/
```

## 15. Acceptance criteria (from spec)

- Product engines cannot directly mutate balances — enforced by RPC-only surface.
- Every posted journal is balanced by currency — validated before insert.
- Every flow has an idempotency key — required field, unique constraint.
- Replaying a completed flow returns the original result — covered by tests.
- Concurrent reservations cannot overspend — CRDB and SQLite concurrency tests.
- Ledger entries, balances, flow status, and outbox events commit atomically — single tx.
- SQLite local development uses `modernc.org/sqlite` — repo `internal/repo/sqlite`.
- CockroachDB production runs with serializable transaction retry handling — retry loop on 40001.

## 16. Open questions / risks

- **sqlc on SQLite TEXT decimals**: sqlc's type overrides for SQLite may need a custom scanner/valuer wrapping `decimal.Decimal`. Risk is low; alternative is to declare the column as `NUMERIC` and convert at the repo boundary.
- **Outbox event payload schema**: events are envelope-shaped (`event_type`, `aggregate_id`, `idempotency_key`, `payload`). Payload schemas are flow-specific and not pinned in this MVP — they'll firm up when a real subscriber lands.
- **No FX**: a multi-currency journal must self-balance per currency. The MVP rejects journals that contain entries in currency A without matching debits/credits in A. FX flows are deferred.
