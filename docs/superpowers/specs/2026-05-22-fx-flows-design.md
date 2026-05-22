# FX Conversion Flows — Design

Date: 2026-05-22
Module: `github.com/caxqueiroz/dledger-go`

## 1. Purpose and scope

Add first-class foreign-exchange (FX) support on top of the existing ledger:

- A new `fx_rates` table the service uses for rate lookup and audit.
- A new `ExecuteExchange` Connect-RPC that performs a two-currency exchange between user and counterparty accounts inside a single atomic `ExecuteFlow`.
- Three rate-management RPCs (`PutFXRate`, `GetFXRate`, `ListFXRates`).
- A documented `fx_pnl` account naming convention for callers that want to record gain/loss; no validator changes — the existing per-currency balance rule already handles N-entry FX journals.

In scope:

- Caller-driven rate management (manual `PutFXRate` from an integration job).
- Two-currency atomic exchange between four named accounts.
- Audit trail: every exchange stores the rate (resolved or supplied) in journal metadata.
- One PR delivering the full feature.

Out of scope:

- Server-side rate fetching from external providers (caller's job).
- Forward / scheduled rates (we only resolve "rate as of T").
- N-currency arbitrage flows.
- Mark-to-market revaluation as a dedicated RPC — for now, callers use raw `ExecuteFlow` with the documented `fx_pnl` pattern.

## 2. Data model

### `fx_rates`

| Column | SQLite | CRDB | Notes |
|---|---|---|---|
| `id` | `TEXT PRIMARY KEY` | `STRING PRIMARY KEY` | UUID, server-generated |
| `tenant_id` | `TEXT NOT NULL` | `STRING NOT NULL` | |
| `base_currency` | `TEXT NOT NULL` | `STRING NOT NULL` | ISO 4217 |
| `quote_currency` | `TEXT NOT NULL` | `STRING NOT NULL` | |
| `rate` | `TEXT NOT NULL` | `DECIMAL(38,18) NOT NULL` | quote per base |
| `source` | `TEXT NOT NULL` | `STRING NOT NULL` | "manual", "ECB", "Bloomberg", … |
| `effective_at` | `TEXT NOT NULL` | `TIMESTAMPTZ NOT NULL` | rate valid from |
| `created_at` | `TEXT DEFAULT (...)` | `TIMESTAMPTZ DEFAULT now()` | wall-clock |
| INDEX | `(tenant_id, base_currency, quote_currency, effective_at DESC)` | same | drives lookup |
| UNIQUE | `(tenant_id, base_currency, quote_currency, effective_at, source)` | same | idempotent `PutFXRate` upserts |

`PutFXRate` is `INSERT … ON CONFLICT DO UPDATE` on the unique tuple so a job that re-posts the same rate is a no-op (the existing row's `rate` is overwritten with the same value).

## 3. Domain types

`internal/ledger/fx.go`:

```go
type FXRate struct {
    ID            string
    TenantID      string
    BaseCurrency  string
    QuoteCurrency string
    Rate          decimal.Decimal
    Source        string
    EffectiveAt   time.Time
    CreatedAt     time.Time
}
```

New error codes in `internal/ledger/errors.go`:

| Code | Maps to Connect | When |
|---|---|---|
| `FX_RATE_NOT_FOUND` | `NotFound` | `GetFXRate` finds no row, or `ExecuteExchange` can't resolve a rate |
| `FX_AMOUNT_MISMATCH` | `InvalidArgument` | `to_amount` differs from `from_amount × rate` by more than the tolerance |

## 4. Repository extensions

Add to `Store`:

```go
InsertFXRate(ctx, ledger.FXRate) error
GetFXRateAt(ctx, tenantID, base, quote string, at time.Time) (*ledger.FXRate, error)
ListFXRates(ctx, ListFXRatesInput) ([]ledger.FXRate, error)
```

`ListFXRatesInput`:

```go
type ListFXRatesInput struct {
    TenantID      string
    BaseCurrency  string  // optional
    QuoteCurrency string  // optional
    Since         *time.Time
    Until         *time.Time
    Limit         int
}
```

Returned rows are sorted by `effective_at DESC, id DESC`. Limit caps at 500.

`GetFXRateAt` returns `nil, nil` (not `ErrNotFound`) when no row matches — service layer translates absence to `FX_RATE_NOT_FOUND` (consistent with the snapshot lookup pattern).

## 5. RPC surface

```proto
service LedgerService {
  // ... existing 12 ...
  rpc ExecuteExchange(ExecuteExchangeRequest) returns (ExecuteExchangeResponse);
  rpc PutFXRate(PutFXRateRequest) returns (PutFXRateResponse);
  rpc GetFXRate(GetFXRateRequest) returns (GetFXRateResponse);
  rpc ListFXRates(ListFXRatesRequest) returns (ListFXRatesResponse);
}

message FXRate {
  string id              = 1;
  string tenant_id       = 2;
  string base_currency   = 3;
  string quote_currency  = 4;
  string rate            = 5;
  string source          = 6;
  google.protobuf.Timestamp effective_at = 7;
  google.protobuf.Timestamp created_at   = 8;
}

message ExecuteExchangeRequest {
  string tenant_id                  = 1 [(buf.validate.field).string.min_len = 1];
  string idempotency_key            = 2 [(buf.validate.field).string.min_len = 1];
  string from_account_id            = 3 [(buf.validate.field).string.min_len = 1];
  string to_account_id              = 4 [(buf.validate.field).string.min_len = 1];
  string from_counter_account_id    = 5 [(buf.validate.field).string.min_len = 1];
  string to_counter_account_id      = 6 [(buf.validate.field).string.min_len = 1];
  string from_amount                = 7 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
  string to_amount                  = 8 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
  string rate                       = 9;   // optional; resolved from fx_rates if empty
  string rate_source                = 10;  // optional; carried in journal metadata
  string source_service             = 11;
  string actor_id                   = 12;
  google.protobuf.Struct metadata   = 13;
}
message ExecuteExchangeResponse {
  string flow_run_id = 1;
  string journal_id  = 2;
  string rate_used   = 3;        // the rate the server applied (resolved or supplied)
  string rate_source = 4;
}

message PutFXRateRequest {
  string tenant_id       = 1 [(buf.validate.field).string.min_len = 1];
  string base_currency   = 2 [(buf.validate.field).string.min_len = 3];
  string quote_currency  = 3 [(buf.validate.field).string.min_len = 3];
  string rate            = 4 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
  string source          = 5 [(buf.validate.field).string.min_len = 1];
  google.protobuf.Timestamp effective_at = 6;  // server uses now() if unset
}
message PutFXRateResponse { FXRate rate = 1; }

message GetFXRateRequest {
  string tenant_id      = 1;
  string base_currency  = 2;
  string quote_currency = 3;
  google.protobuf.Timestamp at = 4;  // server uses now() if unset
}
message GetFXRateResponse { FXRate rate = 1; }

message ListFXRatesRequest {
  string tenant_id      = 1;
  string base_currency  = 2;
  string quote_currency = 3;
  google.protobuf.Timestamp since = 4;
  google.protobuf.Timestamp until = 5;
  int32  page_size      = 6;
}
message ListFXRatesResponse { repeated FXRate rates = 1; }
```

## 6. `ExecuteExchange` semantics

1. Open a flow tx (reuses `BeginFlowTx`).
2. Idempotency lookup on `idempotency_key` via `flow_runs` (no new dedup layer; the inner `ExecuteFlow` handles it).
3. Fetch `from_account` and `to_account`. Derive `from_currency`, `to_currency`. Reject if either equals the other (`CodeAccountCurrencyMismatch` with "from and to must differ").
4. Resolve rate:
   - If `rate` is supplied non-empty, parse it; store `(rate, rate_source)` for audit.
   - Else `GetFXRateAt(tenant, from_currency, to_currency, now())`. Use that row's `rate` and `source`. Error `CodeFXRateNotFound` if none.
5. Validate `from_amount × rate == to_amount` (exact equality; tolerance is 0 for v1). On mismatch return `CodeFXAmountMismatch`.
6. Build the inner `ExecuteFlowRequest`:
   - flow_type: `"EXCHANGE"`
   - idempotency_key: same as the request (inner flow's tx serializes on the unique constraint).
   - one step `"exchange"` with the 4-entry journal described in §6.2 below.
   - metadata carries `{"rate": "...", "rate_source": "...", "from_currency": "...", "to_currency": "..."}`.
7. Call `executeFlowInTx` against the open tx.
8. Commit. Return `flow_run_id`, the resolved `journal_id`, the `rate_used`, and `rate_source`.

### 6.1 Counterparty accounts

The caller supplies four account IDs:

- `from_account_id`: user side, source currency. Debited from user → outflow.
- `from_counter_account_id`: platform side, source currency. Credit-side inventory increases.
- `to_account_id`: user side, target currency. Credited to user → inflow.
- `to_counter_account_id`: platform side, target currency. Debit-side inventory decreases.

The "platform" accounts can be any account that holds the relevant currency — typically `platform:fx_desk:cash:USD`, but the engine doesn't enforce naming.

### 6.2 Generated journal

For `from_amount=100 USD`, `to_amount=89.50 EUR`, `rate=0.895`:

| Direction | Account | Currency | Amount |
|---|---|---|---|
| DEBIT  | `from_counter_account` | USD | 100.00 |
| CREDIT | `from_account`         | USD | 100.00 |
| DEBIT  | `to_account`           | EUR |  89.50 |
| CREDIT | `to_counter_account`   | EUR |  89.50 |

USD self-balances at 0. EUR self-balances at 0. Existing per-currency validator passes.

## 7. P&L pattern (documented, not implemented as RPC)

Callers that want to absorb residual or record revaluation use `ExecuteFlow` directly. Two example shapes:

**Exchange with residual** — user pays 100 USD, gets 89.50 EUR, but the platform "books" the FX at 89.00 (rate 0.89), creating a 0.50 EUR loss:

```
USD: balanced
  DEBIT  platform:fx_desk:cash:USD       100.00 USD
  CREDIT user:1:cash:USD                 100.00 USD

EUR: balanced with P&L absorbing the 0.50 gap
  DEBIT  user:1:cash:EUR                  89.50 EUR
  CREDIT platform:fx_desk:cash:EUR        89.00 EUR
  CREDIT platform:fx_desk:fx_pnl:EUR       0.50 EUR
```

**End-of-day revaluation** — platform marks its open EUR position at a new closing rate:

```
EUR:
  DEBIT  platform:fx_desk:fx_pnl:EUR      12.34 EUR
  CREDIT platform:fx_desk:cash:EUR        12.34 EUR
```

Both are regular `ExecuteFlow` calls — no new validator path. We add an `examples/go/fx_revaluation/` walkthrough demonstrating both shapes.

When real callers need the residual path served by an RPC, we add `ExecuteExchangeWithResidual` as a follow-up. v1 only ships the simple 4-entry form.

## 8. Concurrency

`ExecuteExchange` reuses `executeFlowInTx`, so:

- CRDB: SERIALIZABLE with row-level FOR UPDATE on the four `account_balances` rows (deterministic ordering by account_id then currency).
- SQLite: BEGIN IMMEDIATE serializes writes.
- 40001 retry inherited from `repo.WithRetry`.

`fx_rates` writes are independent — no balance locking needed. Multiple concurrent `PutFXRate` for the same `(tenant, base, quote, effective_at, source)` race on the unique constraint; the `ON CONFLICT DO UPDATE` collapses them safely.

## 9. Outbox events

Per the existing convention, the inner step `exchange` produces:

- `event_type: "EXCHANGE.exchange"`
- `idempotency_key: "<flow_run_id>:exchange"`
- payload: `{"flow_type":"EXCHANGE","step_id":"exchange","journal_id":"...","rate":"...","rate_source":"..."}`

No separate outbox events for rate writes — they're admin data, not balance-affecting.

## 10. Testing

### Domain

- `FXRate` round-trips through repo write/read.

### Service (SQLite-backed)

- `PutFXRate` → `GetFXRate(at=now)` returns the row.
- `PutFXRate` two rates with different `effective_at` → `GetFXRate(at=between)` returns the earlier one; `at=after` returns the later one.
- `GetFXRate` for unknown `(base, quote)` returns `CodeNotFound`.
- `ListFXRates` with `(base, quote)` filter returns rows sorted DESC.
- `ExecuteExchange` happy path:
  - 4 accounts created (user USD, user EUR, platform USD, platform EUR), 2 funded.
  - `ExecuteExchange(from_amount=100 USD, to_amount=89.50 EUR, rate=0.895)`.
  - Verify all four balances moved as expected; rate_used in response equals 0.895.
- `ExecuteExchange` with `rate=""` resolves from `fx_rates` and uses it.
- `ExecuteExchange` with `from_amount × rate ≠ to_amount` returns `InvalidArgument`.
- `ExecuteExchange` with no rate available returns `NotFound`.
- `ExecuteExchange` idempotent replay returns the same flow_run_id.

## 11. Migration plan

One PR, one phase. New migration `0004_fx_rates.sql` on both backends. Four new RPCs (`ExecuteExchange`, `PutFXRate`, `GetFXRate`, `ListFXRates`). Tests + an `examples/go/fx_exchange/` walkthrough demonstrating the standard exchange and an `examples/go/fx_revaluation/` walkthrough demonstrating the P&L pattern.

## 12. Acceptance criteria

- `PutFXRate` upserts; `GetFXRate(at=T)` returns the most-recent rate with `effective_at ≤ T`.
- `ExecuteExchange` produces a 4-entry journal that balances per currency and atomically updates all four account balances.
- Caller-supplied rates and server-resolved rates both work; mismatches reject with `InvalidArgument`.
- Replays of `ExecuteExchange` return the same `flow_run_id`.
- All money movement still goes through `ExecuteFlow`'s orchestrator (via `executeFlowInTx`) — no new direct repo writes to `account_balances`.

## 13. Out of scope

- Server-side rate fetching / scheduled refresh.
- Forward / future-dated rates beyond `effective_at`.
- N-currency arbitrage chains.
- Per-tenant FX rate visibility ACLs (tenant isolation only).
- `ExecuteExchangeWithResidual` — deferred until a real consumer needs it.
- Snapshot retention or reconciliation (their own design docs).
