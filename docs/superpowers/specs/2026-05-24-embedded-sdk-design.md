# Embedded SDK — Design

Date: 2026-05-24
Module: `github.com/caxqueiroz/dledger-go`

## 1. Purpose and scope

Expose dledger-go as a Go SDK so the **tipmarket PAM** (player account management) microservice can either embed the ledger in-process or talk to a hosted dledger-go server over Connect-RPC — using the same Go interface. Swapping modes requires no application-level code changes in PAM.

The SDK ships with a higher-level `Wallet` helper that wraps the common prediction-market patterns (Deposit, Reserve, Commit, Release, Settle, Withdraw, GetWallet) into single-call idiomatic Go methods. PAM uses `Wallet` for the routine 90% and drops to the underlying `Client` interface for anything custom.

In scope:

- `pkg/dledger/` public package.
- `Client` interface mirroring all 21 RPCs (Connect request/response types).
- `NewEmbedded(ctx, Options) (Client, error)` — runs the ledger in-process, owns the DB, starts the scheduler and outbox dispatcher.
- `NewRemote(serverURL, tenant, ...Option) Client` — wraps a Connect HTTP client with an `X-Tenant-Id` transport interceptor.
- `Wallet` helper with per-call funding-account specification.
- `ErrCode` sealed type + `IsErrCode(err, code) bool` that works for both modes.
- Auto migrations on `Open()` (opt-out via `MigrateMode=Skip`).
- Tests for both modes; PAM-style example walkthroughs.

Out of scope:

- A separate-repo SDK (one repo, single release cadence).
- Non-Go SDKs (TypeScript proto bindings already exist via `buf-es`).
- A `Tail()` API for direct outbox polling — the dispatcher + Sink interface is the supported integration path.
- Per-tenant key rotation, ACLs, audit trails beyond what the server already records.
- A separate persistent DB for the SDK in embedded mode — PAM passes the DSN.

## 2. Package layout

```
pkg/dledger/                                 NEW (public)
  client.go             # Client interface
  options.go            # Options, functional helpers (WithLogger, WithHTTPClient, …)
  embedded.go           # NewEmbedded constructor + lifecycle
  remote.go             # NewRemote constructor + tenant interceptor
  errors.go             # ErrCode + IsErrCode
  wallet.go             # Wallet methods (Deposit, Reserve, Commit, Release, Settle, Withdraw, GetWallet)
  wallet_types.go       # input/output structs
  doc.go                # package overview
internal/sdk/                                NEW (tests)
  embedded_test.go      # embedded constructor + lifecycle
  wallet_test.go        # wallet methods
examples/go/sdk_embedded/main.go             NEW
examples/go/sdk_remote/main.go               NEW
docs/ARCHITECTURE.md                         MODIFY (SDK section)
```

`pkg/dledger` is the only public surface (PAM imports it). Tests live under `internal/sdk` so they have access to test helpers without exporting them.

## 3. `Client` interface

```go
package dledger

import (
    "context"

    "connectrpc.com/connect"

    v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// Client is the mode-agnostic surface. Both NewEmbedded and NewRemote return
// implementations of this interface. PAM swaps modes by changing only the
// constructor call.
type Client interface {
    // Accounts
    CreateAccount(context.Context, *connect.Request[v1.CreateAccountRequest]) (*connect.Response[v1.CreateAccountResponse], error)
    GetAccount(context.Context, *connect.Request[v1.GetAccountRequest])       (*connect.Response[v1.GetAccountResponse], error)
    GetBalance(context.Context, *connect.Request[v1.GetBalanceRequest])       (*connect.Response[v1.GetBalanceResponse], error)

    // Journals + flows
    PostJournal(context.Context, *connect.Request[v1.PostJournalRequest]) (*connect.Response[v1.PostJournalResponse], error)
    ExecuteFlow(context.Context, *connect.Request[v1.ExecuteFlowRequest]) (*connect.Response[v1.ExecuteFlowResponse], error)
    GetFlow(context.Context, *connect.Request[v1.GetFlowRequest])         (*connect.Response[v1.GetFlowResponse], error)
    ListAccountActivity(context.Context, *connect.Request[v1.ListAccountActivityRequest]) (*connect.Response[v1.ListAccountActivityResponse], error)

    // Reservations
    CreateReservation(context.Context, *connect.Request[v1.CreateReservationRequest])   (*connect.Response[v1.CreateReservationResponse], error)
    CommitReservation(context.Context, *connect.Request[v1.CommitReservationRequest])   (*connect.Response[v1.CommitReservationResponse], error)
    ReleaseReservation(context.Context, *connect.Request[v1.ReleaseReservationRequest]) (*connect.Response[v1.ReleaseReservationResponse], error)
    GetReservation(context.Context, *connect.Request[v1.GetReservationRequest])         (*connect.Response[v1.GetReservationResponse], error)
    ListReservations(context.Context, *connect.Request[v1.ListReservationsRequest])     (*connect.Response[v1.ListReservationsResponse], error)

    // Snapshots
    TakeBalanceSnapshot(context.Context, *connect.Request[v1.TakeBalanceSnapshotRequest]) (*connect.Response[v1.TakeBalanceSnapshotResponse], error)

    // FX
    ExecuteExchange(context.Context, *connect.Request[v1.ExecuteExchangeRequest]) (*connect.Response[v1.ExecuteExchangeResponse], error)
    PutFXRate(context.Context, *connect.Request[v1.PutFXRateRequest])             (*connect.Response[v1.PutFXRateResponse], error)
    GetFXRate(context.Context, *connect.Request[v1.GetFXRateRequest])             (*connect.Response[v1.GetFXRateResponse], error)
    ListFXRates(context.Context, *connect.Request[v1.ListFXRatesRequest])         (*connect.Response[v1.ListFXRatesResponse], error)

    // Reconciliation
    IngestExternalRecords(context.Context, *connect.Request[v1.IngestExternalRecordsRequest])     (*connect.Response[v1.IngestExternalRecordsResponse], error)
    RunReconciliation(context.Context, *connect.Request[v1.RunReconciliationRequest])             (*connect.Response[v1.RunReconciliationResponse], error)
    GetReconciliationBatch(context.Context, *connect.Request[v1.GetReconciliationBatchRequest])   (*connect.Response[v1.GetReconciliationBatchResponse], error)
    ListDiscrepancies(context.Context, *connect.Request[v1.ListDiscrepanciesRequest])             (*connect.Response[v1.ListDiscrepanciesResponse], error)
    ResolveDiscrepancy(context.Context, *connect.Request[v1.ResolveDiscrepancyRequest])           (*connect.Response[v1.ResolveDiscrepancyResponse], error)

    // Lifecycle
    Close() error
}
```

This interface is structurally identical to `ledgerv1connect.LedgerServiceHandler` plus `Close()`. Both backends satisfy it trivially: embedded delegates to `*service.Server` (which already implements the handler interface); remote delegates to a Connect HTTP client.

## 4. `Options` and constructors

```go
type Backend string

const (
    SQLite Backend = "sqlite"
    CRDB   Backend = "crdb"
)

type MigrateMode int

const (
    MigrateAuto MigrateMode = iota // default; run goose up on Open()
    MigrateSkip                    // do nothing; ops runs cmd/migrate
)

type Options struct {
    Backend          Backend         // required for NewEmbedded
    DSN              string          // required for NewEmbedded
    MigrateMode      MigrateMode     // default MigrateAuto
    OutboxSink       outbox.Sink     // default outbox.LogSink with the provided logger
    DisableScheduler bool            // default false
    Logger           *slog.Logger    // default slog.Default()
}

type Option func(*remoteOptions)

func WithHTTPClient(c *http.Client) Option
func WithLogger(l *slog.Logger) Option
// (room for WithUserAgent, WithTimeouts, etc. — additive)

func NewEmbedded(ctx context.Context, opts Options) (Client, error)
func NewRemote(serverURL, tenantID string, opts ...Option) Client
```

### `NewEmbedded`

1. Validate `opts.Backend` and `opts.DSN`.
2. Open the appropriate `repo.Store` (SQLite or CRDB) at the DSN.
3. If `opts.MigrateMode == MigrateAuto`, run `goose up` against the open DB.
4. Build `*service.Server` from the store.
5. If `!opts.DisableScheduler`, build and start `*scheduler.Scheduler` (uses defaults from `scheduler.New`).
6. Build `*outbox.Dispatcher` with `opts.OutboxSink` (or `outbox.LogSink{Logger: opts.Logger}`); start it.
7. Track an internal `context.CancelFunc` to drive scheduler + dispatcher shutdown on `Close()`.
8. Return an `embeddedClient` whose 21 methods delegate to `*service.Server`.

### `NewRemote`

1. Build a Connect client over `*http.Client`.
2. Add a transport that sets `X-Tenant-Id: <tenantID>` on every request (matches the server's `interceptors.NewTenant`).
3. Return a `remoteClient` that forwards each method to the Connect client.
4. `Close()` is a no-op.

`tenantID` is captured at construction time. Apps with multiple tenants create one `Client` per tenant. (Embedded mode does not need a captured tenant because requests carry `TenantId` in their bodies; PAM is expected to pass it per call.)

## 5. `Wallet` helper

`Wallet` is the idiomatic Go API. One instance per `(Client, tenantID, playerOwnerType)`. Methods take typed inputs and return typed outputs — no Connect types leak through.

```go
type Wallet struct {
    client    Client
    tenant    string
    ownerType string // default "user"; configurable for non-player wallets
}

type WalletOption func(*Wallet)

func WithOwnerType(t string) WalletOption

func NewWallet(c Client, tenantID string, opts ...WalletOption) *Wallet
```

### `EnsurePlayerAccounts`

```go
type PlayerAccounts struct {
    Available string // account id, e.g. "user:<id>:cash_available:USD"
    Reserved  string // "user:<id>:cash_reserved:USD"
}

func (w *Wallet) EnsurePlayerAccounts(ctx context.Context, playerID, currency string) (PlayerAccounts, error)
```

Idempotently creates the two debit-normal accounts for the player. Returns their IDs. Implementation: two `CreateAccount` calls; AlreadyExists errors are swallowed (treated as "already there").

### `Deposit`

```go
type DepositInput struct {
    PlayerID         string
    Currency         string
    Amount           string             // decimal string
    FundingAccountID string             // caller-supplied per call (typically the payment processor's mirror account in dledger)
    ExternalRef      string             // used as journal event_id (for reconciliation against the processor)
    IdempotencyKey   string
    SourceService    string             // e.g. "stripe"
}

type Receipt struct {
    JournalID  string
    FlowRunID  string
}

func (w *Wallet) Deposit(ctx context.Context, in DepositInput) (Receipt, error)
```

Synthesizes a single-step `PostJournal`:

```
DEBIT  user:<player>:cash_available:<ccy>   amount   (user receives)
CREDIT funding_account                       amount   (funding source decreases)
```

### `Reserve`

```go
type ReserveInput struct {
    PlayerID        string
    Currency        string
    Amount          string
    ExpiresAt       time.Time          // zero = no auto-expiry
    IdempotencyKey  string
    SourceService   string
    Metadata        map[string]any
}

type Reservation struct {
    ID, Status                           string
    OriginalAmount, OutstandingAmount    string
    CommittedAmount, ReleasedAmount      string
    ExpiresAt                            time.Time
}

func (w *Wallet) Reserve(ctx context.Context, in ReserveInput) (Reservation, error)
```

Calls `CreateReservation` with source = `user:<player>:cash_available:<ccy>`, reserved = `user:<player>:cash_reserved:<ccy>`.

### `Commit`

```go
type CommitInput struct {
    ReservationID        string
    DestinationAccountID string             // caller-supplied (e.g. market:<id>:collateral_pool:USD)
    Amount               string
    IdempotencyKey       string
    SourceService        string
}

func (w *Wallet) Commit(ctx context.Context, in CommitInput) (Reservation, error)
```

Direct passthrough to `CommitReservation`. Returns the resulting reservation snapshot.

### `Release`

```go
type ReleaseInput struct {
    ReservationID  string
    Amount         string
    IdempotencyKey string
    SourceService  string
}

func (w *Wallet) Release(ctx context.Context, in ReleaseInput) (Reservation, error)
```

Direct passthrough to `ReleaseReservation`.

### `Settle`

```go
type SettleInput struct {
    PlayerID       string             // recipient (winner)
    Currency       string
    Amount         string             // payout amount
    PoolAccountID  string             // caller-supplied (e.g. market:<id>:collateral_pool:USD)
    ExternalRef    string             // used as journal event_id
    IdempotencyKey string
    SourceService  string             // e.g. "market_resolver"
}

func (w *Wallet) Settle(ctx context.Context, in SettleInput) (Receipt, error)
```

Synthesizes a single-step `PostJournal`:

```
DEBIT  user:<winner>:cash_available:<ccy>   amount   (user receives payout)
CREDIT pool_account                          amount   (pool decreases)
```

(This is the "winner is paid from the market collateral pool" pattern.)

### `Withdraw`

```go
type WithdrawInput struct {
    PlayerID            string
    Currency            string
    Amount              string
    WithdrawalAccountID string         // caller-supplied (e.g. platform:withdrawals:pending:USD)
    ExternalRef         string         // used as journal event_id
    IdempotencyKey      string
    SourceService       string         // e.g. "payouts"
}

func (w *Wallet) Withdraw(ctx context.Context, in WithdrawInput) (Receipt, error)
```

Synthesizes a single-step `PostJournal`:

```
DEBIT  withdrawal_account                    amount   (withdrawal pile increases)
CREDIT user:<player>:cash_available:<ccy>   amount   (user balance decreases)
```

### `GetWallet`

```go
type WalletSnapshot struct {
    PlayerID         string
    Currency         string
    Available        decimal.Decimal
    Reserved         decimal.Decimal
    OpenReservations []Reservation
}

func (w *Wallet) GetWallet(ctx context.Context, playerID, currency string) (WalletSnapshot, error)
```

Returns:
- `Available` from `GetBalance` on `user:<id>:cash_available:<ccy>`.
- `Reserved` from `GetBalance` on `user:<id>:cash_reserved:<ccy>`.
- `OpenReservations` from a new `ListReservations` RPC (added in this PR — see §10) filtered by `owner_type=user, owner_id=playerID, status in (HELD,PARTIAL)`.

## 6. Error model

```go
type ErrCode string

const (
    ErrInsufficientFunds          ErrCode = "INSUFFICIENT_FUNDS"
    ErrAccountNotFound            ErrCode = "ACCOUNT_NOT_FOUND"
    ErrAccountCurrencyMismatch    ErrCode = "ACCOUNT_CURRENCY_MISMATCH"
    ErrUnbalancedJournal          ErrCode = "UNBALANCED_JOURNAL"
    ErrDuplicateIdempotencyKey    ErrCode = "DUPLICATE_IDEMPOTENCY_KEY"
    ErrFlowAlreadyCompleted       ErrCode = "FLOW_ALREADY_COMPLETED"
    ErrFlowConflict               ErrCode = "FLOW_CONFLICT"
    ErrInvalidAccountStatus       ErrCode = "INVALID_ACCOUNT_STATUS"
    ErrSerializationRetryExhausted ErrCode = "SERIALIZATION_RETRY_EXHAUSTED"
    ErrReservationNotFound        ErrCode = "RESERVATION_NOT_FOUND"
    ErrReservationClosed          ErrCode = "RESERVATION_CLOSED"
    ErrReservationAmountExceeds   ErrCode = "RESERVATION_AMOUNT_EXCEEDS"
    ErrReservationCurrencyMismatch ErrCode = "RESERVATION_CURRENCY_MISMATCH"
    ErrFXRateNotFound             ErrCode = "FX_RATE_NOT_FOUND"
    ErrFXAmountMismatch           ErrCode = "FX_AMOUNT_MISMATCH"
    ErrDiscrepancyNotFound        ErrCode = "DISCREPANCY_NOT_FOUND"
    ErrDiscrepancyClosed          ErrCode = "DISCREPANCY_CLOSED"
    ErrReconBatchNotFound         ErrCode = "RECON_BATCH_NOT_FOUND"
)

// IsErrCode returns true if err is a Connect error whose
// "ledger-error-code" trailer/header matches code.
// Works for both embedded and remote modes.
func IsErrCode(err error, code ErrCode) bool
```

The server's `ToConnectError` (in `internal/service/errors.go`) already sets `ledger-error-code` on `connect.Error.Meta()`. Both embedded and remote `Client`s return `*connect.Error`. The helper extracts the header and compares.

## 7. Lifecycle

### Embedded

```
NewEmbedded(ctx, opts):
   - open DB → migrate (if MigrateAuto) → service.New → scheduler.New → outbox.NewDispatcher
   - go scheduler.Run(internalCtx); go dispatcher.Run(internalCtx)
   - return embeddedClient{srv, store, cancel}

embeddedClient.Close():
   - cancel internalCtx (scheduler + dispatcher stop)
   - store.Close()
```

The internal context is rooted at `context.Background()`, not the caller's `ctx` — `ctx` is only used for the initial Open call. Otherwise PAM canceling its startup context would silently shut the SDK down.

### Remote

```
NewRemote(serverURL, tenant, opts...):
   - build *http.Client (default if nil)
   - wrap with tenant transport
   - build connect client
   - return remoteClient{client}

remoteClient.Close():
   - return nil  // no resources to release
```

## 8. Concurrency and goroutine safety

- `Client` is safe for concurrent use from multiple goroutines (both modes).
- `Wallet` is safe for concurrent use (stateless beyond captured tenant+ownerType).
- `Close()` is not safe to call concurrently with in-flight requests on the same Client — PAM should drain its own request handlers before calling `Close()`.
- Embedded SDK respects all the existing concurrency guarantees: CRDB serializable + 40001 retry; SQLite `BEGIN IMMEDIATE` + single-writer.

## 9. Tests

`internal/sdk/embedded_test.go`:
- **`TestNewEmbedded_OpensAndMigrates`** — opens SQLite at a temp DSN, asserts migrations ran (tables exist).
- **`TestNewEmbedded_StartsSchedulerAndDispatcher`** — opens with default opts; verifies scheduler and dispatcher are reachable (insert a reservation with `expires_at` in the past; assert it transitions to EXPIRED within a few seconds).
- **`TestNewEmbedded_Close_StopsBackground`** — close the client; goroutines exit cleanly (use `runtime.NumGoroutine()` delta check).
- **`TestNewEmbedded_MigrateSkip_DoesNotRun`** — open with `MigrateSkip` against an empty DB; assert a CreateAccount call fails with a DB-level error (table not found).

`internal/sdk/wallet_test.go`:
- **`TestWallet_EnsurePlayerAccounts_Idempotent`** — call twice, assert same IDs returned.
- **`TestWallet_Deposit_IncreasesAvailable`** — funding → user_available; balance reflects amount.
- **`TestWallet_Reserve_CreatesHold`** — reserves N USD; reservation visible by id.
- **`TestWallet_Commit_FlipsToCommitted`** — full commit; reservation `COMMITTED`, destination balance up.
- **`TestWallet_Release_ReturnsFunds`** — release; user_available restored.
- **`TestWallet_Settle_PaysWinner`** — pool → user_available; winner balance reflects payout.
- **`TestWallet_Withdraw_DecreasesAvailable`** — user_available → withdrawal account; balance decreases.
- **`TestWallet_GetWallet_AvailableAndReserved`** — verifies the snapshot fields.

`pkg/dledger/errors_test.go`:
- **`TestIsErrCode_EmbeddedAndRemote`** — using a Wallet-driven INSUFFICIENT_FUNDS scenario, assert `IsErrCode` returns true for both an embedded Client and a remote Client (httptest server wrapping the same embedded Server).

## 10. `ListReservations` (in scope)

To populate `WalletSnapshot.OpenReservations`, the SDK plan also adds a new RPC:

```proto
message ListReservationsRequest {
  string tenant_id  = 1 [(buf.validate.field).string.min_len = 1];
  string owner_type = 2;          // optional filter, e.g. "user"
  string owner_id   = 3;          // optional filter
  string status     = 4;          // optional: HELD|PARTIAL|COMMITTED|RELEASED|EXPIRED
  int32  page_size  = 5;
}
message ListReservationsResponse { repeated Reservation reservations = 1; }
```

Implementation touchpoints:

- `sql/queries/{sqlite,crdb}/reservations.sql` — new `ListReservations` query joining accounts to derive owner_type/owner_id from the reservation's source account.
- `internal/repo` — `Store.ListReservations(ctx, ListReservationsInput) ([]Reservation, error)`.
- `internal/service/list_reservations.go` — handler.
- Maps `owner_type`/`owner_id` filters by joining `reservations.source_account_id → accounts.{owner_type, owner_id}`.

Adds ~0.5 day. Replaces the v1 "always empty" workaround.

## 11. Outbox events

No new event types. The SDK is a pure consumer/producer wrapper over the existing service; events emitted by the embedded server are the same ones the standalone server emits. PAM's `outbox.Sink` implementation routes them to whatever PAM's event bus is.

## 12. Examples

`examples/go/sdk_embedded/main.go`:

```go
client, _ := dledger.NewEmbedded(ctx, dledger.Options{
    Backend: dledger.SQLite, DSN: "./pam.db",
})
defer client.Close()

w := dledger.NewWallet(client, "tipmarket")
w.EnsurePlayerAccounts(ctx, "player-42", "USD")
w.Deposit(ctx, dledger.DepositInput{
    PlayerID: "player-42", Currency: "USD", Amount: "100",
    FundingAccountID: "platform:stripe:cash:USD",
    ExternalRef: "stripe_ch_abc", IdempotencyKey: "dep-1", SourceService: "stripe",
})
// ... reserve, commit, settle, balance check ...
```

`examples/go/sdk_remote/main.go`: same Wallet code, but constructed via `NewRemote("http://localhost:8080", "tipmarket")`. Demonstrates the swap-mode property.

## 13. Acceptance criteria

- `pkg/dledger.Client` interface mirrors all 21 RPCs + `Close()`.
- `NewEmbedded` opens a working client backed by an in-process `*service.Server`. Scheduler and outbox dispatcher run.
- `NewRemote` returns a working client that talks to a hosted server with `X-Tenant-Id` injected on every call.
- Swapping the constructor changes nothing else in PAM's code.
- `Wallet` provides Deposit / Reserve / Commit / Release / Settle / Withdraw / GetWallet methods that take per-call funding/destination/withdrawal account ids.
- `EnsurePlayerAccounts` is idempotent.
- `IsErrCode` correctly classifies errors from both modes.
- Example walkthrough runs end-to-end against an embedded SDK.
- All money movement still flows through `ExecuteFlow` / `PostJournal` — no new direct repo writes.

## 14. Out of scope (future)

- Multiple wallets per tenant (currently one `ownerType` per Wallet instance).
- Non-Go SDKs.
- gRPC stream support (no streaming RPCs in our service).
- A dedicated `cmd/sdk-demo` binary.
- Token-based auth in remote mode (PAM-to-ledger trust assumed at the network layer).
- Connection pooling for multiple remote clients (`http.Client` already handles this).
- TLS configuration helpers (`WithHTTPClient` is the escape hatch).
