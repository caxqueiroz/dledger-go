# Embedded SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a public `pkg/dledger` Go package that lets PAM either embed dledger-go in-process (`NewEmbedded`) or talk to a hosted server (`NewRemote`) behind the same `Client` interface, plus a `Wallet` helper for common prediction-market flows.

**Architecture:** A thin two-implementation `Client` interface mirrors all 22 RPCs (the existing 21 plus a new `ListReservations`). `NewEmbedded` boots `*service.Server`, scheduler, and outbox dispatcher in the caller's process against an embedded `goose` migrations FS. `NewRemote` wraps a `ledgerv1connect.NewLedgerServiceClient` with a transport that injects `X-Tenant-Id`. The `Wallet` helper layers Deposit/Reserve/Commit/Release/Settle/Withdraw/GetWallet on top of either backend, taking per-call funding/destination/pool/withdrawal account IDs so the SDK never invents accounting policy.

**Tech Stack:** Go 1.26, connectrpc.com/connect, modernc.org/sqlite, pgx/v5 + CockroachDB, sqlc, goose, shopspring/decimal, buf.

---

## File Structure

**New files:**

```
pkg/dledger/
  doc.go                 # package overview
  client.go              # Client interface (22 RPCs + Close)
  options.go             # Backend, MigrateMode, Options, Option (functional)
  errors.go              # ErrCode + IsErrCode
  embedded.go            # NewEmbedded + embeddedClient
  remote.go              # NewRemote + remoteClient + tenant transport
  wallet.go              # Wallet methods (per-call account IDs)
  wallet_types.go        # input/output structs

internal/sdk/
  migrations.go          # //go:embed sqlite + crdb migrations FS
  embedded_test.go       # constructor, migrate, scheduler, dispatcher, Close
  remote_test.go         # tenant transport injection
  wallet_test.go         # all wallet methods end-to-end against embedded
  errors_test.go         # IsErrCode for embedded + remote (httptest)
  testhelpers_test.go    # shared NewWallet/CreateAccounts helpers

examples/go/sdk_embedded/main.go
examples/go/sdk_remote/main.go

sql/queries/sqlite/reservations.sql          # +ListReservations query
sql/queries/crdb/reservations.sql            # +ListReservations query
```

**Modified files:**

```
proto/ledger/v1/ledger.proto                 # +ListReservations RPC + messages
internal/repo/repo.go                        # +Store.ListReservations
internal/repo/sqlite/store.go                # +impl
internal/repo/crdb/store.go                  # +impl
internal/service/server.go                   # (no change; new handler file)
internal/service/list_reservations.go        # new handler
internal/service/list_reservations_test.go   # handler test
docs/ARCHITECTURE.md                         # add SDK section
```

---

## Task 1: Add `ListReservations` RPC to proto + regenerate

**Files:**
- Modify: `proto/ledger/v1/ledger.proto`

- [ ] **Step 1: Add the RPC method to the service block**

In `proto/ledger/v1/ledger.proto`, inside the `service LedgerService { ... }` block, add the RPC after the existing `GetReservation` line (line 21):

```proto
  rpc ListReservations(ListReservationsRequest) returns (ListReservationsResponse);
```

- [ ] **Step 2: Add the request/response messages**

In `proto/ledger/v1/ledger.proto`, after the existing `GetReservationResponse` message (around line 249), add:

```proto
message ListReservationsRequest {
  string tenant_id  = 1 [(buf.validate.field).string.min_len = 1];
  string owner_type = 2;                   // optional filter (e.g. "user")
  string owner_id   = 3;                   // optional filter
  string status     = 4;                   // optional: HELD|PARTIAL|COMMITTED|RELEASED|EXPIRED
  int32  page_size  = 5;                   // 1..500; 0 → 100
}
message ListReservationsResponse {
  repeated Reservation reservations = 1;
}
```

- [ ] **Step 3: Regenerate Go + Connect bindings**

Run: `make proto`
Expected: no errors; new files/symbols appear under `gen/proto/ledger/v1/`.

Verify by grepping for the new types:

Run: `grep -l ListReservationsRequest gen/proto/ledger/v1/`
Expected: `gen/proto/ledger/v1/ledger.pb.go` is listed.

- [ ] **Step 4: Verify the Connect handler interface picked up the new method**

Run: `grep -n "ListReservations" gen/proto/ledger/v1/ledgerv1connect/ledger.connect.go | head -4`
Expected: at least one match showing `LedgerServiceHandler` requires `ListReservations`. The build will now fail until the service implements it (handled in Task 4).

- [ ] **Step 5: Commit**

```bash
git add proto/ledger/v1/ledger.proto gen/proto/ledger/v1/
git commit -m "feat(proto): add ListReservations RPC"
```

---

## Task 2: Add `ListReservations` SQL queries + regenerate sqlc

**Files:**
- Modify: `sql/queries/sqlite/reservations.sql`
- Modify: `sql/queries/crdb/reservations.sql`

- [ ] **Step 1: Add the SQLite query**

Append to `sql/queries/sqlite/reservations.sql`:

```sql
-- name: ListReservations :many
SELECT r.*
FROM reservations r
JOIN accounts a ON a.id = r.source_account_id
WHERE r.tenant_id = ?
  AND (a.owner_type = ?  OR ? = '')
  AND (a.owner_id   = ?  OR ? = '')
  AND (r.status     = ?  OR ? = '')
ORDER BY r.created_at DESC, r.id DESC
LIMIT ?;
```

- [ ] **Step 2: Add the CRDB query**

Append to `sql/queries/crdb/reservations.sql`:

```sql
-- name: ListReservations :many
SELECT r.*
FROM reservations r
JOIN accounts a ON a.id = r.source_account_id
WHERE r.tenant_id = $1
  AND (a.owner_type = $2 OR $2 = '')
  AND (a.owner_id   = $3 OR $3 = '')
  AND (r.status     = $4 OR $4 = '')
ORDER BY r.created_at DESC, r.id DESC
LIMIT $5;
```

- [ ] **Step 3: Regenerate sqlc**

Run: `make sqlc`
Expected: no errors; new method `ListReservations` and a `ListReservationsParams` struct appear in both `gen/sqlite/reservations.sql.go` and `gen/crdb/reservations.sql.go`.

Verify:

Run: `grep -n "func (q \*Queries) ListReservations" gen/sqlite/reservations.sql.go gen/crdb/reservations.sql.go`
Expected: one match per file.

- [ ] **Step 4: Commit**

```bash
git add sql/queries/sqlite/reservations.sql sql/queries/crdb/reservations.sql gen/sqlite/reservations.sql.go gen/crdb/reservations.sql.go
git commit -m "feat(repo): ListReservations sqlc queries"
```

---

## Task 3: Wire `ListReservations` into the `repo.Store` interface + both backends

**Files:**
- Modify: `internal/repo/repo.go`
- Modify: `internal/repo/sqlite/store.go`
- Modify: `internal/repo/crdb/store.go`

- [ ] **Step 1: Add the input struct + method to the Store interface**

In `internal/repo/repo.go`, near the other `List*Input` structs (after `ListFXRatesInput` definition), add:

```go
// ListReservationsInput filters reservations by tenant and optionally by
// owner (derived from the source account), status, and page size.
type ListReservationsInput struct {
	TenantID  string
	OwnerType string // optional
	OwnerID   string // optional
	Status    string // optional: HELD|PARTIAL|COMMITTED|RELEASED|EXPIRED
	Limit     int    // 1..500; 0 → 100
}
```

In the `Store` interface block, in the `// Reservations (read-only)` section, add a line right after `ListExpiredReservations`:

```go
	ListReservations(ctx context.Context, in ListReservationsInput) ([]ledger.Reservation, error)
```

- [ ] **Step 2: Implement on the SQLite store**

In `internal/repo/sqlite/store.go`, immediately after the `ListExpiredReservations` method (around line 308), add:

```go
// ListReservations returns reservations filtered by tenant and optional
// owner/status/limit. Filters use the dual-bind empty-string-sentinel pattern
// (see ListFXRates) so an empty value disables the filter at the SQL level.
func (s *Store) ListReservations(ctx context.Context, in repo.ListReservationsInput) ([]ledger.Reservation, error) {
	limit := int64(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.q.ListReservations(ctx, sqlitestore.ListReservationsParams{
		TenantID:  in.TenantID,
		OwnerType: in.OwnerType,
		Column3:   in.OwnerType,
		OwnerID:   in.OwnerID,
		Column5:   in.OwnerID,
		Status:    in.Status,
		Column7:   in.Status,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Reservation, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToReservation(r))
	}
	return out, nil
}
```

Note: column names (`Column3`, `Column5`, `Column7`) come from sqlc's auto-naming for the second bind of each `OR ? = ''` pair. If sqlc emits different field names, mirror what `gen/sqlite/reservations.sql.go::ListReservationsParams` declares.

- [ ] **Step 3: Implement on the CRDB store**

First, find the right insertion point:

Run: `grep -n "ListExpiredReservations" internal/repo/crdb/store.go`

Then, in `internal/repo/crdb/store.go`, immediately after the `ListExpiredReservations` method, add:

```go
// ListReservations returns reservations filtered by tenant and optional
// owner/status/limit.
func (s *Store) ListReservations(ctx context.Context, in repo.ListReservationsInput) ([]ledger.Reservation, error) {
	limit := int32(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.q.ListReservations(ctx, crdbstore.ListReservationsParams{
		TenantID:  in.TenantID,
		OwnerType: in.OwnerType,
		OwnerID:   in.OwnerID,
		Status:    in.Status,
		Limit:     int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Reservation, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToReservation(r))
	}
	return out, nil
}
```

If pgx-generated `ListReservationsParams` fields use sqlc's autonaming for the dollar-sign duplicate binds, follow what `gen/crdb/reservations.sql.go::ListReservationsParams` actually declares (the second `$2` is typically deduplicated by sqlc on postgres). Build to confirm the exact field names.

- [ ] **Step 4: Verify both backends build**

Run: `go build ./internal/repo/...`
Expected: no errors. The unused-import or missing-field compile errors here surface any drift between sqlc's actual field names and Step 2/3's draft.

- [ ] **Step 5: Verify the iface_assert lines still compile**

Run: `go build ./...`
Expected: only `internal/service` fails (because Task 4 hasn't added the handler yet) — `repo` and its backends must build.

If `*Store` no longer satisfies `repo.Store` you'll see `cannot use (*Store)(nil) as repo.Store value in variable declaration: missing method ListReservations`. Fix the new method's signature to exactly match the interface.

- [ ] **Step 6: Commit**

```bash
git add internal/repo/repo.go internal/repo/sqlite/store.go internal/repo/crdb/store.go
git commit -m "feat(repo): ListReservations on Store"
```

---

## Task 4: Add the `ListReservations` service handler + test

**Files:**
- Create: `internal/service/list_reservations.go`
- Create: `internal/service/list_reservations_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/service/list_reservations_test.go`:

```go
package service

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func TestListReservations_FiltersByOwner(t *testing.T) {
	srv, _ := newTestServer(t)
	ctx := context.Background()
	tenant := "t1"

	// Set up two users with reservable funds, reserve under each.
	mustCreateAccount(t, srv, tenant, "user", "u1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreateAccount(t, srv, tenant, "user", "u1", "cash_reserved", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreateAccount(t, srv, tenant, "user", "u2", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreateAccount(t, srv, tenant, "user", "u2", "cash_reserved", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreateAccount(t, srv, tenant, "platform", "0", "src", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	mustSeed(t, srv, tenant, "user:u1:cash_available:USD", "platform:0:src:USD", "100", "seed-u1")
	mustSeed(t, srv, tenant, "user:u2:cash_available:USD", "platform:0:src:USD", "100", "seed-u2")
	mustReserve(t, srv, tenant, "user:u1:cash_available:USD", "user:u1:cash_reserved:USD", "USD", "10", "res-u1", time.Time{})
	mustReserve(t, srv, tenant, "user:u2:cash_available:USD", "user:u2:cash_reserved:USD", "USD", "20", "res-u2", time.Time{})

	resp, err := srv.ListReservations(ctx, connect.NewRequest(&ledgerv1.ListReservationsRequest{
		TenantId: tenant, OwnerType: "user", OwnerId: "u1",
	}))
	if err != nil {
		t.Fatalf("ListReservations: %v", err)
	}
	if got := len(resp.Msg.GetReservations()); got != 1 {
		t.Fatalf("want 1 reservation for u1, got %d", got)
	}
	if want, got := "10", resp.Msg.GetReservations()[0].GetOriginalAmount(); got != want {
		t.Fatalf("want original=%q got %q", want, got)
	}
}
```

If `newTestServer`, `mustCreateAccount`, `mustSeed`, or `mustReserve` do not already exist as helpers in `internal/service/*_test.go`, look at `internal/service/reservations_test.go` to confirm the exact helper names and adopt them verbatim. (The existing reservation tests already exercise the same setup.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/service/ -run TestListReservations_FiltersByOwner -v`
Expected: build error (`srv.ListReservations undefined`) — Task 4 has not added the handler yet.

- [ ] **Step 3: Implement the handler**

Create `internal/service/list_reservations.go`:

```go
// internal/service/list_reservations.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ListReservations returns reservations filtered by tenant and optional
// owner/status/page_size. Owner filters join through the source account.
func (s *Server) ListReservations(ctx context.Context, req *connect.Request[ledgerv1.ListReservationsRequest]) (*connect.Response[ledgerv1.ListReservationsResponse], error) {
	r := req.Msg
	rows, err := s.Store.ListReservations(ctx, repo.ListReservationsInput{
		TenantID:  r.GetTenantId(),
		OwnerType: r.GetOwnerType(),
		OwnerID:   r.GetOwnerId(),
		Status:    r.GetStatus(),
		Limit:     int(r.GetPageSize()),
	})
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := make([]*ledgerv1.Reservation, 0, len(rows))
	for i := range rows {
		out = append(out, reservationToProto(&rows[i]))
	}
	return connect.NewResponse(&ledgerv1.ListReservationsResponse{Reservations: out}), nil
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `go test ./internal/service/ -run TestListReservations_FiltersByOwner -v`
Expected: PASS.

- [ ] **Step 5: Run the full test suite to catch regressions**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/service/list_reservations.go internal/service/list_reservations_test.go
git commit -m "feat(service): ListReservations handler"
```

---

## Task 5: Scaffold `pkg/dledger` package skeleton (Client interface + Options)

**Files:**
- Create: `pkg/dledger/doc.go`
- Create: `pkg/dledger/client.go`
- Create: `pkg/dledger/options.go`

- [ ] **Step 1: Create the package documentation file**

Create `pkg/dledger/doc.go`:

```go
// Package dledger is the public Go SDK for the dledger-go multi-currency
// double-entry ledger.
//
// Two backends are available and share the same Client interface:
//
//   - NewEmbedded opens an in-process ledger backed by SQLite or CockroachDB.
//   - NewRemote returns a Connect-RPC client that talks to a hosted server.
//
// The Wallet helper layers idiomatic prediction-market operations (Deposit,
// Reserve, Commit, Release, Settle, Withdraw, GetWallet) on top of either
// backend. The SDK never invents account IDs; callers pass funding,
// destination, pool, and withdrawal account IDs per call.
package dledger
```

- [ ] **Step 2: Create the Client interface file**

Create `pkg/dledger/client.go`:

```go
// pkg/dledger/client.go
package dledger

import (
	"context"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// Client is the mode-agnostic ledger surface. Both NewEmbedded and NewRemote
// return implementations of this interface. Swapping modes requires no other
// code change.
type Client interface {
	// Accounts
	CreateAccount(context.Context, *connect.Request[v1.CreateAccountRequest]) (*connect.Response[v1.CreateAccountResponse], error)
	GetAccount(context.Context, *connect.Request[v1.GetAccountRequest]) (*connect.Response[v1.GetAccountResponse], error)
	GetBalance(context.Context, *connect.Request[v1.GetBalanceRequest]) (*connect.Response[v1.GetBalanceResponse], error)

	// Journals + flows
	PostJournal(context.Context, *connect.Request[v1.PostJournalRequest]) (*connect.Response[v1.PostJournalResponse], error)
	ExecuteFlow(context.Context, *connect.Request[v1.ExecuteFlowRequest]) (*connect.Response[v1.ExecuteFlowResponse], error)
	GetFlow(context.Context, *connect.Request[v1.GetFlowRequest]) (*connect.Response[v1.GetFlowResponse], error)
	ListAccountActivity(context.Context, *connect.Request[v1.ListAccountActivityRequest]) (*connect.Response[v1.ListAccountActivityResponse], error)

	// Reservations
	CreateReservation(context.Context, *connect.Request[v1.CreateReservationRequest]) (*connect.Response[v1.CreateReservationResponse], error)
	CommitReservation(context.Context, *connect.Request[v1.CommitReservationRequest]) (*connect.Response[v1.CommitReservationResponse], error)
	ReleaseReservation(context.Context, *connect.Request[v1.ReleaseReservationRequest]) (*connect.Response[v1.ReleaseReservationResponse], error)
	GetReservation(context.Context, *connect.Request[v1.GetReservationRequest]) (*connect.Response[v1.GetReservationResponse], error)
	ListReservations(context.Context, *connect.Request[v1.ListReservationsRequest]) (*connect.Response[v1.ListReservationsResponse], error)

	// Snapshots
	TakeBalanceSnapshot(context.Context, *connect.Request[v1.TakeBalanceSnapshotRequest]) (*connect.Response[v1.TakeBalanceSnapshotResponse], error)

	// FX
	ExecuteExchange(context.Context, *connect.Request[v1.ExecuteExchangeRequest]) (*connect.Response[v1.ExecuteExchangeResponse], error)
	PutFXRate(context.Context, *connect.Request[v1.PutFXRateRequest]) (*connect.Response[v1.PutFXRateResponse], error)
	GetFXRate(context.Context, *connect.Request[v1.GetFXRateRequest]) (*connect.Response[v1.GetFXRateResponse], error)
	ListFXRates(context.Context, *connect.Request[v1.ListFXRatesRequest]) (*connect.Response[v1.ListFXRatesResponse], error)

	// Reconciliation
	IngestExternalRecords(context.Context, *connect.Request[v1.IngestExternalRecordsRequest]) (*connect.Response[v1.IngestExternalRecordsResponse], error)
	RunReconciliation(context.Context, *connect.Request[v1.RunReconciliationRequest]) (*connect.Response[v1.RunReconciliationResponse], error)
	GetReconciliationBatch(context.Context, *connect.Request[v1.GetReconciliationBatchRequest]) (*connect.Response[v1.GetReconciliationBatchResponse], error)
	ListDiscrepancies(context.Context, *connect.Request[v1.ListDiscrepanciesRequest]) (*connect.Response[v1.ListDiscrepanciesResponse], error)
	ResolveDiscrepancy(context.Context, *connect.Request[v1.ResolveDiscrepancyRequest]) (*connect.Response[v1.ResolveDiscrepancyResponse], error)

	// Close releases any background resources (scheduler, dispatcher, DB).
	// Safe to call exactly once; subsequent calls return nil.
	Close() error
}
```

- [ ] **Step 3: Create the Options file**

Create `pkg/dledger/options.go`:

```go
// pkg/dledger/options.go
package dledger

import (
	"log/slog"
	"net/http"

	"github.com/caxqueiroz/dledger-go/internal/outbox"
)

// Backend selects the embedded store implementation.
type Backend string

const (
	SQLite Backend = "sqlite"
	CRDB   Backend = "crdb"
)

// MigrateMode controls whether NewEmbedded runs goose migrations on open.
type MigrateMode int

const (
	// MigrateAuto runs goose up against the embedded migrations FS on Open.
	MigrateAuto MigrateMode = iota
	// MigrateSkip leaves migrations to the operator (cmd/migrate).
	MigrateSkip
)

// Options configures NewEmbedded.
type Options struct {
	Backend          Backend
	DSN              string
	MigrateMode      MigrateMode
	OutboxSink       outbox.Sink  // default outbox.LogSink with Logger
	DisableScheduler bool
	Logger           *slog.Logger // default slog.Default()
}

// Option is a functional option for NewRemote.
type Option func(*remoteOptions)

type remoteOptions struct {
	httpClient *http.Client
	logger     *slog.Logger
}

// WithHTTPClient overrides the http.Client used by NewRemote.
// Set this to configure TLS, timeouts, or proxies.
func WithHTTPClient(c *http.Client) Option {
	return func(o *remoteOptions) { o.httpClient = c }
}

// WithLogger overrides the logger used by NewRemote.
func WithLogger(l *slog.Logger) Option {
	return func(o *remoteOptions) { o.logger = l }
}
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./pkg/dledger/`
Expected: PASS (no constructors yet; this only sanity-checks the types).

- [ ] **Step 5: Commit**

```bash
git add pkg/dledger/doc.go pkg/dledger/client.go pkg/dledger/options.go
git commit -m "feat(sdk): Client interface and Options scaffolding"
```

---

## Task 6: `errors.go` — ErrCode + IsErrCode + unit test

**Files:**
- Create: `pkg/dledger/errors.go`
- Create: `pkg/dledger/errors_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/dledger/errors_test.go`:

```go
// pkg/dledger/errors_test.go
package dledger

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
)

func TestIsErrCode_MatchesHeader(t *testing.T) {
	e := connect.NewError(connect.CodeFailedPrecondition, errors.New("not enough"))
	e.Meta().Set("ledger-error-code", string(ErrInsufficientFunds))
	if !IsErrCode(e, ErrInsufficientFunds) {
		t.Fatalf("expected IsErrCode to return true for matching code")
	}
	if IsErrCode(e, ErrAccountNotFound) {
		t.Fatalf("expected IsErrCode to return false for mismatching code")
	}
}

func TestIsErrCode_NilAndNonConnect(t *testing.T) {
	if IsErrCode(nil, ErrInsufficientFunds) {
		t.Fatalf("nil error must not match")
	}
	if IsErrCode(errors.New("plain"), ErrInsufficientFunds) {
		t.Fatalf("plain error must not match")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/dledger/ -run TestIsErrCode -v`
Expected: build error (`ErrInsufficientFunds undefined`, `IsErrCode undefined`).

- [ ] **Step 3: Implement errors.go**

Create `pkg/dledger/errors.go`:

```go
// pkg/dledger/errors.go
package dledger

import (
	"errors"

	"connectrpc.com/connect"
)

// ErrCode mirrors the ledger.DomainCode values the server sets on the
// "ledger-error-code" Connect header. Works identically for embedded and
// remote backends because both round-trip via *connect.Error.
type ErrCode string

const (
	ErrInsufficientFunds           ErrCode = "INSUFFICIENT_FUNDS"
	ErrAccountNotFound             ErrCode = "ACCOUNT_NOT_FOUND"
	ErrAccountCurrencyMismatch     ErrCode = "ACCOUNT_CURRENCY_MISMATCH"
	ErrUnbalancedJournal           ErrCode = "UNBALANCED_JOURNAL"
	ErrDuplicateIdempotencyKey     ErrCode = "DUPLICATE_IDEMPOTENCY_KEY"
	ErrFlowAlreadyCompleted        ErrCode = "FLOW_ALREADY_COMPLETED"
	ErrFlowConflict                ErrCode = "FLOW_CONFLICT"
	ErrInvalidAccountStatus        ErrCode = "INVALID_ACCOUNT_STATUS"
	ErrSerializationRetryExhausted ErrCode = "SERIALIZATION_RETRY_EXHAUSTED"
	ErrReservationNotFound         ErrCode = "RESERVATION_NOT_FOUND"
	ErrReservationClosed           ErrCode = "RESERVATION_CLOSED"
	ErrReservationAmountExceeds    ErrCode = "RESERVATION_AMOUNT_EXCEEDS"
	ErrReservationCurrencyMismatch ErrCode = "RESERVATION_CURRENCY_MISMATCH"
	ErrFXRateNotFound              ErrCode = "FX_RATE_NOT_FOUND"
	ErrFXAmountMismatch            ErrCode = "FX_AMOUNT_MISMATCH"
	ErrDiscrepancyNotFound         ErrCode = "DISCREPANCY_NOT_FOUND"
	ErrDiscrepancyClosed           ErrCode = "DISCREPANCY_CLOSED"
	ErrReconBatchNotFound          ErrCode = "RECON_BATCH_NOT_FOUND"
)

// IsErrCode reports whether err is a Connect-RPC error whose
// "ledger-error-code" header equals code.
func IsErrCode(err error, code ErrCode) bool {
	if err == nil {
		return false
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return false
	}
	return ce.Meta().Get("ledger-error-code") == string(code)
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run: `go test ./pkg/dledger/ -run TestIsErrCode -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/dledger/errors.go pkg/dledger/errors_test.go
git commit -m "feat(sdk): ErrCode and IsErrCode helper"
```

---

## Task 7: Embed migrations FS for in-process `goose up`

**Files:**
- Create: `internal/sdk/migrations.go`

- [ ] **Step 1: Verify the migrations directory layout**

Run: `ls sql/migrations/sqlite sql/migrations/crdb`
Expected: each lists at least `0001_init.sql ... 0005_reconciliation.sql`.

- [ ] **Step 2: Create the embed wrapper**

The SDK is consumed as a library by PAM; we cannot rely on `sql/migrations/` being present on disk at runtime. Embed both dialect subtrees and expose them as `fs.FS` rooted at the dialect directory (goose expects the migration files at the FS root).

Create `internal/sdk/migrations.go`:

```go
// internal/sdk/migrations.go
package sdk

import (
	"embed"
	"io/fs"
)

//go:embed migrations/sqlite/*.sql migrations/crdb/*.sql
var migrationsFS embed.FS

// SQLiteMigrations returns a filesystem rooted at the SQLite migrations dir.
func SQLiteMigrations() fs.FS {
	sub, err := fs.Sub(migrationsFS, "migrations/sqlite")
	if err != nil {
		panic(err) // unreachable; the directory is statically embedded
	}
	return sub
}

// CRDBMigrations returns a filesystem rooted at the CRDB migrations dir.
func CRDBMigrations() fs.FS {
	sub, err := fs.Sub(migrationsFS, "migrations/crdb")
	if err != nil {
		panic(err)
	}
	return sub
}
```

- [ ] **Step 3: Mirror the migration files under `internal/sdk/migrations/`**

`go:embed` cannot reach outside its package directory. Mirror the source files in via a symlink so they stay in sync with the canonical copies:

```bash
mkdir -p internal/sdk/migrations
ln -s ../../../sql/migrations/sqlite internal/sdk/migrations/sqlite
ln -s ../../../sql/migrations/crdb   internal/sdk/migrations/crdb
```

Verify:

Run: `ls -l internal/sdk/migrations/sqlite/ | head -3 && ls -l internal/sdk/migrations/crdb/ | head -3`
Expected: both list the `0001_init.sql ... 0005_reconciliation.sql` files (resolved through the symlink).

Note: `go:embed` follows symlinks for top-level matches but not always within. If `go build` later complains about the symlink (Go 1.26 toolchain rejects pattern matches reached only through a symlink), replace the symlinks with `cp -R` and add a `make` target that re-syncs them. The simplest fallback is `cp -R sql/migrations/sqlite internal/sdk/migrations/sqlite && cp -R sql/migrations/crdb internal/sdk/migrations/crdb`.

- [ ] **Step 4: Build and verify embed works**

Run: `go build ./internal/sdk/`
Expected: PASS. A failure here that says "pattern migrations/sqlite/*.sql: no matching files found" means the files aren't actually visible to embed; switch to `cp -R` per the note above and retry.

- [ ] **Step 5: Commit**

```bash
git add internal/sdk/migrations.go internal/sdk/migrations
git commit -m "feat(sdk): embed migrations FS for in-process goose"
```

---

## Task 8: `NewEmbedded` constructor + `embeddedClient`

**Files:**
- Create: `pkg/dledger/embedded.go`

- [ ] **Step 1: Implement the embedded constructor**

Create `pkg/dledger/embedded.go`:

```go
// pkg/dledger/embedded.go
package dledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/dledger-go/internal/outbox"
	"github.com/caxqueiroz/dledger-go/internal/repo"
	"github.com/caxqueiroz/dledger-go/internal/repo/crdb"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/scheduler"
	"github.com/caxqueiroz/dledger-go/internal/sdk"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

// NewEmbedded boots an in-process dledger and returns a Client that delegates
// to the local *service.Server. The returned Client owns the database
// connection, the snapshot/expiry/retention scheduler, and the outbox
// dispatcher. Close releases all of them.
func NewEmbedded(ctx context.Context, opts Options) (Client, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.DSN == "" {
		return nil, errors.New("dledger: Options.DSN required")
	}

	var (
		store     repo.Store
		migFs     = sdk.SQLiteMigrations()
		migDrv    = "sqlite"
		migDial   = "sqlite3"
	)
	switch opts.Backend {
	case SQLite, "":
		s, err := sqlite.Open(ctx, opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		store = s
	case CRDB:
		s, err := crdb.Open(ctx, opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("open crdb: %w", err)
		}
		store = s
		migFs = sdk.CRDBMigrations()
		migDrv = "pgx"
		migDial = "postgres"
	default:
		return nil, fmt.Errorf("dledger: unknown backend %q", opts.Backend)
	}

	if opts.MigrateMode == MigrateAuto {
		if err := runMigrations(migDrv, opts.DSN, migFs, migDial); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}

	srv := service.New(store)

	sink := opts.OutboxSink
	if sink == nil {
		sink = outbox.LogSink{Logger: logger}
	}
	disp := outbox.NewDispatcher(outbox.RepoAdapter{Store: store}, sink, outbox.Config{
		Interval: 250 * time.Millisecond, BatchSize: 100,
	})

	bgCtx, cancel := context.WithCancel(context.Background())
	go disp.Run(bgCtx)

	if !opts.DisableScheduler {
		sched := scheduler.New(store, srv)
		sched.Log = logger
		go sched.Run(bgCtx)
	}

	return &embeddedClient{
		LedgerServiceHandler: srv,
		store:                store,
		cancel:               cancel,
	}, nil
}

// embeddedClient delegates the 22 RPCs to *service.Server (which satisfies
// the generated handler interface) and adds Close.
type embeddedClient struct {
	// Embedding the generated handler interface auto-implements all 22 methods.
	ledgerv1connect.LedgerServiceHandler
	store  repo.Store
	cancel context.CancelFunc
	closed bool
}

func (c *embeddedClient) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.store.Close()
}

// runMigrations executes `goose up` against an embedded filesystem.
func runMigrations(driver, dsn string, fsys fs.FS, dialect string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("open for migrate: %w", err)
	}
	defer db.Close()
	if err := goose.SetDialect(dialect); err != nil {
		return err
	}
	goose.SetBaseFS(fsys)
	return goose.Up(db, ".")
}
```

The embedded `ledgerv1connect.LedgerServiceHandler` interface includes all 22 RPC method signatures with the exact same shape as our `Client` interface; embedding it makes `embeddedClient` satisfy `Client` minus `Close`, which is added explicitly. `*service.Server` satisfies the generated interface; after Task 4 it also includes `ListReservations`.

- [ ] **Step 2: Verify it builds**

Run: `go build ./pkg/dledger/`
Expected: PASS. Likely missing import errors get fixed by replacing the alias shim with the direct `ledgerv1connect.LedgerServiceHandler` embedding noted above.

- [ ] **Step 3: Add an interface assertion**

At the bottom of `pkg/dledger/embedded.go`, add:

```go
var _ Client = (*embeddedClient)(nil)
```

Build again:

Run: `go build ./pkg/dledger/`
Expected: PASS. If `*embeddedClient` does not satisfy `Client`, the compiler will list the missing methods — these come from the generated handler interface having different parameter names from `Client`. The signatures must match exactly. If a mismatch surfaces, regenerate the proto (`make proto`) and check that `Client` in `pkg/dledger/client.go` was authored against the regenerated package.

- [ ] **Step 4: Commit**

```bash
git add pkg/dledger/embedded.go
git commit -m "feat(sdk): NewEmbedded constructor and embeddedClient"
```

---

## Task 9: Test `NewEmbedded` lifecycle (open, migrate, schedule, dispatch, close)

**Files:**
- Create: `internal/sdk/embedded_test.go`

- [ ] **Step 1: Write the test**

Create `internal/sdk/embedded_test.go`:

```go
// internal/sdk/embedded_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func TestNewEmbedded_OpensAndMigrates(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	// CreateAccount succeeds only if migrations ran (accounts table exists).
	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
}

func TestNewEmbedded_MigrateSkip_DoesNotRun(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, MigrateMode: dledger.MigrateSkip,
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err == nil {
		t.Fatalf("expected CreateAccount to fail on un-migrated DB")
	}
}

func TestNewEmbedded_Close_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "sdk.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNewEmbedded_Scheduler_ExpiresReservation(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	tenant := "t1"
	// Set up source/available/reserved accounts and seed funds.
	mustCreate(t, c, tenant, "platform", "0", "src", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, tenant, "user", "u1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreate(t, c, tenant, "user", "u1", "cash_reserved", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err = c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: "seed", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "seed-evt",
			Entries: []*ledgerv1.Entry{
				{AccountId: "user:u1:cash_available:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: "platform:0:src:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Reservation with an already-past expiry.
	resv, err := c.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: tenant, IdempotencyKey: "res-1",
		SourceAccountId: "user:u1:cash_available:USD", ReservedAccountId: "user:u1:cash_reserved:USD",
		Currency: "USD", Amount: "10",
		ExpiresAt: timestamp(t, time.Now().Add(-1*time.Second)),
		SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("CreateReservation: %v", err)
	}

	// Poll for up to 2 minutes (expiry tick = 30s default).
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		got, err := c.GetReservation(ctx, connect.NewRequest(&ledgerv1.GetReservationRequest{
			TenantId: tenant, ReservationId: resv.Msg.GetReservation().GetId(),
		}))
		if err != nil {
			t.Fatalf("GetReservation: %v", err)
		}
		if got.Msg.GetReservation().GetStatus() == "EXPIRED" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("reservation did not transition to EXPIRED within deadline")
}
```

Add the shared helpers used by this and later test files in `internal/sdk/testhelpers_test.go`:

```go
// internal/sdk/testhelpers_test.go
package sdk_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func mustCreate(t *testing.T, c dledger.Client, tenant, ownerType, ownerID, acctType, ccy string, nb ledgerv1.NormalBalance) {
	t.Helper()
	_, err := c.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: acctType, Currency: ccy, NormalBalance: nb,
	}))
	if err != nil {
		t.Fatalf("CreateAccount %s:%s: %v", ownerType, ownerID, err)
	}
}

func timestamp(t *testing.T, ts time.Time) *timestamppb.Timestamp {
	t.Helper()
	return timestamppb.New(ts)
}
```

- [ ] **Step 2: Run the tests and verify they pass**

Run: `go test ./internal/sdk/ -v`
Expected: PASS. The scheduler test may take up to ~30s due to the default tick.

- [ ] **Step 3: Commit**

```bash
git add internal/sdk/embedded_test.go internal/sdk/testhelpers_test.go
git commit -m "test(sdk): embedded lifecycle + scheduler expiry"
```

---

## Task 10: `NewRemote` constructor + tenant transport + test

**Files:**
- Create: `pkg/dledger/remote.go`
- Create: `internal/sdk/remote_test.go`

- [ ] **Step 1: Implement the remote constructor**

Create `pkg/dledger/remote.go`:

```go
// pkg/dledger/remote.go
package dledger

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
)

// NewRemote returns a Client that talks to a hosted dledger server.
// tenantID is injected as the X-Tenant-Id header on every request.
func NewRemote(serverURL, tenantID string, opts ...Option) Client {
	o := &remoteOptions{httpClient: http.DefaultClient, logger: slog.Default()}
	for _, fn := range opts {
		fn(o)
	}
	hc := *o.httpClient
	hc.Transport = &tenantTransport{base: roundTripperOr(hc.Transport, http.DefaultTransport), tenant: tenantID}
	rpc := ledgerv1connect.NewLedgerServiceClient(&hc, serverURL)
	return &remoteClient{rpc: rpc}
}

func roundTripperOr(rt http.RoundTripper, fallback http.RoundTripper) http.RoundTripper {
	if rt != nil {
		return rt
	}
	return fallback
}

// tenantTransport sets X-Tenant-Id on every outbound request.
type tenantTransport struct {
	base   http.RoundTripper
	tenant string
}

func (t *tenantTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone so we don't mutate the caller's request.
	req2 := req.Clone(req.Context())
	req2.Header.Set("X-Tenant-Id", t.tenant)
	return t.base.RoundTrip(req2)
}

// remoteClient forwards each RPC to a Connect-RPC client.
type remoteClient struct {
	rpc ledgerv1connect.LedgerServiceClient
}

func (c *remoteClient) Close() error { return nil }

func (c *remoteClient) CreateAccount(ctx context.Context, r *connect.Request[v1.CreateAccountRequest]) (*connect.Response[v1.CreateAccountResponse], error) {
	return c.rpc.CreateAccount(ctx, r)
}
func (c *remoteClient) GetAccount(ctx context.Context, r *connect.Request[v1.GetAccountRequest]) (*connect.Response[v1.GetAccountResponse], error) {
	return c.rpc.GetAccount(ctx, r)
}
func (c *remoteClient) GetBalance(ctx context.Context, r *connect.Request[v1.GetBalanceRequest]) (*connect.Response[v1.GetBalanceResponse], error) {
	return c.rpc.GetBalance(ctx, r)
}
func (c *remoteClient) PostJournal(ctx context.Context, r *connect.Request[v1.PostJournalRequest]) (*connect.Response[v1.PostJournalResponse], error) {
	return c.rpc.PostJournal(ctx, r)
}
func (c *remoteClient) ExecuteFlow(ctx context.Context, r *connect.Request[v1.ExecuteFlowRequest]) (*connect.Response[v1.ExecuteFlowResponse], error) {
	return c.rpc.ExecuteFlow(ctx, r)
}
func (c *remoteClient) GetFlow(ctx context.Context, r *connect.Request[v1.GetFlowRequest]) (*connect.Response[v1.GetFlowResponse], error) {
	return c.rpc.GetFlow(ctx, r)
}
func (c *remoteClient) ListAccountActivity(ctx context.Context, r *connect.Request[v1.ListAccountActivityRequest]) (*connect.Response[v1.ListAccountActivityResponse], error) {
	return c.rpc.ListAccountActivity(ctx, r)
}
func (c *remoteClient) CreateReservation(ctx context.Context, r *connect.Request[v1.CreateReservationRequest]) (*connect.Response[v1.CreateReservationResponse], error) {
	return c.rpc.CreateReservation(ctx, r)
}
func (c *remoteClient) CommitReservation(ctx context.Context, r *connect.Request[v1.CommitReservationRequest]) (*connect.Response[v1.CommitReservationResponse], error) {
	return c.rpc.CommitReservation(ctx, r)
}
func (c *remoteClient) ReleaseReservation(ctx context.Context, r *connect.Request[v1.ReleaseReservationRequest]) (*connect.Response[v1.ReleaseReservationResponse], error) {
	return c.rpc.ReleaseReservation(ctx, r)
}
func (c *remoteClient) GetReservation(ctx context.Context, r *connect.Request[v1.GetReservationRequest]) (*connect.Response[v1.GetReservationResponse], error) {
	return c.rpc.GetReservation(ctx, r)
}
func (c *remoteClient) ListReservations(ctx context.Context, r *connect.Request[v1.ListReservationsRequest]) (*connect.Response[v1.ListReservationsResponse], error) {
	return c.rpc.ListReservations(ctx, r)
}
func (c *remoteClient) TakeBalanceSnapshot(ctx context.Context, r *connect.Request[v1.TakeBalanceSnapshotRequest]) (*connect.Response[v1.TakeBalanceSnapshotResponse], error) {
	return c.rpc.TakeBalanceSnapshot(ctx, r)
}
func (c *remoteClient) ExecuteExchange(ctx context.Context, r *connect.Request[v1.ExecuteExchangeRequest]) (*connect.Response[v1.ExecuteExchangeResponse], error) {
	return c.rpc.ExecuteExchange(ctx, r)
}
func (c *remoteClient) PutFXRate(ctx context.Context, r *connect.Request[v1.PutFXRateRequest]) (*connect.Response[v1.PutFXRateResponse], error) {
	return c.rpc.PutFXRate(ctx, r)
}
func (c *remoteClient) GetFXRate(ctx context.Context, r *connect.Request[v1.GetFXRateRequest]) (*connect.Response[v1.GetFXRateResponse], error) {
	return c.rpc.GetFXRate(ctx, r)
}
func (c *remoteClient) ListFXRates(ctx context.Context, r *connect.Request[v1.ListFXRatesRequest]) (*connect.Response[v1.ListFXRatesResponse], error) {
	return c.rpc.ListFXRates(ctx, r)
}
func (c *remoteClient) IngestExternalRecords(ctx context.Context, r *connect.Request[v1.IngestExternalRecordsRequest]) (*connect.Response[v1.IngestExternalRecordsResponse], error) {
	return c.rpc.IngestExternalRecords(ctx, r)
}
func (c *remoteClient) RunReconciliation(ctx context.Context, r *connect.Request[v1.RunReconciliationRequest]) (*connect.Response[v1.RunReconciliationResponse], error) {
	return c.rpc.RunReconciliation(ctx, r)
}
func (c *remoteClient) GetReconciliationBatch(ctx context.Context, r *connect.Request[v1.GetReconciliationBatchRequest]) (*connect.Response[v1.GetReconciliationBatchResponse], error) {
	return c.rpc.GetReconciliationBatch(ctx, r)
}
func (c *remoteClient) ListDiscrepancies(ctx context.Context, r *connect.Request[v1.ListDiscrepanciesRequest]) (*connect.Response[v1.ListDiscrepanciesResponse], error) {
	return c.rpc.ListDiscrepancies(ctx, r)
}
func (c *remoteClient) ResolveDiscrepancy(ctx context.Context, r *connect.Request[v1.ResolveDiscrepancyRequest]) (*connect.Response[v1.ResolveDiscrepancyResponse], error) {
	return c.rpc.ResolveDiscrepancy(ctx, r)
}

var _ Client = (*remoteClient)(nil)
```

- [ ] **Step 2: Build to verify the interface is satisfied**

Run: `go build ./pkg/dledger/`
Expected: PASS. Any missing-method error here means `Client` and `LedgerServiceClient` have drifted; fix the wrapper.

- [ ] **Step 3: Write the integration test that wraps the same embedded server**

Create `internal/sdk/remote_test.go`:

```go
// internal/sdk/remote_test.go
package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/service"
	"github.com/caxqueiroz/dledger-go/internal/service/interceptors"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// newRemoteAgainstEmbeddedServer spins up an httptest server backed by a
// throwaway SQLite store and returns a remote dledger.Client pointing at it.
// Mirrors how PAM would talk to a hosted dledger.
func newRemoteAgainstEmbeddedServer(t *testing.T, tenant string) dledger.Client {
	t.Helper()
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	// Reuse the embedded constructor to migrate + boot a server, but only
	// to provide the *service.Server we hand to the Connect mux.
	embedded, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("embedded boot: %v", err)
	}

	// To get a *service.Server for the mux we open a second store and a new
	// server against the *already-migrated* DSN (MigrateSkip).
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := service.New(store)

	mux := http.NewServeMux()
	path, handler := ledgerv1connect.NewLedgerServiceHandler(srv,
		connect.WithInterceptors(interceptors.NewTenant()),
	)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() {
		ts.Close()
		_ = store.Close()
		_ = embedded.Close()
	})

	return dledger.NewRemote(ts.URL, tenant)
}

func TestNewRemote_InjectsTenantHeader(t *testing.T) {
	ctx := context.Background()
	c := newRemoteAgainstEmbeddedServer(t, "t1")
	defer c.Close()

	// A CreateAccount round trip succeeds only if X-Tenant-Id reached
	// the NewTenant interceptor (which would 400 otherwise).
	_, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err != nil {
		t.Fatalf("CreateAccount via remote: %v", err)
	}
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/sdk/ -run TestNewRemote -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/dledger/remote.go internal/sdk/remote_test.go
git commit -m "feat(sdk): NewRemote constructor with tenant transport"
```

---

## Task 11: Wallet types + `NewWallet` + `EnsurePlayerAccounts`

**Files:**
- Create: `pkg/dledger/wallet_types.go`
- Create: `pkg/dledger/wallet.go`
- Create: `internal/sdk/wallet_test.go` (initial — extended in later tasks)

- [ ] **Step 1: Create the types file**

Create `pkg/dledger/wallet_types.go`:

```go
// pkg/dledger/wallet_types.go
package dledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// PlayerAccounts is the pair of canonical per-player accounts the SDK manages.
type PlayerAccounts struct {
	Available string // e.g. "user:<id>:cash_available:USD"
	Reserved  string // e.g. "user:<id>:cash_reserved:USD"
}

// Receipt is returned by money-movement Wallet methods that synthesize a
// single-step PostJournal.
type Receipt struct {
	JournalID string
	FlowRunID string
}

// Reservation is the SDK's idiomatic view of a ledger reservation.
type Reservation struct {
	ID                string
	Status            string
	OriginalAmount    string
	OutstandingAmount string
	CommittedAmount   string
	ReleasedAmount    string
	ExpiresAt         time.Time // zero if no expiry
}

type DepositInput struct {
	PlayerID         string
	Currency         string
	Amount           string
	FundingAccountID string
	ExternalRef      string
	IdempotencyKey   string
	SourceService    string
}

type WithdrawInput struct {
	PlayerID            string
	Currency            string
	Amount              string
	WithdrawalAccountID string
	ExternalRef         string
	IdempotencyKey      string
	SourceService       string
}

type ReserveInput struct {
	PlayerID       string
	Currency       string
	Amount         string
	ExpiresAt      time.Time // zero = no auto-expiry
	IdempotencyKey string
	SourceService  string
	Metadata       map[string]any
}

type CommitInput struct {
	ReservationID        string
	DestinationAccountID string
	Amount               string
	IdempotencyKey       string
	SourceService        string
}

type ReleaseInput struct {
	ReservationID  string
	Amount         string
	IdempotencyKey string
	SourceService  string
}

type SettleInput struct {
	PlayerID       string
	Currency       string
	Amount         string
	PoolAccountID  string
	ExternalRef    string
	IdempotencyKey string
	SourceService  string
}

type WalletSnapshot struct {
	PlayerID         string
	Currency         string
	Available        decimal.Decimal
	Reserved         decimal.Decimal
	OpenReservations []Reservation
}
```

- [ ] **Step 2: Create the wallet skeleton with NewWallet + EnsurePlayerAccounts**

Create `pkg/dledger/wallet.go`:

```go
// pkg/dledger/wallet.go
package dledger

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// Wallet is the prediction-market-focused convenience layer over Client.
// Safe for concurrent use; stateless beyond the captured tenant and owner type.
type Wallet struct {
	client    Client
	tenant    string
	ownerType string
}

// WalletOption configures a Wallet at construction.
type WalletOption func(*Wallet)

// WithOwnerType overrides the default owner_type ("user") used to derive
// per-player account IDs.
func WithOwnerType(t string) WalletOption {
	return func(w *Wallet) { w.ownerType = t }
}

// NewWallet returns a Wallet bound to the given client + tenant.
func NewWallet(c Client, tenantID string, opts ...WalletOption) *Wallet {
	w := &Wallet{client: c, tenant: tenantID, ownerType: "user"}
	for _, fn := range opts {
		fn(w)
	}
	return w
}

// EnsurePlayerAccounts idempotently creates the two debit-normal accounts
// (cash_available, cash_reserved) for a player and returns their IDs.
func (w *Wallet) EnsurePlayerAccounts(ctx context.Context, playerID, currency string) (PlayerAccounts, error) {
	avail := w.accountID(playerID, "cash_available", currency)
	resv := w.accountID(playerID, "cash_reserved", currency)
	if err := w.ensureAccount(ctx, playerID, "cash_available", currency); err != nil {
		return PlayerAccounts{}, fmt.Errorf("ensure cash_available: %w", err)
	}
	if err := w.ensureAccount(ctx, playerID, "cash_reserved", currency); err != nil {
		return PlayerAccounts{}, fmt.Errorf("ensure cash_reserved: %w", err)
	}
	return PlayerAccounts{Available: avail, Reserved: resv}, nil
}

func (w *Wallet) accountID(ownerID, acctType, currency string) string {
	return fmt.Sprintf("%s:%s:%s:%s", w.ownerType, ownerID, acctType, currency)
}

// ensureAccount creates an account and swallows "already exists" errors.
// Detection layers: connect.CodeAlreadyExists (if surfaced), then a
// GetAccount probe as a backstop for SQL-generic primary-key conflicts.
func (w *Wallet) ensureAccount(ctx context.Context, ownerID, acctType, currency string) error {
	_, err := w.client.CreateAccount(ctx, connect.NewRequest(&v1.CreateAccountRequest{
		TenantId: w.tenant, OwnerType: w.ownerType, OwnerId: ownerID,
		AccountType: acctType, Currency: currency,
		NormalBalance: v1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err == nil {
		return nil
	}
	if connect.CodeOf(err) == connect.CodeAlreadyExists {
		return nil
	}
	if _, ge := w.client.GetAccount(ctx, connect.NewRequest(&v1.GetAccountRequest{
		TenantId: w.tenant, AccountId: w.accountID(ownerID, acctType, currency),
	})); ge == nil {
		return nil
	}
	return err
}
```

- [ ] **Step 3: Write the idempotency test**

Create `internal/sdk/wallet_test.go`:

```go
// internal/sdk/wallet_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func newWalletWithEmbedded(t *testing.T) (dledger.Client, *dledger.Wallet) {
	t.Helper()
	ctx := context.Background()
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "sdk.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, dledger.NewWallet(c, "t1")
}

func TestWallet_EnsurePlayerAccounts_Idempotent(t *testing.T) {
	_, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	a, err := w.EnsurePlayerAccounts(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := w.EnsurePlayerAccounts(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if a != b {
		t.Fatalf("expected stable account IDs across calls, got %+v vs %+v", a, b)
	}
	if a.Available != "user:p1:cash_available:USD" || a.Reserved != "user:p1:cash_reserved:USD" {
		t.Fatalf("unexpected account IDs: %+v", a)
	}
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/sdk/ -run TestWallet_EnsurePlayerAccounts_Idempotent -v`
Expected: PASS. If the second call fails because CreateAccount doesn't currently return AlreadyExists, the `ensureAccount` fallback (GetAccount check) handles it; the test verifies the helper still returns success.

- [ ] **Step 5: Commit**

```bash
git add pkg/dledger/wallet.go pkg/dledger/wallet_types.go internal/sdk/wallet_test.go
git commit -m "feat(sdk): Wallet base + EnsurePlayerAccounts"
```

---

## Task 12: `Wallet.Deposit` and `Wallet.Withdraw`

**Files:**
- Modify: `pkg/dledger/wallet.go`
- Modify: `internal/sdk/wallet_test.go`

- [ ] **Step 1: Add the Deposit and Withdraw methods**

Append to `pkg/dledger/wallet.go`:

```go
// Deposit credits the player's cash_available by debiting the FundingAccountID.
// FundingAccountID is the caller-owned mirror account (e.g. the payment
// processor's clearing account in dledger).
//
//	DEBIT  user:<player>:cash_available:<ccy>   amount
//	CREDIT funding_account                       amount
func (w *Wallet) Deposit(ctx context.Context, in DepositInput) (Receipt, error) {
	avail := w.accountID(in.PlayerID, "cash_available", in.Currency)
	return w.postJournal(ctx, postJournalArgs{
		IdempotencyKey: in.IdempotencyKey,
		SourceService:  in.SourceService,
		EventID:        in.ExternalRef,
		Debit:          accountAmount{accountID: avail, currency: in.Currency, amount: in.Amount},
		Credit:         accountAmount{accountID: in.FundingAccountID, currency: in.Currency, amount: in.Amount},
	})
}

// Withdraw moves funds from the player's cash_available to the caller's
// WithdrawalAccountID.
//
//	DEBIT  withdrawal_account                    amount
//	CREDIT user:<player>:cash_available:<ccy>   amount
func (w *Wallet) Withdraw(ctx context.Context, in WithdrawInput) (Receipt, error) {
	avail := w.accountID(in.PlayerID, "cash_available", in.Currency)
	return w.postJournal(ctx, postJournalArgs{
		IdempotencyKey: in.IdempotencyKey,
		SourceService:  in.SourceService,
		EventID:        in.ExternalRef,
		Debit:          accountAmount{accountID: in.WithdrawalAccountID, currency: in.Currency, amount: in.Amount},
		Credit:         accountAmount{accountID: avail, currency: in.Currency, amount: in.Amount},
	})
}

type accountAmount struct {
	accountID string
	currency  string
	amount    string
}

type postJournalArgs struct {
	IdempotencyKey string
	SourceService  string
	EventID        string
	Debit          accountAmount
	Credit         accountAmount
}

func (w *Wallet) postJournal(ctx context.Context, a postJournalArgs) (Receipt, error) {
	resp, err := w.client.PostJournal(ctx, connect.NewRequest(&v1.PostJournalRequest{
		TenantId: w.tenant, IdempotencyKey: a.IdempotencyKey, SourceService: a.SourceService,
		Journal: &v1.Journal{
			EventId:       a.EventID,
			SourceService: a.SourceService,
			Entries: []*v1.Entry{
				{AccountId: a.Debit.accountID, Currency: a.Debit.currency, Direction: v1.Direction_DIRECTION_DEBIT, Amount: a.Debit.amount},
				{AccountId: a.Credit.accountID, Currency: a.Credit.currency, Direction: v1.Direction_DIRECTION_CREDIT, Amount: a.Credit.amount},
			},
		},
	}))
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{JournalID: resp.Msg.GetJournalId(), FlowRunID: resp.Msg.GetFlowRunId()}, nil
}
```

- [ ] **Step 2: Add deposit + withdraw tests**

Append to `internal/sdk/wallet_test.go`:

```go
func TestWallet_Deposit_IncreasesAvailable(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "evt-dep-1",
		IdempotencyKey:   "dep-1",
		SourceService:    "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	bal, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: "user:p1:cash_available:USD", Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "100" {
		t.Fatalf("want available=100 got %q", got)
	}
}

func TestWallet_Withdraw_DecreasesAvailable(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, "t1", "platform", "0", "withdraw", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "evt-d", IdempotencyKey: "d", SourceService: "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	if _, err := w.Withdraw(ctx, dledger.WithdrawInput{
		PlayerID: "p1", Currency: "USD", Amount: "30",
		WithdrawalAccountID: "platform:0:withdraw:USD",
		ExternalRef:         "evt-w", IdempotencyKey: "w", SourceService: "payouts",
	}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	bal, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: "user:p1:cash_available:USD", Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "70" {
		t.Fatalf("want available=70 got %q", got)
	}
}
```

Make sure the top of `wallet_test.go` imports `ledgerv1` and `connect`:

```go
import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)
```

- [ ] **Step 3: Run the tests**

Run: `go test ./internal/sdk/ -run "TestWallet_Deposit|TestWallet_Withdraw" -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/dledger/wallet.go internal/sdk/wallet_test.go
git commit -m "feat(sdk): Wallet.Deposit and Wallet.Withdraw"
```

---

## Task 13: `Wallet.Reserve`, `Wallet.Commit`, `Wallet.Release`

**Files:**
- Modify: `pkg/dledger/wallet.go`
- Modify: `internal/sdk/wallet_test.go`

- [ ] **Step 1: Add the three methods**

Append to `pkg/dledger/wallet.go`:

```go
import (
	// add to the existing import block:
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// (Move these imports into the existing import group at top of file; the
// snippet above is illustrative.)

// Reserve places a HELD reservation over the player's cash_available.
func (w *Wallet) Reserve(ctx context.Context, in ReserveInput) (Reservation, error) {
	src := w.accountID(in.PlayerID, "cash_available", in.Currency)
	resv := w.accountID(in.PlayerID, "cash_reserved", in.Currency)
	req := &v1.CreateReservationRequest{
		TenantId: w.tenant, IdempotencyKey: in.IdempotencyKey,
		SourceAccountId: src, ReservedAccountId: resv,
		Currency: in.Currency, Amount: in.Amount,
		SourceService: in.SourceService,
	}
	if !in.ExpiresAt.IsZero() {
		req.ExpiresAt = timestamppb.New(in.ExpiresAt)
	}
	if len(in.Metadata) > 0 {
		md, err := structpb.NewStruct(in.Metadata)
		if err != nil {
			return Reservation{}, fmt.Errorf("metadata: %w", err)
		}
		req.Metadata = md
	}
	resp, err := w.client.CreateReservation(ctx, connect.NewRequest(req))
	if err != nil {
		return Reservation{}, err
	}
	return resvToSDK(resp.Msg.GetReservation()), nil
}

// Commit shifts the named amount from reserved to the caller's
// DestinationAccountID. The reservation may transition to PARTIAL or COMMITTED.
func (w *Wallet) Commit(ctx context.Context, in CommitInput) (Reservation, error) {
	resp, err := w.client.CommitReservation(ctx, connect.NewRequest(&v1.CommitReservationRequest{
		TenantId: w.tenant, ReservationId: in.ReservationID,
		DestinationAccountId: in.DestinationAccountID,
		Amount:               in.Amount,
		IdempotencyKey:       in.IdempotencyKey,
		SourceService:        in.SourceService,
	}))
	if err != nil {
		return Reservation{}, err
	}
	return resvToSDK(resp.Msg.GetReservation()), nil
}

// Release returns the named amount to the player's cash_available.
func (w *Wallet) Release(ctx context.Context, in ReleaseInput) (Reservation, error) {
	resp, err := w.client.ReleaseReservation(ctx, connect.NewRequest(&v1.ReleaseReservationRequest{
		TenantId: w.tenant, ReservationId: in.ReservationID,
		Amount: in.Amount, IdempotencyKey: in.IdempotencyKey,
		SourceService: in.SourceService,
	}))
	if err != nil {
		return Reservation{}, err
	}
	return resvToSDK(resp.Msg.GetReservation()), nil
}

func resvToSDK(p *v1.Reservation) Reservation {
	r := Reservation{
		ID: p.GetId(), Status: p.GetStatus(),
		OriginalAmount: p.GetOriginalAmount(), OutstandingAmount: p.GetOutstandingAmount(),
		CommittedAmount: p.GetCommittedAmount(), ReleasedAmount: p.GetReleasedAmount(),
	}
	if p.GetExpiresAt() != nil {
		r.ExpiresAt = p.GetExpiresAt().AsTime()
	}
	return r
}
```

- [ ] **Step 2: Add reserve/commit/release tests**

Append to `internal/sdk/wallet_test.go`:

```go
func TestWallet_Reserve_Commit_Release(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, "t1", "market", "42", "collateral_pool", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "ev", IdempotencyKey: "d", SourceService: "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	r, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID: "p1", Currency: "USD", Amount: "60",
		IdempotencyKey: "res-1", SourceService: "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if r.Status != "HELD" || r.OutstandingAmount != "60" {
		t.Fatalf("unexpected reservation: %+v", r)
	}

	r, err = w.Commit(ctx, dledger.CommitInput{
		ReservationID:        r.ID,
		DestinationAccountID: "market:42:collateral_pool:USD",
		Amount:               "25",
		IdempotencyKey:       "com-1",
		SourceService:        "matcher",
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if r.Status != "PARTIAL" || r.OutstandingAmount != "35" || r.CommittedAmount != "25" {
		t.Fatalf("unexpected after commit: %+v", r)
	}

	r, err = w.Release(ctx, dledger.ReleaseInput{
		ReservationID: r.ID, Amount: "35",
		IdempotencyKey: "rel-1", SourceService: "matcher",
	})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if r.Status != "RELEASED" || r.OutstandingAmount != "0" || r.ReleasedAmount != "35" {
		t.Fatalf("unexpected after release: %+v", r)
	}
}
```

- [ ] **Step 3: Run the test**

Run: `go test ./internal/sdk/ -run TestWallet_Reserve_Commit_Release -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/dledger/wallet.go internal/sdk/wallet_test.go
git commit -m "feat(sdk): Wallet.Reserve, Commit, Release"
```

---

## Task 14: `Wallet.Settle`

**Files:**
- Modify: `pkg/dledger/wallet.go`
- Modify: `internal/sdk/wallet_test.go`

- [ ] **Step 1: Add the method**

Append to `pkg/dledger/wallet.go`:

```go
// Settle credits a winner from a market collateral pool.
//
//	DEBIT  user:<winner>:cash_available:<ccy>   amount
//	CREDIT pool_account                          amount
func (w *Wallet) Settle(ctx context.Context, in SettleInput) (Receipt, error) {
	avail := w.accountID(in.PlayerID, "cash_available", in.Currency)
	return w.postJournal(ctx, postJournalArgs{
		IdempotencyKey: in.IdempotencyKey,
		SourceService:  in.SourceService,
		EventID:        in.ExternalRef,
		Debit:          accountAmount{accountID: avail, currency: in.Currency, amount: in.Amount},
		Credit:         accountAmount{accountID: in.PoolAccountID, currency: in.Currency, amount: in.Amount},
	})
}
```

- [ ] **Step 2: Add the test**

Append to `internal/sdk/wallet_test.go`:

```go
func TestWallet_Settle_PaysWinner(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "winner", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// Seed the pool with funds (credit-normal pool funded from a debit source).
	mustCreate(t, c, "t1", "market", "42", "collateral_pool", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	if _, err := c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-pool", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "seed-pool",
			Entries: []*ledgerv1.Entry{
				{AccountId: "market:42:collateral_pool:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "200"},
				{AccountId: "platform:0:funding:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "200"},
			},
		},
	})); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if _, err := w.Settle(ctx, dledger.SettleInput{
		PlayerID: "winner", Currency: "USD", Amount: "150",
		PoolAccountID:  "market:42:collateral_pool:USD",
		ExternalRef:    "resolution-1",
		IdempotencyKey: "settle-1",
		SourceService:  "market_resolver",
	}); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	bal, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: "user:winner:cash_available:USD", Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "150" {
		t.Fatalf("want 150 got %q", got)
	}
}
```

- [ ] **Step 3: Run the test**

Run: `go test ./internal/sdk/ -run TestWallet_Settle_PaysWinner -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/dledger/wallet.go internal/sdk/wallet_test.go
git commit -m "feat(sdk): Wallet.Settle"
```

---

## Task 15: `Wallet.GetWallet` (uses `ListReservations`)

**Files:**
- Modify: `pkg/dledger/wallet.go`
- Modify: `internal/sdk/wallet_test.go`

- [ ] **Step 1: Add the method**

Append to `pkg/dledger/wallet.go`:

```go
import (
	// add to existing import group:
	"github.com/shopspring/decimal"
)

// GetWallet returns a snapshot of the player's available/reserved balances
// plus any open reservations (HELD or PARTIAL).
func (w *Wallet) GetWallet(ctx context.Context, playerID, currency string) (WalletSnapshot, error) {
	avail, err := w.balance(ctx, w.accountID(playerID, "cash_available", currency), currency)
	if err != nil {
		return WalletSnapshot{}, fmt.Errorf("available: %w", err)
	}
	resv, err := w.balance(ctx, w.accountID(playerID, "cash_reserved", currency), currency)
	if err != nil {
		return WalletSnapshot{}, fmt.Errorf("reserved: %w", err)
	}

	open := make([]Reservation, 0)
	for _, status := range []string{"HELD", "PARTIAL"} {
		lr, err := w.client.ListReservations(ctx, connect.NewRequest(&v1.ListReservationsRequest{
			TenantId: w.tenant, OwnerType: w.ownerType, OwnerId: playerID, Status: status,
		}))
		if err != nil {
			return WalletSnapshot{}, fmt.Errorf("list reservations %s: %w", status, err)
		}
		for _, p := range lr.Msg.GetReservations() {
			if p.GetCurrency() != currency {
				continue
			}
			open = append(open, resvToSDK(p))
		}
	}

	return WalletSnapshot{
		PlayerID: playerID, Currency: currency,
		Available: avail, Reserved: resv, OpenReservations: open,
	}, nil
}

func (w *Wallet) balance(ctx context.Context, accountID, currency string) (decimal.Decimal, error) {
	resp, err := w.client.GetBalance(ctx, connect.NewRequest(&v1.GetBalanceRequest{
		TenantId: w.tenant, AccountId: accountID, Currency: currency,
	}))
	if err != nil {
		return decimal.Zero, err
	}
	d, err := decimal.NewFromString(resp.Msg.GetBalance().GetNormalized())
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse balance %q: %w", resp.Msg.GetBalance().GetNormalized(), err)
	}
	return d, nil
}
```

- [ ] **Step 2: Add the test**

Append to `internal/sdk/wallet_test.go`:

```go
func TestWallet_GetWallet_AvailableReservedAndOpen(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "ev", IdempotencyKey: "d", SourceService: "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	if _, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID: "p1", Currency: "USD", Amount: "40",
		IdempotencyKey: "r1", SourceService: "matcher",
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	snap, err := w.GetWallet(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("GetWallet: %v", err)
	}
	if snap.Available.String() != "60" {
		t.Fatalf("want available=60 got %s", snap.Available)
	}
	if snap.Reserved.String() != "40" {
		t.Fatalf("want reserved=40 got %s", snap.Reserved)
	}
	if len(snap.OpenReservations) != 1 {
		t.Fatalf("want 1 open reservation got %d", len(snap.OpenReservations))
	}
	if snap.OpenReservations[0].Status != "HELD" {
		t.Fatalf("want HELD got %s", snap.OpenReservations[0].Status)
	}
}
```

- [ ] **Step 3: Run the test**

Run: `go test ./internal/sdk/ -run TestWallet_GetWallet -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/dledger/wallet.go internal/sdk/wallet_test.go
git commit -m "feat(sdk): Wallet.GetWallet using ListReservations"
```

---

## Task 16: End-to-end `IsErrCode` test against both embedded and remote

**Files:**
- Create: `internal/sdk/errors_test.go`

- [ ] **Step 1: Write the cross-mode error test**

Create `internal/sdk/errors_test.go`:

```go
// internal/sdk/errors_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func setupForInsufficient(t *testing.T, c dledger.Client) {
	t.Helper()
	ctx := context.Background()
	w := dledger.NewWallet(c, "t1")
	if _, err := w.EnsurePlayerAccounts(ctx, "broke", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
}

// reserveTooMuch triggers INSUFFICIENT_FUNDS on a freshly-created cash_available
// (zero balance, allow_negative=false).
func reserveTooMuch(ctx context.Context, c dledger.Client) error {
	_, err := c.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId:        "t1",
		IdempotencyKey:  "boom",
		SourceAccountId: "user:broke:cash_available:USD",
		ReservedAccountId: "user:broke:cash_reserved:USD",
		Currency:        "USD",
		Amount:          "999999",
		SourceService:   "test",
	}))
	return err
}

func TestIsErrCode_EmbeddedAndRemote(t *testing.T) {
	ctx := context.Background()

	// Embedded
	emb, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "e.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer emb.Close()
	setupForInsufficient(t, emb)
	if err := reserveTooMuch(ctx, emb); err == nil || !dledger.IsErrCode(err, dledger.ErrInsufficientFunds) {
		t.Fatalf("embedded: want ErrInsufficientFunds, got %v", err)
	}

	// Remote (against the same backend pattern as remote_test.go)
	rem := newRemoteAgainstEmbeddedServer(t, "t1")
	defer rem.Close()
	setupForInsufficient(t, rem)
	if err := reserveTooMuch(ctx, rem); err == nil || !dledger.IsErrCode(err, dledger.ErrInsufficientFunds) {
		t.Fatalf("remote: want ErrInsufficientFunds, got %v", err)
	}
}
```

- [ ] **Step 2: Run the test**

Run: `go test ./internal/sdk/ -run TestIsErrCode_EmbeddedAndRemote -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/sdk/errors_test.go
git commit -m "test(sdk): IsErrCode works for embedded and remote modes"
```

---

## Task 17: Example walkthroughs (`examples/go/sdk_embedded` and `examples/go/sdk_remote`)

**Files:**
- Create: `examples/go/sdk_embedded/main.go`
- Create: `examples/go/sdk_remote/main.go`

- [ ] **Step 1: Write the embedded example**

Create `examples/go/sdk_embedded/main.go`:

```go
// SDK embedded mode walkthrough.
//
// Boots an in-process dledger backed by a SQLite file, then drives a
// player wallet through the typical deposit → reserve → commit cycle.
//
// Run:
//
//	go run ./examples/go/sdk_embedded
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "dledger-sdk-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dsn := filepath.Join(dir, "pam.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn,
	})
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	// Set up the funding account that mirrors the payment processor.
	if _, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "tipmarket", OwnerType: "platform", OwnerId: "0",
		AccountType: "stripe_cash", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	})); err != nil {
		log.Fatalf("funding account: %v", err)
	}

	// Set up a market collateral pool.
	if _, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "tipmarket", OwnerType: "market", OwnerId: "100",
		AccountType: "collateral_pool", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	})); err != nil {
		log.Fatalf("pool account: %v", err)
	}

	w := dledger.NewWallet(c, "tipmarket")
	accts, err := w.EnsurePlayerAccounts(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("ensure accounts: %v", err)
	}
	fmt.Printf("player accounts: %+v\n", accts)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "player-42", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:stripe_cash:USD",
		ExternalRef:      "stripe_ch_abc",
		IdempotencyKey:   "dep-1",
		SourceService:    "stripe",
	}); err != nil {
		log.Fatalf("Deposit: %v", err)
	}

	r, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID: "player-42", Currency: "USD", Amount: "40",
		IdempotencyKey: "res-1", SourceService: "matcher",
	})
	if err != nil {
		log.Fatalf("Reserve: %v", err)
	}
	fmt.Printf("reserved: %s outstanding=%s\n", r.ID, r.OutstandingAmount)

	if _, err := w.Commit(ctx, dledger.CommitInput{
		ReservationID:        r.ID,
		DestinationAccountID: "market:100:collateral_pool:USD",
		Amount:               "40",
		IdempotencyKey:       "com-1",
		SourceService:        "matcher",
	}); err != nil {
		log.Fatalf("Commit: %v", err)
	}

	snap, err := w.GetWallet(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("GetWallet: %v", err)
	}
	fmt.Printf("wallet: available=%s reserved=%s open=%d\n",
		snap.Available, snap.Reserved, len(snap.OpenReservations))
}
```

- [ ] **Step 2: Write the remote example**

Create `examples/go/sdk_remote/main.go`:

```go
// SDK remote mode walkthrough.
//
// Identical Wallet code path to sdk_embedded; only the constructor differs.
// Run a server first (see ../place_order/main.go for instructions):
//
//	go run ./cmd/server --backend=sqlite --dsn=./ledger.db
//	go run ./examples/go/sdk_remote
package main

import (
	"context"
	"fmt"
	"log"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

const (
	serverURL = "http://localhost:8080"
	tenantID  = "tipmarket"
)

func main() {
	ctx := context.Background()
	c := dledger.NewRemote(serverURL, tenantID)
	defer c.Close()

	// Funding + pool accounts.
	for _, in := range []*ledgerv1.CreateAccountRequest{
		{TenantId: tenantID, OwnerType: "platform", OwnerId: "0",
			AccountType: "stripe_cash", Currency: "USD",
			NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT},
		{TenantId: tenantID, OwnerType: "market", OwnerId: "100",
			AccountType: "collateral_pool", Currency: "USD",
			NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT},
	} {
		if _, err := c.CreateAccount(ctx, connect.NewRequest(in)); err != nil {
			log.Printf("create %s:%s (ignoring if already exists): %v",
				in.GetOwnerType(), in.GetAccountType(), err)
		}
	}

	w := dledger.NewWallet(c, tenantID)
	if _, err := w.EnsurePlayerAccounts(ctx, "player-42", "USD"); err != nil {
		log.Fatalf("ensure: %v", err)
	}
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "player-42", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:stripe_cash:USD",
		ExternalRef:      "stripe_ch_remote",
		IdempotencyKey:   "dep-remote-1",
		SourceService:    "stripe",
	}); err != nil {
		log.Fatalf("Deposit: %v", err)
	}
	snap, err := w.GetWallet(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("GetWallet: %v", err)
	}
	fmt.Printf("wallet via remote: available=%s reserved=%s\n", snap.Available, snap.Reserved)
}
```

- [ ] **Step 3: Build the embedded example to verify it compiles and runs**

Run: `go run ./examples/go/sdk_embedded`
Expected: prints `player accounts: ...`, `reserved: ...`, and `wallet: available=60 reserved=0 open=0` (committed all 40 to the pool, so cash_reserved is back to 0).

- [ ] **Step 4: Build (only) the remote example**

Run: `go build ./examples/go/sdk_remote`
Expected: PASS. We don't run it because it requires a live server.

- [ ] **Step 5: Commit**

```bash
git add examples/go/sdk_embedded examples/go/sdk_remote
git commit -m "docs(sdk): embedded + remote example walkthroughs"
```

---

## Task 18: Architecture doc update + final integration check

**Files:**
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: Add an SDK section to `docs/ARCHITECTURE.md`**

Open `docs/ARCHITECTURE.md` and append (or insert above any "Examples" section if one already exists) the following section:

```markdown
## Public SDK (`pkg/dledger`)

The Go SDK lets a consumer microservice — initially the tipmarket PAM — pick
between two modes with the same call site:

- `NewEmbedded(ctx, Options)`: opens an in-process ledger. Owns the DB
  connection, runs the snapshot/expiry/retention scheduler, and starts the
  outbox dispatcher. Goose migrations are embedded into the binary via
  `internal/sdk/migrations.go` so the SDK is fully self-contained.
- `NewRemote(serverURL, tenantID, opts...)`: returns a Connect-RPC client
  with an `X-Tenant-Id` round-tripper.

Both implementations satisfy the same `Client` interface (the 22 RPCs plus
`Close`). The `Wallet` helper wraps the prediction-market primitives —
Deposit, Reserve, Commit, Release, Settle, Withdraw, GetWallet — and takes
funding/destination/pool/withdrawal account IDs per call. The SDK never
invents an accounting policy: callers own the chart of accounts.

Errors are surfaced as `*connect.Error` with a `ledger-error-code` header.
`dledger.IsErrCode(err, code)` matches against the typed `ErrCode`
constants and works for both modes.
```

- [ ] **Step 2: Run the full test suite**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 3: Run the linter**

Run: `golangci-lint run ./...`
Expected: 0 issues.

- [ ] **Step 4: Build everything (binaries + examples)**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs: SDK section in ARCHITECTURE"
```

---

## Self-review notes

**Spec coverage:**
- §2 package layout → Tasks 5–15.
- §3 Client interface (22 RPCs + Close) → Task 5.
- §4 Options/MigrateMode + NewEmbedded → Tasks 5, 7, 8.
- §4 NewRemote with tenant transport → Task 10.
- §5 Wallet helper (Deposit/Reserve/Commit/Release/Settle/Withdraw/GetWallet/EnsurePlayerAccounts) → Tasks 11–15.
- §6 ErrCode + IsErrCode → Task 6 + Task 16.
- §7 lifecycle (background ctx, Close cancels) → Task 8 + Task 9.
- §8 concurrency: implicit in tests (idempotency, retries already handled by `*service.Server`).
- §9 tests: every listed test maps to Tasks 9, 11, 12, 13, 14, 15, 16.
- §10 ListReservations → Tasks 1–4.
- §11 outbox events: unchanged (no new events).
- §12 examples → Task 17.
- §13 acceptance criteria → verified by Tasks 17–18.

**Known caveat — `ensureAccount` "already exists" detection:** the codebase
does not currently surface a typed `ALREADY_EXISTS` for primary-key
conflicts on accounts; the helper falls back to a `GetAccount` probe to
treat that case as success. If the server later adds a typed code, simplify
`ensureAccount` accordingly.

**Known caveat — embed symlinks:** `//go:embed` of files reached only via
symlink fails on some Go toolchains. Task 7 documents the fallback (`cp -R`
instead of `ln -s`) plus a Makefile target if maintainers prefer to keep
the canonical copies under `sql/migrations/`.
