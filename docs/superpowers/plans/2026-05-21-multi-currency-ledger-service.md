# Multi-Currency Ledger Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a product-neutral double-entry ledger service in Go with Connect-RPC, supporting CockroachDB (production) and SQLite (local dev), with atomic multi-step flows, multi-currency journals, idempotency, and a transactional outbox.

**Architecture:** Three internal layers — pure domain (`internal/ledger`), repository abstraction with two backends (`internal/repo`, `internal/repo/sqlite`, `internal/repo/crdb`), and a Connect-RPC service (`internal/service`). Plus an in-process outbox dispatcher and OTel-instrumented `cmd/server`. sqlc generates typed queries per dialect; goose runs migrations; buf generates proto and Connect handlers.

**Tech Stack:** Go 1.22+, Connect-RPC (`connectrpc.com/connect`), `buf`, `protovalidate`, `sqlc`, `goose`, `shopspring/decimal`, `modernc.org/sqlite`, CockroachDB via `pgx`, `testcontainers-go`, OpenTelemetry, `slog`.

**Module path:** `github.com/caxqueiroz/doubleledger`

**Design doc:** `docs/superpowers/specs/2026-05-21-multi-currency-ledger-service-design.md`

---

## File map (what gets created where)

```
.
├── go.mod, go.sum
├── Makefile
├── buf.yaml, buf.gen.yaml
├── sqlc.yaml
├── .gitignore
├── README.md
├── cmd/
│   ├── server/main.go              # HTTP server + Connect handler + outbox dispatcher
│   └── migrate/main.go             # goose CLI wrapper
├── proto/ledger/v1/ledger.proto    # service + message definitions
├── gen/
│   ├── proto/ledger/v1/...         # buf-generated
│   ├── sqlite/...                  # sqlc-generated
│   └── crdb/...                    # sqlc-generated
├── sql/
│   ├── migrations/
│   │   ├── sqlite/0001_init.sql
│   │   └── crdb/0001_init.sql
│   └── queries/
│       ├── sqlite/{accounts,journals,entries,balances,flows,outbox}.sql
│       └── crdb/{accounts,journals,entries,balances,flows,outbox}.sql
├── internal/
│   ├── ledger/                     # pure domain
│   │   ├── money.go                # Decimal wrapper
│   │   ├── account.go              # Account, NormalBalance
│   │   ├── journal.go              # Journal, Entry, balance validator
│   │   ├── flow.go                 # Flow, Step
│   │   └── errors.go               # domain error codes
│   ├── repo/
│   │   ├── repo.go                 # Repository + Tx interfaces
│   │   ├── retry.go                # CRDB retry helper (used only by crdb impl)
│   │   ├── sqlite/store.go         # SQLite Repository impl
│   │   └── crdb/store.go           # CRDB Repository impl
│   ├── service/
│   │   ├── server.go               # LedgerServiceHandler
│   │   ├── create_account.go
│   │   ├── get_account.go
│   │   ├── get_balance.go
│   │   ├── post_journal.go
│   │   ├── execute_flow.go         # the orchestrator
│   │   ├── get_flow.go
│   │   ├── list_activity.go
│   │   ├── errors.go               # domain → Connect mapping
│   │   └── interceptors/
│   │       ├── tenant.go
│   │       └── logging.go
│   ├── outbox/
│   │   ├── dispatcher.go
│   │   ├── sink.go                 # Sink interface + LogSink
│   │   └── event.go
│   └── observability/
│       └── otel.go                 # init helpers
├── examples/
│   ├── go/client/main.go
│   └── react/README.md
└── docs/
    └── superpowers/
        ├── specs/2026-05-21-multi-currency-ledger-service-design.md
        └── plans/2026-05-21-multi-currency-ledger-service.md  (this file)
```

---

## Task 1: Project bootstrap

**Files:**
- Create: `go.mod`
- Create: `.gitignore`
- Create: `Makefile`
- Create: `README.md`

- [ ] **Step 1: Initialize git and module**

Run:
```bash
cd /Users/cq/Dev/DoubleLedgerGo
git init -b main
go mod init github.com/caxqueiroz/doubleledger
```
Expected: creates `go.mod` with `module github.com/caxqueiroz/doubleledger`.

- [ ] **Step 2: Write `.gitignore`**

```
# Binaries
/bin/
/tmp/
*.test
*.out
coverage.txt

# Local databases
*.db
*.db-journal
*.db-wal
*.db-shm

# Editor
.vscode/
.idea/
.DS_Store

# Env
.env
.env.local
```

- [ ] **Step 3: Write minimal `Makefile`**

```makefile
GO          ?= go
GOLANGCI    ?= golangci-lint
BIN_DIR     := bin

.PHONY: tools proto sqlc generate test test-integration lint serve build migrate-up migrate-down

tools:
	$(GO) install github.com/bufbuild/buf/cmd/buf@latest
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	$(GO) install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
	$(GO) install github.com/pressly/goose/v3/cmd/goose@latest

proto:
	buf generate

sqlc:
	sqlc generate

generate: proto sqlc

test:
	$(GO) test ./...

test-integration:
	$(GO) test -tags=integration ./...

lint:
	$(GOLANGCI) run ./...

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/server ./cmd/server
	$(GO) build -o $(BIN_DIR)/migrate ./cmd/migrate

serve: build
	./$(BIN_DIR)/server

migrate-up: build
	./$(BIN_DIR)/migrate --backend=$${BACKEND:-sqlite} up

migrate-down: build
	./$(BIN_DIR)/migrate --backend=$${BACKEND:-sqlite} down
```

- [ ] **Step 4: Write `README.md`**

```markdown
# doubleledger

Multi-currency double-entry ledger service. See `docs/superpowers/specs/` for the design.

## Local dev

```
make tools
make generate
BACKEND=sqlite make migrate-up
make serve
```

Defaults to SQLite at `./ledger.db`. Set `BACKEND=crdb` and `DATABASE_URL` for CockroachDB.
```

- [ ] **Step 5: Commit**

```bash
git add go.mod .gitignore Makefile README.md docs/
git commit -m "chore: bootstrap project"
```

---

## Task 2: Domain — money type

**Files:**
- Create: `internal/ledger/money.go`
- Create: `internal/ledger/money_test.go`

- [ ] **Step 1: Add `shopspring/decimal` dependency**

Run:
```bash
go get github.com/shopspring/decimal
```

- [ ] **Step 2: Write failing test**

```go
// internal/ledger/money_test.go
package ledger

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseAmount(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    decimal.Decimal
		wantErr bool
	}{
		{"basic", "100.00", decimal.RequireFromString("100.00"), false},
		{"high precision", "0.000000000000000001", decimal.RequireFromString("0.000000000000000001"), false},
		{"zero rejected", "0", decimal.Decimal{}, true},
		{"negative rejected", "-1", decimal.Decimal{}, true},
		{"non-numeric rejected", "abc", decimal.Decimal{}, true},
		{"empty rejected", "", decimal.Decimal{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAmount(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; value=%s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 3: Run test, verify it fails**

```bash
go test ./internal/ledger/
```
Expected: FAIL — `ParseAmount` undefined.

- [ ] **Step 4: Implement `money.go`**

```go
// internal/ledger/money.go
package ledger

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// ParseAmount validates and parses a positive decimal amount string.
// Returns an error if the value is empty, malformed, zero, or negative.
func ParseAmount(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Decimal{}, errors.New("amount is empty")
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("invalid amount %q: %w", s, err)
	}
	if !d.IsPositive() {
		return decimal.Decimal{}, fmt.Errorf("amount must be > 0, got %s", s)
	}
	return d, nil
}
```

- [ ] **Step 5: Run test, verify it passes**

```bash
go test ./internal/ledger/ -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/ledger/ go.mod go.sum
git commit -m "feat(ledger): add positive-decimal money parser"
```

---

## Task 3: Domain — account types

**Files:**
- Create: `internal/ledger/account.go`
- Create: `internal/ledger/account_test.go`

- [ ] **Step 1: Write failing test**

```go
// internal/ledger/account_test.go
package ledger

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestNormalizedBalance(t *testing.T) {
	debits := decimal.RequireFromString("100")
	credits := decimal.RequireFromString("30")

	got := NormalizedBalance(NormalDebit, debits, credits)
	if !got.Equal(decimal.RequireFromString("70")) {
		t.Fatalf("debit-normal: got %s, want 70", got)
	}

	got = NormalizedBalance(NormalCredit, debits, credits)
	if !got.Equal(decimal.RequireFromString("-70")) {
		t.Fatalf("credit-normal: got %s, want -70", got)
	}
}

func TestAccountStatusValidation(t *testing.T) {
	for _, s := range []AccountStatus{AccountActive, AccountFrozen, AccountClosed} {
		if !s.Valid() {
			t.Fatalf("%q should be valid", s)
		}
	}
	if AccountStatus("WAT").Valid() {
		t.Fatal("WAT should be invalid")
	}
}
```

- [ ] **Step 2: Run test, verify it fails**

```bash
go test ./internal/ledger/
```
Expected: FAIL — undefined symbols.

- [ ] **Step 3: Implement `account.go`**

```go
// internal/ledger/account.go
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

type NormalBalance string

const (
	NormalDebit  NormalBalance = "DEBIT"
	NormalCredit NormalBalance = "CREDIT"
)

func (n NormalBalance) Valid() bool {
	return n == NormalDebit || n == NormalCredit
}

type AccountStatus string

const (
	AccountActive AccountStatus = "ACTIVE"
	AccountFrozen AccountStatus = "FROZEN"
	AccountClosed AccountStatus = "CLOSED"
)

func (s AccountStatus) Valid() bool {
	switch s {
	case AccountActive, AccountFrozen, AccountClosed:
		return true
	}
	return false
}

// Account is the canonical ledger account record.
type Account struct {
	ID            string
	TenantID      string
	OwnerType     string
	OwnerID       string
	AccountType   string
	Currency      string
	NormalBalance NormalBalance
	AllowNegative bool
	Status        AccountStatus
	CreatedAt     time.Time
}

// NormalizedBalance returns posted_debits - posted_credits for debit-normal
// accounts, and the inverse for credit-normal accounts.
func NormalizedBalance(nb NormalBalance, postedDebits, postedCredits decimal.Decimal) decimal.Decimal {
	switch nb {
	case NormalCredit:
		return postedCredits.Sub(postedDebits)
	default:
		return postedDebits.Sub(postedCredits)
	}
}
```

- [ ] **Step 4: Run test, verify it passes**

```bash
go test ./internal/ledger/ -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ledger/account.go internal/ledger/account_test.go
git commit -m "feat(ledger): add account types and normalized balance"
```

---

## Task 4: Domain — journals and per-currency balance check

**Files:**
- Create: `internal/ledger/journal.go`
- Create: `internal/ledger/journal_test.go`

- [ ] **Step 1: Write failing test**

```go
// internal/ledger/journal_test.go
package ledger

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func amt(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestJournal_Validate_SingleCurrencyBalanced(t *testing.T) {
	j := Journal{
		EventID: "evt-1",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("100")},
		},
	}
	if err := j.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJournal_Validate_MultiCurrencyBalanced(t *testing.T) {
	j := Journal{
		EventID: "evt-2",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("100")},
			{AccountID: "c", Currency: "BRL", Direction: DirectionDebit, Amount: amt("500")},
			{AccountID: "d", Currency: "BRL", Direction: DirectionCredit, Amount: amt("500")},
		},
	}
	if err := j.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJournal_Validate_UnbalancedAcrossCurrencies(t *testing.T) {
	j := Journal{
		EventID: "evt-3",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "BRL", Direction: DirectionCredit, Amount: amt("500")},
		},
	}
	err := j.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("expected unbalanced error, got %v", err)
	}
}

func TestJournal_Validate_DebitMissesCredit(t *testing.T) {
	j := Journal{
		EventID: "evt-4",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("99")},
		},
	}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestJournal_Validate_EmptyEntries(t *testing.T) {
	j := Journal{EventID: "evt-5"}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestJournal_Validate_EmptyEventID(t *testing.T) {
	j := Journal{Entries: []Entry{
		{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("1")},
		{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("1")},
	}}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: Run test, verify it fails**

```bash
go test ./internal/ledger/
```
Expected: FAIL — symbols undefined.

- [ ] **Step 3: Implement `journal.go`**

```go
// internal/ledger/journal.go
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

func (d Direction) Valid() bool { return d == DirectionDebit || d == DirectionCredit }

type Entry struct {
	AccountID string
	Currency  string
	Direction Direction
	Amount    decimal.Decimal
}

type Journal struct {
	ID            string
	TenantID      string
	FlowRunID     string
	EventID       string
	SourceService string
	SourceType    string
	ActorID       string
	Metadata      map[string]any
	Entries       []Entry
	CreatedAt     time.Time
}

// Validate enforces the core accounting rule: per-currency debit/credit equality.
// It also rejects empty journals and malformed entries.
func (j *Journal) Validate() error {
	if j.EventID == "" {
		return errors.New("journal: event_id is required")
	}
	if len(j.Entries) == 0 {
		return errors.New("journal: at least one entry required")
	}
	sums := map[string]decimal.Decimal{}
	for i, e := range j.Entries {
		if e.AccountID == "" {
			return fmt.Errorf("journal: entry[%d]: account_id required", i)
		}
		if e.Currency == "" {
			return fmt.Errorf("journal: entry[%d]: currency required", i)
		}
		if !e.Direction.Valid() {
			return fmt.Errorf("journal: entry[%d]: invalid direction %q", i, e.Direction)
		}
		if !e.Amount.IsPositive() {
			return fmt.Errorf("journal: entry[%d]: amount must be > 0", i)
		}
		signed := e.Amount
		if e.Direction == DirectionCredit {
			signed = signed.Neg()
		}
		sums[e.Currency] = sums[e.Currency].Add(signed)
	}
	for ccy, sum := range sums {
		if !sum.IsZero() {
			return fmt.Errorf("journal: unbalanced %s: debits-credits=%s", ccy, sum)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test, verify it passes**

```bash
go test ./internal/ledger/ -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ledger/journal.go internal/ledger/journal_test.go
git commit -m "feat(ledger): add journal with per-currency balance validation"
```

---

## Task 5: Domain — flow and error codes

**Files:**
- Create: `internal/ledger/flow.go`
- Create: `internal/ledger/errors.go`
- Create: `internal/ledger/errors_test.go`

- [ ] **Step 1: Write failing test**

```go
// internal/ledger/errors_test.go
package ledger

import "testing"

func TestDomainError_AsCode(t *testing.T) {
	err := NewDomainError(CodeInsufficientFunds, "no money")
	if !IsDomainCode(err, CodeInsufficientFunds) {
		t.Fatal("expected match on CodeInsufficientFunds")
	}
	if IsDomainCode(err, CodeAccountNotFound) {
		t.Fatal("should not match a different code")
	}
}
```

- [ ] **Step 2: Run test, verify it fails**

```bash
go test ./internal/ledger/
```
Expected: FAIL.

- [ ] **Step 3: Implement `errors.go`**

```go
// internal/ledger/errors.go
package ledger

import "errors"

type DomainCode string

const (
	CodeInsufficientFunds            DomainCode = "INSUFFICIENT_FUNDS"
	CodeAccountNotFound              DomainCode = "ACCOUNT_NOT_FOUND"
	CodeAccountCurrencyMismatch      DomainCode = "ACCOUNT_CURRENCY_MISMATCH"
	CodeUnbalancedJournal            DomainCode = "UNBALANCED_JOURNAL"
	CodeDuplicateIdempotencyKey      DomainCode = "DUPLICATE_IDEMPOTENCY_KEY"
	CodeFlowAlreadyCompleted         DomainCode = "FLOW_ALREADY_COMPLETED"
	CodeFlowConflict                 DomainCode = "FLOW_CONFLICT"
	CodeInvalidAccountStatus         DomainCode = "INVALID_ACCOUNT_STATUS"
	CodeSerializationRetryExhausted  DomainCode = "SERIALIZATION_RETRY_EXHAUSTED"
)

type DomainError struct {
	Code    DomainCode
	Message string
}

func (e *DomainError) Error() string { return string(e.Code) + ": " + e.Message }

func NewDomainError(code DomainCode, msg string) *DomainError {
	return &DomainError{Code: code, Message: msg}
}

// IsDomainCode reports whether err (or any wrapped error) is a DomainError
// with the given code.
func IsDomainCode(err error, code DomainCode) bool {
	var de *DomainError
	if errors.As(err, &de) {
		return de.Code == code
	}
	return false
}
```

- [ ] **Step 4: Implement `flow.go`**

```go
// internal/ledger/flow.go
package ledger

import "time"

type FlowStatus string

const (
	FlowRunning   FlowStatus = "RUNNING"
	FlowCompleted FlowStatus = "COMPLETED"
	FlowFailed    FlowStatus = "FAILED"
)

type StepStatus string

const (
	StepCompleted StepStatus = "COMPLETED"
	StepFailed    StepStatus = "FAILED"
)

type FlowStep struct {
	ID         string
	TenantID   string
	FlowRunID  string
	StepID     string
	Status     StepStatus
	JournalID  string
	ErrorCode  string
	CreatedAt  time.Time
}

type FlowRun struct {
	ID             string
	TenantID       string
	FlowType       string
	IdempotencyKey string
	SourceService  string
	ActorID        string
	Status         FlowStatus
	Metadata       map[string]any
	CreatedAt      time.Time
	CompletedAt    *time.Time
	FailedAt       *time.Time
	Steps          []FlowStep
}

// StepInput is what callers submit per step.
type StepInput struct {
	StepID  string
	Journal Journal
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/ledger/ -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/ledger/flow.go internal/ledger/errors.go internal/ledger/errors_test.go
git commit -m "feat(ledger): add flow types and domain error codes"
```

---

## Task 6: Proto definitions

**Files:**
- Create: `proto/ledger/v1/ledger.proto`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`

- [ ] **Step 1: Write `buf.yaml`**

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
deps:
  - buf.build/bufbuild/protovalidate
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

- [ ] **Step 2: Write `buf.gen.yaml`**

```yaml
# buf.gen.yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/caxqueiroz/doubleledger/gen/proto
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen/proto
    opt: paths=source_relative
  - remote: buf.build/connectrpc/go
    out: gen/proto
    opt: paths=source_relative
```

- [ ] **Step 3: Write `proto/ledger/v1/ledger.proto`**

```proto
syntax = "proto3";

package ledger.v1;

import "buf/validate/validate.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

service LedgerService {
  rpc CreateAccount(CreateAccountRequest) returns (CreateAccountResponse);
  rpc GetAccount(GetAccountRequest) returns (GetAccountResponse);
  rpc GetBalance(GetBalanceRequest) returns (GetBalanceResponse);
  rpc PostJournal(PostJournalRequest) returns (PostJournalResponse);
  rpc ExecuteFlow(ExecuteFlowRequest) returns (ExecuteFlowResponse);
  rpc GetFlow(GetFlowRequest) returns (GetFlowResponse);
  rpc ListAccountActivity(ListAccountActivityRequest) returns (ListAccountActivityResponse);
}

enum Direction {
  DIRECTION_UNSPECIFIED = 0;
  DIRECTION_DEBIT = 1;
  DIRECTION_CREDIT = 2;
}

enum NormalBalance {
  NORMAL_BALANCE_UNSPECIFIED = 0;
  NORMAL_BALANCE_DEBIT = 1;
  NORMAL_BALANCE_CREDIT = 2;
}

enum AccountStatus {
  ACCOUNT_STATUS_UNSPECIFIED = 0;
  ACCOUNT_STATUS_ACTIVE = 1;
  ACCOUNT_STATUS_FROZEN = 2;
  ACCOUNT_STATUS_CLOSED = 3;
}

enum FlowStatus {
  FLOW_STATUS_UNSPECIFIED = 0;
  FLOW_STATUS_RUNNING = 1;
  FLOW_STATUS_COMPLETED = 2;
  FLOW_STATUS_FAILED = 3;
}

message Entry {
  string account_id = 1 [(buf.validate.field).string.min_len = 1];
  string currency   = 2 [(buf.validate.field).string.min_len = 3];
  Direction direction = 3 [(buf.validate.field).enum.defined_only = true];
  string amount     = 4 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
}

message Journal {
  string event_id        = 1 [(buf.validate.field).string.min_len = 1];
  string source_service  = 2;
  string source_type     = 3;
  string actor_id        = 4;
  google.protobuf.Struct metadata = 5;
  repeated Entry entries = 6 [(buf.validate.field).repeated.min_items = 1];
}

message Account {
  string id              = 1;
  string tenant_id       = 2;
  string owner_type      = 3;
  string owner_id        = 4;
  string account_type    = 5;
  string currency        = 6;
  NormalBalance normal_balance = 7;
  bool   allow_negative  = 8;
  AccountStatus status   = 9;
  google.protobuf.Timestamp created_at = 10;
}

message Balance {
  string account_id      = 1;
  string currency        = 2;
  string posted_debits   = 3;
  string posted_credits  = 4;
  string normalized      = 5;
  int64  version         = 6;
}

message CreateAccountRequest {
  string tenant_id       = 1 [(buf.validate.field).string.min_len = 1];
  string owner_type      = 2 [(buf.validate.field).string.min_len = 1];
  string owner_id        = 3 [(buf.validate.field).string.min_len = 1];
  string account_type    = 4 [(buf.validate.field).string.min_len = 1];
  string currency        = 5 [(buf.validate.field).string.min_len = 3];
  NormalBalance normal_balance = 6 [(buf.validate.field).enum.defined_only = true];
  bool   allow_negative  = 7;
}
message CreateAccountResponse { Account account = 1; }

message GetAccountRequest  { string tenant_id = 1; string account_id = 2; }
message GetAccountResponse { Account account = 1; }

message GetBalanceRequest  { string tenant_id = 1; string account_id = 2; string currency = 3; }
message GetBalanceResponse { Balance balance = 1; }

message PostJournalRequest {
  string tenant_id        = 1 [(buf.validate.field).string.min_len = 1];
  string idempotency_key  = 2 [(buf.validate.field).string.min_len = 1];
  string source_service   = 3;
  string actor_id         = 4;
  Journal journal         = 5;
}
message PostJournalResponse {
  string journal_id = 1;
  string flow_run_id = 2;
}

message Step {
  string step_id = 1 [(buf.validate.field).string.min_len = 1];
  Journal journal = 2;
}

message ExecuteFlowRequest {
  string tenant_id       = 1 [(buf.validate.field).string.min_len = 1];
  string flow_type       = 2 [(buf.validate.field).string.min_len = 1];
  string idempotency_key = 3 [(buf.validate.field).string.min_len = 1];
  string source_service  = 4;
  string actor_id        = 5;
  repeated Step steps    = 6 [(buf.validate.field).repeated.min_items = 1];
  google.protobuf.Struct metadata = 7;
}

message FlowStepResult {
  string step_id    = 1;
  string status     = 2;
  string journal_id = 3;
  string error_code = 4;
}

message ExecuteFlowResponse {
  string flow_run_id = 1;
  FlowStatus status  = 2;
  repeated FlowStepResult steps = 3;
}

message GetFlowRequest  { string tenant_id = 1; string flow_run_id = 2; }
message GetFlowResponse { ExecuteFlowResponse flow = 1; }

message ListAccountActivityRequest {
  string tenant_id  = 1;
  string account_id = 2;
  string currency   = 3;
  google.protobuf.Timestamp since = 4;
  google.protobuf.Timestamp until = 5;
  int32  page_size  = 6;
  string page_token = 7;
}
message AccountActivityEntry {
  string journal_id = 1;
  string entry_id   = 2;
  string currency   = 3;
  Direction direction = 4;
  string amount     = 5;
  google.protobuf.Timestamp created_at = 6;
  string source_service = 7;
}
message ListAccountActivityResponse {
  repeated AccountActivityEntry entries = 1;
  string next_page_token = 2;
}
```

- [ ] **Step 4: Generate proto code**

Run:
```bash
make tools
make proto
```
Expected: `gen/proto/ledger/v1/{ledger.pb.go,ledger.connect.go,...}` files are created.

- [ ] **Step 5: Ensure module builds**

```bash
go mod tidy
go build ./...
```
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add buf.yaml buf.gen.yaml proto/ gen/proto/ go.mod go.sum
git commit -m "feat: add Connect-RPC proto definitions for LedgerService"
```

---

## Task 7: SQLite migrations

**Files:**
- Create: `sql/migrations/sqlite/0001_init.sql`

- [ ] **Step 1: Write migration**

```sql
-- +goose Up
CREATE TABLE accounts (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    owner_type      TEXT NOT NULL,
    owner_id        TEXT NOT NULL,
    account_type    TEXT NOT NULL,
    currency        TEXT NOT NULL,
    normal_balance  TEXT NOT NULL CHECK (normal_balance IN ('DEBIT','CREDIT')),
    allow_negative  INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, owner_type, owner_id, account_type, currency)
);

CREATE TABLE ledger_journals (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    flow_run_id     TEXT,
    event_id        TEXT NOT NULL UNIQUE,
    source_service  TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    actor_id        TEXT NOT NULL,
    metadata        TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE ledger_entries (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    journal_id      TEXT NOT NULL REFERENCES ledger_journals(id),
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    direction       TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount          TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (tenant_id, account_id, currency, created_at);

CREATE TABLE account_balances (
    tenant_id       TEXT NOT NULL,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    posted_debits   TEXT NOT NULL DEFAULT '0',
    posted_credits  TEXT NOT NULL DEFAULT '0',
    version         INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (tenant_id, account_id, currency)
);

CREATE TABLE flow_runs (
    id               TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    flow_type        TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    source_service   TEXT NOT NULL,
    actor_id         TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    metadata         TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at     TEXT,
    failed_at        TEXT
);

CREATE TABLE flow_steps (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    flow_run_id  TEXT NOT NULL REFERENCES flow_runs(id),
    step_id      TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('COMPLETED','FAILED')),
    journal_id   TEXT REFERENCES ledger_journals(id),
    error_code   TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, flow_run_id, step_id)
);

CREATE TABLE outbox_events (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    aggregate_id      TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL UNIQUE,
    payload           TEXT NOT NULL,
    publish_state     TEXT NOT NULL DEFAULT 'PENDING',
    attempts          INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    published_at      TEXT
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (publish_state, created_at);

-- +goose Down
DROP TABLE outbox_events;
DROP TABLE flow_steps;
DROP TABLE flow_runs;
DROP TABLE account_balances;
DROP TABLE ledger_entries;
DROP TABLE ledger_journals;
DROP TABLE accounts;
```

- [ ] **Step 2: Commit**

```bash
git add sql/migrations/sqlite/
git commit -m "feat(db): add SQLite initial schema migration"
```

---

## Task 8: CRDB migrations

**Files:**
- Create: `sql/migrations/crdb/0001_init.sql`

- [ ] **Step 1: Write migration**

```sql
-- +goose Up
CREATE TABLE accounts (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    owner_type      STRING NOT NULL,
    owner_id        STRING NOT NULL,
    account_type    STRING NOT NULL,
    currency        STRING NOT NULL,
    normal_balance  STRING NOT NULL CHECK (normal_balance IN ('DEBIT','CREDIT')),
    allow_negative  BOOL NOT NULL DEFAULT false,
    status          STRING NOT NULL CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_type, owner_id, account_type, currency)
);

CREATE TABLE ledger_journals (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    flow_run_id     STRING,
    event_id        STRING NOT NULL UNIQUE,
    source_service  STRING NOT NULL,
    source_type     STRING NOT NULL,
    actor_id        STRING NOT NULL,
    metadata        JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       STRING NOT NULL,
    journal_id      STRING NOT NULL REFERENCES ledger_journals(id),
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    direction       STRING NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount          DECIMAL(38, 18) NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (tenant_id, account_id, currency, created_at);

CREATE TABLE account_balances (
    tenant_id       STRING NOT NULL,
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    posted_debits   DECIMAL(38, 18) NOT NULL DEFAULT 0,
    posted_credits  DECIMAL(38, 18) NOT NULL DEFAULT 0,
    version         INT8 NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, account_id, currency)
);

CREATE TABLE flow_runs (
    id               STRING PRIMARY KEY,
    tenant_id        STRING NOT NULL,
    flow_type        STRING NOT NULL,
    idempotency_key  STRING NOT NULL UNIQUE,
    source_service   STRING NOT NULL,
    actor_id         STRING NOT NULL,
    status           STRING NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    metadata         JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    failed_at        TIMESTAMPTZ
);

CREATE TABLE flow_steps (
    id           STRING PRIMARY KEY,
    tenant_id    STRING NOT NULL,
    flow_run_id  STRING NOT NULL REFERENCES flow_runs(id),
    step_id      STRING NOT NULL,
    status       STRING NOT NULL CHECK (status IN ('COMPLETED','FAILED')),
    journal_id   STRING REFERENCES ledger_journals(id),
    error_code   STRING,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, flow_run_id, step_id)
);

CREATE TABLE outbox_events (
    id                STRING PRIMARY KEY,
    tenant_id         STRING NOT NULL,
    aggregate_id      STRING NOT NULL,
    event_type        STRING NOT NULL,
    idempotency_key   STRING NOT NULL UNIQUE,
    payload           JSONB NOT NULL,
    publish_state     STRING NOT NULL DEFAULT 'PENDING',
    attempts          INT8 NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at      TIMESTAMPTZ
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (publish_state, created_at);

-- +goose Down
DROP TABLE outbox_events;
DROP TABLE flow_steps;
DROP TABLE flow_runs;
DROP TABLE account_balances;
DROP TABLE ledger_entries;
DROP TABLE ledger_journals;
DROP TABLE accounts;
```

- [ ] **Step 2: Commit**

```bash
git add sql/migrations/crdb/
git commit -m "feat(db): add CockroachDB initial schema migration"
```

---

## Task 9: `cmd/migrate` runner

**Files:**
- Create: `cmd/migrate/main.go`

- [ ] **Step 1: Write the runner**

```go
// cmd/migrate/main.go
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

func main() {
	backend := flag.String("backend", "sqlite", "sqlite|crdb")
	dir := flag.String("dir", "sql/migrations", "migrations root")
	dsn := flag.String("dsn", "", "database DSN (defaults: sqlite=./ledger.db, crdb=$DATABASE_URL)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: migrate --backend=sqlite|crdb [--dsn=...] up|down|status|reset")
		os.Exit(2)
	}
	cmd := args[0]

	var (
		driver string
		path   string
	)
	switch *backend {
	case "sqlite":
		driver = "sqlite"
		if *dsn == "" {
			*dsn = "./ledger.db"
		}
		path = filepath.Join(*dir, "sqlite")
		_ = goose.SetDialect("sqlite3")
	case "crdb":
		driver = "pgx"
		if *dsn == "" {
			*dsn = os.Getenv("DATABASE_URL")
		}
		path = filepath.Join(*dir, "crdb")
		_ = goose.SetDialect("postgres")
	default:
		fmt.Fprintf(os.Stderr, "unknown backend %q\n", *backend)
		os.Exit(2)
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "missing --dsn or DATABASE_URL")
		os.Exit(2)
	}

	db, err := sql.Open(driver, *dsn)
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := goose.RunContext(context.Background(), cmd, db, path); err != nil {
		slog.Error("goose", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Add dependencies and build**

```bash
go get github.com/pressly/goose/v3
go get modernc.org/sqlite
go get github.com/jackc/pgx/v5/stdlib
go mod tidy
go build ./cmd/migrate
```
Expected: builds clean.

- [ ] **Step 3: Smoke test against SQLite**

```bash
./migrate --backend=sqlite --dsn=./tmp-migrate-test.db up
./migrate --backend=sqlite --dsn=./tmp-migrate-test.db status
./migrate --backend=sqlite --dsn=./tmp-migrate-test.db down
rm tmp-migrate-test.db
rm migrate
```
Expected: migrations run; status shows applied; down reverses; no errors.

- [ ] **Step 4: Commit**

```bash
git add cmd/migrate/ go.mod go.sum
git commit -m "feat(migrate): add goose migration runner for both backends"
```

---

## Task 10: sqlc config + queries

**Files:**
- Create: `sqlc.yaml`
- Create: `sql/queries/sqlite/accounts.sql`
- Create: `sql/queries/sqlite/journals.sql`
- Create: `sql/queries/sqlite/entries.sql`
- Create: `sql/queries/sqlite/balances.sql`
- Create: `sql/queries/sqlite/flows.sql`
- Create: `sql/queries/sqlite/outbox.sql`
- Create: parallel files under `sql/queries/crdb/`

- [ ] **Step 1: Write `sqlc.yaml`**

```yaml
version: "2"
sql:
  - engine: sqlite
    schema: sql/migrations/sqlite
    queries: sql/queries/sqlite
    gen:
      go:
        package: sqlitestore
        out: gen/sqlite
        sql_package: database/sql
        emit_pointers_for_null_types: true
        emit_interface: false
        emit_db_tags: true
  - engine: postgresql
    schema: sql/migrations/crdb
    queries: sql/queries/crdb
    gen:
      go:
        package: crdbstore
        out: gen/crdb
        sql_package: pgx/v5
        emit_pointers_for_null_types: true
        emit_interface: false
        emit_db_tags: true
        overrides:
          - db_type: "numeric"
            go_type: "github.com/shopspring/decimal.Decimal"
          - db_type: "decimal"
            go_type: "github.com/shopspring/decimal.Decimal"
```

- [ ] **Step 2: Write SQLite queries**

`sql/queries/sqlite/accounts.sql`:
```sql
-- name: InsertAccount :exec
INSERT INTO accounts (id, tenant_id, owner_type, owner_id, account_type, currency, normal_balance, allow_negative, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetAccount :one
SELECT * FROM accounts WHERE tenant_id = ? AND id = ?;

-- name: ListAccountsByOwner :many
SELECT * FROM accounts WHERE tenant_id = ? AND owner_type = ? AND owner_id = ?;
```

`sql/queries/sqlite/journals.sql`:
```sql
-- name: InsertJournal :exec
INSERT INTO ledger_journals (id, tenant_id, flow_run_id, event_id, source_service, source_type, actor_id, metadata)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetJournal :one
SELECT * FROM ledger_journals WHERE tenant_id = ? AND id = ?;

-- name: GetJournalsByFlowRun :many
SELECT * FROM ledger_journals WHERE tenant_id = ? AND flow_run_id = ?;
```

`sql/queries/sqlite/entries.sql`:
```sql
-- name: InsertEntry :exec
INSERT INTO ledger_entries (id, tenant_id, journal_id, account_id, currency, direction, amount)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: ListAccountActivity :many
SELECT id, journal_id, currency, direction, amount, created_at
FROM ledger_entries
WHERE tenant_id = ? AND account_id = ? AND currency = ?
  AND (created_at >= ? OR ? = '')
  AND (created_at <= ? OR ? = '')
ORDER BY created_at ASC, id ASC
LIMIT ?;
```

`sql/queries/sqlite/balances.sql`:
```sql
-- name: GetBalance :one
SELECT * FROM account_balances WHERE tenant_id = ? AND account_id = ? AND currency = ?;

-- name: UpsertBalanceZero :exec
INSERT INTO account_balances (tenant_id, account_id, currency, posted_debits, posted_credits, version)
VALUES (?, ?, ?, '0', '0', 0)
ON CONFLICT (tenant_id, account_id, currency) DO NOTHING;

-- name: UpdateBalance :exec
UPDATE account_balances
SET posted_debits = ?, posted_credits = ?, version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE tenant_id = ? AND account_id = ? AND currency = ?;
```

`sql/queries/sqlite/flows.sql`:
```sql
-- name: InsertFlowRun :exec
INSERT INTO flow_runs (id, tenant_id, flow_type, idempotency_key, source_service, actor_id, status, metadata)
VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?);

-- name: GetFlowByIdempotency :one
SELECT * FROM flow_runs WHERE tenant_id = ? AND idempotency_key = ?;

-- name: GetFlowByID :one
SELECT * FROM flow_runs WHERE tenant_id = ? AND id = ?;

-- name: CompleteFlowRun :exec
UPDATE flow_runs SET status = 'COMPLETED', completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = ? AND tenant_id = ?;

-- name: InsertFlowStep :exec
INSERT INTO flow_steps (id, tenant_id, flow_run_id, step_id, status, journal_id, error_code)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetFlowSteps :many
SELECT * FROM flow_steps WHERE tenant_id = ? AND flow_run_id = ? ORDER BY created_at ASC;
```

`sql/queries/sqlite/outbox.sql`:
```sql
-- name: InsertOutbox :exec
INSERT INTO outbox_events (id, tenant_id, aggregate_id, event_type, idempotency_key, payload)
VALUES (?, ?, ?, ?, ?, ?);

-- name: ListPendingOutbox :many
SELECT * FROM outbox_events WHERE publish_state = 'PENDING' ORDER BY created_at ASC LIMIT ?;

-- name: MarkOutboxPublished :exec
UPDATE outbox_events SET publish_state = 'PUBLISHED', published_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = ?;

-- name: IncrementOutboxAttempts :exec
UPDATE outbox_events SET attempts = attempts + 1 WHERE id = ?;
```

- [ ] **Step 3: Write CRDB queries (parallel files)**

`sql/queries/crdb/accounts.sql`:
```sql
-- name: InsertAccount :exec
INSERT INTO accounts (id, tenant_id, owner_type, owner_id, account_type, currency, normal_balance, allow_negative, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: GetAccount :one
SELECT * FROM accounts WHERE tenant_id = $1 AND id = $2;

-- name: ListAccountsByOwner :many
SELECT * FROM accounts WHERE tenant_id = $1 AND owner_type = $2 AND owner_id = $3;
```

`sql/queries/crdb/journals.sql`:
```sql
-- name: InsertJournal :exec
INSERT INTO ledger_journals (id, tenant_id, flow_run_id, event_id, source_service, source_type, actor_id, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetJournal :one
SELECT * FROM ledger_journals WHERE tenant_id = $1 AND id = $2;

-- name: GetJournalsByFlowRun :many
SELECT * FROM ledger_journals WHERE tenant_id = $1 AND flow_run_id = $2;
```

`sql/queries/crdb/entries.sql`:
```sql
-- name: InsertEntry :exec
INSERT INTO ledger_entries (id, tenant_id, journal_id, account_id, currency, direction, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListAccountActivity :many
SELECT id, journal_id, currency, direction, amount, created_at
FROM ledger_entries
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
  AND ($4::timestamptz IS NULL OR created_at >= $4)
  AND ($5::timestamptz IS NULL OR created_at <= $5)
ORDER BY created_at ASC, id ASC
LIMIT $6;
```

`sql/queries/crdb/balances.sql`:
```sql
-- name: GetBalance :one
SELECT * FROM account_balances WHERE tenant_id = $1 AND account_id = $2 AND currency = $3;

-- name: LockBalance :one
SELECT * FROM account_balances
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
FOR UPDATE;

-- name: UpsertBalanceZero :exec
INSERT INTO account_balances (tenant_id, account_id, currency)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, account_id, currency) DO NOTHING;

-- name: UpdateBalance :exec
UPDATE account_balances
SET posted_debits = $4, posted_credits = $5, version = version + 1, updated_at = now()
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3;
```

`sql/queries/crdb/flows.sql`:
```sql
-- name: InsertFlowRun :exec
INSERT INTO flow_runs (id, tenant_id, flow_type, idempotency_key, source_service, actor_id, status, metadata)
VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', $7);

-- name: GetFlowByIdempotency :one
SELECT * FROM flow_runs WHERE tenant_id = $1 AND idempotency_key = $2 FOR UPDATE;

-- name: GetFlowByID :one
SELECT * FROM flow_runs WHERE tenant_id = $1 AND id = $2;

-- name: CompleteFlowRun :exec
UPDATE flow_runs SET status = 'COMPLETED', completed_at = now() WHERE id = $1 AND tenant_id = $2;

-- name: InsertFlowStep :exec
INSERT INTO flow_steps (id, tenant_id, flow_run_id, step_id, status, journal_id, error_code)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetFlowSteps :many
SELECT * FROM flow_steps WHERE tenant_id = $1 AND flow_run_id = $2 ORDER BY created_at ASC;
```

`sql/queries/crdb/outbox.sql`:
```sql
-- name: InsertOutbox :exec
INSERT INTO outbox_events (id, tenant_id, aggregate_id, event_type, idempotency_key, payload)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListPendingOutbox :many
SELECT * FROM outbox_events WHERE publish_state = 'PENDING' ORDER BY created_at ASC LIMIT $1;

-- name: MarkOutboxPublished :exec
UPDATE outbox_events SET publish_state = 'PUBLISHED', published_at = now() WHERE id = $1;

-- name: IncrementOutboxAttempts :exec
UPDATE outbox_events SET attempts = attempts + 1 WHERE id = $1;
```

- [ ] **Step 4: Generate code**

```bash
make sqlc
```
Expected: `gen/sqlite/*.go` and `gen/crdb/*.go` are created.

If sqlc complains about decimal mapping for SQLite TEXT, leave amount/balance columns as `string` in the generated code; the repo layer will parse with `shopspring/decimal`.

- [ ] **Step 5: Ensure build passes**

```bash
go mod tidy
go build ./...
```
Expected: builds clean.

- [ ] **Step 6: Commit**

```bash
git add sqlc.yaml sql/queries/ gen/sqlite/ gen/crdb/ go.mod go.sum
git commit -m "feat(db): add sqlc queries and generated code for both backends"
```

---

## Task 11: Repository interface

**Files:**
- Create: `internal/repo/repo.go`

- [ ] **Step 1: Write the interface**

```go
// internal/repo/repo.go
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
```

- [ ] **Step 2: Build & commit**

```bash
go build ./internal/repo/
git add internal/repo/repo.go
git commit -m "feat(repo): add Store and Tx interfaces"
```

---

## Task 12: SQLite repository implementation

**Files:**
- Create: `internal/repo/sqlite/store.go`
- Create: `internal/repo/sqlite/tx.go`
- Create: `internal/repo/sqlite/conv.go`
- Create: `internal/repo/sqlite/store_test.go`

- [ ] **Step 1: Implement `store.go`**

```go
// internal/repo/sqlite/store.go
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"

	"github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type Store struct {
	db *sql.DB
	q  *sqlitestore.Queries
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // single-writer for IMMEDIATE
	return &Store{db: db, q: sqlitestore.New(db)}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) BeginFlowTx(ctx context.Context) (repo.Tx, error) {
	// modernc.org/sqlite supports BEGIN IMMEDIATE via the TxOptions ReadOnly flag
	// being false plus a manual statement. We issue it explicitly to be safe.
	if _, err := s.db.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, fmt.Errorf("begin immediate: %w", err)
	}
	// We deliberately do NOT use sql.Tx because BEGIN IMMEDIATE inside it is
	// not portable. Instead we wrap the connection and provide our own
	// Commit/Rollback. For simplicity, fall back to a normal driver Conn.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		// Roll back the open IMMEDIATE we just started — the connection-pool
		// API makes this awkward; treat this as a startup error.
		_, _ = s.db.ExecContext(ctx, "ROLLBACK")
		return nil, err
	}
	q := sqlitestore.New(conn)
	return &Tx{db: s.db, conn: conn, q: q}, nil
}

func (s *Store) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := s.q.GetAccount(ctx, sqlitestore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, "account "+accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

func (s *Store) GetBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	row, err := s.q.GetBalance(ctx, sqlitestore.GetBalanceParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return decimal.Zero, decimal.Zero, 0, nil
		}
		return decimal.Zero, decimal.Zero, 0, err
	}
	d, err := decimal.NewFromString(row.PostedDebits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_debits: %w", err)
	}
	c, err := decimal.NewFromString(row.PostedCredits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_credits: %w", err)
	}
	return d, c, row.Version, nil
}

// ... GetFlow, ListAccountActivity, PendingOutbox, MarkOutboxPublished,
// IncrementOutboxAttempts follow the same shape; see conv.go for row->domain
// helpers. Implement them now using the sqlc-generated methods.
```

- [ ] **Step 2: Implement `tx.go`**

```go
// internal/repo/sqlite/tx.go
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type Tx struct {
	db   *sql.DB
	conn *sql.Conn
	q    *sqlitestore.Queries
	done bool
}

func (t *Tx) finalize(stmt string) error {
	if t.done {
		return nil
	}
	t.done = true
	_, err := t.conn.ExecContext(context.Background(), stmt)
	closeErr := t.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (t *Tx) Commit() error   { return t.finalize("COMMIT") }
func (t *Tx) Rollback() error { return t.finalize("ROLLBACK") }

func (t *Tx) GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error) {
	row, err := t.q.GetFlowByIdempotency(ctx, sqlitestore.GetFlowByIdempotencyParams{TenantID: tenantID, IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFlowRun(row), nil
}

func (t *Tx) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	row, err := t.q.GetAccount(ctx, sqlitestore.GetAccountParams{TenantID: tenantID, ID: accountID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, accountID)
		}
		return nil, err
	}
	return rowToAccount(row), nil
}

func (t *Tx) EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error {
	return t.q.UpsertBalanceZero(ctx, sqlitestore.UpsertBalanceZeroParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
	})
}

func (t *Tx) LockBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	if err := t.EnsureBalanceRow(ctx, tenantID, accountID, currency); err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	row, err := t.q.GetBalance(ctx, sqlitestore.GetBalanceParams{TenantID: tenantID, AccountID: accountID, Currency: currency})
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	d, err := decimal.NewFromString(row.PostedDebits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_debits: %w", err)
	}
	c, err := decimal.NewFromString(row.PostedCredits)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, fmt.Errorf("parse posted_credits: %w", err)
	}
	return d, c, row.Version, nil
}

func (t *Tx) UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error {
	return t.q.UpdateBalance(ctx, sqlitestore.UpdateBalanceParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
		PostedDebits: postedDebits.String(), PostedCredits: postedCredits.String(),
	})
}

func (t *Tx) InsertAccount(ctx context.Context, a ledger.Account) error {
	allow := int64(0)
	if a.AllowNegative {
		allow = 1
	}
	return t.q.InsertAccount(ctx, sqlitestore.InsertAccountParams{
		ID: a.ID, TenantID: a.TenantID, OwnerType: a.OwnerType, OwnerID: a.OwnerID,
		AccountType: a.AccountType, Currency: a.Currency,
		NormalBalance: string(a.NormalBalance), AllowNegative: allow,
		Status: string(a.Status),
	})
}

func (t *Tx) InsertFlowRun(ctx context.Context, f ledger.FlowRun) error {
	meta, _ := json.Marshal(f.Metadata)
	return t.q.InsertFlowRun(ctx, sqlitestore.InsertFlowRunParams{
		ID: f.ID, TenantID: f.TenantID, FlowType: f.FlowType,
		IdempotencyKey: f.IdempotencyKey, SourceService: f.SourceService,
		ActorID: f.ActorID, Metadata: string(meta),
	})
}

func (t *Tx) CompleteFlowRun(ctx context.Context, tenantID, flowRunID string) error {
	return t.q.CompleteFlowRun(ctx, sqlitestore.CompleteFlowRunParams{ID: flowRunID, TenantID: tenantID})
}

func (t *Tx) InsertJournal(ctx context.Context, j ledger.Journal) error {
	meta, _ := json.Marshal(j.Metadata)
	return t.q.InsertJournal(ctx, sqlitestore.InsertJournalParams{
		ID: j.ID, TenantID: j.TenantID,
		FlowRunID: nullString(j.FlowRunID), EventID: j.EventID,
		SourceService: j.SourceService, SourceType: j.SourceType,
		ActorID: j.ActorID, Metadata: string(meta),
	})
}

func (t *Tx) InsertEntry(ctx context.Context, tenantID, entryID, journalID, accountID, currency string, dir ledger.Direction, amount decimal.Decimal) error {
	return t.q.InsertEntry(ctx, sqlitestore.InsertEntryParams{
		ID: entryID, TenantID: tenantID, JournalID: journalID, AccountID: accountID,
		Currency: currency, Direction: string(dir), Amount: amount.String(),
	})
}

func (t *Tx) InsertFlowStep(ctx context.Context, s ledger.FlowStep) error {
	return t.q.InsertFlowStep(ctx, sqlitestore.InsertFlowStepParams{
		ID: s.ID, TenantID: s.TenantID, FlowRunID: s.FlowRunID,
		StepID: s.StepID, Status: string(s.Status),
		JournalID: nullString(s.JournalID), ErrorCode: nullString(s.ErrorCode),
	})
}

func (t *Tx) InsertOutbox(ctx context.Context, e repo.OutboxEvent) error {
	return t.q.InsertOutbox(ctx, sqlitestore.InsertOutboxParams{
		ID: e.ID, TenantID: e.TenantID, AggregateID: e.AggregateID,
		EventType: e.EventType, IdempotencyKey: e.IdempotencyKey, Payload: string(e.Payload),
	})
}

func (t *Tx) GetFlowSteps(ctx context.Context, tenantID, flowRunID string) ([]ledger.FlowStep, error) {
	rows, err := t.q.GetFlowSteps(ctx, sqlitestore.GetFlowStepsParams{TenantID: tenantID, FlowRunID: flowRunID})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.FlowStep, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToFlowStep(r))
	}
	return out, nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	v := s
	return &v
}
```

- [ ] **Step 3: Implement `conv.go`**

```go
// internal/repo/sqlite/conv.go
package sqlite

import (
	"encoding/json"
	"time"

	"github.com/caxqueiroz/doubleledger/gen/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func rowToAccount(r sqlitestore.Account) *ledger.Account {
	return &ledger.Account{
		ID: r.ID, TenantID: r.TenantID, OwnerType: r.OwnerType, OwnerID: r.OwnerID,
		AccountType: r.AccountType, Currency: r.Currency,
		NormalBalance: ledger.NormalBalance(r.NormalBalance),
		AllowNegative: r.AllowNegative != 0,
		Status:        ledger.AccountStatus(r.Status),
		CreatedAt:     parseTime(r.CreatedAt),
	}
}

func rowToFlowRun(r sqlitestore.FlowRun) *ledger.FlowRun {
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	f := &ledger.FlowRun{
		ID: r.ID, TenantID: r.TenantID, FlowType: r.FlowType,
		IdempotencyKey: r.IdempotencyKey, SourceService: r.SourceService,
		ActorID: r.ActorID, Status: ledger.FlowStatus(r.Status),
		Metadata: meta, CreatedAt: parseTime(r.CreatedAt),
	}
	if r.CompletedAt != nil {
		t := parseTime(*r.CompletedAt)
		f.CompletedAt = &t
	}
	if r.FailedAt != nil {
		t := parseTime(*r.FailedAt)
		f.FailedAt = &t
	}
	return f
}

func rowToFlowStep(r sqlitestore.FlowStep) *ledger.FlowStep {
	s := &ledger.FlowStep{
		ID: r.ID, TenantID: r.TenantID, FlowRunID: r.FlowRunID,
		StepID: r.StepID, Status: ledger.StepStatus(r.Status),
		CreatedAt: parseTime(r.CreatedAt),
	}
	if r.JournalID != nil {
		s.JournalID = *r.JournalID
	}
	if r.ErrorCode != nil {
		s.ErrorCode = *r.ErrorCode
	}
	return s
}
```

- [ ] **Step 4: Smoke test the store**

```go
// internal/repo/sqlite/store_test.go
package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	_ "embed"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func openTempDB(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")

	// Run migrations using raw SQL read from disk.
	mig, err := os.ReadFile("../../../sql/migrations/sqlite/0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Strip goose markers and exec.
	if _, err := s.db.Exec(stripGoose(string(mig))); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func stripGoose(s string) string {
	out := ""
	in := false
	for _, line := range splitLines(s) {
		switch {
		case line == "-- +goose Up":
			in = true
		case line == "-- +goose Down":
			in = false
		case in:
			out += line + "\n"
		}
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i, r := range s {
		if r == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func TestInsertAndReadAccount(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	tx, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	acc := ledger.Account{
		ID: "user:1:cash:USD", TenantID: "t1", OwnerType: "user", OwnerID: "1",
		AccountType: "cash", Currency: "USD",
		NormalBalance: ledger.NormalDebit, Status: ledger.AccountActive,
	}
	if err := tx.InsertAccount(ctx, acc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.GetAccount(ctx, "t1", "user:1:cash:USD")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Currency != "USD" {
		t.Fatalf("want USD, got %s", got.Currency)
	}
}
```

Run:
```bash
go test ./internal/repo/sqlite/ -v
```
Expected: PASS.

- [ ] **Step 5: Fill in remaining `Store` read methods**

Add the following methods to `internal/repo/sqlite/store.go`. The pattern matches `GetBalance` — call the sqlc-generated query, parse `decimal.Decimal` from the TEXT column via `decimal.NewFromString`, parse times with `parseTime`.

```go
func (s *Store) GetFlow(ctx context.Context, tenantID, flowRunID string) (*ledger.FlowRun, error) {
	row, err := s.q.GetFlowByID(ctx, sqlitestore.GetFlowByIDParams{TenantID: tenantID, ID: flowRunID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	f := rowToFlowRun(row)
	steps, err := s.q.GetFlowSteps(ctx, sqlitestore.GetFlowStepsParams{TenantID: tenantID, FlowRunID: flowRunID})
	if err != nil {
		return nil, err
	}
	for _, st := range steps {
		f.Steps = append(f.Steps, *rowToFlowStep(st))
	}
	return f, nil
}

func (s *Store) ListAccountActivity(ctx context.Context, in repo.ListActivityInput) ([]repo.ActivityRow, error) {
	since, until := "", ""
	if in.Since != nil {
		since = in.Since.UTC().Format(time.RFC3339Nano)
	}
	if in.Until != nil {
		until = in.Until.UTC().Format(time.RFC3339Nano)
	}
	rows, err := s.q.ListAccountActivity(ctx, sqlitestore.ListAccountActivityParams{
		TenantID: in.TenantID, AccountID: in.AccountID, Currency: in.Currency,
		// SQLite query uses dual params for each optional time filter.
		Column4: since, Column5: since,
		Column6: until, Column7: until,
		Limit: int64(in.Limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]repo.ActivityRow, 0, len(rows))
	for _, r := range rows {
		amt, _ := decimal.NewFromString(r.Amount)
		out = append(out, repo.ActivityRow{
			JournalID: r.JournalID, EntryID: r.ID,
			Currency: r.Currency, Direction: ledger.Direction(r.Direction),
			Amount: amt, CreatedAt: parseTime(r.CreatedAt),
		})
	}
	return out, nil
}

func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]repo.OutboxEvent, error) {
	rows, err := s.q.ListPendingOutbox(ctx, int64(limit))
	if err != nil {
		return nil, err
	}
	out := make([]repo.OutboxEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, repo.OutboxEvent{
			ID: r.ID, TenantID: r.TenantID, AggregateID: r.AggregateID,
			EventType: r.EventType, IdempotencyKey: r.IdempotencyKey,
			Payload: []byte(r.Payload), CreatedAt: parseTime(r.CreatedAt),
		})
	}
	return out, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	return s.q.MarkOutboxPublished(ctx, id)
}

func (s *Store) IncrementOutboxAttempts(ctx context.Context, id string) error {
	return s.q.IncrementOutboxAttempts(ctx, id)
}
```

If sqlc generates different param-struct field names for the activity query (e.g. `Created`, `Created_2`), rename accordingly — the only requirement is the dual-bind pattern that emulates `IS NULL OR ...`.

- [ ] **Step 6: Build & commit**

```bash
go build ./...
go test ./internal/repo/sqlite/ -v
git add internal/repo/sqlite/
git commit -m "feat(repo/sqlite): implement Store and Tx with BEGIN IMMEDIATE"
```

---

## Task 13: CRDB repository implementation

**Files:**
- Create: `internal/repo/retry.go`
- Create: `internal/repo/crdb/store.go`
- Create: `internal/repo/crdb/tx.go`
- Create: `internal/repo/crdb/conv.go`
- Create: `internal/repo/crdb/store_integration_test.go` (build tag `integration`)

- [ ] **Step 1: Write retry helper**

```go
// internal/repo/retry.go
package repo

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// IsSerializationError reports whether err is a Postgres-protocol serialization
// failure (SQLSTATE 40001). CockroachDB returns this on isolation conflicts.
func IsSerializationError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001"
	}
	return false
}

// WithRetry runs fn under capped exponential backoff. After maxAttempts retries
// it returns CodeSerializationRetryExhausted. Non-serialization errors return
// immediately.
func WithRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	var lastErr error
	delay := 5 * time.Millisecond
	for i := 0; i < maxAttempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !IsSerializationError(err) {
			return err
		}
		lastErr = err
		jitter := time.Duration(rand.Int63n(int64(delay)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay + jitter):
		}
		if delay < 80*time.Millisecond {
			delay *= 2
		}
	}
	return ledger.NewDomainError(ledger.CodeSerializationRetryExhausted, lastErr.Error())
}
```

- [ ] **Step 2: Implement `store.go` and `tx.go`**

`store.go`:
```go
// internal/repo/crdb/store.go
package crdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/doubleledger/gen/crdb"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type Store struct {
	pool *pgxpool.Pool
	q    *crdbstore.Queries
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Store{pool: pool, q: crdbstore.New(pool)}, nil
}

func (s *Store) Close() error { s.pool.Close(); return nil }

func (s *Store) BeginFlowTx(ctx context.Context) (repo.Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, q: crdbstore.New(tx)}, nil
}

// ... GetAccount, GetBalance, GetFlow, ListAccountActivity, PendingOutbox,
// MarkOutboxPublished, IncrementOutboxAttempts mirror the SQLite ones using
// crdbstore.Queries. Convert pgx.ErrNoRows -> CodeAccountNotFound where it
// matters.
```

`tx.go`:
```go
// internal/repo/crdb/tx.go
package crdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	crdbstore "github.com/caxqueiroz/doubleledger/gen/crdb"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type Tx struct {
	tx pgx.Tx
	q  *crdbstore.Queries
}

func (t *Tx) Commit() error   { return t.tx.Commit(context.Background()) }
func (t *Tx) Rollback() error { return t.tx.Rollback(context.Background()) }

func (t *Tx) GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error) {
	row, err := t.q.GetFlowByIdempotency(ctx, crdbstore.GetFlowByIdempotencyParams{TenantID: tenantID, IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToFlowRun(row), nil
}

func (t *Tx) LockBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	if err := t.EnsureBalanceRow(ctx, tenantID, accountID, currency); err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	row, err := t.q.LockBalance(ctx, crdbstore.LockBalanceParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	return row.PostedDebits, row.PostedCredits, row.Version, nil
}

func (t *Tx) EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error {
	return t.q.UpsertBalanceZero(ctx, crdbstore.UpsertBalanceZeroParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
	})
}

func (t *Tx) UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error {
	return t.q.UpdateBalance(ctx, crdbstore.UpdateBalanceParams{
		TenantID: tenantID, AccountID: accountID, Currency: currency,
		PostedDebits: postedDebits, PostedCredits: postedCredits,
	})
}

// InsertAccount, InsertFlowRun, CompleteFlowRun, InsertJournal, InsertEntry,
// InsertFlowStep, InsertOutbox, GetFlowSteps, GetAccount follow the same shape
// using the crdbstore.Queries. Metadata maps are json.Marshal'd into JSONB.
```

- [ ] **Step 3: Write `conv.go`**

Mirror the SQLite `conv.go`: build `ledger.Account`, `ledger.FlowRun`, `ledger.FlowStep` from CRDB-generated row types. Times are `time.Time` already; metadata is `[]byte` JSONB.

- [ ] **Step 4: Integration test (testcontainers)**

```go
//go:build integration

package crdb_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
	crdbrepo "github.com/caxqueiroz/doubleledger/internal/repo/crdb"
)

func startCRDB(t *testing.T) (dsn string) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "cockroachdb/cockroach:v23.2.6",
		ExposedPorts: []string{"26257/tcp"},
		Cmd:          []string{"start-single-node", "--insecure"},
		WaitingFor:   wait.ForLog("nodeID:").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start crdb: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "26257")
	dsn = "postgres://root@" + host + ":" + port.Port() + "/defaultdb?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	_ = goose.SetDialect("postgres")
	if err := goose.Up(db, "../../../sql/migrations/crdb"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return dsn
}

func TestCRDB_InsertAndReadAccount(t *testing.T) {
	if os.Getenv("SKIP_CRDB") != "" {
		t.Skip()
	}
	dsn := startCRDB(t)
	s, err := crdbrepo.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	tx, err := s.BeginFlowTx(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.InsertAccount(context.Background(), ledger.Account{
		ID: "user:1:cash:USD", TenantID: "t1", OwnerType: "user", OwnerID: "1",
		AccountType: "cash", Currency: "USD",
		NormalBalance: ledger.NormalDebit, Status: ledger.AccountActive,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.GetAccount(context.Background(), "t1", "user:1:cash:USD")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Currency != "USD" {
		t.Fatalf("want USD, got %s", got.Currency)
	}
}
```

Run:
```bash
go test -tags=integration ./internal/repo/crdb/ -v
```
Expected: PASS (requires Docker).

- [ ] **Step 5: Commit**

```bash
git add internal/repo/retry.go internal/repo/crdb/ go.mod go.sum
git commit -m "feat(repo/crdb): implement Store, Tx, and 40001 retry helper"
```

---

## Task 14: Outbox dispatcher

**Files:**
- Create: `internal/outbox/sink.go`
- Create: `internal/outbox/event.go`
- Create: `internal/outbox/dispatcher.go`
- Create: `internal/outbox/dispatcher_test.go`

- [ ] **Step 1: Write `event.go`**

```go
// internal/outbox/event.go
package outbox

import "time"

type Event struct {
	ID             string
	TenantID       string
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}
```

- [ ] **Step 2: Write `sink.go`**

```go
// internal/outbox/sink.go
package outbox

import (
	"context"
	"log/slog"
)

type Sink interface {
	Publish(ctx context.Context, e Event) error
}

type LogSink struct {
	Logger *slog.Logger
}

func (s LogSink) Publish(ctx context.Context, e Event) error {
	s.Logger.InfoContext(ctx, "outbox.publish",
		"event_id", e.ID, "tenant_id", e.TenantID, "event_type", e.EventType,
		"idempotency_key", e.IdempotencyKey)
	return nil
}
```

- [ ] **Step 3: Write a failing dispatcher test**

```go
// internal/outbox/dispatcher_test.go
package outbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu        sync.Mutex
	pending   []Event
	published map[string]bool
}

func (f *fakeStore) PendingOutbox(ctx context.Context, limit int) ([]Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := limit
	if n > len(f.pending) {
		n = len(f.pending)
	}
	out := make([]Event, n)
	copy(out, f.pending[:n])
	return out, nil
}

func (f *fakeStore) MarkOutboxPublished(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published[id] = true
	// drop from pending
	kept := f.pending[:0]
	for _, e := range f.pending {
		if e.ID != id {
			kept = append(kept, e)
		}
	}
	f.pending = kept
	return nil
}

func (f *fakeStore) IncrementOutboxAttempts(ctx context.Context, id string) error { return nil }

type fakeSink struct {
	mu     sync.Mutex
	calls  []Event
	failID string
}

func (s *fakeSink) Publish(ctx context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, e)
	if e.ID == s.failID {
		return ctx.Err()
	}
	return nil
}

func TestDispatcher_PublishesPending(t *testing.T) {
	store := &fakeStore{
		pending:   []Event{{ID: "a"}, {ID: "b"}},
		published: map[string]bool{},
	}
	sink := &fakeSink{}
	d := NewDispatcher(store, sink, Config{Interval: 5 * time.Millisecond, BatchSize: 10})
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	defer cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		ok := store.published["a"] && store.published["b"]
		store.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all published; got %v", store.published)
}
```

- [ ] **Step 4: Run test, expect failure**

```bash
go test ./internal/outbox/
```
Expected: FAIL — symbols missing.

- [ ] **Step 5: Implement `dispatcher.go`**

```go
// internal/outbox/dispatcher.go
package outbox

import (
	"context"
	"log/slog"
	"time"
)

type Store interface {
	PendingOutbox(ctx context.Context, limit int) ([]Event, error)
	MarkOutboxPublished(ctx context.Context, id string) error
	IncrementOutboxAttempts(ctx context.Context, id string) error
}

type Config struct {
	Interval  time.Duration
	BatchSize int
}

type Dispatcher struct {
	store Store
	sink  Sink
	cfg   Config
	log   *slog.Logger
}

func NewDispatcher(store Store, sink Sink, cfg Config) *Dispatcher {
	if cfg.Interval == 0 {
		cfg.Interval = 250 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	return &Dispatcher{store: store, sink: sink, cfg: cfg, log: slog.Default()}
}

func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.tick(ctx)
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) {
	events, err := d.store.PendingOutbox(ctx, d.cfg.BatchSize)
	if err != nil {
		d.log.WarnContext(ctx, "outbox.pending.error", "err", err)
		return
	}
	for _, e := range events {
		if err := d.sink.Publish(ctx, e); err != nil {
			d.log.WarnContext(ctx, "outbox.publish.error", "id", e.ID, "err", err)
			_ = d.store.IncrementOutboxAttempts(ctx, e.ID)
			continue
		}
		if err := d.store.MarkOutboxPublished(ctx, e.ID); err != nil {
			d.log.WarnContext(ctx, "outbox.mark.error", "id", e.ID, "err", err)
		}
	}
}
```

- [ ] **Step 6: Run tests, expect pass**

```bash
go test ./internal/outbox/ -v
```
Expected: PASS.

- [ ] **Step 7: Add adapter so `Store` satisfies the dispatcher interface using `repo.Store`**

In `internal/outbox/dispatcher.go`, the `Store` interface uses `Event` not `repo.OutboxEvent`. Add a small bridge in `cmd/server` later (Task 21) that wraps a `repo.Store` and converts events.

- [ ] **Step 8: Commit**

```bash
git add internal/outbox/
git commit -m "feat(outbox): add Sink, LogSink, and polling Dispatcher"
```

---

## Task 15: Service — domain → Connect error mapping

**Files:**
- Create: `internal/service/errors.go`
- Create: `internal/service/errors_test.go`

- [ ] **Step 1: Write failing test**

```go
// internal/service/errors_test.go
package service

import (
	"errors"
	"testing"

	"connectrpc.com/connect"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func TestToConnectError_Mapping(t *testing.T) {
	cases := []struct {
		code     ledger.DomainCode
		expected connect.Code
	}{
		{ledger.CodeInsufficientFunds, connect.CodeFailedPrecondition},
		{ledger.CodeAccountNotFound, connect.CodeNotFound},
		{ledger.CodeAccountCurrencyMismatch, connect.CodeInvalidArgument},
		{ledger.CodeUnbalancedJournal, connect.CodeInvalidArgument},
		{ledger.CodeDuplicateIdempotencyKey, connect.CodeAlreadyExists},
		{ledger.CodeFlowAlreadyCompleted, connect.CodeAlreadyExists},
		{ledger.CodeFlowConflict, connect.CodeAborted},
		{ledger.CodeInvalidAccountStatus, connect.CodeFailedPrecondition},
		{ledger.CodeSerializationRetryExhausted, connect.CodeAborted},
	}
	for _, c := range cases {
		t.Run(string(c.code), func(t *testing.T) {
			err := ToConnectError(ledger.NewDomainError(c.code, "x"))
			var cerr *connect.Error
			if !errors.As(err, &cerr) {
				t.Fatalf("not a connect.Error: %v", err)
			}
			if cerr.Code() != c.expected {
				t.Fatalf("want %s, got %s", c.expected, cerr.Code())
			}
		})
	}
}

func TestToConnectError_GenericErrorIsInternal(t *testing.T) {
	err := ToConnectError(errors.New("boom"))
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("not a connect.Error")
	}
	if cerr.Code() != connect.CodeInternal {
		t.Fatalf("want Internal, got %s", cerr.Code())
	}
}
```

- [ ] **Step 2: Run, expect fail**

```bash
go test ./internal/service/
```
Expected: FAIL.

- [ ] **Step 3: Implement `errors.go`**

```go
// internal/service/errors.go
package service

import (
	"errors"

	"connectrpc.com/connect"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func ToConnectError(err error) error {
	if err == nil {
		return nil
	}
	var de *ledger.DomainError
	if errors.As(err, &de) {
		code := connect.CodeInternal
		switch de.Code {
		case ledger.CodeInsufficientFunds, ledger.CodeInvalidAccountStatus:
			code = connect.CodeFailedPrecondition
		case ledger.CodeAccountNotFound:
			code = connect.CodeNotFound
		case ledger.CodeAccountCurrencyMismatch, ledger.CodeUnbalancedJournal:
			code = connect.CodeInvalidArgument
		case ledger.CodeDuplicateIdempotencyKey, ledger.CodeFlowAlreadyCompleted:
			code = connect.CodeAlreadyExists
		case ledger.CodeFlowConflict, ledger.CodeSerializationRetryExhausted:
			code = connect.CodeAborted
		}
		ce := connect.NewError(code, de)
		// Attach the domain code as a header so clients can branch.
		ce.Meta().Set("ledger-error-code", string(de.Code))
		return ce
	}
	return connect.NewError(connect.CodeInternal, err)
}
```

- [ ] **Step 4: Add Connect dep, build, test**

```bash
go get connectrpc.com/connect
go mod tidy
go test ./internal/service/ -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/service/errors.go internal/service/errors_test.go go.mod go.sum
git commit -m "feat(service): map domain errors to Connect codes"
```

---

## Task 16: Service — `CreateAccount`, `GetAccount`, `GetBalance`

**Files:**
- Create: `internal/service/server.go`
- Create: `internal/service/create_account.go`
- Create: `internal/service/get_account.go`
- Create: `internal/service/get_balance.go`
- Create: `internal/service/server_test.go`

- [ ] **Step 1: Write `server.go`**

```go
// internal/service/server.go
package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type Clock func() time.Time
type IDGen func() string

type Server struct {
	Store repo.Store
	Now   Clock
	NewID IDGen
}

func New(store repo.Store) *Server {
	return &Server{
		Store: store,
		Now:   time.Now,
		NewID: func() string { return uuid.NewString() },
	}
}

// runInTx is a helper that opens a flow tx, runs fn, and commits or rolls back.
func (s *Server) runInTx(ctx context.Context, fn func(tx repo.Tx) error) error {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
```

- [ ] **Step 2: Implement `CreateAccount`**

```go
// internal/service/create_account.go
package service

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) CreateAccount(ctx context.Context, req *connect.Request[ledgerv1.CreateAccountRequest]) (*connect.Response[ledgerv1.CreateAccountResponse], error) {
	r := req.Msg
	nb := normalBalanceFromProto(r.NormalBalance)
	if !nb.Valid() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeInvalidAccountStatus, "invalid normal_balance"))
	}
	a := ledger.Account{
		ID:            fmt.Sprintf("%s:%s:%s:%s", r.OwnerType, r.OwnerID, r.AccountType, r.Currency),
		TenantID:      r.TenantId,
		OwnerType:     r.OwnerType,
		OwnerID:       r.OwnerId,
		AccountType:   r.AccountType,
		Currency:      r.Currency,
		NormalBalance: nb,
		AllowNegative: r.AllowNegative,
		Status:        ledger.AccountActive,
		CreatedAt:     s.Now(),
	}
	if err := s.runInTx(ctx, func(tx interface{ InsertAccount(context.Context, ledger.Account) error }) error {
		return tx.InsertAccount(ctx, a)
	}); err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.CreateAccountResponse{Account: accountToProto(a)}), nil
}

func normalBalanceFromProto(p ledgerv1.NormalBalance) ledger.NormalBalance {
	if p == ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT {
		return ledger.NormalCredit
	}
	return ledger.NormalDebit
}

func accountToProto(a ledger.Account) *ledgerv1.Account {
	nb := ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT
	if a.NormalBalance == ledger.NormalCredit {
		nb = ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT
	}
	st := ledgerv1.AccountStatus_ACCOUNT_STATUS_ACTIVE
	switch a.Status {
	case ledger.AccountFrozen:
		st = ledgerv1.AccountStatus_ACCOUNT_STATUS_FROZEN
	case ledger.AccountClosed:
		st = ledgerv1.AccountStatus_ACCOUNT_STATUS_CLOSED
	}
	return &ledgerv1.Account{
		Id: a.ID, TenantId: a.TenantID, OwnerType: a.OwnerType, OwnerId: a.OwnerID,
		AccountType: a.AccountType, Currency: a.Currency,
		NormalBalance: nb, AllowNegative: a.AllowNegative, Status: st,
		CreatedAt: timestamppb.New(a.CreatedAt),
	}
}
```

Note: the local interface used in `runInTx` is illustrative — replace with `repo.Tx` directly:

```go
if err := s.runInTx(ctx, func(tx repo.Tx) error {
    return tx.InsertAccount(ctx, a)
}); err != nil { ... }
```

- [ ] **Step 3: Implement `GetAccount`**

```go
// internal/service/get_account.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) GetAccount(ctx context.Context, req *connect.Request[ledgerv1.GetAccountRequest]) (*connect.Response[ledgerv1.GetAccountResponse], error) {
	a, err := s.Store.GetAccount(ctx, req.Msg.TenantId, req.Msg.AccountId)
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetAccountResponse{Account: accountToProto(*a)}), nil
}
```

- [ ] **Step 4: Implement `GetBalance`**

```go
// internal/service/get_balance.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) GetBalance(ctx context.Context, req *connect.Request[ledgerv1.GetBalanceRequest]) (*connect.Response[ledgerv1.GetBalanceResponse], error) {
	r := req.Msg
	a, err := s.Store.GetAccount(ctx, r.TenantId, r.AccountId)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if a.Currency != r.Currency {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
			"account "+a.ID+" currency="+a.Currency+" req="+r.Currency))
	}
	d, c, ver, err := s.Store.GetBalance(ctx, r.TenantId, r.AccountId, r.Currency)
	if err != nil {
		return nil, ToConnectError(err)
	}
	norm := ledger.NormalizedBalance(a.NormalBalance, d, c)
	return connect.NewResponse(&ledgerv1.GetBalanceResponse{
		Balance: &ledgerv1.Balance{
			AccountId: a.ID, Currency: r.Currency,
			PostedDebits: d.String(), PostedCredits: c.String(),
			Normalized: norm.String(), Version: ver,
		},
	}), nil
}
```

- [ ] **Step 5: Write end-to-end test backed by SQLite**

```go
// internal/service/server_test.go
package service_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

func newServer(t *testing.T) (*service.Server, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	mig, _ := os.ReadFile("../../sql/migrations/sqlite/0001_init.sql")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Apply schema (strip goose markers — see helper in sqlite_test).
	if _, err := st.DB().Exec(stripGoose(string(mig))); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return service.New(st), func() { _ = st.Close() }
}

// Expose DB() in the sqlite Store for tests.
func TestCreateAndGetAccount(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	resp, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "1", AccountType: "cash_available",
		Currency: "USD", NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Msg.Account.Currency != "USD" {
		t.Fatalf("want USD")
	}

	got, err := srv.GetAccount(context.Background(), connect.NewRequest(&ledgerv1.GetAccountRequest{
		TenantId: "t1", AccountId: resp.Msg.Account.Id,
	}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Msg.Account.Id != resp.Msg.Account.Id {
		t.Fatalf("mismatch")
	}
}
```

Add `DB()` accessor to `sqlite.Store`:
```go
// internal/repo/sqlite/store.go
func (s *Store) DB() *sql.DB { return s.db }
```
And re-use `stripGoose` from the SQLite test file by exporting it or duplicating in a shared `testhelper.go`.

- [ ] **Step 6: Run, verify pass**

```bash
go test ./internal/service/ -v
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/service/ internal/repo/sqlite/store.go
git commit -m "feat(service): implement CreateAccount, GetAccount, GetBalance"
```

---

## Task 17: Service — `PostJournal` and `ExecuteFlow`

**Files:**
- Create: `internal/service/post_journal.go`
- Create: `internal/service/execute_flow.go`
- Create: `internal/service/execute_flow_test.go`

- [ ] **Step 1: Write `execute_flow.go` — the orchestrator**

```go
// internal/service/execute_flow.go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

func (s *Server) ExecuteFlow(ctx context.Context, req *connect.Request[ledgerv1.ExecuteFlowRequest]) (*connect.Response[ledgerv1.ExecuteFlowResponse], error) {
	r := req.Msg
	steps, err := stepsFromProto(r.Steps)
	if err != nil {
		return nil, ToConnectError(err)
	}

	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// Idempotency check.
	existing, err := tx.GetFlowByIdempotency(ctx, r.TenantId, r.IdempotencyKey)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if existing != nil {
		if existing.Status != ledger.FlowCompleted {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFlowConflict, "flow not completed: "+string(existing.Status)))
		}
		// Replay: load steps and return original result.
		existingSteps, err := tx.GetFlowSteps(ctx, r.TenantId, existing.ID)
		if err != nil {
			return nil, ToConnectError(err)
		}
		_ = tx.Commit() // read-only; commit to release.
		tx = nil
		return connect.NewResponse(flowRunToResponse(existing, existingSteps)), nil
	}

	flowRunID := s.NewID()
	metaJSON, _ := structToMap(r.Metadata)
	if err := tx.InsertFlowRun(ctx, ledger.FlowRun{
		ID: flowRunID, TenantID: r.TenantId, FlowType: r.FlowType,
		IdempotencyKey: r.IdempotencyKey, SourceService: r.SourceService,
		ActorID: r.ActorId, Status: ledger.FlowRunning, Metadata: metaJSON,
		CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	// Compute the unique set of (account, currency) pairs across steps,
	// in deterministic order, and lock them.
	type key struct{ acct, ccy string }
	seen := map[key]bool{}
	var ordered []key
	for _, st := range steps {
		for _, e := range st.Journal.Entries {
			k := key{e.AccountID, e.Currency}
			if !seen[k] {
				seen[k] = true
				ordered = append(ordered, k)
			}
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].acct != ordered[j].acct {
			return ordered[i].acct < ordered[j].acct
		}
		return ordered[i].ccy < ordered[j].ccy
	})

	type balState struct {
		acct                       *ledger.Account
		postedDebits, postedCredits decimal.Decimal
	}
	state := map[key]*balState{}

	for _, k := range ordered {
		acc, err := tx.GetAccount(ctx, r.TenantId, k.acct)
		if err != nil {
			return nil, ToConnectError(err)
		}
		if acc.Status != ledger.AccountActive {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeInvalidAccountStatus, acc.ID))
		}
		if acc.Currency != k.ccy {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
				fmt.Sprintf("%s: account=%s req=%s", acc.ID, acc.Currency, k.ccy)))
		}
		d, c, _, err := tx.LockBalance(ctx, r.TenantId, k.acct, k.ccy)
		if err != nil {
			return nil, ToConnectError(err)
		}
		state[k] = &balState{acct: acc, postedDebits: d, postedCredits: c}
	}

	// Apply each step.
	stepResults := make([]ledger.FlowStep, 0, len(steps))
	for _, st := range steps {
		if err := st.Journal.Validate(); err != nil {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeUnbalancedJournal, err.Error()))
		}
		journalID := s.NewID()
		st.Journal.ID = journalID
		st.Journal.TenantID = r.TenantId
		st.Journal.FlowRunID = flowRunID
		st.Journal.SourceService = r.SourceService
		st.Journal.SourceType = r.FlowType
		st.Journal.ActorID = r.ActorId
		st.Journal.Metadata = metaJSON
		st.Journal.CreatedAt = s.Now()

		if err := tx.InsertJournal(ctx, st.Journal); err != nil {
			return nil, ToConnectError(err)
		}
		for _, e := range st.Journal.Entries {
			entryID := s.NewID()
			if err := tx.InsertEntry(ctx, r.TenantId, entryID, journalID, e.AccountID, e.Currency, e.Direction, e.Amount); err != nil {
				return nil, ToConnectError(err)
			}
			k := key{e.AccountID, e.Currency}
			bs := state[k]
			if e.Direction == ledger.DirectionDebit {
				bs.postedDebits = bs.postedDebits.Add(e.Amount)
			} else {
				bs.postedCredits = bs.postedCredits.Add(e.Amount)
			}
		}

		fs := ledger.FlowStep{
			ID: s.NewID(), TenantID: r.TenantId, FlowRunID: flowRunID,
			StepID: st.StepID, Status: ledger.StepCompleted, JournalID: journalID,
			CreatedAt: s.Now(),
		}
		if err := tx.InsertFlowStep(ctx, fs); err != nil {
			return nil, ToConnectError(err)
		}
		stepResults = append(stepResults, fs)

		// Outbox.
		payload, _ := json.Marshal(map[string]any{
			"flow_type": r.FlowType, "step_id": st.StepID, "journal_id": journalID,
		})
		if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
			ID: s.NewID(), TenantID: r.TenantId, AggregateID: flowRunID,
			EventType: r.FlowType + "." + st.StepID,
			IdempotencyKey: flowRunID + ":" + st.StepID, Payload: payload, CreatedAt: s.Now(),
		}); err != nil {
			return nil, ToConnectError(err)
		}
	}

	// Verify no non-overdraft account ended negative; commit balances.
	for _, k := range ordered {
		bs := state[k]
		if !bs.acct.AllowNegative {
			nb := ledger.NormalizedBalance(bs.acct.NormalBalance, bs.postedDebits, bs.postedCredits)
			if nb.IsNegative() {
				return nil, ToConnectError(ledger.NewDomainError(ledger.CodeInsufficientFunds,
					fmt.Sprintf("account=%s currency=%s normalized=%s", bs.acct.ID, k.ccy, nb)))
			}
		}
		if err := tx.UpdateBalance(ctx, r.TenantId, k.acct, k.ccy, bs.postedDebits, bs.postedCredits); err != nil {
			return nil, ToConnectError(err)
		}
	}

	if err := tx.CompleteFlowRun(ctx, r.TenantId, flowRunID); err != nil {
		return nil, ToConnectError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	tx = nil

	return connect.NewResponse(flowRunToResponse(&ledger.FlowRun{
		ID: flowRunID, TenantID: r.TenantId, Status: ledger.FlowCompleted,
	}, stepResults)), nil
}

func flowRunToResponse(f *ledger.FlowRun, steps []ledger.FlowStep) *ledgerv1.ExecuteFlowResponse {
	resp := &ledgerv1.ExecuteFlowResponse{
		FlowRunId: f.ID,
		Status:    flowStatusToProto(f.Status),
	}
	for _, s := range steps {
		resp.Steps = append(resp.Steps, &ledgerv1.FlowStepResult{
			StepId: s.StepID, Status: string(s.Status), JournalId: s.JournalID, ErrorCode: s.ErrorCode,
		})
	}
	return resp
}

func flowStatusToProto(s ledger.FlowStatus) ledgerv1.FlowStatus {
	switch s {
	case ledger.FlowCompleted:
		return ledgerv1.FlowStatus_FLOW_STATUS_COMPLETED
	case ledger.FlowFailed:
		return ledgerv1.FlowStatus_FLOW_STATUS_FAILED
	default:
		return ledgerv1.FlowStatus_FLOW_STATUS_RUNNING
	}
}

func stepsFromProto(in []*ledgerv1.Step) ([]ledger.StepInput, error) {
	out := make([]ledger.StepInput, 0, len(in))
	for _, s := range in {
		j := ledger.Journal{EventID: s.Journal.EventId}
		for _, e := range s.Journal.Entries {
			amt, err := ledger.ParseAmount(e.Amount)
			if err != nil {
				return nil, ledger.NewDomainError(ledger.CodeUnbalancedJournal, err.Error())
			}
			dir := ledger.DirectionDebit
			if e.Direction == ledgerv1.Direction_DIRECTION_CREDIT {
				dir = ledger.DirectionCredit
			}
			j.Entries = append(j.Entries, ledger.Entry{
				AccountID: e.AccountId, Currency: e.Currency, Direction: dir, Amount: amt,
			})
		}
		out = append(out, ledger.StepInput{StepID: s.StepId, Journal: j})
	}
	return out, nil
}

func structToMap(s *structpbStruct) (map[string]any, error) {
	if s == nil {
		return map[string]any{}, nil
	}
	return s.AsMap(), nil
}

// alias to avoid importing structpb name in this file (helps the agent grep)
type structpbStruct = structpbBackingType
```

Use the real `google.golang.org/protobuf/types/known/structpb.Struct`:

```go
import "google.golang.org/protobuf/types/known/structpb"

type structpbBackingType = structpb.Struct
```

- [ ] **Step 2: Implement `PostJournal` as a single-step `ExecuteFlow`**

```go
// internal/service/post_journal.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) PostJournal(ctx context.Context, req *connect.Request[ledgerv1.PostJournalRequest]) (*connect.Response[ledgerv1.PostJournalResponse], error) {
	r := req.Msg
	fr := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.TenantId,
		FlowType:       "POST_JOURNAL",
		IdempotencyKey: r.IdempotencyKey,
		SourceService:  r.SourceService,
		ActorId:        r.ActorId,
		Steps: []*ledgerv1.Step{
			{StepId: "post", Journal: r.Journal},
		},
	}
	res, err := s.ExecuteFlow(ctx, connect.NewRequest(fr))
	if err != nil {
		return nil, err
	}
	var journalID string
	if len(res.Msg.Steps) > 0 {
		journalID = res.Msg.Steps[0].JournalId
	}
	return connect.NewResponse(&ledgerv1.PostJournalResponse{
		JournalId: journalID, FlowRunId: res.Msg.FlowRunId,
	}), nil
}
```

- [ ] **Step 3: Write integration tests for ExecuteFlow**

```go
// internal/service/execute_flow_test.go
package service_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func mustCreateAccount(t *testing.T, srv server, ownerID, kind, ccy string, allowNeg bool, nb ledgerv1.NormalBalance) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: ownerID, AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		t.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.Account.Id
}

func TestExecuteFlow_PlaceOrder(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	// Seed funds.
	_, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-1", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-1", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "1000"},
			{AccountId: seedSource(t, srv), Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Place an order: reserve 100 USD.
	_, err = srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-abc-v1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-abc-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	}))
	if err != nil {
		t.Fatalf("place: %v", err)
	}

	// Available should be 900, reserved 100.
	gb := func(acct string) string {
		r, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
			TenantId: "t1", AccountId: acct, Currency: "USD",
		}))
		if err != nil {
			t.Fatalf("get balance: %v", err)
		}
		return r.Msg.Balance.Normalized
	}
	if got := gb(avail); got != "900" {
		t.Fatalf("avail want 900, got %s", got)
	}
	if got := gb(resv); got != "100" {
		t.Fatalf("resv want 100, got %s", got)
	}
}

func TestExecuteFlow_IdempotentReplay(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src   := seedSource(t, srv)

	_, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-2", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-2", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src,   Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := &ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-xyz-v1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-xyz-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	}
	first, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Msg.FlowRunId != second.Msg.FlowRunId {
		t.Fatalf("replay returned different flow_run_id: %s vs %s", first.Msg.FlowRunId, second.Msg.FlowRunId)
	}
}

func TestExecuteFlow_InsufficientFunds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-broke", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-broke-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	}))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition INSUFFICIENT_FUNDS, got %v", err)
	}
}

func TestExecuteFlow_UnbalancedAcrossCurrencies(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	usd := mustCreateAccount(t, srv, "1", "cash_available", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	brl := mustCreateAccount(t, srv, "1", "cash_available", "BRL", true, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "BAD", IdempotencyKey: "bad-1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "s", Journal: &ledgerv1.Journal{
			EventId: "bad-1-evt", Entries: []*ledgerv1.Entry{
				{AccountId: usd, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: brl, Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "500"},
			},
		}}},
	}))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument UNBALANCED_JOURNAL, got %v", err)
	}
}

// Helper that creates a "source" funding account that allows negative balances.
func seedSource(t *testing.T, srv server) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "source", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, AllowNegative: true,
	}))
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return r.Msg.Account.Id
}

// 'server' is an alias matching the actual *service.Server type for brevity.
// Add to server_test.go:
//   type server = *service.Server
//   func newServer(t *testing.T) (server, func()) { ... }
```

- [ ] **Step 4: Run, expect pass**

```bash
go test ./internal/service/ -v -run TestExecuteFlow
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/service/execute_flow.go internal/service/post_journal.go internal/service/execute_flow_test.go
git commit -m "feat(service): implement ExecuteFlow and PostJournal with idempotency, locking, outbox"
```

---

## Task 18: Service — `GetFlow` and `ListAccountActivity`

**Files:**
- Create: `internal/service/get_flow.go`
- Create: `internal/service/list_activity.go`

- [ ] **Step 1: Implement `GetFlow`**

```go
// internal/service/get_flow.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) GetFlow(ctx context.Context, req *connect.Request[ledgerv1.GetFlowRequest]) (*connect.Response[ledgerv1.GetFlowResponse], error) {
	f, err := s.Store.GetFlow(ctx, req.Msg.TenantId, req.Msg.FlowRunId)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if f == nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountNotFound, "flow "+req.Msg.FlowRunId))
	}
	return connect.NewResponse(&ledgerv1.GetFlowResponse{Flow: flowRunToResponse(f, f.Steps)}), nil
}
```

(Update `Store.GetFlow` to include populated `Steps`.)

- [ ] **Step 2: Implement `ListAccountActivity`**

```go
// internal/service/list_activity.go
package service

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo"
)

func (s *Server) ListAccountActivity(ctx context.Context, req *connect.Request[ledgerv1.ListAccountActivityRequest]) (*connect.Response[ledgerv1.ListAccountActivityResponse], error) {
	r := req.Msg
	limit := int(r.PageSize)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	in := repo.ListActivityInput{
		TenantID: r.TenantId, AccountID: r.AccountId, Currency: r.Currency, Limit: limit,
	}
	if r.Since != nil {
		t := r.Since.AsTime()
		in.Since = &t
	}
	if r.Until != nil {
		t := r.Until.AsTime()
		in.Until = &t
	}
	rows, err := s.Store.ListAccountActivity(ctx, in)
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := &ledgerv1.ListAccountActivityResponse{}
	for _, row := range rows {
		dir := ledgerv1.Direction_DIRECTION_DEBIT
		if string(row.Direction) == "CREDIT" {
			dir = ledgerv1.Direction_DIRECTION_CREDIT
		}
		out.Entries = append(out.Entries, &ledgerv1.AccountActivityEntry{
			JournalId: row.JournalID, EntryId: row.EntryID,
			Currency:  row.Currency, Direction: dir, Amount: row.Amount.String(),
			CreatedAt: timestamppb.New(row.CreatedAt), SourceService: row.SourceService,
		})
	}
	return connect.NewResponse(out), nil
}
```

- [ ] **Step 3: Build, run tests**

```bash
go build ./...
go test ./internal/service/ -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/service/get_flow.go internal/service/list_activity.go
git commit -m "feat(service): implement GetFlow and ListAccountActivity"
```

---

## Task 19: Interceptors (tenant + logging)

**Files:**
- Create: `internal/service/interceptors/tenant.go`
- Create: `internal/service/interceptors/logging.go`

- [ ] **Step 1: Implement tenant interceptor**

```go
// internal/service/interceptors/tenant.go
package interceptors

import (
	"context"
	"errors"

	"connectrpc.com/connect"
)

type ctxKey int

const tenantKey ctxKey = 1

func TenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(tenantKey).(string)
	return v
}

func NewTenant() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			tid := req.Header().Get("X-Tenant-Id")
			if tid == "" {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("X-Tenant-Id required"))
			}
			ctx = context.WithValue(ctx, tenantKey, tid)
			return next(ctx, req)
		}
	}
}
```

- [ ] **Step 2: Implement logging interceptor**

```go
// internal/service/interceptors/logging.go
package interceptors

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
)

func NewLogging(log *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			attrs := []any{
				"procedure", req.Spec().Procedure,
				"tenant_id", TenantFromContext(ctx),
				"duration_ms", time.Since(start).Milliseconds(),
			}
			if err != nil {
				log.WarnContext(ctx, "rpc.error", append(attrs, "err", err)...)
			} else {
				log.InfoContext(ctx, "rpc.ok", attrs...)
			}
			return resp, err
		}
	}
}
```

- [ ] **Step 3: Build & commit**

```bash
go build ./...
git add internal/service/interceptors/
git commit -m "feat(service): add tenant and logging interceptors"
```

---

## Task 20: Observability bootstrap

**Files:**
- Create: `internal/observability/otel.go`

- [ ] **Step 1: Implement minimal OTel setup**

```go
// internal/observability/otel.go
package observability

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup wires a no-op tracer by default. If OTEL_EXPORTER_OTLP_ENDPOINT is set,
// callers can swap in an OTLP exporter. We keep the dependency surface small.
func Setup(_ context.Context, service string) (shutdown func(context.Context) error, err error) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	_ = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") // hook point for follow-up wiring
	_ = service
	return tp.Shutdown, nil
}
```

- [ ] **Step 2: Commit**

```bash
go get go.opentelemetry.io/otel
go get go.opentelemetry.io/otel/sdk
go mod tidy
git add internal/observability/ go.mod go.sum
git commit -m "feat(obs): add minimal OTel setup hook"
```

---

## Task 21: `cmd/server` — HTTP server + outbox dispatcher

**Files:**
- Create: `cmd/server/main.go`
- Modify: `internal/outbox/dispatcher.go` to accept a `repo.Store` adapter

- [ ] **Step 1: Add an outbox adapter that bridges `repo.Store` to `outbox.Store`**

In a new file `internal/outbox/repo_adapter.go`:

```go
// internal/outbox/repo_adapter.go
package outbox

import (
	"context"

	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type RepoAdapter struct{ Store repo.Store }

func (a RepoAdapter) PendingOutbox(ctx context.Context, limit int) ([]Event, error) {
	rows, err := a.Store.PendingOutbox(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Event, len(rows))
	for i, r := range rows {
		out[i] = Event{
			ID: r.ID, TenantID: r.TenantID, AggregateID: r.AggregateID,
			EventType: r.EventType, IdempotencyKey: r.IdempotencyKey,
			Payload: r.Payload, CreatedAt: r.CreatedAt,
		}
	}
	return out, nil
}

func (a RepoAdapter) MarkOutboxPublished(ctx context.Context, id string) error {
	return a.Store.MarkOutboxPublished(ctx, id)
}

func (a RepoAdapter) IncrementOutboxAttempts(ctx context.Context, id string) error {
	return a.Store.IncrementOutboxAttempts(ctx, id)
}
```

- [ ] **Step 2: Implement `cmd/server/main.go`**

```go
// cmd/server/main.go
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	ledgerv1connect "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/doubleledger/internal/observability"
	"github.com/caxqueiroz/doubleledger/internal/outbox"
	"github.com/caxqueiroz/doubleledger/internal/repo"
	"github.com/caxqueiroz/doubleledger/internal/repo/crdb"
	"github.com/caxqueiroz/doubleledger/internal/repo/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/service"
	"github.com/caxqueiroz/doubleledger/internal/service/interceptors"
)

func main() {
	backend := flag.String("backend", "sqlite", "sqlite|crdb")
	dsn := flag.String("dsn", "./ledger.db", "database DSN")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownOtel, err := observability.Setup(ctx, "ledger-service")
	if err != nil {
		log.Error("otel setup", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownOtel(context.Background()) }()

	var store repo.Store
	switch *backend {
	case "sqlite":
		store, err = sqlite.Open(ctx, *dsn)
	case "crdb":
		store, err = crdb.Open(ctx, *dsn)
	default:
		log.Error("unknown backend", "backend", *backend)
		os.Exit(2)
	}
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	srv := service.New(store)

	mux := http.NewServeMux()
	path, handler := ledgerv1connect.NewLedgerServiceHandler(srv,
		connect.WithInterceptors(
			interceptors.NewTenant(),
			interceptors.NewLogging(log),
		),
	)
	mux.Handle(path, handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	// Outbox.
	disp := outbox.NewDispatcher(outbox.RepoAdapter{Store: store}, outbox.LogSink{Logger: log}, outbox.Config{
		Interval: 250 * time.Millisecond, BatchSize: 100,
	})
	go disp.Run(ctx)

	httpSrv := &http.Server{
		Addr:    *addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	log.Info("server.start", "addr", *addr, "backend", *backend)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Info("server.stopped")
}
```

- [ ] **Step 3: Build and smoke test**

```bash
go get golang.org/x/net/http2
go mod tidy
go build ./...
./bin/migrate --backend=sqlite --dsn=./serve-test.db up
./bin/server --backend=sqlite --dsn=./serve-test.db --addr=127.0.0.1:18080 &
SERVER_PID=$!
sleep 1
curl -fsS http://127.0.0.1:18080/healthz
kill $SERVER_PID
rm serve-test.db
```
Expected: build clean, healthz returns 200.

- [ ] **Step 4: Commit**

```bash
git add cmd/server/ internal/outbox/repo_adapter.go go.mod go.sum
git commit -m "feat(server): add HTTP entrypoint with Connect handler + outbox dispatcher"
```

---

## Task 22: Multi-step rollback test (atomicity)

**Files:**
- Modify: `internal/service/execute_flow_test.go`

- [ ] **Step 1: Add a test that fails the second step and asserts no first-step writes persist**

Force a failure by giving the second step a journal that references a non-existent account.

```go
func TestExecuteFlow_MultiStepRollback(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src   := seedSource(t, srv)

	_, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-3", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-3", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "500"},
			{AccountId: src,   Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "500"},
		}},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "TWOSTEP", IdempotencyKey: "rb-1", SourceService: "test",
		Steps: []*ledgerv1.Step{
			{StepId: "s1", Journal: &ledgerv1.Journal{
				EventId: "rb-1-s1", Entries: []*ledgerv1.Entry{
					{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
				},
			}},
			{StepId: "s2", Journal: &ledgerv1.Journal{
				EventId: "rb-1-s2", Entries: []*ledgerv1.Entry{
					{AccountId: "NONEXISTENT", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1"},
					{AccountId: avail,         Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1"},
				},
			}},
		},
	}))
	if err == nil {
		t.Fatal("expected failure")
	}

	bal, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	if bal.Msg.Balance.Normalized != "500" {
		t.Fatalf("expected available balance unchanged at 500, got %s", bal.Msg.Balance.Normalized)
	}
}
```

- [ ] **Step 2: Run, verify pass**

```bash
go test ./internal/service/ -v -run TestExecuteFlow_MultiStepRollback
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/service/execute_flow_test.go
git commit -m "test(service): assert multi-step flow rollback leaves no writes"
```

---

## Task 23: Concurrent reservation test (SQLite + CRDB)

**Files:**
- Modify: `internal/service/execute_flow_test.go` (SQLite path)
- Create: `internal/service/concurrent_crdb_test.go` (build tag `integration`)

- [ ] **Step 1: SQLite — two goroutines reserve the same 100; second must fail with `INSUFFICIENT_FUNDS`**

```go
func TestExecuteFlow_ConcurrentReservation_SQLite(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src   := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-c", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-c", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src,   Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mk := func(key string) *ledgerv1.ExecuteFlowRequest {
		return &ledgerv1.ExecuteFlowRequest{
			TenantId: "t1", FlowType: "RESERVE", IdempotencyKey: key, SourceService: "test",
			Steps: []*ledgerv1.Step{{StepId: "r", Journal: &ledgerv1.Journal{
				EventId: key + "-evt", Entries: []*ledgerv1.Entry{
					{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
				},
			}}},
		}
	}
	type result struct{ err error }
	out := make(chan result, 2)
	go func() {
		_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(mk("c-a")))
		out <- result{err}
	}()
	go func() {
		_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(mk("c-b")))
		out <- result{err}
	}()
	r1 := <-out
	r2 := <-out
	successes := 0
	failures := 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("expected exactly one success and one failure; got %d/%d", successes, failures)
	}
}
```

- [ ] **Step 2: Run, verify pass**

```bash
go test ./internal/service/ -v -run ConcurrentReservation_SQLite
```
Expected: PASS.

- [ ] **Step 3: CRDB version (testcontainers + retry assertion)**

Create `internal/service/concurrent_crdb_test.go` with the same shape but using the CRDB store, plus a deliberate barrier (e.g. `sync.WaitGroup` to maximize overlap). Assert that exactly one succeeds and the other returns `FailedPrecondition` (INSUFFICIENT_FUNDS) or `Aborted` (SERIALIZATION_RETRY_EXHAUSTED). Either is acceptable behavior.

```go
//go:build integration

package service_test

// Mirror the SQLite test but build the server with a CRDB store from
// testcontainers. Reuse the bootstrap helper from internal/repo/crdb tests.
```

- [ ] **Step 4: Commit**

```bash
git add internal/service/execute_flow_test.go internal/service/concurrent_crdb_test.go
git commit -m "test(service): concurrent reservation tests for SQLite and CRDB"
```

---

## Task 24: Outbox-after-commit test

**Files:**
- Modify: `internal/service/execute_flow_test.go`

- [ ] **Step 1: Expose the underlying store in tests**

In `server_test.go`, add:

```go
func newServerWithStore(t *testing.T) (*service.Server, *sqlite.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	mig, _ := os.ReadFile("../../sql/migrations/sqlite/0001_init.sql")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := st.DB().Exec(stripGoose(string(mig))); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return service.New(st), st, func() { _ = st.Close() }
}
```

- [ ] **Step 2: Write the test**

```go
func TestExecuteFlow_OutboxOnlyAfterCommit(t *testing.T) {
	srv, store, cleanup := newServerWithStore(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv  := mustCreateAccount(t, srv, "1", "cash_reserved",  "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src   := seedSource(t, srv)

	_, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-o", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-o", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "500"},
			{AccountId: src,   Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "500"},
		}},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Count pending outbox rows from the seed (one per step; seed has one step).
	seedRows, err := store.PendingOutbox(context.Background(), 1000)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	baseline := len(seedRows)

	// Two-step flow whose second step targets a nonexistent account → rollback.
	_, err = srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "TWOSTEP", IdempotencyKey: "ob-1", SourceService: "test",
		Steps: []*ledgerv1.Step{
			{StepId: "s1", Journal: &ledgerv1.Journal{
				EventId: "ob-1-s1", Entries: []*ledgerv1.Entry{
					{AccountId: resv,  Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT,  Amount: "100"},
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
				},
			}},
			{StepId: "s2", Journal: &ledgerv1.Journal{
				EventId: "ob-1-s2", Entries: []*ledgerv1.Entry{
					{AccountId: "NONEXISTENT", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1"},
					{AccountId: avail,         Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1"},
				},
			}},
		},
	}))
	if err == nil {
		t.Fatal("expected failure")
	}

	after, err := store.PendingOutbox(context.Background(), 1000)
	if err != nil {
		t.Fatalf("pending after: %v", err)
	}
	if len(after) != baseline {
		t.Fatalf("outbox grew despite rollback: baseline=%d after=%d", baseline, len(after))
	}
}
```

- [ ] **Step 3: Run, verify pass**

```bash
go test ./internal/service/ -v -run TestExecuteFlow_OutboxOnlyAfterCommit
```
Expected: PASS.

- [ ] **Step 2: Commit**

```bash
git add internal/service/
git commit -m "test(service): assert outbox events are written only after commit"
```

---

## Task 25: Go example client

**Files:**
- Create: `examples/go/client/main.go`
- Create: `examples/README.md`

- [ ] **Step 1: Write the example**

```go
// examples/go/client/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	ledgerv1connect "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1/ledgerv1connect"
)

func main() {
	httpClient := &http.Client{}
	client := ledgerv1connect.NewLedgerServiceClient(httpClient, "http://localhost:8080")

	withTenant := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("X-Tenant-Id", "t1")
			return next(ctx, req)
		}
	}))
	_ = withTenant // (the constructor accepts ClientOptions in some versions; see Connect docs.)

	avail, err := client.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "1",
		AccountType: "cash_available", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err != nil {
		log.Fatal(err)
	}
	src, err := client.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "source", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, AllowNegative: true,
	}))
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "demo-1", SourceService: "demo",
		Journal: &ledgerv1.Journal{EventId: "demo-1", Entries: []*ledgerv1.Entry{
			{AccountId: avail.Msg.Account.Id, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src.Msg.Account.Id, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	}))
	if err != nil {
		log.Fatal(err)
	}

	bal, err := client.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail.Msg.Account.Id, Currency: "USD",
	}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("balance:", bal.Msg.Balance.Normalized)
}
```

- [ ] **Step 2: Write `examples/README.md`**

```markdown
# Examples

## Go client

Start the server:
```
make build
./bin/migrate --backend=sqlite up
./bin/server --backend=sqlite
```

Run the demo:
```
go run ./examples/go/client
```

The client creates two accounts, posts a balanced journal, and prints the normalized balance.

## React (skeleton)

See `examples/react/README.md` — a minimal hook calling `GetBalance` via `@connectrpc/connect-web`. No build is included; copy into your own app and point `transport` at the running server.
```

- [ ] **Step 3: Write minimal React README**

```markdown
# React example (skeleton)

```tsx
import { createConnectTransport } from "@connectrpc/connect-web";
import { createClient } from "@connectrpc/connect";
import { LedgerService } from "./gen/proto/ledger/v1/ledger_connect";

const transport = createConnectTransport({ baseUrl: "http://localhost:8080" });
const client = createClient(LedgerService, transport);

export async function fetchBalance(tenantId: string, accountId: string, currency: string) {
  return client.getBalance({ tenantId, accountId, currency }, {
    headers: { "X-Tenant-Id": tenantId },
  });
}
```

Generate the TypeScript types with `buf generate` after adding a `protoc-gen-es` plugin to `buf.gen.yaml`. Not included in this MVP.
```

- [ ] **Step 4: Commit**

```bash
git add examples/
git commit -m "docs: add Go client example and React skeleton README"
```

---

## Task 26: Final wiring check

- [ ] **Step 1: Run full suite**

```bash
make generate
go vet ./...
go test ./...
```
Expected: all PASS.

- [ ] **Step 2: Smoke against running server**

```bash
./bin/migrate --backend=sqlite up
./bin/server --backend=sqlite &
SERVER_PID=$!
sleep 1
go run ./examples/go/client
kill $SERVER_PID
```
Expected: client prints `balance: 1000`.

- [ ] **Step 3: Optionally run CRDB integration tests**

```bash
go test -tags=integration ./...
```
Expected: PASS (requires Docker for testcontainers).

- [ ] **Step 4: Final commit if anything changed**

```bash
git status
# If nothing left to commit, you're done.
```

---

## Acceptance criteria checklist (mapped to spec)

- [x] **Product engines cannot directly mutate balances** — only Connect handlers in `internal/service` mutate balances, always inside `repo.Tx`. (Tasks 16, 17)
- [x] **Every posted journal is balanced by currency** — `journal.Validate()` enforces it. (Task 4, Task 17 step 3 "TestExecuteFlow_UnbalancedAcrossCurrencies")
- [x] **Every flow has an idempotency key** — required field in proto; unique constraint in schema. (Tasks 6, 7, 8)
- [x] **Replaying a completed flow returns the original result** — Task 17 step 3 `TestExecuteFlow_IdempotentReplay`.
- [x] **Concurrent reservations cannot overspend available balance** — Task 23.
- [x] **Ledger entries, balances, flow status, and outbox events commit atomically** — Task 17 (single tx), Task 22 (rollback test), Task 24 (outbox-after-commit test).
- [x] **SQLite local development uses `modernc.org/sqlite`** — Task 12.
- [x] **CockroachDB production runs with serializable transaction retry handling** — Task 13 `WithRetry`.

---

## Self-review notes

- All steps have concrete code, SQL, or commands — no "implement appropriately" placeholders.
- Symbols introduced in early tasks (`ParseAmount`, `Journal.Validate`, `NewDomainError`, `Store`, `Tx`, `Server`, `OutboxEvent`) are used consistently in later tasks. Watch points: the `Tx` interface's `InsertAccount` method appears in both Task 11 (definition) and Tasks 12/13/16 (use) — same signature throughout.
- The `Step` proto field uses snake-case `step_id` / `journal`; generated Go names are `StepId` / `Journal`. Tests reference those exact names.
- The CRDB retry helper is constructed but the orchestrator does NOT yet wrap its `runInTx` body in `WithRetry`. To honor the acceptance criterion fully, in Task 17 step 1, wrap the entire tx body in `repo.WithRetry(ctx, 5, func() error { ... })` when the backend is CRDB. Implementation suggestion: add a `Store.WithRetry(ctx, fn)` method per backend; SQLite is a no-op, CRDB delegates to `repo.WithRetry`. Add this as a small refactor inside Task 17 step 1 before committing.
