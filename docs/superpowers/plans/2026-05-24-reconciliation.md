# Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a reconciliation feature that ingests external transaction records, matches them deterministically against ledger journals by `event_id`, surfaces three kinds of discrepancies (`MISSING_IN_LEDGER`, `MISSING_IN_EXTERNAL`, `AMOUNT_MISMATCH`), and lets operators resolve each — optionally posting an adjustment journal in the same transaction via `executeFlowInTx`.

**Architecture:** Three new tables (`external_records`, `reconciliation_batches`, `discrepancies`) on both backends. A new `internal/recon/` package holding ingest, matcher, and resolver logic. Five new Connect-RPCs (`IngestExternalRecords`, `RunReconciliation`, `GetReconciliationBatch`, `ListDiscrepancies`, `ResolveDiscrepancy`). Matcher uses one in-memory pass after loading external records and journals filtered by `(tenant, source, window)`. All money movement still goes through `ExecuteFlow`.

**Tech Stack:** Go 1.26, Connect-RPC, sqlc per-dialect, goose, `shopspring/decimal`, SQLite, CockroachDB.

**Design doc:** `docs/superpowers/specs/2026-05-24-reconciliation-design.md`

---

## File map

```
internal/ledger/recon.go                                 NEW
internal/ledger/errors.go                                MODIFY (3 new codes)

sql/migrations/sqlite/0005_reconciliation.sql            NEW
sql/migrations/crdb/0005_reconciliation.sql              NEW

sql/queries/sqlite/external_records.sql                  NEW
sql/queries/crdb/external_records.sql                    NEW
sql/queries/sqlite/reconciliation_batches.sql            NEW
sql/queries/crdb/reconciliation_batches.sql              NEW
sql/queries/sqlite/discrepancies.sql                     NEW
sql/queries/crdb/discrepancies.sql                       NEW
gen/{sqlite,crdb}/...                                     REGEN

internal/repo/repo.go                                    MODIFY (Store + Tx extensions)
internal/repo/sqlite/{store,tx,conv}.go                  MODIFY
internal/repo/crdb/{store,tx,conv}.go                    MODIFY
internal/service/server_test.go                          MODIFY (apply migration 0005)
internal/scheduler/scheduler_test.go                     MODIFY (apply migration 0005)

proto/ledger/v1/ledger.proto                             MODIFY (5 RPCs + messages)
gen/proto/...                                            REGEN

internal/service/errors.go                               MODIFY (map 3 new codes)
internal/service/recon_helpers.go                        NEW (proto<->domain)
internal/service/ingest_external_records.go              NEW
internal/service/run_reconciliation.go                   NEW
internal/service/get_reconciliation_batch.go             NEW
internal/service/list_discrepancies.go                   NEW
internal/service/resolve_discrepancy.go                  NEW

internal/recon/matcher.go                                NEW
internal/recon/resolver.go                               NEW
# (ingest logic is small enough to live in the service handler directly)

internal/service/recon_test.go                           NEW

examples/go/reconciliation/main.go                       NEW
examples/README.md                                       MODIFY (table)
docs/ARCHITECTURE.md                                     MODIFY (Reconciliation section)
```

---

## Task 1: Domain types + error codes

**Files:**
- Create: `internal/ledger/recon.go`
- Modify: `internal/ledger/errors.go`

- [ ] **Step 1: Add error codes**

In `internal/ledger/errors.go`, append to the existing `const ( ... )` block:

```go
	CodeDiscrepancyNotFound DomainCode = "DISCREPANCY_NOT_FOUND"
	CodeDiscrepancyClosed   DomainCode = "DISCREPANCY_CLOSED"
	CodeReconBatchNotFound  DomainCode = "RECON_BATCH_NOT_FOUND"
```

- [ ] **Step 2: Create `internal/ledger/recon.go`**

```go
// recon.go declares the reconciliation domain types.
package ledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// ExternalRecordStatus is the lifecycle of a single external record's match.
type ExternalRecordStatus string

const (
	ExternalUnmatched  ExternalRecordStatus = "UNMATCHED"
	ExternalMatched    ExternalRecordStatus = "MATCHED"
	ExternalMismatched ExternalRecordStatus = "MISMATCHED"
)

// ExternalRecord is a single transaction reported by an external source.
type ExternalRecord struct {
	ID               string
	TenantID         string
	Source           string
	ExternalRef      string
	Amount           decimal.Decimal
	Currency         string
	OccurredAt       time.Time
	AccountID        string                 // optional anchor for amount checks
	RawPayload       map[string]any
	MatchStatus      ExternalRecordStatus
	MatchedJournalID string                 // empty if unmatched
	CreatedAt        time.Time
}

// BatchStatus is the lifecycle of a reconciliation run.
type BatchStatus string

const (
	BatchRunning   BatchStatus = "RUNNING"
	BatchCompleted BatchStatus = "COMPLETED"
	BatchFailed    BatchStatus = "FAILED"
)

// ReconciliationBatch is one run of the matcher over a (source, window).
type ReconciliationBatch struct {
	ID                     string
	TenantID               string
	IdempotencyKey         string
	Source                 string
	WindowStart            time.Time
	WindowEnd              time.Time
	Status                 BatchStatus
	IngestedCount          int32
	MatchedCount           int32
	MismatchedCount        int32
	MissingInLedgerCount   int32
	MissingInExternalCount int32
	StartedAt              time.Time
	CompletedAt            time.Time
	ActorID                string
}

// DiscrepancyType classifies what doesn't agree.
type DiscrepancyType string

const (
	DiscrepancyMissingInLedger   DiscrepancyType = "MISSING_IN_LEDGER"
	DiscrepancyMissingInExternal DiscrepancyType = "MISSING_IN_EXTERNAL"
	DiscrepancyAmountMismatch    DiscrepancyType = "AMOUNT_MISMATCH"
)

// DiscrepancyStatus is the resolution state.
type DiscrepancyStatus string

const (
	DiscrepancyOpen     DiscrepancyStatus = "OPEN"
	DiscrepancyResolved DiscrepancyStatus = "RESOLVED"
	DiscrepancyIgnored  DiscrepancyStatus = "IGNORED"
)

// Closed reports whether s is a terminal status.
func (s DiscrepancyStatus) Closed() bool {
	return s == DiscrepancyResolved || s == DiscrepancyIgnored
}

// Discrepancy is one unmatched-or-mismatched pair.
type Discrepancy struct {
	ID                  string
	TenantID            string
	BatchID             string
	Type                DiscrepancyType
	ExternalRecordID    string             // empty for MISSING_IN_EXTERNAL
	JournalID           string             // empty for MISSING_IN_LEDGER
	Status              DiscrepancyStatus
	ResolutionJournalID string             // empty until resolved with an adjustment
	ResolutionNote      string
	ResolvedBy          string
	ResolvedAt          time.Time          // zero until resolved/ignored
	CreatedAt           time.Time
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
git commit -m "feat(ledger): add reconciliation domain types and error codes"
```

---

## Task 2: Migration 0005

**Files:**
- Create: `sql/migrations/sqlite/0005_reconciliation.sql`
- Create: `sql/migrations/crdb/0005_reconciliation.sql`

- [ ] **Step 1: SQLite migration**

```sql
-- sql/migrations/sqlite/0005_reconciliation.sql
-- +goose Up
CREATE TABLE external_records (
    id                  TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    source              TEXT NOT NULL,
    external_ref        TEXT NOT NULL,
    amount              TEXT NOT NULL,
    currency            TEXT NOT NULL,
    occurred_at         TEXT NOT NULL,
    account_id          TEXT,
    raw_payload         TEXT NOT NULL DEFAULT '{}',
    match_status        TEXT NOT NULL DEFAULT 'UNMATCHED' CHECK (match_status IN ('UNMATCHED','MATCHED','MISMATCHED')),
    matched_journal_id  TEXT REFERENCES ledger_journals(id),
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, source, external_ref)
);
CREATE INDEX external_records_window_idx ON external_records (tenant_id, source, occurred_at);
CREATE INDEX external_records_match_idx  ON external_records (tenant_id, source, match_status);

CREATE TABLE reconciliation_batches (
    id                          TEXT PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    idempotency_key             TEXT NOT NULL UNIQUE,
    source                      TEXT NOT NULL,
    window_start                TEXT NOT NULL,
    window_end                  TEXT NOT NULL,
    status                      TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    ingested_count              INTEGER NOT NULL DEFAULT 0,
    matched_count               INTEGER NOT NULL DEFAULT 0,
    mismatched_count            INTEGER NOT NULL DEFAULT 0,
    missing_in_ledger_count     INTEGER NOT NULL DEFAULT 0,
    missing_in_external_count   INTEGER NOT NULL DEFAULT 0,
    started_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at                TEXT,
    actor_id                    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE discrepancies (
    id                      TEXT PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    batch_id                TEXT NOT NULL REFERENCES reconciliation_batches(id),
    type                    TEXT NOT NULL CHECK (type IN ('MISSING_IN_LEDGER','MISSING_IN_EXTERNAL','AMOUNT_MISMATCH')),
    external_record_id      TEXT REFERENCES external_records(id),
    journal_id              TEXT REFERENCES ledger_journals(id),
    status                  TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED','IGNORED')),
    resolution_journal_id   TEXT REFERENCES ledger_journals(id),
    resolution_note         TEXT NOT NULL DEFAULT '',
    resolved_by             TEXT NOT NULL DEFAULT '',
    resolved_at             TEXT,
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX discrepancies_status_idx ON discrepancies (tenant_id, status, batch_id);

-- +goose Down
DROP TABLE discrepancies;
DROP TABLE reconciliation_batches;
DROP TABLE external_records;
```

- [ ] **Step 2: CRDB migration**

```sql
-- sql/migrations/crdb/0005_reconciliation.sql
-- +goose Up
CREATE TABLE external_records (
    id                  STRING PRIMARY KEY,
    tenant_id           STRING NOT NULL,
    source              STRING NOT NULL,
    external_ref        STRING NOT NULL,
    amount              DECIMAL(38, 18) NOT NULL,
    currency            STRING NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    account_id          STRING,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::JSONB,
    match_status        STRING NOT NULL DEFAULT 'UNMATCHED' CHECK (match_status IN ('UNMATCHED','MATCHED','MISMATCHED')),
    matched_journal_id  STRING REFERENCES ledger_journals(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, source, external_ref)
);
CREATE INDEX external_records_window_idx ON external_records (tenant_id, source, occurred_at);
CREATE INDEX external_records_match_idx  ON external_records (tenant_id, source, match_status);

CREATE TABLE reconciliation_batches (
    id                          STRING PRIMARY KEY,
    tenant_id                   STRING NOT NULL,
    idempotency_key             STRING NOT NULL UNIQUE,
    source                      STRING NOT NULL,
    window_start                TIMESTAMPTZ NOT NULL,
    window_end                  TIMESTAMPTZ NOT NULL,
    status                      STRING NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    ingested_count              INT8 NOT NULL DEFAULT 0,
    matched_count               INT8 NOT NULL DEFAULT 0,
    mismatched_count            INT8 NOT NULL DEFAULT 0,
    missing_in_ledger_count     INT8 NOT NULL DEFAULT 0,
    missing_in_external_count   INT8 NOT NULL DEFAULT 0,
    started_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                TIMESTAMPTZ,
    actor_id                    STRING NOT NULL DEFAULT ''
);

CREATE TABLE discrepancies (
    id                      STRING PRIMARY KEY,
    tenant_id               STRING NOT NULL,
    batch_id                STRING NOT NULL REFERENCES reconciliation_batches(id),
    type                    STRING NOT NULL CHECK (type IN ('MISSING_IN_LEDGER','MISSING_IN_EXTERNAL','AMOUNT_MISMATCH')),
    external_record_id      STRING REFERENCES external_records(id),
    journal_id              STRING REFERENCES ledger_journals(id),
    status                  STRING NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED','IGNORED')),
    resolution_journal_id   STRING REFERENCES ledger_journals(id),
    resolution_note         STRING NOT NULL DEFAULT '',
    resolved_by             STRING NOT NULL DEFAULT '',
    resolved_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX discrepancies_status_idx ON discrepancies (tenant_id, status, batch_id);

-- +goose Down
DROP TABLE discrepancies;
DROP TABLE reconciliation_batches;
DROP TABLE external_records;
```

- [ ] **Step 3: Smoke (SQLite)**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
mkdir -p bin
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/recon-mig.db
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db up
sqlite3 /tmp/recon-mig.db ".tables" | tr '\n ' ' '
echo
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db down
./bin/migrate --backend=sqlite --dsn=/tmp/recon-mig.db down
rm -rf /tmp/recon-mig.db bin
```

Expected `.tables` includes `external_records`, `reconciliation_batches`, `discrepancies`. Five down invocations reverse 0005→0001 cleanly.

- [ ] **Step 4: Commit**

```bash
git add sql/migrations/
git commit -m "feat(db): add reconciliation tables"
```

---

## Task 3: sqlc queries

**Files:**
- Create: `sql/queries/sqlite/external_records.sql`
- Create: `sql/queries/crdb/external_records.sql`
- Create: `sql/queries/sqlite/reconciliation_batches.sql`
- Create: `sql/queries/crdb/reconciliation_batches.sql`
- Create: `sql/queries/sqlite/discrepancies.sql`
- Create: `sql/queries/crdb/discrepancies.sql`
- Regenerate `gen/{sqlite,crdb}/...`

- [ ] **Step 1: SQLite external_records queries**

```sql
-- sql/queries/sqlite/external_records.sql

-- name: InsertExternalRecord :execrows
INSERT INTO external_records
    (id, tenant_id, source, external_ref, amount, currency, occurred_at, account_id, raw_payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tenant_id, source, external_ref) DO NOTHING;

-- name: ListExternalRecordsForRecon :many
SELECT * FROM external_records
WHERE tenant_id = ? AND source = ?
  AND occurred_at >= ? AND occurred_at <= ?
  AND match_status = 'UNMATCHED'
ORDER BY occurred_at ASC;

-- name: UpdateExternalRecordMatch :exec
UPDATE external_records
SET match_status = ?, matched_journal_id = ?
WHERE id = ? AND tenant_id = ?;
```

- [ ] **Step 2: CRDB external_records queries**

```sql
-- sql/queries/crdb/external_records.sql

-- name: InsertExternalRecord :execrows
INSERT INTO external_records
    (id, tenant_id, source, external_ref, amount, currency, occurred_at, account_id, raw_payload)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, source, external_ref) DO NOTHING;

-- name: ListExternalRecordsForRecon :many
SELECT * FROM external_records
WHERE tenant_id = $1 AND source = $2
  AND occurred_at >= $3 AND occurred_at <= $4
  AND match_status = 'UNMATCHED'
ORDER BY occurred_at ASC;

-- name: UpdateExternalRecordMatch :exec
UPDATE external_records
SET match_status = $1, matched_journal_id = $2
WHERE id = $3 AND tenant_id = $4;
```

- [ ] **Step 3: SQLite reconciliation_batches queries**

```sql
-- sql/queries/sqlite/reconciliation_batches.sql

-- name: InsertReconBatch :exec
INSERT INTO reconciliation_batches
    (id, tenant_id, idempotency_key, source, window_start, window_end, status, actor_id)
VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?);

-- name: GetReconBatch :one
SELECT * FROM reconciliation_batches WHERE tenant_id = ? AND id = ?;

-- name: GetReconBatchByIdempotency :one
SELECT * FROM reconciliation_batches WHERE tenant_id = ? AND idempotency_key = ?;

-- name: CompleteReconBatch :exec
UPDATE reconciliation_batches
SET status = 'COMPLETED',
    completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
    ingested_count = ?, matched_count = ?, mismatched_count = ?,
    missing_in_ledger_count = ?, missing_in_external_count = ?
WHERE id = ? AND tenant_id = ?;

-- name: ListJournalsForRecon :many
SELECT * FROM ledger_journals
WHERE tenant_id = ? AND source_service = ?
  AND created_at >= ? AND created_at <= ?
ORDER BY created_at ASC;

-- name: SumJournalEntries :one
SELECT
    CAST(COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN CAST(amount AS REAL) ELSE 0.0 END), 0.0) AS REAL) AS debits,
    CAST(COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN CAST(amount AS REAL) ELSE 0.0 END), 0.0) AS REAL) AS credits
FROM ledger_entries
WHERE tenant_id = ? AND journal_id = ? AND account_id = ? AND currency = ?;
```

- [ ] **Step 4: CRDB reconciliation_batches queries**

```sql
-- sql/queries/crdb/reconciliation_batches.sql

-- name: InsertReconBatch :exec
INSERT INTO reconciliation_batches
    (id, tenant_id, idempotency_key, source, window_start, window_end, status, actor_id)
VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', $7);

-- name: GetReconBatch :one
SELECT * FROM reconciliation_batches WHERE tenant_id = $1 AND id = $2;

-- name: GetReconBatchByIdempotency :one
SELECT * FROM reconciliation_batches WHERE tenant_id = $1 AND idempotency_key = $2 FOR UPDATE;

-- name: CompleteReconBatch :exec
UPDATE reconciliation_batches
SET status = 'COMPLETED',
    completed_at = now(),
    ingested_count = $1, matched_count = $2, mismatched_count = $3,
    missing_in_ledger_count = $4, missing_in_external_count = $5
WHERE id = $6 AND tenant_id = $7;

-- name: ListJournalsForRecon :many
SELECT * FROM ledger_journals
WHERE tenant_id = $1 AND source_service = $2
  AND created_at >= $3 AND created_at <= $4
ORDER BY created_at ASC;

-- name: SumJournalEntries :one
SELECT
    CAST(COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END), 0) AS DECIMAL(38, 18)) AS debits,
    CAST(COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END), 0) AS DECIMAL(38, 18)) AS credits
FROM ledger_entries
WHERE tenant_id = $1 AND journal_id = $2 AND account_id = $3 AND currency = $4;
```

- [ ] **Step 5: SQLite discrepancies queries**

```sql
-- sql/queries/sqlite/discrepancies.sql

-- name: InsertDiscrepancy :exec
INSERT INTO discrepancies
    (id, tenant_id, batch_id, type, external_record_id, journal_id)
VALUES (?, ?, ?, ?, ?, ?);

-- name: GetDiscrepancy :one
SELECT * FROM discrepancies WHERE tenant_id = ? AND id = ?;

-- name: ListDiscrepancies :many
SELECT * FROM discrepancies
WHERE tenant_id = ?
  AND (batch_id = ? OR ? = '')
  AND (status = ? OR ? = '')
ORDER BY created_at DESC
LIMIT ?;

-- name: ResolveDiscrepancyRow :exec
UPDATE discrepancies
SET status = ?, resolution_journal_id = ?, resolution_note = ?,
    resolved_by = ?,
    resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = ? AND tenant_id = ?;
```

- [ ] **Step 6: CRDB discrepancies queries**

```sql
-- sql/queries/crdb/discrepancies.sql

-- name: InsertDiscrepancy :exec
INSERT INTO discrepancies
    (id, tenant_id, batch_id, type, external_record_id, journal_id)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: GetDiscrepancy :one
SELECT * FROM discrepancies WHERE tenant_id = $1 AND id = $2;

-- name: LockDiscrepancy :one
SELECT * FROM discrepancies WHERE tenant_id = $1 AND id = $2 FOR UPDATE;

-- name: ListDiscrepancies :many
SELECT * FROM discrepancies
WHERE tenant_id = $1
  AND ($2::text = '' OR batch_id = $2)
  AND ($3::text = '' OR status = $3)
ORDER BY created_at DESC
LIMIT $4;

-- name: ResolveDiscrepancyRow :exec
UPDATE discrepancies
SET status = $1, resolution_journal_id = $2, resolution_note = $3,
    resolved_by = $4,
    resolved_at = now()
WHERE id = $5 AND tenant_id = $6;
```

- [ ] **Step 7: Regenerate**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" sqlc generate
go build ./gen/... ./...
```

- [ ] **Step 8: Report generated param names**

```bash
grep -A 3 "ListDiscrepanciesParams" gen/sqlite/discrepancies.sql.go gen/crdb/discrepancies.sql.go
grep -A 3 "InsertExternalRecordParams" gen/sqlite/external_records.sql.go gen/crdb/external_records.sql.go
grep -A 3 "CompleteReconBatchParams" gen/sqlite/reconciliation_batches.sql.go gen/crdb/reconciliation_batches.sql.go
```

Note the actual field names for the dual-bind `ListDiscrepancies` params (likely `BatchID`/`Column3`/`Status`/`Column5` for SQLite, and `Column2`/`Column3` for CRDB). These get used in Task 4 step 2.

- [ ] **Step 9: Commit**

```bash
git add sql/queries/ gen/sqlite/ gen/crdb/
git commit -m "feat(db): add sqlc queries for reconciliation"
```

---

## Task 4: Repository extension

**Files:**
- Modify: `internal/repo/repo.go`
- Modify: `internal/repo/sqlite/{store,tx,conv}.go`
- Modify: `internal/repo/crdb/{store,tx,conv}.go`
- Modify: `internal/service/server_test.go`
- Modify: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: Extend `Store` and `Tx` in `internal/repo/repo.go`**

Insert under existing FX section (before `Close()`):

```go
	// Reconciliation (read + write)
	InsertExternalRecord(ctx context.Context, r ledger.ExternalRecord) (inserted bool, err error)
	ListExternalRecordsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.ExternalRecord, error)
	ListJournalsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.Journal, error)
	GetReconBatch(ctx context.Context, tenantID, batchID string) (*ledger.ReconciliationBatch, error)
	ListDiscrepancies(ctx context.Context, in ListDiscrepanciesInput) ([]ledger.Discrepancy, error)
	GetDiscrepancy(ctx context.Context, tenantID, discrepancyID string) (*ledger.Discrepancy, error)
```

Add to `Tx`:

```go
	// Reconciliation (transactional)
	GetReconBatchByIdempotency(ctx context.Context, tenantID, key string) (*ledger.ReconciliationBatch, error)
	InsertReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error
	CompleteReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error
	UpdateExternalRecordMatch(ctx context.Context, tenantID, id string, status ledger.ExternalRecordStatus, journalID string) error
	SumJournalEntries(ctx context.Context, tenantID, journalID, accountID, currency string) (debits, credits decimal.Decimal, err error)
	InsertDiscrepancy(ctx context.Context, d ledger.Discrepancy) error
	LockDiscrepancy(ctx context.Context, tenantID, id string) (*ledger.Discrepancy, error)
	ResolveDiscrepancyRow(ctx context.Context, d ledger.Discrepancy) error
```

Add input type next to existing `List*Input` types:

```go
type ListDiscrepanciesInput struct {
	TenantID string
	BatchID  string // optional
	Status   string // optional
	Limit    int
}
```

- [ ] **Step 2: SQLite implementations**

Append to `internal/repo/sqlite/store.go` (read-only methods on `*Store`):

```go
func (s *Store) InsertExternalRecord(ctx context.Context, r ledger.ExternalRecord) (bool, error) {
	payload, _ := json.Marshal(r.RawPayload)
	var acct *string
	if r.AccountID != "" {
		v := r.AccountID
		acct = &v
	}
	n, err := s.q.InsertExternalRecord(ctx, sqlitestore.InsertExternalRecordParams{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount.String(), Currency: r.Currency,
		OccurredAt: r.OccurredAt.UTC().Format(sqliteTimeFormat),
		AccountID:  acct,
		RawPayload: string(payload),
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) ListExternalRecordsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.ExternalRecord, error) {
	rows, err := s.q.ListExternalRecordsForRecon(ctx, sqlitestore.ListExternalRecordsForReconParams{
		TenantID: tenantID, Source: source,
		OccurredAt:   windowStart.UTC().Format(sqliteTimeFormat),
		OccurredAt_2: windowEnd.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.ExternalRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToExternalRecord(r))
	}
	return out, nil
}

func (s *Store) ListJournalsForRecon(ctx context.Context, tenantID, source string, windowStart, windowEnd time.Time) ([]ledger.Journal, error) {
	rows, err := s.q.ListJournalsForRecon(ctx, sqlitestore.ListJournalsForReconParams{
		TenantID: tenantID, SourceService: source,
		CreatedAt:   windowStart.UTC().Format(sqliteTimeFormat),
		CreatedAt_2: windowEnd.UTC().Format(sqliteTimeFormat),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Journal, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToJournal(r))
	}
	return out, nil
}

func (s *Store) GetReconBatch(ctx context.Context, tenantID, batchID string) (*ledger.ReconciliationBatch, error) {
	row, err := s.q.GetReconBatch(ctx, sqlitestore.GetReconBatchParams{TenantID: tenantID, ID: batchID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeReconBatchNotFound, batchID)
		}
		return nil, err
	}
	return rowToReconBatch(row), nil
}

func (s *Store) ListDiscrepancies(ctx context.Context, in repo.ListDiscrepanciesInput) ([]ledger.Discrepancy, error) {
	limit := int64(in.Limit)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	// Inspect gen/sqlite/discrepancies.sql.go for the actual dual-bind field
	// names. Likely Column3/Column5 mirror BatchID/Status.
	rows, err := s.q.ListDiscrepancies(ctx, sqlitestore.ListDiscrepanciesParams{
		TenantID: in.TenantID,
		BatchID:  toNullString(in.BatchID),
		Column3:  in.BatchID,
		Status:   in.Status,
		Column5:  in.Status,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ledger.Discrepancy, 0, len(rows))
	for _, r := range rows {
		out = append(out, *rowToDiscrepancy(r))
	}
	return out, nil
}

func (s *Store) GetDiscrepancy(ctx context.Context, tenantID, discrepancyID string) (*ledger.Discrepancy, error) {
	row, err := s.q.GetDiscrepancy(ctx, sqlitestore.GetDiscrepancyParams{TenantID: tenantID, ID: discrepancyID})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeDiscrepancyNotFound, discrepancyID)
		}
		return nil, err
	}
	return rowToDiscrepancy(row), nil
}

func toNullString(s string) *string {
	if s == "" {
		return nil
	}
	v := s
	return &v
}
```

If sqlc didn't generate `BatchID *string` for the optional `batch_id = ? OR ? = ''` pattern (it may emit `string` instead), pass `in.BatchID` directly and drop the `toNullString` helper for that field — the pattern is the same as fxr_rates `ListFXRates` filtering used elsewhere.

Append `internal/repo/sqlite/tx.go` (transactional methods on `*Tx`):

```go
func (t *Tx) GetReconBatchByIdempotency(ctx context.Context, tenantID, key string) (*ledger.ReconciliationBatch, error) {
	row, err := t.q.GetReconBatchByIdempotency(ctx, sqlitestore.GetReconBatchByIdempotencyParams{TenantID: tenantID, IdempotencyKey: key})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rowToReconBatch(row), nil
}

func (t *Tx) InsertReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error {
	return t.q.InsertReconBatch(ctx, sqlitestore.InsertReconBatchParams{
		ID: b.ID, TenantID: b.TenantID, IdempotencyKey: b.IdempotencyKey,
		Source: b.Source,
		WindowStart: b.WindowStart.UTC().Format(sqliteTimeFormat),
		WindowEnd:   b.WindowEnd.UTC().Format(sqliteTimeFormat),
		ActorID:     b.ActorID,
	})
}

func (t *Tx) CompleteReconBatch(ctx context.Context, b ledger.ReconciliationBatch) error {
	return t.q.CompleteReconBatch(ctx, sqlitestore.CompleteReconBatchParams{
		IngestedCount: int64(b.IngestedCount),
		MatchedCount:  int64(b.MatchedCount),
		MismatchedCount: int64(b.MismatchedCount),
		MissingInLedgerCount:   int64(b.MissingInLedgerCount),
		MissingInExternalCount: int64(b.MissingInExternalCount),
		ID: b.ID, TenantID: b.TenantID,
	})
}

func (t *Tx) UpdateExternalRecordMatch(ctx context.Context, tenantID, id string, status ledger.ExternalRecordStatus, journalID string) error {
	var jid *string
	if journalID != "" {
		v := journalID
		jid = &v
	}
	return t.q.UpdateExternalRecordMatch(ctx, sqlitestore.UpdateExternalRecordMatchParams{
		MatchStatus:      string(status),
		MatchedJournalID: jid,
		ID:               id,
		TenantID:         tenantID,
	})
}

func (t *Tx) SumJournalEntries(ctx context.Context, tenantID, journalID, accountID, currency string) (decimal.Decimal, decimal.Decimal, error) {
	row, err := t.q.SumJournalEntries(ctx, sqlitestore.SumJournalEntriesParams{
		TenantID: tenantID, JournalID: journalID, AccountID: accountID, Currency: currency,
	})
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	return decimal.NewFromFloat(row.Debits), decimal.NewFromFloat(row.Credits), nil
}

func (t *Tx) InsertDiscrepancy(ctx context.Context, d ledger.Discrepancy) error {
	var ext, jid *string
	if d.ExternalRecordID != "" {
		v := d.ExternalRecordID
		ext = &v
	}
	if d.JournalID != "" {
		v := d.JournalID
		jid = &v
	}
	return t.q.InsertDiscrepancy(ctx, sqlitestore.InsertDiscrepancyParams{
		ID: d.ID, TenantID: d.TenantID, BatchID: d.BatchID,
		Type: string(d.Type),
		ExternalRecordID: ext,
		JournalID:        jid,
	})
}

func (t *Tx) LockDiscrepancy(ctx context.Context, tenantID, id string) (*ledger.Discrepancy, error) {
	// SQLite: BEGIN IMMEDIATE serializes writes; a plain SELECT suffices.
	row, err := t.q.GetDiscrepancy(ctx, sqlitestore.GetDiscrepancyParams{TenantID: tenantID, ID: id})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ledger.NewDomainError(ledger.CodeDiscrepancyNotFound, id)
		}
		return nil, err
	}
	return rowToDiscrepancy(row), nil
}

func (t *Tx) ResolveDiscrepancyRow(ctx context.Context, d ledger.Discrepancy) error {
	var rj *string
	if d.ResolutionJournalID != "" {
		v := d.ResolutionJournalID
		rj = &v
	}
	return t.q.ResolveDiscrepancyRow(ctx, sqlitestore.ResolveDiscrepancyRowParams{
		Status:                string(d.Status),
		ResolutionJournalID:   rj,
		ResolutionNote:        d.ResolutionNote,
		ResolvedBy:            d.ResolvedBy,
		ID:                    d.ID,
		TenantID:              d.TenantID,
	})
}
```

- [ ] **Step 3: SQLite `conv.go` helpers**

Append to `internal/repo/sqlite/conv.go`:

```go
func rowToExternalRecord(r sqlitestore.ExternalRecord) *ledger.ExternalRecord {
	amt, _ := decimal.NewFromString(r.Amount)
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.RawPayload), &meta)
	res := &ledger.ExternalRecord{
		ID: r.ID, TenantID: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: amt, Currency: r.Currency,
		OccurredAt:  parseTime(r.OccurredAt),
		RawPayload:  meta,
		MatchStatus: ledger.ExternalRecordStatus(r.MatchStatus),
		CreatedAt:   parseTime(r.CreatedAt),
	}
	if r.AccountID != nil {
		res.AccountID = *r.AccountID
	}
	if r.MatchedJournalID != nil {
		res.MatchedJournalID = *r.MatchedJournalID
	}
	return res
}

func rowToReconBatch(r sqlitestore.ReconciliationBatch) *ledger.ReconciliationBatch {
	b := &ledger.ReconciliationBatch{
		ID: r.ID, TenantID: r.TenantID, IdempotencyKey: r.IdempotencyKey,
		Source: r.Source,
		WindowStart: parseTime(r.WindowStart),
		WindowEnd:   parseTime(r.WindowEnd),
		Status: ledger.BatchStatus(r.Status),
		IngestedCount: int32(r.IngestedCount), MatchedCount: int32(r.MatchedCount),
		MismatchedCount: int32(r.MismatchedCount),
		MissingInLedgerCount: int32(r.MissingInLedgerCount),
		MissingInExternalCount: int32(r.MissingInExternalCount),
		StartedAt: parseTime(r.StartedAt),
		ActorID:   r.ActorID,
	}
	if r.CompletedAt != nil {
		b.CompletedAt = parseTime(*r.CompletedAt)
	}
	return b
}

func rowToDiscrepancy(r sqlitestore.Discrepancy) *ledger.Discrepancy {
	d := &ledger.Discrepancy{
		ID: r.ID, TenantID: r.TenantID, BatchID: r.BatchID,
		Type:           ledger.DiscrepancyType(r.Type),
		Status:         ledger.DiscrepancyStatus(r.Status),
		ResolutionNote: r.ResolutionNote,
		ResolvedBy:     r.ResolvedBy,
		CreatedAt:      parseTime(r.CreatedAt),
	}
	if r.ExternalRecordID != nil {
		d.ExternalRecordID = *r.ExternalRecordID
	}
	if r.JournalID != nil {
		d.JournalID = *r.JournalID
	}
	if r.ResolutionJournalID != nil {
		d.ResolutionJournalID = *r.ResolutionJournalID
	}
	if r.ResolvedAt != nil {
		d.ResolvedAt = parseTime(*r.ResolvedAt)
	}
	return d
}

func rowToJournal(r sqlitestore.LedgerJournal) *ledger.Journal {
	meta := map[string]any{}
	_ = json.Unmarshal([]byte(r.Metadata), &meta)
	j := &ledger.Journal{
		ID: r.ID, TenantID: r.TenantID,
		EventID: r.EventID, SourceService: r.SourceService, SourceType: r.SourceType,
		ActorID: r.ActorID, Metadata: meta,
		CreatedAt: parseTime(r.CreatedAt),
	}
	if r.FlowRunID != nil {
		j.FlowRunID = *r.FlowRunID
	}
	return j
}
```

If `rowToJournal` already exists somewhere in the SQLite repo package (it shouldn't yet), don't duplicate. Reuse the existing implementation.

- [ ] **Step 4: CRDB implementations (mirror Step 2 + Step 3)**

Same shape as SQLite but:
- `decimal.Decimal` directly (no String/parse).
- `pgtype.Timestamptz` for times — pass `pgtype.Timestamptz{Time: t.UTC(), Valid: true}`; on read use `.Time` when `.Valid`.
- `[]byte` for JSONB — `json.Marshal`/`json.Unmarshal` directly.
- Nullable strings come as `*string` (because of `emit_pointers_for_null_types: true`).
- For the dual-bind list query, sqlc emits `Column2`/`Column3` typed `string` (matches what we saw on FX rates).
- `LockDiscrepancy` calls the CRDB-only `LockDiscrepancy` (`FOR UPDATE`) query.

Append the mirror methods to `internal/repo/crdb/store.go`, `tx.go`, `conv.go`.

- [ ] **Step 5: Update test bootstrap to apply migration 0005**

In `internal/service/server_test.go`, append `"0005_reconciliation.sql"` to the migrations slice (the `newServerWithStore` helper).

In `internal/scheduler/scheduler_test.go`, do the same to its `setup` migration slice.

- [ ] **Step 6: Build + tests**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./...
go vet ./...
go test ./internal/repo/sqlite/ -v
go test ./internal/service/ -v
go test ./internal/scheduler/ -v
```

Iterate on sqlc param field-name details until clean.

- [ ] **Step 7: Commit**

```bash
git add internal/repo/ internal/service/server_test.go internal/scheduler/scheduler_test.go
git commit -m "feat(repo): reconciliation read/write methods for both backends"
```

---

## Task 5: Proto additions + stubs

**Files:**
- Modify: `proto/ledger/v1/ledger.proto`
- Create: `internal/service/recon_stubs.go`
- Regenerate `gen/proto/...`

- [ ] **Step 1: Add to `service LedgerService` block**

After `ListFXRates`:

```proto
  rpc IngestExternalRecords(IngestExternalRecordsRequest) returns (IngestExternalRecordsResponse);
  rpc RunReconciliation(RunReconciliationRequest) returns (RunReconciliationResponse);
  rpc GetReconciliationBatch(GetReconciliationBatchRequest) returns (GetReconciliationBatchResponse);
  rpc ListDiscrepancies(ListDiscrepanciesRequest) returns (ListDiscrepanciesResponse);
  rpc ResolveDiscrepancy(ResolveDiscrepancyRequest) returns (ResolveDiscrepancyResponse);
```

- [ ] **Step 2: Append messages at end of `proto/ledger/v1/ledger.proto`**

```proto
message ExternalRecord {
  string id              = 1;
  string tenant_id       = 2;
  string source          = 3;
  string external_ref    = 4;
  string amount          = 5;
  string currency        = 6;
  google.protobuf.Timestamp occurred_at = 7;
  string account_id      = 8;
  google.protobuf.Struct raw_payload = 9;
  string match_status    = 10;
  string matched_journal_id = 11;
  google.protobuf.Timestamp created_at = 12;
}

message ExternalRecordInput {
  string source          = 1 [(buf.validate.field).string.min_len = 1];
  string external_ref    = 2 [(buf.validate.field).string.min_len = 1];
  string amount          = 3 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"];
  string currency        = 4 [(buf.validate.field).string.min_len = 3];
  google.protobuf.Timestamp occurred_at = 5;
  string account_id      = 6;
  google.protobuf.Struct raw_payload = 7;
}

message IngestExternalRecordsRequest {
  string tenant_id                       = 1 [(buf.validate.field).string.min_len = 1];
  repeated ExternalRecordInput records   = 2 [(buf.validate.field).repeated.min_items = 1];
}
message IngestExternalRecordsResponse {
  int32 inserted = 1;
  int32 skipped  = 2;
}

message RunReconciliationRequest {
  string tenant_id       = 1 [(buf.validate.field).string.min_len = 1];
  string idempotency_key = 2 [(buf.validate.field).string.min_len = 1];
  string source          = 3 [(buf.validate.field).string.min_len = 1];
  google.protobuf.Timestamp window_start = 4;
  google.protobuf.Timestamp window_end   = 5;
  string actor_id        = 6;
}

message ReconciliationBatch {
  string id              = 1;
  string tenant_id       = 2;
  string source          = 3;
  google.protobuf.Timestamp window_start = 4;
  google.protobuf.Timestamp window_end   = 5;
  string status          = 6;
  int32  ingested_count  = 7;
  int32  matched_count   = 8;
  int32  mismatched_count = 9;
  int32  missing_in_ledger_count   = 10;
  int32  missing_in_external_count = 11;
  google.protobuf.Timestamp started_at   = 12;
  google.protobuf.Timestamp completed_at = 13;
  string actor_id        = 14;
}
message RunReconciliationResponse { ReconciliationBatch batch = 1; }

message GetReconciliationBatchRequest  { string tenant_id = 1; string batch_id = 2; }
message GetReconciliationBatchResponse { ReconciliationBatch batch = 1; }

message Discrepancy {
  string id                       = 1;
  string tenant_id                = 2;
  string batch_id                 = 3;
  string type                     = 4;
  string external_record_id       = 5;
  string journal_id               = 6;
  string status                   = 7;
  string resolution_journal_id    = 8;
  string resolution_note          = 9;
  string resolved_by              = 10;
  google.protobuf.Timestamp resolved_at = 11;
  google.protobuf.Timestamp created_at  = 12;
}

message ListDiscrepanciesRequest {
  string tenant_id  = 1;
  string batch_id   = 2;
  string status     = 3;
  int32  page_size  = 4;
}
message ListDiscrepanciesResponse {
  repeated Discrepancy discrepancies = 1;
}

message ResolveDiscrepancyRequest {
  string tenant_id              = 1 [(buf.validate.field).string.min_len = 1];
  string discrepancy_id         = 2 [(buf.validate.field).string.min_len = 1];
  string resolution             = 3 [(buf.validate.field).string.min_len = 1];
  ExecuteFlowRequest adjustment = 4;
  string note                   = 5;
  string idempotency_key        = 6 [(buf.validate.field).string.min_len = 1];
  string actor_id               = 7;
}
message ResolveDiscrepancyResponse { Discrepancy discrepancy = 1; }
```

- [ ] **Step 3: Regenerate**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
PATH="$(go env GOPATH)/bin:$PATH" buf generate
go build ./... 2>&1 | head -10
```

Build will fail because `*service.Server` doesn't implement the 5 new methods.

- [ ] **Step 4: Create `internal/service/recon_stubs.go`**

```go
// recon_stubs.go: temporary stubs replaced by Tasks 7-12.
package service

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) IngestExternalRecords(ctx context.Context, req *connect.Request[ledgerv1.IngestExternalRecordsRequest]) (*connect.Response[ledgerv1.IngestExternalRecordsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("IngestExternalRecords not implemented yet"))
}

func (s *Server) RunReconciliation(ctx context.Context, req *connect.Request[ledgerv1.RunReconciliationRequest]) (*connect.Response[ledgerv1.RunReconciliationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("RunReconciliation not implemented yet"))
}

func (s *Server) GetReconciliationBatch(ctx context.Context, req *connect.Request[ledgerv1.GetReconciliationBatchRequest]) (*connect.Response[ledgerv1.GetReconciliationBatchResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("GetReconciliationBatch not implemented yet"))
}

func (s *Server) ListDiscrepancies(ctx context.Context, req *connect.Request[ledgerv1.ListDiscrepanciesRequest]) (*connect.Response[ledgerv1.ListDiscrepanciesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ListDiscrepancies not implemented yet"))
}

func (s *Server) ResolveDiscrepancy(ctx context.Context, req *connect.Request[ledgerv1.ResolveDiscrepancyRequest]) (*connect.Response[ledgerv1.ResolveDiscrepancyResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ResolveDiscrepancy not implemented yet"))
}
```

- [ ] **Step 5: Build + tests**

```bash
go build ./...
go test ./internal/service/ -v
```

All existing tests must still pass.

- [ ] **Step 6: Commit**

```bash
git add proto/ gen/proto/ internal/service/recon_stubs.go
git commit -m "feat(proto): add reconciliation RPCs with stubs"
```

---

## Task 6: Error mapping + recon_helpers

**Files:**
- Modify: `internal/service/errors.go`
- Create: `internal/service/recon_helpers.go`

- [ ] **Step 1: Extend `ToConnectError`**

In `internal/service/errors.go`:

```go
		case ledger.CodeAccountNotFound, ledger.CodeReservationNotFound, ledger.CodeFXRateNotFound,
			ledger.CodeDiscrepancyNotFound, ledger.CodeReconBatchNotFound:
			code = connect.CodeNotFound
		case ledger.CodeInsufficientFunds, ledger.CodeInvalidAccountStatus, ledger.CodeReservationClosed,
			ledger.CodeDiscrepancyClosed:
			code = connect.CodeFailedPrecondition
```

Keep all other cases unchanged.

- [ ] **Step 2: Create `internal/service/recon_helpers.go`**

```go
// recon_helpers.go: proto<->domain conversions for reconciliation types.
package service

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func externalRecordToProto(r *ledger.ExternalRecord) *ledgerv1.ExternalRecord {
	return &ledgerv1.ExternalRecord{
		Id: r.ID, TenantId: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount.String(), Currency: r.Currency,
		OccurredAt: timestamppb.New(r.OccurredAt),
		AccountId:  r.AccountID,
		MatchStatus:      string(r.MatchStatus),
		MatchedJournalId: r.MatchedJournalID,
		CreatedAt:        timestamppb.New(r.CreatedAt),
	}
}

func reconBatchToProto(b *ledger.ReconciliationBatch) *ledgerv1.ReconciliationBatch {
	p := &ledgerv1.ReconciliationBatch{
		Id: b.ID, TenantId: b.TenantID, Source: b.Source,
		WindowStart: timestamppb.New(b.WindowStart),
		WindowEnd:   timestamppb.New(b.WindowEnd),
		Status: string(b.Status),
		IngestedCount: b.IngestedCount, MatchedCount: b.MatchedCount,
		MismatchedCount: b.MismatchedCount,
		MissingInLedgerCount: b.MissingInLedgerCount,
		MissingInExternalCount: b.MissingInExternalCount,
		StartedAt: timestamppb.New(b.StartedAt),
		ActorId:   b.ActorID,
	}
	if !b.CompletedAt.IsZero() {
		p.CompletedAt = timestamppb.New(b.CompletedAt)
	}
	return p
}

func discrepancyToProto(d *ledger.Discrepancy) *ledgerv1.Discrepancy {
	p := &ledgerv1.Discrepancy{
		Id: d.ID, TenantId: d.TenantID, BatchId: d.BatchID,
		Type: string(d.Type),
		ExternalRecordId: d.ExternalRecordID,
		JournalId:        d.JournalID,
		Status:           string(d.Status),
		ResolutionJournalId: d.ResolutionJournalID,
		ResolutionNote:      d.ResolutionNote,
		ResolvedBy:          d.ResolvedBy,
		CreatedAt:           timestamppb.New(d.CreatedAt),
	}
	if !d.ResolvedAt.IsZero() {
		p.ResolvedAt = timestamppb.New(d.ResolvedAt)
	}
	return p
}
```

- [ ] **Step 3: Build + commit**

```bash
go build ./...
go test ./internal/service/ -v -run TestToConnectError
git add internal/service/errors.go internal/service/recon_helpers.go
git commit -m "feat(service): map reconciliation error codes + proto helpers"
```

---

## Task 7: Matcher package

**Files:**
- Create: `internal/recon/matcher.go`

- [ ] **Step 1: Implement matcher**

```go
// matcher.go: core reconciliation matching algorithm.
package recon

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// IDGen returns a fresh unique id. Caller-injected so the matcher can be tested
// deterministically.
type IDGen func() string

// Clock returns "now". Caller-injected for testability.
type Clock func() time.Time

// MatchResult summarises a reconciliation pass.
type MatchResult struct {
	Ingested          int32
	Matched           int32
	Mismatched        int32
	MissingInLedger   int32
	MissingInExternal int32
	Discrepancies     []ledger.Discrepancy
}

// Run loads external records and journals for (tenantID, source, window) and
// produces a MatchResult. It also updates each external record's match_status
// (and matched_journal_id) inside tx. Discrepancy rows are NOT inserted here —
// the caller writes them after deciding the batch id.
func Run(
	ctx context.Context,
	tx repo.Tx,
	store repo.Store,
	tenantID, source string,
	windowStart, windowEnd time.Time,
) (MatchResult, error) {
	ext, err := store.ListExternalRecordsForRecon(ctx, tenantID, source, windowStart, windowEnd)
	if err != nil {
		return MatchResult{}, err
	}
	journals, err := store.ListJournalsForRecon(ctx, tenantID, source, windowStart, windowEnd)
	if err != nil {
		return MatchResult{}, err
	}

	byEventID := make(map[string]*ledger.Journal, len(journals))
	for i := range journals {
		j := &journals[i]
		byEventID[j.EventID] = j
	}

	res := MatchResult{Ingested: int32(len(ext))}

	for _, e := range ext {
		j, ok := byEventID[e.ExternalRef]
		if !ok {
			res.MissingInLedger++
			res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
				TenantID: tenantID, Type: ledger.DiscrepancyMissingInLedger,
				ExternalRecordID: e.ID,
			})
			continue
		}
		// Match by ref. Verify amount if anchor account is set.
		if e.AccountID != "" {
			debits, credits, sErr := tx.SumJournalEntries(ctx, tenantID, j.ID, e.AccountID, e.Currency)
			if sErr != nil {
				return MatchResult{}, sErr
			}
			signed := debits.Sub(credits)
			if !signed.Equal(e.Amount) {
				if err := tx.UpdateExternalRecordMatch(ctx, tenantID, e.ID, ledger.ExternalMismatched, j.ID); err != nil {
					return MatchResult{}, err
				}
				res.Mismatched++
				res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
					TenantID: tenantID, Type: ledger.DiscrepancyAmountMismatch,
					ExternalRecordID: e.ID, JournalID: j.ID,
				})
				delete(byEventID, e.ExternalRef)
				continue
			}
		}
		if err := tx.UpdateExternalRecordMatch(ctx, tenantID, e.ID, ledger.ExternalMatched, j.ID); err != nil {
			return MatchResult{}, err
		}
		res.Matched++
		delete(byEventID, e.ExternalRef)
	}

	// Whatever remains in byEventID had no matching external record.
	for _, j := range byEventID {
		res.MissingInExternal++
		res.Discrepancies = append(res.Discrepancies, ledger.Discrepancy{
			TenantID: tenantID, Type: ledger.DiscrepancyMissingInExternal,
			JournalID: j.ID,
		})
	}

	_ = decimal.Zero // silence unused import warning if math drift forces removal
	return res, nil
}
```

- [ ] **Step 2: Build + commit**

```bash
go build ./...
git add internal/recon/
git commit -m "feat(recon): matcher with ref-based matching and amount check"
```

---

## Task 8: IngestExternalRecords + GetReconciliationBatch + ListDiscrepancies handlers

**Files:**
- Create: `internal/service/ingest_external_records.go`
- Create: `internal/service/get_reconciliation_batch.go`
- Create: `internal/service/list_discrepancies.go`
- Modify: `internal/service/recon_stubs.go` (drop the three stubs)

- [ ] **Step 1: `IngestExternalRecords`**

```go
// ingest_external_records.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) IngestExternalRecords(ctx context.Context, req *connect.Request[ledgerv1.IngestExternalRecordsRequest]) (*connect.Response[ledgerv1.IngestExternalRecordsResponse], error) {
	r := req.Msg
	var inserted, skipped int32
	for _, in := range r.GetRecords() {
		amt, err := ledger.ParseAmount(in.GetAmount())
		if err != nil {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeUnbalancedJournal, "amount: "+err.Error()))
		}
		occurred := s.Now()
		if in.GetOccurredAt() != nil {
			occurred = in.GetOccurredAt().AsTime()
		}
		rec := ledger.ExternalRecord{
			ID: s.NewID(), TenantID: r.GetTenantId(),
			Source: in.GetSource(), ExternalRef: in.GetExternalRef(),
			Amount: amt, Currency: in.GetCurrency(),
			OccurredAt: occurred,
			AccountID:  in.GetAccountId(),
			RawPayload: mustStructToMap(in.GetRawPayload()),
		}
		ok, err := s.Store.InsertExternalRecord(ctx, rec)
		if err != nil {
			return nil, ToConnectError(err)
		}
		if ok {
			inserted++
		} else {
			skipped++
		}
	}
	return connect.NewResponse(&ledgerv1.IngestExternalRecordsResponse{Inserted: inserted, Skipped: skipped}), nil
}
```

- [ ] **Step 2: `GetReconciliationBatch`**

```go
// get_reconciliation_batch.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) GetReconciliationBatch(ctx context.Context, req *connect.Request[ledgerv1.GetReconciliationBatchRequest]) (*connect.Response[ledgerv1.GetReconciliationBatchResponse], error) {
	b, err := s.Store.GetReconBatch(ctx, req.Msg.GetTenantId(), req.Msg.GetBatchId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetReconciliationBatchResponse{Batch: reconBatchToProto(b)}), nil
}
```

- [ ] **Step 3: `ListDiscrepancies`**

```go
// list_discrepancies.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ListDiscrepancies(ctx context.Context, req *connect.Request[ledgerv1.ListDiscrepanciesRequest]) (*connect.Response[ledgerv1.ListDiscrepanciesResponse], error) {
	r := req.Msg
	in := repo.ListDiscrepanciesInput{
		TenantID: r.GetTenantId(),
		BatchID:  r.GetBatchId(),
		Status:   r.GetStatus(),
		Limit:    int(r.GetPageSize()),
	}
	rows, err := s.Store.ListDiscrepancies(ctx, in)
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := &ledgerv1.ListDiscrepanciesResponse{}
	for i := range rows {
		out.Discrepancies = append(out.Discrepancies, discrepancyToProto(&rows[i]))
	}
	return connect.NewResponse(out), nil
}
```

- [ ] **Step 4: Drop the three stubs**

In `internal/service/recon_stubs.go`, delete the `IngestExternalRecords`, `GetReconciliationBatch`, and `ListDiscrepancies` stubs. Leave `RunReconciliation` and `ResolveDiscrepancy` stubs in place (Tasks 9 and 10 will replace them).

- [ ] **Step 5: Build + tests**

```bash
go build ./...
go vet ./...
go test ./internal/service/ -v
```

- [ ] **Step 6: Commit**

```bash
git add internal/service/
git commit -m "feat(service): IngestExternalRecords, GetReconciliationBatch, ListDiscrepancies"
```

---

## Task 9: `RunReconciliation` handler

**Files:**
- Create: `internal/service/run_reconciliation.go`
- Modify: `internal/service/recon_stubs.go` (drop `RunReconciliation`)

- [ ] **Step 1: Implement**

```go
// run_reconciliation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/recon"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) RunReconciliation(ctx context.Context, req *connect.Request[ledgerv1.RunReconciliationRequest]) (*connect.Response[ledgerv1.RunReconciliationResponse], error) {
	r := req.Msg

	windowStart := s.Now()
	if r.GetWindowStart() != nil {
		windowStart = r.GetWindowStart().AsTime()
	}
	windowEnd := s.Now()
	if r.GetWindowEnd() != nil {
		windowEnd = r.GetWindowEnd().AsTime()
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

	// Idempotent replay
	existing, err := tx.GetReconBatchByIdempotency(ctx, r.GetTenantId(), r.GetIdempotencyKey())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if existing != nil {
		if existing.Status != ledger.BatchCompleted {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFlowConflict, "batch not completed: "+string(existing.Status)))
		}
		_ = tx.Commit()
		committed = true
		return connect.NewResponse(&ledgerv1.RunReconciliationResponse{Batch: reconBatchToProto(existing)}), nil
	}

	batch := ledger.ReconciliationBatch{
		ID: s.NewID(), TenantID: r.GetTenantId(),
		IdempotencyKey: r.GetIdempotencyKey(),
		Source:         r.GetSource(),
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		Status:         ledger.BatchRunning,
		StartedAt:      s.Now(),
		ActorID:        r.GetActorId(),
	}
	if err := tx.InsertReconBatch(ctx, batch); err != nil {
		return nil, ToConnectError(err)
	}

	// Matcher does the heavy lifting.
	res, err := recon.Run(ctx, tx, s.Store, r.GetTenantId(), r.GetSource(), windowStart, windowEnd)
	if err != nil {
		return nil, ToConnectError(err)
	}

	// Persist discrepancy rows + emit outbox events.
	for i := range res.Discrepancies {
		d := &res.Discrepancies[i]
		d.ID = s.NewID()
		d.BatchID = batch.ID
		d.Status = ledger.DiscrepancyOpen
		d.CreatedAt = s.Now()
		if err := tx.InsertDiscrepancy(ctx, *d); err != nil {
			return nil, ToConnectError(err)
		}
		payload, _ := json.Marshal(map[string]any{
			"discrepancy_id": d.ID, "type": string(d.Type), "batch_id": batch.ID,
		})
		if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
			ID: s.NewID(), TenantID: batch.TenantID, AggregateID: d.ID,
			EventType:      "DISCREPANCY_OPENED",
			IdempotencyKey: d.ID + ":opened",
			Payload:        payload, CreatedAt: s.Now(),
		}); err != nil {
			return nil, ToConnectError(err)
		}
	}

	batch.Status = ledger.BatchCompleted
	batch.IngestedCount = res.Ingested
	batch.MatchedCount = res.Matched
	batch.MismatchedCount = res.Mismatched
	batch.MissingInLedgerCount = res.MissingInLedger
	batch.MissingInExternalCount = res.MissingInExternal
	batch.CompletedAt = s.Now()
	if err := tx.CompleteReconBatch(ctx, batch); err != nil {
		return nil, ToConnectError(err)
	}

	// Batch-level outbox event.
	bp, _ := json.Marshal(map[string]any{
		"batch_id": batch.ID, "source": batch.Source,
		"matched": res.Matched, "missing_in_ledger": res.MissingInLedger,
		"missing_in_external": res.MissingInExternal, "mismatched": res.Mismatched,
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: batch.TenantID, AggregateID: batch.ID,
		EventType:      "RECON_BATCH_COMPLETED",
		IdempotencyKey: batch.ID + ":completed",
		Payload:        bp, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.RunReconciliationResponse{Batch: reconBatchToProto(&batch)}), nil
}
```

- [ ] **Step 2: Drop the stub**

Remove `RunReconciliation` from `internal/service/recon_stubs.go`.

- [ ] **Step 3: Build + commit**

```bash
go build ./...
go vet ./...
go test ./internal/service/ -v
git add internal/service/run_reconciliation.go internal/service/recon_stubs.go
git commit -m "feat(service): RunReconciliation orchestrator"
```

---

## Task 10: `ResolveDiscrepancy` handler

**Files:**
- Create: `internal/service/resolve_discrepancy.go`
- Delete: `internal/service/recon_stubs.go` (last stub goes)

- [ ] **Step 1: Implement**

```go
// resolve_discrepancy.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ResolveDiscrepancy(ctx context.Context, req *connect.Request[ledgerv1.ResolveDiscrepancyRequest]) (*connect.Response[ledgerv1.ResolveDiscrepancyResponse], error) {
	r := req.Msg

	resolution := r.GetResolution()
	if resolution != "RESOLVED" && resolution != "IGNORED" {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch, "resolution must be RESOLVED or IGNORED"))
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

	d, err := tx.LockDiscrepancy(ctx, r.GetTenantId(), r.GetDiscrepancyId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if d.Status.Closed() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeDiscrepancyClosed, "status="+string(d.Status)))
	}

	var resolutionJournalID string
	if resolution == "RESOLVED" && r.GetAdjustment() != nil {
		// Run the embedded ExecuteFlow inside this tx.
		adj := r.GetAdjustment()
		// Derive a deterministic idempotency key for the adjustment flow.
		adj.IdempotencyKey = d.ID + ":resolve:" + r.GetIdempotencyKey()
		flowResp, ferr := s.executeFlowInTx(ctx, tx, adj)
		if ferr != nil {
			return nil, ToConnectError(ferr)
		}
		resolutionJournalID = flowResp.GetFlowRunId()
		// flowResp's first step's JournalId is the adjustment journal proper.
		if len(flowResp.GetSteps()) > 0 {
			resolutionJournalID = flowResp.GetSteps()[0].GetJournalId()
		}
	}

	d.Status = ledger.DiscrepancyStatus(resolution)
	d.ResolutionJournalID = resolutionJournalID
	d.ResolutionNote = r.GetNote()
	d.ResolvedBy = r.GetActorId()
	d.ResolvedAt = s.Now()
	if err := tx.ResolveDiscrepancyRow(ctx, *d); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"discrepancy_id":         d.ID,
		"status":                 string(d.Status),
		"resolution_journal_id":  d.ResolutionJournalID,
	})
	eventType := "DISCREPANCY_RESOLVED"
	if d.Status == ledger.DiscrepancyIgnored {
		eventType = "DISCREPANCY_IGNORED"
	}
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: d.TenantID, AggregateID: d.ID,
		EventType:      eventType,
		IdempotencyKey: d.ID + ":" + string(d.Status) + ":" + r.GetIdempotencyKey(),
		Payload:        payload, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.ResolveDiscrepancyResponse{Discrepancy: discrepancyToProto(d)}), nil
}
```

- [ ] **Step 2: Delete stub file**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
rm internal/service/recon_stubs.go
```

- [ ] **Step 3: Build + tests**

```bash
go build ./...
go vet ./...
go test ./internal/service/ -v
```

All existing tests must still pass.

- [ ] **Step 4: Commit**

```bash
git add internal/service/resolve_discrepancy.go internal/service/recon_stubs.go
git commit -m "feat(service): ResolveDiscrepancy with optional adjustment journal"
```

---

## Task 11: End-to-end tests

**Files:**
- Create: `internal/service/recon_test.go`

- [ ] **Step 1: Write the tests**

```go
// recon_test.go
package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func TestIngest_HappyPathAndIdempotent(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	now := time.Now()
	records := []*ledgerv1.ExternalRecordInput{
		{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now)},
		{Source: "stripe", ExternalRef: "tx_002", Amount: "200", Currency: "USD", OccurredAt: timestamppb.New(now)},
		{Source: "stripe", ExternalRef: "tx_003", Amount: "300", Currency: "USD", OccurredAt: timestamppb.New(now)},
	}

	r1, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: records,
	}))
	if err != nil {
		t.Fatalf("ingest1: %v", err)
	}
	if r1.Msg.GetInserted() != 3 || r1.Msg.GetSkipped() != 0 {
		t.Fatalf("first: want 3 inserted / 0 skipped, got %d/%d", r1.Msg.GetInserted(), r1.Msg.GetSkipped())
	}

	r2, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: records,
	}))
	if err != nil {
		t.Fatalf("ingest2: %v", err)
	}
	if r2.Msg.GetInserted() != 0 || r2.Msg.GetSkipped() != 3 {
		t.Fatalf("replay: want 0 inserted / 3 skipped, got %d/%d", r2.Msg.GetInserted(), r2.Msg.GetSkipped())
	}
}

func TestRunReconciliation_AllMatched(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	// Two journals with source_service="stripe" and external_ref-style event_ids.
	for _, ref := range []string{"tx_001", "tx_002"} {
		if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
			TenantId: "t1", IdempotencyKey: "recon-seed-" + ref, SourceService: "stripe",
			Journal: &ledgerv1.Journal{EventId: ref, Entries: []*ledgerv1.Entry{
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			}},
		})); err != nil {
			t.Fatalf("seed %s: %v", ref, err)
		}
	}

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_002", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "batch-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	b := resp.Msg.GetBatch()
	if b.GetMatchedCount() != 2 {
		t.Fatalf("matched: want 2, got %d", b.GetMatchedCount())
	}
	if b.GetMissingInLedgerCount() != 0 || b.GetMissingInExternalCount() != 0 || b.GetMismatchedCount() != 0 {
		t.Fatalf("expected no discrepancies, got missing_in_ledger=%d missing_in_external=%d mismatched=%d",
			b.GetMissingInLedgerCount(), b.GetMissingInExternalCount(), b.GetMismatchedCount())
	}
}

func TestRunReconciliation_MissingInLedger(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_missing", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mil-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMissingInLedgerCount(); got != 1 {
		t.Fatalf("want 1 missing-in-ledger, got %d", got)
	}

	disc, err := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: resp.Msg.GetBatch().GetId(),
	}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(disc.Msg.GetDiscrepancies()) != 1 || disc.Msg.GetDiscrepancies()[0].GetType() != "MISSING_IN_LEDGER" {
		t.Fatalf("expected MISSING_IN_LEDGER, got %v", disc.Msg.GetDiscrepancies())
	}
}

func TestRunReconciliation_MissingInExternal(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "orphan-seed", SourceService: "stripe",
		Journal: &ledgerv1.Journal{EventId: "tx_orphan", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	now := time.Now()
	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mie-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMissingInExternalCount(); got != 1 {
		t.Fatalf("want 1 missing-in-external, got %d", got)
	}
}

func TestRunReconciliation_AmountMismatch(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "mm-seed", SourceService: "stripe",
		Journal: &ledgerv1.Journal{EventId: "tx_mm", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_mm", Amount: "999", Currency: "USD",
				OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mm-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMismatchedCount(); got != 1 {
		t.Fatalf("want 1 mismatched, got %d", got)
	}
}

func TestResolveDiscrepancy_WithAdjustment(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)
	now := time.Now()

	// Ingest an external record that has no matching journal.
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_late", Amount: "75", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "adj-batch", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	disc, err := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	if err != nil || len(disc.Msg.GetDiscrepancies()) != 1 {
		t.Fatalf("list: %v / %v", err, disc.Msg.GetDiscrepancies())
	}
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	// Resolve with an adjustment that books the missing $75.
	adj := &ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "ADJUSTMENT", SourceService: "recon",
		Steps: []*ledgerv1.Step{{
			StepId: "adjust",
			Journal: &ledgerv1.Journal{
				EventId: "tx_late",
				Entries: []*ledgerv1.Entry{
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "75"},
					{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "75"},
				},
			},
		}},
	}
	res, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "RESOLVED",
		Adjustment: adj, IdempotencyKey: "r1", Note: "late tx booked",
	}))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.Msg.GetDiscrepancy().GetStatus() != "RESOLVED" {
		t.Fatalf("status: want RESOLVED, got %s", res.Msg.GetDiscrepancy().GetStatus())
	}
	if res.Msg.GetDiscrepancy().GetResolutionJournalId() == "" {
		t.Fatalf("resolution_journal_id should be linked")
	}

	bal, _ := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if bal.Msg.GetBalance().GetNormalized() != "75" {
		t.Fatalf("avail: want 75, got %s", bal.Msg.GetBalance().GetNormalized())
	}
}

func TestResolveDiscrepancy_NoAdjustment(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_noadj", Amount: "10", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "noadj", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	disc, _ := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	res, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "IGNORED",
		IdempotencyKey: "r2", Note: "known noise",
	}))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.Msg.GetDiscrepancy().GetStatus() != "IGNORED" {
		t.Fatalf("status: want IGNORED, got %s", res.Msg.GetDiscrepancy().GetStatus())
	}
	if res.Msg.GetDiscrepancy().GetResolutionJournalId() != "" {
		t.Fatalf("resolution_journal_id should be empty")
	}
}

func TestResolveDiscrepancy_AlreadyClosed(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_dup", Amount: "10", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, _ := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "dup", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	disc, _ := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	if _, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "IGNORED", IdempotencyKey: "first",
	})); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	_, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "RESOLVED", IdempotencyKey: "second",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition CodeDiscrepancyClosed, got %v", err)
	}
}

func TestRunReconciliation_IdempotentReplay(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_idem", Amount: "1", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	req := &ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "idem", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}
	r1, err := srv.RunReconciliation(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	r2, err := srv.RunReconciliation(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if r1.Msg.GetBatch().GetId() != r2.Msg.GetBatch().GetId() {
		t.Fatalf("replay batch id mismatch: %s vs %s", r1.Msg.GetBatch().GetId(), r2.Msg.GetBatch().GetId())
	}
}
```

- [ ] **Step 2: Run**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go test ./internal/service/ -v -run "Recon|Resolve|Ingest"
```

All 9 tests must pass.

- [ ] **Step 3: Commit**

```bash
git add internal/service/recon_test.go
git commit -m "test(service): reconciliation lifecycle scenarios"
```

---

## Task 12: Example walkthrough + arch doc

**Files:**
- Create: `examples/go/reconciliation/main.go`
- Modify: `examples/README.md`
- Modify: `docs/ARCHITECTURE.md`

- [ ] **Step 1: `examples/go/reconciliation/main.go`**

```go
// Reconciliation walkthrough:
//   1. Create user + source accounts.
//   2. Post two "stripe" journals (event_id = tx ref).
//   3. Ingest three external records: two matching, one missing-in-ledger.
//   4. Run reconciliation: expect 2 matched, 1 missing-in-ledger.
//   5. Resolve the missing one by posting an adjustment journal.
//
// Run the server first; see ../place_order/main.go for setup.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
)

const (
	serverURL = "http://localhost:8080"
	tenant    = "t1"
)

type tenantRT struct{ tenant string }

func (t tenantRT) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Tenant-Id", t.tenant)
	return http.DefaultTransport.RoundTrip(req)
}

func main() {
	client := ledgerv1connect.NewLedgerServiceClient(
		&http.Client{Transport: tenantRT{tenant: tenant}}, serverURL)
	ctx := context.Background()

	avail := createAccount(ctx, client, "user", "1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := createAccount(ctx, client, "platform", "0", "source", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	// 2 matching journals
	for _, ref := range []string{"tx_001", "tx_002"} {
		postJournal(ctx, client, "seed-"+ref, "stripe", ref, []*ledgerv1.Entry{
			entry(avail, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "100"),
			entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "100"),
		})
	}
	fmt.Println("seeded two journals from source 'stripe'")

	// 3 external records: 2 match, 1 missing-in-ledger
	now := time.Now()
	if _, err := client.IngestExternalRecords(ctx, connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: tenant,
		Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_002", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_999", Amount: "42", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		log.Fatalf("ingest: %v", err)
	}
	fmt.Println("ingested 3 external records")

	// Run reconciliation
	bResp, err := client.RunReconciliation(ctx, connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: tenant, IdempotencyKey: "demo-batch", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	b := bResp.Msg.GetBatch()
	fmt.Printf("batch %s: matched=%d missing_in_ledger=%d missing_in_external=%d\n",
		b.GetId(), b.GetMatchedCount(), b.GetMissingInLedgerCount(), b.GetMissingInExternalCount())

	// List the open discrepancy
	dResp, err := client.ListDiscrepancies(ctx, connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: tenant, BatchId: b.GetId(), Status: "OPEN",
	}))
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	if len(dResp.Msg.GetDiscrepancies()) != 1 {
		log.Fatalf("want 1 discrepancy, got %d", len(dResp.Msg.GetDiscrepancies()))
	}
	d := dResp.Msg.GetDiscrepancies()[0]
	fmt.Printf("open discrepancy: id=%s type=%s\n", d.GetId(), d.GetType())

	// Resolve by posting the missing journal.
	adj := &ledgerv1.ExecuteFlowRequest{
		TenantId: tenant, FlowType: "ADJUSTMENT", SourceService: "recon",
		Steps: []*ledgerv1.Step{{
			StepId: "adjust",
			Journal: &ledgerv1.Journal{
				EventId: "tx_999",
				Entries: []*ledgerv1.Entry{
					entry(avail, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "42"),
					entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "42"),
				},
			},
		}},
	}
	res, err := client.ResolveDiscrepancy(ctx, connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: tenant, DiscrepancyId: d.GetId(), Resolution: "RESOLVED",
		Adjustment: adj, IdempotencyKey: "demo-resolve", Note: "back-booked from stripe",
	}))
	if err != nil {
		log.Fatalf("resolve: %v", err)
	}
	fmt.Printf("resolved: status=%s resolution_journal_id=%s\n",
		res.Msg.GetDiscrepancy().GetStatus(), res.Msg.GetDiscrepancy().GetResolutionJournalId())
}

func createAccount(ctx context.Context, c ledgerv1connect.LedgerServiceClient, ownerType, ownerID, kind, ccy string, nb ledgerv1.NormalBalance, allowNeg bool) string {
	r, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		log.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

func entry(acct, ccy string, dir ledgerv1.Direction, amount string) *ledgerv1.Entry {
	return &ledgerv1.Entry{AccountId: acct, Currency: ccy, Direction: dir, Amount: amount}
}

func postJournal(ctx context.Context, c ledgerv1connect.LedgerServiceClient, key, source, ref string, entries []*ledgerv1.Entry) {
	if _, err := c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: key, SourceService: source,
		Journal: &ledgerv1.Journal{EventId: ref, Entries: entries},
	})); err != nil {
		log.Fatalf("post %s: %v", key, err)
	}
}
```

- [ ] **Step 2: Update `examples/README.md`**

Add row to the Go examples table:

```markdown
| [`go/reconciliation`](go/reconciliation/main.go) | Ingest external records, run reconciliation, list discrepancies, resolve one with an adjustment journal. |
```

- [ ] **Step 3: Update `docs/ARCHITECTURE.md`**

Add a new subsection under "Key flows" (after FX):

```markdown
### Reconciliation — match, surface, resolve

`IngestExternalRecords` writes external-source transaction records (idempotent on `(tenant, source, external_ref)`). `RunReconciliation` opens one flow tx and:

1. Loads external records and ledger journals for `(tenant, source, window)`.
2. Indexes journals by `event_id`.
3. For each external record: if a journal with `event_id == external_ref` exists, optionally verifies the amount against `sum(debit-credit) on entries(journal) WHERE account_id = external.account_id` (when an anchor account was supplied). Match → `MATCHED`. Amount diverges → `MISMATCHED` + `AMOUNT_MISMATCH` discrepancy. No journal → `MISSING_IN_LEDGER` discrepancy.
4. Journals not seen in step 3 become `MISSING_IN_EXTERNAL` discrepancies.
5. Writes the batch summary, all discrepancy rows, and outbox events in the same tx as the in-memory match → commit.

`ResolveDiscrepancy` takes a discrepancy from `OPEN` to `RESOLVED` or `IGNORED`. When `RESOLVED` with an embedded `ExecuteFlowRequest`, the adjustment flow runs in the same tx via `executeFlowInTx`, and its `journal_id` is linked into `resolution_journal_id`.

All money movement still flows through `ExecuteFlow` — recon never writes ledger entries directly.
```

Also add a `reconciliation_batches`, `external_records`, `discrepancies` row to the data-model table and the three new error codes to the error mapping table.

- [ ] **Step 4: Build + commit**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go build ./examples/...
go build ./...
git add examples/ docs/ARCHITECTURE.md
git commit -m "docs: reconciliation example and architecture section"
```

---

## Task 13: Final wiring check

- [ ] **Step 1: Full suite**

```bash
cd /Users/cq/Dev/DoubleLedgerGo
go vet ./...
go test ./...
golangci-lint run ./...
```

All three must be clean.

- [ ] **Step 2: End-to-end smoke**

```bash
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/migrate ./cmd/migrate
rm -f /tmp/recon-e2e.db
./bin/migrate --backend=sqlite --dsn=/tmp/recon-e2e.db up
./bin/server --backend=sqlite --dsn=/tmp/recon-e2e.db --addr=127.0.0.1:18096 &
PID=$!
sleep 1
curl -fsS http://127.0.0.1:18096/healthz; echo

# Ingest one record
curl -fsS -X POST http://127.0.0.1:18096/ledger.v1.LedgerService/IngestExternalRecords \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: t1' \
  -d '{"tenant_id":"t1","records":[{"source":"stripe","external_ref":"tx_xyz","amount":"42","currency":"USD","occurred_at":"2026-05-24T10:00:00Z"}]}'
echo

# Run reconciliation
curl -fsS -X POST http://127.0.0.1:18096/ledger.v1.LedgerService/RunReconciliation \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: t1' \
  -d '{"tenant_id":"t1","idempotency_key":"smoke","source":"stripe","window_start":"2026-05-23T00:00:00Z","window_end":"2026-05-25T00:00:00Z"}'
echo

kill $PID 2>/dev/null; wait $PID 2>/dev/null
rm -rf /tmp/recon-e2e.db bin
```

Expected: healthz 200; Ingest returns `{"inserted":1,"skipped":0}`; RunReconciliation returns a batch with `"missingInLedgerCount":1`.

- [ ] **Step 3: Commit if anything moved; otherwise done**

```bash
git status
```

---

## Self-review notes

- **Spec coverage**:
  - §2 matching contract → Task 7 matcher.
  - §3 data model → Tasks 2 (migrations) + 3 (queries).
  - §4 domain types + error codes → Task 1.
  - §5 RPC surface → Tasks 5 (proto) + 8/9/10 (handlers).
  - §6 RunReconciliation algorithm → Task 7 matcher + Task 9 orchestration.
  - §7 ResolveDiscrepancy semantics → Task 10.
  - §8 layout → file map at top.
  - §9 outbox events → Tasks 9 and 10.
  - §10 concurrency → uses existing tx semantics; no new locking primitives needed.
  - §11 tests → Task 11 (9 tests).
- **Placeholder scan**: explicit "inspect sqlc-generated names" hints in Task 4 are concrete debugging instructions, not "TBD". All code blocks are complete.
- **Type consistency**: `ExternalRecord`, `Discrepancy`, `ReconciliationBatch` Go fields, proto fields, and store methods agree throughout. `recon.Run` signature is consistent between Task 7 (definition) and Task 9 (call site). The `mustStructToMap` helper used in Task 8 already exists (added in the reservation phase).
- **One important deviation from the spec**: I moved ingestion business logic into the handler instead of into a separate `internal/recon/ingest.go` file — the logic is small enough (~30 lines) that an extra file is overhead. Easy to extract later if it grows.
