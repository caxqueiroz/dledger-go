# Reconciliation — Design

Date: 2026-05-24
Module: `github.com/caxqueiroz/dledger-go`

## 1. Purpose and scope

Add reconciliation: ingest external transaction records (from a payment processor, bank, or exchange settlement file), match them deterministically against the ledger, surface discrepancies, and let operators resolve each discrepancy — optionally posting an adjustment journal through the existing `ExecuteFlow` orchestrator so the ledger and the external source agree.

In scope:

- Three new tables: `external_records`, `reconciliation_batches`, `discrepancies`.
- Five new Connect-RPCs.
- A new `internal/recon` package with the matching algorithm and discrepancy-resolution logic.
- Reference-only matching (deterministic): `external_records.external_ref == ledger_journals.event_id`, scoped by `tenant_id` and matching `external_records.source` against `ledger_journals.source_service`.
- Amount-mismatch detection when refs match but the external amount doesn't equal the sum of debits to a caller-specified `account_id` in that journal.
- Idempotent ingestion (duplicates collapse on `(tenant, source, external_ref)`).
- Idempotent reconciliation runs (`idempotency_key` on the batch).
- `ResolveDiscrepancy` can post an adjustment journal in the same tx as it flips the discrepancy to `RESOLVED`.

Out of scope:

- Fuzzy/amount-window matching (would be v2).
- Rule-based matching engine (Blnk-style).
- CSV / file upload — ingestion is RPC only.
- Auto-matching at ingest time — kept pure write; matching happens in `RunReconciliation`.
- Webhook integrations to specific processors (callers post records themselves).

## 2. Matching contract

The deterministic matching key is:

```
external_records.external_ref == ledger_journals.event_id
AND external_records.tenant_id == ledger_journals.tenant_id
AND external_records.source    == ledger_journals.source_service
```

**Convention enforced by docs**: integrations wanting reconciliation set `journal.event_id` to the external system's transaction id (Stripe charge id, bank txn id, exchange settlement id). This aligns naturally with the existing UNIQUE constraint on `ledger_journals.event_id` and gives idempotency on `PostJournal`/`ExecuteFlow` for free.

**Source-of-truth account for amount checks**: each ingested `external_record` carries an optional `account_id` field — the user-side account that should reflect the external transfer. When refs match, the matcher computes the sum of debit-direction entries to that `account_id` within the matched journal and compares against `external_record.amount`. Mismatch → `AMOUNT_MISMATCH` discrepancy. If `account_id` is empty, only the ref match is verified; the amount check is skipped.

## 3. Data model

### `external_records`

| Column | Type | Notes |
|---|---|---|
| `id` | string PK | UUID |
| `tenant_id` | string | |
| `source` | string | `"stripe"`, `"acme_bank"`, ... |
| `external_ref` | string | third-party transaction id |
| `amount` | DECIMAL(38,18) (CRDB) / TEXT (SQLite) | |
| `currency` | string | ISO 4217 |
| `occurred_at` | timestamp | when the external event happened |
| `account_id` | string NULL | user-side ledger account used for amount check |
| `raw_payload` | JSONB / TEXT | full original record for audit |
| `match_status` | string CHECK | `UNMATCHED`, `MATCHED`, `MISMATCHED` |
| `matched_journal_id` | string FK NULL | set when matched |
| `created_at` | timestamp | |
| UNIQUE | `(tenant_id, source, external_ref)` | idempotent ingest |
| INDEX | `(tenant_id, source, occurred_at)` | drives window scans |
| INDEX | `(tenant_id, source, match_status)` | drives matcher candidate query |

### `reconciliation_batches`

| Column | Type | Notes |
|---|---|---|
| `id` | string PK | UUID |
| `tenant_id` | string | |
| `idempotency_key` | string UNIQUE | one batch per logical run |
| `source` | string | one source per batch |
| `window_start`, `window_end` | timestamp | match window for journals + external records |
| `status` | string CHECK | `RUNNING`, `COMPLETED`, `FAILED` |
| `ingested_count` | int | total external records in scope |
| `matched_count` | int | refs matched cleanly |
| `mismatched_count` | int | refs matched but amount diverged |
| `missing_in_ledger_count` | int | external record had no journal |
| `missing_in_external_count` | int | journal had no external record |
| `started_at`, `completed_at` | timestamp | |
| `actor_id` | string | who triggered the run |

### `discrepancies`

| Column | Type | Notes |
|---|---|---|
| `id` | string PK | UUID |
| `tenant_id` | string | |
| `batch_id` | string FK reconciliation_batches | |
| `type` | string CHECK | `MISSING_IN_LEDGER`, `MISSING_IN_EXTERNAL`, `AMOUNT_MISMATCH` |
| `external_record_id` | string FK NULL | first + third types |
| `journal_id` | string FK NULL | second + third types |
| `status` | string CHECK | `OPEN`, `RESOLVED`, `IGNORED` |
| `resolution_journal_id` | string FK NULL | adjustment journal posted at resolve time |
| `resolution_note` | string NULL | free-text from operator |
| `resolved_by` | string NULL | actor id |
| `resolved_at`, `created_at` | timestamp | |
| INDEX | `(tenant_id, status, batch_id)` | drives listing |

## 4. Domain types and error codes

### `internal/ledger/recon.go` (new)

```go
type ExternalRecord struct {
    ID               string
    TenantID         string
    Source           string
    ExternalRef      string
    Amount           decimal.Decimal
    Currency         string
    OccurredAt       time.Time
    AccountID        string                  // optional
    RawPayload       map[string]any
    MatchStatus      ExternalRecordStatus    // UNMATCHED | MATCHED | MISMATCHED
    MatchedJournalID string                  // empty if unmatched
    CreatedAt        time.Time
}

type ReconciliationBatch struct {
    ID                     string
    TenantID               string
    IdempotencyKey         string
    Source                 string
    WindowStart, WindowEnd time.Time
    Status                 BatchStatus       // RUNNING | COMPLETED | FAILED
    IngestedCount          int
    MatchedCount           int
    MismatchedCount        int
    MissingInLedgerCount   int
    MissingInExternalCount int
    StartedAt, CompletedAt time.Time
    ActorID                string
}

type Discrepancy struct {
    ID                   string
    TenantID             string
    BatchID              string
    Type                 DiscrepancyType   // MISSING_IN_LEDGER | MISSING_IN_EXTERNAL | AMOUNT_MISMATCH
    ExternalRecordID     string            // optional FK
    JournalID            string            // optional FK
    Status               DiscrepancyStatus // OPEN | RESOLVED | IGNORED
    ResolutionJournalID  string            // optional FK
    ResolutionNote       string
    ResolvedBy           string
    ResolvedAt           time.Time
    CreatedAt            time.Time
}

func (s DiscrepancyStatus) Closed() bool {
    return s == DiscrepancyResolved || s == DiscrepancyIgnored
}
```

### New `DomainCode`s

| Code | Connect code | When |
|---|---|---|
| `DISCREPANCY_NOT_FOUND` | `NotFound` | `ResolveDiscrepancy` on unknown id |
| `DISCREPANCY_CLOSED` | `FailedPrecondition` | resolution on an already-closed discrepancy |
| `RECON_BATCH_NOT_FOUND` | `NotFound` | `GetReconciliationBatch` on unknown id |

## 5. RPC surface

```proto
service LedgerService {
  // ... existing 16 ...
  rpc IngestExternalRecords(IngestExternalRecordsRequest) returns (IngestExternalRecordsResponse);
  rpc RunReconciliation(RunReconciliationRequest) returns (RunReconciliationResponse);
  rpc GetReconciliationBatch(GetReconciliationBatchRequest) returns (GetReconciliationBatchResponse);
  rpc ListDiscrepancies(ListDiscrepanciesRequest) returns (ListDiscrepanciesResponse);
  rpc ResolveDiscrepancy(ResolveDiscrepancyRequest) returns (ResolveDiscrepancyResponse);
}

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
  string account_id      = 6;             // optional anchor for amount check
  google.protobuf.Struct raw_payload = 7;
}
message IngestExternalRecordsRequest {
  string tenant_id                       = 1 [(buf.validate.field).string.min_len = 1];
  repeated ExternalRecordInput records   = 2 [(buf.validate.field).repeated.min_items = 1];
}
message IngestExternalRecordsResponse {
  int32 inserted    = 1;   // newly inserted rows
  int32 skipped     = 2;   // already-existed by (tenant, source, external_ref)
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
  string batch_id   = 2;     // optional
  string status     = 3;     // optional: OPEN, RESOLVED, IGNORED
  int32  page_size  = 4;
}
message ListDiscrepanciesResponse {
  repeated Discrepancy discrepancies = 1;
}

message ResolveDiscrepancyRequest {
  string tenant_id        = 1 [(buf.validate.field).string.min_len = 1];
  string discrepancy_id   = 2 [(buf.validate.field).string.min_len = 1];
  string resolution       = 3 [(buf.validate.field).string.in = ["RESOLVED", "IGNORED"]];
  ExecuteFlowRequest adjustment = 4;   // optional; only used when resolution=RESOLVED
  string note             = 5;
  string idempotency_key  = 6 [(buf.validate.field).string.min_len = 1];
  string actor_id         = 7;
}
message ResolveDiscrepancyResponse { Discrepancy discrepancy = 1; }
```

## 6. `RunReconciliation` algorithm

```
1. Begin flow tx.
2. Lookup batch by (tenant, idempotency_key). If status=COMPLETED, return existing batch (replay).
   If status=RUNNING with different request fingerprint, error CodeFlowConflict.
3. Insert batch (RUNNING) with started_at=now and request fields.
4. external = SELECT * FROM external_records
              WHERE tenant_id=? AND source=?
                AND occurred_at >= window_start AND occurred_at <= window_end
                AND match_status='UNMATCHED'
              ORDER BY occurred_at;
5. ledger = SELECT * FROM ledger_journals
            WHERE tenant_id=? AND source_service=?
              AND created_at >= window_start AND created_at <= window_end;
6. Index ledger by event_id (in-memory map[string]Journal).
7. For each external record:
   a. j, ok = ledger[external.external_ref]
   b. If !ok:
        - INSERT discrepancy(type=MISSING_IN_LEDGER, external_record_id=external.id).
        - missing_in_ledger_count++
   c. Else if external.account_id != "" :
        - sum = sum of (DEBIT amount - CREDIT amount) on entries(j) WHERE account_id=external.account_id AND currency=external.currency
        - If sum != external.amount:
            - UPDATE external_records SET match_status='MISMATCHED', matched_journal_id=j.id.
            - INSERT discrepancy(type=AMOUNT_MISMATCH, external_record_id=external.id, journal_id=j.id).
            - mismatched_count++
        - Else:
            - UPDATE external_records SET match_status='MATCHED', matched_journal_id=j.id.
            - matched_count++
   d. Else (no account anchor):
        - UPDATE external_records SET match_status='MATCHED', matched_journal_id=j.id.
        - matched_count++
   e. Remove j from the in-memory map.
8. For each j remaining in the map (no matching external record):
   - INSERT discrepancy(type=MISSING_IN_EXTERNAL, journal_id=j.id).
   - missing_in_external_count++
9. UPDATE batch (status=COMPLETED, counts, completed_at=now).
10. INSERT outbox events:
    - DISCREPANCY_OPENED per new discrepancy.
    - RECON_BATCH_COMPLETED for the batch.
11. Commit.
```

CRDB: full retry via `repo.WithRetry`. SQLite: `BEGIN IMMEDIATE` as today.

### Amount sum semantics

For a `MATCHED` journal with `external.account_id = A` and `external.amount = X` in `external.currency = C`:

- Compute `signedSum = Σ (entry.amount if entry.direction=DEBIT else -entry.amount)` over `entries(j) WHERE account_id=A AND currency=C`.
- A match means `signedSum == X` (the journal moved exactly `X` of `C` *into* the user-side account).

For credit-normal accounts, callers should set `external.account_id` to the *source* account they're settling against; the matcher treats DEBIT as positive uniformly.

## 7. `ResolveDiscrepancy` semantics

1. Begin flow tx.
2. SELECT … FOR UPDATE the discrepancy by id (CRDB; SQLite tx-serializes anyway).
3. If `status != OPEN`, return `CodeFailedPrecondition` (`DISCREPANCY_CLOSED`).
4. If `resolution = RESOLVED` AND `adjustment` is set:
   - Synthesize an `ExecuteFlowRequest` from `adjustment`. Caller provides the full journal; the recon code doesn't auto-construct entries (too much policy).
   - Call `executeFlowInTx(ctx, tx, adjustment)`.
   - Capture `flow_run_id`.
5. UPDATE discrepancy: `status, resolved_by, resolved_at=now, resolution_journal_id (if any), resolution_note`.
6. INSERT outbox `DISCREPANCY_RESOLVED` event (or `DISCREPANCY_IGNORED`).
7. Commit.

Idempotency: caller-supplied `idempotency_key` on the request; the adjustment flow's own idempotency key derives from `<discrepancy_id>:resolve:<key>` so retries are safe.

## 8. Layout

```
internal/ledger/recon.go                        # types + statuses
internal/ledger/errors.go                       # 3 new codes

internal/recon/                                  # new package
  ingest.go                                     # ingestion business logic
  matcher.go                                    # RunReconciliation algorithm
  resolver.go                                   # discrepancy resolution

internal/repo/repo.go                           # Store extensions
internal/repo/sqlite/{store,tx,conv}.go         # impls
internal/repo/crdb/{store,tx,conv}.go           # impls

internal/service/
  errors.go                                     # map 3 new codes
  ingest_external_records.go                    # handler
  run_reconciliation.go                         # handler
  get_reconciliation_batch.go
  list_discrepancies.go
  resolve_discrepancy.go
  recon_helpers.go                              # proto conversions

internal/service/recon_test.go                  # 9 SQLite tests

sql/migrations/{sqlite,crdb}/0005_reconciliation.sql
sql/queries/{sqlite,crdb}/external_records.sql
sql/queries/{sqlite,crdb}/reconciliation_batches.sql
sql/queries/{sqlite,crdb}/discrepancies.sql

examples/go/reconciliation/main.go              # walkthrough
docs/ARCHITECTURE.md                            # Recon section
```

## 9. Outbox events

| Event type | When |
|---|---|
| `RECON_BATCH_COMPLETED` | After every successful `RunReconciliation`, with summary counts in payload |
| `DISCREPANCY_OPENED` | Per discrepancy created during a batch run |
| `DISCREPANCY_RESOLVED` | When status flips to RESOLVED |
| `DISCREPANCY_IGNORED` | When status flips to IGNORED |

All carry `tenant_id`, `discrepancy_id`/`batch_id` in `aggregate_id`, and idempotency keys derived from the entity id.

## 10. Concurrency

- `IngestExternalRecords`: no transaction needed beyond per-row inserts. UNIQUE constraint dedupes.
- `RunReconciliation`: full flow tx. CRDB serializable + 40001 retry. SQLite BEGIN IMMEDIATE.
- `ResolveDiscrepancy`: full flow tx with `SELECT … FOR UPDATE` on the discrepancy row (CRDB).
- Multi-tenant: every query filters on `tenant_id`. The tenant interceptor injects it from the request header as today.

## 11. Tests (SQLite-backed, service-level)

1. **`IngestHappyPath`** — insert 3 records, replay same payload, assert 3 inserted then 0 inserted + 3 skipped.
2. **`RunReconciliation_AllMatched`** — 2 journals + 2 matching external records → batch COMPLETED, `matched=2`, zero discrepancies.
3. **`RunReconciliation_MissingInLedger`** — external record without matching journal → 1 discrepancy of type `MISSING_IN_LEDGER`.
4. **`RunReconciliation_MissingInExternal`** — journal without matching external record → 1 discrepancy of type `MISSING_IN_EXTERNAL`.
5. **`RunReconciliation_AmountMismatch`** — refs match, amount diverges → external record marked MISMATCHED + discrepancy `AMOUNT_MISMATCH`.
6. **`ResolveDiscrepancy_WithAdjustment`** — discrepancy + adjustment journal → discrepancy RESOLVED, adjustment posted (balances change), `resolution_journal_id` linked.
7. **`ResolveDiscrepancy_NoAdjustment`** — flip RESOLVED with no journal; verify status only.
8. **`ResolveDiscrepancy_AlreadyClosed`** — second resolve on a closed discrepancy → `FailedPrecondition`.
9. **`RunReconciliation_IdempotentReplay`** — same `idempotency_key` returns the same `batch_id`, no new discrepancies.

## 12. Phasing

One PR, 14 tasks (similar shape to the FX plan). Migration 0005 + sqlc + domain + repo + 5 RPCs + recon package + tests + example + arch doc.

## 13. Acceptance criteria

- An external record can be ingested; replays of the same `(tenant, source, external_ref)` collapse cleanly.
- A reconciliation run produces a batch with correct summary counts; discrepancies of all three types are surfaced.
- A discrepancy can be resolved with or without an adjustment journal; resolved discrepancies cannot be re-resolved.
- All money movement still flows through `ExecuteFlow` — recon code never writes `ledger_entries` directly.
- Outbox events are written transactionally with the database changes; no event escapes on rollback.

## 14. Out of scope

- Fuzzy / amount-window matching.
- Rule-based / per-tenant matching configuration.
- CSV ingestion.
- Discrepancy notifications (callers consume the outbox).
- Bulk discrepancy resolution.
- Reconciliation across multiple sources in a single batch.
- Periodic auto-reconciliation (could be a scheduler tick in v2, similar to retention).
