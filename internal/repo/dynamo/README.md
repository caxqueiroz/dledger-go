# DynamoDB Backend

Concise reference for the DynamoDB implementation of `repo.Store`. This backend
targets AWS DynamoDB (and ExtendDB locally) using a single-table design with
optimistic concurrency control.

---

## 1. Table Layout

All items live in one DynamoDB table whose name is supplied as the `DSN` in
`dledger.Options`. The primary key is `pk` (string, hash-only). A secondary
sort key is never used on the base table.

### Item kinds and PK prefixes

| Prefix | Item kind | Contents |
|--------|-----------|----------|
| `ACC#` | Account | `account_id`, owner fields, currency, normal balance |
| `ACCU#` | Account uniqueness marker | Prevents duplicate `(tenant, ownerType, ownerID, accountType, currency)` |
| `BAL#` | Balance | `posted_debits`, `posted_credits`, `version` |
| `JRN#` | Journal + entries | Embedded entries array; written once per flow |
| `EVT#` | Event uniqueness marker | `event_id` de-duplication within journals |
| `FLOW#` | Flow run + steps | `status`, embedded steps array; `FIDEMP#` is the idempotency marker |
| `FIDEMP#` | Flow idempotency marker | Maps `(tenant, idempotency_key)` → flow run ID |
| `OBX#` | Outbox event | Published atomically with the flow; marked published after dispatch |
| `RES#` | Reservation | Amounts, status, version; `RIDEMP#` is the idempotency marker |
| `RIDEMP#` | Reservation idempotency marker | Maps `(tenant, idempotency_key)` → reservation ID |

### GSI1 — outbox pending + reservation expiry

Hash key: `gsi1pk` / Range key: `gsi1sk`

Two logical query patterns share GSI1:

- **Pending outbox**: `gsi1pk = "OBX#PENDING"`, sort key is `outbox_id`. Items
  are removed from GSI1 (attribute becomes null/absent) after they are marked
  published.
- **Reservation expiry**: `gsi1pk = "RESEXP"`, sort key is
  `"<expires_at_rfc3339>#<tenant_id>#<reservation_id>"`. Terminal reservations
  (RELEASED, COMMITTED) omit `gsi1pk`/`gsi1sk` so they drop out of the index.

`ListExpiredReservations` queries `gsi1pk = "RESEXP" AND gsi1sk < cutoff`.

### GSI2 — reservation owner lookup

Hash key: `gsi2pk` = `"RESOWN#<tenant>#<ownerType>#<ownerID>"` / Range key:
`gsi2sk` (reservation created-at timestamp).

`ListReservations` queries by owner using GSI2 and filters by status
client-side.

---

## 2. OCC Commit Model

### Write buffer

`Tx` is a struct that accumulates all writes in memory (`puts map[string]*pendingPut`).
No DynamoDB calls are made until `Commit()`. Reads inside a transaction use
strongly-consistent `GetItem` calls and cache results in the overlay to avoid
redundant round-trips.

### Versioned balance rows

Each balance item carries a `version` counter (integer, starts at 1). On first
write of a balance row the condition is `attribute_not_exists(pk)`; on
subsequent writes it is `version = :v` where `:v` is the version that was read.
The overlay (`txBalance.readVersion`) stores the version at load time.

### Single-call atomicity

`Commit()` serialises all buffered puts into a single `TransactWriteItems` call
(max 100 items — see section 5). The call is atomic: all items land together or
none do.

### Conflict classification table

When `TransactWriteItems` returns `TransactionCanceledException`, the
`CancellationReasons` slice is positional: index `i` maps to `t.order[i]`.

| Failing item condition | PK prefix | Classification | Retryable |
|------------------------|-----------|----------------|-----------|
| `condVersionEquals` | any (`BAL#`, `RES#`) | `SERIALIZATION_RETRY_EXHAUSTED` | Yes |
| `condNotExists` | `BAL#` | `SERIALIZATION_RETRY_EXHAUSTED` (fresh-balance race) | Yes |
| `condNotExists` | `ACC#`, `ACCU#`, `FIDEMP#`, `EVT#`, `RIDEMP#`, `OBX#`, other | `FLOW_CONFLICT` | No |

Both `SERIALIZATION_RETRY_EXHAUSTED` and `FLOW_CONFLICT` map to
`connect.CodeAborted` at the service layer (see `internal/service/errors.go`).
Callers distinguish them via the `ledger-error-code` response header using
`dledger.IsErrCode`.

---

## 3. Caller Retry Contract

**The service layer does NOT retry OCC conflicts.** `create_reservation.go`,
`commit_reservation.go`, and `execute_flow.go` call `BeginFlowTx`, do work, and
return the error directly to the caller without any retry loop.

This is identical to the SQL backends (SQLite, CRDB). Under the DynamoDB
backend, serialization conflicts are more likely under high write contention
(many goroutines modifying the same balance row simultaneously), because
DynamoDB lacks row-level locks and the OCC window is wider than a CRDB
serializable transaction.

**Callers MUST implement retry-on-SERIALIZATION_RETRY_EXHAUSTED.** The
canonical pattern (from the concurrency test suite):

```go
const maxRetries = 50
for attempt := 0; attempt < maxRetries; attempt++ {
    r, err := wallet.Reserve(ctx, in)
    if err == nil {
        return r, nil
    }
    if dledger.IsErrCode(err, dledger.ErrSerializationRetryExhausted) ||
        dledger.IsErrCode(err, dledger.ErrFlowConflict) {
        continue // retry with the SAME idempotency key — safe by design
    }
    return r, err // non-retryable
}
```

Retrying with the **same idempotency key** is safe: the first attempt that
committed wrote the `FIDEMP#` or `RIDEMP#` marker atomically, so subsequent
attempts hit the idempotent-replay path and return the cached result without
re-executing the flow.

---

## 4. Lock-set == Write-set Invariant (Write-skew Prevention)

Every `executeFlowInTx` step that **reads** a balance row via `LockBalance`
**must also write** that row via `UpdateBalance`. The OCC version condition on
each balance row means any concurrent write to the same row causes this
transaction to fail and retry. If a balance row were read (and thus its version
observed) but then NOT written, a concurrent writer could change that row and
this transaction would commit without detecting the conflict — classic
write-skew.

**Maintenance invariant:** any future code path that reads a balance (e.g. via
`LockBalance` or `EnsureBalanceRow`) inside a transaction and then skips the
corresponding `UpdateBalance` call would reintroduce write-skew for that row.
Every locked balance must be included in the final write set.

---

## 5. 100-Item TransactWriteItems Bound

`TransactWriteItems` accepts at most 100 items per call (AWS hard limit). If
`Commit()` is called with more than 100 buffered puts it returns immediately
with `FLOW_TOO_LARGE` (`connect.CodeFailedPrecondition`). Multi-step flows with
many entries and outbox events can approach this limit; if a flow is projected
to exceed 100 items it must be split into smaller flows.

---

## 6. Behavioral Deviations vs SQL Backends

### Atomic flow visibility (no RUNNING state)

SQL backends can persist a flow in `RUNNING` status before its steps are
committed. The DynamoDB backend writes the flow item only in the `COMPLETED`
state (all steps embedded) via the single `TransactWriteItems` commit. A
flow is either absent (still in-progress) or fully committed (COMPLETED) — no
intermediate RUNNING state is visible to readers.

### Unsupported areas

The following `repo.Store` methods return `UNSUPPORTED_OPERATION` under the
DynamoDB backend. The 21 service-layer tests that exercise these features skip
automatically when `DLEDGER_TEST_BACKEND=dynamo`:

| Feature area | Unsupported methods |
|---|---|
| Account activity | `ListAccountActivity` |
| Balance snapshots | `InsertSnapshot`, `GetSnapshotBefore`, `SumEntriesBetween`, `ListTenantBalances` |
| FX rates | `UpsertFXRate`, `GetFXRateAt`, `ListFXRates` |
| Reconciliation | `InsertExternalRecord`, `GetReconBatch` |
| Discrepancies | `ListDiscrepancies`, `GetDiscrepancy` |

Tests skipped under `DLEDGER_TEST_BACKEND=dynamo`:
`TestListAccountActivity_ReturnsEntries`,
`TestPutAndGetFXRate`, `TestGetFXRate_TimeOrdered`, `TestGetFXRate_NotFound`,
`TestExecuteExchange_HappyPath`, `TestExecuteExchange_ResolvesRateFromStore`,
`TestExecuteExchange_AmountMismatch`, `TestExecuteExchange_NoRateAvailable`,
`TestExecuteExchange_IdempotentReplay`,
`TestIngest_HappyPathAndIdempotent`,
`TestRunReconciliation_AllMatched`, `TestRunReconciliation_MissingInLedger`,
`TestRunReconciliation_MissingInExternal`, `TestRunReconciliation_AmountMismatch`,
`TestResolveDiscrepancy_WithAdjustment`, `TestResolveDiscrepancy_NoAdjustment`,
`TestResolveDiscrepancy_AlreadyClosed`, `TestRunReconciliation_IdempotentReplay`,
`TestTakeBalanceSnapshot_SingleRow`, `TestGetBalance_AsOfHistoricalPoint`,
`TestTakeBalanceSnapshot_BulkTenantWide`.

### Quiet scheduler no-ops

`ListTenantsDueForSnapshot` and `PruneSnapshotsOlderThan` return `(nil, nil)` /
`(0, nil)` so the background snapshot scheduler finds nothing to process rather
than crashing.

---

## 7. Operational Pitfalls

### EnsureTable does not add GSIs to pre-existing tables

`EnsureTable` calls `CreateTable` and swallows `ResourceInUseException`. If the
table already exists with a different GSI schema (e.g. GSI2 was added after
GSI1 in an earlier deployment), the existing table is left unchanged. Pre-existing
deployments that predate GSI2 must be updated manually (`UpdateTable` +
`wait-for-index-active`) or recreated.

### GSI propagation lag affects GSI1 and GSI2 reads

Writes to items that project onto GSI1 or GSI2 propagate asynchronously in
real DynamoDB (~200–300ms typical; occasionally longer). In ExtendDB (the local
test server used in CI) propagation is usually synchronous but the
`waitForGSI()` helper in the test suite adds 250ms as a guard. Reads via GSI1
(`ListExpiredReservations`, `PendingOutbox`) and GSI2 (`ListReservations`) may
transiently miss just-written items. The base table is always strongly
consistent via `ConsistentRead: true`.
