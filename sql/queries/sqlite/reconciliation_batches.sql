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
