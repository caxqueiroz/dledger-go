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
