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
