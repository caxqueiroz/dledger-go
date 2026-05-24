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
