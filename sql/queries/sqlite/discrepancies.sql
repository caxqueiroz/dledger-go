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
