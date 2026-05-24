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
