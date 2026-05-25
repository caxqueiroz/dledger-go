-- name: InsertReservation :exec
INSERT INTO reservations (
    id, tenant_id, idempotency_key, source_account_id, reserved_account_id,
    currency, original_amount, outstanding_amount, status, expires_at,
    flow_run_id, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetReservation :one
SELECT * FROM reservations WHERE tenant_id = ? AND id = ?;

-- name: GetReservationByIdempotency :one
SELECT * FROM reservations WHERE tenant_id = ? AND idempotency_key = ?;

-- name: UpdateReservationAmounts :exec
UPDATE reservations
SET outstanding_amount = ?, committed_amount = ?, released_amount = ?,
    status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE tenant_id = ? AND id = ?;

-- name: ListExpiredReservations :many
SELECT id, tenant_id FROM reservations
WHERE status IN ('HELD', 'PARTIAL')
  AND expires_at IS NOT NULL
  AND expires_at <= ?
ORDER BY expires_at ASC
LIMIT ?;

-- name: ListReservations :many
SELECT r.*
FROM reservations r
JOIN accounts a ON a.id = r.source_account_id
WHERE r.tenant_id = ?
  AND (a.owner_type = ?  OR ? = '')
  AND (a.owner_id   = ?  OR ? = '')
  AND (r.status     = ?  OR ? = '')
ORDER BY r.created_at DESC, r.id DESC
LIMIT ?;
