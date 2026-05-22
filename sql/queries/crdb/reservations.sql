-- name: InsertReservation :exec
INSERT INTO reservations (
    id, tenant_id, idempotency_key, source_account_id, reserved_account_id,
    currency, original_amount, outstanding_amount, status, expires_at,
    flow_run_id, metadata
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- name: GetReservation :one
SELECT * FROM reservations WHERE tenant_id = $1 AND id = $2;

-- name: LockReservation :one
SELECT * FROM reservations WHERE tenant_id = $1 AND id = $2 FOR UPDATE;

-- name: GetReservationByIdempotency :one
SELECT * FROM reservations WHERE tenant_id = $1 AND idempotency_key = $2;

-- name: UpdateReservationAmounts :exec
UPDATE reservations
SET outstanding_amount = $1, committed_amount = $2, released_amount = $3,
    status = $4, updated_at = now()
WHERE tenant_id = $5 AND id = $6;

-- name: ListExpiredReservations :many
SELECT id, tenant_id FROM reservations
WHERE status IN ('HELD', 'PARTIAL')
  AND expires_at IS NOT NULL
  AND expires_at <= $1
ORDER BY expires_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;
