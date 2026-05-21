-- name: InsertEntry :exec
INSERT INTO ledger_entries (id, tenant_id, journal_id, account_id, currency, direction, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListAccountActivity :many
SELECT id, journal_id, currency, direction, amount, created_at
FROM ledger_entries
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
  AND ($4::timestamptz IS NULL OR created_at >= $4)
  AND ($5::timestamptz IS NULL OR created_at <= $5)
ORDER BY created_at ASC, id ASC
LIMIT $6;
