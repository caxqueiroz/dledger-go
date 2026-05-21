-- name: InsertEntry :exec
INSERT INTO ledger_entries (id, tenant_id, journal_id, account_id, currency, direction, amount)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: ListAccountActivity :many
SELECT id, journal_id, currency, direction, amount, created_at
FROM ledger_entries
WHERE tenant_id = ? AND account_id = ? AND currency = ?
  AND (created_at >= ? OR ? = '')
  AND (created_at <= ? OR ? = '')
ORDER BY created_at ASC, id ASC
LIMIT ?;
