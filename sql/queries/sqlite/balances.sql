-- name: GetBalance :one
SELECT * FROM account_balances WHERE tenant_id = ? AND account_id = ? AND currency = ?;

-- name: UpsertBalanceZero :exec
INSERT INTO account_balances (tenant_id, account_id, currency, posted_debits, posted_credits, version)
VALUES (?, ?, ?, '0', '0', 0)
ON CONFLICT (tenant_id, account_id, currency) DO NOTHING;

-- name: UpdateBalance :exec
UPDATE account_balances
SET posted_debits = ?, posted_credits = ?, version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE tenant_id = ? AND account_id = ? AND currency = ?;
