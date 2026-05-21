-- name: GetBalance :one
SELECT * FROM account_balances WHERE tenant_id = $1 AND account_id = $2 AND currency = $3;

-- name: LockBalance :one
SELECT * FROM account_balances
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
FOR UPDATE;

-- name: UpsertBalanceZero :exec
INSERT INTO account_balances (tenant_id, account_id, currency)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, account_id, currency) DO NOTHING;

-- name: UpdateBalance :exec
UPDATE account_balances
SET posted_debits = $4, posted_credits = $5, version = version + 1, updated_at = now()
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3;
