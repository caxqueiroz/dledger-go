-- name: InsertAccount :exec
INSERT INTO accounts (id, tenant_id, owner_type, owner_id, account_type, currency, normal_balance, allow_negative, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: GetAccount :one
SELECT * FROM accounts WHERE tenant_id = $1 AND id = $2;

-- name: ListAccountsByOwner :many
SELECT * FROM accounts WHERE tenant_id = $1 AND owner_type = $2 AND owner_id = $3;
