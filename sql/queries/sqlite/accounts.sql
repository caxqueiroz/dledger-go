-- name: InsertAccount :exec
INSERT INTO accounts (id, tenant_id, owner_type, owner_id, account_type, currency, normal_balance, allow_negative, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetAccount :one
SELECT * FROM accounts WHERE tenant_id = ? AND id = ?;

-- name: ListAccountsByOwner :many
SELECT * FROM accounts WHERE tenant_id = ? AND owner_type = ? AND owner_id = ?;
