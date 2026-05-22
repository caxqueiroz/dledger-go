-- name: InsertSnapshot :exec
INSERT INTO balance_snapshots (id, tenant_id, account_id, currency, posted_debits, posted_credits, version, snapshot_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetLatestSnapshotBefore :one
SELECT * FROM balance_snapshots
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3 AND snapshot_at <= $4
ORDER BY snapshot_at DESC, id DESC
LIMIT 1;

-- name: SumEntriesBetween :one
SELECT
    CAST(COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN amount END), 0) AS DECIMAL(38, 18)) AS debits,
    CAST(COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount END), 0) AS DECIMAL(38, 18)) AS credits
FROM ledger_entries
WHERE tenant_id = $1 AND account_id = $2 AND currency = $3
  AND created_at > $4 AND created_at <= $5;

-- name: ListAllBalancesForTenant :many
SELECT account_id, currency, posted_debits, posted_credits, version
FROM account_balances
WHERE tenant_id = $1
ORDER BY account_id, currency;
