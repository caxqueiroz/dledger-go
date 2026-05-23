-- name: InsertSnapshot :exec
INSERT INTO balance_snapshots (id, tenant_id, account_id, currency, posted_debits, posted_credits, version, snapshot_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetLatestSnapshotBefore :one
SELECT * FROM balance_snapshots
WHERE tenant_id = ? AND account_id = ? AND currency = ? AND snapshot_at <= ?
ORDER BY snapshot_at DESC, id DESC
LIMIT 1;

-- name: SumEntriesBetween :one
SELECT
    CAST(COALESCE(SUM(CASE WHEN direction = 'DEBIT'  THEN CAST(amount AS REAL) END), 0.0) AS REAL) AS debits,
    CAST(COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN CAST(amount AS REAL) END), 0.0) AS REAL) AS credits
FROM ledger_entries
WHERE tenant_id = ? AND account_id = ? AND currency = ?
  AND created_at > ? AND created_at <= ?;

-- name: ListAllBalancesForTenant :many
SELECT account_id, currency, posted_debits, posted_credits, version
FROM account_balances
WHERE tenant_id = ?
ORDER BY account_id, currency;

-- name: ListTenantsDueForSnapshot :many
SELECT DISTINCT a.tenant_id
FROM accounts a
WHERE NOT EXISTS (
    SELECT 1 FROM balance_snapshots bs
    WHERE bs.tenant_id = a.tenant_id AND bs.snapshot_at > ?
)
LIMIT ?;

-- name: PruneSnapshotsOlderThan :execrows
DELETE FROM balance_snapshots
WHERE id IN (
  SELECT bs.id FROM balance_snapshots bs
  WHERE bs.snapshot_at < ?
    AND EXISTS (
      SELECT 1 FROM balance_snapshots bs2
      WHERE bs2.tenant_id = bs.tenant_id
        AND bs2.account_id = bs.account_id
        AND bs2.currency = bs.currency
        AND bs2.snapshot_at > bs.snapshot_at
    )
  LIMIT ?
);
