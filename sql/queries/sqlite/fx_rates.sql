-- name: UpsertFXRate :one
INSERT INTO fx_rates (id, tenant_id, base_currency, quote_currency, rate, source, effective_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tenant_id, base_currency, quote_currency, effective_at, source)
DO UPDATE SET rate = excluded.rate
RETURNING *;

-- name: GetFXRateAt :one
SELECT * FROM fx_rates
WHERE tenant_id = ? AND base_currency = ? AND quote_currency = ?
  AND effective_at <= ?
ORDER BY effective_at DESC, id DESC
LIMIT 1;

-- name: ListFXRates :many
SELECT * FROM fx_rates
WHERE tenant_id = ?
  AND (base_currency = ? OR ? = '')
  AND (quote_currency = ? OR ? = '')
  AND (effective_at >= ? OR ? = '')
  AND (effective_at <= ? OR ? = '')
ORDER BY effective_at DESC, id DESC
LIMIT ?;
