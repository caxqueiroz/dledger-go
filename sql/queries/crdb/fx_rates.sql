-- name: UpsertFXRate :one
INSERT INTO fx_rates (id, tenant_id, base_currency, quote_currency, rate, source, effective_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id, base_currency, quote_currency, effective_at, source)
DO UPDATE SET rate = excluded.rate
RETURNING *;

-- name: GetFXRateAt :one
SELECT * FROM fx_rates
WHERE tenant_id = $1 AND base_currency = $2 AND quote_currency = $3
  AND effective_at <= $4
ORDER BY effective_at DESC, id DESC
LIMIT 1;

-- name: ListFXRates :many
SELECT * FROM fx_rates
WHERE tenant_id = $1
  AND ($2::text = '' OR base_currency = $2)
  AND ($3::text = '' OR quote_currency = $3)
  AND ($4::timestamptz IS NULL OR effective_at >= $4)
  AND ($5::timestamptz IS NULL OR effective_at <= $5)
ORDER BY effective_at DESC, id DESC
LIMIT $6;
