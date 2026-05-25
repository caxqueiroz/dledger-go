-- +goose Up
CREATE TABLE fx_rates (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    base_currency   TEXT NOT NULL,
    quote_currency  TEXT NOT NULL,
    rate            TEXT NOT NULL,
    source          TEXT NOT NULL,
    effective_at    TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, base_currency, quote_currency, effective_at, source)
);
CREATE INDEX fx_rates_lookup_idx
    ON fx_rates (tenant_id, base_currency, quote_currency, effective_at DESC);

-- +goose Down
DROP TABLE fx_rates;
