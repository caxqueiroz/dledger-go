-- +goose Up
CREATE TABLE fx_rates (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    base_currency   STRING NOT NULL,
    quote_currency  STRING NOT NULL,
    rate            DECIMAL(38, 18) NOT NULL,
    source          STRING NOT NULL,
    effective_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, base_currency, quote_currency, effective_at, source)
);
CREATE INDEX fx_rates_lookup_idx
    ON fx_rates (tenant_id, base_currency, quote_currency, effective_at DESC);

-- +goose Down
DROP TABLE fx_rates;
