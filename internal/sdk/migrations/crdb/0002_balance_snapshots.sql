-- +goose Up
CREATE TABLE balance_snapshots (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    posted_debits   DECIMAL(38, 18) NOT NULL,
    posted_credits  DECIMAL(38, 18) NOT NULL,
    version         INT8 NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX balance_snapshots_lookup_idx
    ON balance_snapshots (tenant_id, account_id, currency, snapshot_at DESC);

-- +goose Down
DROP TABLE balance_snapshots;
