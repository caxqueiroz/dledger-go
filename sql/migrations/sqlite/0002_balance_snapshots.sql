-- +goose Up
CREATE TABLE balance_snapshots (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    posted_debits   TEXT NOT NULL,
    posted_credits  TEXT NOT NULL,
    version         INTEGER NOT NULL,
    snapshot_at     TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX balance_snapshots_lookup_idx
    ON balance_snapshots (tenant_id, account_id, currency, snapshot_at DESC);

-- +goose Down
DROP TABLE balance_snapshots;
