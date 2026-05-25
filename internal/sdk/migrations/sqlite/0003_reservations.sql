-- +goose Up
CREATE TABLE reservations (
    id                    TEXT PRIMARY KEY,
    tenant_id             TEXT NOT NULL,
    idempotency_key       TEXT NOT NULL UNIQUE,
    source_account_id     TEXT NOT NULL REFERENCES accounts(id),
    reserved_account_id   TEXT NOT NULL REFERENCES accounts(id),
    currency              TEXT NOT NULL,
    original_amount       TEXT NOT NULL,
    outstanding_amount    TEXT NOT NULL,
    committed_amount      TEXT NOT NULL DEFAULT '0',
    released_amount       TEXT NOT NULL DEFAULT '0',
    status                TEXT NOT NULL CHECK (status IN ('HELD','PARTIAL','COMMITTED','RELEASED','EXPIRED')),
    expires_at            TEXT,
    flow_run_id           TEXT NOT NULL REFERENCES flow_runs(id),
    metadata              TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX reservations_expiry_idx ON reservations (tenant_id, status, expires_at);

-- +goose Down
DROP TABLE reservations;
