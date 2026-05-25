-- +goose Up
CREATE TABLE accounts (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    owner_type      TEXT NOT NULL,
    owner_id        TEXT NOT NULL,
    account_type    TEXT NOT NULL,
    currency        TEXT NOT NULL,
    normal_balance  TEXT NOT NULL CHECK (normal_balance IN ('DEBIT','CREDIT')),
    allow_negative  INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, owner_type, owner_id, account_type, currency)
);

CREATE TABLE ledger_journals (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    flow_run_id     TEXT,
    event_id        TEXT NOT NULL UNIQUE,
    source_service  TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    actor_id        TEXT NOT NULL,
    metadata        TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE ledger_entries (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    journal_id      TEXT NOT NULL REFERENCES ledger_journals(id),
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    direction       TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount          TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (tenant_id, account_id, currency, created_at);

CREATE TABLE account_balances (
    tenant_id       TEXT NOT NULL,
    account_id      TEXT NOT NULL REFERENCES accounts(id),
    currency        TEXT NOT NULL,
    posted_debits   TEXT NOT NULL DEFAULT '0',
    posted_credits  TEXT NOT NULL DEFAULT '0',
    version         INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (tenant_id, account_id, currency)
);

CREATE TABLE flow_runs (
    id               TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    flow_type         TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    source_service   TEXT NOT NULL,
    actor_id         TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    metadata         TEXT NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at     TEXT,
    failed_at        TEXT
);

CREATE TABLE flow_steps (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    flow_run_id  TEXT NOT NULL REFERENCES flow_runs(id),
    step_id      TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('COMPLETED','FAILED')),
    journal_id   TEXT REFERENCES ledger_journals(id),
    error_code   TEXT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, flow_run_id, step_id)
);

CREATE TABLE outbox_events (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    aggregate_id      TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL UNIQUE,
    payload           TEXT NOT NULL,
    publish_state     TEXT NOT NULL DEFAULT 'PENDING',
    attempts          INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    published_at      TEXT
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (publish_state, created_at);

-- +goose Down
DROP TABLE outbox_events;
DROP TABLE flow_steps;
DROP TABLE flow_runs;
DROP TABLE account_balances;
DROP TABLE ledger_entries;
DROP TABLE ledger_journals;
DROP TABLE accounts;
