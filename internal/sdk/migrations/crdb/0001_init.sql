-- +goose Up
CREATE TABLE accounts (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    owner_type      STRING NOT NULL,
    owner_id        STRING NOT NULL,
    account_type    STRING NOT NULL,
    currency        STRING NOT NULL,
    normal_balance  STRING NOT NULL CHECK (normal_balance IN ('DEBIT','CREDIT')),
    allow_negative  BOOL NOT NULL DEFAULT false,
    status          STRING NOT NULL CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_type, owner_id, account_type, currency)
);

CREATE TABLE ledger_journals (
    id              STRING PRIMARY KEY,
    tenant_id       STRING NOT NULL,
    flow_run_id     STRING,
    event_id        STRING NOT NULL UNIQUE,
    source_service  STRING NOT NULL,
    source_type     STRING NOT NULL,
    actor_id        STRING NOT NULL,
    metadata        JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       STRING NOT NULL,
    journal_id      STRING NOT NULL REFERENCES ledger_journals(id),
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    direction       STRING NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount          DECIMAL(38, 18) NOT NULL CHECK (amount > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (tenant_id, account_id, currency, created_at);

CREATE TABLE account_balances (
    tenant_id       STRING NOT NULL,
    account_id      STRING NOT NULL REFERENCES accounts(id),
    currency        STRING NOT NULL,
    posted_debits   DECIMAL(38, 18) NOT NULL DEFAULT 0,
    posted_credits  DECIMAL(38, 18) NOT NULL DEFAULT 0,
    version         INT8 NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, account_id, currency)
);

CREATE TABLE flow_runs (
    id               STRING PRIMARY KEY,
    tenant_id        STRING NOT NULL,
    flow_type        STRING NOT NULL,
    idempotency_key  STRING NOT NULL UNIQUE,
    source_service   STRING NOT NULL,
    actor_id         STRING NOT NULL,
    status           STRING NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    metadata         JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    failed_at        TIMESTAMPTZ
);

CREATE TABLE flow_steps (
    id           STRING PRIMARY KEY,
    tenant_id    STRING NOT NULL,
    flow_run_id  STRING NOT NULL REFERENCES flow_runs(id),
    step_id      STRING NOT NULL,
    status       STRING NOT NULL CHECK (status IN ('COMPLETED','FAILED')),
    journal_id   STRING REFERENCES ledger_journals(id),
    error_code   STRING,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, flow_run_id, step_id)
);

CREATE TABLE outbox_events (
    id                STRING PRIMARY KEY,
    tenant_id         STRING NOT NULL,
    aggregate_id      STRING NOT NULL,
    event_type        STRING NOT NULL,
    idempotency_key   STRING NOT NULL UNIQUE,
    payload           JSONB NOT NULL,
    publish_state     STRING NOT NULL DEFAULT 'PENDING',
    attempts          INT8 NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at      TIMESTAMPTZ
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
