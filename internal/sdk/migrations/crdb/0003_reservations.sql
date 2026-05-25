-- +goose Up
CREATE TABLE reservations (
    id                    STRING PRIMARY KEY,
    tenant_id             STRING NOT NULL,
    idempotency_key       STRING NOT NULL UNIQUE,
    source_account_id     STRING NOT NULL REFERENCES accounts(id),
    reserved_account_id   STRING NOT NULL REFERENCES accounts(id),
    currency              STRING NOT NULL,
    original_amount       DECIMAL(38, 18) NOT NULL,
    outstanding_amount    DECIMAL(38, 18) NOT NULL,
    committed_amount      DECIMAL(38, 18) NOT NULL DEFAULT 0,
    released_amount       DECIMAL(38, 18) NOT NULL DEFAULT 0,
    status                STRING NOT NULL CHECK (status IN ('HELD','PARTIAL','COMMITTED','RELEASED','EXPIRED')),
    expires_at            TIMESTAMPTZ,
    flow_run_id           STRING NOT NULL REFERENCES flow_runs(id),
    metadata              JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX reservations_expiry_idx ON reservations (tenant_id, status, expires_at);

-- +goose Down
DROP TABLE reservations;
