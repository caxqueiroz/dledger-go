-- +goose Up
CREATE TABLE external_records (
    id                  STRING PRIMARY KEY,
    tenant_id           STRING NOT NULL,
    source              STRING NOT NULL,
    external_ref        STRING NOT NULL,
    amount              DECIMAL(38, 18) NOT NULL,
    currency            STRING NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    account_id          STRING,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::JSONB,
    match_status        STRING NOT NULL DEFAULT 'UNMATCHED' CHECK (match_status IN ('UNMATCHED','MATCHED','MISMATCHED')),
    matched_journal_id  STRING REFERENCES ledger_journals(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, source, external_ref)
);
CREATE INDEX external_records_window_idx ON external_records (tenant_id, source, occurred_at);
CREATE INDEX external_records_match_idx  ON external_records (tenant_id, source, match_status);

CREATE TABLE reconciliation_batches (
    id                          STRING PRIMARY KEY,
    tenant_id                   STRING NOT NULL,
    idempotency_key             STRING NOT NULL UNIQUE,
    source                      STRING NOT NULL,
    window_start                TIMESTAMPTZ NOT NULL,
    window_end                  TIMESTAMPTZ NOT NULL,
    status                      STRING NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    ingested_count              INT8 NOT NULL DEFAULT 0,
    matched_count               INT8 NOT NULL DEFAULT 0,
    mismatched_count            INT8 NOT NULL DEFAULT 0,
    missing_in_ledger_count     INT8 NOT NULL DEFAULT 0,
    missing_in_external_count   INT8 NOT NULL DEFAULT 0,
    started_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                TIMESTAMPTZ,
    actor_id                    STRING NOT NULL DEFAULT ''
);

CREATE TABLE discrepancies (
    id                      STRING PRIMARY KEY,
    tenant_id               STRING NOT NULL,
    batch_id                STRING NOT NULL REFERENCES reconciliation_batches(id),
    type                    STRING NOT NULL CHECK (type IN ('MISSING_IN_LEDGER','MISSING_IN_EXTERNAL','AMOUNT_MISMATCH')),
    external_record_id      STRING REFERENCES external_records(id),
    journal_id              STRING REFERENCES ledger_journals(id),
    status                  STRING NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED','IGNORED')),
    resolution_journal_id   STRING REFERENCES ledger_journals(id),
    resolution_note         STRING NOT NULL DEFAULT '',
    resolved_by             STRING NOT NULL DEFAULT '',
    resolved_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX discrepancies_status_idx ON discrepancies (tenant_id, status, batch_id);

-- +goose Down
DROP TABLE discrepancies;
DROP TABLE reconciliation_batches;
DROP TABLE external_records;
