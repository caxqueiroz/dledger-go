-- +goose Up
CREATE TABLE external_records (
    id                  TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    source              TEXT NOT NULL,
    external_ref        TEXT NOT NULL,
    amount              TEXT NOT NULL,
    currency            TEXT NOT NULL,
    occurred_at         TEXT NOT NULL,
    account_id          TEXT,
    raw_payload         TEXT NOT NULL DEFAULT '{}',
    match_status        TEXT NOT NULL DEFAULT 'UNMATCHED' CHECK (match_status IN ('UNMATCHED','MATCHED','MISMATCHED')),
    matched_journal_id  TEXT REFERENCES ledger_journals(id),
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (tenant_id, source, external_ref)
);
CREATE INDEX external_records_window_idx ON external_records (tenant_id, source, occurred_at);
CREATE INDEX external_records_match_idx  ON external_records (tenant_id, source, match_status);

CREATE TABLE reconciliation_batches (
    id                          TEXT PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    idempotency_key             TEXT NOT NULL UNIQUE,
    source                      TEXT NOT NULL,
    window_start                TEXT NOT NULL,
    window_end                  TEXT NOT NULL,
    status                      TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')),
    ingested_count              INTEGER NOT NULL DEFAULT 0,
    matched_count               INTEGER NOT NULL DEFAULT 0,
    mismatched_count            INTEGER NOT NULL DEFAULT 0,
    missing_in_ledger_count     INTEGER NOT NULL DEFAULT 0,
    missing_in_external_count   INTEGER NOT NULL DEFAULT 0,
    started_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at                TEXT,
    actor_id                    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE discrepancies (
    id                      TEXT PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    batch_id                TEXT NOT NULL REFERENCES reconciliation_batches(id),
    type                    TEXT NOT NULL CHECK (type IN ('MISSING_IN_LEDGER','MISSING_IN_EXTERNAL','AMOUNT_MISMATCH')),
    external_record_id      TEXT REFERENCES external_records(id),
    journal_id              TEXT REFERENCES ledger_journals(id),
    status                  TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED','IGNORED')),
    resolution_journal_id   TEXT REFERENCES ledger_journals(id),
    resolution_note         TEXT NOT NULL DEFAULT '',
    resolved_by             TEXT NOT NULL DEFAULT '',
    resolved_at             TEXT,
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX discrepancies_status_idx ON discrepancies (tenant_id, status, batch_id);

-- +goose Down
DROP TABLE discrepancies;
DROP TABLE reconciliation_batches;
DROP TABLE external_records;
