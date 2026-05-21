-- name: InsertJournal :exec
INSERT INTO ledger_journals (id, tenant_id, flow_run_id, event_id, source_service, source_type, actor_id, metadata)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetJournal :one
SELECT * FROM ledger_journals WHERE tenant_id = ? AND id = ?;

-- name: GetJournalsByFlowRun :many
SELECT * FROM ledger_journals WHERE tenant_id = ? AND flow_run_id = ?;
