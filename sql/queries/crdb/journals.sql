-- name: InsertJournal :exec
INSERT INTO ledger_journals (id, tenant_id, flow_run_id, event_id, source_service, source_type, actor_id, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetJournal :one
SELECT * FROM ledger_journals WHERE tenant_id = $1 AND id = $2;

-- name: GetJournalsByFlowRun :many
SELECT * FROM ledger_journals WHERE tenant_id = $1 AND flow_run_id = $2;
