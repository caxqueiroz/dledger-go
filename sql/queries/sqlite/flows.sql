-- name: InsertFlowRun :exec
INSERT INTO flow_runs (id, tenant_id, flow_type, idempotency_key, source_service, actor_id, status, metadata)
VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?);

-- name: GetFlowByIdempotency :one
SELECT * FROM flow_runs WHERE tenant_id = ? AND idempotency_key = ?;

-- name: GetFlowByID :one
SELECT * FROM flow_runs WHERE tenant_id = ? AND id = ?;

-- name: CompleteFlowRun :exec
UPDATE flow_runs SET status = 'COMPLETED', completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = ? AND tenant_id = ?;

-- name: InsertFlowStep :exec
INSERT INTO flow_steps (id, tenant_id, flow_run_id, step_id, status, journal_id, error_code)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetFlowSteps :many
SELECT * FROM flow_steps WHERE tenant_id = ? AND flow_run_id = ? ORDER BY created_at ASC;
