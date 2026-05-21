-- name: InsertFlowRun :exec
INSERT INTO flow_runs (id, tenant_id, flow_type, idempotency_key, source_service, actor_id, status, metadata)
VALUES ($1, $2, $3, $4, $5, $6, 'RUNNING', $7);

-- name: GetFlowByIdempotency :one
SELECT * FROM flow_runs WHERE tenant_id = $1 AND idempotency_key = $2 FOR UPDATE;

-- name: GetFlowByID :one
SELECT * FROM flow_runs WHERE tenant_id = $1 AND id = $2;

-- name: CompleteFlowRun :exec
UPDATE flow_runs SET status = 'COMPLETED', completed_at = now() WHERE id = $1 AND tenant_id = $2;

-- name: InsertFlowStep :exec
INSERT INTO flow_steps (id, tenant_id, flow_run_id, step_id, status, journal_id, error_code)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetFlowSteps :many
SELECT * FROM flow_steps WHERE tenant_id = $1 AND flow_run_id = $2 ORDER BY created_at ASC;
