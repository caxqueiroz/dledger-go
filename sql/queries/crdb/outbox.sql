-- name: InsertOutbox :exec
INSERT INTO outbox_events (id, tenant_id, aggregate_id, event_type, idempotency_key, payload)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListPendingOutbox :many
SELECT * FROM outbox_events WHERE publish_state = 'PENDING' ORDER BY created_at ASC LIMIT $1;

-- name: MarkOutboxPublished :exec
UPDATE outbox_events SET publish_state = 'PUBLISHED', published_at = now() WHERE id = $1;

-- name: IncrementOutboxAttempts :exec
UPDATE outbox_events SET attempts = attempts + 1 WHERE id = $1;
