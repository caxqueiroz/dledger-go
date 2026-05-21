-- name: InsertOutbox :exec
INSERT INTO outbox_events (id, tenant_id, aggregate_id, event_type, idempotency_key, payload)
VALUES (?, ?, ?, ?, ?, ?);

-- name: ListPendingOutbox :many
SELECT * FROM outbox_events WHERE publish_state = 'PENDING' ORDER BY created_at ASC LIMIT ?;

-- name: MarkOutboxPublished :exec
UPDATE outbox_events SET publish_state = 'PUBLISHED', published_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = ?;

-- name: IncrementOutboxAttempts :exec
UPDATE outbox_events SET attempts = attempts + 1 WHERE id = ?;
