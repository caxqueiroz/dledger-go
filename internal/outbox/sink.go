package outbox

import (
	"context"
	"log/slog"
)

type Sink interface {
	Publish(ctx context.Context, e Event) error
}

type LogSink struct {
	Logger *slog.Logger
}

func (s LogSink) Publish(ctx context.Context, e Event) error {
	s.Logger.InfoContext(ctx, "outbox.publish",
		"event_id", e.ID, "tenant_id", e.TenantID, "event_type", e.EventType,
		"idempotency_key", e.IdempotencyKey)
	return nil
}
