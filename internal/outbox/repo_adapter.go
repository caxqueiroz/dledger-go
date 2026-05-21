package outbox

import (
	"context"

	"github.com/caxqueiroz/doubleledger/internal/repo"
)

type RepoAdapter struct{ Store repo.Store }

func (a RepoAdapter) PendingOutbox(ctx context.Context, limit int) ([]Event, error) {
	rows, err := a.Store.PendingOutbox(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Event, len(rows))
	for i, r := range rows {
		out[i] = Event{
			ID: r.ID, TenantID: r.TenantID, AggregateID: r.AggregateID,
			EventType: r.EventType, IdempotencyKey: r.IdempotencyKey,
			Payload: r.Payload, CreatedAt: r.CreatedAt,
		}
	}
	return out, nil
}

func (a RepoAdapter) MarkOutboxPublished(ctx context.Context, id string) error {
	return a.Store.MarkOutboxPublished(ctx, id)
}

func (a RepoAdapter) IncrementOutboxAttempts(ctx context.Context, id string) error {
	return a.Store.IncrementOutboxAttempts(ctx, id)
}
