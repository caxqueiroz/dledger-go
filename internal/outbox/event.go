package outbox

import "time"

type Event struct {
	ID             string
	TenantID       string
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}
