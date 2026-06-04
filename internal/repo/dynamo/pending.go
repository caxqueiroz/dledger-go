// Temporary stubs — replaced by Tasks 8 (reservations) and 9 (outbox).
// Task 8 reservation stubs have been moved to reads_reservations.go and
// the Tx methods are implemented in tx.go.
package dynamo

import (
	"context"

	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ---------------------------------------------------------------------------
// Outbox store methods (Task 9)
// ---------------------------------------------------------------------------

// PendingOutbox returns (nil, nil) so the background outbox dispatcher stays
// quiet rather than crashing on an unsupported-operation error.
func (s *Store) PendingOutbox(_ context.Context, _ int) ([]repo.OutboxEvent, error) {
	return nil, nil
}

func (s *Store) MarkOutboxPublished(_ context.Context, _ string) error {
	return errUnsupported("MarkOutboxPublished (implemented in a later task)")
}

func (s *Store) IncrementOutboxAttempts(_ context.Context, _ string) error {
	return errUnsupported("IncrementOutboxAttempts (implemented in a later task)")
}
