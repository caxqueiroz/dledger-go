// Temporary stubs — replaced by Tasks 8 and 9.
package dynamo

import (
	"context"
	"time"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ---------------------------------------------------------------------------
// Reservations (Task 8)
// ---------------------------------------------------------------------------

func (s *Store) GetReservation(_ context.Context, _, _ string) (*ledger.Reservation, error) {
	return nil, errUnsupported("GetReservation (implemented in a later task)")
}

// ListExpiredReservations is a quiet scheduler no-op: returns (nil, nil) so
// the expiry scheduler simply finds nothing to expire rather than crashing.
func (s *Store) ListExpiredReservations(_ context.Context, _ time.Time, _ int) ([]repo.ExpiredReservation, error) {
	return nil, nil
}

func (s *Store) ListReservations(_ context.Context, _ repo.ListReservationsInput) ([]ledger.Reservation, error) {
	return nil, errUnsupported("ListReservations (implemented in a later task)")
}

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
