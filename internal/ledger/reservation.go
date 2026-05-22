// internal/ledger/reservation.go
package ledger

import (
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

type ReservationStatus string

const (
	ReservationHeld      ReservationStatus = "HELD"
	ReservationPartial   ReservationStatus = "PARTIAL"
	ReservationCommitted ReservationStatus = "COMMITTED"
	ReservationReleased  ReservationStatus = "RELEASED"
	ReservationExpired   ReservationStatus = "EXPIRED"
)

// Closed reports whether s is a terminal status (no further transitions allowed).
func (s ReservationStatus) Closed() bool {
	switch s {
	case ReservationCommitted, ReservationReleased, ReservationExpired:
		return true
	}
	return false
}

// Reservation models a held-fund object with partial commit/release support
// and optional auto-expiry.
type Reservation struct {
	ID                string
	TenantID          string
	IdempotencyKey    string
	SourceAccountID   string
	ReservedAccountID string
	Currency          string
	OriginalAmount    decimal.Decimal
	OutstandingAmount decimal.Decimal
	CommittedAmount   decimal.Decimal
	ReleasedAmount    decimal.Decimal
	Status            ReservationStatus
	ExpiresAt         *time.Time
	FlowRunID         string
	Metadata          map[string]any
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Validate enforces the conservation invariant: outstanding + committed +
// released must always equal original.
func (r *Reservation) Validate() error {
	sum := r.OutstandingAmount.Add(r.CommittedAmount).Add(r.ReleasedAmount)
	if !sum.Equal(r.OriginalAmount) {
		return errors.New("reservation: outstanding+committed+released != original")
	}
	return nil
}
