// pkg/dledger/wallet_types.go
package dledger

import (
	"time"

	"github.com/shopspring/decimal"
)

// PlayerAccounts is the pair of canonical per-player accounts the SDK manages.
type PlayerAccounts struct {
	Available string // e.g. "user:<id>:cash_available:USD"
	Reserved  string // e.g. "user:<id>:cash_reserved:USD"
}

// Receipt is returned by money-movement Wallet methods that synthesize a
// single-step PostJournal.
type Receipt struct {
	JournalID string
	FlowRunID string
}

// Reservation is the SDK's idiomatic view of a ledger reservation.
type Reservation struct {
	ID                string
	Status            string
	OriginalAmount    string
	OutstandingAmount string
	CommittedAmount   string
	ReleasedAmount    string
	ExpiresAt         time.Time // zero if no expiry
}

type DepositInput struct {
	PlayerID         string
	Currency         string
	Amount           string
	FundingAccountID string
	ExternalRef      string
	IdempotencyKey   string
	SourceService    string
}

type WithdrawInput struct {
	PlayerID            string
	Currency            string
	Amount              string
	WithdrawalAccountID string
	ExternalRef         string
	IdempotencyKey      string
	SourceService       string
}

type ReserveInput struct {
	PlayerID       string
	Currency       string
	Amount         string
	ExpiresAt      time.Time // zero = no auto-expiry
	IdempotencyKey string
	SourceService  string
	Metadata       map[string]any
}

type CommitInput struct {
	ReservationID        string
	DestinationAccountID string
	Amount               string
	IdempotencyKey       string
	SourceService        string
}

type ReleaseInput struct {
	ReservationID  string
	Amount         string
	IdempotencyKey string
	SourceService  string
}

type SettleInput struct {
	PlayerID       string
	Currency       string
	Amount         string
	PoolAccountID  string
	ExternalRef    string
	IdempotencyKey string
	SourceService  string
}

type WalletSnapshot struct {
	PlayerID         string
	Currency         string
	Available        decimal.Decimal
	Reserved         decimal.Decimal
	OpenReservations []Reservation
}
