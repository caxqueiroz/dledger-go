// pkg/dledger/errors.go
package dledger

import (
	"errors"

	"connectrpc.com/connect"
)

// ErrCode mirrors the ledger.DomainCode values the server sets on the
// "ledger-error-code" Connect header. Works identically for embedded and
// remote backends because both round-trip via *connect.Error.
type ErrCode string

const (
	ErrInsufficientFunds           ErrCode = "INSUFFICIENT_FUNDS"
	ErrAccountNotFound             ErrCode = "ACCOUNT_NOT_FOUND"
	ErrAccountCurrencyMismatch     ErrCode = "ACCOUNT_CURRENCY_MISMATCH"
	ErrUnbalancedJournal           ErrCode = "UNBALANCED_JOURNAL"
	ErrDuplicateIdempotencyKey     ErrCode = "DUPLICATE_IDEMPOTENCY_KEY"
	ErrFlowAlreadyCompleted        ErrCode = "FLOW_ALREADY_COMPLETED"
	ErrFlowConflict                ErrCode = "FLOW_CONFLICT"
	ErrInvalidAccountStatus        ErrCode = "INVALID_ACCOUNT_STATUS"
	ErrSerializationRetryExhausted ErrCode = "SERIALIZATION_RETRY_EXHAUSTED"
	ErrReservationNotFound         ErrCode = "RESERVATION_NOT_FOUND"
	ErrReservationClosed           ErrCode = "RESERVATION_CLOSED"
	ErrReservationAmountExceeds    ErrCode = "RESERVATION_AMOUNT_EXCEEDS"
	ErrReservationCurrencyMismatch ErrCode = "RESERVATION_CURRENCY_MISMATCH"
	ErrFXRateNotFound              ErrCode = "FX_RATE_NOT_FOUND"
	ErrFXAmountMismatch            ErrCode = "FX_AMOUNT_MISMATCH"
	ErrDiscrepancyNotFound         ErrCode = "DISCREPANCY_NOT_FOUND"
	ErrDiscrepancyClosed           ErrCode = "DISCREPANCY_CLOSED"
	ErrReconBatchNotFound          ErrCode = "RECON_BATCH_NOT_FOUND"
)

// IsErrCode reports whether err is a Connect-RPC error whose
// "ledger-error-code" header equals code.
func IsErrCode(err error, code ErrCode) bool {
	if err == nil {
		return false
	}
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return false
	}
	return ce.Meta().Get("ledger-error-code") == string(code)
}
