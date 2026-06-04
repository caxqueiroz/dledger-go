package ledger

import "errors"

type DomainCode string

const (
	CodeInsufficientFunds           DomainCode = "INSUFFICIENT_FUNDS"
	CodeAccountNotFound             DomainCode = "ACCOUNT_NOT_FOUND"
	CodeAccountCurrencyMismatch     DomainCode = "ACCOUNT_CURRENCY_MISMATCH"
	CodeUnbalancedJournal           DomainCode = "UNBALANCED_JOURNAL"
	CodeDuplicateIdempotencyKey     DomainCode = "DUPLICATE_IDEMPOTENCY_KEY"
	CodeFlowAlreadyCompleted        DomainCode = "FLOW_ALREADY_COMPLETED"
	CodeFlowConflict                DomainCode = "FLOW_CONFLICT"
	CodeInvalidAccountStatus        DomainCode = "INVALID_ACCOUNT_STATUS"
	CodeSerializationRetryExhausted DomainCode = "SERIALIZATION_RETRY_EXHAUSTED"
	CodeReservationNotFound         DomainCode = "RESERVATION_NOT_FOUND"
	CodeReservationClosed           DomainCode = "RESERVATION_CLOSED"
	CodeReservationAmountExceeds    DomainCode = "RESERVATION_AMOUNT_EXCEEDS"
	CodeReservationCurrencyMismatch DomainCode = "RESERVATION_CURRENCY_MISMATCH"
	CodeFXRateNotFound              DomainCode = "FX_RATE_NOT_FOUND"
	CodeFXAmountMismatch            DomainCode = "FX_AMOUNT_MISMATCH"
	CodeDiscrepancyNotFound         DomainCode = "DISCREPANCY_NOT_FOUND"
	CodeDiscrepancyClosed           DomainCode = "DISCREPANCY_CLOSED"
	CodeReconBatchNotFound          DomainCode = "RECON_BATCH_NOT_FOUND"
	CodeUnsupportedOperation        DomainCode = "UNSUPPORTED_OPERATION"
)

type DomainError struct {
	Code    DomainCode
	Message string
}

func (e *DomainError) Error() string { return string(e.Code) + ": " + e.Message }

func NewDomainError(code DomainCode, msg string) *DomainError {
	return &DomainError{Code: code, Message: msg}
}

// IsDomainCode reports whether err (or any wrapped error) is a DomainError
// with the given code.
func IsDomainCode(err error, code DomainCode) bool {
	if de, ok := errors.AsType[*DomainError](err); ok {
		return de.Code == code
	}
	return false
}
