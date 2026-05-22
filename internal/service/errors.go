package service

import (
	"errors"

	"connectrpc.com/connect"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

// ToConnectError converts a domain error into a Connect-RPC error with the
// appropriate code, and attaches the domain code in a response header so
// clients can branch on it.
func ToConnectError(err error) error {
	if err == nil {
		return nil
	}
	if de, ok := errors.AsType[*ledger.DomainError](err); ok {
		code := connect.CodeInternal
		switch de.Code {
		case ledger.CodeInsufficientFunds, ledger.CodeInvalidAccountStatus, ledger.CodeReservationClosed:
			code = connect.CodeFailedPrecondition
		case ledger.CodeAccountNotFound, ledger.CodeReservationNotFound:
			code = connect.CodeNotFound
		case ledger.CodeAccountCurrencyMismatch, ledger.CodeUnbalancedJournal,
			ledger.CodeReservationAmountExceeds, ledger.CodeReservationCurrencyMismatch:
			code = connect.CodeInvalidArgument
		case ledger.CodeDuplicateIdempotencyKey, ledger.CodeFlowAlreadyCompleted:
			code = connect.CodeAlreadyExists
		case ledger.CodeFlowConflict, ledger.CodeSerializationRetryExhausted:
			code = connect.CodeAborted
		}
		ce := connect.NewError(code, de)
		ce.Meta().Set("ledger-error-code", string(de.Code))
		return ce
	}
	return connect.NewError(connect.CodeInternal, err)
}
