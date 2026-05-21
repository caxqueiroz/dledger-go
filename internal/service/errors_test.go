package service

import (
	"errors"
	"testing"

	"connectrpc.com/connect"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func TestToConnectError_Mapping(t *testing.T) {
	cases := []struct {
		code     ledger.DomainCode
		expected connect.Code
	}{
		{ledger.CodeInsufficientFunds, connect.CodeFailedPrecondition},
		{ledger.CodeAccountNotFound, connect.CodeNotFound},
		{ledger.CodeAccountCurrencyMismatch, connect.CodeInvalidArgument},
		{ledger.CodeUnbalancedJournal, connect.CodeInvalidArgument},
		{ledger.CodeDuplicateIdempotencyKey, connect.CodeAlreadyExists},
		{ledger.CodeFlowAlreadyCompleted, connect.CodeAlreadyExists},
		{ledger.CodeFlowConflict, connect.CodeAborted},
		{ledger.CodeInvalidAccountStatus, connect.CodeFailedPrecondition},
		{ledger.CodeSerializationRetryExhausted, connect.CodeAborted},
	}
	for _, c := range cases {
		t.Run(string(c.code), func(t *testing.T) {
			err := ToConnectError(ledger.NewDomainError(c.code, "x"))
			var cerr *connect.Error
			if !errors.As(err, &cerr) {
				t.Fatalf("not a connect.Error: %v", err)
			}
			if cerr.Code() != c.expected {
				t.Fatalf("want %s, got %s", c.expected, cerr.Code())
			}
		})
	}
}

func TestToConnectError_GenericErrorIsInternal(t *testing.T) {
	err := ToConnectError(errors.New("boom"))
	var cerr *connect.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("not a connect.Error")
	}
	if cerr.Code() != connect.CodeInternal {
		t.Fatalf("want Internal, got %s", cerr.Code())
	}
}

func TestToConnectError_NilReturnsNil(t *testing.T) {
	if err := ToConnectError(nil); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
}
