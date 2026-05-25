// pkg/dledger/errors_test.go
package dledger

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
)

func TestIsErrCode_MatchesHeader(t *testing.T) {
	e := connect.NewError(connect.CodeFailedPrecondition, errors.New("not enough"))
	e.Meta().Set("ledger-error-code", string(ErrInsufficientFunds))
	if !IsErrCode(e, ErrInsufficientFunds) {
		t.Fatalf("expected IsErrCode to return true for matching code")
	}
	if IsErrCode(e, ErrAccountNotFound) {
		t.Fatalf("expected IsErrCode to return false for mismatching code")
	}
}

func TestIsErrCode_NilAndNonConnect(t *testing.T) {
	if IsErrCode(nil, ErrInsufficientFunds) {
		t.Fatalf("nil error must not match")
	}
	if IsErrCode(errors.New("plain"), ErrInsufficientFunds) {
		t.Fatalf("plain error must not match")
	}
}
