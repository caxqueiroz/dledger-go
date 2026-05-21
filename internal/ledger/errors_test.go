package ledger

import "testing"

func TestDomainError_AsCode(t *testing.T) {
	err := NewDomainError(CodeInsufficientFunds, "no money")
	if !IsDomainCode(err, CodeInsufficientFunds) {
		t.Fatal("expected match on CodeInsufficientFunds")
	}
	if IsDomainCode(err, CodeAccountNotFound) {
		t.Fatal("should not match a different code")
	}
}
