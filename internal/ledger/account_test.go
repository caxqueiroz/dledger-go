package ledger

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestNormalizedBalance(t *testing.T) {
	debits := decimal.RequireFromString("100")
	credits := decimal.RequireFromString("30")

	got := NormalizedBalance(NormalDebit, debits, credits)
	if !got.Equal(decimal.RequireFromString("70")) {
		t.Fatalf("debit-normal: got %s, want 70", got)
	}

	got = NormalizedBalance(NormalCredit, debits, credits)
	if !got.Equal(decimal.RequireFromString("-70")) {
		t.Fatalf("credit-normal: got %s, want -70", got)
	}
}

func TestAccountStatusValidation(t *testing.T) {
	for _, s := range []AccountStatus{AccountActive, AccountFrozen, AccountClosed} {
		if !s.Valid() {
			t.Fatalf("%q should be valid", s)
		}
	}
	if AccountStatus("WAT").Valid() {
		t.Fatal("WAT should be invalid")
	}
}
