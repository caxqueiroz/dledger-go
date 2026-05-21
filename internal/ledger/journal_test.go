package ledger

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func amt(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestJournal_Validate_SingleCurrencyBalanced(t *testing.T) {
	j := Journal{
		EventID: "evt-1",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("100")},
		},
	}
	if err := j.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJournal_Validate_MultiCurrencyBalanced(t *testing.T) {
	j := Journal{
		EventID: "evt-2",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("100")},
			{AccountID: "c", Currency: "BRL", Direction: DirectionDebit, Amount: amt("500")},
			{AccountID: "d", Currency: "BRL", Direction: DirectionCredit, Amount: amt("500")},
		},
	}
	if err := j.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJournal_Validate_UnbalancedAcrossCurrencies(t *testing.T) {
	j := Journal{
		EventID: "evt-3",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "BRL", Direction: DirectionCredit, Amount: amt("500")},
		},
	}
	err := j.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("expected unbalanced error, got %v", err)
	}
}

func TestJournal_Validate_DebitMissesCredit(t *testing.T) {
	j := Journal{
		EventID: "evt-4",
		Entries: []Entry{
			{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("100")},
			{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("99")},
		},
	}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestJournal_Validate_EmptyEntries(t *testing.T) {
	j := Journal{EventID: "evt-5"}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestJournal_Validate_EmptyEventID(t *testing.T) {
	j := Journal{Entries: []Entry{
		{AccountID: "a", Currency: "USD", Direction: DirectionDebit, Amount: amt("1")},
		{AccountID: "b", Currency: "USD", Direction: DirectionCredit, Amount: amt("1")},
	}}
	if err := j.Validate(); err == nil {
		t.Fatal("expected error")
	}
}
