package ledger

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseAmount(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    decimal.Decimal
		wantErr bool
	}{
		{"basic", "100.00", decimal.RequireFromString("100.00"), false},
		{"high precision", "0.000000000000000001", decimal.RequireFromString("0.000000000000000001"), false},
		{"zero rejected", "0", decimal.Decimal{}, true},
		{"negative rejected", "-1", decimal.Decimal{}, true},
		{"non-numeric rejected", "abc", decimal.Decimal{}, true},
		{"empty rejected", "", decimal.Decimal{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAmount(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; value=%s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}
