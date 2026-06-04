package dynamo

import "testing"

func TestKeyBuilders(t *testing.T) {
	tests := []struct{ name, got, want string }{
		{"account", accountPK("t1", "a1"), "ACC#t1#a1"},
		{"account uniq", accountUniqPK("t1", "user", "p42", "cash_available", "BRL"), "ACCU#t1#user#p42#cash_available#BRL"},
		{"balance", balancePK("t1", "a1", "BRL"), "BAL#t1#a1#BRL"},
		{"journal", journalPK("t1", "j1"), "JRN#t1#j1"},
		{"event uniq", eventUniqPK("evt-9"), "EVT#evt-9"},
		{"flow", flowPK("t1", "f1"), "FLOW#t1#f1"},
		{"flow idemp", flowIdempPK("t1", "k1"), "FIDEMP#t1#k1"},
		{"outbox", outboxPK("o1"), "OBX#o1"},
		{"reservation", reservationPK("t1", "r1"), "RES#t1#r1"},
		{"res idemp", resIdempPK("t1", "k1"), "RIDEMP#t1#k1"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}
