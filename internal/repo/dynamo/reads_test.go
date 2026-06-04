package dynamo

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func TestGetBalanceMissingIsZero(t *testing.T) {
	s := newTestStore(t)
	d, c, ver, err := s.GetBalance(context.Background(), "t1", "a-none", "BRL")
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !d.Equal(decimal.Zero) || !c.Equal(decimal.Zero) || ver != 0 {
		t.Errorf("got %s/%s/%d, want 0/0/0", d, c, ver)
	}
}

func TestGetAccountMissing(t *testing.T) {
	s := newTestStore(t)
	a, err := s.GetAccount(context.Background(), "t1", "missing")
	// sqlite contract: nil account + domain error CodeAccountNotFound (not nil,nil).
	if a != nil {
		t.Errorf("GetAccount missing: got account %+v, want nil", a)
	}
	if err == nil {
		t.Error("GetAccount missing: want domain error, got nil")
	}
	if !ledger.IsDomainCode(err, ledger.CodeAccountNotFound) {
		t.Errorf("GetAccount missing: want CodeAccountNotFound, got %v", err)
	}
}

func TestGetFlowMissing(t *testing.T) {
	s := newTestStore(t)
	f, err := s.GetFlow(context.Background(), "t1", "no-such-flow")
	// sqlite contract: nil, nil when not found.
	if err != nil {
		t.Fatalf("GetFlow missing: unexpected error: %v", err)
	}
	if f != nil {
		t.Errorf("GetFlow missing: got %+v, want nil", f)
	}
}

func TestUnsupportedSurface(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.GetFXRateAt(context.Background(), "t1", "USD", "BRL", s.now()); err == nil {
		t.Error("GetFXRateAt: want unsupported error")
	}
	if rows, err := s.ListTenantsDueForSnapshot(context.Background(), s.now(), 10); err != nil || rows != nil {
		t.Errorf("ListTenantsDueForSnapshot = %v/%v, want nil/nil quiet no-op", rows, err)
	}
	if n, err := s.PruneSnapshotsOlderThan(context.Background(), s.now(), 10); err != nil || n != 0 {
		t.Errorf("PruneSnapshotsOlderThan = %d/%v, want 0/nil quiet no-op", n, err)
	}
}
