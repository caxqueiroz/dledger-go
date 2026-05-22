package service_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

func newServer(t *testing.T) (*service.Server, func()) {
	t.Helper()
	srv, _, cleanup := newServerWithStore(t)
	return srv, cleanup
}

func newServerWithStore(t *testing.T) (*service.Server, *sqlite.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	migrations := []string{
		"../../sql/migrations/sqlite/0001_init.sql",
		"../../sql/migrations/sqlite/0002_balance_snapshots.sql",
		"../../sql/migrations/sqlite/0003_reservations.sql",
	}
	for _, path := range migrations {
		mig, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := st.DB().Exec(sqlite.StripGoose(string(mig))); err != nil {
			t.Fatalf("migrate %s: %v", path, err)
		}
	}
	return service.New(st), st, func() { _ = st.Close() }
}

func TestCreateAndGetAccount(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	resp, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "1", AccountType: "cash_available",
		Currency: "USD", NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Msg.GetAccount().GetCurrency() != "USD" {
		t.Fatalf("want USD, got %s", resp.Msg.GetAccount().GetCurrency())
	}

	got, err := srv.GetAccount(context.Background(), connect.NewRequest(&ledgerv1.GetAccountRequest{
		TenantId: "t1", AccountId: resp.Msg.GetAccount().GetId(),
	}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Msg.GetAccount().GetId() != resp.Msg.GetAccount().GetId() {
		t.Fatalf("id mismatch: want %s, got %s", resp.Msg.GetAccount().GetId(), got.Msg.GetAccount().GetId())
	}
}

func TestGetBalance_EmptyAccountReturnsZeroes(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	resp, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "1", AccountType: "cash_available",
		Currency: "USD", NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	bal, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: resp.Msg.GetAccount().GetId(), Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if bal.Msg.GetBalance().GetNormalized() != "0" {
		t.Fatalf("want 0, got %s", bal.Msg.GetBalance().GetNormalized())
	}
}
