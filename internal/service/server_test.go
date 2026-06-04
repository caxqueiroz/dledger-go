package service_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
	"github.com/caxqueiroz/dledger-go/internal/repo/dynamo"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

// newServer returns a ready server backed by the backend selected by
// DLEDGER_TEST_BACKEND (default: sqlite).
func newServer(t *testing.T) (*service.Server, func()) {
	t.Helper()
	srv, _, cleanup := newServerWithStore(t)
	return srv, cleanup
}

// newServerWithStore returns a ready server and its underlying repo.Store.
//
// Backend selection via DLEDGER_TEST_BACKEND:
//   - unset / "sqlite": temporary SQLite file + StripGoose migrations (existing behaviour).
//   - "dynamo": skips unless AWS_ENDPOINT_URL_DYNAMODB is set; provisions a
//     uniquely-named table ("dltest_<8 rand chars>_ledger"), registers a
//     t.Cleanup to delete it, and returns the dynamo Store as repo.Store.
func newServerWithStore(t *testing.T) (*service.Server, repo.Store, func()) {
	t.Helper()

	switch os.Getenv("DLEDGER_TEST_BACKEND") {
	case "dynamo":
		return newServerWithDynamo(t)
	default:
		return newServerWithSQLite(t)
	}
}

// newServerWithSQLite is the original implementation: temp SQLite + migrations.
func newServerWithSQLite(t *testing.T) (*service.Server, repo.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	migrations := []string{
		"../sdk/migrations/sqlite/0001_init.sql",
		"../sdk/migrations/sqlite/0002_balance_snapshots.sql",
		"../sdk/migrations/sqlite/0003_reservations.sql",
		"../sdk/migrations/sqlite/0004_fx_rates.sql",
		"../sdk/migrations/sqlite/0005_reconciliation.sql",
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

// newServerWithDynamo provisions a unique test table on the local DynamoDB
// endpoint (ExtendDB in CI). Skips unless AWS_ENDPOINT_URL_DYNAMODB is set.
func newServerWithDynamo(t *testing.T) (*service.Server, repo.Store, func()) {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") == "" {
		t.Skip("dynamo backend requested but AWS_ENDPOINT_URL_DYNAMODB is not set")
	}
	ctx := context.Background()
	table := fmt.Sprintf("dltest_%s_ledger", randHex8())
	st, err := dynamo.Open(ctx, table)
	if err != nil {
		t.Fatalf("dynamo.Open: %v", err)
	}
	if err := st.EnsureTable(ctx); err != nil {
		t.Fatalf("EnsureTable %s: %v", table, err)
	}
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = st.DeleteTable(cleanupCtx)
		_ = st.Close()
	}
	t.Cleanup(cleanup)
	return service.New(st), st, cleanup
}

// randHex8 returns 8 random lowercase hex characters.
func randHex8() string {
	const chars = "abcdef0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// skipUnsupportedOnDynamo skips the test when the dynamo backend is active and
// the feature under test is not supported by the DynamoDB store in this
// milestone (FX rates, reconciliation, snapshots, ListAccountActivity).
func skipUnsupportedOnDynamo(t *testing.T) {
	t.Helper()
	if os.Getenv("DLEDGER_TEST_BACKEND") == "dynamo" {
		t.Skip("unsupported on dynamodb backend (FX/recon/snapshots/activity)")
	}
}

// waitForGSIPropagation sleeps briefly to allow DynamoDB GSI writes to become
// visible. GSIs are eventually consistent; in ExtendDB (local test server) the
// propagation is fast but not instantaneous. This helper is gated on the dynamo
// backend so it has zero cost on SQLite.
//
// Only call this in tests that query a GSI immediately after a write. Do NOT
// use it to paper over assertion failures — only call it before reads whose
// latency is documented as eventually consistent in
// internal/repo/dynamo/README.md (GSI: ListReservations, ListExpiredReservations).
func waitForGSIPropagation(t *testing.T) {
	t.Helper()
	if os.Getenv("DLEDGER_TEST_BACKEND") != "dynamo" {
		return
	}
	// 300 ms matches the guard used in internal/repo/dynamo/reservations_test.go
	// (waitForGSI: 250 ms). Use a slightly larger value at the service layer to
	// account for the extra round-trips involved.
	time.Sleep(300 * time.Millisecond)
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
