package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func openTempDB(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")

	mig, err := os.ReadFile("../../../sql/migrations/sqlite/0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	s, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.db.Exec(StripGoose(string(mig))); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}


func TestInsertAndReadAccount(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	tx, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	acc := ledger.Account{
		ID:            "user:1:cash:USD",
		TenantID:      "t1",
		OwnerType:     "user",
		OwnerID:       "1",
		AccountType:   "cash",
		Currency:      "USD",
		NormalBalance: ledger.NormalDebit,
		Status:        ledger.AccountActive,
	}
	if err := tx.InsertAccount(ctx, acc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.GetAccount(ctx, "t1", "user:1:cash:USD")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Currency != "USD" {
		t.Fatalf("want USD, got %s", got.Currency)
	}
}

func TestGetAccountNotFound(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	_, err := s.GetAccount(ctx, "t1", "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing account")
	}
	if !ledger.IsDomainCode(err, ledger.CodeAccountNotFound) {
		t.Fatalf("expected CodeAccountNotFound, got: %v", err)
	}
}

func TestLockAndUpdateBalance(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	// Insert account first
	tx, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	acc := ledger.Account{
		ID:            "user:2:cash:EUR",
		TenantID:      "t1",
		OwnerType:     "user",
		OwnerID:       "2",
		AccountType:   "cash",
		Currency:      "EUR",
		NormalBalance: ledger.NormalDebit,
		Status:        ledger.AccountActive,
	}
	if err := tx.InsertAccount(ctx, acc); err != nil {
		t.Fatalf("insert account: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Now lock balance and update it
	tx2, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	d, c, ver, err := tx2.LockBalance(ctx, "t1", "user:2:cash:EUR", "EUR")
	if err != nil {
		t.Fatalf("lock balance: %v", err)
	}
	if !d.IsZero() || !c.IsZero() || ver != 0 {
		t.Fatalf("expected zero balance, got d=%s c=%s ver=%d", d, c, ver)
	}

	hundred, _ := decimal.NewFromString("100.00")
	newD := d.Add(hundred)
	if err := tx2.UpdateBalance(ctx, "t1", "user:2:cash:EUR", "EUR", newD, c); err != nil {
		t.Fatalf("update balance: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	// Verify updated balance
	gotD, gotC, gotVer, err := s.GetBalance(ctx, "t1", "user:2:cash:EUR", "EUR")
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	hundredCheck, _ := decimal.NewFromString("100.00")
	if !gotD.Equal(hundredCheck) {
		t.Fatalf("want posted_debits=100.00, got %s", gotD)
	}
	if !gotC.IsZero() {
		t.Fatalf("want posted_credits=0, got %s", gotC)
	}
	if gotVer != 1 {
		t.Fatalf("want version=1, got %d", gotVer)
	}
}

func TestGetBalanceNotFound(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	d, c, ver, err := s.GetBalance(ctx, "t1", "nonexistent", "USD")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !d.IsZero() || !c.IsZero() || ver != 0 {
		t.Fatalf("expected zero values for missing balance")
	}
}

func TestFlowRoundTrip(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	tx, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	flow := ledger.FlowRun{
		ID:             "flow-1",
		TenantID:       "t1",
		FlowType:       "transfer",
		IdempotencyKey: "idem-1",
		SourceService:  "test",
		ActorID:        "actor-1",
		Metadata:       map[string]any{"key": "value"},
	}
	if err := tx.InsertFlowRun(ctx, flow); err != nil {
		t.Fatalf("insert flow: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := s.GetFlow(ctx, "t1", "flow-1")
	if err != nil {
		t.Fatalf("get flow: %v", err)
	}
	if got == nil {
		t.Fatal("expected flow, got nil")
	}
	if got.FlowType != "transfer" {
		t.Fatalf("want flow_type=transfer, got %s", got.FlowType)
	}
	if got.Status != ledger.FlowRunning {
		t.Fatalf("want status=RUNNING, got %s", got.Status)
	}
}

func TestIdempotencyLookup(t *testing.T) {
	s := openTempDB(t)
	ctx := context.Background()

	tx, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	flow := ledger.FlowRun{
		ID:             "flow-2",
		TenantID:       "t1",
		FlowType:       "transfer",
		IdempotencyKey: "idem-unique",
		SourceService:  "test",
		ActorID:        "actor-1",
	}
	if err := tx.InsertFlowRun(ctx, flow); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := s.BeginFlowTx(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback() }()

	got, err := tx2.GetFlowByIdempotency(ctx, "t1", "idem-unique")
	if err != nil {
		t.Fatalf("get by idempotency: %v", err)
	}
	if got == nil || got.ID != "flow-2" {
		t.Fatalf("expected flow-2, got %v", got)
	}

	// missing key should return nil
	missing, err := tx2.GetFlowByIdempotency(ctx, "t1", "idem-missing")
	if err != nil {
		t.Fatalf("unexpected error for missing key: %v", err)
	}
	if missing != nil {
		t.Fatal("expected nil for missing idempotency key")
	}
}

