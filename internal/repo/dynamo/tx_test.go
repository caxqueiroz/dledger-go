package dynamo

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

// mustBegin calls BeginFlowTx and asserts the concrete type is *Tx.
func mustBegin(t *testing.T, s *Store) *Tx {
	t.Helper()
	tx, err := s.BeginFlowTx(context.Background())
	if err != nil {
		t.Fatalf("BeginFlowTx: %v", err)
	}
	dynTx, ok := tx.(*Tx)
	if !ok {
		t.Fatalf("BeginFlowTx: want *Tx, got %T", tx)
	}
	return dynTx
}

// makeAccount is a helper that builds a minimal ledger.Account.
func makeAccount(tenantID, id, ownerType, ownerID, accType, currency string) ledger.Account {
	return ledger.Account{
		ID:            id,
		TenantID:      tenantID,
		OwnerType:     ownerType,
		OwnerID:       ownerID,
		AccountType:   accType,
		Currency:      currency,
		NormalBalance: ledger.NormalDebit,
		AllowNegative: false,
		Status:        ledger.AccountActive,
		CreatedAt:     time.Now().UTC(),
	}
}

// ---------------------------------------------------------------------------
// TestTxBalanceLifecycle
// ---------------------------------------------------------------------------

func TestTxBalanceLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	a1 := makeAccount("t1", "acct-lifecycle-1", "user", "u1", "checking", "USD")

	// Tx1: InsertAccount + first UpdateBalance
	tx1 := mustBegin(t, s)
	if err := tx1.InsertAccount(ctx, a1); err != nil {
		t.Fatalf("tx1 InsertAccount: %v", err)
	}

	d0, c0, ver0, err := tx1.LockBalance(ctx, "t1", a1.ID, "USD")
	if err != nil {
		t.Fatalf("tx1 LockBalance fresh: %v", err)
	}
	if !d0.IsZero() || !c0.IsZero() || ver0 != 0 {
		t.Errorf("fresh balance want 0/0/0, got %s/%s/%d", d0, c0, ver0)
	}

	hundred := decimal.NewFromInt(100)
	if err := tx1.UpdateBalance(ctx, "t1", a1.ID, "USD", hundred, decimal.Zero); err != nil {
		t.Fatalf("tx1 UpdateBalance: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 Commit: %v", err)
	}

	// Verify via store read-only path
	d, c, v, err := s.GetBalance(ctx, "t1", a1.ID, "USD")
	if err != nil {
		t.Fatalf("GetBalance after tx1: %v", err)
	}
	if !d.Equal(hundred) || !c.IsZero() || v != 1 {
		t.Errorf("after tx1 want 100/0/1, got %s/%s/%d", d, c, v)
	}

	// Tx2: LockBalance → sees version 1; UpdateBalance → 150; Commit
	tx2 := mustBegin(t, s)
	d2, c2, ver2, err := tx2.LockBalance(ctx, "t1", a1.ID, "USD")
	if err != nil {
		t.Fatalf("tx2 LockBalance: %v", err)
	}
	if !d2.Equal(hundred) || !c2.IsZero() || ver2 != 1 {
		t.Errorf("tx2 LockBalance want 100/0/1, got %s/%s/%d", d2, c2, ver2)
	}

	onefifty := decimal.NewFromInt(150)
	if err := tx2.UpdateBalance(ctx, "t1", a1.ID, "USD", onefifty, decimal.Zero); err != nil {
		t.Fatalf("tx2 UpdateBalance: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("tx2 Commit: %v", err)
	}

	// Final state: 150/0/version 2
	d3, c3, v3, err := s.GetBalance(ctx, "t1", a1.ID, "USD")
	if err != nil {
		t.Fatalf("GetBalance after tx2: %v", err)
	}
	if !d3.Equal(onefifty) || !c3.IsZero() || v3 != 2 {
		t.Errorf("after tx2 want 150/0/2, got %s/%s/%d", d3, c3, v3)
	}
}

// ---------------------------------------------------------------------------
// TestTxConflictOnStaleVersion
// ---------------------------------------------------------------------------

func TestTxConflictOnStaleVersion(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	a := makeAccount("t1", "acct-conflict-1", "user", "u2", "checking", "USD")

	// Seed: insert account + balance = 10
	txSeed := mustBegin(t, s)
	if err := txSeed.InsertAccount(ctx, a); err != nil {
		t.Fatalf("seed InsertAccount: %v", err)
	}
	ten := decimal.NewFromInt(10)
	if err := txSeed.UpdateBalance(ctx, "t1", a.ID, "USD", ten, decimal.Zero); err != nil {
		t.Fatalf("seed UpdateBalance: %v", err)
	}
	if err := txSeed.Commit(); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}

	// Both txA and txB read the same version=1 balance
	txA := mustBegin(t, s)
	txB := mustBegin(t, s)

	_, _, verA, err := txA.LockBalance(ctx, "t1", a.ID, "USD")
	if err != nil {
		t.Fatalf("txA LockBalance: %v", err)
	}
	_, _, verB, err := txB.LockBalance(ctx, "t1", a.ID, "USD")
	if err != nil {
		t.Fatalf("txB LockBalance: %v", err)
	}
	if verA != 1 || verB != 1 {
		t.Fatalf("both should read version 1; got verA=%d verB=%d", verA, verB)
	}

	twenty := decimal.NewFromInt(20)
	thirty := decimal.NewFromInt(30)

	if err := txA.UpdateBalance(ctx, "t1", a.ID, "USD", twenty, decimal.Zero); err != nil {
		t.Fatalf("txA UpdateBalance: %v", err)
	}
	if err := txB.UpdateBalance(ctx, "t1", a.ID, "USD", thirty, decimal.Zero); err != nil {
		t.Fatalf("txB UpdateBalance: %v", err)
	}

	// txA commits successfully
	if err := txA.Commit(); err != nil {
		t.Fatalf("txA Commit: unexpected error: %v", err)
	}

	// txB MUST fail with CodeSerializationRetryExhausted
	errB := txB.Commit()
	if errB == nil {
		t.Fatal("txB Commit: expected conflict error, got nil")
	}
	if !ledger.IsDomainCode(errB, ledger.CodeSerializationRetryExhausted) {
		t.Errorf("txB Commit: want CodeSerializationRetryExhausted, got %v", errB)
	}

	// Final state: txA's value (20) at version 2 — txB wrote nothing
	d, c, v, err := s.GetBalance(ctx, "t1", a.ID, "USD")
	if err != nil {
		t.Fatalf("GetBalance after conflict: %v", err)
	}
	if !d.Equal(twenty) || !c.IsZero() || v != 2 {
		t.Errorf("post-conflict state want 20/0/2, got %s/%s/%d", d, c, v)
	}
	// Verify account still readable (no partial writes)
	acct, err := s.GetAccount(ctx, "t1", a.ID)
	if err != nil || acct == nil {
		t.Fatalf("GetAccount after conflict: acct=%v err=%v", acct, err)
	}
}

// ---------------------------------------------------------------------------
// TestTxDuplicateAccountUniqueness
// ---------------------------------------------------------------------------

func TestTxDuplicateAccountUniqueness(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// First account: owner tuple = (user, u3, checking, USD)
	a1 := makeAccount("t1", "acct-uniq-1", "user", "u3", "checking", "USD")
	tx1 := mustBegin(t, s)
	if err := tx1.InsertAccount(ctx, a1); err != nil {
		t.Fatalf("tx1 InsertAccount: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 Commit: %v", err)
	}

	// Second account: different ID, SAME owner tuple → uniqueness marker collision
	// The ACCU# item fails condNotExists → non-retryable CodeFlowConflict.
	a2 := makeAccount("t1", "acct-uniq-2", "user", "u3", "checking", "USD")
	tx2 := mustBegin(t, s)
	if err := tx2.InsertAccount(ctx, a2); err != nil {
		t.Fatalf("tx2 InsertAccount (buffer): %v", err)
	}
	errCommit := tx2.Commit()
	if errCommit == nil {
		t.Fatal("tx2 Commit: expected uniqueness conflict, got nil")
	}
	// ACCU# prefix → non-retryable duplicate: CodeFlowConflict (not serialization).
	if !ledger.IsDomainCode(errCommit, ledger.CodeFlowConflict) {
		t.Errorf("tx2 Commit: want CodeFlowConflict (duplicate ACCU#), got %v", errCommit)
	}

	// The second account must NOT exist
	_, err := s.GetAccount(ctx, "t1", a2.ID)
	if !ledger.IsDomainCode(err, ledger.CodeAccountNotFound) {
		t.Errorf("a2 should not exist; GetAccount returned: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestTxDoubleCommitErrors — pure unit, no env required
// ---------------------------------------------------------------------------

func TestTxDoubleCommitErrors(t *testing.T) {
	// Build a Tx directly without a live Store; both empty-buffer and
	// double-commit checks fire before any network access.
	newEmptyTx := func() *Tx {
		return &Tx{
			store:    &Store{},
			ctx:      context.Background(),
			puts:     make(map[string]*pendingPut),
			balances: make(map[string]*txBalance),
		}
	}

	t.Run("double Commit errors", func(t *testing.T) {
		tx := newEmptyTx()
		// First Commit on empty tx must succeed (nil).
		if err := tx.Commit(); err != nil {
			t.Fatalf("first Commit (empty): want nil, got %v", err)
		}
		// Second Commit must error.
		if err := tx.Commit(); err == nil {
			t.Fatal("second Commit: want error, got nil")
		}
	})

	t.Run("Commit after Rollback errors", func(t *testing.T) {
		tx := newEmptyTx()
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: want nil, got %v", err)
		}
		// Commit after Rollback must also error (tx is done).
		if err := tx.Commit(); err == nil {
			t.Fatal("Commit after Rollback: want error, got nil")
		}
	})

	t.Run("Rollback after Commit is silent no-op", func(t *testing.T) {
		tx := newEmptyTx()
		if err := tx.Commit(); err != nil {
			t.Fatalf("first Commit: want nil, got %v", err)
		}
		// defer Rollback pattern: must remain silent no-op.
		if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback after Commit: want nil, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestTxRollbackWritesNothing
// ---------------------------------------------------------------------------

func TestTxRollbackWritesNothing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	a := makeAccount("t1", "acct-rollback-1", "user", "u4", "checking", "USD")
	tx := mustBegin(t, s)
	if err := tx.InsertAccount(ctx, a); err != nil {
		t.Fatalf("InsertAccount: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Store must report account not found
	_, err := s.GetAccount(ctx, "t1", a.ID)
	if err == nil {
		t.Fatal("GetAccount: expected error (not found), got nil")
	}
	if !ledger.IsDomainCode(err, ledger.CodeAccountNotFound) {
		t.Errorf("expected CodeAccountNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestTxCommitTooLarge — pure unit, no env required
// ---------------------------------------------------------------------------

func TestTxCommitTooLarge(t *testing.T) {
	// Build a Tx directly without a live Store; size check must fire before any
	// network access, so the nil db is never reached.
	tx := &Tx{
		store:    &Store{},
		ctx:      context.Background(),
		puts:     make(map[string]*pendingPut),
		balances: make(map[string]*txBalance),
	}

	// Buffer maxTxItems+1 distinct puts
	for i := 0; i <= maxTxItems; i++ {
		pk := "UNIT#" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		// Use a minimal item so MarshalMap never fails
		item := map[string]interface{}{"pk": pk}
		if err := tx.put(pk, item, condNone, 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	err := tx.Commit()
	if err == nil {
		t.Fatal("Commit: expected CodeFlowTooLarge error, got nil")
	}
	if !ledger.IsDomainCode(err, ledger.CodeFlowTooLarge) {
		t.Errorf("Commit: want CodeFlowTooLarge, got %v", err)
	}
}
