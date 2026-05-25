// internal/scheduler/scheduler_test.go
package scheduler_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/scheduler"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

func setup(t *testing.T) (*service.Server, *sqlite.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	st, err := sqlite.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, name := range []string{
		"0001_init.sql",
		"0002_balance_snapshots.sql",
		"0003_reservations.sql",
		"0004_fx_rates.sql",
		"0005_reconciliation.sql",
	} {
		mig, err := os.ReadFile(filepath.Join("../sdk/migrations/sqlite", name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := st.DB().Exec(sqlite.StripGoose(string(mig))); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
	srv := service.New(st)
	return srv, st, func() { _ = st.Close() }
}

func mkAccount(t *testing.T, srv *service.Server, ownerID, kind string, nb ledgerv1.NormalBalance, allowNeg bool) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: ownerID, AccountType: kind,
		Currency: "USD", NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		t.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

func TestScheduler_ExpiresOverdueReservation(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	source := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	reserved := mkAccount(t, srv, "1", "cash_reserved", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := mkAccount(t, srv, "0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	if _, err := srv.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-exp", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-exp", Entries: []*ledgerv1.Entry{
			{AccountId: source, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "200"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "200"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Reservation with an already-elapsed deadline.
	past := time.Now().Add(-1 * time.Second)
	r, err := srv.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "exp-res",
		SourceAccountId: source, ReservedAccountId: reserved,
		Currency: "USD", Amount: "150",
		ExpiresAt: timestamppb.New(past),
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	resID := r.Msg.GetReservation().GetId()

	// Spin up the scheduler with a tight tick so it fires quickly.
	sched := scheduler.New(store, srv)
	sched.Cfg.ExpiryTick = 50 * time.Millisecond
	sched.Cfg.SnapshotTick = time.Hour // effectively disable in this test

	runCtx, cancel := context.WithCancel(ctx)
	go sched.Run(runCtx)
	defer cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, gErr := srv.GetReservation(ctx, connect.NewRequest(&ledgerv1.GetReservationRequest{
			TenantId: "t1", ReservationId: resID,
		}))
		if gErr == nil && got.Msg.GetReservation().GetStatus() == "EXPIRED" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("reservation did not transition to EXPIRED in time")
}

func TestScheduler_DoesNotExpireFutureReservation(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	source := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	reserved := mkAccount(t, srv, "1", "cash_reserved", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := mkAccount(t, srv, "0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	if _, err := srv.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-fut", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-fut", Entries: []*ledgerv1.Entry{
			{AccountId: source, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "200"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "200"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	future := time.Now().Add(1 * time.Hour)
	r, err := srv.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "fut-res",
		SourceAccountId: source, ReservedAccountId: reserved,
		Currency: "USD", Amount: "150",
		ExpiresAt: timestamppb.New(future),
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	resID := r.Msg.GetReservation().GetId()

	sched := scheduler.New(store, srv)
	sched.Cfg.ExpiryTick = 30 * time.Millisecond
	sched.Cfg.SnapshotTick = time.Hour
	runCtx, cancel := context.WithCancel(ctx)
	go sched.Run(runCtx)
	defer cancel()

	// Wait a few ticks then verify still HELD.
	time.Sleep(200 * time.Millisecond)
	got, err := srv.GetReservation(ctx, connect.NewRequest(&ledgerv1.GetReservationRequest{
		TenantId: "t1", ReservationId: resID,
	}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Msg.GetReservation().GetStatus() != "HELD" {
		t.Fatalf("want HELD (not expired), got %s", got.Msg.GetReservation().GetStatus())
	}
}

func TestScheduler_SnapshotsDueTenants(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	// Touch a balance row so the bulk snapshot has something to capture.
	avail := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := mkAccount(t, srv, "0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	if _, err := srv.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-tick-seed", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-tick-seed", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "50"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "50"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Configure a near-zero snapshot interval so the tick fires immediately.
	sched := scheduler.New(store, srv)
	sched.Cfg.SnapshotTick = 50 * time.Millisecond
	sched.Cfg.SnapshotInterval = 1 * time.Millisecond
	sched.Cfg.ExpiryTick = time.Hour // disable in this test

	runCtx, cancel := context.WithCancel(ctx)
	go sched.Run(runCtx)
	defer cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := store.GetSnapshotBefore(ctx, "t1", avail, "USD", time.Now())
		if err != nil {
			t.Fatalf("get snapshot: %v", err)
		}
		if snap != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("snapshot tick never captured a snapshot for tenant t1")
}

func TestRetention_DeletesOldNonLatestSnapshot(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	old := ledger.BalanceSnapshot{
		ID: "snap-old", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 0,
		SnapshotAt: time.Now().Add(-48 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, old); err != nil {
		t.Fatalf("insert old: %v", err)
	}
	newer := ledger.BalanceSnapshot{
		ID: "snap-new", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 1,
		SnapshotAt: time.Now().Add(-1 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, newer); err != nil {
		t.Fatalf("insert new: %v", err)
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionAge = 24 * time.Hour
	sched.Cfg.BatchN = 100
	sched.RetentionTickForTest(ctx)

	got, err := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now().Add(-25*time.Hour))
	if err != nil {
		t.Fatalf("get old: %v", err)
	}
	if got != nil {
		t.Fatalf("old snapshot was not deleted: %+v", got)
	}
	newest, err := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now())
	if err != nil {
		t.Fatalf("get newest: %v", err)
	}
	if newest == nil || newest.ID != "snap-new" {
		t.Fatalf("newest snapshot missing; want snap-new")
	}
}

func TestRetention_PreservesMostRecentEvenWhenOlder(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	only := ledger.BalanceSnapshot{
		ID: "snap-only", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 0,
		SnapshotAt: time.Now().Add(-365 * 24 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, only); err != nil {
		t.Fatalf("insert only: %v", err)
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionAge = 30 * 24 * time.Hour
	sched.Cfg.BatchN = 100
	sched.RetentionTickForTest(ctx)

	got, err := store.GetSnapshotBefore(ctx, "t1", acct, "USD", time.Now())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil || got.ID != "snap-only" {
		t.Fatalf("only snapshot was deleted despite being the most-recent for its key")
	}
}

func TestRetention_RespectsBatchLimit(t *testing.T) {
	srv, store, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()

	acct := mkAccount(t, srv, "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	// 1 fresh + 5 old. The fresh is the most-recent and is always preserved;
	// the 5 olds are all eligible because a newer one (the fresh one) exists.
	fresh := ledger.BalanceSnapshot{
		ID: "snap-fresh", TenantID: "t1", AccountID: acct, Currency: "USD",
		PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: 999,
		SnapshotAt: time.Now().Add(-1 * time.Hour),
	}
	if err := store.InsertSnapshot(ctx, fresh); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}
	for i := range 5 {
		old := ledger.BalanceSnapshot{
			ID:           fmt.Sprintf("snap-old-%d", i),
			TenantID:     "t1",
			AccountID:    acct,
			Currency:     "USD",
			PostedDebits: decimal.Zero, PostedCredits: decimal.Zero, Version: int64(i),
			SnapshotAt: time.Now().Add(time.Duration(-(48 + i)) * time.Hour),
		}
		if err := store.InsertSnapshot(ctx, old); err != nil {
			t.Fatalf("insert old %d: %v", i, err)
		}
	}

	sched := scheduler.New(store, srv)
	sched.Cfg.RetentionAge = 24 * time.Hour
	sched.Cfg.BatchN = 2

	sched.RetentionTickForTest(ctx)
	if remaining := countSnapshots(t, store, acct); remaining != 4 {
		t.Fatalf("after one tick: want 4 snapshots remaining, got %d", remaining)
	}

	sched.RetentionTickForTest(ctx)
	if remaining := countSnapshots(t, store, acct); remaining != 2 {
		t.Fatalf("after two ticks: want 2 remaining, got %d", remaining)
	}
}

func countSnapshots(t *testing.T, store *sqlite.Store, accountID string) int {
	t.Helper()
	row := store.DB().QueryRow(
		"SELECT COUNT(*) FROM balance_snapshots WHERE tenant_id = ? AND account_id = ?",
		"t1", accountID,
	)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
