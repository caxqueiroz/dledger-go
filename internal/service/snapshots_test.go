// internal/service/snapshots_test.go
package service_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func TestTakeBalanceSnapshot_SingleRow(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-seed", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-seed", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "300"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "300"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, err := srv.TakeBalanceSnapshot(context.Background(), connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if resp.Msg.GetSnapshotsTaken() != 1 {
		t.Fatalf("want 1 snapshot, got %d", resp.Msg.GetSnapshotsTaken())
	}
}

func TestGetBalance_AsOfHistoricalPoint(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-1", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-1", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("first deposit: %v", err)
	}

	if _, err := srv.TakeBalanceSnapshot(context.Background(), connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	})); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	asOf := time.Now()

	// Wait so the next entry's created_at is strictly later than asOf.
	time.Sleep(20 * time.Millisecond)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "snap-2", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "snap-2", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "250"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "250"},
		}},
	})); err != nil {
		t.Fatalf("second deposit: %v", err)
	}

	// Current balance: 350.
	now, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if got := now.Msg.GetBalance().GetNormalized(); got != "350" {
		t.Fatalf("current: want 350, got %s", got)
	}

	// As-of asOf: 100.
	historical, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
		AsOf: timestamppb.New(asOf),
	}))
	if err != nil {
		t.Fatalf("get as_of: %v", err)
	}
	if got := historical.Msg.GetBalance().GetNormalized(); got != "100" {
		t.Fatalf("as_of: want 100, got %s", got)
	}
}

func TestTakeBalanceSnapshot_BulkTenantWide(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	// Touch both accounts so balance rows exist.
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "bulk-seed", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "bulk-seed", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "50"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "50"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, err := srv.TakeBalanceSnapshot(context.Background(), connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: "t1",
	}))
	if err != nil {
		t.Fatalf("bulk snapshot: %v", err)
	}
	if got := resp.Msg.GetSnapshotsTaken(); got < 2 {
		t.Fatalf("want >= 2 snapshots, got %d", got)
	}
}
