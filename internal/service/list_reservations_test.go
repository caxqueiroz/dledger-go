// internal/service/list_reservations_test.go
package service_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// TestListReservations_FiltersByOwner sets up reservations for two different
// users in the same tenant and asserts the filter returns only the matching
// user's reservation.
func TestListReservations_FiltersByOwner(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	ctx := context.Background()

	src := seedSource(t, srv)

	// User 1 accounts
	u1Avail := mustCreateAccount(t, srv, "u1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	u1Resv := mustCreateAccount(t, srv, "u1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	// User 2 accounts
	u2Avail := mustCreateAccount(t, srv, "u2", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	u2Resv := mustCreateAccount(t, srv, "u2", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	// Seed both users with 100 USD.
	for _, in := range []struct {
		avail string
		key   string
	}{
		{u1Avail, "seed-u1"},
		{u2Avail, "seed-u2"},
	} {
		if _, err := srv.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
			TenantId: "t1", IdempotencyKey: in.key, SourceService: "test",
			Journal: &ledgerv1.Journal{EventId: in.key, Entries: []*ledgerv1.Entry{
				{AccountId: in.avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			}},
		})); err != nil {
			t.Fatalf("seed %s: %v", in.key, err)
		}
	}

	// Reserve under each user.
	if _, err := srv.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-u1",
		SourceAccountId: u1Avail, ReservedAccountId: u1Resv,
		Currency: "USD", Amount: "10", SourceService: "test",
	})); err != nil {
		t.Fatalf("reserve u1: %v", err)
	}
	if _, err := srv.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-u2",
		SourceAccountId: u2Avail, ReservedAccountId: u2Resv,
		Currency: "USD", Amount: "20", SourceService: "test",
	})); err != nil {
		t.Fatalf("reserve u2: %v", err)
	}

	// waitForGSIPropagation is a no-op on sqlite; on the dynamo backend the GSI2
	// index (used by ListReservations) is eventually consistent — give it a moment
	// to catch up before asserting. See internal/repo/dynamo/README.md.
	waitForGSIPropagation(t)

	resp, err := srv.ListReservations(ctx, connect.NewRequest(&ledgerv1.ListReservationsRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "u1",
	}))
	if err != nil {
		t.Fatalf("ListReservations: %v", err)
	}
	if got := len(resp.Msg.GetReservations()); got != 1 {
		t.Fatalf("want 1 reservation for u1, got %d", got)
	}
	if got, want := resp.Msg.GetReservations()[0].GetOriginalAmount(), "10"; got != want {
		t.Fatalf("want original=%s got %s", want, got)
	}

	resp2, err := srv.ListReservations(ctx, connect.NewRequest(&ledgerv1.ListReservationsRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: "u2",
	}))
	if err != nil {
		t.Fatalf("ListReservations u2: %v", err)
	}
	if got := len(resp2.Msg.GetReservations()); got != 1 {
		t.Fatalf("u2 want 1 reservation, got %d", got)
	}
	if got, want := resp2.Msg.GetReservations()[0].GetOriginalAmount(), "20"; got != want {
		t.Fatalf("u2 want original=%s got %s", want, got)
	}
}
