// internal/service/reservations_test.go
package service_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

// seedAndReserve creates the three accounts (source, reserved, destination),
// funds the source account with 1000 USD, and creates a reservation for the
// given amount. Returns the reservation id and all three account ids.
func seedAndReserve(t *testing.T, srv *service.Server, amount string) (resvID, sourceID, reservedID, destID string) {
	t.Helper()
	src := seedSource(t, srv)
	sourceID = mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	reservedID = mustCreateAccount(t, srv, "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	destID = mustCreateAccount(t, srv, "2", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "res-seed-" + t.Name(), SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "res-seed-" + t.Name(), Entries: []*ledgerv1.Entry{
			{AccountId: sourceID, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r, err := srv.CreateReservation(context.Background(), connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-" + t.Name(),
		SourceAccountId: sourceID, ReservedAccountId: reservedID,
		Currency: "USD", Amount: amount, SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}
	return r.Msg.GetReservation().GetId(), sourceID, reservedID, destID
}

func balanceOf(t *testing.T, srv *service.Server, acct string) string {
	t.Helper()
	r, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: acct, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("get balance %s: %v", acct, err)
	}
	return r.Msg.GetBalance().GetNormalized()
}

func TestCreateReservation_HoldsFunds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	_, sourceID, reservedID, _ := seedAndReserve(t, srv, "300")
	if got := balanceOf(t, srv, sourceID); got != "700" {
		t.Fatalf("source: want 700, got %s", got)
	}
	if got := balanceOf(t, srv, reservedID); got != "300" {
		t.Fatalf("reserved: want 300, got %s", got)
	}
}

func TestReservation_PartialCommitThenFinalCommit(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "300")

	r1, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "100", IdempotencyKey: "c1",
	}))
	if err != nil {
		t.Fatalf("commit 1: %v", err)
	}
	if r1.Msg.GetReservation().GetStatus() != "PARTIAL" {
		t.Fatalf("after partial commit: want PARTIAL, got %s", r1.Msg.GetReservation().GetStatus())
	}
	if r1.Msg.GetReservation().GetOutstandingAmount() != "200" {
		t.Fatalf("outstanding: want 200, got %s", r1.Msg.GetReservation().GetOutstandingAmount())
	}

	r2, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "200", IdempotencyKey: "c2",
	}))
	if err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if r2.Msg.GetReservation().GetStatus() != "COMMITTED" {
		t.Fatalf("final commit: want COMMITTED, got %s", r2.Msg.GetReservation().GetStatus())
	}
	if r2.Msg.GetReservation().GetOutstandingAmount() != "0" {
		t.Fatalf("outstanding: want 0, got %s", r2.Msg.GetReservation().GetOutstandingAmount())
	}
}

func TestReservation_PartialReleaseThenCommit(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "300")

	if _, err := srv.ReleaseReservation(context.Background(), connect.NewRequest(&ledgerv1.ReleaseReservationRequest{
		TenantId: "t1", ReservationId: resvID, Amount: "100", IdempotencyKey: "r1",
	})); err != nil {
		t.Fatalf("release: %v", err)
	}

	r, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "200", IdempotencyKey: "c1",
	}))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if r.Msg.GetReservation().GetStatus() != "COMMITTED" {
		t.Fatalf("status: want COMMITTED, got %s", r.Msg.GetReservation().GetStatus())
	}
}

func TestReservation_CommitClosed(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "100")

	if _, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "100", IdempotencyKey: "c1",
	})); err != nil {
		t.Fatalf("commit: %v", err)
	}

	_, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "1", IdempotencyKey: "c2",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition CodeReservationClosed, got %v", err)
	}
}

func TestReservation_AmountExceeds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, destID := seedAndReserve(t, srv, "100")

	_, err := srv.CommitReservation(context.Background(), connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: "t1", ReservationId: resvID, DestinationAccountId: destID,
		Amount: "101", IdempotencyKey: "x1",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument CodeReservationAmountExceeds, got %v", err)
	}
}

func TestReservation_IdempotentCreate(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, _ := seedAndReserve(t, srv, "100")

	// Replay using same idempotency key — must return the same reservation.
	r, err := srv.CreateReservation(context.Background(), connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: "t1", IdempotencyKey: "res-" + t.Name(),
		SourceAccountId: "ignored", ReservedAccountId: "ignored",
		Currency: "USD", Amount: "100",
	}))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if r.Msg.GetReservation().GetId() != resvID {
		t.Fatalf("replay returned different id: %s vs %s", r.Msg.GetReservation().GetId(), resvID)
	}
}

func TestReservation_GetReservation(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	resvID, _, _, _ := seedAndReserve(t, srv, "100")

	r, err := srv.GetReservation(context.Background(), connect.NewRequest(&ledgerv1.GetReservationRequest{
		TenantId: "t1", ReservationId: resvID,
	}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if r.Msg.GetReservation().GetStatus() != "HELD" {
		t.Fatalf("want HELD, got %s", r.Msg.GetReservation().GetStatus())
	}
}
