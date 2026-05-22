// Reservation lifecycle walkthrough.
//
// Demonstrates the full HELD → PARTIAL → COMMITTED|RELEASED lifecycle,
// including partial commits and partial releases, plus idempotent replay.
//
// Steps:
//
//  1. Seed funds into a user's cash_available account.
//  2. CreateReservation for 300 USD → status HELD, 300 outstanding.
//  3. CommitReservation 100 USD to a destination account → status PARTIAL,
//     outstanding 200, committed 100.
//  4. ReleaseReservation 50 USD → status still PARTIAL, outstanding 150,
//     released 50.
//  5. CommitReservation the remaining 150 USD → status COMMITTED.
//  6. Replay step 2 with the same idempotency key and verify the same
//     reservation_id is returned.
//
// Run the server first; see ../place_order/main.go for setup.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
)

const (
	serverURL = "http://localhost:8080"
	tenant    = "t1"
)

type tenantRT struct{ tenant string }

func (t tenantRT) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Tenant-Id", t.tenant)
	return http.DefaultTransport.RoundTrip(req)
}

func main() {
	client := ledgerv1connect.NewLedgerServiceClient(
		&http.Client{Transport: tenantRT{tenant: tenant}}, serverURL)
	ctx := context.Background()

	src := createAccount(ctx, client, "platform", "0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	avail := createAccount(ctx, client, "user", "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	resv := createAccount(ctx, client, "user", "1", "cash_reserved", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	dest := createAccount(ctx, client, "market", "42", "collateral_pool", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	postJournal(ctx, client, "seed-res", []*ledgerv1.Entry{
		entry(avail, ledgerv1.Direction_DIRECTION_DEBIT, "1000"),
		entry(src, ledgerv1.Direction_DIRECTION_CREDIT, "1000"),
	})

	// 1. Create a reservation for 300 USD.
	r1, err := client.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: tenant, IdempotencyKey: "res-demo-1",
		SourceAccountId: avail, ReservedAccountId: resv,
		Currency: "USD", Amount: "300", SourceService: "example",
	}))
	if err != nil {
		log.Fatalf("create: %v", err)
	}
	id := r1.Msg.GetReservation().GetId()
	dump("after create", r1.Msg.GetReservation())

	// 2. Partial commit of 100 USD.
	r2, err := client.CommitReservation(ctx, connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: tenant, ReservationId: id, DestinationAccountId: dest,
		Amount: "100", IdempotencyKey: "c-100",
	}))
	if err != nil {
		log.Fatalf("commit 100: %v", err)
	}
	dump("after commit 100", r2.Msg.GetReservation())

	// 3. Partial release of 50 USD (back to source).
	r3, err := client.ReleaseReservation(ctx, connect.NewRequest(&ledgerv1.ReleaseReservationRequest{
		TenantId: tenant, ReservationId: id, Amount: "50", IdempotencyKey: "r-50",
	}))
	if err != nil {
		log.Fatalf("release 50: %v", err)
	}
	dump("after release 50", r3.Msg.GetReservation())

	// 4. Final commit of 150 USD.
	r4, err := client.CommitReservation(ctx, connect.NewRequest(&ledgerv1.CommitReservationRequest{
		TenantId: tenant, ReservationId: id, DestinationAccountId: dest,
		Amount: "150", IdempotencyKey: "c-150",
	}))
	if err != nil {
		log.Fatalf("commit 150: %v", err)
	}
	dump("after final commit", r4.Msg.GetReservation())

	// 5. Idempotent replay of the create.
	rr, err := client.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: tenant, IdempotencyKey: "res-demo-1",
		SourceAccountId: "ignored", ReservedAccountId: "ignored",
		Currency: "USD", Amount: "999", SourceService: "example",
	}))
	if err != nil {
		log.Fatalf("replay: %v", err)
	}
	if rr.Msg.GetReservation().GetId() != id {
		log.Fatalf("replay id mismatch")
	}
	fmt.Println("idempotent replay returned the same reservation id")
}

func dump(label string, r *ledgerv1.Reservation) {
	fmt.Printf("%s: status=%s outstanding=%s committed=%s released=%s\n",
		label, r.GetStatus(), r.GetOutstandingAmount(), r.GetCommittedAmount(), r.GetReleasedAmount())
}

func createAccount(ctx context.Context, c ledgerv1connect.LedgerServiceClient, ownerType, ownerID, kind string, nb ledgerv1.NormalBalance, allowNeg bool) string {
	r, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: kind, Currency: "USD",
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		log.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

func entry(acct string, dir ledgerv1.Direction, amount string) *ledgerv1.Entry {
	return &ledgerv1.Entry{AccountId: acct, Currency: "USD", Direction: dir, Amount: amount}
}

func postJournal(ctx context.Context, c ledgerv1connect.LedgerServiceClient, key string, entries []*ledgerv1.Entry) {
	if _, err := c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: key, SourceService: "example",
		Journal: &ledgerv1.Journal{EventId: key, Entries: entries},
	})); err != nil {
		log.Fatalf("post journal %s: %v", key, err)
	}
}
