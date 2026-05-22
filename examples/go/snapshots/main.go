// Balance snapshot and historical (as_of) query walkthrough.
//
// Demonstrates point-in-time balance reconstruction:
//
//  1. Seed 100 USD into a user account.
//  2. Take a snapshot — captures the current balance row.
//  3. Record asOf := time.Now().
//  4. Post a second deposit of 250 USD (totalling 350).
//  5. GetBalance with no as_of → 350 (current).
//  6. GetBalance with as_of := asOf → 100 (reconstructed from the
//     snapshot taken in step 2; the 250 deposit is excluded because it
//     was posted after the snapshot's snapshot_at).
//
// Run the server first; see ../place_order/main.go for setup.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	postJournal(ctx, client, "snap-1", []*ledgerv1.Entry{
		entry(avail, ledgerv1.Direction_DIRECTION_DEBIT, "100"),
		entry(src, ledgerv1.Direction_DIRECTION_CREDIT, "100"),
	})
	fmt.Println("seeded 100 USD")

	r, err := client.TakeBalanceSnapshot(ctx, connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{
		TenantId: tenant, AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		log.Fatalf("snapshot: %v", err)
	}
	fmt.Printf("took snapshot: %d row(s)\n", r.Msg.GetSnapshotsTaken())

	asOf := time.Now()
	time.Sleep(20 * time.Millisecond) // so the next entry has a strictly later created_at

	postJournal(ctx, client, "snap-2", []*ledgerv1.Entry{
		entry(avail, ledgerv1.Direction_DIRECTION_DEBIT, "250"),
		entry(src, ledgerv1.Direction_DIRECTION_CREDIT, "250"),
	})
	fmt.Println("posted another 250 USD")

	now, err := client.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		log.Fatalf("balance now: %v", err)
	}
	fmt.Printf("current balance:    %s\n", now.Msg.GetBalance().GetNormalized())

	hist, err := client.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: avail, Currency: "USD",
		AsOf: timestamppb.New(asOf),
	}))
	if err != nil {
		log.Fatalf("balance as_of: %v", err)
	}
	fmt.Printf("balance as of %s: %s\n", asOf.Format(time.RFC3339Nano), hist.Msg.GetBalance().GetNormalized())
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
