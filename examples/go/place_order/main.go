// Prediction-market order placement walkthrough.
//
// Demonstrates the canonical PLACE_ORDER flow: a user has cash_available
// funds; placing an order reserves part of that balance by moving funds
// into a cash_reserved account inside a single atomic ExecuteFlow.
//
// Steps in this example:
//
//  1. Create a funding "source" account (credit-normal, allow negative) to
//     bootstrap the user's wallet.
//  2. Create the user's cash_available and cash_reserved accounts.
//  3. Seed 1000 USD into cash_available via PostJournal.
//  4. Execute the PLACE_ORDER flow reserving 100 USD.
//  5. Read balances to verify: cash_available=900, cash_reserved=100.
//
// Run the server first:
//
//	make build
//	./bin/migrate --backend=sqlite --dsn=./ledger.db up
//	./bin/server --backend=sqlite --dsn=./ledger.db
//
// Then in another shell:
//
//	go run ./examples/go/place_order
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
	httpClient := &http.Client{Transport: tenantRT{tenant: tenant}}
	client := ledgerv1connect.NewLedgerServiceClient(httpClient, serverURL)
	ctx := context.Background()

	src := createAccount(ctx, client, "platform", "0", "source", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	avail := createAccount(ctx, client, "user", "1", "cash_available", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	resv := createAccount(ctx, client, "user", "1", "cash_reserved", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)

	fmt.Println("seeded 1000 USD into cash_available")
	postJournal(ctx, client, "seed-place-order", []*ledgerv1.Entry{
		entry(avail, ledgerv1.Direction_DIRECTION_DEBIT, "1000"),
		entry(src, ledgerv1.Direction_DIRECTION_CREDIT, "1000"),
	})

	fmt.Println("placing order: reserve 100 USD")
	if _, err := client.ExecuteFlow(ctx, connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId:       tenant,
		FlowType:       "PLACE_ORDER",
		IdempotencyKey: "ord-abc-v1",
		SourceService:  "example",
		Steps: []*ledgerv1.Step{{
			StepId: "reserve",
			Journal: &ledgerv1.Journal{
				EventId: "ord-abc-reserve",
				Entries: []*ledgerv1.Entry{
					entry(resv, ledgerv1.Direction_DIRECTION_DEBIT, "100"),
					entry(avail, ledgerv1.Direction_DIRECTION_CREDIT, "100"),
				},
			},
		}},
	})); err != nil {
		log.Fatalf("place order: %v", err)
	}

	fmt.Printf("cash_available = %s\n", balance(ctx, client, avail))
	fmt.Printf("cash_reserved  = %s\n", balance(ctx, client, resv))
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

func balance(ctx context.Context, c ledgerv1connect.LedgerServiceClient, acct string) string {
	r, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: acct, Currency: "USD",
	}))
	if err != nil {
		log.Fatalf("balance %s: %v", acct, err)
	}
	return r.Msg.GetBalance().GetNormalized()
}
