package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1/ledgerv1connect"
)

type tenantRT struct{ tenant string }

func (t tenantRT) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Tenant-Id", t.tenant)
	return http.DefaultTransport.RoundTrip(req)
}

func main() {
	httpClient := &http.Client{Transport: tenantRT{tenant: "t1"}}
	client := ledgerv1connect.NewLedgerServiceClient(httpClient, "http://localhost:8080")
	ctx := context.Background()

	avail, err := client.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId:      "t1",
		OwnerType:     "user",
		OwnerId:       "1",
		AccountType:   "cash_available",
		Currency:      "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err != nil {
		log.Fatalf("create avail: %v", err)
	}

	src, err := client.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId:      "t1",
		OwnerType:     "platform",
		OwnerId:       "0",
		AccountType:   "source",
		Currency:      "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
		AllowNegative: true,
	}))
	if err != nil {
		log.Fatalf("create src: %v", err)
	}

	if _, err := client.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId:       "t1",
		IdempotencyKey: "demo-1",
		SourceService:  "demo",
		Journal: &ledgerv1.Journal{
			EventId: "demo-1",
			Entries: []*ledgerv1.Entry{
				{
					AccountId: avail.Msg.GetAccount().GetId(),
					Currency:  "USD",
					Direction: ledgerv1.Direction_DIRECTION_DEBIT,
					Amount:    "1000",
				},
				{
					AccountId: src.Msg.GetAccount().GetId(),
					Currency:  "USD",
					Direction: ledgerv1.Direction_DIRECTION_CREDIT,
					Amount:    "1000",
				},
			},
		},
	})); err != nil {
		log.Fatalf("post: %v", err)
	}

	bal, err := client.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId:  "t1",
		AccountId: avail.Msg.GetAccount().GetId(),
		Currency:  "USD",
	}))
	if err != nil {
		log.Fatalf("balance: %v", err)
	}
	fmt.Println("balance:", bal.Msg.GetBalance().GetNormalized())
}
