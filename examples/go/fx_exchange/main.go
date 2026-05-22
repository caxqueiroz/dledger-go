// FX exchange walkthrough. Creates a USD/EUR rate, exchanges $100 for €89.50
// at rate 0.895 between user accounts and platform fx_desk accounts.
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

	src := createAccount(ctx, client, "platform", "0", "source", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	userUSD := createAccount(ctx, client, "user", "1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	userEUR := createAccount(ctx, client, "user", "1", "cash_available", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	platUSD := createAccount(ctx, client, "platform", "fx_desk", "cash", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	platEUR := createAccount(ctx, client, "platform", "fx_desk", "cash", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	postJournal(ctx, client, "fx-seed", []*ledgerv1.Entry{
		entry(userUSD, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "100"),
		entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "100"),
	})
	fmt.Println("funded 100 USD")

	if _, err := client.PutFXRate(ctx, connect.NewRequest(&ledgerv1.PutFXRateRequest{
		TenantId: tenant, BaseCurrency: "USD", QuoteCurrency: "EUR",
		Rate: "0.895", Source: "manual", EffectiveAt: timestamppb.New(time.Now()),
	})); err != nil {
		log.Fatalf("put rate: %v", err)
	}
	fmt.Println("recorded USD/EUR rate 0.895")

	resp, err := client.ExecuteExchange(ctx, connect.NewRequest(&ledgerv1.ExecuteExchangeRequest{
		TenantId: tenant, IdempotencyKey: "ex-demo",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "89.500",
		SourceService: "demo",
	}))
	if err != nil {
		log.Fatalf("exchange: %v", err)
	}
	fmt.Printf("exchange done: flow_run_id=%s rate=%s source=%s\n",
		resp.Msg.GetFlowRunId(), resp.Msg.GetRateUsed(), resp.Msg.GetRateSource())

	for _, a := range []struct {
		name, acct, ccy string
	}{
		{"user USD", userUSD, "USD"}, {"user EUR", userEUR, "EUR"},
		{"platform USD", platUSD, "USD"}, {"platform EUR", platEUR, "EUR"},
	} {
		fmt.Printf("%s = %s\n", a.name, balance(ctx, client, a.acct, a.ccy))
	}
}

func createAccount(ctx context.Context, c ledgerv1connect.LedgerServiceClient, ownerType, ownerID, kind, ccy string, nb ledgerv1.NormalBalance, allowNeg bool) string {
	r, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		log.Fatalf("create %s/%s/%s: %v", ownerType, kind, ccy, err)
	}
	return r.Msg.GetAccount().GetId()
}

func entry(acct, ccy string, dir ledgerv1.Direction, amount string) *ledgerv1.Entry {
	return &ledgerv1.Entry{AccountId: acct, Currency: ccy, Direction: dir, Amount: amount}
}

func postJournal(ctx context.Context, c ledgerv1connect.LedgerServiceClient, key string, entries []*ledgerv1.Entry) {
	if _, err := c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: key, SourceService: "demo",
		Journal: &ledgerv1.Journal{EventId: key, Entries: entries},
	})); err != nil {
		log.Fatalf("post %s: %v", key, err)
	}
}

func balance(ctx context.Context, c ledgerv1connect.LedgerServiceClient, acct, ccy string) string {
	r, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: acct, Currency: ccy,
	}))
	if err != nil {
		log.Fatalf("balance %s: %v", acct, err)
	}
	return r.Msg.GetBalance().GetNormalized()
}
