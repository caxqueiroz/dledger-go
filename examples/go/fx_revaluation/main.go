// Documented fx_pnl pattern walkthrough: exchange-with-residual + end-of-day
// mark-to-market revaluation. Both shapes are regular ExecuteFlow calls — no
// new RPC needed.
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

	src := createAccount(ctx, client, "platform", "0", "source", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	srcEUR := createAccount(ctx, client, "platform", "0", "source", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	userUSD := createAccount(ctx, client, "user", "1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	userEUR := createAccount(ctx, client, "user", "1", "cash_available", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	platUSD := createAccount(ctx, client, "platform", "fx_desk", "cash", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	platEUR := createAccount(ctx, client, "platform", "fx_desk", "cash", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)
	fxPnlEUR := createAccount(ctx, client, "platform", "fx_desk", "fx_pnl", "EUR", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	postJournal(ctx, client, "rev-seed-usd", []*ledgerv1.Entry{
		entry(userUSD, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "100"),
		entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "100"),
	})
	postJournal(ctx, client, "rev-seed-eur", []*ledgerv1.Entry{
		entry(platEUR, "EUR", ledgerv1.Direction_DIRECTION_DEBIT, "1000"),
		entry(srcEUR, "EUR", ledgerv1.Direction_DIRECTION_CREDIT, "1000"),
	})
	fmt.Println("funded 100 USD and 1000 EUR")

	// (a) Exchange-with-residual: user pays 100 USD, gets 89.50 EUR; platform
	// books at 89.00; 0.50 EUR difference hits fx_pnl.
	if _, err := client.ExecuteFlow(ctx, connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: tenant, FlowType: "EXCHANGE_RESIDUAL", IdempotencyKey: "rev-residual",
		SourceService: "demo",
		Steps: []*ledgerv1.Step{{StepId: "exchange", Journal: &ledgerv1.Journal{
			EventId: "rev-residual-evt", Entries: []*ledgerv1.Entry{
				entry(platUSD, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "100"),
				entry(userUSD, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "100"),
				entry(userEUR, "EUR", ledgerv1.Direction_DIRECTION_DEBIT, "89.50"),
				entry(platEUR, "EUR", ledgerv1.Direction_DIRECTION_CREDIT, "89.00"),
				entry(fxPnlEUR, "EUR", ledgerv1.Direction_DIRECTION_CREDIT, "0.50"),
			},
		}}},
	})); err != nil {
		log.Fatalf("residual exchange: %v", err)
	}
	fmt.Println("exchange-with-residual posted; 0.50 EUR booked to fx_pnl")

	// (b) End-of-day revaluation: 12.34 EUR loss on the platform's open EUR position.
	if _, err := client.ExecuteFlow(ctx, connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: tenant, FlowType: "FX_REVALUATION", IdempotencyKey: "rev-mtm",
		SourceService: "demo",
		Steps: []*ledgerv1.Step{{StepId: "mtm", Journal: &ledgerv1.Journal{
			EventId: "rev-mtm-evt", Entries: []*ledgerv1.Entry{
				entry(fxPnlEUR, "EUR", ledgerv1.Direction_DIRECTION_DEBIT, "12.34"),
				entry(platEUR, "EUR", ledgerv1.Direction_DIRECTION_CREDIT, "12.34"),
			},
		}}},
	})); err != nil {
		log.Fatalf("mtm: %v", err)
	}
	fmt.Println("mark-to-market posted; 12.34 EUR loss recorded against fx_pnl")

	fmt.Printf("fx_pnl EUR balance = %s (credit-normal: positive = platform gain)\n",
		balance(ctx, client, fxPnlEUR, "EUR"))
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
