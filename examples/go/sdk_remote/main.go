// SDK remote mode walkthrough.
//
// Identical Wallet code path to sdk_embedded; only the constructor differs.
// Run a server first (see ../place_order/main.go for instructions):
//
//	go run ./cmd/server --backend=sqlite --dsn=./ledger.db
//	go run ./examples/go/sdk_remote
package main

import (
	"context"
	"fmt"
	"log"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

const (
	serverURL = "http://localhost:8080"
	tenantID  = "tipmarket"
)

func main() {
	ctx := context.Background()
	c := dledger.NewRemote(serverURL, tenantID)
	defer c.Close()

	// Funding + pool accounts.
	for _, in := range []*ledgerv1.CreateAccountRequest{
		{TenantId: tenantID, OwnerType: "platform", OwnerId: "0",
			AccountType:   "stripe_cash",
			Currency:      "USD",
			NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT},
		{TenantId: tenantID, OwnerType: "market", OwnerId: "100",
			AccountType:   "collateral_pool",
			Currency:      "USD",
			NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT},
	} {
		if _, err := c.CreateAccount(ctx, connect.NewRequest(in)); err != nil {
			log.Printf("create %s:%s (ignoring if already exists): %v",
				in.GetOwnerType(), in.GetAccountType(), err)
		}
	}

	w := dledger.NewWallet(c, tenantID)
	if _, err := w.EnsurePlayerAccounts(ctx, "player-42", "USD"); err != nil {
		log.Fatalf("ensure: %v", err)
	}
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "player-42", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:stripe_cash:USD",
		ExternalRef:      "stripe_ch_remote",
		IdempotencyKey:   "dep-remote-1",
		SourceService:    "stripe",
	}); err != nil {
		log.Fatalf("Deposit: %v", err)
	}
	snap, err := w.GetWallet(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("GetWallet: %v", err)
	}
	fmt.Printf("wallet via remote: available=%s reserved=%s\n", snap.Available, snap.Reserved)
}
