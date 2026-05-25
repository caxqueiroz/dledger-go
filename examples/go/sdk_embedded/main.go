// SDK embedded mode walkthrough.
//
// Boots an in-process dledger backed by a SQLite file, then drives a
// player wallet through the typical deposit → reserve → commit cycle.
//
// Run:
//
//	go run ./examples/go/sdk_embedded
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "dledger-sdk-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dsn := filepath.Join(dir, "pam.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn,
	})
	if err != nil {
		log.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	// Set up the funding account that mirrors the payment processor.
	if _, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "tipmarket", OwnerType: "platform", OwnerId: "0",
		AccountType:   "stripe_cash",
		Currency:      "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	})); err != nil {
		log.Fatalf("funding account: %v", err)
	}

	// Set up a market collateral pool.
	if _, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "tipmarket", OwnerType: "market", OwnerId: "100",
		AccountType:   "collateral_pool",
		Currency:      "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
	})); err != nil {
		log.Fatalf("pool account: %v", err)
	}

	w := dledger.NewWallet(c, "tipmarket")
	accts, err := w.EnsurePlayerAccounts(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("ensure accounts: %v", err)
	}
	fmt.Printf("player accounts: %+v\n", accts)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "player-42", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:stripe_cash:USD",
		ExternalRef:      "stripe_ch_abc",
		IdempotencyKey:   "dep-1",
		SourceService:    "stripe",
	}); err != nil {
		log.Fatalf("Deposit: %v", err)
	}

	r, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID: "player-42", Currency: "USD", Amount: "40",
		IdempotencyKey: "res-1", SourceService: "matcher",
	})
	if err != nil {
		log.Fatalf("Reserve: %v", err)
	}
	fmt.Printf("reserved: %s outstanding=%s\n", r.ID, r.OutstandingAmount)

	if _, err := w.Commit(ctx, dledger.CommitInput{
		ReservationID:        r.ID,
		DestinationAccountID: "market:100:collateral_pool:USD",
		Amount:               "40",
		IdempotencyKey:       "com-1",
		SourceService:        "matcher",
	}); err != nil {
		log.Fatalf("Commit: %v", err)
	}

	snap, err := w.GetWallet(ctx, "player-42", "USD")
	if err != nil {
		log.Fatalf("GetWallet: %v", err)
	}
	fmt.Printf("wallet: available=%s reserved=%s open=%d\n",
		snap.Available, snap.Reserved, len(snap.OpenReservations))
}
