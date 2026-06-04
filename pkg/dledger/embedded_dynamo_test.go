// pkg/dledger/embedded_dynamo_test.go
package dledger

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	dynamostore "github.com/caxqueiroz/dledger-go/internal/repo/dynamo"
)

// TestEmbeddedDynamo proves a PostJournal + GetBalance + idempotent replay
// end-to-end cycle against a real DynamoDB-compatible endpoint (ExtendDB).
// The test is skipped automatically when AWS_ENDPOINT_URL_DYNAMODB is unset.
func TestEmbeddedDynamo(t *testing.T) {
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") == "" {
		t.Skip("AWS_ENDPOINT_URL_DYNAMODB not set; skipping DynamoDB integration test")
	}

	ctx := context.Background()

	// Unique table per test run to avoid cross-run collisions.
	table := fmt.Sprintf("dltest_%08x_ledger", rand.Uint32()) //nolint:gosec

	c, err := NewEmbedded(ctx, Options{
		Backend:          DynamoDB,
		DSN:              table,
		MigrateMode:      MigrateAuto,
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Cleanup: delete the table after the test regardless of outcome.
	t.Cleanup(func() {
		s, err := dynamostore.Open(context.Background(), table)
		if err != nil {
			t.Logf("cleanup Open: %v", err)
			return
		}
		if err := s.DeleteTable(context.Background()); err != nil {
			t.Logf("cleanup DeleteTable: %v", err)
		}
		_ = s.Close()
	})

	tenant := "t1"

	// -------------------------------------------------------------------
	// Step 1: create two accounts.
	// funding: credit-normal, AllowNegative=true (platform source of funds)
	// player:  debit-normal,  AllowNegative=false (player cash wallet)
	// -------------------------------------------------------------------
	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: "platform", OwnerId: "0",
		AccountType: "funding", Currency: "BRL",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
		AllowNegative: true,
	}))
	if err != nil {
		t.Fatalf("CreateAccount (funding): %v", err)
	}

	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: "user", OwnerId: "p1",
		AccountType: "cash_available", Currency: "BRL",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT,
		AllowNegative: false,
	}))
	if err != nil {
		t.Fatalf("CreateAccount (player): %v", err)
	}

	// -------------------------------------------------------------------
	// Step 2: PostJournal "dep-1" — debit player 100 / credit funding 100.
	// Player is debit-normal so a DEBIT entry increases their normalized
	// balance; credit-normal funding is the source.
	// -------------------------------------------------------------------
	_, err = c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: "dep-1", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "evt-dep-1",
			Entries: []*ledgerv1.Entry{
				{AccountId: "user:p1:cash_available:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: "platform:0:funding:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("PostJournal dep-1: %v", err)
	}

	// -------------------------------------------------------------------
	// Step 3: GetBalance — player should show normalized = 100.
	// -------------------------------------------------------------------
	balResp, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: "user:p1:cash_available:BRL", Currency: "BRL",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	got := balResp.Msg.GetBalance().GetNormalized()
	if got != "100" && got != "100.00" {
		t.Fatalf("want balance 100 (or 100.00), got %q", got)
	}

	// -------------------------------------------------------------------
	// Step 4: Idempotent replay — same IdempotencyKey "dep-1", different
	// EventId. Must succeed and leave the balance unchanged.
	// -------------------------------------------------------------------
	_, err = c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: "dep-1", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "evt-dep-1-replay",
			Entries: []*ledgerv1.Entry{
				{AccountId: "user:p1:cash_available:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: "platform:0:funding:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}

	// Balance must be unchanged after replay.
	balResp2, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: tenant, AccountId: "user:p1:cash_available:BRL", Currency: "BRL",
	}))
	if err != nil {
		t.Fatalf("GetBalance after replay: %v", err)
	}
	got2 := balResp2.Msg.GetBalance().GetNormalized()
	if got2 != "100" && got2 != "100.00" {
		t.Fatalf("want balance 100 after replay, got %q", got2)
	}

	// -------------------------------------------------------------------
	// Step 5: Negative case — attempt to overdraft the player account.
	// player is debit-normal + AllowNegative=false; a CREDIT entry reduces
	// their normalized balance. Posting credit 500 on player / debit 500 on
	// funding would take the player to -400, which must be rejected.
	// -------------------------------------------------------------------
	_, err = c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: "overdraft-1", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "evt-overdraft-1",
			Entries: []*ledgerv1.Entry{
				{AccountId: "user:p1:cash_available:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "500"},
				{AccountId: "platform:0:funding:BRL", Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "500"},
			},
		},
	}))
	if err == nil {
		t.Fatal("expected overdraft to fail with INSUFFICIENT_FUNDS, got nil error")
	}
	var cerr *connect.Error
	if !asConnectError(err, &cerr) {
		t.Fatalf("expected *connect.Error, got %T: %v", err, err)
	}
	if cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("expected CodeFailedPrecondition for insufficient funds, got %v", cerr.Code())
	}
	if !IsErrCode(err, ErrInsufficientFunds) {
		t.Fatalf("expected ledger-error-code=INSUFFICIENT_FUNDS, got: %v", err)
	}
}

// asConnectError unwraps err into *connect.Error (mirrors errors.As behaviour).
func asConnectError(err error, target **connect.Error) bool {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return false
	}
	*target = ce
	return true
}
