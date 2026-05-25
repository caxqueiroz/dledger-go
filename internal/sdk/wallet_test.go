// internal/sdk/wallet_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// newWalletWithEmbedded boots an embedded SDK and returns the client + a Wallet
// bound to tenant "t1". Scheduler is disabled because Wallet tests don't need it.
func newWalletWithEmbedded(t *testing.T) (dledger.Client, *dledger.Wallet) {
	t.Helper()
	ctx := context.Background()
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "sdk.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, dledger.NewWallet(c, "t1")
}

func TestWallet_EnsurePlayerAccounts_Idempotent(t *testing.T) {
	_, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	a, err := w.EnsurePlayerAccounts(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := w.EnsurePlayerAccounts(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if a != b {
		t.Fatalf("expected stable account IDs across calls, got %+v vs %+v", a, b)
	}
	if a.Available != "user:p1:cash_available:USD" || a.Reserved != "user:p1:cash_reserved:USD" {
		t.Fatalf("unexpected account IDs: %+v", a)
	}
}

func TestWallet_Deposit_IncreasesAvailable(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "evt-dep-1",
		IdempotencyKey:   "dep-1",
		SourceService:    "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	bal, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: "user:p1:cash_available:USD", Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "100" {
		t.Fatalf("want available=100 got %q", got)
	}
}

func TestWallet_Withdraw_DecreasesAvailable(t *testing.T) {
	c, w := newWalletWithEmbedded(t)
	ctx := context.Background()
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	mustCreate(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, "t1", "platform", "0", "withdraw", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "evt-d", IdempotencyKey: "d", SourceService: "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	if _, err := w.Withdraw(ctx, dledger.WithdrawInput{
		PlayerID: "p1", Currency: "USD", Amount: "30",
		WithdrawalAccountID: "platform:0:withdraw:USD",
		ExternalRef:         "evt-w", IdempotencyKey: "w", SourceService: "payouts",
	}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	bal, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: "user:p1:cash_available:USD", Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "70" {
		t.Fatalf("want available=70 got %q", got)
	}
}
