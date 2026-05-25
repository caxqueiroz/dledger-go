// internal/sdk/wallet_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"

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
