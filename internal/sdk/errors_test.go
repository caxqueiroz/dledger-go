// internal/sdk/errors_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func setupForInsufficient(t *testing.T, c dledger.Client) {
	t.Helper()
	ctx := context.Background()
	w := dledger.NewWallet(c, "t1")
	if _, err := w.EnsurePlayerAccounts(ctx, "broke", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
}

// reserveTooMuch triggers INSUFFICIENT_FUNDS on a freshly-created cash_available
// (zero balance, allow_negative=false).
func reserveTooMuch(ctx context.Context, c dledger.Client) error {
	_, err := c.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId:          "t1",
		IdempotencyKey:    "boom",
		SourceAccountId:   "user:broke:cash_available:USD",
		ReservedAccountId: "user:broke:cash_reserved:USD",
		Currency:          "USD",
		Amount:            "999999",
		SourceService:     "test",
	}))
	return err
}

func TestIsErrCode_EmbeddedAndRemote(t *testing.T) {
	ctx := context.Background()

	// Embedded
	emb, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "e.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer emb.Close()
	setupForInsufficient(t, emb)
	if err := reserveTooMuch(ctx, emb); err == nil || !dledger.IsErrCode(err, dledger.ErrInsufficientFunds) {
		t.Fatalf("embedded: want ErrInsufficientFunds, got %v", err)
	}

	// Remote (against an httptest server wrapping the same embedded server pattern)
	rem := newRemoteAgainstEmbeddedServer(t, "t1")
	defer rem.Close()
	setupForInsufficient(t, rem)
	if err := reserveTooMuch(ctx, rem); err == nil || !dledger.IsErrCode(err, dledger.ErrInsufficientFunds) {
		t.Fatalf("remote: want ErrInsufficientFunds, got %v", err)
	}
}
