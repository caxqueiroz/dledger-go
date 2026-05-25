//go:build integration

package sdk_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func startCRDBForSDK(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "cockroachdb/cockroach:v24.1.5",
		ExposedPorts: []string{"26257/tcp"},
		Cmd:          []string{"start-single-node", "--insecure"},
		WaitingFor:   wait.ForLog("nodeID:").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start crdb: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "26257")
	return "postgres://root@" + host + ":" + port.Port() + "/defaultdb?sslmode=disable"
}

// TestNewEmbedded_CRDB_EndToEnd boots the SDK in embedded mode against a real
// CRDB container, runs migrations via the embedded FS, and drives a Wallet
// through deposit + reserve + commit. Proves the SDK works identically on the
// production backend.
func TestNewEmbedded_CRDB_EndToEnd(t *testing.T) {
	ctx := context.Background()
	dsn := startCRDBForSDK(t)

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.CRDB, DSN: dsn,
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	tenant := "t1"
	mustCreate(t, c, tenant, "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, tenant, "market", "42", "collateral_pool", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	w := dledger.NewWallet(c, tenant)
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID: "p1", Currency: "USD", Amount: "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "ext-d", IdempotencyKey: "d", SourceService: "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	r, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID: "p1", Currency: "USD", Amount: "40",
		IdempotencyKey: "r", SourceService: "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := w.Commit(ctx, dledger.CommitInput{
		ReservationID:        r.ID,
		DestinationAccountID: "market:42:collateral_pool:USD",
		Amount:               "40", IdempotencyKey: "c", SourceService: "matcher",
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	snap, err := w.GetWallet(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("GetWallet: %v", err)
	}
	if snap.Available.String() != "60" {
		t.Fatalf("want available=60 got %s", snap.Available)
	}
	if snap.Reserved.String() != "0" {
		t.Fatalf("want reserved=0 got %s", snap.Reserved)
	}
	if len(snap.OpenReservations) != 0 {
		t.Fatalf("want 0 open got %d", len(snap.OpenReservations))
	}

	// IsErrCode round-trip on CRDB
	if err := func() error {
		_, e := c.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
			TenantId:          tenant,
			IdempotencyKey:    "boom",
			SourceAccountId:   "user:p1:cash_available:USD",
			ReservedAccountId: "user:p1:cash_reserved:USD",
			Currency:          "USD", Amount: "9999999",
			SourceService: "test",
		}))
		return e
	}(); err == nil || !dledger.IsErrCode(err, dledger.ErrInsufficientFunds) {
		t.Fatalf("want INSUFFICIENT_FUNDS, got %v", err)
	}
}
