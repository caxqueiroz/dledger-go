// internal/sdk/embedded_test.go
package sdk_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

func TestNewEmbedded_OpensAndMigrates(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	// CreateAccount succeeds only if migrations ran (accounts table exists).
	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
}

func TestNewEmbedded_MigrateSkip_DoesNotRun(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, MigrateMode: dledger.MigrateSkip,
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	_, err = c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err == nil {
		t.Fatalf("expected CreateAccount to fail on un-migrated DB")
	}
}

func TestNewEmbedded_Close_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: filepath.Join(t.TempDir(), "sdk.db"),
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestNewEmbedded_Scheduler_ExpiresReservation verifies the scheduler
// goroutine actually runs by setting up a reservation that expired in the
// past and polling until it transitions to EXPIRED.
//
// The scheduler's expiry tick defaults to 30s, so the polling deadline
// must be at least one tick. We use 2 minutes to keep the test stable
// under load.
func TestNewEmbedded_Scheduler_ExpiresReservation(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	defer c.Close()

	tenant := "t1"
	mustCreate(t, c, tenant, "platform", "0", "src", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	mustCreate(t, c, tenant, "user", "u1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	mustCreate(t, c, tenant, "user", "u1", "cash_reserved", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err = c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: "seed", SourceService: "test",
		Journal: &ledgerv1.Journal{
			EventId: "seed-evt",
			Entries: []*ledgerv1.Entry{
				{AccountId: "user:u1:cash_available:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: "platform:0:src:USD", Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Reservation with an already-past expiry.
	resv, err := c.CreateReservation(ctx, connect.NewRequest(&ledgerv1.CreateReservationRequest{
		TenantId: tenant, IdempotencyKey: "res-1",
		SourceAccountId:   "user:u1:cash_available:USD",
		ReservedAccountId: "user:u1:cash_reserved:USD",
		Currency:          "USD", Amount: "10",
		ExpiresAt:     timestamp(t, time.Now().Add(-1*time.Second)),
		SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("CreateReservation: %v", err)
	}

	// Poll for up to 2 minutes (expiry tick = 30s default).
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		got, err := c.GetReservation(ctx, connect.NewRequest(&ledgerv1.GetReservationRequest{
			TenantId: tenant, ReservationId: resv.Msg.GetReservation().GetId(),
		}))
		if err != nil {
			t.Fatalf("GetReservation: %v", err)
		}
		if got.Msg.GetReservation().GetStatus() == "EXPIRED" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("reservation did not transition to EXPIRED within deadline")
}
