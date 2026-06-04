// Package dynamo_test contains integration tests for the DynamoDB reservation
// surface, exercised through the public Wallet API (dledger.Wallet).
package dynamo_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	dynamostore "github.com/caxqueiroz/dledger-go/internal/repo/dynamo"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// walletFixture holds a full embedded DynamoDB client, a Wallet, and the raw
// Store so tests can call low-level store methods (e.g. ListExpiredReservations).
type walletFixture struct {
	Client dledger.Client
	Wallet *dledger.Wallet
	Store  *dynamostore.Store
}

// newDynamoWallet boots a full embedded DynamoDB client and a Wallet bound to
// "t1". The table is unique per test and deleted on cleanup.
// Skipped when AWS_ENDPOINT_URL_DYNAMODB is unset.
func newDynamoWallet(t *testing.T) walletFixture {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") == "" {
		t.Skip("AWS_ENDPOINT_URL_DYNAMODB not set; skipping DynamoDB integration test")
	}
	ctx := context.Background()
	table := fmt.Sprintf("dltest_%08x_ledger", rand.Uint32()) //nolint:gosec
	c, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend:          dledger.DynamoDB,
		DSN:              table,
		MigrateMode:      dledger.MigrateAuto,
		DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	// Open a second handle to the same table for raw store reads.
	rawStore, err := dynamostore.Open(ctx, table)
	if err != nil {
		_ = c.Close()
		t.Fatalf("Open raw store: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := rawStore.DeleteTable(cleanupCtx); err != nil {
			t.Logf("cleanup DeleteTable: %v", err)
		}
		_ = rawStore.Close()
		_ = c.Close()
	})
	return walletFixture{
		Client: c,
		Wallet: dledger.NewWallet(c, "t1"),
		Store:  rawStore,
	}
}

// mustCreateAccount creates an account and fatals on error.
func mustCreateAccount(t *testing.T, c dledger.Client, tenant, ownerType, ownerID, acctType, ccy string, nb ledgerv1.NormalBalance) {
	t.Helper()
	_, err := c.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: acctType, Currency: ccy, NormalBalance: nb,
	}))
	if err != nil {
		t.Fatalf("CreateAccount %s:%s:%s: %v", ownerType, ownerID, acctType, err)
	}
}

// waitForGSI sleeps long enough for the GSI to catch up in the test environment.
// ExtendDB propagates GSI writes synchronously in most cases but we add a small
// guard to avoid flakiness.
func waitForGSI() {
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") != "" {
		time.Sleep(250 * time.Millisecond)
	}
}

// pollUntil retries fn every 100ms up to 10 times until it returns true.
// Used for GSI-backed reads on ExtendDB, whose index propagation can
// occasionally exceed a fixed sleep.
func pollUntil(t *testing.T, fn func() bool) {
	t.Helper()
	for range 10 {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("pollUntil: condition not reached within 1s")
}

// ---------------------------------------------------------------------------
// TestWalletReserveCommitReleaseOnDynamo
// ---------------------------------------------------------------------------

// TestWalletReserveCommitReleaseOnDynamo is the primary TDD scenario:
//  1. Deposit 100 USD.
//  2. Reserve 40 → wallet shows available 60 / reserved 40 with 1 open reservation.
//  3. Commit 25 to a destination account.
//  4. Release 15 → reserved 0, no open reservations.
//  5. Replay Reserve with same idempotency key returns SAME reservation ID.
func TestWalletReserveCommitReleaseOnDynamo(t *testing.T) {
	fx := newDynamoWallet(t)
	c, w := fx.Client, fx.Wallet
	ctx := context.Background()

	// Provision player accounts + funding account
	if _, err := w.EnsurePlayerAccounts(ctx, "p1", "USD"); err != nil {
		t.Fatalf("EnsurePlayerAccounts: %v", err)
	}
	mustCreateAccount(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	// Destination account for Commit
	mustCreateAccount(t, c, "t1", "market", "42", "collateral_pool", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	// Deposit 100
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID:         "p1",
		Currency:         "USD",
		Amount:           "100",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "ev-dep-1",
		IdempotencyKey:   "dep-1",
		SourceService:    "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	// Reserve 40
	r, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID:       "p1",
		Currency:       "USD",
		Amount:         "40",
		IdempotencyKey: "res-1",
		SourceService:  "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if r.Status != "HELD" {
		t.Fatalf("Reserve: want HELD got %q", r.Status)
	}
	if r.OutstandingAmount != "40" {
		t.Fatalf("Reserve: want OutstandingAmount=40 got %q", r.OutstandingAmount)
	}
	reservationID := r.ID

	// Wallet snapshot: available 60, reserved 40, 1 open reservation
	waitForGSI()
	snap, err := w.GetWallet(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("GetWallet: %v", err)
	}
	if snap.Available.String() != "60" {
		t.Fatalf("GetWallet: want available=60 got %s", snap.Available)
	}
	if snap.Reserved.String() != "40" {
		t.Fatalf("GetWallet: want reserved=40 got %s", snap.Reserved)
	}
	if len(snap.OpenReservations) != 1 {
		t.Fatalf("GetWallet: want 1 open reservation got %d", len(snap.OpenReservations))
	}
	if snap.OpenReservations[0].ID != reservationID {
		t.Fatalf("GetWallet: reservation ID mismatch want %s got %s", reservationID, snap.OpenReservations[0].ID)
	}

	// Commit 25
	r, err = w.Commit(ctx, dledger.CommitInput{
		ReservationID:        reservationID,
		DestinationAccountID: "market:42:collateral_pool:USD",
		Amount:               "25",
		IdempotencyKey:       "com-1",
		SourceService:        "matcher",
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if r.Status != "PARTIAL" {
		t.Fatalf("Commit: want PARTIAL got %q", r.Status)
	}
	if r.OutstandingAmount != "15" {
		t.Fatalf("Commit: want OutstandingAmount=15 got %q", r.OutstandingAmount)
	}
	if r.CommittedAmount != "25" {
		t.Fatalf("Commit: want CommittedAmount=25 got %q", r.CommittedAmount)
	}

	// Release 15 (the remaining outstanding)
	r, err = w.Release(ctx, dledger.ReleaseInput{
		ReservationID:  reservationID,
		Amount:         "15",
		IdempotencyKey: "rel-1",
		SourceService:  "matcher",
	})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if r.Status != "RELEASED" {
		t.Fatalf("Release: want RELEASED got %q", r.Status)
	}
	if r.OutstandingAmount != "0" {
		t.Fatalf("Release: want OutstandingAmount=0 got %q", r.OutstandingAmount)
	}
	if r.ReleasedAmount != "15" {
		t.Fatalf("Release: want ReleasedAmount=15 got %q", r.ReleasedAmount)
	}

	// After full release: no open reservations
	waitForGSI()
	snap2, err := w.GetWallet(ctx, "p1", "USD")
	if err != nil {
		t.Fatalf("GetWallet after release: %v", err)
	}
	if len(snap2.OpenReservations) != 0 {
		t.Fatalf("GetWallet after release: want 0 open reservations got %d", len(snap2.OpenReservations))
	}

	// Idempotent replay: same idempotency key must return same reservation ID
	r2, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID:       "p1",
		Currency:       "USD",
		Amount:         "40",
		IdempotencyKey: "res-1", // same key
		SourceService:  "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve (replay): %v", err)
	}
	if r2.ID != reservationID {
		t.Fatalf("Reserve (replay): want same ID=%s got %s", reservationID, r2.ID)
	}
}

// ---------------------------------------------------------------------------
// TestReservationExpiryListing
// ---------------------------------------------------------------------------

// TestReservationExpiryListing verifies:
//  1. A reservation with ExpiresAt in the past appears in ListExpiredReservations.
//  2. A terminal (RELEASED) reservation does NOT appear in the list.
func TestReservationExpiryListing(t *testing.T) {
	fx := newDynamoWallet(t)
	c, w, s := fx.Client, fx.Wallet, fx.Store
	ctx := context.Background()

	if _, err := w.EnsurePlayerAccounts(ctx, "p2", "USD"); err != nil {
		t.Fatalf("EnsurePlayerAccounts: %v", err)
	}
	mustCreateAccount(t, c, "t1", "platform", "0", "funding", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	// Deposit so Reserve has funds
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID:         "p2",
		Currency:         "USD",
		Amount:           "200",
		FundingAccountID: "platform:0:funding:USD",
		ExternalRef:      "ev-dep-2",
		IdempotencyKey:   "dep-2",
		SourceService:    "stripe",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	// Reserve with expiry 1s in the future
	expiresAt := time.Now().UTC().Add(1 * time.Second)
	r1, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID:       "p2",
		Currency:       "USD",
		Amount:         "50",
		ExpiresAt:      expiresAt,
		IdempotencyKey: "res-exp-1",
		SourceService:  "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve (expiring): %v", err)
	}

	// Reserve another that we will release (terminal — should NOT appear in expired list)
	r2, err := w.Reserve(ctx, dledger.ReserveInput{
		PlayerID:       "p2",
		Currency:       "USD",
		Amount:         "30",
		ExpiresAt:      expiresAt,
		IdempotencyKey: "res-exp-2",
		SourceService:  "matcher",
	})
	if err != nil {
		t.Fatalf("Reserve (to-be-released): %v", err)
	}

	// Release r2 fully → terminal RELEASED status
	mustCreateAccount(t, c, "t1", "market", "99", "collateral_pool", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	if _, err := w.Release(ctx, dledger.ReleaseInput{
		ReservationID:  r2.ID,
		Amount:         "30",
		IdempotencyKey: "rel-exp-2",
		SourceService:  "matcher",
	}); err != nil {
		t.Fatalf("Release r2: %v", err)
	}

	// Wait past expiry + GSI propagation
	time.Sleep(1500 * time.Millisecond)
	waitForGSI()

	expired, err := s.ListExpiredReservations(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("ListExpiredReservations: %v", err)
	}

	// r1 must appear; r2 must NOT (terminal)
	found := false
	for _, e := range expired {
		if e.ID == r1.ID && e.TenantID == "t1" {
			found = true
		}
		if e.ID == r2.ID {
			t.Errorf("ListExpiredReservations: released reservation r2 (%s) must not appear", r2.ID)
		}
	}
	if !found {
		t.Errorf("ListExpiredReservations: want r1 (%s) in results; got %+v", r1.ID, expired)
	}
}
