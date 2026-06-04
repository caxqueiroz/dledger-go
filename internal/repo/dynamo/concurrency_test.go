// Package dynamo_test — double-spend concurrency suite.
//
// These tests are the milestone's final money-safety gate for the DynamoDB
// backend. They exercise parallel Reserve operations and verify that:
//   - No more funds are reserved than the account contains (no double-spend).
//   - Idempotency is upheld under concurrent same-key races.
//   - A shared destination account conserves total system money.
//
// Run with -count=3 to shake flakes:
//
//	set -a; source .env; set +a
//	go test ./internal/repo/dynamo/ -run 'TestNoDoubleSpend|TestParallelSame|TestConcurrentFlowsShared' -v -count=3
package dynamo_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// ---------------------------------------------------------------------------
// Error-classification helpers (derived from the real error surface).
// ---------------------------------------------------------------------------

// isConflict returns true when err carries a retryable or flow-conflict ledger
// error code: either SERIALIZATION_RETRY_EXHAUSTED or FLOW_CONFLICT. Both
// surface as connect.CodeAborted (see internal/service/errors.go).
func isConflict(err error) bool {
	return dledger.IsErrCode(err, dledger.ErrSerializationRetryExhausted) ||
		dledger.IsErrCode(err, dledger.ErrFlowConflict)
}

// isInsufficientFunds returns true when err carries INSUFFICIENT_FUNDS.
func isInsufficientFunds(err error) bool {
	return dledger.IsErrCode(err, dledger.ErrInsufficientFunds)
}

// isOtherError returns true when the error is non-nil and is neither a conflict
// nor an insufficient-funds condition. Such errors are unexpected in
// double-spend scenarios and cause test failures when asserted.
func isOtherError(err error) bool {
	if err == nil {
		return false
	}
	return !isConflict(err) && !isInsufficientFunds(err)
}

// ---------------------------------------------------------------------------
// TestNoDoubleSpendUnderParallelReserves
// ---------------------------------------------------------------------------

// TestNoDoubleSpendUnderParallelReserves boots a wallet with 100.00 BRL, then
// fires 20 parallel goroutines each trying to Reserve(10.00) with a DISTINCT
// idempotency key. Each goroutine retries up to 50× on conflict errors (the
// retry budget is generous so that under contention every goroutine eventually
// receives a definitive answer: either success or INSUFFICIENT_FUNDS). After
// all goroutines complete:
//
//  1. Exactly 10 succeeded (100 / 10 = 10 maximum reservations).
//  2. Every non-success outcome is INSUFFICIENT_FUNDS (any other error type
//     immediately fails the test — it signals a genuine bug).
//  3. GetWallet: available + reserved == 100 AND reserved == 100.
func TestNoDoubleSpendUnderParallelReserves(t *testing.T) {
	fx := newDynamoWallet(t)
	c, w := fx.Client, fx.Wallet
	ctx := context.Background()

	const playerID = "nodbl-p1"
	const currency = "BRL"
	const goroutines = 20
	const amount = "10"
	// 50 retries: generous enough that every goroutine receives a definitive
	// answer (success or INSUFFICIENT_FUNDS) even under high contention.
	const maxRetries = 50

	// Provision accounts.
	if _, err := w.EnsurePlayerAccounts(ctx, playerID, currency); err != nil {
		t.Fatalf("EnsurePlayerAccounts: %v", err)
	}
	mustCreateAccount(t, c, "t1", "platform", "nodbl-src", "funding", currency, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	// Deposit 100 BRL.
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID:         playerID,
		Currency:         currency,
		Amount:           "100",
		FundingAccountID: "platform:nodbl-src:funding:BRL",
		ExternalRef:      "ev-nodbl-dep",
		IdempotencyKey:   "nodbl-dep-1",
		SourceService:    "test",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	var (
		successCount  atomic.Int64
		insuffCount   atomic.Int64
		totalRetries  atomic.Int64
		unexpectedErr atomic.Value // stores first unexpected error string
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("nodbl-res-%d", i)
			var lastErr error
			for attempt := 0; attempt < maxRetries; attempt++ {
				_, err := w.Reserve(ctx, dledger.ReserveInput{
					PlayerID:       playerID,
					Currency:       currency,
					Amount:         amount,
					IdempotencyKey: key,
					SourceService:  "test",
				})
				if err == nil {
					successCount.Add(1)
					return
				}
				lastErr = err
				if isConflict(err) {
					totalRetries.Add(1)
					// Retry same key — idempotent replay is safe.
					continue
				}
				// Non-conflict error: stop retrying.
				break
			}
			// Goroutine ended without success.
			if lastErr != nil {
				if isInsufficientFunds(lastErr) {
					insuffCount.Add(1)
					return
				}
				// Any other error is unexpected — conflicts exhausted without
				// a definitive answer are also unexpected given the generous
				// retry budget.
				unexpectedErr.CompareAndSwap(nil, lastErr.Error())
			}
		}()
	}

	wg.Wait()

	// Report retry activity for diagnostic visibility.
	t.Logf("successCount=%d insuffCount=%d totalRetries=%d",
		successCount.Load(), insuffCount.Load(), totalRetries.Load())

	// 1. No unexpected errors.
	if v := unexpectedErr.Load(); v != nil {
		t.Fatalf("unexpected error (not conflict or insufficient-funds): %s", v.(string))
	}

	// 2. Exactly 10 succeeded.
	if got := successCount.Load(); got != 10 {
		t.Fatalf("want exactly 10 successes, got %d", got)
	}

	// 3. Every non-success goroutine saw INSUFFICIENT_FUNDS.
	if successCount.Load()+insuffCount.Load() != goroutines {
		t.Fatalf("want success+insuffFunds==%d, got success=%d insuffFunds=%d",
			goroutines, successCount.Load(), insuffCount.Load())
	}

	// 4. Money invariant: available + reserved == 100, reserved == 100.
	// Poll until the GSI-backed OpenReservations count stabilises at 10 (one per
	// successful goroutine) before running the full strict assertion block.
	var snap dledger.WalletSnapshot
	pollUntil(t, func() bool {
		s, err := w.GetWallet(ctx, playerID, currency)
		if err != nil {
			return false
		}
		snap = s
		return len(snap.OpenReservations) == 10
	})
	total := snap.Available.Add(snap.Reserved)
	if total.String() != "100" {
		t.Fatalf("money invariant: available(%s) + reserved(%s) = %s, want 100",
			snap.Available, snap.Reserved, total)
	}
	if snap.Reserved.String() != "100" {
		t.Fatalf("want reserved=100, got %s", snap.Reserved)
	}
	t.Logf("final wallet: available=%s reserved=%s openReservations=%d",
		snap.Available, snap.Reserved, len(snap.OpenReservations))
}

// ---------------------------------------------------------------------------
// TestParallelSameIdempotencyKey
// ---------------------------------------------------------------------------

// TestParallelSameIdempotencyKey fires 10 goroutines each trying to
// Reserve(10.00) with the SAME idempotency key. Goroutines retry both
// serialization conflicts AND flow-conflict errors (a concurrent same-key insert
// losing the FIDEMP/RIDEMP race emits FlowConflict; on retry the RIDEMP marker
// already exists and the idempotent replay path returns success with the
// original reservation ID).
//
// After all goroutines complete:
//  1. ALL goroutines ended with success (no final errors).
//  2. Every successful goroutine received the SAME reservation ID.
//  3. GetWallet: exactly ONE 10.00 reservation; available == 90.
func TestParallelSameIdempotencyKey(t *testing.T) {
	fx := newDynamoWallet(t)
	c, w := fx.Client, fx.Wallet
	ctx := context.Background()

	const playerID = "sameidem-p1"
	const currency = "BRL"
	const goroutines = 10
	const amount = "10"
	const maxRetries = 10
	const sharedKey = "sameidem-res-shared"

	// Provision accounts.
	if _, err := w.EnsurePlayerAccounts(ctx, playerID, currency); err != nil {
		t.Fatalf("EnsurePlayerAccounts: %v", err)
	}
	mustCreateAccount(t, c, "t1", "platform", "sameidem-src", "funding", currency, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	// Deposit 100 BRL — enough for 10 × 10 if idempotency breaks.
	if _, err := w.Deposit(ctx, dledger.DepositInput{
		PlayerID:         playerID,
		Currency:         currency,
		Amount:           "100",
		FundingAccountID: "platform:sameidem-src:funding:BRL",
		ExternalRef:      "ev-sameidem-dep",
		IdempotencyKey:   "sameidem-dep-1",
		SourceService:    "test",
	}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	type result struct {
		id  string
		err error
	}

	results := make([]result, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	var totalRetries atomic.Int64

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			var lastErr error
			for attempt := 0; attempt < maxRetries; attempt++ {
				r, err := w.Reserve(ctx, dledger.ReserveInput{
					PlayerID:       playerID,
					Currency:       currency,
					Amount:         amount,
					IdempotencyKey: sharedKey, // same for all goroutines
					SourceService:  "test",
				})
				if err == nil {
					results[i] = result{id: r.ID}
					return
				}
				lastErr = err
				if isConflict(err) {
					totalRetries.Add(1)
					continue
				}
				break
			}
			results[i] = result{err: lastErr}
		}()
	}

	wg.Wait()

	t.Logf("totalRetries=%d", totalRetries.Load())

	// 1. All goroutines succeeded.
	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d ended with error: %v", i, r.err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// 2. All returned the same reservation ID.
	baseID := results[0].id
	for i, r := range results {
		if r.id != baseID {
			t.Errorf("goroutine %d: want ID=%s got ID=%s", i, baseID, r.id)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// 3. Exactly one 10.00 reservation; available == 90.
	// Poll until the GSI-backed OpenReservations list shows exactly 1 entry
	// before running the full strict assertion block.
	var snap dledger.WalletSnapshot
	pollUntil(t, func() bool {
		s, err := w.GetWallet(ctx, playerID, currency)
		if err != nil {
			return false
		}
		snap = s
		return len(snap.OpenReservations) == 1
	})
	if len(snap.OpenReservations) != 1 {
		t.Fatalf("want exactly 1 open reservation, got %d", len(snap.OpenReservations))
	}
	if snap.OpenReservations[0].ID != baseID {
		t.Fatalf("open reservation ID mismatch: want %s got %s", baseID, snap.OpenReservations[0].ID)
	}
	if snap.Available.String() != "90" {
		t.Fatalf("want available=90, got %s", snap.Available)
	}
	if snap.Reserved.String() != "10" {
		t.Fatalf("want reserved=10, got %s", snap.Reserved)
	}
	t.Logf("final wallet: available=%s reserved=%s", snap.Available, snap.Reserved)
}

// ---------------------------------------------------------------------------
// TestConcurrentFlowsSharedAccount
// ---------------------------------------------------------------------------

// TestConcurrentFlowsSharedAccount is the T5 reviewer's shared-house-account
// scenario: two players (A and B) each deposit 50 BRL. Ten parallel flows each
// move 1.00 from alternating players to a single shared destination account,
// using distinct idempotency keys + conflict retries.
//
// After all flows complete:
//  1. Destination balance == number of successful commits (all 10 when retries
//     are sufficient; money invariant holds regardless).
//  2. Total system money is conserved: sum of all three accounts' available+reserved == 100.
func TestConcurrentFlowsSharedAccount(t *testing.T) {
	fx := newDynamoWallet(t)
	c, w := fx.Client, fx.Wallet
	ctx := context.Background()

	const currency = "BRL"
	const goroutines = 10
	// 50 retries per step: generous enough to handle all 10 goroutines committing
	// to the single destination balance under maximum serialization contention.
	const maxRetries = 50

	// Provision player A and player B accounts.
	for _, pid := range []string{"sha-pA", "sha-pB"} {
		if _, err := w.EnsurePlayerAccounts(ctx, pid, currency); err != nil {
			t.Fatalf("EnsurePlayerAccounts(%s): %v", pid, err)
		}
	}
	// Funding account (source of deposits).
	mustCreateAccount(t, c, "t1", "platform", "sha-fund", "funding", currency, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	// Shared destination account.
	mustCreateAccount(t, c, "t1", "platform", "sha-dest", "shared_pool", currency, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	// Deposit 50 to each player.
	for i, pid := range []string{"sha-pA", "sha-pB"} {
		if _, err := w.Deposit(ctx, dledger.DepositInput{
			PlayerID:         pid,
			Currency:         currency,
			Amount:           "50",
			FundingAccountID: "platform:sha-fund:funding:BRL",
			ExternalRef:      fmt.Sprintf("ev-sha-dep-%d", i),
			IdempotencyKey:   fmt.Sprintf("sha-dep-%d", i),
			SourceService:    "test",
		}); err != nil {
			t.Fatalf("Deposit(%s): %v", pid, err)
		}
	}

	var (
		totalRetries   atomic.Int64
		commitCount    atomic.Int64
		unexpectedErr  atomic.Value
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()

			// Alternate between player A and player B.
			players := []string{"sha-pA", "sha-pB"}
			fromPlayer := players[i%2]
			// Use a fresh Reserve + Commit per flow (distinct idempotency keys).
			resKey := fmt.Sprintf("sha-res-%d", i)
			comKey := fmt.Sprintf("sha-com-%d", i)

			var reservationID string

			// --- Reserve step ---
			for attempt := 0; attempt < maxRetries; attempt++ {
				r, err := w.Reserve(ctx, dledger.ReserveInput{
					PlayerID:       fromPlayer,
					Currency:       currency,
					Amount:         "1",
					IdempotencyKey: resKey,
					SourceService:  "test",
				})
				if err == nil {
					reservationID = r.ID
					break
				}
				if isConflict(err) {
					totalRetries.Add(1)
					continue
				}
				unexpectedErr.CompareAndSwap(nil, fmt.Sprintf("goroutine %d Reserve: %v", i, err))
				return
			}
			if reservationID == "" {
				unexpectedErr.CompareAndSwap(nil, fmt.Sprintf("goroutine %d: Reserve exhausted %d retries", i, maxRetries))
				return
			}

			// --- Commit step ---
			for attempt := 0; attempt < maxRetries; attempt++ {
				_, err := w.Commit(ctx, dledger.CommitInput{
					ReservationID:        reservationID,
					DestinationAccountID: "platform:sha-dest:shared_pool:BRL",
					Amount:               "1",
					IdempotencyKey:       comKey,
					SourceService:        "test",
				})
				if err == nil {
					commitCount.Add(1)
					return
				}
				if isConflict(err) {
					totalRetries.Add(1)
					continue
				}
				unexpectedErr.CompareAndSwap(nil, fmt.Sprintf("goroutine %d Commit: %v", i, err))
				return
			}
			unexpectedErr.CompareAndSwap(nil, fmt.Sprintf("goroutine %d: Commit exhausted %d retries", i, maxRetries))
		}()
	}

	wg.Wait()

	t.Logf("commitCount=%d totalRetries=%d", commitCount.Load(), totalRetries.Load())

	// No unexpected errors.
	if v := unexpectedErr.Load(); v != nil {
		t.Fatalf("unexpected error: %s", v.(string))
	}

	// All 10 commits must have succeeded.
	if commitCount.Load() != goroutines {
		t.Fatalf("want %d commits, got %d", goroutines, commitCount.Load())
	}

	// Poll until the GSI-backed OpenReservations lists are empty for both
	// players (all 10 goroutines committed → no open reservations remain)
	// before running the full strict assertion block.
	var snapA, snapB dledger.WalletSnapshot
	pollUntil(t, func() bool {
		a, err := w.GetWallet(ctx, "sha-pA", currency)
		if err != nil {
			return false
		}
		b, err := w.GetWallet(ctx, "sha-pB", currency)
		if err != nil {
			return false
		}
		snapA, snapB = a, b
		return len(snapA.OpenReservations) == 0 && len(snapB.OpenReservations) == 0
	})

	// 1. Destination balance == 10.00.
	// Poll until the destination balance reaches 10 (all 10 commits must have
	// landed; ExtendDB balance updates are occasionally propagated with a small
	// delay under concurrent load).
	var destBal string
	pollUntil(t, func() bool {
		r, err := c.GetBalance(ctx, connect.NewRequest(&ledgerv1.GetBalanceRequest{
			TenantId:  "t1",
			AccountId: "platform:sha-dest:shared_pool:BRL",
			Currency:  currency,
		}))
		if err != nil {
			return false
		}
		destBal = r.Msg.GetBalance().GetNormalized()
		return destBal == "10" || destBal == "10.00"
	})
	if destBal != "10" && destBal != "10.00" {
		t.Fatalf("want destination balance=10, got %s", destBal)
	}

	// 2. Total system money conserved: sum all three wallets' available+reserved == 100.

	// Parse destination balance.
	var destAvail float64
	if _, err := fmt.Sscanf(destBal, "%f", &destAvail); err != nil {
		t.Fatalf("parse dest balance %q: %v", destBal, err)
	}

	totalA := snapA.Available.Add(snapA.Reserved)
	totalB := snapB.Available.Add(snapB.Reserved)

	aFloat, _ := totalA.Float64()
	bFloat, _ := totalB.Float64()
	systemTotal := aFloat + bFloat + destAvail

	t.Logf("playerA available=%s reserved=%s | playerB available=%s reserved=%s | dest=%s | systemTotal=%.2f",
		snapA.Available, snapA.Reserved, snapB.Available, snapB.Reserved, destBal, systemTotal)

	const epsilon = 0.001
	if systemTotal < 100.0-epsilon || systemTotal > 100.0+epsilon {
		t.Fatalf("money conservation violated: systemTotal=%.4f, want 100.00", systemTotal)
	}
}
