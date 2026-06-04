package dynamo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ---------------------------------------------------------------------------
// TestOutboxLifecycle
// ---------------------------------------------------------------------------

// TestOutboxLifecycle is the primary TDD scenario for Task 9:
//
//  1. Tx inserts two outbox events (o1 older, o2 newer) via distinct CreatedAt
//     timestamps and Commits.
//  2. PendingOutbox(10) returns exactly 2 events, OLDEST FIRST.
//  3. MarkOutboxPublished("o1") removes it from the pending GSI.
//     IncrementOutboxAttempts("o2") bumps its attempts counter.
//  4. PendingOutbox(10) returns exactly [o2].
//  5. MarkOutboxPublished("missing") returns an error (condition failure).
func TestOutboxLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant := "t-outbox-lifecycle"

	// Use explicit, deterministically-ordered timestamps so GSI1SK ordering is
	// guaranteed regardless of wall-clock resolution.
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Second)

	o1 := repo.OutboxEvent{
		ID:             "o1",
		TenantID:       tenant,
		AggregateID:    "agg-1",
		EventType:      "test.event.v1",
		IdempotencyKey: "idemp-o1",
		Payload:        []byte(`{"hello":"o1"}`),
		CreatedAt:      t1,
	}
	o2 := repo.OutboxEvent{
		ID:             "o2",
		TenantID:       tenant,
		AggregateID:    "agg-2",
		EventType:      "test.event.v1",
		IdempotencyKey: "idemp-o2",
		Payload:        []byte(`{"hello":"o2"}`),
		CreatedAt:      t2,
	}

	// Step 1: insert via Tx and commit.
	tx := mustBegin(t, s)
	if err := tx.InsertOutbox(ctx, o1); err != nil {
		t.Fatalf("InsertOutbox o1: %v", err)
	}
	if err := tx.InsertOutbox(ctx, o2); err != nil {
		t.Fatalf("InsertOutbox o2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Wait for GSI propagation in local ExtendDB.
	waitForGSI(t)

	// Step 2: PendingOutbox → exactly 2, oldest first.
	pending, err := s.PendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("PendingOutbox: want 2 events, got %d", len(pending))
	}
	if pending[0].ID != "o1" {
		t.Errorf("PendingOutbox ordering: want pending[0].ID=o1, got %q", pending[0].ID)
	}
	if pending[1].ID != "o2" {
		t.Errorf("PendingOutbox ordering: want pending[1].ID=o2, got %q", pending[1].ID)
	}

	// Assert fields round-trip.
	if string(pending[0].Payload) != string(o1.Payload) {
		t.Errorf("o1 Payload: want %s, got %s", o1.Payload, pending[0].Payload)
	}
	if pending[0].TenantID != o1.TenantID {
		t.Errorf("o1 TenantID: want %s, got %s", o1.TenantID, pending[0].TenantID)
	}
	if pending[0].EventType != o1.EventType {
		t.Errorf("o1 EventType: want %s, got %s", o1.EventType, pending[0].EventType)
	}
	if !pending[0].CreatedAt.Equal(o1.CreatedAt) {
		t.Errorf("o1 CreatedAt: want %v, got %v", o1.CreatedAt, pending[0].CreatedAt)
	}

	// Step 3: MarkOutboxPublished(o1) and IncrementOutboxAttempts(o2).
	if err := s.MarkOutboxPublished(ctx, "o1"); err != nil {
		t.Fatalf("MarkOutboxPublished o1: %v", err)
	}
	if err := s.IncrementOutboxAttempts(ctx, "o2"); err != nil {
		t.Fatalf("IncrementOutboxAttempts o2: %v", err)
	}

	// Wait for GSI propagation.
	waitForGSI(t)

	// Assert o2 attempts increment landed in the database.
	var rawO2 outboxItem
	found, err := s.getItem(ctx, outboxPK("o2"), &rawO2)
	if err != nil || !found {
		t.Fatalf("getItem o2 = %v/%v, want found", found, err)
	}
	if rawO2.Attempts != 1 {
		t.Errorf("o2 attempts = %d, want 1", rawO2.Attempts)
	}

	// Step 4: PendingOutbox → exactly [o2].
	pending2, err := s.PendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("PendingOutbox step4: %v", err)
	}
	if len(pending2) != 1 {
		t.Fatalf("PendingOutbox step4: want 1 event, got %d: %+v", len(pending2), pending2)
	}
	if pending2[0].ID != "o2" {
		t.Errorf("PendingOutbox step4: want o2, got %q", pending2[0].ID)
	}

	// Step 5: MarkOutboxPublished("missing") → error.
	if err := s.MarkOutboxPublished(ctx, "missing"); err == nil {
		t.Fatal("MarkOutboxPublished(missing): want error, got nil")
	}
}

// waitForGSI sleeps 250 ms when a local DynamoDB endpoint is configured so
// that GSI updates have time to propagate.
func waitForGSI(t *testing.T) {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") != "" {
		time.Sleep(250 * time.Millisecond)
	}
}
