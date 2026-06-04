package dynamo

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newID() string { return uuid.NewString() }

// makeJournal builds a minimal balanced journal (debit + credit, same amount).
func makeJournal(tenantID, flowRunID, eventID, acctDebit, acctCredit, ccy string, amount decimal.Decimal) ledger.Journal {
	return ledger.Journal{
		ID:            newID(),
		TenantID:      tenantID,
		FlowRunID:     flowRunID,
		EventID:       eventID,
		SourceService: "test-svc",
		SourceType:    "transfer",
		ActorID:       "actor-1",
		Metadata:      map[string]any{},
		Entries: []ledger.Entry{
			{AccountID: acctDebit, Currency: ccy, Direction: ledger.DirectionDebit, Amount: amount},
			{AccountID: acctCredit, Currency: ccy, Direction: ledger.DirectionCredit, Amount: amount},
		},
		CreatedAt: time.Now().UTC(),
	}
}

// makeFlowRun builds a minimal FlowRun in RUNNING status.
func makeFlowRun(tenantID, idempKey string) ledger.FlowRun {
	return ledger.FlowRun{
		ID:             newID(),
		TenantID:       tenantID,
		FlowType:       "transfer",
		IdempotencyKey: idempKey,
		SourceService:  "test-svc",
		ActorID:        "actor-1",
		Status:         ledger.FlowRunning,
		Metadata:       map[string]any{},
		CreatedAt:      time.Now().UTC(),
	}
}

// makeFlowStep builds a FlowStep for a given flow.
func makeFlowStep(tenantID, flowRunID, journalID string) ledger.FlowStep {
	return ledger.FlowStep{
		ID:        newID(),
		TenantID:  tenantID,
		FlowRunID: flowRunID,
		StepID:    "step-1",
		Status:    ledger.StepCompleted,
		JournalID: journalID,
		CreatedAt: time.Now().UTC(),
	}
}

// ---------------------------------------------------------------------------
// TestTxFlowJournalOutboxRoundTrip
// ---------------------------------------------------------------------------

// TestTxFlowJournalOutboxRoundTrip is the primary TDD scenario:
//
//  1. Tx1: idempotency miss → InsertFlowRun → InsertJournal → InsertEntry×2 →
//     InsertFlowStep → InsertOutbox → CompleteFlowRun → Commit
//  2. Tx2: GetFlowByIdempotency returns the COMPLETED flow; GetFlowSteps returns step.
//  3. Tx3: Duplicate event_id → Commit fails with CodeFlowConflict.
func TestTxFlowJournalOutboxRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant := "t-roundtrip"
	idempKey := "idemp-" + newID()
	amount := decimal.NewFromInt(50)

	acctA := "acct-A-" + newID()
	acctB := "acct-B-" + newID()

	// -----------------------------------------------------------------------
	// Tx1: full flow lifecycle
	// -----------------------------------------------------------------------
	tx1 := mustBegin(t, s)

	// Idempotency miss before insert
	existing, err := tx1.GetFlowByIdempotency(ctx, tenant, idempKey)
	if err != nil {
		t.Fatalf("tx1 GetFlowByIdempotency (miss): %v", err)
	}
	if existing != nil {
		t.Fatalf("tx1 GetFlowByIdempotency (miss): want nil, got %+v", existing)
	}

	// Insert flow run
	f1 := makeFlowRun(tenant, idempKey)
	if err := tx1.InsertFlowRun(ctx, f1); err != nil {
		t.Fatalf("tx1 InsertFlowRun: %v", err)
	}

	// Insert journal (event_id must be unique)
	eventID := "evt-" + newID()
	j1 := makeJournal(tenant, f1.ID, eventID, acctA, acctB, "USD", amount)
	if err := tx1.InsertJournal(ctx, j1); err != nil {
		t.Fatalf("tx1 InsertJournal: %v", err)
	}

	// Insert 2 entries
	if err := tx1.InsertEntry(ctx, tenant, newID(), j1.ID, acctA, "USD", ledger.DirectionDebit, amount); err != nil {
		t.Fatalf("tx1 InsertEntry debit: %v", err)
	}
	if err := tx1.InsertEntry(ctx, tenant, newID(), j1.ID, acctB, "USD", ledger.DirectionCredit, amount); err != nil {
		t.Fatalf("tx1 InsertEntry credit: %v", err)
	}

	// Insert flow step
	fs1 := makeFlowStep(tenant, f1.ID, j1.ID)
	if err := tx1.InsertFlowStep(ctx, fs1); err != nil {
		t.Fatalf("tx1 InsertFlowStep: %v", err)
	}

	// Insert outbox
	obEvent := repo.OutboxEvent{
		ID:             newID(),
		TenantID:       tenant,
		AggregateID:    f1.ID,
		EventType:      "transfer.step-1",
		IdempotencyKey: f1.ID + ":step-1",
		Payload:        []byte(`{"step":"step-1"}`),
		CreatedAt:      time.Now().UTC(),
	}
	if err := tx1.InsertOutbox(ctx, obEvent); err != nil {
		t.Fatalf("tx1 InsertOutbox: %v", err)
	}

	// Complete flow run
	if err := tx1.CompleteFlowRun(ctx, tenant, f1.ID); err != nil {
		t.Fatalf("tx1 CompleteFlowRun: %v", err)
	}

	// Commit
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 Commit: %v", err)
	}

	// -----------------------------------------------------------------------
	// Tx2: read back the committed data
	// -----------------------------------------------------------------------
	tx2 := mustBegin(t, s)

	got, err := tx2.GetFlowByIdempotency(ctx, tenant, idempKey)
	if err != nil {
		t.Fatalf("tx2 GetFlowByIdempotency: %v", err)
	}
	if got == nil {
		t.Fatal("tx2 GetFlowByIdempotency: want flow, got nil")
	}
	if got.ID != f1.ID {
		t.Errorf("tx2 GetFlowByIdempotency: want ID=%s, got %s", f1.ID, got.ID)
	}
	if got.Status != ledger.FlowCompleted {
		t.Errorf("tx2 GetFlowByIdempotency: want COMPLETED, got %s", got.Status)
	}
	if got.CompletedAt == nil {
		t.Error("tx2 GetFlowByIdempotency: CompletedAt must be set")
	}

	steps, err := tx2.GetFlowSteps(ctx, tenant, f1.ID)
	if err != nil {
		t.Fatalf("tx2 GetFlowSteps: %v", err)
	}
	if len(steps) != 1 {
		t.Fatalf("tx2 GetFlowSteps: want 1 step, got %d", len(steps))
	}
	if steps[0].JournalID != j1.ID {
		t.Errorf("tx2 GetFlowSteps: step JournalID want %s, got %s", j1.ID, steps[0].JournalID)
	}

	if err := tx2.Rollback(); err != nil {
		t.Fatalf("tx2 Rollback: %v", err)
	}

	// -----------------------------------------------------------------------
	// Tx3: duplicate event_id → CodeFlowConflict
	// -----------------------------------------------------------------------
	tx3 := mustBegin(t, s)

	f2 := makeFlowRun(tenant, "idemp-dup-"+newID())
	if err := tx3.InsertFlowRun(ctx, f2); err != nil {
		t.Fatalf("tx3 InsertFlowRun: %v", err)
	}

	// Reuse the SAME eventID → EVT# marker collides
	jDup := makeJournal(tenant, f2.ID, eventID, acctA, acctB, "USD", amount)
	if err := tx3.InsertJournal(ctx, jDup); err != nil {
		t.Fatalf("tx3 InsertJournal (dup event): %v", err)
	}

	commitErr := tx3.Commit()
	if commitErr == nil {
		t.Fatal("tx3 Commit: expected CodeFlowConflict for duplicate event_id, got nil")
	}
	if !ledger.IsDomainCode(commitErr, ledger.CodeFlowConflict) {
		t.Errorf("tx3 Commit: want CodeFlowConflict, got %v", commitErr)
	}
}

// ---------------------------------------------------------------------------
// TestTxFlowOverlayVisibility
// ---------------------------------------------------------------------------

// TestTxFlowOverlayVisibility verifies carry-forward overlay reads within a
// single uncommitted transaction: GetFlowByIdempotency and GetFlowSteps must
// return buffered data without hitting DynamoDB.
func TestTxFlowOverlayVisibility(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant := "t-overlay"
	idempKey := "idemp-overlay-" + newID()

	tx := mustBegin(t, s)

	f := makeFlowRun(tenant, idempKey)
	if err := tx.InsertFlowRun(ctx, f); err != nil {
		t.Fatalf("InsertFlowRun: %v", err)
	}

	// GetFlowByIdempotency must return the buffered RUNNING flow without commit.
	got, err := tx.GetFlowByIdempotency(ctx, tenant, idempKey)
	if err != nil {
		t.Fatalf("GetFlowByIdempotency (overlay): %v", err)
	}
	if got == nil {
		t.Fatal("GetFlowByIdempotency (overlay): want buffered flow, got nil")
	}
	if got.ID != f.ID {
		t.Errorf("GetFlowByIdempotency (overlay): want ID=%s, got %s", f.ID, got.ID)
	}
	if got.Status != ledger.FlowRunning {
		t.Errorf("GetFlowByIdempotency (overlay): want RUNNING, got %s", got.Status)
	}

	// No steps yet
	steps0, err := tx.GetFlowSteps(ctx, tenant, f.ID)
	if err != nil {
		t.Fatalf("GetFlowSteps (overlay, empty): %v", err)
	}
	if len(steps0) != 0 {
		t.Errorf("GetFlowSteps (overlay, empty): want 0, got %d", len(steps0))
	}

	// Insert a journal so we have a journalID for the step
	jID := newID()
	j := ledger.Journal{
		ID: jID, TenantID: tenant, FlowRunID: f.ID,
		EventID:   "evt-ov-" + newID(),
		CreatedAt: time.Now().UTC(),
		Entries: []ledger.Entry{
			{AccountID: "acct-x", Currency: "USD", Direction: ledger.DirectionDebit, Amount: decimal.NewFromInt(1)},
			{AccountID: "acct-y", Currency: "USD", Direction: ledger.DirectionCredit, Amount: decimal.NewFromInt(1)},
		},
	}
	if err := tx.InsertJournal(ctx, j); err != nil {
		t.Fatalf("InsertJournal: %v", err)
	}

	// InsertFlowStep — must be visible in GetFlowSteps within same tx
	fs := makeFlowStep(tenant, f.ID, jID)
	if err := tx.InsertFlowStep(ctx, fs); err != nil {
		t.Fatalf("InsertFlowStep: %v", err)
	}

	steps1, err := tx.GetFlowSteps(ctx, tenant, f.ID)
	if err != nil {
		t.Fatalf("GetFlowSteps (overlay, after insert): %v", err)
	}
	if len(steps1) != 1 {
		t.Fatalf("GetFlowSteps (overlay, after insert): want 1, got %d", len(steps1))
	}
	if steps1[0].JournalID != jID {
		t.Errorf("GetFlowSteps (overlay): step JournalID want %s, got %s", jID, steps1[0].JournalID)
	}

	// Must return copies, not aliases — mutating the returned slice must not
	// affect the overlay.
	steps1[0].JournalID = "mutated"
	steps2, err := tx.GetFlowSteps(ctx, tenant, f.ID)
	if err != nil {
		t.Fatalf("GetFlowSteps (overlay, copy check): %v", err)
	}
	if steps2[0].JournalID == "mutated" {
		t.Error("GetFlowSteps (overlay): returned alias, not copy")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestTxJournalEntriesEmbedded
// ---------------------------------------------------------------------------

// TestTxJournalEntriesEmbedded verifies that after commit the journal item in
// DynamoDB contains the 2 entries (embedded) with exact amounts.
func TestTxJournalEntriesEmbedded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant := "t-journal-emb"
	amount1 := decimal.NewFromInt(75)
	amount2 := decimal.NewFromInt(75)

	f := makeFlowRun(tenant, "idemp-emb-"+newID())

	tx := mustBegin(t, s)

	if err := tx.InsertFlowRun(ctx, f); err != nil {
		t.Fatalf("InsertFlowRun: %v", err)
	}

	j := makeJournal(tenant, f.ID, "evt-emb-"+newID(), "acct-p", "acct-q", "EUR", amount1)
	if err := tx.InsertJournal(ctx, j); err != nil {
		t.Fatalf("InsertJournal: %v", err)
	}

	if err := tx.InsertEntry(ctx, tenant, newID(), j.ID, "acct-p", "EUR", ledger.DirectionDebit, amount1); err != nil {
		t.Fatalf("InsertEntry debit: %v", err)
	}
	if err := tx.InsertEntry(ctx, tenant, newID(), j.ID, "acct-q", "EUR", ledger.DirectionCredit, amount2); err != nil {
		t.Fatalf("InsertEntry credit: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Read journal item directly from DynamoDB and assert embedded entries.
	var jItem journalItem
	found, err := s.getItem(ctx, journalPK(tenant, j.ID), &jItem)
	if err != nil {
		t.Fatalf("getItem journal: %v", err)
	}
	if !found {
		t.Fatal("getItem journal: not found after commit")
	}
	if len(jItem.Entries) != 2 {
		t.Fatalf("journal entries: want 2, got %d", len(jItem.Entries))
	}

	// Verify amounts round-trip correctly
	gotAmt0, err := decimal.NewFromString(jItem.Entries[0].Amount)
	if err != nil {
		t.Fatalf("parse entry[0].amount: %v", err)
	}
	gotAmt1, err := decimal.NewFromString(jItem.Entries[1].Amount)
	if err != nil {
		t.Fatalf("parse entry[1].amount: %v", err)
	}

	// Both entries are amount1/amount2 (75); order may vary
	sumGot := gotAmt0.Add(gotAmt1)
	sumWant := amount1.Add(amount2)
	if !sumGot.Equal(sumWant) {
		t.Errorf("entries amount sum: want %s, got %s", sumWant, sumGot)
	}

	// Verify directions present
	dirs := map[string]bool{}
	for _, e := range jItem.Entries {
		dirs[e.Direction] = true
	}
	if !dirs[string(ledger.DirectionDebit)] || !dirs[string(ledger.DirectionCredit)] {
		t.Errorf("entries directions incomplete: %v", dirs)
	}
}

// ---------------------------------------------------------------------------
// TestInsertFlowStepBeforeRunErrors
// ---------------------------------------------------------------------------

// TestInsertFlowStepBeforeRunErrors verifies the guard that prevents
// InsertFlowStep without a prior InsertFlowRun in the same tx.
func TestInsertFlowStepBeforeRunErrors(t *testing.T) {
	// Pure unit — no live store needed; buffer manipulation only.
	tx := &Tx{
		store:    &Store{},
		ctx:      context.Background(),
		puts:     make(map[string]*pendingPut),
		balances: make(map[string]*txBalance),
		flows:    make(map[string]*flowState),
		journals: make(map[string]*ledger.Journal),
		flowIdemp: make(map[string]string),
	}

	fs := ledger.FlowStep{
		ID: newID(), TenantID: "t1", FlowRunID: "unknown-flow",
		StepID: "s1", Status: ledger.StepCompleted, JournalID: newID(),
		CreatedAt: time.Now(),
	}
	err := tx.InsertFlowStep(context.Background(), fs)
	if err == nil {
		t.Fatal("InsertFlowStep before InsertFlowRun: want error, got nil")
	}
}

// ---------------------------------------------------------------------------
// TestInsertEntryBeforeJournalErrors
// ---------------------------------------------------------------------------

// TestInsertEntryBeforeJournalErrors verifies the guard that prevents
// InsertEntry without a prior InsertJournal in the same tx.
func TestInsertEntryBeforeJournalErrors(t *testing.T) {
	tx := &Tx{
		store:    &Store{},
		ctx:      context.Background(),
		puts:     make(map[string]*pendingPut),
		balances: make(map[string]*txBalance),
		flows:    make(map[string]*flowState),
		journals: make(map[string]*ledger.Journal),
		flowIdemp: make(map[string]string),
	}

	err := tx.InsertEntry(context.Background(), "t1", newID(), "unknown-journal",
		"acct-z", "USD", ledger.DirectionDebit, decimal.NewFromInt(10))
	if err == nil {
		t.Fatal("InsertEntry before InsertJournal: want error, got nil")
	}
}
