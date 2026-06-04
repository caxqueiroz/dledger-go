package dynamo

import (
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestAccountItemRoundTrip(t *testing.T) {
	a := ledger.Account{
		ID: "a1", TenantID: "t1", OwnerType: "user", OwnerID: "p42",
		AccountType: "cash_available", Currency: "BRL",
		NormalBalance: ledger.NormalDebit, AllowNegative: false,
		Status: ledger.AccountActive, CreatedAt: ts("2026-06-04T10:00:00.5Z"),
	}
	got, err := accountFromItem(accountToItem(a))
	if err != nil {
		t.Fatalf("accountFromItem: %v", err)
	}
	if !reflect.DeepEqual(*got, a) {
		t.Errorf("round trip = %+v, want %+v", *got, a)
	}
}

func TestJournalItemRoundTrip(t *testing.T) {
	j := ledger.Journal{
		ID: "j1", TenantID: "t1", FlowRunID: "f1", EventID: "e1",
		SourceService: "matcher", SourceType: "PLACE_ORDER", ActorID: "p42",
		Metadata: map[string]any{"k": "v"},
		Entries: []ledger.Entry{
			{AccountID: "a1", Currency: "BRL", Direction: ledger.DirectionDebit, Amount: decimal.RequireFromString("10.50")},
			{AccountID: "a2", Currency: "BRL", Direction: ledger.DirectionCredit, Amount: decimal.RequireFromString("10.50")},
		},
		CreatedAt: ts("2026-06-04T10:00:00Z"),
	}
	got, err := journalFromItem(journalToItem(j))
	if err != nil {
		t.Fatalf("journalFromItem: %v", err)
	}
	if got.ID != j.ID || got.EventID != j.EventID || len(got.Entries) != 2 ||
		!got.Entries[0].Amount.Equal(j.Entries[0].Amount) ||
		got.Entries[1].Direction != ledger.DirectionCredit ||
		!reflect.DeepEqual(got.Metadata, j.Metadata) {
		t.Errorf("round trip = %+v, want %+v", got, j)
	}
}

func TestFlowItemRoundTrip(t *testing.T) {
	f := ledger.FlowRun{
		ID: "f1", TenantID: "t1", FlowType: "DEPOSIT", IdempotencyKey: "k1",
		SourceService: "test", ActorID: "p1", Status: ledger.FlowCompleted,
		Metadata: map[string]any{}, CreatedAt: ts("2026-06-04T10:00:00Z"),
	}
	steps := []ledger.FlowStep{{
		ID: "s1", TenantID: "t1", FlowRunID: "f1", StepID: "step-1",
		Status: ledger.StepCompleted, JournalID: "j1", CreatedAt: ts("2026-06-04T10:00:01Z"),
	}}
	gotF, gotSteps, err := flowFromItem(flowToItem(f, steps))
	if err != nil {
		t.Fatalf("flowFromItem: %v", err)
	}
	if gotF.ID != f.ID || gotF.Status != ledger.FlowCompleted || gotF.IdempotencyKey != "k1" ||
		len(gotSteps) != 1 || gotSteps[0].StepID != "step-1" || gotSteps[0].JournalID != "j1" {
		t.Errorf("round trip = %+v steps=%+v", gotF, gotSteps)
	}
}

func TestFlowItemWithCompletedAt(t *testing.T) {
	completed := ts("2026-06-04T10:05:00Z")
	f := ledger.FlowRun{
		ID: "f2", TenantID: "t1", FlowType: "WITHDRAW", IdempotencyKey: "k2",
		SourceService: "test", ActorID: "p1", Status: ledger.FlowCompleted,
		Metadata: map[string]any{}, CreatedAt: ts("2026-06-04T10:00:00Z"),
		CompletedAt: &completed,
	}
	gotF, _, err := flowFromItem(flowToItem(f, nil))
	if err != nil {
		t.Fatalf("flowFromItem: %v", err)
	}
	if gotF.CompletedAt == nil || !gotF.CompletedAt.Equal(completed) {
		t.Errorf("CompletedAt = %v, want %v", gotF.CompletedAt, completed)
	}
	if gotF.FailedAt != nil {
		t.Errorf("FailedAt should be nil, got %v", gotF.FailedAt)
	}
}

func TestReservationItemRoundTrip(t *testing.T) {
	exp := ts("2026-06-04T11:00:00Z")
	r := ledger.Reservation{
		ID: "r1", TenantID: "t1", IdempotencyKey: "k1",
		SourceAccountID: "a1", ReservedAccountID: "a2", Currency: "BRL",
		OriginalAmount:    decimal.RequireFromString("40"),
		OutstandingAmount: decimal.RequireFromString("40"),
		CommittedAmount:   decimal.Zero,
		ReleasedAmount:    decimal.Zero,
		Status:            ledger.ReservationHeld,
		ExpiresAt:         &exp,
		FlowRunID:         "f1",
		Metadata:          map[string]any{},
		CreatedAt:         ts("2026-06-04T10:00:00Z"),
		UpdatedAt:         ts("2026-06-04T10:00:00Z"),
	}
	it := reservationToItem(r, 1)
	got, ver, err := reservationFromItem(it)
	if err != nil {
		t.Fatalf("reservationFromItem: %v", err)
	}
	if ver != 1 || got.ID != r.ID || !got.OutstandingAmount.Equal(r.OutstandingAmount) ||
		got.Status != ledger.ReservationHeld || got.ExpiresAt == nil || !got.ExpiresAt.Equal(exp) {
		t.Errorf("round trip = %+v ver=%d, want %+v ver=1", got, ver, r)
	}
	if it.GSI1PK != gsiReservationExpiry || it.GSI1SK == "" {
		t.Errorf("gsi keys = %q/%q, want RESEXP/<expiry sk>", it.GSI1PK, it.GSI1SK)
	}

	// Terminal status should not get GSI keys.
	r.Status = ledger.ReservationReleased
	it2 := reservationToItem(r, 2)
	if it2.GSI1PK != "" {
		t.Errorf("terminal reservation gsi1pk = %q, want empty", it2.GSI1PK)
	}
}

func TestOutboxItemRoundTrip(t *testing.T) {
	payload := []byte(`{"order_id":"o1"}`)
	e := repo.OutboxEvent{
		ID:             "ob1",
		TenantID:       "t1",
		AggregateID:    "o1",
		EventType:      "ORDER_PLACED",
		IdempotencyKey: "k1",
		Payload:        payload,
		CreatedAt:      ts("2026-06-04T10:00:00Z"),
	}
	it := outboxToItem(e)
	if it.PK != outboxPK("ob1") {
		t.Errorf("PK = %q, want %q", it.PK, outboxPK("ob1"))
	}
	if it.GSI1PK != gsiOutboxPending {
		t.Errorf("GSI1PK = %q, want %q", it.GSI1PK, gsiOutboxPending)
	}
	if it.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", it.Attempts)
	}
	if it.Payload != string(payload) {
		t.Errorf("Payload = %q, want %q", it.Payload, string(payload))
	}
}
