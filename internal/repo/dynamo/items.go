package dynamo

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

const timeFmt = time.RFC3339Nano

// fmtTime formats t as RFC3339Nano in UTC. Returns "" for zero time.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeFmt)
}

// fmtTimePtr formats a *time.Time; returns "" for nil or zero.
func fmtTimePtr(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeFmt)
}

// parseTime parses an RFC3339Nano string. Returns zero time for empty string.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(timeFmt, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parseTime %q: %w", s, err)
	}
	return t, nil
}

// parseTimePtr parses an RFC3339Nano string into a *time.Time. Returns nil for
// empty string.
func parseTimePtr(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(timeFmt, s)
	if err != nil {
		return nil, fmt.Errorf("parseTimePtr %q: %w", s, err)
	}
	return &t, nil
}

// parseDec parses a decimal string; field is used in error messages.
func parseDec(s, field string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Zero, nil
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero, fmt.Errorf("parseDec %s=%q: %w", field, s, err)
	}
	return d, nil
}

// marshalMeta serialises a metadata map to JSON. nil → "{}".
func marshalMeta(m map[string]any) string {
	if m == nil {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// unmarshalMeta deserialises a JSON metadata string. Empty/"{}" → empty map.
func unmarshalMeta(s string) map[string]any {
	m := map[string]any{}
	if s == "" || s == "{}" {
		return m
	}
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

// ---------------------------------------------------------------------------
// accountItem
// ---------------------------------------------------------------------------

type accountItem struct {
	PK            string `dynamodbav:"pk"`
	TenantID      string `dynamodbav:"tenant_id"`
	AccountID     string `dynamodbav:"account_id"`
	OwnerType     string `dynamodbav:"owner_type"`
	OwnerID       string `dynamodbav:"owner_id"`
	AccountType   string `dynamodbav:"account_type"`
	Currency      string `dynamodbav:"currency"`
	NormalBalance string `dynamodbav:"normal_balance"`
	AllowNegative bool   `dynamodbav:"allow_negative"`
	Status        string `dynamodbav:"status"`
	CreatedAt     string `dynamodbav:"created_at"`
}

func accountToItem(a ledger.Account) accountItem {
	return accountItem{
		PK:            accountPK(a.TenantID, a.ID),
		TenantID:      a.TenantID,
		AccountID:     a.ID,
		OwnerType:     a.OwnerType,
		OwnerID:       a.OwnerID,
		AccountType:   a.AccountType,
		Currency:      a.Currency,
		NormalBalance: string(a.NormalBalance),
		AllowNegative: a.AllowNegative,
		Status:        string(a.Status),
		CreatedAt:     fmtTime(a.CreatedAt),
	}
}

func accountFromItem(it accountItem) (*ledger.Account, error) {
	createdAt, err := parseTime(it.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("accountFromItem: %w", err)
	}
	return &ledger.Account{
		ID:            it.AccountID,
		TenantID:      it.TenantID,
		OwnerType:     it.OwnerType,
		OwnerID:       it.OwnerID,
		AccountType:   it.AccountType,
		Currency:      it.Currency,
		NormalBalance: ledger.NormalBalance(it.NormalBalance),
		AllowNegative: it.AllowNegative,
		Status:        ledger.AccountStatus(it.Status),
		CreatedAt:     createdAt,
	}, nil
}

// ---------------------------------------------------------------------------
// balanceItem  (struct only — used by later tasks)
// ---------------------------------------------------------------------------

type balanceItem struct {
	PK            string `dynamodbav:"pk"`
	TenantID      string `dynamodbav:"tenant_id"`
	AccountID     string `dynamodbav:"account_id"`
	Currency      string `dynamodbav:"currency"`
	PostedDebits  string `dynamodbav:"posted_debits"`
	PostedCredits string `dynamodbav:"posted_credits"`
	Version       int64  `dynamodbav:"version"`
	UpdatedAt     string `dynamodbav:"updated_at"`
}

// ---------------------------------------------------------------------------
// entryItem / journalItem
// ---------------------------------------------------------------------------

type entryItem struct {
	AccountID string `dynamodbav:"account_id"`
	Currency  string `dynamodbav:"currency"`
	Direction string `dynamodbav:"direction"`
	Amount    string `dynamodbav:"amount"`
}

type journalItem struct {
	PK            string      `dynamodbav:"pk"`
	TenantID      string      `dynamodbav:"tenant_id"`
	JournalID     string      `dynamodbav:"journal_id"`
	FlowRunID     string      `dynamodbav:"flow_run_id"`
	EventID       string      `dynamodbav:"event_id"`
	SourceService string      `dynamodbav:"source_service"`
	SourceType    string      `dynamodbav:"source_type"`
	ActorID       string      `dynamodbav:"actor_id"`
	Metadata      string      `dynamodbav:"metadata"`
	Entries       []entryItem `dynamodbav:"entries"`
	CreatedAt     string      `dynamodbav:"created_at"`
}

func journalToItem(j ledger.Journal) journalItem {
	entries := make([]entryItem, len(j.Entries))
	for i, e := range j.Entries {
		entries[i] = entryItem{
			AccountID: e.AccountID,
			Currency:  e.Currency,
			Direction: string(e.Direction),
			Amount:    e.Amount.String(),
		}
	}
	return journalItem{
		PK:            journalPK(j.TenantID, j.ID),
		TenantID:      j.TenantID,
		JournalID:     j.ID,
		FlowRunID:     j.FlowRunID,
		EventID:       j.EventID,
		SourceService: j.SourceService,
		SourceType:    j.SourceType,
		ActorID:       j.ActorID,
		Metadata:      marshalMeta(j.Metadata),
		Entries:       entries,
		CreatedAt:     fmtTime(j.CreatedAt),
	}
}

func journalFromItem(it journalItem) (*ledger.Journal, error) {
	createdAt, err := parseTime(it.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("journalFromItem: %w", err)
	}
	entries := make([]ledger.Entry, len(it.Entries))
	for i, e := range it.Entries {
		amt, err := parseDec(e.Amount, fmt.Sprintf("entries[%d].amount", i))
		if err != nil {
			return nil, fmt.Errorf("journalFromItem: %w", err)
		}
		entries[i] = ledger.Entry{
			AccountID: e.AccountID,
			Currency:  e.Currency,
			Direction: ledger.Direction(e.Direction),
			Amount:    amt,
		}
	}
	return &ledger.Journal{
		ID:            it.JournalID,
		TenantID:      it.TenantID,
		FlowRunID:     it.FlowRunID,
		EventID:       it.EventID,
		SourceService: it.SourceService,
		SourceType:    it.SourceType,
		ActorID:       it.ActorID,
		Metadata:      unmarshalMeta(it.Metadata),
		Entries:       entries,
		CreatedAt:     createdAt,
	}, nil
}

// ---------------------------------------------------------------------------
// flowStepItem / flowItem
// ---------------------------------------------------------------------------

type flowStepItem struct {
	ID        string `dynamodbav:"id"`
	TenantID  string `dynamodbav:"tenant_id"`
	FlowRunID string `dynamodbav:"flow_run_id"`
	StepID    string `dynamodbav:"step_id"`
	Status    string `dynamodbav:"status"`
	JournalID string `dynamodbav:"journal_id"`
	ErrorCode string `dynamodbav:"error_code"`
	CreatedAt string `dynamodbav:"created_at"`
}

type flowItem struct {
	PK             string         `dynamodbav:"pk"`
	TenantID       string         `dynamodbav:"tenant_id"`
	FlowRunID      string         `dynamodbav:"flow_run_id"`
	FlowType       string         `dynamodbav:"flow_type"`
	IdempotencyKey string         `dynamodbav:"idempotency_key"`
	SourceService  string         `dynamodbav:"source_service"`
	ActorID        string         `dynamodbav:"actor_id"`
	Status         string         `dynamodbav:"status"`
	Metadata       string         `dynamodbav:"metadata"`
	CreatedAt      string         `dynamodbav:"created_at"`
	CompletedAt    string         `dynamodbav:"completed_at"`
	FailedAt       string         `dynamodbav:"failed_at"`
	Steps          []flowStepItem `dynamodbav:"steps"`
}

// flowToItem converts a FlowRun and its steps to a single DynamoDB item.
// The FlowRun.Steps field is ignored; pass steps explicitly so the caller
// controls which step list is persisted.
func flowToItem(f ledger.FlowRun, steps []ledger.FlowStep) flowItem {
	stepItems := make([]flowStepItem, len(steps))
	for i, s := range steps {
		stepItems[i] = flowStepItem{
			ID:        s.ID,
			TenantID:  s.TenantID,
			FlowRunID: s.FlowRunID,
			StepID:    s.StepID,
			Status:    string(s.Status),
			JournalID: s.JournalID,
			ErrorCode: s.ErrorCode,
			CreatedAt: fmtTime(s.CreatedAt),
		}
	}
	return flowItem{
		PK:             flowPK(f.TenantID, f.ID),
		TenantID:       f.TenantID,
		FlowRunID:      f.ID,
		FlowType:       f.FlowType,
		IdempotencyKey: f.IdempotencyKey,
		SourceService:  f.SourceService,
		ActorID:        f.ActorID,
		Status:         string(f.Status),
		Metadata:       marshalMeta(f.Metadata),
		CreatedAt:      fmtTime(f.CreatedAt),
		CompletedAt:    fmtTimePtr(f.CompletedAt),
		FailedAt:       fmtTimePtr(f.FailedAt),
		Steps:          stepItems,
	}
}

func flowFromItem(it flowItem) (*ledger.FlowRun, []ledger.FlowStep, error) {
	createdAt, err := parseTime(it.CreatedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("flowFromItem created_at: %w", err)
	}
	completedAt, err := parseTimePtr(it.CompletedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("flowFromItem completed_at: %w", err)
	}
	failedAt, err := parseTimePtr(it.FailedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("flowFromItem failed_at: %w", err)
	}

	steps := make([]ledger.FlowStep, len(it.Steps))
	for i, s := range it.Steps {
		stepCreatedAt, err := parseTime(s.CreatedAt)
		if err != nil {
			return nil, nil, fmt.Errorf("flowFromItem steps[%d].created_at: %w", i, err)
		}
		steps[i] = ledger.FlowStep{
			ID:        s.ID,
			TenantID:  s.TenantID,
			FlowRunID: s.FlowRunID,
			StepID:    s.StepID,
			Status:    ledger.StepStatus(s.Status),
			JournalID: s.JournalID,
			ErrorCode: s.ErrorCode,
			CreatedAt: stepCreatedAt,
		}
	}

	f := &ledger.FlowRun{
		ID:             it.FlowRunID,
		TenantID:       it.TenantID,
		FlowType:       it.FlowType,
		IdempotencyKey: it.IdempotencyKey,
		SourceService:  it.SourceService,
		ActorID:        it.ActorID,
		Status:         ledger.FlowStatus(it.Status),
		Metadata:       unmarshalMeta(it.Metadata),
		CreatedAt:      createdAt,
		CompletedAt:    completedAt,
		FailedAt:       failedAt,
	}
	return f, steps, nil
}

// ---------------------------------------------------------------------------
// outboxItem
// ---------------------------------------------------------------------------

type outboxItem struct {
	PK             string `dynamodbav:"pk"`
	TenantID       string `dynamodbav:"tenant_id"`
	AggregateID    string `dynamodbav:"aggregate_id"`
	EventType      string `dynamodbav:"event_type"`
	IdempotencyKey string `dynamodbav:"idempotency_key"`
	Payload        string `dynamodbav:"payload"`
	Attempts       int    `dynamodbav:"attempts"`
	CreatedAt      string `dynamodbav:"created_at"`
	GSI1PK         string `dynamodbav:"gsi1pk,omitempty"`
	GSI1SK         string `dynamodbav:"gsi1sk,omitempty"`
}

func outboxToItem(e repo.OutboxEvent) outboxItem {
	createdAt := fmtTime(e.CreatedAt)
	return outboxItem{
		PK:             outboxPK(e.ID),
		TenantID:       e.TenantID,
		AggregateID:    e.AggregateID,
		EventType:      e.EventType,
		IdempotencyKey: e.IdempotencyKey,
		Payload:        string(e.Payload),
		Attempts:       0,
		CreatedAt:      createdAt,
		GSI1PK:         gsiOutboxPending,
		GSI1SK:         createdAt + "#" + e.ID,
	}
}

// ---------------------------------------------------------------------------
// reservationItem
// ---------------------------------------------------------------------------

type reservationItem struct {
	PK                string `dynamodbav:"pk"`
	TenantID          string `dynamodbav:"tenant_id"`
	ReservationID     string `dynamodbav:"reservation_id"`
	IdempotencyKey    string `dynamodbav:"idempotency_key"`
	SourceAccountID   string `dynamodbav:"source_account_id"`
	OwnerType         string `dynamodbav:"owner_type"`
	OwnerID           string `dynamodbav:"owner_id"`
	ReservedAccountID string `dynamodbav:"reserved_account_id"`
	Currency          string `dynamodbav:"currency"`
	OriginalAmount    string `dynamodbav:"original_amount"`
	OutstandingAmount string `dynamodbav:"outstanding_amount"`
	CommittedAmount   string `dynamodbav:"committed_amount"`
	ReleasedAmount    string `dynamodbav:"released_amount"`
	Status            string `dynamodbav:"status"`
	ExpiresAt         string `dynamodbav:"expires_at"`
	FlowRunID         string `dynamodbav:"flow_run_id"`
	Metadata          string `dynamodbav:"metadata"`
	CreatedAt         string `dynamodbav:"created_at"`
	UpdatedAt         string `dynamodbav:"updated_at"`
	Version           int64  `dynamodbav:"version"`
	GSI1PK            string `dynamodbav:"gsi1pk,omitempty"`
	GSI1SK            string `dynamodbav:"gsi1sk,omitempty"`
	// GSI2: account-scoped index for ListReservations by (tenant, ownerType, ownerID).
	// Set only while ACTIVE (HELD or PARTIAL) so terminal items fall off the index.
	GSI2PK string `dynamodbav:"gsi2pk,omitempty"`
	GSI2SK string `dynamodbav:"gsi2sk,omitempty"`
}

// reservationActive reports whether a reservation status is active (not terminal).
// Active statuses are HELD and PARTIAL.
func reservationActive(status ledger.ReservationStatus) bool {
	return status == ledger.ReservationHeld || status == ledger.ReservationPartial
}

// parseAccountOwner extracts (ownerType, ownerID) from an account ID of the
// form "<ownerType>:<ownerID>:<accountType>:<currency>". Returns ("", "") if
// the format is not recognised.
func parseAccountOwner(accountID string) (ownerType, ownerID string) {
	parts := strings.SplitN(accountID, ":", 4)
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func reservationToItem(r ledger.Reservation, version int64) reservationItem {
	expiresAt := fmtTimePtr(r.ExpiresAt)
	ownerType, ownerID := parseAccountOwner(r.SourceAccountID)
	it := reservationItem{
		PK:                reservationPK(r.TenantID, r.ID),
		TenantID:          r.TenantID,
		ReservationID:     r.ID,
		IdempotencyKey:    r.IdempotencyKey,
		SourceAccountID:   r.SourceAccountID,
		OwnerType:         ownerType,
		OwnerID:           ownerID,
		ReservedAccountID: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount.String(),
		OutstandingAmount: r.OutstandingAmount.String(),
		CommittedAmount:   r.CommittedAmount.String(),
		ReleasedAmount:    r.ReleasedAmount.String(),
		Status:            string(r.Status),
		ExpiresAt:         expiresAt,
		FlowRunID:         r.FlowRunID,
		Metadata:          marshalMeta(r.Metadata),
		CreatedAt:         fmtTime(r.CreatedAt),
		UpdatedAt:         fmtTime(r.UpdatedAt),
		Version:           version,
	}
	// GSI1: expiry index — only for active reservations with an expiry set.
	if reservationActive(r.Status) && expiresAt != "" {
		it.GSI1PK = gsiReservationExpiry
		it.GSI1SK = expiresAt + "#" + r.TenantID + "#" + r.ID
	}
	// GSI2: owner-scoped index — only for active reservations.
	// gsi2pk = "RESOWN#<tenant>#<ownerType>#<ownerID>"
	// gsi2sk = "<createdAt>#<id>"
	if reservationActive(r.Status) && ownerType != "" && ownerID != "" {
		it.GSI2PK = gsiResOwnerPK(r.TenantID, ownerType, ownerID)
		it.GSI2SK = fmtTime(r.CreatedAt) + "#" + r.ID
	}
	return it
}

func reservationFromItem(it reservationItem) (*ledger.Reservation, int64, error) {
	originalAmt, err := parseDec(it.OriginalAmount, "original_amount")
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	outstandingAmt, err := parseDec(it.OutstandingAmount, "outstanding_amount")
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	committedAmt, err := parseDec(it.CommittedAmount, "committed_amount")
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	releasedAmt, err := parseDec(it.ReleasedAmount, "released_amount")
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	expiresAt, err := parseTimePtr(it.ExpiresAt)
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	createdAt, err := parseTime(it.CreatedAt)
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	updatedAt, err := parseTime(it.UpdatedAt)
	if err != nil {
		return nil, 0, fmt.Errorf("reservationFromItem: %w", err)
	}
	r := &ledger.Reservation{
		ID:                it.ReservationID,
		TenantID:          it.TenantID,
		IdempotencyKey:    it.IdempotencyKey,
		SourceAccountID:   it.SourceAccountID,
		ReservedAccountID: it.ReservedAccountID,
		Currency:          it.Currency,
		OriginalAmount:    originalAmt,
		OutstandingAmount: outstandingAmt,
		CommittedAmount:   committedAmt,
		ReleasedAmount:    releasedAmt,
		Status:            ledger.ReservationStatus(it.Status),
		ExpiresAt:         expiresAt,
		FlowRunID:         it.FlowRunID,
		Metadata:          unmarshalMeta(it.Metadata),
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}
	return r, it.Version, nil
}
