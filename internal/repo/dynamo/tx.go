package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// maxTxItems is the hard upper bound on the number of Put operations that a
// single TransactWriteItems call may contain (DynamoDB limit: 100 items).
const maxTxItems = 100

// ---------------------------------------------------------------------------
// condKind: condition kind for optimistic-concurrency puts
// ---------------------------------------------------------------------------

type condKind int

const (
	condNone          condKind = iota // no condition
	condNotExists                     // attribute_not_exists(pk)
	condVersionEquals                 // version = :v
)

// ---------------------------------------------------------------------------
// pendingPut: a buffered write operation
// ---------------------------------------------------------------------------

type pendingPut struct {
	pk      string
	item    map[string]types.AttributeValue
	cond    condKind
	version int64 // meaningful only for condVersionEquals
}

// ---------------------------------------------------------------------------
// txBalance: in-memory overlay for a single balance row
// ---------------------------------------------------------------------------

type txBalance struct {
	tenantID  string
	accountID string
	currency  string
	debits    decimal.Decimal
	credits   decimal.Decimal
	// readVersion is the version we read from the store (0 if row did not exist).
	readVersion int64
	// existed is true when the balance row already existed in the store before
	// this transaction (i.e. the row was not brand-new in this tx).
	existed bool
}

// ---------------------------------------------------------------------------
// Tx: in-memory write buffer with optimistic-concurrency commit
// ---------------------------------------------------------------------------

// flowState is the in-memory overlay for a single flow run and its steps
// within this transaction.
type flowState struct {
	run   ledger.FlowRun
	steps []ledger.FlowStep
}

// resState is the in-memory overlay for a single reservation within this tx.
type resState struct {
	res         ledger.Reservation
	readVersion int64
	// existed is true when the reservation row was already in the store before
	// this transaction; false when InsertReservation created it fresh in-tx.
	existed bool
}

// Tx is a write-buffered, single-use transaction over DynamoDB. All writes
// are held in memory and atomically flushed via TransactWriteItems on Commit.
// Not safe for concurrent use.
type Tx struct {
	store *Store
	ctx   context.Context // captured at Begin; used for Commit/Rollback

	// puts holds the accumulated write operations keyed by pk.
	puts map[string]*pendingPut
	// order preserves insertion order so the TransactWriteItems slice is
	// deterministic and matches the design spec.
	order []string

	// balances is the in-memory overlay for balance rows touched in this tx.
	balances map[string]*txBalance

	// flows is the in-memory overlay for flow runs keyed by flow PK
	// (flowPK(tenantID, flowRunID)).
	flows map[string]*flowState

	// journals is the in-memory overlay for journals keyed by journal PK
	// (journalPK(tenantID, journalID)).
	journals map[string]*ledger.Journal

	// flowIdemp maps idempotency PK (flowIdempPK(tenantID, key)) to flow PK
	// (flowPK(tenantID, flowRunID)) for overlay lookups.
	flowIdemp map[string]string

	// reservations is the in-memory overlay for reservation rows touched in
	// this tx, keyed by reservationPK(tenantID, reservationID).
	reservations map[string]*resState

	done bool
}

// ---------------------------------------------------------------------------
// BeginFlowTx opens a new buffered transaction.
// ---------------------------------------------------------------------------

func (s *Store) BeginFlowTx(ctx context.Context) (repo.Tx, error) {
	return &Tx{
		store:        s,
		ctx:          ctx,
		puts:         make(map[string]*pendingPut),
		order:        nil,
		balances:     make(map[string]*txBalance),
		flows:        make(map[string]*flowState),
		journals:     make(map[string]*ledger.Journal),
		flowIdemp:    make(map[string]string),
		reservations: make(map[string]*resState),
	}, nil
}

// ---------------------------------------------------------------------------
// put: buffer a write operation
// ---------------------------------------------------------------------------

// put marshals in and adds it to the write buffer under pk.
// If pk was already buffered, the item is replaced but the ORIGINAL cond and
// version are preserved (so the condition that was established first wins).
func (t *Tx) put(pk string, in any, cond condKind, version int64) error {
	item, err := attributevalue.MarshalMap(in)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", pk, err)
	}
	if existing, ok := t.puts[pk]; ok {
		// Replace item but keep original cond + version
		existing.item = item
		return nil
	}
	t.puts[pk] = &pendingPut{pk: pk, item: item, cond: cond, version: version}
	t.order = append(t.order, pk)
	return nil
}

// ---------------------------------------------------------------------------
// loadBalance: overlay-cached balance read
// ---------------------------------------------------------------------------

// loadBalance returns the txBalance for (tenant, acct, ccy), reading from the
// store on first access and caching for subsequent calls within this tx.
func (t *Tx) loadBalance(ctx context.Context, tenantID, accountID, currency string) (*txBalance, error) {
	key := balancePK(tenantID, accountID, currency)
	if b, ok := t.balances[key]; ok {
		return b, nil
	}

	d, c, ver, err := t.store.GetBalance(ctx, tenantID, accountID, currency)
	if err != nil {
		return nil, err
	}

	// When version==0 and both amounts are zero, GetBalance returns the same
	// values for a missing row AND for a row that genuinely has 0/0/version=0
	// (which should not normally exist but must be handled defensively). Probe
	// the item directly to decide whether the row exists.
	existed := ver != 0 || !d.IsZero() || !c.IsZero()
	if !existed {
		var probe balanceItem
		found, probeErr := t.store.getItem(ctx, key, &probe)
		if probeErr != nil {
			return nil, probeErr
		}
		existed = found
	}

	b := &txBalance{
		tenantID:    tenantID,
		accountID:   accountID,
		currency:    currency,
		debits:      d,
		credits:     c,
		readVersion: ver,
		existed:     existed,
	}
	t.balances[key] = b
	return b, nil
}

// ---------------------------------------------------------------------------
// EnsureBalanceRow
// ---------------------------------------------------------------------------

// EnsureBalanceRow primes the in-memory overlay cache for the balance row so
// that a subsequent UpdateBalance within this transaction observes the correct
// baseline. If the row already exists in the store, existed is true and
// UpdateBalance will use condVersionEquals; if the row is brand-new, existed is
// false and UpdateBalance will use condNotExists.
func (t *Tx) EnsureBalanceRow(ctx context.Context, tenantID, accountID, currency string) error {
	_, err := t.loadBalance(ctx, tenantID, accountID, currency)
	return err
}

// ---------------------------------------------------------------------------
// LockBalance
// ---------------------------------------------------------------------------

// LockBalance returns the current (debits, credits, version) for the balance
// row. The row is cached so that a subsequent UpdateBalance within this tx
// uses the correct baseline version.
func (t *Tx) LockBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	b, err := t.loadBalance(ctx, tenantID, accountID, currency)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	return b.debits, b.credits, b.readVersion, nil
}

// ---------------------------------------------------------------------------
// UpdateBalance
// ---------------------------------------------------------------------------

// UpdateBalance overwrites the balance amounts in the overlay and buffers a
// full balanceItem Put guarded by the appropriate OCC condition.
func (t *Tx) UpdateBalance(ctx context.Context, tenantID, accountID, currency string, postedDebits, postedCredits decimal.Decimal) error {
	b, err := t.loadBalance(ctx, tenantID, accountID, currency)
	if err != nil {
		return err
	}

	// Mutate overlay
	b.debits = postedDebits
	b.credits = postedCredits

	newVersion := b.readVersion + 1
	it := balanceItem{
		PK:            balancePK(tenantID, accountID, currency),
		TenantID:      tenantID,
		AccountID:     accountID,
		Currency:      currency,
		PostedDebits:  postedDebits.String(),
		PostedCredits: postedCredits.String(),
		Version:       newVersion,
		UpdatedAt:     fmtTime(t.store.now()),
	}

	cond := condVersionEquals
	if !b.existed {
		cond = condNotExists
	}

	return t.put(it.PK, it, cond, b.readVersion)
}

// ---------------------------------------------------------------------------
// GetAccount — overlay then store
// ---------------------------------------------------------------------------

// GetAccount fetches the account from the in-transaction overlay first, then
// falls through to the underlying store. Matches the sqlite not-found contract:
// returns CodeAccountNotFound DomainError when absent.
func (t *Tx) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	pk := accountPK(tenantID, accountID)
	if pp, ok := t.puts[pk]; ok {
		// Decode the buffered item back into an accountItem
		var it accountItem
		if err := attributevalue.UnmarshalMap(pp.item, &it); err != nil {
			return nil, fmt.Errorf("unmarshal buffered account %s: %w", pk, err)
		}
		return accountFromItem(it)
	}
	return t.store.GetAccount(ctx, tenantID, accountID)
}

// ---------------------------------------------------------------------------
// InsertAccount
// ---------------------------------------------------------------------------

// InsertAccount buffers a condNotExists Put for the account item AND for the
// uniqueness marker (tenant + ownerType + ownerID + accountType + currency).
func (t *Tx) InsertAccount(ctx context.Context, a ledger.Account) error {
	it := accountToItem(a)
	if err := t.put(it.PK, it, condNotExists, 0); err != nil {
		return err
	}
	// Uniqueness marker: only pk field — prevents a second account with the
	// same (tenantID, ownerType, ownerID, accountType, currency) tuple.
	uniqPK := accountUniqPK(a.TenantID, a.OwnerType, a.OwnerID, a.AccountType, a.Currency)
	uniqItem := map[string]string{
		"pk":         uniqPK,
		"account_id": a.ID,
	}
	return t.put(uniqPK, uniqItem, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

// Commit atomically flushes all buffered writes via TransactWriteItems.
//
// Conflict classification uses the typed positional CancellationReasons slice
// returned by DynamoDB (and ExtendDB). Index i of CancellationReasons maps to
// t.order[i] (i.e. t.puts[t.order[i]]). The FIRST ConditionalCheckFailed
// reason is classified as:
//
//   - condVersionEquals → retryable: CodeSerializationRetryExhausted (stale read)
//   - condNotExists, pk prefix "BAL#" → retryable: CodeSerializationRetryExhausted
//     (fresh-balance race: another tx just created the same new balance row)
//   - condNotExists, any other pk prefix → non-retryable duplicate:
//     "ACC#"/"ACCU#" prefixes → CodeFlowConflict (no dedicated account-duplicate
//     code exists; CodeFlowConflict maps to connect.CodeAborted at the service layer)
//     all other prefixes ("FIDEMP#", "EVT#", "RIDEMP#", etc.) → CodeFlowConflict
//
// ExtendDB (the local DynamoDB-compatible test server used in CI) populates
// CancellationReasons identically to AWS DynamoDB — empirically verified; the
// string-matching fallbacks have been removed.
func (t *Tx) Commit() error {
	if t.done {
		return errors.New("dynamo: tx already finished")
	}
	t.done = true

	n := len(t.puts)
	if n == 0 {
		return nil
	}

	if n > maxTxItems {
		return ledger.NewDomainError(
			ledger.CodeFlowTooLarge,
			fmt.Sprintf("flow writes %d items, limit %d", n, maxTxItems),
		)
	}

	// Build TransactWriteItems in insertion order
	items := make([]types.TransactWriteItem, 0, n)
	for _, pk := range t.order {
		pp := t.puts[pk]
		put := &types.Put{
			TableName: aws.String(t.store.table),
			Item:      pp.item,
		}
		switch pp.cond {
		case condNotExists:
			put.ConditionExpression = aws.String("attribute_not_exists(pk)")
		case condVersionEquals:
			put.ConditionExpression = aws.String("version = :v")
			put.ExpressionAttributeValues = map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", pp.version)},
			}
		}
		items = append(items, types.TransactWriteItem{Put: put})
	}

	_, err := t.store.db.TransactWriteItems(t.ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err == nil {
		return nil
	}

	// Map TransactionCanceledException → classified domain error.
	// CancellationReasons is positional: index i corresponds to t.order[i].
	var tce *types.TransactionCanceledException
	if errors.As(err, &tce) {
		for i, reason := range tce.CancellationReasons {
			if reason.Code == nil || *reason.Code != "ConditionalCheckFailed" {
				continue
			}
			// Identify the failing item.
			pk := ""
			if i < len(t.order) {
				pk = t.order[i]
				if pp, ok := t.puts[pk]; ok {
					switch pp.cond {
					case condVersionEquals:
						// Stale read: another writer committed a newer version.
						return ledger.NewDomainError(
							ledger.CodeSerializationRetryExhausted,
							fmt.Sprintf("dynamodb version conflict on %s", pk),
						)
					case condNotExists:
						if strings.HasPrefix(pk, "BAL#") {
							// Fresh-balance race: two concurrent txs both tried to
							// create the same brand-new balance row.
							return ledger.NewDomainError(
								ledger.CodeSerializationRetryExhausted,
								fmt.Sprintf("dynamodb fresh-balance race on %s", pk),
							)
						}
						// Any other condNotExists failure is a true duplicate:
						// ACC#, ACCU#, FIDEMP#, EVT#, RIDEMP#, etc.
						return ledger.NewDomainError(
							ledger.CodeFlowConflict,
							fmt.Sprintf("dynamodb duplicate item on %s", pk),
						)
					}
				}
			}
			// Fallback: index out of range or cond unknown — treat as conflict.
			return ledger.NewDomainError(
				ledger.CodeFlowConflict,
				fmt.Sprintf("dynamodb conditional check failed on %s", pk),
			)
		}
	}

	return fmt.Errorf("transact write: %w", err)
}

// ---------------------------------------------------------------------------
// Rollback
// ---------------------------------------------------------------------------

// Rollback marks the transaction as done and discards all buffered writes.
// Always returns nil.
func (t *Tx) Rollback() error {
	t.done = true
	t.puts = nil
	t.order = nil
	t.balances = nil
	t.flows = nil
	t.journals = nil
	t.flowIdemp = nil
	t.reservations = nil
	return nil
}

// ---------------------------------------------------------------------------
// Flow run overlay helpers
// ---------------------------------------------------------------------------

// putFlow re-serialises the flowState into the write buffer under the flow PK.
// On first call the put is registered with condNotExists; subsequent calls
// replace the item bytes but preserve the original condition (coalescing).
func (t *Tx) putFlow(pk string, fs *flowState) error {
	item := flowToItem(fs.run, fs.steps)
	return t.put(pk, item, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// InsertFlowRun
// ---------------------------------------------------------------------------

// InsertFlowRun records the flow run in the overlay and buffers:
//   - the flow item (condNotExists)
//   - the FIDEMP# idempotency marker (condNotExists)
func (t *Tx) InsertFlowRun(_ context.Context, f ledger.FlowRun) error {
	pk := flowPK(f.TenantID, f.ID)

	// Record in overlay
	t.flows[pk] = &flowState{run: f}

	// Buffer the flow item (condNotExists)
	if err := t.putFlow(pk, t.flows[pk]); err != nil {
		return err
	}

	// Buffer FIDEMP# idempotency marker (condNotExists)
	idempPK := flowIdempPK(f.TenantID, f.IdempotencyKey)
	idempItem := map[string]string{
		"pk":          idempPK,
		"flow_run_id": f.ID,
	}
	if err := t.put(idempPK, idempItem, condNotExists, 0); err != nil {
		return err
	}

	// Record in flowIdemp overlay so GetFlowByIdempotency can find it without
	// a DynamoDB round-trip while the tx is uncommitted.
	t.flowIdemp[idempPK] = pk

	return nil
}

// ---------------------------------------------------------------------------
// InsertFlowStep
// ---------------------------------------------------------------------------

// InsertFlowStep appends a step to the overlay flowState and re-puts the flow
// item. Returns an error if InsertFlowRun was not called first in this tx.
func (t *Tx) InsertFlowStep(_ context.Context, s ledger.FlowStep) error {
	pk := flowPK(s.TenantID, s.FlowRunID)
	fs, ok := t.flows[pk]
	if !ok {
		return fmt.Errorf("dynamo: InsertFlowStep before InsertFlowRun in tx (flow %s)", s.FlowRunID)
	}
	fs.steps = append(fs.steps, s)
	return t.putFlow(pk, fs)
}

// ---------------------------------------------------------------------------
// CompleteFlowRun
// ---------------------------------------------------------------------------

// CompleteFlowRun sets the flow status to COMPLETED and stamps CompletedAt.
// Returns an error if InsertFlowRun was not called first in this tx.
func (t *Tx) CompleteFlowRun(_ context.Context, tenantID, flowRunID string) error {
	pk := flowPK(tenantID, flowRunID)
	fs, ok := t.flows[pk]
	if !ok {
		return fmt.Errorf("dynamo: CompleteFlowRun before InsertFlowRun in tx (flow %s)", flowRunID)
	}
	now := t.store.now()
	fs.run.Status = ledger.FlowCompleted
	fs.run.CompletedAt = &now
	return t.putFlow(pk, fs)
}

// ---------------------------------------------------------------------------
// GetFlowByIdempotency
// ---------------------------------------------------------------------------

// GetFlowByIdempotency checks the tx overlay first, then falls through to
// DynamoDB. Returns (nil, nil) when not found — matching the sqlite contract.
func (t *Tx) GetFlowByIdempotency(ctx context.Context, tenantID, key string) (*ledger.FlowRun, error) {
	idempPK := flowIdempPK(tenantID, key)

	// Overlay: check flowIdemp mapping first
	if flowPKVal, ok := t.flowIdemp[idempPK]; ok {
		if fs, ok2 := t.flows[flowPKVal]; ok2 {
			// Return a copy with a copied steps slice to prevent caller mutation
			// from affecting the overlay.
			runCopy := fs.run
			stepsCopy := make([]ledger.FlowStep, len(fs.steps))
			copy(stepsCopy, fs.steps)
			runCopy.Steps = stepsCopy
			return &runCopy, nil
		}
	}

	// Fall through to DynamoDB: read FIDEMP# marker first
	type fidempItem struct {
		PK        string `dynamodbav:"pk"`
		FlowRunID string `dynamodbav:"flow_run_id"`
	}
	var marker fidempItem
	found, err := t.store.getItem(ctx, idempPK, &marker)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	// Fetch the full flow item
	return t.store.GetFlow(ctx, tenantID, marker.FlowRunID)
}

// ---------------------------------------------------------------------------
// GetFlowSteps
// ---------------------------------------------------------------------------

// GetFlowSteps returns the steps for the given flow. The overlay is checked
// first; if the flow is not buffered in this tx, the flow item is read from
// DynamoDB. Returns (nil, nil) when not found.
func (t *Tx) GetFlowSteps(ctx context.Context, tenantID, flowRunID string) ([]ledger.FlowStep, error) {
	pk := flowPK(tenantID, flowRunID)

	// Overlay
	if fs, ok := t.flows[pk]; ok {
		out := make([]ledger.FlowStep, len(fs.steps))
		copy(out, fs.steps)
		return out, nil
	}

	// Fall through to DynamoDB
	f, err := t.store.GetFlow(ctx, tenantID, flowRunID)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, nil
	}
	return f.Steps, nil
}

// ---------------------------------------------------------------------------
// InsertJournal
// ---------------------------------------------------------------------------

// InsertJournal records the journal (without entries) in the overlay and
// buffers:
//   - the JRN# journal item (condNotExists) — entry list starts empty here;
//     InsertEntry accumulates into the overlay and re-puts the item.
//   - the EVT# event uniqueness marker (condNotExists).
func (t *Tx) InsertJournal(_ context.Context, j ledger.Journal) error {
	// Store in overlay with entries stripped — entries accumulate via InsertEntry.
	jCopy := j
	jCopy.Entries = nil
	jPK := journalPK(j.TenantID, j.ID)
	t.journals[jPK] = &jCopy

	// Buffer journal item (condNotExists)
	item := journalToItem(jCopy)
	if err := t.put(jPK, item, condNotExists, 0); err != nil {
		return err
	}

	// Buffer EVT# uniqueness marker (condNotExists)
	evtPK := eventUniqPK(j.EventID)
	evtItem := map[string]string{
		"pk":         evtPK,
		"journal_id": j.ID,
	}
	return t.put(evtPK, evtItem, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// InsertEntry
// ---------------------------------------------------------------------------

// InsertEntry appends an entry to the overlaid journal and re-puts the journal
// item. The entryID parameter is accepted for interface compatibility but is
// not stored — ledger.Entry has no ID field.
func (t *Tx) InsertEntry(_ context.Context, tenantID, _ /* entryID — Entry has no ID field */, journalID, accountID, currency string, direction ledger.Direction, amount decimal.Decimal) error {
	jPK := journalPK(tenantID, journalID)
	jPtr, ok := t.journals[jPK]
	if !ok {
		return fmt.Errorf("dynamo: InsertEntry before InsertJournal in tx (journal %s)", journalID)
	}

	jPtr.Entries = append(jPtr.Entries, ledger.Entry{
		AccountID: accountID,
		Currency:  currency,
		Direction: direction,
		Amount:    amount,
	})

	// Re-put the journal item (coalesce keeps condNotExists from first put).
	item := journalToItem(*jPtr)
	return t.put(jPK, item, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// InsertOutbox
// ---------------------------------------------------------------------------

// InsertOutbox buffers the outbox event item (condNotExists).
func (t *Tx) InsertOutbox(_ context.Context, e repo.OutboxEvent) error {
	item := outboxToItem(e)
	return t.put(item.PK, item, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// InsertReservation
// ---------------------------------------------------------------------------

// InsertReservation buffers:
//   - the RES# reservation item (condNotExists, version 1)
//   - the RIDEMP# idempotency marker (condNotExists)
//
// It also primes the overlay so that UpdateReservationAmounts can be called
// without a prior LockReservation.
func (t *Tx) InsertReservation(_ context.Context, r ledger.Reservation) error {
	pk := reservationPK(r.TenantID, r.ID)

	// Prime overlay (existed=false → fresh insert)
	rs := &resState{res: r, readVersion: 0, existed: false}
	t.reservations[pk] = rs

	// Buffer RES# item (condNotExists)
	item := reservationToItem(r, 1)
	if err := t.put(pk, item, condNotExists, 0); err != nil {
		return err
	}

	// Buffer RIDEMP# idempotency marker (condNotExists)
	idempPK := resIdempPK(r.TenantID, r.IdempotencyKey)
	idempItem := map[string]string{
		"pk":             idempPK,
		"reservation_id": r.ID,
	}
	return t.put(idempPK, idempItem, condNotExists, 0)
}

// ---------------------------------------------------------------------------
// LockReservation
// ---------------------------------------------------------------------------

// LockReservation returns the reservation for the given ID. The overlay is
// checked first (copy returned); otherwise the item is read from DynamoDB and
// cached. Returns CodeReservationNotFound when absent — matching the sqlite
// contract exactly.
func (t *Tx) LockReservation(ctx context.Context, tenantID, reservationID string) (*ledger.Reservation, error) {
	pk := reservationPK(tenantID, reservationID)

	// Overlay hit: return a copy to prevent caller mutation.
	if rs, ok := t.reservations[pk]; ok {
		cp := rs.res
		return &cp, nil
	}

	// Read from DynamoDB.
	var it reservationItem
	found, err := t.store.getItem(ctx, pk, &it)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ledger.NewDomainError(ledger.CodeReservationNotFound, reservationID)
	}
	r, ver, err := reservationFromItem(it)
	if err != nil {
		return nil, err
	}

	// Cache in overlay.
	t.reservations[pk] = &resState{res: *r, readVersion: ver, existed: true}

	cp := *r
	return &cp, nil
}

// ---------------------------------------------------------------------------
// GetReservationByIdempotency
// ---------------------------------------------------------------------------

// GetReservationByIdempotency looks up a reservation by idempotency key.
// Checks the overlay first (iterates; maps are small), then falls back to the
// RIDEMP# marker in DynamoDB. Returns (nil, nil) when not found — matching the
// sqlite contract.
func (t *Tx) GetReservationByIdempotency(ctx context.Context, tenantID, key string) (*ledger.Reservation, error) {
	// Overlay: iterate for matching IdempotencyKey.
	for _, rs := range t.reservations {
		if rs.res.TenantID == tenantID && rs.res.IdempotencyKey == key {
			cp := rs.res
			return &cp, nil
		}
	}

	// Fall through to DynamoDB: read RIDEMP# marker first.
	idempPK := resIdempPK(tenantID, key)
	type ridempItem struct {
		PK            string `dynamodbav:"pk"`
		ReservationID string `dynamodbav:"reservation_id"`
	}
	var marker ridempItem
	found, err := t.store.getItem(ctx, idempPK, &marker)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	// Use LockReservation to read + cache the full reservation item.
	return t.LockReservation(ctx, tenantID, marker.ReservationID)
}

// ---------------------------------------------------------------------------
// UpdateReservationAmounts
// ---------------------------------------------------------------------------

// UpdateReservationAmounts mutates the overlay reservation and buffers an
// updated Put. Requires that LockReservation (or InsertReservation) was called
// first in this tx; returns an error otherwise.
//
// Condition used:
//   - existed=true  → condVersionEquals(readVersion) — stale-read protection
//   - existed=false → condNotExists (coalesced: original condition is kept)
func (t *Tx) UpdateReservationAmounts(_ context.Context, tenantID, reservationID string, outstanding, committed, released decimal.Decimal, status ledger.ReservationStatus) error {
	pk := reservationPK(tenantID, reservationID)
	rs, ok := t.reservations[pk]
	if !ok {
		return fmt.Errorf("dynamo: UpdateReservationAmounts before LockReservation in tx (reservation %s)", reservationID)
	}

	// Mutate overlay copy.
	rs.res.OutstandingAmount = outstanding
	rs.res.CommittedAmount = committed
	rs.res.ReleasedAmount = released
	rs.res.Status = status
	rs.res.UpdatedAt = t.store.now()

	// Buffer updated item; coalescing in put() preserves the original condition
	// from InsertReservation (condNotExists) or uses condVersionEquals for
	// a row that already existed in the store.
	if rs.existed {
		item := reservationToItem(rs.res, rs.readVersion+1)
		return t.put(pk, item, condVersionEquals, rs.readVersion)
	}
	// Fresh-in-tx: re-put with condNotExists; coalescing keeps original cond.
	item := reservationToItem(rs.res, 1)
	return t.put(pk, item, condNotExists, 0)
}

func (t *Tx) GetReconBatchByIdempotency(_ context.Context, _, _ string) (*ledger.ReconciliationBatch, error) {
	return nil, errUnsupported("Tx.GetReconBatchByIdempotency")
}

func (t *Tx) InsertReconBatch(_ context.Context, _ ledger.ReconciliationBatch) error {
	return errUnsupported("Tx.InsertReconBatch")
}

func (t *Tx) CompleteReconBatch(_ context.Context, _ ledger.ReconciliationBatch) error {
	return errUnsupported("Tx.CompleteReconBatch")
}

func (t *Tx) ListExternalRecordsForRecon(_ context.Context, _, _ string, _, _ time.Time) ([]ledger.ExternalRecord, error) {
	return nil, errUnsupported("Tx.ListExternalRecordsForRecon")
}

func (t *Tx) ListJournalsForRecon(_ context.Context, _, _ string, _, _ time.Time) ([]ledger.Journal, error) {
	return nil, errUnsupported("Tx.ListJournalsForRecon")
}

func (t *Tx) UpdateExternalRecordMatch(_ context.Context, _, _ string, _ ledger.ExternalRecordStatus, _ string) error {
	return errUnsupported("Tx.UpdateExternalRecordMatch")
}

func (t *Tx) SumJournalEntries(_ context.Context, _, _, _, _ string) (decimal.Decimal, decimal.Decimal, error) {
	return decimal.Zero, decimal.Zero, errUnsupported("Tx.SumJournalEntries")
}

func (t *Tx) InsertDiscrepancy(_ context.Context, _ ledger.Discrepancy) error {
	return errUnsupported("Tx.InsertDiscrepancy")
}

func (t *Tx) LockDiscrepancy(_ context.Context, _, _ string) (*ledger.Discrepancy, error) {
	return nil, errUnsupported("Tx.LockDiscrepancy")
}

func (t *Tx) ResolveDiscrepancyRow(_ context.Context, _ ledger.Discrepancy) error {
	return errUnsupported("Tx.ResolveDiscrepancyRow")
}
