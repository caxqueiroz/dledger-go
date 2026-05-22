package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/structpb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ExecuteFlow is the public Connect handler. It opens a tx, runs the
// orchestrator body, and commits (or rolls back on error).
func (s *Server) ExecuteFlow(ctx context.Context, req *connect.Request[ledgerv1.ExecuteFlowRequest]) (*connect.Response[ledgerv1.ExecuteFlowResponse], error) {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	resp, err := s.executeFlowInTx(ctx, tx, req.Msg)
	if err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(resp), nil
}

// executeFlowInTx runs the orchestrator body against an existing tx. It does
// NOT begin, commit, or rollback. Callers own the tx lifecycle. Returns the
// raw domain error (NOT wrapped in connect.Error).
func (s *Server) executeFlowInTx(ctx context.Context, tx repo.Tx, r *ledgerv1.ExecuteFlowRequest) (*ledgerv1.ExecuteFlowResponse, error) {
	steps, err := stepsFromProto(r.GetSteps())
	if err != nil {
		return nil, err
	}

	// Idempotency check.
	existing, err := tx.GetFlowByIdempotency(ctx, r.GetTenantId(), r.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Status != ledger.FlowCompleted {
			return nil, ledger.NewDomainError(ledger.CodeFlowConflict, "flow not completed: "+string(existing.Status))
		}
		existingSteps, err := tx.GetFlowSteps(ctx, r.GetTenantId(), existing.ID)
		if err != nil {
			return nil, err
		}
		return flowRunToResponse(existing, existingSteps), nil
	}

	flowRunID := s.NewID()
	metaMap := structToMap(r.GetMetadata())

	if err := tx.InsertFlowRun(ctx, ledger.FlowRun{
		ID: flowRunID, TenantID: r.GetTenantId(), FlowType: r.GetFlowType(),
		IdempotencyKey: r.GetIdempotencyKey(), SourceService: r.GetSourceService(),
		ActorID: r.GetActorId(), Status: ledger.FlowRunning, Metadata: metaMap,
		CreatedAt: s.Now(),
	}); err != nil {
		return nil, err
	}

	// Collect unique (account, currency) keys deterministically.
	type key struct{ acct, ccy string }
	seen := map[key]bool{}
	var ordered []key
	for _, st := range steps {
		for _, e := range st.Journal.Entries {
			k := key{e.AccountID, e.Currency}
			if !seen[k] {
				seen[k] = true
				ordered = append(ordered, k)
			}
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].acct != ordered[j].acct {
			return ordered[i].acct < ordered[j].acct
		}
		return ordered[i].ccy < ordered[j].ccy
	})

	type balState struct {
		acct                        *ledger.Account
		postedDebits, postedCredits decimal.Decimal
	}
	state := map[key]*balState{}

	for _, k := range ordered {
		acc, err := tx.GetAccount(ctx, r.GetTenantId(), k.acct)
		if err != nil {
			return nil, err
		}
		if acc.Status != ledger.AccountActive {
			return nil, ledger.NewDomainError(ledger.CodeInvalidAccountStatus, acc.ID)
		}
		if acc.Currency != k.ccy {
			return nil, ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
				fmt.Sprintf("%s: account=%s req=%s", acc.ID, acc.Currency, k.ccy))
		}
		d, c, _, err := tx.LockBalance(ctx, r.GetTenantId(), k.acct, k.ccy)
		if err != nil {
			return nil, err
		}
		state[k] = &balState{acct: acc, postedDebits: d, postedCredits: c}
	}

	// Apply each step: journal, entries, in-memory balance accumulation, flow_step, outbox.
	stepResults := make([]ledger.FlowStep, 0, len(steps))
	for _, st := range steps {
		if err := st.Journal.Validate(); err != nil {
			return nil, ledger.NewDomainError(ledger.CodeUnbalancedJournal, err.Error())
		}
		journalID := s.NewID()
		st.Journal.ID = journalID
		st.Journal.TenantID = r.GetTenantId()
		st.Journal.FlowRunID = flowRunID
		st.Journal.SourceService = r.GetSourceService()
		st.Journal.SourceType = r.GetFlowType()
		st.Journal.ActorID = r.GetActorId()
		st.Journal.Metadata = metaMap
		st.Journal.CreatedAt = s.Now()

		if err := tx.InsertJournal(ctx, st.Journal); err != nil {
			return nil, err
		}
		for _, e := range st.Journal.Entries {
			entryID := s.NewID()
			if err := tx.InsertEntry(ctx, r.GetTenantId(), entryID, journalID, e.AccountID, e.Currency, e.Direction, e.Amount); err != nil {
				return nil, err
			}
			k := key{e.AccountID, e.Currency}
			bs := state[k]
			if e.Direction == ledger.DirectionDebit {
				bs.postedDebits = bs.postedDebits.Add(e.Amount)
			} else {
				bs.postedCredits = bs.postedCredits.Add(e.Amount)
			}
		}

		fs := ledger.FlowStep{
			ID: s.NewID(), TenantID: r.GetTenantId(), FlowRunID: flowRunID,
			StepID: st.StepID, Status: ledger.StepCompleted, JournalID: journalID,
			CreatedAt: s.Now(),
		}
		if err := tx.InsertFlowStep(ctx, fs); err != nil {
			return nil, err
		}
		stepResults = append(stepResults, fs)

		payload, _ := json.Marshal(map[string]any{
			"flow_type": r.GetFlowType(), "step_id": st.StepID, "journal_id": journalID,
		})
		if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
			ID: s.NewID(), TenantID: r.GetTenantId(), AggregateID: flowRunID,
			EventType:      r.GetFlowType() + "." + st.StepID,
			IdempotencyKey: flowRunID + ":" + st.StepID, Payload: payload, CreatedAt: s.Now(),
		}); err != nil {
			return nil, err
		}
	}

	// Verify non-overdraft accounts didn't end negative; persist balances.
	for _, k := range ordered {
		bs := state[k]
		if !bs.acct.AllowNegative {
			nb := ledger.NormalizedBalance(bs.acct.NormalBalance, bs.postedDebits, bs.postedCredits)
			if nb.IsNegative() {
				return nil, ledger.NewDomainError(ledger.CodeInsufficientFunds,
					fmt.Sprintf("account=%s currency=%s normalized=%s", bs.acct.ID, k.ccy, nb))
			}
		}
		if err := tx.UpdateBalance(ctx, r.GetTenantId(), k.acct, k.ccy, bs.postedDebits, bs.postedCredits); err != nil {
			return nil, err
		}
	}

	if err := tx.CompleteFlowRun(ctx, r.GetTenantId(), flowRunID); err != nil {
		return nil, err
	}

	return flowRunToResponse(&ledger.FlowRun{
		ID: flowRunID, TenantID: r.GetTenantId(), Status: ledger.FlowCompleted,
	}, stepResults), nil
}

func flowRunToResponse(f *ledger.FlowRun, steps []ledger.FlowStep) *ledgerv1.ExecuteFlowResponse {
	resp := &ledgerv1.ExecuteFlowResponse{
		FlowRunId: f.ID,
		Status:    flowStatusToProto(f.Status),
	}
	for _, s := range steps {
		resp.Steps = append(resp.Steps, &ledgerv1.FlowStepResult{
			StepId: s.StepID, Status: string(s.Status), JournalId: s.JournalID, ErrorCode: s.ErrorCode,
		})
	}
	return resp
}

func flowStatusToProto(s ledger.FlowStatus) ledgerv1.FlowStatus {
	switch s {
	case ledger.FlowCompleted:
		return ledgerv1.FlowStatus_FLOW_STATUS_COMPLETED
	case ledger.FlowFailed:
		return ledgerv1.FlowStatus_FLOW_STATUS_FAILED
	default:
		return ledgerv1.FlowStatus_FLOW_STATUS_RUNNING
	}
}

func stepsFromProto(in []*ledgerv1.Step) ([]ledger.StepInput, error) {
	out := make([]ledger.StepInput, 0, len(in))
	for _, s := range in {
		j := ledger.Journal{EventID: s.GetJournal().GetEventId()}
		for _, e := range s.GetJournal().GetEntries() {
			amt, err := ledger.ParseAmount(e.GetAmount())
			if err != nil {
				return nil, ledger.NewDomainError(ledger.CodeUnbalancedJournal, err.Error())
			}
			dir := ledger.DirectionDebit
			if e.GetDirection() == ledgerv1.Direction_DIRECTION_CREDIT {
				dir = ledger.DirectionCredit
			}
			j.Entries = append(j.Entries, ledger.Entry{
				AccountID: e.GetAccountId(), Currency: e.GetCurrency(), Direction: dir, Amount: amt,
			})
		}
		out = append(out, ledger.StepInput{StepID: s.GetStepId(), Journal: j})
	}
	return out, nil
}

func structToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	return s.AsMap()
}

func mapToStruct(m map[string]any) *structpb.Struct {
	if len(m) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}
