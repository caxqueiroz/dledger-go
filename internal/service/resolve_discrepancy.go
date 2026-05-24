// resolve_discrepancy.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ResolveDiscrepancy(ctx context.Context, req *connect.Request[ledgerv1.ResolveDiscrepancyRequest]) (*connect.Response[ledgerv1.ResolveDiscrepancyResponse], error) {
	r := req.Msg

	resolution := r.GetResolution()
	if resolution != "RESOLVED" && resolution != "IGNORED" {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch, "resolution must be RESOLVED or IGNORED"))
	}

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

	d, err := tx.LockDiscrepancy(ctx, r.GetTenantId(), r.GetDiscrepancyId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if d.Status.Closed() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeDiscrepancyClosed, "status="+string(d.Status)))
	}

	var resolutionJournalID string
	if resolution == "RESOLVED" && r.GetAdjustment() != nil {
		adj := r.GetAdjustment()
		// Derive a deterministic idempotency key for the adjustment flow.
		adj.IdempotencyKey = d.ID + ":resolve:" + r.GetIdempotencyKey()
		flowResp, ferr := s.executeFlowInTx(ctx, tx, adj)
		if ferr != nil {
			return nil, ToConnectError(ferr)
		}
		if len(flowResp.GetSteps()) > 0 {
			resolutionJournalID = flowResp.GetSteps()[0].GetJournalId()
		}
	}

	d.Status = ledger.DiscrepancyStatus(resolution)
	d.ResolutionJournalID = resolutionJournalID
	d.ResolutionNote = r.GetNote()
	d.ResolvedBy = r.GetActorId()
	d.ResolvedAt = s.Now()
	if err := tx.ResolveDiscrepancyRow(ctx, *d); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"discrepancy_id":        d.ID,
		"status":                string(d.Status),
		"resolution_journal_id": d.ResolutionJournalID,
	})
	eventType := "DISCREPANCY_RESOLVED"
	if d.Status == ledger.DiscrepancyIgnored {
		eventType = "DISCREPANCY_IGNORED"
	}
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: d.TenantID, AggregateID: d.ID,
		EventType:      eventType,
		IdempotencyKey: d.ID + ":" + string(d.Status) + ":" + r.GetIdempotencyKey(),
		Payload:        payload, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.ResolveDiscrepancyResponse{Discrepancy: discrepancyToProto(d)}), nil
}
