// run_reconciliation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/recon"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) RunReconciliation(ctx context.Context, req *connect.Request[ledgerv1.RunReconciliationRequest]) (*connect.Response[ledgerv1.RunReconciliationResponse], error) {
	r := req.Msg

	windowStart := s.Now()
	if r.GetWindowStart() != nil {
		windowStart = r.GetWindowStart().AsTime()
	}
	windowEnd := s.Now()
	if r.GetWindowEnd() != nil {
		windowEnd = r.GetWindowEnd().AsTime()
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

	// Idempotent replay
	existing, err := tx.GetReconBatchByIdempotency(ctx, r.GetTenantId(), r.GetIdempotencyKey())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if existing != nil {
		if existing.Status != ledger.BatchCompleted {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFlowConflict, "batch not completed: "+string(existing.Status)))
		}
		if err := tx.Commit(); err != nil {
			return nil, ToConnectError(err)
		}
		committed = true
		return connect.NewResponse(&ledgerv1.RunReconciliationResponse{Batch: reconBatchToProto(existing)}), nil
	}

	batch := ledger.ReconciliationBatch{
		ID: s.NewID(), TenantID: r.GetTenantId(),
		IdempotencyKey: r.GetIdempotencyKey(),
		Source:         r.GetSource(),
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		Status:         ledger.BatchRunning,
		StartedAt:      s.Now(),
		ActorID:        r.GetActorId(),
	}
	if err := tx.InsertReconBatch(ctx, batch); err != nil {
		return nil, ToConnectError(err)
	}

	// Matcher does the heavy lifting.
	res, err := recon.Run(ctx, tx, r.GetTenantId(), r.GetSource(), windowStart, windowEnd)
	if err != nil {
		return nil, ToConnectError(err)
	}

	// Persist discrepancy rows + emit outbox events.
	for i := range res.Discrepancies {
		d := &res.Discrepancies[i]
		d.ID = s.NewID()
		d.BatchID = batch.ID
		d.Status = ledger.DiscrepancyOpen
		d.CreatedAt = s.Now()
		if err := tx.InsertDiscrepancy(ctx, *d); err != nil {
			return nil, ToConnectError(err)
		}
		payload, _ := json.Marshal(map[string]any{
			"discrepancy_id": d.ID, "type": string(d.Type), "batch_id": batch.ID,
		})
		if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
			ID: s.NewID(), TenantID: batch.TenantID, AggregateID: d.ID,
			EventType:      "DISCREPANCY_OPENED",
			IdempotencyKey: d.ID + ":opened",
			Payload:        payload, CreatedAt: s.Now(),
		}); err != nil {
			return nil, ToConnectError(err)
		}
	}

	batch.Status = ledger.BatchCompleted
	batch.IngestedCount = res.Ingested
	batch.MatchedCount = res.Matched
	batch.MismatchedCount = res.Mismatched
	batch.MissingInLedgerCount = res.MissingInLedger
	batch.MissingInExternalCount = res.MissingInExternal
	batch.CompletedAt = s.Now()
	if err := tx.CompleteReconBatch(ctx, batch); err != nil {
		return nil, ToConnectError(err)
	}

	bp, _ := json.Marshal(map[string]any{
		"batch_id": batch.ID, "source": batch.Source,
		"matched": res.Matched, "missing_in_ledger": res.MissingInLedger,
		"missing_in_external": res.MissingInExternal, "mismatched": res.Mismatched,
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: batch.TenantID, AggregateID: batch.ID,
		EventType:      "RECON_BATCH_COMPLETED",
		IdempotencyKey: batch.ID + ":completed",
		Payload:        bp, CreatedAt: s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.RunReconciliationResponse{Batch: reconBatchToProto(&batch)}), nil
}
