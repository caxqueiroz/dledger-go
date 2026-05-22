// internal/service/expire_reservation.go
package service

import (
	"context"
	"encoding/json"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ExpireReservation is called by the scheduler. It mirrors ReleaseReservation
// but marks the final status as EXPIRED. Not exposed as a public RPC.
func (s *Server) ExpireReservation(ctx context.Context, tenantID, reservationID string) error {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.LockReservation(ctx, tenantID, reservationID)
	if err != nil {
		return err
	}
	if res.Status.Closed() {
		// Another transition got here first.
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}
	amount := res.OutstandingAmount
	if amount.IsZero() {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	}

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       tenantID,
		FlowType:       "EXPIRE_RESERVATION",
		IdempotencyKey: res.ID + ":expire",
		SourceService:  "scheduler",
		ActorId:        "system",
		Steps: []*ledgerv1.Step{{
			StepId: "expire",
			Journal: &ledgerv1.Journal{
				EventId: res.ID + ":expire",
				Entries: []*ledgerv1.Entry{
					{AccountId: res.SourceAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: amount.String()},
					{AccountId: res.ReservedAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: amount.String()},
				},
			},
		}},
	}
	if _, err := s.executeFlowInTx(ctx, tx, flowReq); err != nil {
		return err
	}

	res.ReleasedAmount = res.ReleasedAmount.Add(amount)
	res.OutstandingAmount = res.OutstandingAmount.Sub(amount)
	res.Status = ledger.ReservationExpired
	res.UpdatedAt = s.Now()

	if err := tx.UpdateReservationAmounts(ctx, tenantID, res.ID,
		res.OutstandingAmount, res.CommittedAmount, res.ReleasedAmount, res.Status); err != nil {
		return err
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID,
		"amount":         amount.String(),
		"status":         "EXPIRED",
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType:      "RESERVATION_EXPIRED",
		IdempotencyKey: res.ID + ":expired",
		Payload:        payload,
		CreatedAt:      s.Now(),
	}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
