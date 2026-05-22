// internal/service/release_reservation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ReleaseReservation(ctx context.Context, req *connect.Request[ledgerv1.ReleaseReservationRequest]) (*connect.Response[ledgerv1.ReleaseReservationResponse], error) {
	r := req.Msg
	amount, err := ledger.ParseAmount(r.GetAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds, err.Error()))
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

	res, err := tx.LockReservation(ctx, r.GetTenantId(), r.GetReservationId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if res.Status.Closed() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationClosed, "status="+string(res.Status)))
	}
	if amount.GreaterThan(res.OutstandingAmount) {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeReservationAmountExceeds,
			"amount="+amount.String()+" outstanding="+res.OutstandingAmount.String()))
	}

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "RELEASE_RESERVATION",
		IdempotencyKey: res.ID + ":release:" + r.GetIdempotencyKey(),
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{{
			StepId: "release",
			Journal: &ledgerv1.Journal{
				EventId: res.ID + ":release:" + r.GetIdempotencyKey(),
				Entries: []*ledgerv1.Entry{
					{AccountId: res.SourceAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: amount.String()},
					{AccountId: res.ReservedAccountID, Currency: res.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: amount.String()},
				},
			},
		}},
	}
	if _, err := s.executeFlowInTx(ctx, tx, flowReq); err != nil {
		return nil, ToConnectError(err)
	}

	res.OutstandingAmount = res.OutstandingAmount.Sub(amount)
	res.ReleasedAmount = res.ReleasedAmount.Add(amount)
	res.UpdatedAt = s.Now()
	switch {
	case res.OutstandingAmount.IsZero():
		res.Status = ledger.ReservationReleased
	default:
		res.Status = ledger.ReservationPartial
	}

	if err := tx.UpdateReservationAmounts(ctx, r.GetTenantId(), res.ID,
		res.OutstandingAmount, res.CommittedAmount, res.ReleasedAmount, res.Status); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID,
		"amount":         amount.String(),
		"outstanding":    res.OutstandingAmount.String(),
		"status":         string(res.Status),
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType:      "RESERVATION_RELEASED",
		IdempotencyKey: res.ID + ":released:" + r.GetIdempotencyKey(),
		Payload:        payload,
		CreatedAt:      s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.ReleaseReservationResponse{Reservation: reservationToProto(res)}), nil
}
