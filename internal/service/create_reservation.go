// internal/service/create_reservation.go
package service

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) CreateReservation(ctx context.Context, req *connect.Request[ledgerv1.CreateReservationRequest]) (*connect.Response[ledgerv1.CreateReservationResponse], error) {
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

	// Idempotent replay.
	existing, err := tx.GetReservationByIdempotency(ctx, r.GetTenantId(), r.GetIdempotencyKey())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if existing != nil {
		if err := tx.Commit(); err != nil {
			return nil, ToConnectError(err)
		}
		committed = true
		return connect.NewResponse(&ledgerv1.CreateReservationResponse{Reservation: reservationToProto(existing)}), nil
	}

	// Inner flow: move funds source -> reserved.
	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "CREATE_RESERVATION",
		IdempotencyKey: r.GetIdempotencyKey() + ":create",
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Steps: []*ledgerv1.Step{{
			StepId: "reserve",
			Journal: &ledgerv1.Journal{
				EventId: r.GetIdempotencyKey() + ":reserve",
				Entries: []*ledgerv1.Entry{
					{AccountId: r.GetReservedAccountId(), Currency: r.GetCurrency(), Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: r.GetAmount()},
					{AccountId: r.GetSourceAccountId(), Currency: r.GetCurrency(), Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: r.GetAmount()},
				},
			},
		}},
	}
	flowResp, err := s.executeFlowInTx(ctx, tx, flowReq)
	if err != nil {
		return nil, ToConnectError(err)
	}

	res := ledger.Reservation{
		ID:                s.NewID(),
		TenantID:          r.GetTenantId(),
		IdempotencyKey:    r.GetIdempotencyKey(),
		SourceAccountID:   r.GetSourceAccountId(),
		ReservedAccountID: r.GetReservedAccountId(),
		Currency:          r.GetCurrency(),
		OriginalAmount:    amount,
		OutstandingAmount: amount,
		CommittedAmount:   decimal.Zero,
		ReleasedAmount:    decimal.Zero,
		Status:            ledger.ReservationHeld,
		FlowRunID:         flowResp.GetFlowRunId(),
		Metadata:          structToMap(r.GetMetadata()),
		CreatedAt:         s.Now(),
		UpdatedAt:         s.Now(),
	}
	if r.GetExpiresAt() != nil {
		t := r.GetExpiresAt().AsTime()
		res.ExpiresAt = &t
	}
	if err := tx.InsertReservation(ctx, res); err != nil {
		return nil, ToConnectError(err)
	}

	payload, _ := json.Marshal(map[string]any{
		"reservation_id": res.ID,
		"amount":         amount.String(),
		"currency":       res.Currency,
	})
	if err := tx.InsertOutbox(ctx, repo.OutboxEvent{
		ID: s.NewID(), TenantID: res.TenantID, AggregateID: res.ID,
		EventType:      "RESERVATION_CREATED",
		IdempotencyKey: res.ID + ":created",
		Payload:        payload,
		CreatedAt:      s.Now(),
	}); err != nil {
		return nil, ToConnectError(err)
	}

	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true
	return connect.NewResponse(&ledgerv1.CreateReservationResponse{Reservation: reservationToProto(&res)}), nil
}
