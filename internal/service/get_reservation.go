// internal/service/get_reservation.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) GetReservation(ctx context.Context, req *connect.Request[ledgerv1.GetReservationRequest]) (*connect.Response[ledgerv1.GetReservationResponse], error) {
	res, err := s.Store.GetReservation(ctx, req.Msg.GetTenantId(), req.Msg.GetReservationId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetReservationResponse{Reservation: reservationToProto(res)}), nil
}
