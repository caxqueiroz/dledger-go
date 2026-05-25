// internal/service/list_reservations.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// ListReservations returns reservations filtered by tenant and optional
// owner/status/page_size. Owner filters join through the source account.
func (s *Server) ListReservations(ctx context.Context, req *connect.Request[ledgerv1.ListReservationsRequest]) (*connect.Response[ledgerv1.ListReservationsResponse], error) {
	r := req.Msg
	rows, err := s.Store.ListReservations(ctx, repo.ListReservationsInput{
		TenantID:  r.GetTenantId(),
		OwnerType: r.GetOwnerType(),
		OwnerID:   r.GetOwnerId(),
		Status:    r.GetStatus(),
		Limit:     int(r.GetPageSize()),
	})
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := make([]*ledgerv1.Reservation, 0, len(rows))
	for i := range rows {
		out = append(out, reservationToProto(&rows[i]))
	}
	return connect.NewResponse(&ledgerv1.ListReservationsResponse{Reservations: out}), nil
}
