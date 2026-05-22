// internal/service/reservation_stubs.go
//
// Temporary stubs to satisfy the LedgerServiceHandler interface between
// Task 16 (proto landed) and Tasks 18-20 (real handlers).
// Task 20 will replace GetReservation.
package service

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) GetReservation(ctx context.Context, req *connect.Request[ledgerv1.GetReservationRequest]) (*connect.Response[ledgerv1.GetReservationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("GetReservation not implemented yet"))
}
