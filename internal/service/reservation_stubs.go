// internal/service/reservation_stubs.go
//
// Temporary stubs to satisfy the LedgerServiceHandler interface between
// Task 16 (proto landed) and Tasks 18-20 (real handlers).
// Tasks 18, 19, 20 will replace these.
package service

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

func (s *Server) CreateReservation(ctx context.Context, req *connect.Request[ledgerv1.CreateReservationRequest]) (*connect.Response[ledgerv1.CreateReservationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("CreateReservation not implemented yet"))
}

func (s *Server) CommitReservation(ctx context.Context, req *connect.Request[ledgerv1.CommitReservationRequest]) (*connect.Response[ledgerv1.CommitReservationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("CommitReservation not implemented yet"))
}

func (s *Server) ReleaseReservation(ctx context.Context, req *connect.Request[ledgerv1.ReleaseReservationRequest]) (*connect.Response[ledgerv1.ReleaseReservationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ReleaseReservation not implemented yet"))
}

func (s *Server) GetReservation(ctx context.Context, req *connect.Request[ledgerv1.GetReservationRequest]) (*connect.Response[ledgerv1.GetReservationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("GetReservation not implemented yet"))
}
