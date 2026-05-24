// recon_stubs.go: temporary stubs replaced by Tasks 9-10.
package service

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) RunReconciliation(ctx context.Context, req *connect.Request[ledgerv1.RunReconciliationRequest]) (*connect.Response[ledgerv1.RunReconciliationResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("RunReconciliation not implemented yet"))
}

func (s *Server) ResolveDiscrepancy(ctx context.Context, req *connect.Request[ledgerv1.ResolveDiscrepancyRequest]) (*connect.Response[ledgerv1.ResolveDiscrepancyResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ResolveDiscrepancy not implemented yet"))
}
