package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func (s *Server) GetFlow(ctx context.Context, req *connect.Request[ledgerv1.GetFlowRequest]) (*connect.Response[ledgerv1.GetFlowResponse], error) {
	f, err := s.Store.GetFlow(ctx, req.Msg.GetTenantId(), req.Msg.GetFlowRunId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if f == nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountNotFound, "flow "+req.Msg.GetFlowRunId()))
	}
	return connect.NewResponse(&ledgerv1.GetFlowResponse{Flow: flowRunToResponse(f, f.Steps)}), nil
}
