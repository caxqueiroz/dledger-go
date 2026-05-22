package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// GetAccount fetches a single account by tenant and account ID.
func (s *Server) GetAccount(ctx context.Context, req *connect.Request[ledgerv1.GetAccountRequest]) (*connect.Response[ledgerv1.GetAccountResponse], error) {
	a, err := s.Store.GetAccount(ctx, req.Msg.GetTenantId(), req.Msg.GetAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetAccountResponse{Account: accountToProto(*a)}), nil
}
