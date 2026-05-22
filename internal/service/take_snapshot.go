package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
)

// Stub: real implementation lands in Task 6.
func (s *Server) TakeBalanceSnapshot(ctx context.Context, req *connect.Request[ledgerv1.TakeBalanceSnapshotRequest]) (*connect.Response[ledgerv1.TakeBalanceSnapshotResponse], error) {
	return connect.NewResponse(&ledgerv1.TakeBalanceSnapshotResponse{}), nil
}
