// get_reconciliation_batch.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) GetReconciliationBatch(ctx context.Context, req *connect.Request[ledgerv1.GetReconciliationBatchRequest]) (*connect.Response[ledgerv1.GetReconciliationBatchResponse], error) {
	b, err := s.Store.GetReconBatch(ctx, req.Msg.GetTenantId(), req.Msg.GetBatchId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.GetReconciliationBatchResponse{Batch: reconBatchToProto(b)}), nil
}
