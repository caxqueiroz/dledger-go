// list_discrepancies.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ListDiscrepancies(ctx context.Context, req *connect.Request[ledgerv1.ListDiscrepanciesRequest]) (*connect.Response[ledgerv1.ListDiscrepanciesResponse], error) {
	r := req.Msg
	in := repo.ListDiscrepanciesInput{
		TenantID: r.GetTenantId(),
		BatchID:  r.GetBatchId(),
		Status:   r.GetStatus(),
		Limit:    int(r.GetPageSize()),
	}
	rows, err := s.Store.ListDiscrepancies(ctx, in)
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := &ledgerv1.ListDiscrepanciesResponse{}
	for i := range rows {
		out.Discrepancies = append(out.Discrepancies, discrepancyToProto(&rows[i]))
	}
	return connect.NewResponse(out), nil
}
