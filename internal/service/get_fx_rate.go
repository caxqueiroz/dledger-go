// get_fx_rate.go: GetFXRate handler.
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) GetFXRate(ctx context.Context, req *connect.Request[ledgerv1.GetFXRateRequest]) (*connect.Response[ledgerv1.GetFXRateResponse], error) {
	r := req.Msg
	at := s.Now()
	if r.GetAt() != nil {
		at = r.GetAt().AsTime()
	}
	got, err := s.Store.GetFXRateAt(ctx, r.GetTenantId(), r.GetBaseCurrency(), r.GetQuoteCurrency(), at)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if got == nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXRateNotFound,
			r.GetBaseCurrency()+"/"+r.GetQuoteCurrency()))
	}
	return connect.NewResponse(&ledgerv1.GetFXRateResponse{Rate: fxRateToProto(got)}), nil
}
