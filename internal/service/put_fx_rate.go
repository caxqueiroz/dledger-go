// put_fx_rate.go: PutFXRate handler.
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) PutFXRate(ctx context.Context, req *connect.Request[ledgerv1.PutFXRateRequest]) (*connect.Response[ledgerv1.PutFXRateResponse], error) {
	r := req.Msg
	rate, err := ledger.ParseAmount(r.GetRate())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXAmountMismatch, err.Error()))
	}
	effective := s.Now()
	if r.GetEffectiveAt() != nil {
		effective = r.GetEffectiveAt().AsTime()
	}
	fxr := ledger.FXRate{
		ID:            s.NewID(),
		TenantID:      r.GetTenantId(),
		BaseCurrency:  r.GetBaseCurrency(),
		QuoteCurrency: r.GetQuoteCurrency(),
		Rate:          rate,
		Source:        r.GetSource(),
		EffectiveAt:   effective,
	}
	got, err := s.Store.UpsertFXRate(ctx, fxr)
	if err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.PutFXRateResponse{Rate: fxRateToProto(got)}), nil
}
