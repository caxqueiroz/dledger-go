// list_fx_rates.go: ListFXRates handler.
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ListFXRates(ctx context.Context, req *connect.Request[ledgerv1.ListFXRatesRequest]) (*connect.Response[ledgerv1.ListFXRatesResponse], error) {
	r := req.Msg
	in := repo.ListFXRatesInput{
		TenantID:      r.GetTenantId(),
		BaseCurrency:  r.GetBaseCurrency(),
		QuoteCurrency: r.GetQuoteCurrency(),
		Limit:         int(r.GetPageSize()),
	}
	if r.GetSince() != nil {
		t := r.GetSince().AsTime()
		in.Since = &t
	}
	if r.GetUntil() != nil {
		t := r.GetUntil().AsTime()
		in.Until = &t
	}
	rows, err := s.Store.ListFXRates(ctx, in)
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := &ledgerv1.ListFXRatesResponse{}
	for i := range rows {
		out.Rates = append(out.Rates, fxRateToProto(&rows[i]))
	}
	return connect.NewResponse(out), nil
}
