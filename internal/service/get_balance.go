package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// GetBalance returns the current balance for an account and currency.
func (s *Server) GetBalance(ctx context.Context, req *connect.Request[ledgerv1.GetBalanceRequest]) (*connect.Response[ledgerv1.GetBalanceResponse], error) {
	r := req.Msg
	a, err := s.Store.GetAccount(ctx, r.GetTenantId(), r.GetAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if a.Currency != r.GetCurrency() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
			"account "+a.ID+" currency="+a.Currency+" req="+r.GetCurrency()))
	}
	d, c, ver, err := s.Store.GetBalance(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency())
	if err != nil {
		return nil, ToConnectError(err)
	}
	norm := ledger.NormalizedBalance(a.NormalBalance, d, c)
	return connect.NewResponse(&ledgerv1.GetBalanceResponse{
		Balance: &ledgerv1.Balance{
			AccountId:     a.ID,
			Currency:      r.GetCurrency(),
			PostedDebits:  d.String(),
			PostedCredits: c.String(),
			Normalized:    norm.String(),
			Version:       ver,
		},
	}), nil
}
