package service

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// GetBalance returns the balance for an account and currency.
// If as_of is set, it reconstructs the balance at that point in time using
// the latest snapshot before as_of plus any entries between snapshot and as_of.
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

	var d, c decimal.Decimal
	var ver int64

	if r.GetAsOf() != nil {
		at := r.GetAsOf().AsTime()

		snap, sErr := s.Store.GetSnapshotBefore(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency(), at)
		if sErr != nil {
			return nil, ToConnectError(sErr)
		}
		var snapAt time.Time // zero value: before any real entry
		if snap != nil {
			d = snap.PostedDebits
			c = snap.PostedCredits
			ver = snap.Version
			snapAt = snap.SnapshotAt
		}
		addD, addC, eErr := s.Store.SumEntriesBetween(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency(), snapAt, at)
		if eErr != nil {
			return nil, ToConnectError(eErr)
		}
		d = d.Add(addD)
		c = c.Add(addC)
	} else {
		d, c, ver, err = s.Store.GetBalance(ctx, r.GetTenantId(), r.GetAccountId(), r.GetCurrency())
		if err != nil {
			return nil, ToConnectError(err)
		}
	}

	norm := ledger.NormalizedBalance(a.NormalBalance, d, c)
	return connect.NewResponse(&ledgerv1.GetBalanceResponse{
		Balance: &ledgerv1.Balance{
			AccountId:    a.ID,
			Currency:     r.GetCurrency(),
			PostedDebits: d.String(), PostedCredits: c.String(),
			Normalized: norm.String(), Version: ver,
		},
	}), nil
}
