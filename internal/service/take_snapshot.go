// internal/service/take_snapshot.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) TakeBalanceSnapshot(ctx context.Context, req *connect.Request[ledgerv1.TakeBalanceSnapshotRequest]) (*connect.Response[ledgerv1.TakeBalanceSnapshotResponse], error) {
	r := req.Msg
	tenant := r.GetTenantId()
	if tenant == "" {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch, "tenant_id required"))
	}

	now := s.Now()

	// Single-row variant
	if r.GetAccountId() != "" && r.GetCurrency() != "" {
		d, c, ver, err := s.Store.GetBalance(ctx, tenant, r.GetAccountId(), r.GetCurrency())
		if err != nil {
			return nil, ToConnectError(err)
		}
		snap := ledger.BalanceSnapshot{
			ID: s.NewID(), TenantID: tenant,
			AccountID: r.GetAccountId(), Currency: r.GetCurrency(),
			PostedDebits: d, PostedCredits: c, Version: ver,
			SnapshotAt: now,
		}
		if err := s.Store.InsertSnapshot(ctx, snap); err != nil {
			return nil, ToConnectError(err)
		}
		return connect.NewResponse(&ledgerv1.TakeBalanceSnapshotResponse{SnapshotsTaken: 1}), nil
	}

	// Bulk variant: snapshot every account_balances row for the tenant.
	rows, err := s.Store.ListTenantBalances(ctx, tenant)
	if err != nil {
		return nil, ToConnectError(err)
	}
	taken := int32(0)
	for _, b := range rows {
		snap := ledger.BalanceSnapshot{
			ID: s.NewID(), TenantID: tenant,
			AccountID: b.AccountID, Currency: b.Currency,
			PostedDebits: b.PostedDebits, PostedCredits: b.PostedCredits, Version: b.Version,
			SnapshotAt: now,
		}
		if err := s.Store.InsertSnapshot(ctx, snap); err != nil {
			return nil, ToConnectError(err)
		}
		taken++
	}
	return connect.NewResponse(&ledgerv1.TakeBalanceSnapshotResponse{SnapshotsTaken: taken}), nil
}
