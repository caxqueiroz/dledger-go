package service

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// CreateAccount creates a new ledger account.
func (s *Server) CreateAccount(ctx context.Context, req *connect.Request[ledgerv1.CreateAccountRequest]) (*connect.Response[ledgerv1.CreateAccountResponse], error) {
	r := req.Msg
	nb := normalBalanceFromProto(r.GetNormalBalance())
	if !nb.Valid() {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeInvalidAccountStatus, "invalid normal_balance"))
	}
	a := ledger.Account{
		ID:            fmt.Sprintf("%s:%s:%s:%s", r.GetOwnerType(), r.GetOwnerId(), r.GetAccountType(), r.GetCurrency()),
		TenantID:      r.GetTenantId(),
		OwnerType:     r.GetOwnerType(),
		OwnerID:       r.GetOwnerId(),
		AccountType:   r.GetAccountType(),
		Currency:      r.GetCurrency(),
		NormalBalance: nb,
		AllowNegative: r.GetAllowNegative(),
		Status:        ledger.AccountActive,
		CreatedAt:     s.Now(),
	}
	if err := s.runInTx(ctx, func(tx repo.Tx) error {
		return tx.InsertAccount(ctx, a)
	}); err != nil {
		return nil, ToConnectError(err)
	}
	return connect.NewResponse(&ledgerv1.CreateAccountResponse{Account: accountToProto(a)}), nil
}

func normalBalanceFromProto(p ledgerv1.NormalBalance) ledger.NormalBalance {
	if p == ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT {
		return ledger.NormalCredit
	}
	return ledger.NormalDebit
}

func accountToProto(a ledger.Account) *ledgerv1.Account {
	nb := ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT
	if a.NormalBalance == ledger.NormalCredit {
		nb = ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT
	}
	st := ledgerv1.AccountStatus_ACCOUNT_STATUS_ACTIVE
	switch a.Status {
	case ledger.AccountFrozen:
		st = ledgerv1.AccountStatus_ACCOUNT_STATUS_FROZEN
	case ledger.AccountClosed:
		st = ledgerv1.AccountStatus_ACCOUNT_STATUS_CLOSED
	}
	return &ledgerv1.Account{
		Id:            a.ID,
		TenantId:      a.TenantID,
		OwnerType:     a.OwnerType,
		OwnerId:       a.OwnerID,
		AccountType:   a.AccountType,
		Currency:      a.Currency,
		NormalBalance: nb,
		AllowNegative: a.AllowNegative,
		Status:        st,
		CreatedAt:     timestamppb.New(a.CreatedAt),
	}
}
