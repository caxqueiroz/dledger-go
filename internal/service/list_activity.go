package service

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

func (s *Server) ListAccountActivity(ctx context.Context, req *connect.Request[ledgerv1.ListAccountActivityRequest]) (*connect.Response[ledgerv1.ListAccountActivityResponse], error) {
	r := req.Msg
	limit := int(r.GetPageSize())
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	in := repo.ListActivityInput{
		TenantID: r.GetTenantId(), AccountID: r.GetAccountId(), Currency: r.GetCurrency(), Limit: limit,
	}
	if r.GetSince() != nil {
		t := r.GetSince().AsTime()
		in.Since = &t
	}
	if r.GetUntil() != nil {
		t := r.GetUntil().AsTime()
		in.Until = &t
	}
	rows, err := s.Store.ListAccountActivity(ctx, in)
	if err != nil {
		return nil, ToConnectError(err)
	}
	out := &ledgerv1.ListAccountActivityResponse{}
	for _, row := range rows {
		dir := ledgerv1.Direction_DIRECTION_DEBIT
		if string(row.Direction) == "CREDIT" {
			dir = ledgerv1.Direction_DIRECTION_CREDIT
		}
		out.Entries = append(out.Entries, &ledgerv1.AccountActivityEntry{
			JournalId: row.JournalID, EntryId: row.EntryID,
			Currency:  row.Currency, Direction: dir, Amount: row.Amount.String(),
			CreatedAt: timestamppb.New(row.CreatedAt), SourceService: row.SourceService,
		})
	}
	return connect.NewResponse(out), nil
}
