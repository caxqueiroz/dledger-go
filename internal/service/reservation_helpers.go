// internal/service/reservation_helpers.go
package service

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

func reservationToProto(r *ledger.Reservation) *ledgerv1.Reservation {
	p := &ledgerv1.Reservation{
		Id: r.ID, TenantId: r.TenantID, Status: string(r.Status),
		SourceAccountId: r.SourceAccountID, ReservedAccountId: r.ReservedAccountID,
		Currency:          r.Currency,
		OriginalAmount:    r.OriginalAmount.String(),
		OutstandingAmount: r.OutstandingAmount.String(),
		CommittedAmount:   r.CommittedAmount.String(),
		ReleasedAmount:    r.ReleasedAmount.String(),
		FlowRunId:         r.FlowRunID,
		CreatedAt:         timestamppb.New(r.CreatedAt),
		UpdatedAt:         timestamppb.New(r.UpdatedAt),
	}
	if r.ExpiresAt != nil {
		p.ExpiresAt = timestamppb.New(*r.ExpiresAt)
	}
	return p
}
