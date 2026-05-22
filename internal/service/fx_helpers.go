package service

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func fxRateToProto(r *ledger.FXRate) *ledgerv1.FXRate {
	return &ledgerv1.FXRate{
		Id: r.ID, TenantId: r.TenantID,
		BaseCurrency: r.BaseCurrency, QuoteCurrency: r.QuoteCurrency,
		Rate: r.Rate.String(), Source: r.Source,
		EffectiveAt: timestamppb.New(r.EffectiveAt),
		CreatedAt:   timestamppb.New(r.CreatedAt),
	}
}
