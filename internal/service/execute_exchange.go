// execute_exchange.go: ExecuteExchange handler.
package service

import (
	"context"

	"connectrpc.com/connect"
	"github.com/shopspring/decimal"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) ExecuteExchange(ctx context.Context, req *connect.Request[ledgerv1.ExecuteExchangeRequest]) (*connect.Response[ledgerv1.ExecuteExchangeResponse], error) {
	r := req.Msg

	fromAmount, err := ledger.ParseAmount(r.GetFromAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXAmountMismatch, "from_amount: "+err.Error()))
	}
	toAmount, err := ledger.ParseAmount(r.GetToAmount())
	if err != nil {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXAmountMismatch, "to_amount: "+err.Error()))
	}

	// Resolve accounts outside the transaction to avoid deadlock on single-connection stores.
	fromAcc, err := s.Store.GetAccount(ctx, r.GetTenantId(), r.GetFromAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	toAcc, err := s.Store.GetAccount(ctx, r.GetTenantId(), r.GetToAccountId())
	if err != nil {
		return nil, ToConnectError(err)
	}
	if fromAcc.Currency == toAcc.Currency {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeAccountCurrencyMismatch,
			"from and to currencies must differ"))
	}

	// Resolve rate outside the transaction for the same reason.
	rate := decimal.Zero
	rateSource := r.GetRateSource()
	if r.GetRate() != "" {
		rate, err = decimal.NewFromString(r.GetRate())
		if err != nil || !rate.IsPositive() {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXAmountMismatch, "invalid rate"))
		}
	} else {
		got, gErr := s.Store.GetFXRateAt(ctx, r.GetTenantId(), fromAcc.Currency, toAcc.Currency, s.Now())
		if gErr != nil {
			return nil, ToConnectError(gErr)
		}
		if got == nil {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXRateNotFound,
				fromAcc.Currency+"/"+toAcc.Currency))
		}
		rate = got.Rate
		rateSource = got.Source
	}

	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return nil, ToConnectError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Validate amount math: from_amount * rate == to_amount.
	expected := fromAmount.Mul(rate)
	if !expected.Equal(toAmount) {
		return nil, ToConnectError(ledger.NewDomainError(ledger.CodeFXAmountMismatch,
			"from_amount*rate="+expected.String()+" != to_amount="+toAmount.String()))
	}

	// Build inner ExecuteFlow request.
	metaMap := structToMap(r.GetMetadata())
	metaMap["rate"] = rate.String()
	metaMap["rate_source"] = rateSource
	metaMap["from_currency"] = fromAcc.Currency
	metaMap["to_currency"] = toAcc.Currency
	metaStruct := mapToStruct(metaMap)

	flowReq := &ledgerv1.ExecuteFlowRequest{
		TenantId:       r.GetTenantId(),
		FlowType:       "EXCHANGE",
		IdempotencyKey: r.GetIdempotencyKey(),
		SourceService:  r.GetSourceService(),
		ActorId:        r.GetActorId(),
		Metadata:       metaStruct,
		Steps: []*ledgerv1.Step{{
			StepId: "exchange",
			Journal: &ledgerv1.Journal{
				EventId: r.GetIdempotencyKey() + ":exchange",
				Entries: []*ledgerv1.Entry{
					{AccountId: r.GetFromCounterAccountId(), Currency: fromAcc.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: r.GetFromAmount()},
					{AccountId: r.GetFromAccountId(), Currency: fromAcc.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: r.GetFromAmount()},
					{AccountId: r.GetToAccountId(), Currency: toAcc.Currency, Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: r.GetToAmount()},
					{AccountId: r.GetToCounterAccountId(), Currency: toAcc.Currency, Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: r.GetToAmount()},
				},
			},
		}},
	}

	flowResp, err := s.executeFlowInTx(ctx, tx, flowReq)
	if err != nil {
		return nil, ToConnectError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, ToConnectError(err)
	}
	committed = true

	var journalID string
	if len(flowResp.GetSteps()) > 0 {
		journalID = flowResp.GetSteps()[0].GetJournalId()
	}
	return connect.NewResponse(&ledgerv1.ExecuteExchangeResponse{
		FlowRunId:  flowResp.GetFlowRunId(),
		JournalId:  journalID,
		RateUsed:   rate.String(),
		RateSource: rateSource,
	}), nil
}
