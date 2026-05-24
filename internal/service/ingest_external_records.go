// ingest_external_records.go
package service

import (
	"context"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func (s *Server) IngestExternalRecords(ctx context.Context, req *connect.Request[ledgerv1.IngestExternalRecordsRequest]) (*connect.Response[ledgerv1.IngestExternalRecordsResponse], error) {
	r := req.Msg
	var inserted, skipped int32
	for _, in := range r.GetRecords() {
		amt, err := ledger.ParseAmount(in.GetAmount())
		if err != nil {
			return nil, ToConnectError(ledger.NewDomainError(ledger.CodeUnbalancedJournal, "amount: "+err.Error()))
		}
		occurred := s.Now()
		if in.GetOccurredAt() != nil {
			occurred = in.GetOccurredAt().AsTime()
		}
		rec := ledger.ExternalRecord{
			ID: s.NewID(), TenantID: r.GetTenantId(),
			Source: in.GetSource(), ExternalRef: in.GetExternalRef(),
			Amount: amt, Currency: in.GetCurrency(),
			OccurredAt: occurred,
			AccountID:  in.GetAccountId(),
			RawPayload: structToMap(in.GetRawPayload()),
		}
		ok, err := s.Store.InsertExternalRecord(ctx, rec)
		if err != nil {
			return nil, ToConnectError(err)
		}
		if ok {
			inserted++
		} else {
			skipped++
		}
	}
	return connect.NewResponse(&ledgerv1.IngestExternalRecordsResponse{Inserted: inserted, Skipped: skipped}), nil
}
