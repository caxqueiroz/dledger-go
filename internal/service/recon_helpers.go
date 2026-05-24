// recon_helpers.go: proto<->domain conversions for reconciliation types.
package service

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

func externalRecordToProto(r *ledger.ExternalRecord) *ledgerv1.ExternalRecord {
	return &ledgerv1.ExternalRecord{
		Id: r.ID, TenantId: r.TenantID,
		Source: r.Source, ExternalRef: r.ExternalRef,
		Amount: r.Amount.String(), Currency: r.Currency,
		OccurredAt: timestamppb.New(r.OccurredAt),
		AccountId:  r.AccountID,
		MatchStatus:      string(r.MatchStatus),
		MatchedJournalId: r.MatchedJournalID,
		CreatedAt:        timestamppb.New(r.CreatedAt),
	}
}

func reconBatchToProto(b *ledger.ReconciliationBatch) *ledgerv1.ReconciliationBatch {
	p := &ledgerv1.ReconciliationBatch{
		Id: b.ID, TenantId: b.TenantID, Source: b.Source,
		WindowStart: timestamppb.New(b.WindowStart),
		WindowEnd:   timestamppb.New(b.WindowEnd),
		Status: string(b.Status),
		IngestedCount: b.IngestedCount, MatchedCount: b.MatchedCount,
		MismatchedCount: b.MismatchedCount,
		MissingInLedgerCount:   b.MissingInLedgerCount,
		MissingInExternalCount: b.MissingInExternalCount,
		StartedAt: timestamppb.New(b.StartedAt),
		ActorId:   b.ActorID,
	}
	if !b.CompletedAt.IsZero() {
		p.CompletedAt = timestamppb.New(b.CompletedAt)
	}
	return p
}

func discrepancyToProto(d *ledger.Discrepancy) *ledgerv1.Discrepancy {
	p := &ledgerv1.Discrepancy{
		Id: d.ID, TenantId: d.TenantID, BatchId: d.BatchID,
		Type: string(d.Type),
		ExternalRecordId: d.ExternalRecordID,
		JournalId:        d.JournalID,
		Status:           string(d.Status),
		ResolutionJournalId: d.ResolutionJournalID,
		ResolutionNote:      d.ResolutionNote,
		ResolvedBy:          d.ResolvedBy,
		CreatedAt:           timestamppb.New(d.CreatedAt),
	}
	if !d.ResolvedAt.IsZero() {
		p.ResolvedAt = timestamppb.New(d.ResolvedAt)
	}
	return p
}
