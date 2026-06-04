// recon_test.go
package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func TestIngest_HappyPathAndIdempotent(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	now := time.Now()
	records := []*ledgerv1.ExternalRecordInput{
		{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now)},
		{Source: "stripe", ExternalRef: "tx_002", Amount: "200", Currency: "USD", OccurredAt: timestamppb.New(now)},
		{Source: "stripe", ExternalRef: "tx_003", Amount: "300", Currency: "USD", OccurredAt: timestamppb.New(now)},
	}

	r1, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: records,
	}))
	if err != nil {
		t.Fatalf("ingest1: %v", err)
	}
	if r1.Msg.GetInserted() != 3 || r1.Msg.GetSkipped() != 0 {
		t.Fatalf("first: want 3/0, got %d/%d", r1.Msg.GetInserted(), r1.Msg.GetSkipped())
	}

	r2, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: records,
	}))
	if err != nil {
		t.Fatalf("ingest2: %v", err)
	}
	if r2.Msg.GetInserted() != 0 || r2.Msg.GetSkipped() != 3 {
		t.Fatalf("replay: want 0/3, got %d/%d", r2.Msg.GetInserted(), r2.Msg.GetSkipped())
	}
}

func TestRunReconciliation_AllMatched(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	for _, ref := range []string{"tx_001", "tx_002"} {
		if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
			TenantId: "t1", IdempotencyKey: "recon-seed-" + ref, SourceService: "stripe",
			Journal: &ledgerv1.Journal{EventId: ref, Entries: []*ledgerv1.Entry{
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			}},
		})); err != nil {
			t.Fatalf("seed %s: %v", ref, err)
		}
	}

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_002", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "batch-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	b := resp.Msg.GetBatch()
	if b.GetMatchedCount() != 2 {
		t.Fatalf("matched: want 2, got %d", b.GetMatchedCount())
	}
	if b.GetMissingInLedgerCount() != 0 || b.GetMissingInExternalCount() != 0 || b.GetMismatchedCount() != 0 {
		t.Fatalf("expected no discrepancies, got missing_in_ledger=%d missing_in_external=%d mismatched=%d",
			b.GetMissingInLedgerCount(), b.GetMissingInExternalCount(), b.GetMismatchedCount())
	}
}

func TestRunReconciliation_MissingInLedger(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_missing", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mil-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMissingInLedgerCount(); got != 1 {
		t.Fatalf("want 1 missing-in-ledger, got %d", got)
	}

	disc, err := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: resp.Msg.GetBatch().GetId(),
	}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(disc.Msg.GetDiscrepancies()) != 1 || disc.Msg.GetDiscrepancies()[0].GetType() != "MISSING_IN_LEDGER" {
		t.Fatalf("expected MISSING_IN_LEDGER, got %v", disc.Msg.GetDiscrepancies())
	}
}

func TestRunReconciliation_MissingInExternal(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "orphan-seed", SourceService: "stripe",
		Journal: &ledgerv1.Journal{EventId: "tx_orphan", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	now := time.Now()
	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mie-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMissingInExternalCount(); got != 1 {
		t.Fatalf("want 1 missing-in-external, got %d", got)
	}
}

func TestRunReconciliation_AmountMismatch(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "mm-seed", SourceService: "stripe",
		Journal: &ledgerv1.Journal{EventId: "tx_mm", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	now := time.Now()
	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_mm", Amount: "999", Currency: "USD",
				OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "mm-1", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := resp.Msg.GetBatch().GetMismatchedCount(); got != 1 {
		t.Fatalf("want 1 mismatched, got %d", got)
	}
}

func TestResolveDiscrepancy_WithAdjustment(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_late", Amount: "75", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "adj-batch", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	disc, err := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	if err != nil || len(disc.Msg.GetDiscrepancies()) != 1 {
		t.Fatalf("list: %v / %v", err, disc.Msg.GetDiscrepancies())
	}
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	adj := &ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "ADJUSTMENT", SourceService: "recon",
		Steps: []*ledgerv1.Step{{
			StepId: "adjust",
			Journal: &ledgerv1.Journal{
				EventId: "tx_late",
				Entries: []*ledgerv1.Entry{
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "75"},
					{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "75"},
				},
			},
		}},
	}
	res, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "RESOLVED",
		Adjustment: adj, IdempotencyKey: "r1", Note: "late tx booked",
	}))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.Msg.GetDiscrepancy().GetStatus() != "RESOLVED" {
		t.Fatalf("status: want RESOLVED, got %s", res.Msg.GetDiscrepancy().GetStatus())
	}
	if res.Msg.GetDiscrepancy().GetResolutionJournalId() == "" {
		t.Fatalf("resolution_journal_id should be linked")
	}

	bal, _ := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if bal.Msg.GetBalance().GetNormalized() != "75" {
		t.Fatalf("avail: want 75, got %s", bal.Msg.GetBalance().GetNormalized())
	}
}

func TestResolveDiscrepancy_NoAdjustment(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_noadj", Amount: "10", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, err := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "noadj", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	disc, _ := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	res, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "IGNORED",
		IdempotencyKey: "r2", Note: "known noise",
	}))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if res.Msg.GetDiscrepancy().GetStatus() != "IGNORED" {
		t.Fatalf("status: want IGNORED, got %s", res.Msg.GetDiscrepancy().GetStatus())
	}
	if res.Msg.GetDiscrepancy().GetResolutionJournalId() != "" {
		t.Fatalf("resolution_journal_id should be empty")
	}
}

func TestResolveDiscrepancy_AlreadyClosed(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_dup", Amount: "10", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	bResp, _ := srv.RunReconciliation(context.Background(), connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "dup", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	disc, _ := srv.ListDiscrepancies(context.Background(), connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: "t1", BatchId: bResp.Msg.GetBatch().GetId(),
	}))
	did := disc.Msg.GetDiscrepancies()[0].GetId()

	if _, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "IGNORED", IdempotencyKey: "first",
	})); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	_, err := srv.ResolveDiscrepancy(context.Background(), connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: "t1", DiscrepancyId: did, Resolution: "RESOLVED", IdempotencyKey: "second",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition CodeDiscrepancyClosed, got %v", err)
	}
}

func TestRunReconciliation_IdempotentReplay(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	now := time.Now()

	if _, err := srv.IngestExternalRecords(context.Background(), connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: "t1", Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_idem", Amount: "1", Currency: "USD", OccurredAt: timestamppb.New(now)},
		},
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	req := &ledgerv1.RunReconciliationRequest{
		TenantId: "t1", IdempotencyKey: "idem", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}
	r1, err := srv.RunReconciliation(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	r2, err := srv.RunReconciliation(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if r1.Msg.GetBatch().GetId() != r2.Msg.GetBatch().GetId() {
		t.Fatalf("replay batch id mismatch: %s vs %s", r1.Msg.GetBatch().GetId(), r2.Msg.GetBatch().GetId())
	}
}
