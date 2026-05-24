// Reconciliation walkthrough:
//  1. Create user + source accounts.
//  2. Post two "stripe" journals (event_id = tx ref).
//  3. Ingest three external records: two matching, one missing-in-ledger.
//  4. Run reconciliation: expect 2 matched, 1 missing-in-ledger.
//  5. Resolve the missing one by posting an adjustment journal.
//
// Run the server first; see ../place_order/main.go for setup.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
)

const (
	serverURL = "http://localhost:8080"
	tenant    = "t1"
)

type tenantRT struct{ tenant string }

func (t tenantRT) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Tenant-Id", t.tenant)
	return http.DefaultTransport.RoundTrip(req)
}

func main() {
	client := ledgerv1connect.NewLedgerServiceClient(
		&http.Client{Transport: tenantRT{tenant: tenant}}, serverURL)
	ctx := context.Background()

	avail := createAccount(ctx, client, "user", "1", "cash_available", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT, false)
	src := createAccount(ctx, client, "platform", "0", "source", "USD", ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, true)

	for _, ref := range []string{"tx_001", "tx_002"} {
		postJournal(ctx, client, "seed-"+ref, "stripe", ref, []*ledgerv1.Entry{
			entry(avail, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "100"),
			entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "100"),
		})
	}
	fmt.Println("seeded two journals from source 'stripe'")

	now := time.Now()
	if _, err := client.IngestExternalRecords(ctx, connect.NewRequest(&ledgerv1.IngestExternalRecordsRequest{
		TenantId: tenant,
		Records: []*ledgerv1.ExternalRecordInput{
			{Source: "stripe", ExternalRef: "tx_001", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_002", Amount: "100", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
			{Source: "stripe", ExternalRef: "tx_999", Amount: "42", Currency: "USD", OccurredAt: timestamppb.New(now), AccountId: avail},
		},
	})); err != nil {
		log.Fatalf("ingest: %v", err)
	}
	fmt.Println("ingested 3 external records")

	bResp, err := client.RunReconciliation(ctx, connect.NewRequest(&ledgerv1.RunReconciliationRequest{
		TenantId: tenant, IdempotencyKey: "demo-batch", Source: "stripe",
		WindowStart: timestamppb.New(now.Add(-time.Hour)),
		WindowEnd:   timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	b := bResp.Msg.GetBatch()
	fmt.Printf("batch %s: matched=%d missing_in_ledger=%d missing_in_external=%d\n",
		b.GetId(), b.GetMatchedCount(), b.GetMissingInLedgerCount(), b.GetMissingInExternalCount())

	dResp, err := client.ListDiscrepancies(ctx, connect.NewRequest(&ledgerv1.ListDiscrepanciesRequest{
		TenantId: tenant, BatchId: b.GetId(), Status: "OPEN",
	}))
	if err != nil {
		log.Fatalf("list: %v", err)
	}
	if len(dResp.Msg.GetDiscrepancies()) != 1 {
		log.Fatalf("want 1 discrepancy, got %d", len(dResp.Msg.GetDiscrepancies()))
	}
	d := dResp.Msg.GetDiscrepancies()[0]
	fmt.Printf("open discrepancy: id=%s type=%s\n", d.GetId(), d.GetType())

	adj := &ledgerv1.ExecuteFlowRequest{
		TenantId: tenant, FlowType: "ADJUSTMENT", SourceService: "recon",
		Steps: []*ledgerv1.Step{{
			StepId: "adjust",
			Journal: &ledgerv1.Journal{
				EventId: "tx_999",
				Entries: []*ledgerv1.Entry{
					entry(avail, "USD", ledgerv1.Direction_DIRECTION_DEBIT, "42"),
					entry(src, "USD", ledgerv1.Direction_DIRECTION_CREDIT, "42"),
				},
			},
		}},
	}
	res, err := client.ResolveDiscrepancy(ctx, connect.NewRequest(&ledgerv1.ResolveDiscrepancyRequest{
		TenantId: tenant, DiscrepancyId: d.GetId(), Resolution: "RESOLVED",
		Adjustment: adj, IdempotencyKey: "demo-resolve", Note: "back-booked from stripe",
	}))
	if err != nil {
		log.Fatalf("resolve: %v", err)
	}
	fmt.Printf("resolved: status=%s resolution_journal_id=%s\n",
		res.Msg.GetDiscrepancy().GetStatus(), res.Msg.GetDiscrepancy().GetResolutionJournalId())
}

func createAccount(ctx context.Context, c ledgerv1connect.LedgerServiceClient, ownerType, ownerID, kind, ccy string, nb ledgerv1.NormalBalance, allowNeg bool) string {
	r, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		log.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

func entry(acct, ccy string, dir ledgerv1.Direction, amount string) *ledgerv1.Entry {
	return &ledgerv1.Entry{AccountId: acct, Currency: ccy, Direction: dir, Amount: amount}
}

func postJournal(ctx context.Context, c ledgerv1connect.LedgerServiceClient, key, source, ref string, entries []*ledgerv1.Entry) {
	if _, err := c.PostJournal(ctx, connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: tenant, IdempotencyKey: key, SourceService: source,
		Journal: &ledgerv1.Journal{EventId: ref, Entries: entries},
	})); err != nil {
		log.Fatalf("post %s: %v", key, err)
	}
}
