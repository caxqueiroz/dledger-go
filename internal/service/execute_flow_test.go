package service_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

func mustCreateAccount(t *testing.T, srv *service.Server, ownerID, kind, ccy string, allowNeg bool, nb ledgerv1.NormalBalance) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "user", OwnerId: ownerID, AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		t.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

// seedSource creates a credit-normal "source" account that allows negative
// balances, used to fund debit-normal user accounts in tests.
func seedSource(t *testing.T, srv *service.Server) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "source", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT, AllowNegative: true,
	}))
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	return r.Msg.GetAccount().GetId()
}

func TestExecuteFlow_PlaceOrder(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()

	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv := mustCreateAccount(t, srv, "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-1", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-1", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-abc-v1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-abc-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	})); err != nil {
		t.Fatalf("place: %v", err)
	}

	gb := func(acct string) string {
		r, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
			TenantId: "t1", AccountId: acct, Currency: "USD",
		}))
		if err != nil {
			t.Fatalf("get balance: %v", err)
		}
		return r.Msg.GetBalance().GetNormalized()
	}
	if got := gb(avail); got != "900" {
		t.Fatalf("avail want 900, got %s", got)
	}
	if got := gb(resv); got != "100" {
		t.Fatalf("resv want 100, got %s", got)
	}
}

func TestExecuteFlow_IdempotentReplay(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv := mustCreateAccount(t, srv, "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := seedSource(t, srv)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-2", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-2", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "1000"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "1000"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := &ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-xyz-v1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-xyz-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	}
	first, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Msg.GetFlowRunId() != second.Msg.GetFlowRunId() {
		t.Fatalf("replay returned different flow_run_id: %s vs %s", first.Msg.GetFlowRunId(), second.Msg.GetFlowRunId())
	}
}

func TestExecuteFlow_InsufficientFunds(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	avail := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv := mustCreateAccount(t, srv, "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "PLACE_ORDER", IdempotencyKey: "ord-broke", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "reserve", Journal: &ledgerv1.Journal{
			EventId: "ord-broke-reserve", Entries: []*ledgerv1.Entry{
				{AccountId: resv, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
			},
		}}},
	}))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeFailedPrecondition {
		t.Fatalf("want FailedPrecondition INSUFFICIENT_FUNDS, got %v", err)
	}
}

func TestExecuteFlow_UnbalancedAcrossCurrencies(t *testing.T) {
	srv, cleanup := newServer(t)
	defer cleanup()
	usd := mustCreateAccount(t, srv, "1", "cash_available", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	brl := mustCreateAccount(t, srv, "1", "cash_available", "BRL", true, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)

	_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(&ledgerv1.ExecuteFlowRequest{
		TenantId: "t1", FlowType: "BAD", IdempotencyKey: "bad-1", SourceService: "test",
		Steps: []*ledgerv1.Step{{StepId: "s", Journal: &ledgerv1.Journal{
			EventId: "bad-1-evt", Entries: []*ledgerv1.Entry{
				{AccountId: usd, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
				{AccountId: brl, Currency: "BRL", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "500"},
			},
		}}},
	}))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument UNBALANCED_JOURNAL, got %v", err)
	}
}
