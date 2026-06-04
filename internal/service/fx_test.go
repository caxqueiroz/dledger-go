// fx_test.go: FX rate management and ExecuteExchange scenarios.
package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

func putFXRate(t *testing.T, srv *service.Server, base, quote, rate, source string, at time.Time) {
	t.Helper()
	if _, err := srv.PutFXRate(context.Background(), connect.NewRequest(&ledgerv1.PutFXRateRequest{
		TenantId: "t1", BaseCurrency: base, QuoteCurrency: quote,
		Rate: rate, Source: source, EffectiveAt: timestamppb.New(at),
	})); err != nil {
		t.Fatalf("put rate %s/%s: %v", base, quote, err)
	}
}

func TestPutAndGetFXRate(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	now := time.Now()
	putFXRate(t, srv, "USD", "EUR", "0.895", "manual", now)

	r, err := srv.GetFXRate(context.Background(), connect.NewRequest(&ledgerv1.GetFXRateRequest{
		TenantId: "t1", BaseCurrency: "USD", QuoteCurrency: "EUR",
		At: timestamppb.New(now.Add(time.Second)),
	}))
	if err != nil {
		t.Fatalf("get rate: %v", err)
	}
	if r.Msg.GetRate().GetRate() != "0.895" {
		t.Fatalf("want 0.895, got %s", r.Msg.GetRate().GetRate())
	}
}

func TestGetFXRate_TimeOrdered(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	t0 := time.Now()
	putFXRate(t, srv, "USD", "EUR", "0.89", "manual", t0)
	putFXRate(t, srv, "USD", "EUR", "0.90", "manual", t0.Add(2*time.Second))

	r, err := srv.GetFXRate(context.Background(), connect.NewRequest(&ledgerv1.GetFXRateRequest{
		TenantId: "t1", BaseCurrency: "USD", QuoteCurrency: "EUR",
		At: timestamppb.New(t0.Add(1 * time.Second)),
	}))
	if err != nil {
		t.Fatalf("get rate: %v", err)
	}
	if r.Msg.GetRate().GetRate() != "0.89" {
		t.Fatalf("want 0.89, got %s", r.Msg.GetRate().GetRate())
	}

	r, err = srv.GetFXRate(context.Background(), connect.NewRequest(&ledgerv1.GetFXRateRequest{
		TenantId: "t1", BaseCurrency: "USD", QuoteCurrency: "EUR",
		At: timestamppb.New(t0.Add(5 * time.Second)),
	}))
	if err != nil {
		t.Fatalf("get rate later: %v", err)
	}
	if r.Msg.GetRate().GetRate() != "0.9" {
		t.Fatalf("later: want 0.9, got %s", r.Msg.GetRate().GetRate())
	}
}

func TestGetFXRate_NotFound(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	_, err := srv.GetFXRate(context.Background(), connect.NewRequest(&ledgerv1.GetFXRateRequest{
		TenantId: "t1", BaseCurrency: "USD", QuoteCurrency: "EUR",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeNotFound {
		t.Fatalf("want NotFound CodeFXRateNotFound, got %v", err)
	}
}

func TestExecuteExchange_HappyPath(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()

	userUSD := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	userEUR := mustCreateAccount(t, srv, "1", "cash_available", "EUR", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	platUSD := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	platEUR := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "EUR", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	src := seedSource(t, srv)
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "fx-seed-usd", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "fx-seed-usd", Entries: []*ledgerv1.Entry{
			{AccountId: userUSD, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed usd: %v", err)
	}

	resp, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(&ledgerv1.ExecuteExchangeRequest{
		TenantId: "t1", IdempotencyKey: "ex-1",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "89.500", Rate: "0.895",
		RateSource: "manual", SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if resp.Msg.GetRateUsed() != "0.895" {
		t.Fatalf("rate_used: want 0.895, got %s", resp.Msg.GetRateUsed())
	}

	bal := func(acct, ccy string) string {
		r, _ := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
			TenantId: "t1", AccountId: acct, Currency: ccy,
		}))
		return r.Msg.GetBalance().GetNormalized()
	}
	if got := bal(userUSD, "USD"); got != "0" {
		t.Fatalf("userUSD: want 0, got %s", got)
	}
	if got := bal(userEUR, "EUR"); got != "89.5" {
		t.Fatalf("userEUR: want 89.5, got %s", got)
	}
	if got := bal(platUSD, "USD"); got != "-100" {
		t.Fatalf("platUSD: want -100, got %s", got)
	}
	if got := bal(platEUR, "EUR"); got != "89.5" {
		t.Fatalf("platEUR: want 89.5, got %s", got)
	}
}

func TestExecuteExchange_ResolvesRateFromStore(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	userUSD := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	userEUR := mustCreateAccount(t, srv, "1", "cash_available", "EUR", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	platUSD := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	platEUR := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "EUR", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	src := seedSource(t, srv)
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "fx-seed2-usd", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "fx-seed2-usd", Entries: []*ledgerv1.Entry{
			{AccountId: userUSD, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "200"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "200"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	putFXRate(t, srv, "USD", "EUR", "0.9", "manual", time.Now())

	resp, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(&ledgerv1.ExecuteExchangeRequest{
		TenantId: "t1", IdempotencyKey: "ex-2",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "90.0",
		SourceService: "test",
	}))
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if resp.Msg.GetRateUsed() != "0.9" {
		t.Fatalf("rate_used: want 0.9, got %s", resp.Msg.GetRateUsed())
	}
	if resp.Msg.GetRateSource() != "manual" {
		t.Fatalf("rate_source: want manual, got %s", resp.Msg.GetRateSource())
	}
}

func TestExecuteExchange_AmountMismatch(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	userUSD := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	userEUR := mustCreateAccount(t, srv, "1", "cash_available", "EUR", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	platUSD := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	platEUR := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "EUR", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	_, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(&ledgerv1.ExecuteExchangeRequest{
		TenantId: "t1", IdempotencyKey: "ex-bad",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "999", Rate: "0.895",
		SourceService: "test",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("want InvalidArgument CodeFXAmountMismatch, got %v", err)
	}
}

func TestExecuteExchange_NoRateAvailable(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	userUSD := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	userEUR := mustCreateAccount(t, srv, "1", "cash_available", "EUR", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	platUSD := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	platEUR := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "EUR", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	_, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(&ledgerv1.ExecuteExchangeRequest{
		TenantId: "t1", IdempotencyKey: "ex-no-rate",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "90",
		SourceService: "test",
	}))
	cerr, ok := errors.AsType[*connect.Error](err)
	if !ok || cerr.Code() != connect.CodeNotFound {
		t.Fatalf("want NotFound CodeFXRateNotFound, got %v", err)
	}
}

func TestExecuteExchange_IdempotentReplay(t *testing.T) {
	skipUnsupportedOnDynamo(t)
	srv, cleanup := newServer(t)
	defer cleanup()
	userUSD := mustCreateAccount(t, srv, "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	userEUR := mustCreateAccount(t, srv, "1", "cash_available", "EUR", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	platUSD := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	platEUR := mustCreatePlatformAccount(t, srv, "fx_desk", "cash", "EUR", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)
	src := seedSource(t, srv)
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "fx-seed3-usd", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "fx-seed3-usd", Entries: []*ledgerv1.Entry{
			{AccountId: userUSD, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := &ledgerv1.ExecuteExchangeRequest{
		TenantId: "t1", IdempotencyKey: "ex-replay",
		FromAccountId: userUSD, ToAccountId: userEUR,
		FromCounterAccountId: platUSD, ToCounterAccountId: platEUR,
		FromAmount: "100", ToAmount: "89.500", Rate: "0.895",
		SourceService: "test",
	}
	r1, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	r2, err := srv.ExecuteExchange(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if r1.Msg.GetFlowRunId() != r2.Msg.GetFlowRunId() {
		t.Fatalf("replay flow_run_id mismatch: %s vs %s", r1.Msg.GetFlowRunId(), r2.Msg.GetFlowRunId())
	}
}
