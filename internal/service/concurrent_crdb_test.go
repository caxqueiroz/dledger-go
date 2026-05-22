//go:build integration

package service_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo/crdb"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

func startCRDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "cockroachdb/cockroach:v24.1.5",
		ExposedPorts: []string{"26257/tcp"},
		Cmd:          []string{"start-single-node", "--insecure"},
		WaitingFor:   wait.ForLog("nodeID:").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start crdb: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "26257")
	dsn := "postgres://root@" + host + ":" + port.Port() + "/defaultdb?sslmode=disable"

	// Apply migrations using goose
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	_ = goose.SetDialect("postgres")
	if err := goose.Up(db, "../../sql/migrations/crdb"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return dsn
}

func newCRDBServer(t *testing.T) (*service.Server, func()) {
	t.Helper()
	dsn := startCRDB(t)
	st, err := crdb.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return service.New(st), func() { _ = st.Close() }
}

func mustCreateAccountCRDB(t *testing.T, srv *service.Server, ownerType, ownerID, kind, ccy string, allowNeg bool, nb ledgerv1.NormalBalance) string {
	t.Helper()
	r, err := srv.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: ownerType, OwnerId: ownerID, AccountType: kind, Currency: ccy,
		NormalBalance: nb, AllowNegative: allowNeg,
	}))
	if err != nil {
		t.Fatalf("create %s: %v", kind, err)
	}
	return r.Msg.GetAccount().GetId()
}

func TestExecuteFlow_ConcurrentReservation_CRDB(t *testing.T) {
	srv, cleanup := newCRDBServer(t)
	defer cleanup()

	avail := mustCreateAccountCRDB(t, srv, "user", "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	resv := mustCreateAccountCRDB(t, srv, "user", "1", "cash_reserved", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := mustCreateAccountCRDB(t, srv, "platform", "0", "source", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	// Seed exactly 100 USD.
	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "seed-crdb-conc", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "seed-crdb-conc", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mk := func(key string) *ledgerv1.ExecuteFlowRequest {
		return &ledgerv1.ExecuteFlowRequest{
			TenantId: "t1", FlowType: "RESERVE", IdempotencyKey: key, SourceService: "test",
			Steps: []*ledgerv1.Step{{StepId: "r", Journal: &ledgerv1.Journal{
				EventId: key + "-evt", Entries: []*ledgerv1.Entry{
					{AccountId: resv, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "100"},
					{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "100"},
				},
			}}},
		}
	}
	type result struct{ err error }
	out := make(chan result, 2)
	go func() {
		_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(mk("crdb-a")))
		out <- result{err}
	}()
	go func() {
		_, err := srv.ExecuteFlow(context.Background(), connect.NewRequest(mk("crdb-b")))
		out <- result{err}
	}()
	r1 := <-out
	r2 := <-out
	successes := 0
	failures := 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			successes++
			continue
		}
		// Either INSUFFICIENT_FUNDS (FailedPrecondition) or SERIALIZATION_RETRY_EXHAUSTED (Aborted) is acceptable.
		cerr, ok := errors.AsType[*connect.Error](r.err)
		if !ok {
			t.Fatalf("non-connect error: %v", r.err)
		}
		if cerr.Code() != connect.CodeFailedPrecondition && cerr.Code() != connect.CodeAborted {
			t.Fatalf("unexpected failure code %s: %v", cerr.Code(), r.err)
		}
		failures++
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("expected exactly one success and one failure; got %d/%d", successes, failures)
	}
}

func TestExecuteFlow_SmokeAgainstCRDB(t *testing.T) {
	srv, cleanup := newCRDBServer(t)
	defer cleanup()

	avail := mustCreateAccountCRDB(t, srv, "user", "1", "cash_available", "USD", false, ledgerv1.NormalBalance_NORMAL_BALANCE_DEBIT)
	src := mustCreateAccountCRDB(t, srv, "platform", "0", "source", "USD", true, ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT)

	if _, err := srv.PostJournal(context.Background(), connect.NewRequest(&ledgerv1.PostJournalRequest{
		TenantId: "t1", IdempotencyKey: "crdb-smoke", SourceService: "test",
		Journal: &ledgerv1.Journal{EventId: "crdb-smoke", Entries: []*ledgerv1.Entry{
			{AccountId: avail, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_DEBIT, Amount: "250"},
			{AccountId: src, Currency: "USD", Direction: ledgerv1.Direction_DIRECTION_CREDIT, Amount: "250"},
		}},
	})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	bal, err := srv.GetBalance(context.Background(), connect.NewRequest(&ledgerv1.GetBalanceRequest{
		TenantId: "t1", AccountId: avail, Currency: "USD",
	}))
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if got := bal.Msg.GetBalance().GetNormalized(); got != "250" {
		t.Fatalf("want 250, got %s", got)
	}
}
