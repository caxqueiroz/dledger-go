// internal/sdk/remote_test.go
package sdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/service"
	"github.com/caxqueiroz/dledger-go/internal/service/interceptors"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// newRemoteAgainstEmbeddedServer spins up an httptest server backed by a
// throwaway SQLite store and returns a remote dledger.Client pointing at it.
// Mirrors how PAM would talk to a hosted dledger.
func newRemoteAgainstEmbeddedServer(t *testing.T, tenant string) dledger.Client {
	t.Helper()
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "sdk.db")

	// Reuse the embedded constructor to migrate + boot a server, but only
	// to provide the *service.Server we hand to the Connect mux.
	embedded, err := dledger.NewEmbedded(ctx, dledger.Options{
		Backend: dledger.SQLite, DSN: dsn, DisableScheduler: true,
	})
	if err != nil {
		t.Fatalf("embedded boot: %v", err)
	}

	// Open a second store and a new server against the *already-migrated* DSN
	// so we can hand a *service.Server to the Connect mux.
	store, err := sqlite.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := service.New(store)

	mux := http.NewServeMux()
	path, handler := ledgerv1connect.NewLedgerServiceHandler(srv,
		connect.WithInterceptors(interceptors.NewTenant()),
	)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(func() {
		ts.Close()
		_ = store.Close()
		_ = embedded.Close()
	})

	return dledger.NewRemote(ts.URL, tenant)
}

func TestNewRemote_InjectsTenantHeader(t *testing.T) {
	ctx := context.Background()
	c := newRemoteAgainstEmbeddedServer(t, "t1")
	defer c.Close()

	// A CreateAccount round trip succeeds only if X-Tenant-Id reached
	// the NewTenant interceptor (which would 400 otherwise).
	_, err := c.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: "t1", OwnerType: "platform", OwnerId: "0",
		AccountType: "src", Currency: "USD",
		NormalBalance: ledgerv1.NormalBalance_NORMAL_BALANCE_CREDIT,
	}))
	if err != nil {
		t.Fatalf("CreateAccount via remote: %v", err)
	}
}
