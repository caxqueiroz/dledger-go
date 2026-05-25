// pkg/dledger/remote.go
package dledger

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
)

// NewRemote returns a Client that talks to a hosted dledger server.
// tenantID is injected as the X-Tenant-Id header on every request.
func NewRemote(serverURL, tenantID string, opts ...Option) Client {
	o := &remoteOptions{httpClient: http.DefaultClient, logger: slog.Default()}
	for _, fn := range opts {
		fn(o)
	}
	hc := *o.httpClient
	hc.Transport = &tenantTransport{base: roundTripperOr(hc.Transport, http.DefaultTransport), tenant: tenantID}
	rpc := ledgerv1connect.NewLedgerServiceClient(&hc, serverURL)
	return &remoteClient{rpc: rpc}
}

func roundTripperOr(rt http.RoundTripper, fallback http.RoundTripper) http.RoundTripper {
	if rt != nil {
		return rt
	}
	return fallback
}

// tenantTransport sets X-Tenant-Id on every outbound request.
type tenantTransport struct {
	base   http.RoundTripper
	tenant string
}

func (t *tenantTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2.Header.Set("X-Tenant-Id", t.tenant)
	return t.base.RoundTrip(req2)
}

// remoteClient forwards each RPC to a Connect-RPC client.
type remoteClient struct {
	rpc ledgerv1connect.LedgerServiceClient
}

func (c *remoteClient) Close() error { return nil }

func (c *remoteClient) CreateAccount(ctx context.Context, r *connect.Request[v1.CreateAccountRequest]) (*connect.Response[v1.CreateAccountResponse], error) {
	return c.rpc.CreateAccount(ctx, r)
}
func (c *remoteClient) GetAccount(ctx context.Context, r *connect.Request[v1.GetAccountRequest]) (*connect.Response[v1.GetAccountResponse], error) {
	return c.rpc.GetAccount(ctx, r)
}
func (c *remoteClient) GetBalance(ctx context.Context, r *connect.Request[v1.GetBalanceRequest]) (*connect.Response[v1.GetBalanceResponse], error) {
	return c.rpc.GetBalance(ctx, r)
}
func (c *remoteClient) PostJournal(ctx context.Context, r *connect.Request[v1.PostJournalRequest]) (*connect.Response[v1.PostJournalResponse], error) {
	return c.rpc.PostJournal(ctx, r)
}
func (c *remoteClient) ExecuteFlow(ctx context.Context, r *connect.Request[v1.ExecuteFlowRequest]) (*connect.Response[v1.ExecuteFlowResponse], error) {
	return c.rpc.ExecuteFlow(ctx, r)
}
func (c *remoteClient) GetFlow(ctx context.Context, r *connect.Request[v1.GetFlowRequest]) (*connect.Response[v1.GetFlowResponse], error) {
	return c.rpc.GetFlow(ctx, r)
}
func (c *remoteClient) ListAccountActivity(ctx context.Context, r *connect.Request[v1.ListAccountActivityRequest]) (*connect.Response[v1.ListAccountActivityResponse], error) {
	return c.rpc.ListAccountActivity(ctx, r)
}
func (c *remoteClient) TakeBalanceSnapshot(ctx context.Context, r *connect.Request[v1.TakeBalanceSnapshotRequest]) (*connect.Response[v1.TakeBalanceSnapshotResponse], error) {
	return c.rpc.TakeBalanceSnapshot(ctx, r)
}
func (c *remoteClient) CreateReservation(ctx context.Context, r *connect.Request[v1.CreateReservationRequest]) (*connect.Response[v1.CreateReservationResponse], error) {
	return c.rpc.CreateReservation(ctx, r)
}
func (c *remoteClient) CommitReservation(ctx context.Context, r *connect.Request[v1.CommitReservationRequest]) (*connect.Response[v1.CommitReservationResponse], error) {
	return c.rpc.CommitReservation(ctx, r)
}
func (c *remoteClient) ReleaseReservation(ctx context.Context, r *connect.Request[v1.ReleaseReservationRequest]) (*connect.Response[v1.ReleaseReservationResponse], error) {
	return c.rpc.ReleaseReservation(ctx, r)
}
func (c *remoteClient) GetReservation(ctx context.Context, r *connect.Request[v1.GetReservationRequest]) (*connect.Response[v1.GetReservationResponse], error) {
	return c.rpc.GetReservation(ctx, r)
}
func (c *remoteClient) ListReservations(ctx context.Context, r *connect.Request[v1.ListReservationsRequest]) (*connect.Response[v1.ListReservationsResponse], error) {
	return c.rpc.ListReservations(ctx, r)
}
func (c *remoteClient) ExecuteExchange(ctx context.Context, r *connect.Request[v1.ExecuteExchangeRequest]) (*connect.Response[v1.ExecuteExchangeResponse], error) {
	return c.rpc.ExecuteExchange(ctx, r)
}
func (c *remoteClient) PutFXRate(ctx context.Context, r *connect.Request[v1.PutFXRateRequest]) (*connect.Response[v1.PutFXRateResponse], error) {
	return c.rpc.PutFXRate(ctx, r)
}
func (c *remoteClient) GetFXRate(ctx context.Context, r *connect.Request[v1.GetFXRateRequest]) (*connect.Response[v1.GetFXRateResponse], error) {
	return c.rpc.GetFXRate(ctx, r)
}
func (c *remoteClient) ListFXRates(ctx context.Context, r *connect.Request[v1.ListFXRatesRequest]) (*connect.Response[v1.ListFXRatesResponse], error) {
	return c.rpc.ListFXRates(ctx, r)
}
func (c *remoteClient) IngestExternalRecords(ctx context.Context, r *connect.Request[v1.IngestExternalRecordsRequest]) (*connect.Response[v1.IngestExternalRecordsResponse], error) {
	return c.rpc.IngestExternalRecords(ctx, r)
}
func (c *remoteClient) RunReconciliation(ctx context.Context, r *connect.Request[v1.RunReconciliationRequest]) (*connect.Response[v1.RunReconciliationResponse], error) {
	return c.rpc.RunReconciliation(ctx, r)
}
func (c *remoteClient) GetReconciliationBatch(ctx context.Context, r *connect.Request[v1.GetReconciliationBatchRequest]) (*connect.Response[v1.GetReconciliationBatchResponse], error) {
	return c.rpc.GetReconciliationBatch(ctx, r)
}
func (c *remoteClient) ListDiscrepancies(ctx context.Context, r *connect.Request[v1.ListDiscrepanciesRequest]) (*connect.Response[v1.ListDiscrepanciesResponse], error) {
	return c.rpc.ListDiscrepancies(ctx, r)
}
func (c *remoteClient) ResolveDiscrepancy(ctx context.Context, r *connect.Request[v1.ResolveDiscrepancyRequest]) (*connect.Response[v1.ResolveDiscrepancyResponse], error) {
	return c.rpc.ResolveDiscrepancy(ctx, r)
}

var _ Client = (*remoteClient)(nil)
