// pkg/dledger/client.go
package dledger

import (
	"context"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// Client is the mode-agnostic ledger surface. Both NewEmbedded and NewRemote
// return implementations of this interface. Swapping modes requires no other
// code change.
type Client interface {
	// Accounts
	CreateAccount(context.Context, *connect.Request[v1.CreateAccountRequest]) (*connect.Response[v1.CreateAccountResponse], error)
	GetAccount(context.Context, *connect.Request[v1.GetAccountRequest]) (*connect.Response[v1.GetAccountResponse], error)
	GetBalance(context.Context, *connect.Request[v1.GetBalanceRequest]) (*connect.Response[v1.GetBalanceResponse], error)

	// Journals + flows
	PostJournal(context.Context, *connect.Request[v1.PostJournalRequest]) (*connect.Response[v1.PostJournalResponse], error)
	ExecuteFlow(context.Context, *connect.Request[v1.ExecuteFlowRequest]) (*connect.Response[v1.ExecuteFlowResponse], error)
	GetFlow(context.Context, *connect.Request[v1.GetFlowRequest]) (*connect.Response[v1.GetFlowResponse], error)
	ListAccountActivity(context.Context, *connect.Request[v1.ListAccountActivityRequest]) (*connect.Response[v1.ListAccountActivityResponse], error)

	// Snapshots
	TakeBalanceSnapshot(context.Context, *connect.Request[v1.TakeBalanceSnapshotRequest]) (*connect.Response[v1.TakeBalanceSnapshotResponse], error)

	// Reservations
	CreateReservation(context.Context, *connect.Request[v1.CreateReservationRequest]) (*connect.Response[v1.CreateReservationResponse], error)
	CommitReservation(context.Context, *connect.Request[v1.CommitReservationRequest]) (*connect.Response[v1.CommitReservationResponse], error)
	ReleaseReservation(context.Context, *connect.Request[v1.ReleaseReservationRequest]) (*connect.Response[v1.ReleaseReservationResponse], error)
	GetReservation(context.Context, *connect.Request[v1.GetReservationRequest]) (*connect.Response[v1.GetReservationResponse], error)
	ListReservations(context.Context, *connect.Request[v1.ListReservationsRequest]) (*connect.Response[v1.ListReservationsResponse], error)

	// FX
	ExecuteExchange(context.Context, *connect.Request[v1.ExecuteExchangeRequest]) (*connect.Response[v1.ExecuteExchangeResponse], error)
	PutFXRate(context.Context, *connect.Request[v1.PutFXRateRequest]) (*connect.Response[v1.PutFXRateResponse], error)
	GetFXRate(context.Context, *connect.Request[v1.GetFXRateRequest]) (*connect.Response[v1.GetFXRateResponse], error)
	ListFXRates(context.Context, *connect.Request[v1.ListFXRatesRequest]) (*connect.Response[v1.ListFXRatesResponse], error)

	// Reconciliation
	IngestExternalRecords(context.Context, *connect.Request[v1.IngestExternalRecordsRequest]) (*connect.Response[v1.IngestExternalRecordsResponse], error)
	RunReconciliation(context.Context, *connect.Request[v1.RunReconciliationRequest]) (*connect.Response[v1.RunReconciliationResponse], error)
	GetReconciliationBatch(context.Context, *connect.Request[v1.GetReconciliationBatchRequest]) (*connect.Response[v1.GetReconciliationBatchResponse], error)
	ListDiscrepancies(context.Context, *connect.Request[v1.ListDiscrepanciesRequest]) (*connect.Response[v1.ListDiscrepanciesResponse], error)
	ResolveDiscrepancy(context.Context, *connect.Request[v1.ResolveDiscrepancyRequest]) (*connect.Response[v1.ResolveDiscrepancyResponse], error)

	// Close releases background resources (scheduler, dispatcher, DB connections).
	// Idempotent — safe to call multiple times. Not safe to call concurrently
	// with in-flight requests; callers should drain pending requests first.
	Close() error
}
