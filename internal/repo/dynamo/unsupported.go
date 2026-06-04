package dynamo

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// errUnsupported returns a DomainError signalling that the named operation is
// not implemented by the DynamoDB backend in this milestone.
func errUnsupported(op string) error {
	return ledger.NewDomainError(ledger.CodeUnsupportedOperation, "dynamodb backend: "+op+" not supported")
}

// ---------------------------------------------------------------------------
// ListAccountActivity
// ---------------------------------------------------------------------------

func (s *Store) ListAccountActivity(_ context.Context, _ repo.ListActivityInput) ([]repo.ActivityRow, error) {
	return nil, errUnsupported("ListAccountActivity")
}

// ---------------------------------------------------------------------------
// Snapshots (unsupported — except quiet scheduler no-ops below)
// ---------------------------------------------------------------------------

func (s *Store) InsertSnapshot(_ context.Context, _ ledger.BalanceSnapshot) error {
	return errUnsupported("InsertSnapshot")
}

func (s *Store) GetSnapshotBefore(_ context.Context, _, _, _ string, _ time.Time) (*ledger.BalanceSnapshot, error) {
	return nil, errUnsupported("GetSnapshotBefore")
}

func (s *Store) SumEntriesBetween(_ context.Context, _, _, _ string, _, _ time.Time) (decimal.Decimal, decimal.Decimal, error) {
	return decimal.Zero, decimal.Zero, errUnsupported("SumEntriesBetween")
}

func (s *Store) ListTenantBalances(_ context.Context, _ string) ([]repo.TenantBalanceRow, error) {
	return nil, errUnsupported("ListTenantBalances")
}

// ListTenantsDueForSnapshot is a quiet scheduler no-op: returns (nil, nil) so
// the snapshot scheduler simply finds nothing to do rather than crashing.
func (s *Store) ListTenantsDueForSnapshot(_ context.Context, _ time.Time, _ int) ([]string, error) {
	return nil, nil
}

// PruneSnapshotsOlderThan is a quiet scheduler no-op: returns (0, nil).
func (s *Store) PruneSnapshotsOlderThan(_ context.Context, _ time.Time, _ int) (int64, error) {
	return 0, nil
}

// ---------------------------------------------------------------------------
// FX rates
// ---------------------------------------------------------------------------

func (s *Store) UpsertFXRate(_ context.Context, _ ledger.FXRate) (*ledger.FXRate, error) {
	return nil, errUnsupported("UpsertFXRate")
}

func (s *Store) GetFXRateAt(_ context.Context, _, _, _ string, _ time.Time) (*ledger.FXRate, error) {
	return nil, errUnsupported("GetFXRateAt")
}

func (s *Store) ListFXRates(_ context.Context, _ repo.ListFXRatesInput) ([]ledger.FXRate, error) {
	return nil, errUnsupported("ListFXRates")
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

func (s *Store) InsertExternalRecord(_ context.Context, _ ledger.ExternalRecord) (bool, error) {
	return false, errUnsupported("InsertExternalRecord")
}

func (s *Store) ListExternalRecordsForRecon(_ context.Context, _, _ string, _, _ time.Time) ([]ledger.ExternalRecord, error) {
	return nil, errUnsupported("ListExternalRecordsForRecon")
}

func (s *Store) ListJournalsForRecon(_ context.Context, _, _ string, _, _ time.Time) ([]ledger.Journal, error) {
	return nil, errUnsupported("ListJournalsForRecon")
}

func (s *Store) GetReconBatch(_ context.Context, _, _ string) (*ledger.ReconciliationBatch, error) {
	return nil, errUnsupported("GetReconBatch")
}

func (s *Store) ListDiscrepancies(_ context.Context, _ repo.ListDiscrepanciesInput) ([]ledger.Discrepancy, error) {
	return nil, errUnsupported("ListDiscrepancies")
}

func (s *Store) GetDiscrepancy(_ context.Context, _, _ string) (*ledger.Discrepancy, error) {
	return nil, errUnsupported("GetDiscrepancy")
}
