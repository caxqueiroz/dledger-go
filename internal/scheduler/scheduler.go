// internal/scheduler/scheduler.go
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1"
	"github.com/caxqueiroz/doubleledger/internal/repo"
	"github.com/caxqueiroz/doubleledger/internal/service"
)

type Snapshotter interface {
	TakeBalanceSnapshot(ctx context.Context, req *connect.Request[ledgerv1.TakeBalanceSnapshotRequest]) (*connect.Response[ledgerv1.TakeBalanceSnapshotResponse], error)
}

// Reservations is satisfied by *service.Server once Phase B (Task 21) adds
// ExpireReservation. Until then, the runtime cast in New returns nil and the
// expiryTick is a no-op.
type Reservations interface {
	ExpireReservation(ctx context.Context, tenantID, reservationID string) error
}

type Config struct {
	SnapshotTick     time.Duration // default 5m
	SnapshotInterval time.Duration // default 24h
	ExpiryTick       time.Duration // default 30s
	BatchN           int           // default 100
}

type Scheduler struct {
	Store        repo.Store
	Snapshotter  Snapshotter
	Reservations Reservations
	Cfg          Config
	Log          *slog.Logger
}

func New(store repo.Store, srv *service.Server) *Scheduler {
	var resv Reservations
	if r, ok := any(srv).(Reservations); ok {
		resv = r
	}
	return &Scheduler{
		Store:        store,
		Snapshotter:  srv,
		Reservations: resv,
		Cfg: Config{
			SnapshotTick:     5 * time.Minute,
			SnapshotInterval: 24 * time.Hour,
			ExpiryTick:       30 * time.Second,
			BatchN:           100,
		},
		Log: slog.Default(),
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	snapT := time.NewTicker(s.Cfg.SnapshotTick)
	defer snapT.Stop()
	expT := time.NewTicker(s.Cfg.ExpiryTick)
	defer expT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-snapT.C:
			s.snapshotTick(ctx)
		case <-expT.C:
			s.expiryTick(ctx)
		}
	}
}

func (s *Scheduler) snapshotTick(_ context.Context) {
	// Phase A: tenant discovery / per-tenant scheduling lands in a follow-up.
	// Operators trigger snapshots manually via the TakeBalanceSnapshot RPC.
}

func (s *Scheduler) expiryTick(ctx context.Context) {
	if s.Reservations == nil {
		return
	}
	now := time.Now()
	rows, err := s.Store.ListExpiredReservations(ctx, now, s.Cfg.BatchN)
	if err != nil {
		s.Log.WarnContext(ctx, "scheduler.list_expired", "err", err)
		return
	}
	for _, r := range rows {
		if err := s.Reservations.ExpireReservation(ctx, r.TenantID, r.ID); err != nil {
			s.Log.WarnContext(ctx, "scheduler.expire", "tenant_id", r.TenantID, "id", r.ID, "err", err)
		}
	}
}
