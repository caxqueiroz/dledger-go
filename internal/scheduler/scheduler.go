// internal/scheduler/scheduler.go
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/internal/repo"
	"github.com/caxqueiroz/dledger-go/internal/service"
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
	RetentionTick    time.Duration // default 1h
	RetentionAge     time.Duration // default 90 * 24h
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
			RetentionTick:    1 * time.Hour,
			RetentionAge:     90 * 24 * time.Hour,
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
	retT := time.NewTicker(s.Cfg.RetentionTick)
	defer retT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-snapT.C:
			s.snapshotTick(ctx)
		case <-expT.C:
			s.expiryTick(ctx)
		case <-retT.C:
			s.retentionTick(ctx)
		}
	}
}

func (s *Scheduler) snapshotTick(ctx context.Context) {
	cutoff := time.Now().Add(-s.Cfg.SnapshotInterval)
	tenants, err := s.Store.ListTenantsDueForSnapshot(ctx, cutoff, s.Cfg.BatchN)
	if err != nil {
		s.Log.WarnContext(ctx, "scheduler.list_tenants_due", "err", err)
		return
	}
	for _, tenantID := range tenants {
		req := connect.NewRequest(&ledgerv1.TakeBalanceSnapshotRequest{TenantId: tenantID})
		resp, err := s.Snapshotter.TakeBalanceSnapshot(ctx, req)
		if err != nil {
			s.Log.WarnContext(ctx, "scheduler.snapshot", "tenant_id", tenantID, "err", err)
			continue
		}
		s.Log.InfoContext(ctx, "scheduler.snapshot.taken",
			"tenant_id", tenantID, "count", resp.Msg.GetSnapshotsTaken())
	}
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

func (s *Scheduler) retentionTick(ctx context.Context) {
	cutoff := time.Now().Add(-s.Cfg.RetentionAge)
	n, err := s.Store.PruneSnapshotsOlderThan(ctx, cutoff, s.Cfg.BatchN)
	if err != nil {
		s.Log.WarnContext(ctx, "scheduler.retention.error", "err", err)
		return
	}
	if n > 0 {
		s.Log.InfoContext(ctx, "scheduler.retention.deleted",
			"count", n, "cutoff", cutoff.Format(time.RFC3339))
	}
}

// RetentionTickForTest exposes retentionTick for deterministic test invocations.
// Production code uses the scheduler's Run loop.
func (s *Scheduler) RetentionTickForTest(ctx context.Context) {
	s.retentionTick(ctx)
}
