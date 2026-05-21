package outbox

import (
	"context"
	"log/slog"
	"time"
)

type Store interface {
	PendingOutbox(ctx context.Context, limit int) ([]Event, error)
	MarkOutboxPublished(ctx context.Context, id string) error
	IncrementOutboxAttempts(ctx context.Context, id string) error
}

type Config struct {
	Interval  time.Duration
	BatchSize int
}

type Dispatcher struct {
	store Store
	sink  Sink
	cfg   Config
	log   *slog.Logger
}

func NewDispatcher(store Store, sink Sink, cfg Config) *Dispatcher {
	if cfg.Interval == 0 {
		cfg.Interval = 250 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	return &Dispatcher{store: store, sink: sink, cfg: cfg, log: slog.Default()}
}

func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.tick(ctx)
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) {
	events, err := d.store.PendingOutbox(ctx, d.cfg.BatchSize)
	if err != nil {
		d.log.WarnContext(ctx, "outbox.pending.error", "err", err)
		return
	}
	for _, e := range events {
		if err := d.sink.Publish(ctx, e); err != nil {
			d.log.WarnContext(ctx, "outbox.publish.error", "id", e.ID, "err", err)
			_ = d.store.IncrementOutboxAttempts(ctx, e.ID)
			continue
		}
		if err := d.store.MarkOutboxPublished(ctx, e.ID); err != nil {
			d.log.WarnContext(ctx, "outbox.mark.error", "id", e.ID, "err", err)
		}
	}
}
