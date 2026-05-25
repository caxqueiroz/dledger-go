// pkg/dledger/embedded.go
package dledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"

	ledgerv1connect "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/dledger-go/internal/outbox"
	"github.com/caxqueiroz/dledger-go/internal/repo"
	"github.com/caxqueiroz/dledger-go/internal/repo/crdb"
	"github.com/caxqueiroz/dledger-go/internal/repo/sqlite"
	"github.com/caxqueiroz/dledger-go/internal/scheduler"
	"github.com/caxqueiroz/dledger-go/internal/sdk"
	"github.com/caxqueiroz/dledger-go/internal/service"
)

// NewEmbedded boots an in-process dledger and returns a Client that delegates
// to the local *service.Server. The returned Client owns the database
// connection, the snapshot/expiry/retention scheduler, and the outbox
// dispatcher. Close releases all of them.
func NewEmbedded(ctx context.Context, opts Options) (Client, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.DSN == "" {
		return nil, errors.New("dledger: Options.DSN required")
	}

	var (
		store   repo.Store
		migFs   = sdk.SQLiteMigrations()
		migDrv  = "sqlite"
		migDial = "sqlite3"
	)
	switch opts.Backend {
	case SQLite, "":
		s, err := sqlite.Open(ctx, opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("open sqlite: %w", err)
		}
		store = s
	case CRDB:
		s, err := crdb.Open(ctx, opts.DSN)
		if err != nil {
			return nil, fmt.Errorf("open crdb: %w", err)
		}
		store = s
		migFs = sdk.CRDBMigrations()
		migDrv = "pgx"
		migDial = "postgres"
	default:
		return nil, fmt.Errorf("dledger: unknown backend %q", opts.Backend)
	}

	if opts.MigrateMode == MigrateAuto {
		if err := runMigrations(migDrv, opts.DSN, migFs, migDial); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}

	srv := service.New(store)

	sink := opts.OutboxSink
	if sink == nil {
		sink = outbox.LogSink{Logger: logger}
	}
	disp := outbox.NewDispatcher(outbox.RepoAdapter{Store: store}, sink, outbox.Config{
		Interval: 250 * time.Millisecond, BatchSize: 100,
	})

	bgCtx, cancel := context.WithCancel(context.Background())
	go disp.Run(bgCtx)

	if !opts.DisableScheduler {
		sched := scheduler.New(store, srv)
		sched.Log = logger
		go sched.Run(bgCtx)
	}

	return &embeddedClient{
		LedgerServiceHandler: srv,
		store:                store,
		cancel:               cancel,
	}, nil
}

// embeddedClient delegates the 22 RPCs to *service.Server (which satisfies
// the generated handler interface) and adds Close.
type embeddedClient struct {
	// Embedding the generated handler interface auto-implements all 22 methods.
	ledgerv1connect.LedgerServiceHandler
	store  repo.Store
	cancel context.CancelFunc
	closed bool
}

func (c *embeddedClient) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.store.Close()
}

// runMigrations executes `goose up` against an embedded filesystem.
func runMigrations(driver, dsn string, fsys fs.FS, dialect string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("open for migrate: %w", err)
	}
	defer db.Close()
	if err := goose.SetDialect(dialect); err != nil {
		return err
	}
	goose.SetBaseFS(fsys)
	return goose.Up(db, ".")
}

var _ Client = (*embeddedClient)(nil)
