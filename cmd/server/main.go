package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"

	ledgerv1connect "github.com/caxqueiroz/doubleledger/gen/proto/ledger/v1/ledgerv1connect"
	"github.com/caxqueiroz/doubleledger/internal/observability"
	"github.com/caxqueiroz/doubleledger/internal/outbox"
	"github.com/caxqueiroz/doubleledger/internal/repo"
	"github.com/caxqueiroz/doubleledger/internal/repo/crdb"
	"github.com/caxqueiroz/doubleledger/internal/repo/sqlite"
	"github.com/caxqueiroz/doubleledger/internal/scheduler"
	"github.com/caxqueiroz/doubleledger/internal/service"
	"github.com/caxqueiroz/doubleledger/internal/service/interceptors"
)

func main() {
	backend := flag.String("backend", "sqlite", "sqlite|crdb")
	dsn := flag.String("dsn", "./ledger.db", "database DSN")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownOtel, err := observability.Setup(ctx, "ledger-service")
	if err != nil {
		log.Error("otel setup", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownOtel(context.Background()) }()

	var store repo.Store
	switch *backend {
	case "sqlite":
		store, err = sqlite.Open(ctx, *dsn)
	case "crdb":
		store, err = crdb.Open(ctx, *dsn)
	default:
		log.Error("unknown backend", "backend", *backend)
		os.Exit(2)
	}
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	srv := service.New(store)

	mux := http.NewServeMux()
	path, handler := ledgerv1connect.NewLedgerServiceHandler(srv,
		connect.WithInterceptors(
			interceptors.NewTenant(),
			interceptors.NewLogging(log),
		),
	)
	mux.Handle(path, handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	disp := outbox.NewDispatcher(outbox.RepoAdapter{Store: store}, outbox.LogSink{Logger: log}, outbox.Config{
		Interval: 250 * time.Millisecond, BatchSize: 100,
	})
	go disp.Run(ctx)

	sched := scheduler.New(store, srv)
	go sched.Run(ctx)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpSrv := &http.Server{
		Addr:      *addr,
		Handler:   mux,
		Protocols: protocols,
	}
	log.Info("server.start", "addr", *addr, "backend", *backend)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Info("server.stopped")
}
