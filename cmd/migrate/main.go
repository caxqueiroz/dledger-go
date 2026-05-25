package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

func main() {
	backend := flag.String("backend", "sqlite", "sqlite|crdb")
	dir := flag.String("dir", "internal/sdk/migrations", "migrations root")
	dsn := flag.String("dsn", "", "database DSN (defaults: sqlite=./ledger.db, crdb=$DATABASE_URL)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: migrate --backend=sqlite|crdb [--dsn=...] up|down|status|reset")
		os.Exit(2)
	}
	cmd := args[0]

	var (
		driver string
		path   string
	)
	switch *backend {
	case "sqlite":
		driver = "sqlite"
		if *dsn == "" {
			*dsn = "./ledger.db"
		}
		path = filepath.Join(*dir, "sqlite")
		_ = goose.SetDialect("sqlite3")
	case "crdb":
		driver = "pgx"
		if *dsn == "" {
			*dsn = os.Getenv("DATABASE_URL")
		}
		path = filepath.Join(*dir, "crdb")
		_ = goose.SetDialect("postgres")
	default:
		fmt.Fprintf(os.Stderr, "unknown backend %q\n", *backend)
		os.Exit(2)
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "missing --dsn or DATABASE_URL")
		os.Exit(2)
	}

	db, err := sql.Open(driver, *dsn)
	if err != nil {
		slog.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := goose.RunContext(context.Background(), cmd, db, path); err != nil {
		slog.Error("goose", "err", err)
		os.Exit(1)
	}
}
