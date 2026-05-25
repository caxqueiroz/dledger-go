// Package sdk exposes embedded migration files and other shared SDK internals.
package sdk

import (
	"embed"
	"io/fs"
)

//go:embed migrations/sqlite/*.sql migrations/crdb/*.sql
var migrationsFS embed.FS

// SQLiteMigrations returns an fs.FS rooted at the SQLite migrations dir.
// goose expects migration files at the FS root, so the prefix is stripped.
func SQLiteMigrations() fs.FS {
	sub, err := fs.Sub(migrationsFS, "migrations/sqlite")
	if err != nil {
		panic(err) // unreachable; the directory is statically embedded
	}
	return sub
}

// CRDBMigrations returns an fs.FS rooted at the CRDB migrations dir.
func CRDBMigrations() fs.FS {
	sub, err := fs.Sub(migrationsFS, "migrations/crdb")
	if err != nil {
		panic(err)
	}
	return sub
}
