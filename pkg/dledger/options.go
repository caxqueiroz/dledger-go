// pkg/dledger/options.go
package dledger

import (
	"log/slog"
	"net/http"

	"github.com/caxqueiroz/dledger-go/internal/outbox"
)

// Backend selects the embedded store implementation.
type Backend string

const (
	SQLite Backend = "sqlite"
	CRDB   Backend = "crdb"
)

// MigrateMode controls whether NewEmbedded runs goose migrations on open.
type MigrateMode int

const (
	// MigrateAuto runs goose up against the embedded migrations FS on Open.
	MigrateAuto MigrateMode = iota
	// MigrateSkip leaves migrations to the operator (cmd/migrate).
	MigrateSkip
)

// Options configures NewEmbedded.
type Options struct {
	Backend          Backend
	DSN              string
	MigrateMode      MigrateMode
	OutboxSink       outbox.Sink  // default outbox.LogSink with Logger
	DisableScheduler bool
	Logger           *slog.Logger // default slog.Default()
}

// Option is a functional option for NewRemote.
type Option func(*remoteOptions)

type remoteOptions struct {
	httpClient *http.Client
	logger     *slog.Logger
}

// WithHTTPClient overrides the http.Client used by NewRemote.
// Set this to configure TLS, timeouts, or proxies.
func WithHTTPClient(c *http.Client) Option {
	return func(o *remoteOptions) { o.httpClient = c }
}

// WithLogger overrides the logger used by NewRemote.
func WithLogger(l *slog.Logger) Option {
	return func(o *remoteOptions) { o.logger = l }
}
