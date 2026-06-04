// pkg/dledger/options.go
package dledger

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/caxqueiroz/dledger-go/internal/outbox"
)

// Backend selects the embedded store implementation.
type Backend string

const (
	SQLite Backend = "sqlite"
	CRDB   Backend = "crdb"
	// DynamoDB stores the ledger on a DynamoDB-compatible endpoint. DSN is the
	// table name; endpoint/region/credentials come from standard AWS env vars.
	DynamoDB Backend = "dynamodb"
)

// MigrateMode controls whether NewEmbedded runs goose migrations on open.
type MigrateMode int

const (
	// MigrateAuto runs goose up against the embedded migrations FS on Open.
	MigrateAuto MigrateMode = iota
	// MigrateSkip leaves migrations to the operator (cmd/migrate).
	MigrateSkip
)

// Sink is a re-export of outbox.Sink. Implement it to receive ledger events
// emitted by the embedded backend.
type Sink = outbox.Sink

// Options configures NewEmbedded.
type Options struct {
	Backend     Backend
	DSN         string
	MigrateMode MigrateMode
	// OutboxSink receives ledger events from the embedded backend. Defaults to
	// a LogSink backed by Options.Logger.
	OutboxSink       Sink
	DisableScheduler bool
	Logger           *slog.Logger // default slog.Default()
	// SchedulerExpiryTick overrides the scheduler's expiry tick interval.
	// Zero uses the default (30s). Useful in tests.
	SchedulerExpiryTick time.Duration
}

// Option is a functional option for NewRemote. For NewEmbedded configuration,
// use the Options struct instead.
type Option func(*remoteOptions)

type remoteOptions struct {
	httpClient *http.Client
}

// WithHTTPClient overrides the http.Client used by NewRemote.
// Set this to configure TLS, timeouts, or proxies.
func WithHTTPClient(c *http.Client) Option {
	return func(o *remoteOptions) { o.httpClient = c }
}
