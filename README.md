# dledger-go

A product-neutral, multi-currency, double-entry ledger service in Go.

[![Connect-RPC](https://img.shields.io/badge/api-Connect--RPC-2C3E50)](https://connectrpc.com/)
[![Go 1.26](https://img.shields.io/badge/go-1.26-00ADD8)](https://go.dev/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

dledger-go owns double-entry accounting, multi-currency balances, reservations with auto-expiry, point-in-time balance snapshots, idempotent atomic flows, and a transactional outbox. Product engines submit accounting *intents* via Connect-RPC — they never mutate balances directly.

## Highlights

- **Double-entry, multi-currency, per-currency balanced**. One `Journal` may move USD and BRL in the same transaction; each currency must self-balance.
- **Atomic multi-step flows**. `ExecuteFlow` runs N steps in one DB transaction. Any failure rolls back the entire flow including outbox events.
- **First-class reservations**. `HELD → PARTIAL → COMMITTED|RELEASED|EXPIRED` with partial commits / releases and scheduler-driven auto-expiry.
- **Point-in-time balances**. `TakeBalanceSnapshot` captures rows; `GetBalance(as_of=T)` reconstructs balances at any past time.
- **Idempotency everywhere**. Required `idempotency_key` on every flow / reservation transition / outbox event.
- **Two backends behind one interface**. CockroachDB (production) with serializable + 40001 retry; SQLite (`modernc.org/sqlite`) for local dev. Same code, same tests.
- **Tenant-scoped from day one**. `X-Tenant-Id` interceptor + filtered queries on every table.
- **Connect-RPC**. gRPC + gRPC-Web + HTTP/JSON over a single endpoint, generated Go and TypeScript clients.
- **Transactional outbox**. Events written inside the tx; a polling `Dispatcher` publishes after commit via a pluggable `Sink`.

## Quickstart

```bash
git clone https://github.com/caxqueiroz/dledger-go.git
cd dledger-go
make tools          # installs buf, sqlc, goose, protoc plugins
make generate       # proto + sqlc codegen
make build          # bin/server + bin/migrate

./bin/migrate --backend=sqlite --dsn=./ledger.db up
./bin/server   --backend=sqlite --dsn=./ledger.db
```

The server listens on `:8080` by default. Health check: `curl http://localhost:8080/healthz`.

Run the curl walkthrough in another shell:

```bash
./examples/curl/quickstart.sh
```

Or a Go example:

```bash
go run ./examples/go/place_order
go run ./examples/go/reservations
go run ./examples/go/snapshots
```

See [`examples/README.md`](examples/README.md) for the full list.

## API surface

Eleven Connect-RPC methods, all under `ledger.v1.LedgerService`:

| RPC | Purpose |
|---|---|
| `CreateAccount` | Open a ledger account |
| `GetAccount` | Fetch an account by id |
| `GetBalance` | Current or `as_of` historical balance |
| `PostJournal` | Single-step balanced journal |
| `ExecuteFlow` | Atomic multi-step flow with idempotency |
| `GetFlow` | Replay a completed flow's result |
| `ListAccountActivity` | Per-account entry history |
| `TakeBalanceSnapshot` | Capture current balance(s) |
| `CreateReservation` | Hold funds with optional expiry |
| `CommitReservation` | Move all-or-part of a held amount to a destination |
| `ReleaseReservation` | Return all-or-part of a held amount to source |
| `GetReservation` | Inspect a reservation |

The reservation auto-expiry is driven by an in-process scheduler — not an RPC.

Proto source: [`proto/ledger/v1/ledger.proto`](proto/ledger/v1/ledger.proto).

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full architecture document: layers, data model, key flows (`ExecuteFlow` orchestration, reservation lifecycle, snapshot reconstruction), concurrency model, idempotency, outbox semantics, error mapping, observability, and extension points.

Spec and plan documents live under [`docs/superpowers/`](docs/superpowers/).

## Configuration

| Flag | Default | Notes |
|---|---|---|
| `--backend` | `sqlite` | `sqlite` or `crdb` |
| `--dsn` | `./ledger.db` | SQLite path or CRDB connection string |
| `--addr` | `:8080` | Listen address |

| Env var | Effect |
|---|---|
| `DATABASE_URL` | Used as `--dsn` when `--backend=crdb` and `--dsn` is empty |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Enables OTLP/HTTP trace export |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Same; takes precedence over the above |
| `OTEL_EXPORTER_OTLP_INSECURE=true` | Disables TLS on the OTLP exporter |

## Development

```bash
make test               # unit tests (no Docker required)
make test-integration   # CRDB integration tests via testcontainers-go (Docker required)
make lint               # golangci-lint (if installed)
```

Migrations live in [`sql/migrations/{sqlite,crdb}/`](sql/migrations). Add a new `NNNN_name.sql` per goose conventions and bump the dialect-specific files together.

## Project layout

```
proto/                          .proto contracts
gen/proto/                      generated Go + Connect handlers
gen/{sqlite,crdb}/              sqlc-generated query bindings
sql/migrations/{sqlite,crdb}/   goose migrations
sql/queries/{sqlite,crdb}/      sqlc query sources
internal/ledger/                pure domain types and rules
internal/repo/                  Store + Tx interfaces
internal/repo/{sqlite,crdb}/    backend implementations
internal/service/               Connect-RPC handlers + ExecuteFlow orchestrator
internal/scheduler/             expiry + snapshot tickers
internal/outbox/                Sink interface + polling Dispatcher
internal/observability/         OTel setup
cmd/server/                     HTTP entrypoint
cmd/migrate/                    goose CLI wrapper
examples/                       Go + curl + React walkthroughs
docs/                           Architecture + specs + plans
```

## License

Apache 2.0. See [LICENSE](LICENSE).
