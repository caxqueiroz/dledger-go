# Examples

End-to-end walkthroughs of the dledger-go service. Each example is a single-file Go program (or shell script) that talks to a running server.

## Prerequisites

Start the server in one terminal:

```bash
make build
./bin/migrate --backend=sqlite --dsn=./ledger.db up
./bin/server --backend=sqlite --dsn=./ledger.db
```

Then run any example below in another terminal.

## Go examples

| Path | What it shows |
|---|---|
| [`go/place_order`](go/place_order/main.go) | Canonical `PLACE_ORDER` flow — reserve 100 USD from cash_available into cash_reserved inside one atomic `ExecuteFlow`. Also creates accounts, posts the seed journal, and reads back balances. |
| [`go/reservations`](go/reservations/main.go) | Full reservation lifecycle: create → partial commit → partial release → final commit, plus idempotent replay. |
| [`go/snapshots`](go/snapshots/main.go) | `TakeBalanceSnapshot` + `GetBalance(as_of=T)` historical reconstruction. |
| [`go/fx_exchange`](go/fx_exchange/main.go) | `ExecuteExchange` walkthrough — record a USD/EUR rate, exchange 100 USD for 89.50 EUR via the platform's FX desk. |
| [`go/fx_revaluation`](go/fx_revaluation/main.go) | Documented `fx_pnl` pattern via raw `ExecuteFlow`: exchange-with-residual and end-of-day mark-to-market. |
| [`go/reconciliation`](go/reconciliation/main.go) | Ingest external records, run reconciliation, list discrepancies, resolve one with an adjustment journal. |

Run with:

```bash
go run ./examples/go/<name>
```

## curl quickstart

[`curl/quickstart.sh`](curl/quickstart.sh) — uses Connect's JSON codec to exercise the API from the shell:

```bash
./examples/curl/quickstart.sh
```

Requires `jq`. Optionally set `URL=...` and `TENANT=...` env vars to point at a different server or tenant.

## React

[`react/`](react/) — minimal TypeScript client using the auto-generated Connect-ES bindings. See `react/README.md`. The folder includes a `package.json` and `tsconfig.json`; run `npm install && npm run typecheck` to verify locally.

## Authentication

All examples set the `X-Tenant-Id` header on every request — the server's tenant interceptor rejects requests without it. The Go examples use a custom `http.RoundTripper`; the curl script sets the header explicitly; the React example sets it per call.
