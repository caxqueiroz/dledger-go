# doubleledger

Multi-currency double-entry ledger service. See `docs/superpowers/specs/` for the design.

## Local dev

```
make tools
make generate
BACKEND=sqlite make migrate-up
make serve
```

Defaults to SQLite at `./ledger.db`. Set `BACKEND=crdb` and `DATABASE_URL` for CockroachDB.
