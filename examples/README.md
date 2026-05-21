# Examples

## Go client

Start the server:

```
make build
./bin/migrate --backend=sqlite up
./bin/server --backend=sqlite
```

Run the demo (in another shell):

```
go run ./examples/go/client
```

The client creates two accounts, posts a balanced journal, and prints the normalized balance.

## React (skeleton)

See `examples/react/README.md` — a minimal hook calling `GetBalance` via `@connectrpc/connect-web`. No build is included; copy into your own app and point `transport` at the running server.
