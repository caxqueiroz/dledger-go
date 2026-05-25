// Package dledger is the public Go SDK for the dledger-go multi-currency
// double-entry ledger.
//
// Two backends are available and share the same Client interface:
//
//   - NewEmbedded opens an in-process ledger backed by SQLite or CockroachDB.
//   - NewRemote returns a Connect-RPC client that talks to a hosted server.
//
// The Wallet helper layers idiomatic prediction-market operations (Deposit,
// Reserve, Commit, Release, Settle, Withdraw, GetWallet) on top of either
// backend. The SDK never invents account IDs; callers pass funding,
// destination, pool, and withdrawal account IDs per call.
package dledger
