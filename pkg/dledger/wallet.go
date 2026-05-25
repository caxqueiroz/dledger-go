// pkg/dledger/wallet.go
package dledger

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	v1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

// Wallet is the prediction-market-focused convenience layer over Client.
// Safe for concurrent use; stateless beyond the captured tenant and owner type.
type Wallet struct {
	client    Client
	tenant    string
	ownerType string
}

// WalletOption configures a Wallet at construction.
type WalletOption func(*Wallet)

// WithOwnerType overrides the default owner_type ("user") used to derive
// per-player account IDs.
func WithOwnerType(t string) WalletOption {
	return func(w *Wallet) { w.ownerType = t }
}

// NewWallet returns a Wallet bound to the given client + tenant.
func NewWallet(c Client, tenantID string, opts ...WalletOption) *Wallet {
	w := &Wallet{client: c, tenant: tenantID, ownerType: "user"}
	for _, fn := range opts {
		fn(w)
	}
	return w
}

// EnsurePlayerAccounts idempotently creates the two debit-normal accounts
// (cash_available, cash_reserved) for a player and returns their IDs.
func (w *Wallet) EnsurePlayerAccounts(ctx context.Context, playerID, currency string) (PlayerAccounts, error) {
	avail := w.accountID(playerID, "cash_available", currency)
	resv := w.accountID(playerID, "cash_reserved", currency)
	if err := w.ensureAccount(ctx, playerID, "cash_available", currency); err != nil {
		return PlayerAccounts{}, fmt.Errorf("ensure cash_available: %w", err)
	}
	if err := w.ensureAccount(ctx, playerID, "cash_reserved", currency); err != nil {
		return PlayerAccounts{}, fmt.Errorf("ensure cash_reserved: %w", err)
	}
	return PlayerAccounts{Available: avail, Reserved: resv}, nil
}

func (w *Wallet) accountID(ownerID, acctType, currency string) string {
	return fmt.Sprintf("%s:%s:%s:%s", w.ownerType, ownerID, acctType, currency)
}

// ensureAccount creates an account and swallows "already exists" errors.
// Detection layers: connect.CodeAlreadyExists (if surfaced), then a
// GetAccount probe as a backstop for SQL-generic primary-key conflicts.
func (w *Wallet) ensureAccount(ctx context.Context, ownerID, acctType, currency string) error {
	_, err := w.client.CreateAccount(ctx, connect.NewRequest(&v1.CreateAccountRequest{
		TenantId: w.tenant, OwnerType: w.ownerType, OwnerId: ownerID,
		AccountType: acctType, Currency: currency,
		NormalBalance: v1.NormalBalance_NORMAL_BALANCE_DEBIT,
	}))
	if err == nil {
		return nil
	}
	if connect.CodeOf(err) == connect.CodeAlreadyExists {
		return nil
	}
	if _, ge := w.client.GetAccount(ctx, connect.NewRequest(&v1.GetAccountRequest{
		TenantId: w.tenant, AccountId: w.accountID(ownerID, acctType, currency),
	})); ge == nil {
		return nil
	}
	return err
}
