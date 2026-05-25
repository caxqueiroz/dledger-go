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

// Deposit credits the player's cash_available by debiting the FundingAccountID.
// FundingAccountID is the caller-owned mirror account (e.g. the payment
// processor's clearing account in dledger).
//
//	DEBIT  user:<player>:cash_available:<ccy>   amount
//	CREDIT funding_account                       amount
func (w *Wallet) Deposit(ctx context.Context, in DepositInput) (Receipt, error) {
	avail := w.accountID(in.PlayerID, "cash_available", in.Currency)
	return w.postJournal(ctx, postJournalArgs{
		IdempotencyKey: in.IdempotencyKey,
		SourceService:  in.SourceService,
		EventID:        in.ExternalRef,
		Debit:          accountAmount{accountID: avail, currency: in.Currency, amount: in.Amount},
		Credit:         accountAmount{accountID: in.FundingAccountID, currency: in.Currency, amount: in.Amount},
	})
}

// Withdraw moves funds from the player's cash_available to the caller's
// WithdrawalAccountID.
//
//	DEBIT  withdrawal_account                    amount
//	CREDIT user:<player>:cash_available:<ccy>   amount
func (w *Wallet) Withdraw(ctx context.Context, in WithdrawInput) (Receipt, error) {
	avail := w.accountID(in.PlayerID, "cash_available", in.Currency)
	return w.postJournal(ctx, postJournalArgs{
		IdempotencyKey: in.IdempotencyKey,
		SourceService:  in.SourceService,
		EventID:        in.ExternalRef,
		Debit:          accountAmount{accountID: in.WithdrawalAccountID, currency: in.Currency, amount: in.Amount},
		Credit:         accountAmount{accountID: avail, currency: in.Currency, amount: in.Amount},
	})
}

type accountAmount struct {
	accountID string
	currency  string
	amount    string
}

type postJournalArgs struct {
	IdempotencyKey string
	SourceService  string
	EventID        string
	Debit          accountAmount
	Credit         accountAmount
}

func (w *Wallet) postJournal(ctx context.Context, a postJournalArgs) (Receipt, error) {
	resp, err := w.client.PostJournal(ctx, connect.NewRequest(&v1.PostJournalRequest{
		TenantId: w.tenant, IdempotencyKey: a.IdempotencyKey, SourceService: a.SourceService,
		Journal: &v1.Journal{
			EventId:       a.EventID,
			SourceService: a.SourceService,
			Entries: []*v1.Entry{
				{AccountId: a.Debit.accountID, Currency: a.Debit.currency, Direction: v1.Direction_DIRECTION_DEBIT, Amount: a.Debit.amount},
				{AccountId: a.Credit.accountID, Currency: a.Credit.currency, Direction: v1.Direction_DIRECTION_CREDIT, Amount: a.Credit.amount},
			},
		},
	}))
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{JournalID: resp.Msg.GetJournalId(), FlowRunID: resp.Msg.GetFlowRunId()}, nil
}
