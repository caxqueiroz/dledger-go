package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/shopspring/decimal"

	"github.com/caxqueiroz/dledger-go/internal/ledger"
)

// getItem performs a strongly-consistent GetItem for the given pk.
// Returns (true, nil) with out populated on hit; (false, nil) when the item
// does not exist; (false, err) on any DynamoDB or unmarshal error.
func (s *Store) getItem(ctx context.Context, pk string, out any) (bool, error) {
	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, fmt.Errorf("get %s: %w", pk, err)
	}
	if len(resp.Item) == 0 {
		return false, nil
	}
	if err := attributevalue.UnmarshalMap(resp.Item, out); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", pk, err)
	}
	return true, nil
}

// GetAccount fetches an account by (tenantID, accountID).
// Returns (nil, CodeAccountNotFound DomainError) when not found — matching the
// sqlite contract exactly.
func (s *Store) GetAccount(ctx context.Context, tenantID, accountID string) (*ledger.Account, error) {
	var it accountItem
	found, err := s.getItem(ctx, accountPK(tenantID, accountID), &it)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ledger.NewDomainError(ledger.CodeAccountNotFound, "account "+accountID)
	}
	a, err := accountFromItem(it)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// GetBalance returns (postedDebits, postedCredits, version) for a balance row.
// Returns (0, 0, 0, nil) when no row exists — matching the sqlite contract.
func (s *Store) GetBalance(ctx context.Context, tenantID, accountID, currency string) (decimal.Decimal, decimal.Decimal, int64, error) {
	var it balanceItem
	found, err := s.getItem(ctx, balancePK(tenantID, accountID, currency), &it)
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	if !found {
		return decimal.Zero, decimal.Zero, 0, nil
	}
	d, err := parseDec(it.PostedDebits, "posted_debits")
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	c, err := parseDec(it.PostedCredits, "posted_credits")
	if err != nil {
		return decimal.Zero, decimal.Zero, 0, err
	}
	return d, c, it.Version, nil
}

// GetFlow fetches a FlowRun with its embedded steps by ID.
// Returns (nil, nil) when not found — matching the sqlite contract.
func (s *Store) GetFlow(ctx context.Context, tenantID, flowRunID string) (*ledger.FlowRun, error) {
	var it flowItem
	found, err := s.getItem(ctx, flowPK(tenantID, flowRunID), &it)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	f, steps, err := flowFromItem(it)
	if err != nil {
		return nil, err
	}
	f.Steps = steps
	return f, nil
}
