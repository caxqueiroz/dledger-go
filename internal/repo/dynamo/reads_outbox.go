package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// PendingOutbox returns up to limit outbox events that are still in PENDING
// state (i.e. their GSI1PK = gsiOutboxPending). Events are returned in
// ascending GSI1SK order (oldest first) because DynamoDB's default
// ScanIndexForward is ascending.
func (s *Store) PendingOutbox(ctx context.Context, limit int) ([]repo.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	resp, err := s.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(gsi1),
		KeyConditionExpression: aws.String("gsi1pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: gsiOutboxPending},
		},
		// ScanIndexForward defaults to true (ascending) — oldest GSI1SK first.
		Limit: aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, fmt.Errorf("PendingOutbox query: %w", err)
	}

	out := make([]repo.OutboxEvent, 0, len(resp.Items))
	for _, raw := range resp.Items {
		var it outboxItem
		if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
			return nil, fmt.Errorf("PendingOutbox unmarshal: %w", err)
		}
		createdAt, err := parseTime(it.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("PendingOutbox parseTime: %w", err)
		}
		out = append(out, repo.OutboxEvent{
			ID:             it.PK[len("OBX#"):], // strip the "OBX#" prefix
			TenantID:       it.TenantID,
			AggregateID:    it.AggregateID,
			EventType:      it.EventType,
			IdempotencyKey: it.IdempotencyKey,
			Payload:        []byte(it.Payload),
			CreatedAt:      createdAt,
		})
	}
	return out, nil
}

// MarkOutboxPublished removes the GSI1PK/GSI1SK attributes from the outbox
// item (taking it off the pending index) and stamps published_at with the
// store clock. The ConditionExpression attribute_exists(pk) ensures that a
// missing ID returns an error rather than silently succeeding.
func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	pk := outboxPK(id)
	now := fmtTime(s.now())

	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression:    aws.String("REMOVE gsi1pk, gsi1sk SET published_at = :t"),
		ConditionExpression: aws.String("attribute_exists(pk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: now},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if isCondCheckFailed(err, &ccf) {
			return fmt.Errorf("MarkOutboxPublished %q: item not found: %w", id, err)
		}
		return fmt.Errorf("MarkOutboxPublished %q: %w", id, err)
	}
	return nil
}

// IncrementOutboxAttempts atomically increments the attempts counter on the
// outbox item by 1. The ConditionExpression attribute_exists(pk) ensures that
// a missing ID returns an error.
func (s *Store) IncrementOutboxAttempts(ctx context.Context, id string) error {
	pk := outboxPK(id)

	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression:    aws.String("ADD attempts :one"),
		ConditionExpression: aws.String("attribute_exists(pk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if isCondCheckFailed(err, &ccf) {
			return fmt.Errorf("IncrementOutboxAttempts %q: item not found: %w", id, err)
		}
		return fmt.Errorf("IncrementOutboxAttempts %q: %w", id, err)
	}
	return nil
}

// isCondCheckFailed reports whether err wraps a ConditionalCheckFailedException.
func isCondCheckFailed(err error, target **types.ConditionalCheckFailedException) bool {
	return errors.As(err, target)
}
