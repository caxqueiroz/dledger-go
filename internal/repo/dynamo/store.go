package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// Store implements repo.Store on a single DynamoDB table. The DSN passed to
// dledger.NewEmbedded is the table name; endpoint, region, credentials and CA
// bundle come from standard AWS env vars, so the same code talks to ExtendDB
// locally and DynamoDB on AWS.
type Store struct {
	db    *dynamodb.Client
	table string
	clock func() time.Time
}

// Open builds the DynamoDB client from the default AWS config chain.
func Open(ctx context.Context, table string) (*Store, error) {
	if table == "" {
		return nil, errors.New("dynamo: table name (DSN) required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %w", err)
	}
	return &Store{db: dynamodb.NewFromConfig(cfg), table: table, clock: time.Now}, nil
}

// now is the store clock; tests may override via the clock field.
func (s *Store) now() time.Time { return s.clock() }

// EnsureTable creates the ledger table if missing and waits for ACTIVE.
// Safe to call repeatedly; this is the DynamoDB analogue of MigrateAuto.
func (s *Store) EnsureTable(ctx context.Context) error {
	_, err := s.db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(s.table),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi1pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi1sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi2pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi2sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsi1),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi1pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("gsi1sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(gsi2),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsi2pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("gsi2sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	waiter := dynamodb.NewTableExistsWaiter(s.db)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)}, 30*time.Second); err != nil {
		return fmt.Errorf("waiting for table active: %w", err)
	}
	return nil
}

// DeleteTable removes the table. Tests only.
func (s *Store) DeleteTable(ctx context.Context) error {
	_, err := s.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(s.table)})
	return err
}

// Close releases nothing today (the SDK client is connectionless) but
// satisfies repo.Store.
func (s *Store) Close() error { return nil }

// Compile-time assertion: *Store must implement repo.Store completely.
// The interface is now fully implemented (Task 9 completes the outbox methods).
var _ repo.Store = (*Store)(nil)
