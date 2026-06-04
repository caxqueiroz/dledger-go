package dynamo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newTestStore provisions a uniquely-named table on the local DynamoDB
// endpoint and tears it down after the test. Skips without the endpoint.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	if os.Getenv("AWS_ENDPOINT_URL_DYNAMODB") == "" {
		t.Skip("AWS_ENDPOINT_URL_DYNAMODB not set; skipping integration test")
	}
	ctx := context.Background()
	table := "dltest_" + uuid.NewString()[:8] + "_ledger"
	s, err := Open(ctx, table)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.EnsureTable(ctx); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.DeleteTable(cleanupCtx)
		_ = s.Close()
	})
	return s
}
