package dynamo

import (
	"context"
	"testing"
)

func TestEnsureTableIdempotent(t *testing.T) {
	s := newTestStore(t)
	if err := s.EnsureTable(context.Background()); err != nil {
		t.Fatalf("second EnsureTable: %v", err)
	}
}
