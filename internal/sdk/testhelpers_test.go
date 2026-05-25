// internal/sdk/testhelpers_test.go
package sdk_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
	"github.com/caxqueiroz/dledger-go/pkg/dledger"
)

// mustCreate creates an account on the embedded SDK client and t.Fatal on error.
func mustCreate(t *testing.T, c dledger.Client, tenant, ownerType, ownerID, acctType, ccy string, nb ledgerv1.NormalBalance) {
	t.Helper()
	_, err := c.CreateAccount(context.Background(), connect.NewRequest(&ledgerv1.CreateAccountRequest{
		TenantId: tenant, OwnerType: ownerType, OwnerId: ownerID,
		AccountType: acctType, Currency: ccy, NormalBalance: nb,
	}))
	if err != nil {
		t.Fatalf("CreateAccount %s:%s: %v", ownerType, ownerID, err)
	}
}

// timestamp wraps a time.Time as a protobuf Timestamp.
func timestamp(t *testing.T, ts time.Time) *timestamppb.Timestamp {
	t.Helper()
	return timestamppb.New(ts)
}
