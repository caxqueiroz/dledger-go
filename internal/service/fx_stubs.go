// fx_stubs.go: temporary stubs replaced by Tasks 7 and 8.
package service

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	ledgerv1 "github.com/caxqueiroz/dledger-go/gen/proto/ledger/v1"
)

func (s *Server) ExecuteExchange(ctx context.Context, req *connect.Request[ledgerv1.ExecuteExchangeRequest]) (*connect.Response[ledgerv1.ExecuteExchangeResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("ExecuteExchange not implemented yet"))
}
