package interceptors

import (
	"context"
	"errors"

	"connectrpc.com/connect"
)

type ctxKey int

const tenantKey ctxKey = 1

func TenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(tenantKey).(string)
	return v
}

func NewTenant() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			tid := req.Header().Get("X-Tenant-Id")
			if tid == "" {
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("X-Tenant-Id required"))
			}
			ctx = context.WithValue(ctx, tenantKey, tid)
			return next(ctx, req)
		}
	}
}
