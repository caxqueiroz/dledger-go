package interceptors

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
)

func NewLogging(log *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			attrs := []any{
				"procedure", req.Spec().Procedure,
				"tenant_id", TenantFromContext(ctx),
				"duration_ms", time.Since(start).Milliseconds(),
			}
			if err != nil {
				log.WarnContext(ctx, "rpc.error", append(attrs, "err", err)...)
			} else {
				log.InfoContext(ctx, "rpc.ok", attrs...)
			}
			return resp, err
		}
	}
}
