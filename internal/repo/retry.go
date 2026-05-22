package repo

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/caxqueiroz/doubleledger/internal/ledger"
)

// IsSerializationError reports whether err is a Postgres-protocol serialization
// failure (SQLSTATE 40001). CockroachDB returns this on isolation conflicts.
func IsSerializationError(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "40001"
	}
	return false
}

// WithRetry runs fn under capped exponential backoff. After maxAttempts retries
// it returns CodeSerializationRetryExhausted. Non-serialization errors return
// immediately.
func WithRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	var lastErr error
	delay := 5 * time.Millisecond
	for range maxAttempts {
		err := fn()
		if err == nil {
			return nil
		}
		if !IsSerializationError(err) {
			return err
		}
		lastErr = err
		jitter := time.Duration(rand.Int64N(int64(delay)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay + jitter):
		}
		if delay < 80*time.Millisecond {
			delay *= 2
		}
	}
	return ledger.NewDomainError(ledger.CodeSerializationRetryExhausted, lastErr.Error())
}
