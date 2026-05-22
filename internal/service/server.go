package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/caxqueiroz/dledger-go/internal/repo"
)

// Clock is a function that returns the current time.
type Clock func() time.Time

// IDGen is a function that returns a new unique ID string.
type IDGen func() string

// Server implements the LedgerService Connect-RPC handler.
type Server struct {
	Store repo.Store
	Now   Clock
	NewID IDGen
}

// New creates a new Server with a real clock and UUID generator.
func New(store repo.Store) *Server {
	return &Server{
		Store: store,
		Now:   time.Now,
		NewID: func() string { return uuid.NewString() },
	}
}

// runInTx opens a flow transaction, runs fn, and commits or rolls back.
func (s *Server) runInTx(ctx context.Context, fn func(tx repo.Tx) error) error {
	tx, err := s.Store.BeginFlowTx(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
