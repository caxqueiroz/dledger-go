package crdb

import "github.com/caxqueiroz/doubleledger/internal/repo"

var (
	_ repo.Store = (*Store)(nil)
	_ repo.Tx    = (*Tx)(nil)
)
