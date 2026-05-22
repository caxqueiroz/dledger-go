package crdb

import "github.com/caxqueiroz/dledger-go/internal/repo"

var (
	_ repo.Store = (*Store)(nil)
	_ repo.Tx    = (*Tx)(nil)
)
