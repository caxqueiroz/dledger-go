package sqlite

import "github.com/caxqueiroz/doubleledger/internal/repo"

var _ repo.Store = (*Store)(nil)
var _ repo.Tx = (*Tx)(nil)
