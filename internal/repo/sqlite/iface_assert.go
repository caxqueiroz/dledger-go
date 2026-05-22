package sqlite

import "github.com/caxqueiroz/dledger-go/internal/repo"

var _ repo.Store = (*Store)(nil)
var _ repo.Tx = (*Tx)(nil)
