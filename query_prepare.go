package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qir"
)

type preparedFieldResolver[K ~string | ~uint64, V any] struct {
	db *DB[K, V]
}

func (r preparedFieldResolver[K, V]) ResolveField(name string) (int, bool) {
	acc, ok := r.db.indexedFieldByName[name]
	if !ok {
		return 0, false
	}
	return acc.ordinal, true
}

func (db *DB[K, V]) prepareQuery(q *qx.QX) (*qir.Query, error) {
	return qir.PrepareQueryResolved(q, preparedFieldResolver[K, V]{db: db})
}

func (db *DB[K, V]) prepareCountExpr(expr qx.Expr) (*qir.Query, error) {
	return qir.PrepareCountExprResolved(preparedFieldResolver[K, V]{db: db}, expr)
}

func (db *DB[K, V]) prepareCountExprs(exprs ...qx.Expr) (*qir.Query, error) {
	return qir.PrepareCountExprsResolved(preparedFieldResolver[K, V]{db: db}, exprs...)
}
