package qir

import "github.com/vapstack/pooled"

var exprPool = pooled.Slices[Expr]{MaxCap: 256, Clear: pooled.ClearCap}

func GetExprSlice(capHint int) []Expr { return exprPool.Get(capHint) }
func ReleaseExprSlice(buf []Expr)     { exprPool.Put(buf) }

var queryPool = pooled.Pointers[Query]{
	Cleanup: func(q *Query) {
		q.releaseOwned()
	},
	Clear: false,
}
