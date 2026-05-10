package qir

import "github.com/vapstack/rbi/internal/pooled"

var exprPool = pooled.NewSlicePool[Expr](256, pooled.ClearCap)

func GetExprSlice(capHint int) []Expr { return exprPool.Get(capHint) }
func ReleaseExprSlice(buf []Expr)     { exprPool.Put(buf) }

var queryPool = pooled.Pointers[Query]{
	Cleanup: func(q *Query) {
		q.releaseOwned()
	},
	Clear: false,
}

var nilPrepareFieldOrdinalsPool = pooled.Maps[string, int]{
	NewCap: 8,
	MaxLen: 64,
}
