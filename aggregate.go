package rbi

import (
	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/qagg"
)

type (
	Result    = qagg.Result
	Row       = qagg.Row
	Value     = qagg.Value
	ValueKind = qagg.ValueKind
)

const (
	ValueKindNone   = qagg.ValueKindNone
	ValueKindAny    = qagg.ValueKindAny
	ValueKindBool   = qagg.ValueKindBool
	ValueKindInt    = qagg.ValueKindInt
	ValueKindUint   = qagg.ValueKindUint
	ValueKindFloat  = qagg.ValueKindFloat
	ValueKindString = qagg.ValueKindString
)

// Aggregate evaluates a reduction query against the current index snapshot.
func (db *DB[K, V]) Aggregate(q *qx.QX) (Result, error) {
	if err := db.beginOp(); err != nil {
		return Result{}, err
	}
	defer db.endOp()

	if db.engine == nil {
		return Result{}, ErrNoIndex
	}

	prepared, err := qagg.Prepare(q, db.engine.schema)
	if err != nil {
		return Result{}, err
	}
	defer prepared.Release()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)

	return qagg.Execute(view, snap, prepared)
}

// Count evaluates the given filter predicates and returns the number of matching records.
// Zero predicates mean match-all.
func (db *DB[K, V]) Count(exprs ...qx.Expr) (uint64, error) {
	if err := db.beginOp(); err != nil {
		return 0, err
	}
	defer db.endOp()

	if db.engine == nil {
		return 0, ErrNoIndex
	}
	prepared, err := qagg.PrepareCount(db.engine.schema, exprs...)
	if err != nil {
		return 0, err
	}
	defer prepared.Release()

	snap, seq, ref := db.engine.snapshot.PinCurrent()
	defer db.engine.snapshot.Unpin(seq, ref)

	view := db.engine.exec.AcquireView(snap)
	defer db.engine.exec.ReleaseView(view)
	return qagg.Count(view, prepared, true)
}
