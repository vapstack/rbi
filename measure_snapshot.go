package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/pools"
)

type measureFieldBatchDeltas struct {
	fields  *pooled.Slice[*pooled.Slice[measureBatchDelta]]
	touched []int
}

var measureBatchDeltaSlotSlicePool = pooled.Slices[*pooled.Slice[measureBatchDelta]]{Clear: true}

func newMeasureFieldBatchDeltas(fieldCount int) measureFieldBatchDeltas {
	fields := measureBatchDeltaSlotSlicePool.Get()
	fields.SetLen(fieldCount)
	return measureFieldBatchDeltas{
		fields:  fields,
		touched: pools.GetIntSlice(fieldCount),
	}
}

func (deltas *measureFieldBatchDeltas) append(ordinal int, delta measureBatchDelta) {
	buf := deltas.fields.Get(ordinal)
	if buf == nil {
		buf = measureBatchDeltaSlicePool.Get()
		deltas.fields.Set(ordinal, buf)
		deltas.touched = append(deltas.touched, ordinal)
	}
	buf.Append(delta)
}

func (deltas *measureFieldBatchDeltas) release() {
	for i := 0; i < deltas.fields.Len(); i++ {
		buf := deltas.fields.Get(i)
		if buf != nil {
			measureBatchDeltaSlicePool.Put(buf)
			deltas.fields.Set(i, nil)
		}
	}
	measureBatchDeltaSlotSlicePool.Put(deltas.fields)
	deltas.fields = nil
	pools.PutIntSlice(deltas.touched)
	deltas.touched = nil
}

func (db *DB[K, V]) forEachModifiedMeasureField(v1 *V, v2 *V, fn func(measureFieldAccessor) bool) {
	if len(db.engine.measureFieldAccess) == 0 {
		return
	}
	if v1 == nil || v2 == nil {
		for _, acc := range db.engine.measureFieldAccess {
			if !fn(acc) {
				return
			}
		}
		return
	}
	ptr1 := unsafe.Pointer(v1)
	ptr2 := unsafe.Pointer(v2)
	for _, acc := range db.engine.measureFieldAccess {
		if acc.modified != nil && acc.modified(ptr1, ptr2) && !fn(acc) {
			return
		}
	}
}

func (acc measureFieldAccessor) collectSnapshotMeasureDelta(
	idx uint64,
	oldPtr unsafe.Pointer,
	newPtr unsafe.Pointer,
	deltas *measureFieldBatchDeltas,
) {
	var oldValue uint64
	var oldOK bool
	if oldPtr != nil {
		oldValue, oldOK = acc.read(oldPtr)
	}
	var newValue uint64
	var newOK bool
	if newPtr != nil {
		newValue, newOK = acc.read(newPtr)
	}
	if oldOK == newOK && oldValue == newValue {
		return
	}
	deltas.append(acc.ordinal, measureBatchDelta{
		id:    idx,
		newOK: newOK,
		new:   newValue,
	})
}

func (qe *queryEngine) collectSnapshotMeasureEntryDiffs(op snapshotBatchEntry, deltas *measureFieldBatchDeltas, patchMap map[string]*field) {
	if op.patchOnly {
		for i, patchField := range op.patch {
			fieldDef, ok := patchMap[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := qe.measureFieldMap[fieldDef.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchMap[op.patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			acc.collectSnapshotMeasureDelta(op.idx, op.oldVal, op.newVal, deltas)
		}
		return
	}
	if op.oldVal == nil || op.newVal == nil {
		for _, acc := range qe.measureFieldAccess {
			acc.collectSnapshotMeasureDelta(op.idx, op.oldVal, op.newVal, deltas)
		}
		return
	}
	for _, acc := range qe.measureFieldAccess {
		if acc.modified != nil && acc.modified(op.oldVal, op.newVal) {
			acc.collectSnapshotMeasureDelta(op.idx, op.oldVal, op.newVal, deltas)
		}
	}
}

func applyMeasureFieldBatchDeltas(next *indexSnapshot, deltas *measureFieldBatchDeltas) {
	for i := range deltas.touched {
		ordinal := deltas.touched[i]
		base := next.measure.Get(ordinal)
		storage := applyMeasureDeltasOwned(base, deltas.fields.Get(ordinal))
		deltas.fields.Set(ordinal, nil)
		if storage != base {
			next.measure.Set(ordinal, storage)
		}
	}
}
