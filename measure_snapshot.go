package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
)

type measureFieldBatchDeltas struct {
	fields  *pooled.Slice[*pooled.Slice[measureBatchDelta]]
	touched *pooled.Slice[int]
}

var measureBatchDeltaSlotSlicePool = pooled.Slices[*pooled.Slice[measureBatchDelta]]{Clear: true}

func newMeasureFieldBatchDeltas(fieldCount int) measureFieldBatchDeltas {
	fields := measureBatchDeltaSlotSlicePool.Get()
	fields.SetLen(fieldCount)
	return measureFieldBatchDeltas{
		fields:  fields,
		touched: fieldIndexOrdinalSlicePool.Get(),
	}
}

func (deltas *measureFieldBatchDeltas) append(ordinal int, delta measureBatchDelta) {
	buf := deltas.fields.Get(ordinal)
	if buf == nil {
		buf = measureBatchDeltaSlicePool.Get()
		deltas.fields.Set(ordinal, buf)
		deltas.touched.Append(ordinal)
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
	fieldIndexOrdinalSlicePool.Put(deltas.touched)
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

func (db *DB[K, V]) forEachSnapshotModifiedMeasureField(op snapshotBatchEntry[K, V], fn func(measureFieldAccessor) bool) {
	if len(db.engine.measureFieldAccess) == 0 {
		return
	}
	req := op.req
	if req != nil &&
		req.op == autoBatchPatch &&
		op.oldVal != nil &&
		op.newVal != nil &&
		len(req.beforeProcess) == 0 &&
		len(req.beforeStore) == 0 {
		for i, patchField := range req.patch {
			f, ok := db.patchMap[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := db.engine.measureFieldMap[f.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := db.patchMap[req.patch[j].Name]
				if ok && prev.DBName == f.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if !fn(acc) {
				return
			}
		}
		return
	}
	db.forEachModifiedMeasureField(op.oldVal, op.newVal, fn)
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

func (db *DB[K, V]) collectSnapshotMeasureEntryDiffs(op snapshotBatchEntry[K, V], deltas *measureFieldBatchDeltas) {
	var ptrOld, ptrNew unsafe.Pointer
	if op.oldVal != nil {
		ptrOld = unsafe.Pointer(op.oldVal)
	}
	if op.newVal != nil {
		ptrNew = unsafe.Pointer(op.newVal)
	}
	db.forEachSnapshotModifiedMeasureField(op, func(acc measureFieldAccessor) bool {
		acc.collectSnapshotMeasureDelta(op.idx, ptrOld, ptrNew, deltas)
		return true
	})
}

func applyMeasureFieldBatchDeltas(next *indexSnapshot, deltas *measureFieldBatchDeltas) {
	for i := 0; i < deltas.touched.Len(); i++ {
		ordinal := deltas.touched.Get(i)
		base := next.measure.Get(ordinal)
		storage := applyMeasureDeltasOwned(base, deltas.fields.Get(ordinal))
		deltas.fields.Set(ordinal, nil)
		if storage != base {
			next.measure.Set(ordinal, storage)
		}
	}
}
