package rbi

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/schema"
)

func (db *DB[K, V]) forEachModifiedMeasureField(v1 *V, v2 *V, fn func(schema.MeasureFieldAccessor) bool) {
	if len(db.engine.schema.Measures) == 0 {
		return
	}
	if v1 == nil || v2 == nil {
		for _, acc := range db.engine.schema.Measures {
			if !fn(acc) {
				return
			}
		}
		return
	}
	ptr1 := unsafe.Pointer(v1)
	ptr2 := unsafe.Pointer(v2)
	for _, acc := range db.engine.schema.Measures {
		if acc.Modified(ptr1, ptr2) && !fn(acc) {
			return
		}
	}
}

func collectSnapshotMeasureDelta(
	acc schema.MeasureFieldAccessor,
	idx uint64,
	oldPtr unsafe.Pointer,
	newPtr unsafe.Pointer,
	deltas *indexdata.MeasureDeltaBatch,
) {
	var oldValue uint64
	var oldOK bool
	if oldPtr != nil {
		oldValue, oldOK = acc.Read(oldPtr)
	}
	var newValue uint64
	var newOK bool
	if newPtr != nil {
		newValue, newOK = acc.Read(newPtr)
	}
	if oldOK == newOK && oldValue == newValue {
		return
	}
	deltas.Append(acc.Ordinal, idx, newOK, newValue)
}

func (qe *queryEngine) collectSnapshotMeasureEntryDiffs(op snapshotBatchEntry, deltas *indexdata.MeasureDeltaBatch, patchFields map[string]*schema.Field) {
	if op.patchOnly {
		for i, patchField := range op.patch {
			fieldDef, ok := patchFields[patchField.Name]
			if !ok {
				continue
			}
			acc, ok := qe.schema.MeasuresByName[fieldDef.DBName]
			if !ok {
				continue
			}
			duplicate := false
			for j := 0; j < i; j++ {
				prev, ok := patchFields[op.patch[j].Name]
				if ok && prev.DBName == fieldDef.DBName {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			collectSnapshotMeasureDelta(acc, op.idx, op.oldVal, op.newVal, deltas)
		}
		return
	}
	if op.oldVal == nil || op.newVal == nil {
		for _, acc := range qe.schema.Measures {
			collectSnapshotMeasureDelta(acc, op.idx, op.oldVal, op.newVal, deltas)
		}
		return
	}
	for _, acc := range qe.schema.Measures {
		if acc.Modified(op.oldVal, op.newVal) {
			collectSnapshotMeasureDelta(acc, op.idx, op.oldVal, op.newVal, deltas)
		}
	}
}
