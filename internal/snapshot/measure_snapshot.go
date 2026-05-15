package snapshot

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/schema"
)

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
