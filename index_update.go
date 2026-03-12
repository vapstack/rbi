package rbi

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func addIdxToDeltaChangeMap(m map[string]map[string]indexDeltaEntry, field, key string, idx uint64, isAdd bool) {
	fm := m[field]
	if fm == nil {
		fm = getWriteDeltaInnerMap()
		m[field] = fm
	}
	e := fm[key]
	op := indexDeltaEntry{}
	if isAdd {
		op.addSingle = idx
		op.addSingleSet = true
	} else {
		op.delSingle = idx
		op.delSingleSet = true
	}
	// write-delta entries are tx-local; merge directly into owned bitmaps
	e = deltaEntryMergeOwned(e, op)
	if deltaEntryIsEmpty(e) {
		delete(fm, key)
		if len(fm) == 0 {
			delete(m, field)
		}
		return
	}
	fm[key] = e
}

type uniqueBatchCheckState struct {
	leaving map[string]map[string]*roaring64.Bitmap
	seen    map[string]map[string]uint64
}

type uniqueLeavingTouch struct {
	field string
	key   string
	added bool
}

type uniqueSeenWrite struct {
	field string
	key   string
}

func (db *DB[K, V]) newUniqueBatchCheckState() uniqueBatchCheckState {
	return uniqueBatchCheckState{
		leaving: getUniqueLeavingOuterMap(),
		seen:    getUniqueSeenOuterMap(),
	}
}

func (db *DB[K, V]) releaseUniqueBatchCheckState(state uniqueBatchCheckState) {
	if state.leaving != nil {
		releaseUniqueLeavingOuterMap(state.leaving)
		state.leaving = nil
	}
	if state.seen != nil {
		releaseUniqueSeenOuterMap(state.seen)
		state.seen = nil
	}
}

func (db *DB[K, V]) markUniqueBatchLeaving(state uniqueBatchCheckState, field, key string, idx uint64) bool {
	if key == nilValue {
		return false
	}
	fm := state.leaving[field]
	if fm == nil {
		fm = getUniqueLeavingInnerMap()
		state.leaving[field] = fm
	}
	bm := fm[key]
	if bm == nil {
		bm = getRoaringBuf()
		fm[key] = bm
	}
	if bm.Contains(idx) {
		return false
	}
	bm.Add(idx)
	return true
}

func (db *DB[K, V]) appendUniqueBatchLeavingTouch(state uniqueBatchCheckState, touched []uniqueLeavingTouch, field, key string, idx uint64) []uniqueLeavingTouch {
	added := db.markUniqueBatchLeaving(state, field, key, idx)
	return append(touched, uniqueLeavingTouch{field: field, key: key, added: added})
}

func rollbackUniqueBatchLeaving(state uniqueBatchCheckState, touched []uniqueLeavingTouch, idx uint64) {
	for i := len(touched) - 1; i >= 0; i-- {
		t := touched[i]
		if !t.added {
			continue
		}
		fm := state.leaving[t.field]
		if fm == nil {
			continue
		}
		bm := fm[t.key]
		if bm == nil {
			continue
		}
		bm.Remove(idx)
		if bm.IsEmpty() {
			delete(fm, t.key)
			releaseRoaringBuf(bm)
		}
		if len(fm) == 0 {
			delete(state.leaving, t.field)
			releaseUniqueLeavingInnerMap(fm)
		}
	}
}

func (db *DB[K, V]) checkUniqueBatchCandidateAndCollectSeen(
	state uniqueBatchCheckState,
	idx uint64,
	acc indexedFieldAccessor,
	ptr unsafe.Pointer,
	seenWrites []uniqueSeenWrite,
) ([]uniqueSeenWrite, error) {
	single, _, ok := acc.getter(ptr)
	if !ok || single == nilValue {
		return seenWrites, nil
	}

	if sm := state.seen[acc.name]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.name)
		}
	}

	bm, releaseBM := lookupUniqueBitmap(db, acc.name, single)
	if bm == nil || bm.IsEmpty() {
		releaseBM()
		return append(seenWrites, uniqueSeenWrite{field: acc.name, key: single}), nil
	}

	var lv *roaring64.Bitmap
	if fm := state.leaving[acc.name]; fm != nil {
		lv = fm[single]
	}

	if lv == nil || lv.IsEmpty() {
		if bm.GetCardinality() == 1 && bm.Contains(idx) {
			releaseBM()
			return append(seenWrites, uniqueSeenWrite{field: acc.name, key: single}), nil
		}
		releaseBM()
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}

	iter := bm.Iterator()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		releaseBM()
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}
	releaseBM()
	return append(seenWrites, uniqueSeenWrite{field: acc.name, key: single}), nil
}

func (db *DB[K, V]) checkUniqueBatchAppend(state uniqueBatchCheckState, idx uint64, oldVal, newVal *V, modified []string) error {
	if len(db.uniqueFieldAccessors) == 0 {
		return nil
	}
	var (
		touchedInline [8]uniqueLeavingTouch
		touched       = touchedInline[:0]
	)
	if oldVal != nil {
		ptrOld := unsafe.Pointer(oldVal)
		if newVal == nil {
			for _, acc := range db.uniqueFieldAccessors {
				single, _, ok := acc.getter(ptrOld)
				if !ok {
					continue
				}
				touched = db.appendUniqueBatchLeavingTouch(state, touched, acc.name, single, idx)
			}
		} else {
			for _, name := range modified {
				acc, ok := db.indexedFieldByName[name]
				if !ok || !acc.field.Unique || acc.field.Slice {
					continue
				}
				single, _, ok := acc.getter(ptrOld)
				if !ok {
					continue
				}
				touched = db.appendUniqueBatchLeavingTouch(state, touched, acc.name, single, idx)
			}
		}
	}

	if newVal == nil {
		return nil
	}

	var (
		seenWritesInline [8]uniqueSeenWrite
		seenWrites       = seenWritesInline[:0]
	)

	ptrNew := unsafe.Pointer(newVal)
	if oldVal == nil {
		for _, acc := range db.uniqueFieldAccessors {
			var err error
			seenWrites, err = db.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, ptrNew, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	} else {
		for _, name := range modified {
			acc, ok := db.indexedFieldByName[name]
			if !ok || !acc.field.Unique || acc.field.Slice {
				continue
			}
			var err error
			seenWrites, err = db.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, ptrNew, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	}

	for _, w := range seenWrites {
		sm := state.seen[w.field]
		if sm == nil {
			sm = getUniqueSeenInnerMap()
			state.seen[w.field] = sm
		}
		sm[w.key] = idx
	}
	return nil
}

func noReleaseUniqueBitmap() {}

func lookupUniqueBitmap[K ~string | ~uint64, V any](db *DB[K, V], field, key string) (*roaring64.Bitmap, func()) {
	bm, owned := db.fieldLookupOwned(field, key, nil)
	if !owned {
		return bm, noReleaseUniqueBitmap
	}
	return bm, func() {
		if bm != nil {
			releaseRoaringBuf(bm)
		}
	}
}

func (db *DB[K, V]) applyDeltaAllIndexedFields(
	ptr unsafe.Pointer,
	idx uint64,
	isAdd bool,
	indexChanges, lenChanges map[string]map[string]indexDeltaEntry,
) {
	for _, acc := range db.indexedFieldAccess {
		db.applyDeltaFieldAccessor(ptr, idx, acc, isAdd, indexChanges, lenChanges)
	}
}

func (db *DB[K, V]) applyDeltaModifiedIndexedFields(
	ptr unsafe.Pointer,
	idx uint64,
	modified []string,
	isAdd bool,
	indexChanges, lenChanges map[string]map[string]indexDeltaEntry,
) {
	for _, name := range modified {
		acc, ok := db.indexedFieldByName[name]
		if !ok {
			continue
		}
		db.applyDeltaFieldAccessor(ptr, idx, acc, isAdd, indexChanges, lenChanges)
	}
}

func (db *DB[K, V]) applyDeltaFieldAccessor(
	ptr unsafe.Pointer,
	idx uint64,
	acc indexedFieldAccessor,
	isAdd bool,
	indexChanges, lenChanges map[string]map[string]indexDeltaEntry,
) {
	single, multi, ok := acc.getter(ptr)
	if !ok {
		return
	}
	if acc.field.Slice {
		for _, sval := range multi {
			addIdxToDeltaChangeMap(indexChanges, acc.name, sval, idx, isAdd)
		}
		n := distinctCount(multi)
		useZeroComplement := db.lenZeroComplement[acc.name]
		if !useZeroComplement || n > 0 {
			addIdxToDeltaChangeMap(lenChanges, acc.name, uint64ByteStr(uint64(n)), idx, isAdd)
		}
		if useZeroComplement && n > 0 {
			addIdxToDeltaChangeMap(lenChanges, acc.name, lenIndexNonEmptyKey, idx, isAdd)
		}
		return
	}
	addIdxToDeltaChangeMap(indexChanges, acc.name, single, idx, isAdd)
}

func (db *DB[K, V]) checkUniqueOnWriteMulti(idxs []uint64, oldVals, newVals []*V, modified [][]string) error {
	if len(idxs) == 0 || len(db.uniqueFieldAccessors) == 0 {
		return nil
	}

	// Collapse duplicate ids to their net old->new state within this tx.
	// Uniqueness should be validated against final committed state, not
	// transient intermediate values inside one BatchSet/BatchPatch call.
	collapseNeeded := false
	seenIdx := make(map[uint64]struct{}, len(idxs))
	for _, idx := range idxs {
		if _, ok := seenIdx[idx]; ok {
			collapseNeeded = true
			break
		}
		seenIdx[idx] = struct{}{}
	}

	if collapseNeeded {
		type uniqueAgg struct {
			idx uint64
			old *V
			new *V
		}

		order := make([]uniqueAgg, 0, len(idxs))
		pos := make(map[uint64]int, len(idxs))
		for i, idx := range idxs {
			p, ok := pos[idx]
			if !ok {
				p = len(order)
				pos[idx] = p
				order = append(order, uniqueAgg{
					idx: idx,
					old: oldVals[i],
				})
			}
			if newVals != nil {
				order[p].new = newVals[i]
			}
		}

		collapsedIdxs := make([]uint64, 0, len(order))
		collapsedOld := make([]*V, 0, len(order))
		var collapsedNew []*V
		if newVals != nil {
			collapsedNew = make([]*V, 0, len(order))
		}
		collapsedModified := make([][]string, 0, len(order))

		for _, it := range order {
			collapsedIdxs = append(collapsedIdxs, it.idx)
			collapsedOld = append(collapsedOld, it.old)
			if newVals != nil {
				collapsedNew = append(collapsedNew, it.new)
			}
			if it.old != nil && it.new != nil {
				collapsedModified = append(collapsedModified, db.getModifiedIndexedFields(it.old, it.new))
			} else {
				collapsedModified = append(collapsedModified, nil)
			}
		}

		idxs = collapsedIdxs
		oldVals = collapsedOld
		newVals = collapsedNew
		modified = collapsedModified
	}

	leaving := getUniqueLeavingOuterMap()
	defer releaseUniqueLeavingOuterMap(leaving)

	seen := getUniqueSeenOuterMap()
	defer releaseUniqueSeenOuterMap(seen)

	for i, idx := range idxs {
		oldVal := oldVals[i]
		var newVal *V
		if newVals != nil {
			newVal = newVals[i]
		}
		if oldVal == nil {
			continue
		}

		ptr := unsafe.Pointer(oldVal)

		// On delete (newVal == nil), the record releases all unique scalar values.
		// On update, only modified unique fields can release previous values.
		if newVal == nil {
			for _, acc := range db.uniqueFieldAccessors {
				single, _, ok := acc.getter(ptr)
				if !ok || single == nilValue {
					continue
				}
				m := leaving[acc.name]
				if m == nil {
					m = getUniqueLeavingInnerMap()
					leaving[acc.name] = m
				}
				bm := m[single]
				if bm == nil {
					bm = getRoaringBuf()
					m[single] = bm
				}
				bm.Add(idx)
			}
			continue
		}

		for _, name := range modified[i] {
			acc, ok := db.indexedFieldByName[name]
			if !ok || !acc.field.Unique || acc.field.Slice {
				continue
			}
			single, _, ok := acc.getter(ptr)
			if !ok || single == nilValue {
				continue
			}
			m := leaving[acc.name]
			if m == nil {
				m = getUniqueLeavingInnerMap()
				leaving[acc.name] = m
			}
			bm := m[single]
			if bm == nil {
				bm = getRoaringBuf()
				m[single] = bm
			}
			bm.Add(idx)
		}
	}

	if newVals == nil {
		return nil // deletion
	}

	for i, idx := range idxs {

		newVal := newVals[i]
		if newVal == nil {
			continue
		}

		ptr := unsafe.Pointer(newVal)
		if oldVals[i] == nil {
			for _, acc := range db.uniqueFieldAccessors {
				if err := db.checkUniqueBatchCandidate(idx, ptr, acc, seen, leaving); err != nil {
					return err
				}
			}
			continue
		}

		for _, name := range modified[i] {
			acc, ok := db.indexedFieldByName[name]
			if !ok || !acc.field.Unique || acc.field.Slice {
				continue
			}
			if err := db.checkUniqueBatchCandidate(idx, ptr, acc, seen, leaving); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB[K, V]) checkUniqueBatchCandidate(
	idx uint64,
	ptr unsafe.Pointer,
	acc indexedFieldAccessor,
	seen map[string]map[string]uint64,
	leaving map[string]map[string]*roaring64.Bitmap,
) error {
	single, _, ok := acc.getter(ptr)
	if !ok || single == nilValue {
		return nil
	}

	sm := seen[acc.name]
	if sm == nil {
		sm = getUniqueSeenInnerMap()
		seen[acc.name] = sm
	}
	if prev, ok := sm[single]; ok && prev != idx {
		return fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.name)
	}
	sm[single] = idx

	bm, releaseBM := lookupUniqueBitmap(db, acc.name, single)
	if bm == nil || bm.IsEmpty() {
		releaseBM()
		return nil
	}

	var lv *roaring64.Bitmap
	if fm := leaving[acc.name]; fm != nil {
		lv = fm[single]
	}

	if lv == nil || lv.IsEmpty() {
		if bm.GetCardinality() == 1 && bm.Contains(idx) {
			releaseBM()
			return nil
		}
		releaseBM()
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}

	iter := bm.Iterator()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		releaseBM()
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}
	releaseBM()
	return nil
}

type preparedSnapshotDelta struct {
	indexDelta  map[string]*fieldIndexDelta
	lenDelta    map[string]*fieldIndexDelta
	universeAdd *roaring64.Bitmap
	universeRem *roaring64.Bitmap
}

func releasePreparedFieldDelta(delta *fieldIndexDelta) {
	if delta == nil {
		return
	}
	if delta.singleSet {
		releaseRoaringBuf(delta.singleEntry.add)
		releaseRoaringBuf(delta.singleEntry.del)
		return
	}
	releaseWriteDeltaInnerMap(delta.byKey)
}

func releasePreparedFieldDeltaMap(delta map[string]*fieldIndexDelta) {
	if len(delta) == 0 {
		return
	}
	for f, d := range delta {
		releasePreparedFieldDelta(d)
		delete(delta, f)
	}
}

func (delta *preparedSnapshotDelta) release() {
	if delta == nil {
		return
	}
	releasePreparedFieldDeltaMap(delta.indexDelta)
	releasePreparedFieldDeltaMap(delta.lenDelta)
	releaseRoaringBuf(delta.universeAdd)
	releaseRoaringBuf(delta.universeRem)
	delta.indexDelta = nil
	delta.lenDelta = nil
	delta.universeAdd = nil
	delta.universeRem = nil
}

func (delta *preparedSnapshotDelta) releaseTransientUniverse() {
	if delta == nil {
		return
	}
	releaseRoaringBuf(delta.universeAdd)
	releaseRoaringBuf(delta.universeRem)
	delta.universeAdd = nil
	delta.universeRem = nil
}

func (db *DB[K, V]) prepareSnapshotWriteDeltaBatch(idxs []uint64, oldVals, newVals []*V, modified [][]string) preparedSnapshotDelta {
	db.mu.RLock()
	defer db.mu.RUnlock()

	indexChanges := getWriteDeltaOuterMap()
	lenChanges := getWriteDeltaOuterMap()

	var add *roaring64.Bitmap
	var rem *roaring64.Bitmap

	for i, idx := range idxs {
		oldV := oldVals[i]
		var newV *V
		if newVals != nil {
			newV = newVals[i]
		}
		var mods []string
		if modified != nil {
			mods = modified[i]
		} else if oldV != nil && newV != nil {
			mods = db.getModifiedIndexedFields(oldV, newV)
		}

		if oldV != nil {
			ptr := unsafe.Pointer(oldV)
			if newV == nil {
				db.applyDeltaAllIndexedFields(ptr, idx, false, indexChanges, lenChanges)
			} else {
				db.applyDeltaModifiedIndexedFields(ptr, idx, mods, false, indexChanges, lenChanges)
			}
		}

		if newV != nil {
			ptr := unsafe.Pointer(newV)
			if oldV == nil {
				db.applyDeltaAllIndexedFields(ptr, idx, true, indexChanges, lenChanges)
			} else {
				db.applyDeltaModifiedIndexedFields(ptr, idx, mods, true, indexChanges, lenChanges)
			}
		}

		switch {
		case oldV == nil && newV != nil:
			if add == nil {
				add = getRoaringBuf()
			}
			add.Add(idx)
		case oldV != nil && newV == nil:
			if rem == nil {
				rem = getRoaringBuf()
			}
			rem.Add(idx)
		}
	}

	delta := preparedSnapshotDelta{
		indexDelta: buildSnapshotDeltaMap(indexChanges, func(field string) bool {
			return fieldUsesFixed8Keys(db.fields[field])
		}),
		lenDelta:    buildSnapshotDeltaMap(lenChanges, func(string) bool { return true }),
		universeAdd: add,
		universeRem: rem,
	}
	releaseWriteDeltaOuterMap(indexChanges)
	releaseWriteDeltaOuterMap(lenChanges)
	return delta
}

func (db *DB[K, V]) prepareSnapshotWriteDelta(idx uint64, oldVal, newVal *V, modified []string) preparedSnapshotDelta {
	db.mu.RLock()
	defer db.mu.RUnlock()

	indexChanges := getWriteDeltaOuterMap()
	lenChanges := getWriteDeltaOuterMap()

	var universeAdd *roaring64.Bitmap
	var universeDel *roaring64.Bitmap

	if oldVal != nil {
		ptr := unsafe.Pointer(oldVal)
		if newVal == nil {
			db.applyDeltaAllIndexedFields(ptr, idx, false, indexChanges, lenChanges)
		} else {
			db.applyDeltaModifiedIndexedFields(ptr, idx, modified, false, indexChanges, lenChanges)
		}
	}

	if newVal != nil {
		ptr := unsafe.Pointer(newVal)
		if oldVal == nil {
			db.applyDeltaAllIndexedFields(ptr, idx, true, indexChanges, lenChanges)
		} else {
			db.applyDeltaModifiedIndexedFields(ptr, idx, modified, true, indexChanges, lenChanges)
		}
	}

	switch {
	case oldVal == nil && newVal != nil:
		universeAdd = getRoaringBuf()
		universeAdd.Add(idx)
	case oldVal != nil && newVal == nil:
		universeDel = getRoaringBuf()
		universeDel.Add(idx)
	}

	delta := preparedSnapshotDelta{
		indexDelta: buildSnapshotDeltaMap(indexChanges, func(field string) bool {
			return fieldUsesFixed8Keys(db.fields[field])
		}),
		lenDelta:    buildSnapshotDeltaMap(lenChanges, func(string) bool { return true }),
		universeAdd: universeAdd,
		universeRem: universeDel,
	}
	releaseWriteDeltaOuterMap(indexChanges)
	releaseWriteDeltaOuterMap(lenChanges)
	return delta
}

func (db *DB[K, V]) publishWriteDeltaBatch(txID uint64, idxs []uint64, oldVals, newVals []*V, modified [][]string) {
	indexChanges := getWriteDeltaOuterMap()
	defer releaseWriteDeltaOuterMap(indexChanges)

	lenChanges := getWriteDeltaOuterMap()
	defer releaseWriteDeltaOuterMap(lenChanges)

	var add *roaring64.Bitmap
	var rem *roaring64.Bitmap
	defer func() {
		releaseRoaringBuf(add)
		releaseRoaringBuf(rem)
	}()

	for i, idx := range idxs {
		oldV := oldVals[i]
		var newV *V
		if newVals != nil {
			newV = newVals[i]
		}
		var mods []string
		if modified != nil {
			mods = modified[i]
		} else if oldV != nil && newV != nil {
			mods = db.getModifiedIndexedFields(oldV, newV)
		}

		if oldV != nil {
			ptr := unsafe.Pointer(oldV)
			if newV == nil {
				db.applyDeltaAllIndexedFields(ptr, idx, false, indexChanges, lenChanges)
			} else {
				db.applyDeltaModifiedIndexedFields(ptr, idx, mods, false, indexChanges, lenChanges)
			}
		}

		if newV != nil {
			ptr := unsafe.Pointer(newV)
			if oldV == nil {
				db.applyDeltaAllIndexedFields(ptr, idx, true, indexChanges, lenChanges)
			} else {
				db.applyDeltaModifiedIndexedFields(ptr, idx, mods, true, indexChanges, lenChanges)
			}
		}

		switch {
		case oldV == nil && newV != nil:
			if add == nil {
				add = getRoaringBuf()
			}
			add.Add(idx)
		case oldV != nil && newV == nil:
			if rem == nil {
				rem = getRoaringBuf()
			}
			rem.Add(idx)
		}
	}

	db.publishSnapshotWithAccumDeltaNoLock(txID, indexChanges, lenChanges, add, rem)
}

func distinctCount(s []string) int {
	n := len(s)
	switch n {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		if s[0] != s[1] {
			return 2
		}
		return 1
	case 3:
		a, b, c := s[0], s[1], s[2]
		if a == b {
			if b == c {
				return 1
			}
			return 2
		}
		if a == c || b == c {
			return 2
		}
		return 3
	}
	if n <= 8 {
		return distinctCountLoop(s, n)
	}
	return len(dedupStringsInplace(s))
}

func distinctCountLoop(s []string, n int) int {
	uniq := 0
OUTER:
	for i := 0; i < n; i++ {
		v := s[i]
		for k := 0; k < i; k++ {
			if s[k] == v {
				continue OUTER
			}
		}
		uniq++
	}
	return uniq
}

func (db *DB[K, V]) checkUniqueOnWrite(idx uint64, oldVal, newVal *V, modified []string) error {
	if newVal == nil || len(db.uniqueFieldAccessors) == 0 {
		return nil
	}

	ptr := unsafe.Pointer(newVal)

	if oldVal == nil {
		for _, acc := range db.uniqueFieldAccessors {
			if err := db.checkUniqueSingleCandidate(idx, ptr, acc); err != nil {
				return err
			}
		}
		return nil
	}

	for _, name := range modified {
		acc, ok := db.indexedFieldByName[name]
		if !ok || !acc.field.Unique || acc.field.Slice {
			continue
		}
		if err := db.checkUniqueSingleCandidate(idx, ptr, acc); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB[K, V]) checkUniqueSingleCandidate(idx uint64, ptr unsafe.Pointer, acc indexedFieldAccessor) error {
	single, _, ok := acc.getter(ptr)
	if !ok || single == nilValue {
		return nil // as in classic sql, multiple nulls are ok
	}

	bm, releaseBM := lookupUniqueBitmap(db, acc.name, single)
	if bm == nil || bm.IsEmpty() {
		releaseBM()
		return nil
	}
	if bm.GetCardinality() == 1 && bm.Contains(idx) {
		releaseBM()
		return nil
	}
	releaseBM()
	// value stored in `single` can contain binary - do not print
	return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
}

func (db *DB[K, V]) publishWriteDelta(txID uint64, idx uint64, oldVal, newVal *V, modified []string) {
	indexChanges := getWriteDeltaOuterMap()
	defer releaseWriteDeltaOuterMap(indexChanges)

	lenChanges := getWriteDeltaOuterMap()
	defer releaseWriteDeltaOuterMap(lenChanges)

	var universeAdd *roaring64.Bitmap
	var universeDel *roaring64.Bitmap
	defer func() {
		releaseRoaringBuf(universeAdd)
		releaseRoaringBuf(universeDel)
	}()

	if oldVal != nil {
		ptr := unsafe.Pointer(oldVal)
		if newVal == nil {
			db.applyDeltaAllIndexedFields(ptr, idx, false, indexChanges, lenChanges)
		} else {
			db.applyDeltaModifiedIndexedFields(ptr, idx, modified, false, indexChanges, lenChanges)
		}
	}

	if newVal != nil {
		ptr := unsafe.Pointer(newVal)
		if oldVal == nil {
			db.applyDeltaAllIndexedFields(ptr, idx, true, indexChanges, lenChanges)
		} else {
			db.applyDeltaModifiedIndexedFields(ptr, idx, modified, true, indexChanges, lenChanges)
		}
	}

	switch {
	case oldVal == nil && newVal != nil:
		universeAdd = getRoaringBuf()
		universeAdd.Add(idx)
	case oldVal != nil && newVal == nil:
		universeDel = getRoaringBuf()
		universeDel.Add(idx)
	}

	db.publishSnapshotWithAccumDeltaNoLock(
		txID,
		indexChanges,
		lenChanges,
		universeAdd,
		universeDel,
	)
}

const (
	pooledUniqueOuterMaxLen     = 128
	pooledUniqueInnerMaxLen     = 2048
	pooledWriteDeltaOuterMaxLen = 128
	pooledWriteDeltaInnerMaxLen = 2048
)

var uniqueLeavingOuterPool = sync.Pool{
	New: func() any {
		return make(map[string]map[string]*roaring64.Bitmap, 8)
	},
}

var uniqueLeavingInnerPool = sync.Pool{
	New: func() any {
		return make(map[string]*roaring64.Bitmap, 8)
	},
}

var uniqueSeenOuterPool = sync.Pool{
	New: func() any {
		return make(map[string]map[string]uint64, 8)
	},
}

var uniqueSeenInnerPool = sync.Pool{
	New: func() any {
		return make(map[string]uint64, 8)
	},
}

func getUniqueLeavingOuterMap() map[string]map[string]*roaring64.Bitmap {
	return uniqueLeavingOuterPool.Get().(map[string]map[string]*roaring64.Bitmap)
}

func releaseUniqueLeavingOuterMap(m map[string]map[string]*roaring64.Bitmap) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueLeavingInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingOuterPool.Put(m)
}

func getUniqueLeavingInnerMap() map[string]*roaring64.Bitmap {
	return uniqueLeavingInnerPool.Get().(map[string]*roaring64.Bitmap)
}

func releaseUniqueLeavingInnerMap(m map[string]*roaring64.Bitmap) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	for _, bm := range m {
		releaseRoaringBuf(bm)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueLeavingInnerPool.Put(m)
}

func getUniqueSeenOuterMap() map[string]map[string]uint64 {
	return uniqueSeenOuterPool.Get().(map[string]map[string]uint64)
}

func releaseUniqueSeenOuterMap(m map[string]map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueOuterMaxLen
	for _, inner := range m {
		releaseUniqueSeenInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	uniqueSeenOuterPool.Put(m)
}

func getUniqueSeenInnerMap() map[string]uint64 {
	return uniqueSeenInnerPool.Get().(map[string]uint64)
}

func releaseUniqueSeenInnerMap(m map[string]uint64) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledUniqueInnerMaxLen
	clear(m)
	if oversized {
		return
	}
	uniqueSeenInnerPool.Put(m)
}

var writeDeltaOuterPool = sync.Pool{
	New: func() any {
		return make(map[string]map[string]indexDeltaEntry, 8)
	},
}

var writeDeltaInnerPool = sync.Pool{
	New: func() any {
		return make(map[string]indexDeltaEntry, 8)
	},
}

func getWriteDeltaOuterMap() map[string]map[string]indexDeltaEntry {
	return writeDeltaOuterPool.Get().(map[string]map[string]indexDeltaEntry)
}

func releaseWriteDeltaOuterMap(m map[string]map[string]indexDeltaEntry) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledWriteDeltaOuterMaxLen
	for _, inner := range m {
		releaseWriteDeltaInnerMap(inner)
	}
	clear(m)
	if oversized {
		return
	}
	writeDeltaOuterPool.Put(m)
}

func getWriteDeltaInnerMap() map[string]indexDeltaEntry {
	return writeDeltaInnerPool.Get().(map[string]indexDeltaEntry)
}

func releaseWriteDeltaInnerMap(m map[string]indexDeltaEntry) {
	if m == nil {
		return
	}
	oversized := len(m) > pooledWriteDeltaInnerMaxLen
	for _, e := range m {
		releaseRoaringBuf(e.add)
		releaseRoaringBuf(e.del)
	}
	clear(m)
	if oversized {
		return
	}
	writeDeltaInnerPool.Put(m)
}
