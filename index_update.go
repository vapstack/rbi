package rbi

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

type uniqueBatchCheckState struct {
	leaving map[string]map[string]posting.List
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
	fm := state.leaving[field]
	if fm == nil {
		fm = getUniqueLeavingInnerMap()
		state.leaving[field] = fm
	}
	bm := fm[key]
	if !bm.CheckedAdd(idx) {
		return false
	}
	fm[key] = bm
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
		ids := fm[t.key]
		if ids.IsEmpty() {
			continue
		}
		ids.Remove(idx)
		if ids.IsEmpty() {
			delete(fm, t.key)
		} else {
			fm[t.key] = ids
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
	single, ok, isNil := acc.uniqueGetter(ptr)
	if !ok || isNil {
		return seenWrites, nil
	}

	if sm := state.seen[acc.name]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.name)
		}
	}

	ids := db.getSnapshot().fieldLookupPostingRetained(acc.name, single)
	if ids.IsEmpty() {
		return append(seenWrites, uniqueSeenWrite{field: acc.name, key: single}), nil
	}

	var lv posting.List
	if fm := state.leaving[acc.name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return append(seenWrites, uniqueSeenWrite{field: acc.name, key: single}), nil
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}

	iter := ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}
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
				single, ok, isNil := acc.uniqueGetter(ptrOld)
				if !ok || isNil {
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
				single, ok, isNil := acc.uniqueGetter(ptrOld)
				if !ok || isNil {
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
				collapsedModified = append(collapsedModified, db.getModifiedUniqueFields(it.old, it.new))
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
				single, ok, isNil := acc.uniqueGetter(ptr)
				if !ok || isNil {
					continue
				}
				m := leaving[acc.name]
				if m == nil {
					m = getUniqueLeavingInnerMap()
					leaving[acc.name] = m
				}
				ids := m[single]
				ids.Add(idx)
				m[single] = ids
			}
			continue
		}

		for _, name := range modified[i] {
			acc, ok := db.indexedFieldByName[name]
			if !ok || !acc.field.Unique || acc.field.Slice {
				continue
			}
			single, ok, isNil := acc.uniqueGetter(ptr)
			if !ok || isNil {
				continue
			}
			m := leaving[acc.name]
			if m == nil {
				m = getUniqueLeavingInnerMap()
				leaving[acc.name] = m
			}
			ids := m[single]
			ids.Add(idx)
			m[single] = ids
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
	leaving map[string]map[string]posting.List,
) error {
	single, ok, isNil := acc.uniqueGetter(ptr)
	if !ok || isNil {
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

	ids := db.getSnapshot().fieldLookupPostingRetained(acc.name, single)
	if ids.IsEmpty() {
		return nil
	}

	var lv posting.List
	if fm := leaving[acc.name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return nil
		}
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}

	iter := ids.Iter()
	defer iter.Release()
	for iter.HasNext() {
		other := iter.Next()
		if other == idx {
			continue
		}
		if lv.Contains(other) {
			continue
		}
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.name)
	}
	return nil
}

func countDistinct(s []string) int {
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
		return countDistinctLinear(s, n)
	}
	return len(dedupStringsInplace(s))
}

func countDistinctLinear(s []string, n int) int {
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

const (
	pooledUniqueOuterMaxLen = 128
	pooledUniqueInnerMaxLen = 2048
)
