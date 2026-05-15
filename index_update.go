package rbi

import (
	"fmt"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/schema"
)

type uniqueBatchCheckState struct {
	leaving map[string]map[keycodec.IndexLookupKey]posting.List
	seen    map[string]map[keycodec.IndexLookupKey]uint64
}

type uniqueLeavingTouch struct {
	field string
	key   keycodec.IndexLookupKey
	added bool
}

type uniqueSeenWrite struct {
	field string
	key   keycodec.IndexLookupKey
}

func markUniqueBatchLeaving(state uniqueBatchCheckState, field string, key keycodec.IndexLookupKey, idx uint64) bool {
	fm := state.leaving[field]
	if fm == nil {
		fm = uniqueLeavingInnerPool.Get()
		state.leaving[field] = fm
	}
	bm := fm[key]
	var added bool
	bm, added = bm.BuildAddedChecked(idx)
	if !added {
		return false
	}
	fm[key] = bm
	return true
}

func appendUniqueBatchLeavingTouch(state uniqueBatchCheckState, touched []uniqueLeavingTouch, field string, key keycodec.IndexLookupKey, idx uint64) []uniqueLeavingTouch {
	added := markUniqueBatchLeaving(state, field, key, idx)
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
		ids = ids.BuildRemoved(idx)
		if ids.IsEmpty() {
			delete(fm, t.key)
		} else {
			fm[t.key] = ids
		}
		if len(fm) == 0 {
			delete(state.leaving, t.field)
			uniqueLeavingInnerPool.Put(fm)
		}
	}
}

func (qe *queryEngine) checkUniqueBatchCandidateAndCollectSeen(
	state uniqueBatchCheckState,
	idx uint64,
	acc schema.IndexedFieldAccessor,
	ptr unsafe.Pointer,
	seenWrites []uniqueSeenWrite,
) ([]uniqueSeenWrite, error) {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return seenWrites, nil
	}

	if sm := state.seen[acc.Name]; sm != nil {
		if prev, ok := sm[single]; ok && prev != idx {
			return seenWrites, fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.Name)
		}
	}

	ids := qe.snapshot.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
	}

	var lv posting.List
	if fm := state.leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
		}
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
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
		return seenWrites, fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
	}
	return append(seenWrites, uniqueSeenWrite{field: acc.Name, key: single}), nil
}

func (qe *queryEngine) checkUniqueBatchAppend(state uniqueBatchCheckState, idx uint64, oldVal, newVal unsafe.Pointer) error {
	if len(qe.schema.Unique) == 0 {
		return nil
	}
	var (
		touchedInline [8]uniqueLeavingTouch
		touched       = touchedInline[:0]
	)
	if oldVal != nil {
		if newVal == nil {
			for _, acc := range qe.schema.Unique {
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
			}
		} else {
			for _, acc := range qe.schema.Unique {
				if !acc.Modified(oldVal, newVal) {
					continue
				}
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				touched = appendUniqueBatchLeavingTouch(state, touched, acc.Name, single, idx)
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

	if oldVal == nil {
		for _, acc := range qe.schema.Unique {
			var err error
			seenWrites, err = qe.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	} else {
		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			var err error
			seenWrites, err = qe.checkUniqueBatchCandidateAndCollectSeen(state, idx, acc, newVal, seenWrites)
			if err != nil {
				rollbackUniqueBatchLeaving(state, touched, idx)
				return err
			}
		}
	}

	for _, w := range seenWrites {
		sm := state.seen[w.field]
		if sm == nil {
			sm = uniqueSeenInnerPool.Get()
			state.seen[w.field] = sm
		}
		sm[w.key] = idx
	}
	return nil
}

func collapseUniqueWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer, pos map[uint64]int) ([]uint64, []unsafe.Pointer, []unsafe.Pointer) {
	n := 0
	for i, idx := range idxs {
		p := pos[idx]
		if p != 0 {
			if newVals != nil {
				newVals[p-1] = newVals[i]
			}
			continue
		}
		pos[idx] = n + 1
		if n != i {
			idxs[n] = idx
			oldVals[n] = oldVals[i]
			if newVals != nil {
				newVals[n] = newVals[i]
			}
		}
		n++
	}
	idxs = idxs[:n]
	oldVals = oldVals[:n]
	if newVals != nil {
		newVals = newVals[:n]
	}
	return idxs, oldVals, newVals
}

func (qe *queryEngine) checkUniqueOnWriteMulti(idxs []uint64, oldVals, newVals []unsafe.Pointer) error {
	if len(idxs) == 0 || len(qe.schema.Unique) == 0 {
		return nil
	}

	pos := uint64IntMapPool.Get(len(idxs))
	defer uint64IntMapPool.Put(pos)

	idxs, oldVals, newVals = collapseUniqueWriteMulti(idxs, oldVals, newVals, pos)

	leaving := uniqueLeavingOuterPool.Get()
	defer uniqueLeavingOuterPool.Put(leaving)

	seen := uniqueSeenOuterPool.Get()
	defer uniqueSeenOuterPool.Put(seen)

	for i, idx := range idxs {
		oldVal := oldVals[i]
		var newVal unsafe.Pointer
		if newVals != nil {
			newVal = newVals[i]
		}
		if oldVal == nil {
			continue
		}

		// On delete (newVal == nil), the record releases all unique scalar values.
		// On update, only modified unique fields can release previous values.
		if newVal == nil {
			for _, acc := range qe.schema.Unique {
				single, ok, isNil := acc.UniqueGetter(oldVal)
				if !ok || isNil {
					continue
				}
				m := leaving[acc.Name]
				if m == nil {
					m = uniqueLeavingInnerPool.Get()
					leaving[acc.Name] = m
				}
				ids := m[single]
				ids = ids.BuildAdded(idx)
				m[single] = ids
			}
			continue
		}

		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVal, newVal) {
				continue
			}
			single, ok, isNil := acc.UniqueGetter(oldVal)
			if !ok || isNil {
				continue
			}
			m := leaving[acc.Name]
			if m == nil {
				m = uniqueLeavingInnerPool.Get()
				leaving[acc.Name] = m
			}
			ids := m[single]
			ids = ids.BuildAdded(idx)
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

		if oldVals[i] == nil {
			for _, acc := range qe.schema.Unique {
				if err := qe.checkUniqueBatchCandidate(idx, newVal, acc, seen, leaving); err != nil {
					return err
				}
			}
			continue
		}

		for _, acc := range qe.schema.Unique {
			if !acc.Modified(oldVals[i], newVal) {
				continue
			}
			if err := qe.checkUniqueBatchCandidate(idx, newVal, acc, seen, leaving); err != nil {
				return err
			}
		}
	}
	return nil
}

func (qe *queryEngine) checkUniqueBatchCandidate(
	idx uint64,
	ptr unsafe.Pointer,
	acc schema.IndexedFieldAccessor,
	seen map[string]map[keycodec.IndexLookupKey]uint64,
	leaving map[string]map[keycodec.IndexLookupKey]posting.List,
) error {
	single, ok, isNil := acc.UniqueGetter(ptr)
	if !ok || isNil {
		return nil
	}

	sm := seen[acc.Name]
	if sm == nil {
		sm = uniqueSeenInnerPool.Get()
		seen[acc.Name] = sm
	}
	if prev, ok := sm[single]; ok && prev != idx {
		return fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, acc.Name)
	}
	sm[single] = idx

	ids := qe.snapshot.Current().FieldLookupPostingRetainedKey(acc.Name, single)
	if ids.IsEmpty() {
		return nil
	}

	var lv posting.List
	if fm := leaving[acc.Name]; fm != nil {
		lv = fm[single]
	}

	if lv.IsEmpty() {
		if ids.Cardinality() == 1 && ids.Contains(idx) {
			return nil
		}
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
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
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, acc.Name)
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
