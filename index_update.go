package rbi

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func (db *DB[K, V]) checkUniqueOnWriteMulti(idxs []uint64, oldVals, newVals []*V, modified [][]string) error {

	leaving := make(map[string]map[string]*roaring64.Bitmap)
	seen := make(map[string]map[string]uint64)

	for i, idx := range idxs {
		oldVal := oldVals[i]
		var newVal *V
		if newVals != nil {
			newVal = newVals[i]
		}
		if oldVal == nil || newVal == nil {
			continue
		}

		ptr := unsafe.Pointer(oldVal)

		for _, name := range modified[i] {
			f := db.fields[name]
			if f == nil || !f.Unique {
				continue
			}
			single, _, ok := db.getters[name](ptr)
			if !ok || single == nilValue {
				continue
			}
			m := leaving[name]
			if m == nil {
				m = make(map[string]*roaring64.Bitmap)
				leaving[name] = m
			}
			bm := m[single]
			if bm == nil {
				bm = roaring64.NewBitmap()
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

		for _, name := range modified[i] {
			f := db.fields[name]
			if f == nil || !f.Unique {
				continue
			}

			single, _, ok := db.getters[name](ptr)
			if !ok || single == nilValue {
				continue
			}

			sm := seen[name]
			if sm == nil {
				sm = make(map[string]uint64)
				seen[name] = sm
			}
			if prev, ok := sm[single]; ok && prev != idx {
				return fmt.Errorf("%w: duplicate value for field %v within batch", ErrUniqueViolation, name)
			}
			sm[single] = idx

			slice := db.index[name]
			if slice == nil {
				continue
			}
			bm := findIndex(slice, single)
			if bm == nil || bm.IsEmpty() {
				continue
			}

			var lv *roaring64.Bitmap
			if fm := leaving[name]; fm != nil {
				lv = fm[single]
			}

			if lv == nil || lv.IsEmpty() {
				if bm.GetCardinality() == 1 && bm.Contains(idx) {
					continue
				}
				return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
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
				return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
			}
		}
	}
	return nil
}

func (db *DB[K, V]) setIndexOnSuccessMulti(err error, idxs []uint64, oldVals, newVals []*V, modified [][]string) error {
	if err != nil {
		return err
	}
	if db.noIndex.Load() {
		return nil
	}

	additions := make(map[string]map[string]*roaring64.Bitmap) // move outer maps to sync.pool?
	removals := make(map[string]map[string]*roaring64.Bitmap)

	for i, idx := range idxs {

		oldV := oldVals[i]
		var newV *V
		if newVals != nil {
			newV = newVals[i]
		}

		if oldV != nil {
			ptr := unsafe.Pointer(oldV)

			var mods []string
			if modified != nil {
				mods = modified[i]
			} else {
				mods = db.getModifiedIndexedFields(oldV, newV)
			}

			for _, dbname := range mods {
				f := db.fields[dbname]
				single, multi, ok := db.getters[dbname](ptr)
				if !ok {
					continue
				}

				remField := removals[dbname]
				if remField == nil {
					remField = make(map[string]*roaring64.Bitmap)
					removals[dbname] = remField
				}

				if f.Slice {
					for _, sval := range multi {
						bm := remField[sval]
						if bm == nil {
							bm = roaring64.NewBitmap()
							remField[sval] = bm
						}
						bm.Add(idx)
					}

					if lenSlice := db.lenIndex[dbname]; lenSlice != nil {
						oldLen := distinctCount(multi)
						keyLen := uint64ByteStr(uint64(oldLen))
						if bm := findIndex(lenSlice, keyLen); bm != nil {
							bm.Remove(idx)
						}
					}

				} else {
					bm := remField[single]
					if bm == nil {
						bm = roaring64.NewBitmap()
						remField[single] = bm
					}
					bm.Add(idx)
				}
			}
		}

		if newV != nil {
			ptr := unsafe.Pointer(newV)

			var mods []string
			if modified != nil {
				mods = modified[i]
			} else {
				mods = db.getModifiedIndexedFields(oldV, newV)
			}

			for _, dbname := range mods {
				f := db.fields[dbname]
				single, multi, ok := db.getters[dbname](ptr)
				if !ok {
					continue
				}

				addField := additions[dbname]
				if addField == nil {
					addField = make(map[string]*roaring64.Bitmap)
					additions[dbname] = addField
				}

				if f.Slice {
					for _, sval := range multi {
						bm := addField[sval]
						if bm == nil {
							bm = roaring64.NewBitmap()
							addField[sval] = bm
						}
						bm.Add(idx)
					}

					lenSlice := db.lenIndex[dbname]
					if lenSlice == nil {
						s := make([]index, 0, 8)
						lenSlice = &s
						db.lenIndex[dbname] = lenSlice
					}
					newLen := distinctCount(multi)
					keyLen := uint64ByteStr(uint64(newLen))
					findInsert(lenSlice, keyLen).Add(idx)

				} else {
					bm := addField[single]
					if bm == nil {
						bm = roaring64.NewBitmap()
						addField[single] = bm
					}
					bm.Add(idx)
				}
			}
		}

		switch {
		case oldV == nil && newV != nil:
			db.universe.Add(idx)
		case oldV != nil && newV == nil:
			db.universe.Remove(idx)
		}
	}

	for dbname, removedKeys := range removals {
		slice := db.index[dbname]
		if slice == nil {
			s := make([]index, 0, initialIndexLen)
			slice = &s
			db.index[dbname] = slice
		}
		for key, bmRemoved := range removedKeys {
			if bm := findIndex(slice, key); bm != nil {
				bm.AndNot(bmRemoved)
			}
		}
	}

	for dbname, addedKeys := range additions {
		slice := db.index[dbname]
		if slice == nil {
			s := make([]index, 0, initialIndexLen)
			slice = &s
			db.index[dbname] = slice
		}

		newKeys := make([]index, 0, len(addedKeys))

		for key, bmAdded := range addedKeys {
			if bm := findIndex(slice, key); bm != nil {
				bm.Or(bmAdded)
			} else {
				newKeys = append(newKeys, index{
					Key: key,
					IDs: bmAdded,
				})
			}
		}

		if len(newKeys) == 0 {
			continue
		}

		slices.SortFunc(newKeys, func(a, b index) int {
			return strings.Compare(a.Key, b.Key)
		})

		old := *slice
		merged := make([]index, 0, len(old)+len(newKeys))
		i, k := 0, 0

		for i < len(old) && k < len(newKeys) {
			if old[i].Key < newKeys[k].Key {
				merged = append(merged, old[i])
				i++
			} else if old[i].Key > newKeys[k].Key {
				merged = append(merged, newKeys[k])
				k++
			} else {
				old[i].IDs.Or(newKeys[k].IDs)
				merged = append(merged, old[i])
				i++
				k++
			}
		}

		merged = append(merged, old[i:]...)
		merged = append(merged, newKeys[k:]...)

		*slice = merged
	}

	return nil
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
	// return distinctCountMap(s, n)
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

func (db *DB[K, V]) checkUniqueOnWrite(idx uint64, newVal *V, modified []string) error {
	if newVal == nil {
		return nil
	}

	ptr := unsafe.Pointer(newVal)

	for _, name := range modified {
		f := db.fields[name]
		if f == nil || !f.Unique || f.Slice {
			continue
		}

		single, _, ok := db.getters[name](ptr)
		if !ok {
			continue
		}

		if single == nilValue {
			continue // as in classic sql, multiple nulls are ok
		}

		slice := db.index[name]
		if slice == nil {
			continue
		}
		bm := findIndex(slice, single)
		if bm == nil || bm.IsEmpty() {
			continue
		}
		if bm.GetCardinality() == 1 && bm.Contains(idx) {
			continue
		}
		// value stored in `single` can contain binary - do not print
		return fmt.Errorf("%w: value for field %v already exists", ErrUniqueViolation, name)
	}
	return nil
}

func (db *DB[K, V]) setIndexOnSuccess(err error, idx uint64, oldVal, newVal *V, modified []string) error {
	if err != nil {
		return err
	}
	if db.noIndex.Load() {
		return nil
	}

	if oldVal != nil {
		ptr := unsafe.Pointer(oldVal)
		for _, dbname := range modified {
			f := db.fields[dbname]
			single, multi, ok := db.getters[dbname](ptr)
			if !ok {
				continue
			}
			if slice := db.index[dbname]; slice != nil {
				if f.Slice {
					for _, sval := range multi {
						if bm := findIndex(slice, sval); bm != nil {
							bm.Remove(idx) // removal from the slice will be handled on index save
						}
					}
				} else {
					if bm := findIndex(slice, single); bm != nil {
						bm.Remove(idx) // removal from the slice will be handled on index save
					}
				}
			}
			if f.Slice {
				if lenSlice := db.lenIndex[dbname]; lenSlice != nil {
					oldLen := distinctCount(multi)
					keyLen := uint64ByteStr(uint64(oldLen))
					if bm := findIndex(lenSlice, keyLen); bm != nil {
						bm.Remove(idx)
					}
				}
			}
		}
	}

	if newVal != nil {
		ptr := unsafe.Pointer(newVal)
		for _, dbname := range modified {
			f := db.fields[dbname]
			single, multi, ok := db.getters[dbname](ptr)
			if !ok {
				continue
			}
			slice := db.index[dbname]
			if slice == nil {
				s := make([]index, 0, initialIndexLen)
				slice = &s
				db.index[dbname] = slice
			}
			if f.Slice {
				for _, sval := range multi {
					findInsert(slice, sval).Add(idx)
				}

				lenSlice := db.lenIndex[dbname]
				if lenSlice == nil {
					s := make([]index, 0, 8)
					lenSlice = &s
					db.lenIndex[dbname] = lenSlice
				}
				newLen := distinctCount(multi)
				keyLen := uint64ByteStr(uint64(newLen))
				findInsert(lenSlice, keyLen).Add(idx)

			} else {
				findInsert(slice, single).Add(idx)
			}
		}
	}

	switch {
	case oldVal == nil && newVal != nil:
		db.universe.Add(idx)
	case oldVal != nil && newVal == nil:
		db.universe.Remove(idx)
	}

	return nil
}
