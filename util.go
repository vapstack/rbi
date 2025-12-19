package rbi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// MakePatch returns a PreCommitFunc that populates the provided target slice
// with a patch describing all fields that have changed between old and new
// values of a record.
//
// The returned function compares both indexed and non-indexed fields.
// For each modified field, it appends a Field entry with the field name
// and a deep copy of the new value.
//
// The returned PreCommitFunc should not be used with Delete or DeleteMany (where newv is always nil).
func (db *DB[K, V]) MakePatch(target *[]Field) PreCommitFunc[K, V] {

	return func(tx *bbolt.Tx, key K, oldv, newv *V) error {

		*target = (*target)[:0]

		var rvOld, rvNew reflect.Value
		if oldv != nil {
			rvOld = reflect.ValueOf(oldv).Elem()
		}
		if newv != nil {
			rvNew = reflect.ValueOf(newv).Elem()
		}

		processed := make(map[string]struct{})

		mods := db.getModifiedIndexedFields(oldv, newv)

		for _, dbname := range mods {
			f := db.fields[dbname]

			processed[f.Name] = struct{}{}

			var newValue any
			if rvNew.IsValid() {
				newValue = rvNew.FieldByIndex(f.Index).Interface()
			}

			*target = append(*target, Field{
				Name:  dbname,
				Value: deepCopyValue(newValue),
			})
		}

		for patchKey, f := range db.patchMap {

			if _, ok := processed[f.Name]; ok {
				continue
			}

			var v1, v2 any
			if rvOld.IsValid() {
				v1 = rvOld.FieldByIndex(f.Index).Interface()
			}
			if rvNew.IsValid() {
				v2 = rvNew.FieldByIndex(f.Index).Interface()
			}

			if !reflect.DeepEqual(v1, v2) {
				*target = append(*target, Field{
					Name:  patchKey,
					Value: deepCopyValue(v2),
				})
			}
			processed[f.Name] = struct{}{}
		}

		return nil
	}
}

func (db *DB[K, V]) keyFromID(id K) []byte {
	if db.strkey {
		s := *(*string)(unsafe.Pointer(&id))
		return unsafe.Slice(unsafe.StringData(s), len(s))
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], *(*uint64)(unsafe.Pointer(&id)))
	return key[:]
}

func (db *DB[K, V]) idFromKey(b []byte) K {
	if db.strkey {
		s := string(b) // must allocate here because bytes are from bbolt
		return *(*K)(unsafe.Pointer(&s))
	}
	v := binary.BigEndian.Uint64(b)
	return *(*K)(unsafe.Pointer(&v))
}

func (db *DB[K, V]) idxFromID(id K) uint64 {
	if db.strkey {
		s := *(*string)(unsafe.Pointer(&id))
		db.strmap.Lock()
		idx := db.strmap.createIdxNoLock(s)
		db.strmap.Unlock()
		return idx
	}
	return *(*uint64)(unsafe.Pointer(&id))
}

func (db *DB[K, V]) idxsFromID(ids []K) []uint64 {
	if db.strkey {
		s := *(*[]string)(unsafe.Pointer(&ids))
		db.strmap.Lock()
		idxs := db.strmap.createIdxsNoLock(s)
		db.strmap.Unlock()
		return idxs
	}
	return *(*[]uint64)(unsafe.Pointer(&ids))
}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := new(V)
	if err := msgpack.NewDecoder(bytes.NewReader(b)).Decode(v); err != nil {
		return nil, err
	}
	return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	if err := msgpack.NewEncoder(b).Encode(v); err != nil {
		return err
	}
	return nil
}

/**/

var encodePool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func getEncodeBuf() *bytes.Buffer {
	return encodePool.Get().(*bytes.Buffer)
}

func releaseEncodeBuf(b *bytes.Buffer) {
	b.Reset()
	encodePool.Put(b)
}

/**/

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

func touch(name string) error {
	file, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	return file.Close()
}

func closeFile(f *os.File) { _ = f.Close() }

/**/

func sanitizeSuffix(s string) string {
	out := make([]rune, 0, len(s))
	dot := false

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
			dot = false
		case r >= 'A' && r <= 'Z':
			out = append(out, r)
			dot = false
		case r >= '0' && r <= '9':
			out = append(out, r)
			dot = false
		case r == '_':
			out = append(out, r)
			dot = false
		case r == '.':
			if !dot {
				out = append(out, r)
			}
			dot = true
		default:
		}
	}
	return strings.Trim(string(out), ".")
}

func dedupStringsInplace(s []string) []string {
	if len(s) < 2 {
		return s
	}
	slices.Sort(s)
	w := 1
	for i := 1; i < len(s); i++ {
		if s[i] != s[w-1] {
			s[w] = s[i]
			w++
		}
	}
	return s[:w]
}

func uint64Bytes(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return key[:]
}

func uint64ByteStr(v uint64) string {
	b := uint64Bytes(v)
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func int64ByteStr(v int64) string {
	return uint64ByteStr(uint64(v) ^ (uint64(1) << 63))
}

func float64ByteStr(f float64) string {
	u := math.Float64bits(f)
	const sign = uint64(1) << 63
	if u&sign != 0 {
		u = ^u // negative: flip all bits
	} else {
		u ^= sign // non-negative (includes +0): flip sign bit
	}
	return uint64ByteStr(u)
}

/**/

var (
	registryMu sync.Mutex
	registry   = make(map[string]struct{})
)

func regInstance(dbPath, bucket string) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return fmt.Errorf("error getting absolute file path: %w", err)
	}

	key := abs + "::" + bucket
	if _, exists := registry[key]; exists {
		return fmt.Errorf("rbi is alread open for \"%v\" at %v", bucket, dbPath)
	}

	registry[key] = struct{}{}
	return nil
}

func unregInstance(dbPath, bucket string) {
	registryMu.Lock()
	defer registryMu.Unlock()

	absPath, _ := filepath.Abs(dbPath)
	key := absPath + "::" + bucket
	delete(registry, key)
}
