package rbi

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name is the Go struct field name,
// and whose Value is a deep copy of the value taken from newVal.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V) []Field {
	return db.makePatch(oldVal, newVal, make([]Field, 0))
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field) []Field {
	return db.makePatch(oldVal, newVal, dst)
}

var patchMapPool = sync.Pool{
	New: func() any {
		return make(map[string]struct{})
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field) []Field {

	target = target[:0]

	if newVal == nil {
		return target
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	processed := patchMapPool.Get().(map[string]struct{})
	clear(processed)
	defer patchMapPool.Put(processed)

	mods := db.getModifiedIndexedFields(oldVal, newVal)

	for _, dbname := range mods {
		f := db.fields[dbname]

		processed[f.Name] = struct{}{}

		var newValue any
		if rvNew.IsValid() {
			newValue = rvNew.FieldByIndex(f.Index).Interface()
		}

		target = append(target, Field{
			Name:  f.Name, // dbname,
			Value: deepCopyValue(newValue),
		})
	}

	for patchKey, f := range db.patchMap {
		if patchKey != f.Name {
			continue
		}
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
			target = append(target, Field{
				Name:  f.Name,
				Value: deepCopyValue(v2),
			})
		}
		processed[f.Name] = struct{}{}
	}

	return target
}

// CollectPatch returns a PreCommitFunc that populates the provided target slice
// with a patch describing all fields that have changed between old and new
// values of a record.
//
// The returned function compares both indexed and non-indexed fields.
// For each modified field, it appends a Field entry with the field name
// and a deep copy of the new value.
//
// IMPORTANT: When used with SetMany/PatchMany, this callback is invoked once per record.
// Since it always overwrites the same target slice, the final contents will correspond
// to the last processed record only. For batched writes, use CollectPatchMany instead.
//
// The returned PreCommitFunc should not be used with Delete or DeleteMany
// (where newVal is always nil); in such cases the resulting slice will be empty.
/*
func (db *DB[K, V]) CollectPatch(target *[]Field) PreCommitFunc[K, V] {

	return func(tx *bbolt.Tx, _ K, oldVal, newVal *V) error {

		*target = (*target)[:0]

		if newVal == nil {
			return nil
		}

		var rvOld, rvNew reflect.Value
		if oldVal != nil {
			rvOld = reflect.ValueOf(oldVal).Elem()
		}
		rvNew = reflect.ValueOf(newVal).Elem()

		processed := make(map[string]struct{})

		mods := db.getModifiedIndexedFields(oldVal, newVal)

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
*/

// CollectPatchMany returns a PreCommitFunc that collects per-record patches into target,
// keyed by the record ID.
//
// It is designed for SetMany/PatchMany: the callback is invoked once per record, and
// this helper stores the corresponding patch under target[key].
//
// The returned function compares both indexed and non-indexed fields.
// For each modified field, it appends a Field entry with the field name
// and a deep copy of the new value.
//
// If newVal is nil (e.g. Delete/DeleteMany), no patch is produced for that key.
// The provided map must be initialized by the caller.
/*
func (db *DB[K, V]) CollectPatchMany(target map[K][]Field) PreCommitFunc[K, V] {

	return func(tx *bbolt.Tx, key K, oldVal, newVal *V) error {

		if newVal == nil {
			return nil
		}

		var rvOld, rvNew reflect.Value
		if oldVal != nil {
			rvOld = reflect.ValueOf(oldVal).Elem()
		}
		rvNew = reflect.ValueOf(newVal).Elem()

		out := target[key] // reuse existing if exists
		out = out[:0]

		processed := make(map[string]struct{})

		mods := db.getModifiedIndexedFields(oldVal, newVal)

		for _, dbname := range mods {
			f := db.fields[dbname]

			processed[f.Name] = struct{}{}

			var newValue any
			if rvNew.IsValid() {
				newValue = rvNew.FieldByIndex(f.Index).Interface()
			}

			out = append(out, Field{
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
				out = append(out, Field{
					Name:  patchKey,
					Value: deepCopyValue(v2),
				})
			}
			processed[f.Name] = struct{}{}
		}

		target[key] = out
		return nil
	}
}
*/

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

var msgpackEncPool = sync.Pool{New: func() any { return msgpack.NewEncoder(io.Discard) }}
var msgpackDecPool = sync.Pool{New: func() any { return msgpack.NewDecoder(strings.NewReader("")) }}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := new(V)
	dec := msgpackDecPool.Get().(*msgpack.Decoder)
	dec.Reset(bytes.NewReader(b))
	err := dec.Decode(v)
	msgpackDecPool.Put(dec)
	if err != nil {
		return nil, err
	}
	return v, nil

	// if err := msgpack.NewDecoder(bytes.NewReader(b)).Decode(v); err != nil {
	// 	return nil, err
	// }
	// return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	enc := msgpackEncPool.Get().(*msgpack.Encoder)
	enc.Reset(b)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err

	// if err := msgpack.NewEncoder(b).Encode(v); err != nil {
	// 	return err
	// }
	// return nil
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
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0600)
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
