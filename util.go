package rbi

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Codec overrides default msgpack encoding/decoding for *V.
//
// EncodeRBI must write the full encoded form of the receiver to w.
// DecodeRBI must populate the receiver from the encoded form read from r.
//
// RBI does not retry decoding with msgpack when DecodeRBI returns an error.
// If fallback decoding is needed, implement it inside the
// custom codec, for example by using github.com/vmihailenco/msgpack/v5.
type Codec interface {
	EncodeRBI(io.Writer) error
	DecodeRBI(io.Reader) error
}

var codecType = reflect.TypeFor[Codec]()

// PatchOption controls MakePatch behaviour.
type PatchOption uint8

const (
	// PatchJSON makes MakePatch emit json tag names when present.
	// Fields without a json tag fall back to their Go struct field name.
	PatchJSON PatchOption = 1 << iota
)

// MakePatch builds and returns a patch describing fields that changed between
// oldVal and newVal.
//
// The patch includes both indexed and non-indexed fields. For every modified
// field it adds a Field entry whose Name uses the db tag when present or
// Go struct field name otherwise.
//
// When PatchJSON is passed, Name uses the json tag when present or
// Go struct field name otherwise.
//
// Value is always a deep copy taken from newVal.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatch(oldVal, newVal *V, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, nil, useJSON)
}

// MakePatchInto is like MakePatch, but writes the result into the provided
// buffer to reduce allocations.
//
// dst is treated as scratch space: it will be reset to length 0 and then filled
// with the resulting patch. The returned slice may refer to the same underlying
// array or a grown one if capacity is insufficient.
//
// If newVal is nil, it returns an empty slice.
func (db *DB[K, V]) MakePatchInto(oldVal, newVal *V, dst []Field, opts ...PatchOption) []Field {
	useJSON := false
	for _, opt := range opts {
		if opt == PatchJSON {
			useJSON = true
		}
	}
	return db.makePatch(oldVal, newVal, dst, useJSON)
}

type patchScratch struct {
	seen []bool
}

var patchScratchPool = pooled.Pointers[patchScratch]{
	Cleanup: func(scratch *patchScratch) {
		clear(scratch.seen[:cap(scratch.seen)])
		scratch.seen = scratch.seen[:0]
	},
}

func (db *DB[K, V]) makePatch(oldVal, newVal *V, target []Field, useJSON bool) []Field {
	target = target[:0]

	if newVal == nil {
		return target
	}

	var rvOld, rvNew reflect.Value
	if oldVal != nil {
		rvOld = reflect.ValueOf(oldVal).Elem()
	}
	rvNew = reflect.ValueOf(newVal).Elem()

	scratch := patchScratchPool.Get()
	patchAccess := db.schema.Patch.Access
	scratch.seen = slices.Grow(scratch.seen[:0], len(patchAccess))[:len(patchAccess)]
	defer patchScratchPool.Put(scratch)

	newPtr := unsafe.Pointer(newVal)
	oldPtr := unsafe.Pointer(nil)
	if oldVal != nil {
		oldPtr = unsafe.Pointer(oldVal)
	}

	db.forEachModifiedIndexedField(oldVal, newVal, func(acc schema.IndexedFieldAccessor) bool {
		if acc.PatchOrdinal < 0 {
			return true
		}
		patchAcc := patchAccess[acc.PatchOrdinal]
		var value any
		if patchAcc.CopyValue != nil {
			value = patchAcc.CopyValue(newPtr)
		} else {
			value = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		}
		name := patchAcc.Field.DBName
		if useJSON {
			name = patchAcc.Field.JSONName
		}
		scratch.seen[acc.PatchOrdinal] = true
		target = append(target, Field{
			Name:  name,
			Value: value,
		})
		return true
	})

	for ordinal, patchAcc := range patchAccess {
		if scratch.seen[ordinal] {
			continue
		}

		var newValue any
		if rvOld.IsValid() {
			if patchAcc.ValueEqual != nil {
				if patchAcc.ValueEqual(oldPtr, newPtr) {
					continue
				}
			} else {
				oldValue := rvOld.FieldByIndex(patchAcc.Field.Index).Interface()
				newValue = rvNew.FieldByIndex(patchAcc.Field.Index).Interface()
				if reflect.DeepEqual(oldValue, newValue) {
					continue
				}
			}
		}
		if patchAcc.CopyValue != nil {
			newValue = patchAcc.CopyValue(newPtr)
		} else if newValue == nil {
			newValue = deepCopyValue(rvNew.FieldByIndex(patchAcc.Field.Index).Interface())
		} else {
			newValue = deepCopyValue(newValue)
		}
		name := patchAcc.Field.DBName
		if useJSON {
			name = patchAcc.Field.JSONName
		}

		target = append(target, Field{
			Name:  name,
			Value: newValue,
		})
	}

	return target
}

func (db *DB[K, V]) userKeyFromBytes(b []byte) K {
	return keycodec.UserKeyFromBytes[K](b, db.strKey)
}

func (db *DB[K, V]) idxFromUserKey(id K) uint64 {
	idx, _ := db.idxFromUserKeyWithCreated(id)
	return idx
}

func (db *DB[K, V]) idxFromUserKeyWithCreated(id K) (uint64, bool) {
	if db.strKey {
		s := *(*string)(unsafe.Pointer(&id))
		return db.strMap.Create(s)
	}
	return *(*uint64)(unsafe.Pointer(&id)), false
}

var msgpackEncPool = pooled.Pointers[msgpack.Encoder]{
	New: func() *msgpack.Encoder { return msgpack.NewEncoder(io.Discard) },
	Cleanup: func(enc *msgpack.Encoder) {
		enc.Reset(io.Discard)
	},
}

var msgpackDecPool = pooled.Pointers[msgpack.Decoder]{
	New: func() *msgpack.Decoder {
		return msgpack.NewDecoder(strings.NewReader(""))
	},
}

var decodeReaderPool = pooled.Pointers[bytes.Reader]{
	Clear: true,
}

func defaultCodecMethods[V any]() (func(*V, io.Writer) error, func(*V, io.Reader) error, error) {
	t := reflect.TypeFor[*V]()
	if !t.Implements(codecType) {
		return nil, nil, nil
	}
	if _, bad := reflect.TypeFor[V]().MethodByName("DecodeRBI"); bad {
		return nil, nil, fmt.Errorf("invalid Codec implementation for %v: DecodeRBI must have pointer receiver", t)
	}

	encodeMethod, ok := t.MethodByName("EncodeRBI")
	if !ok {
		return nil, nil, nil
	}
	encodeFn, ok := encodeMethod.Func.Interface().(func(*V, io.Writer) error)
	if !ok {
		return nil, nil, nil
	}

	decodeMethod, ok := t.MethodByName("DecodeRBI")
	if !ok {
		return nil, nil, nil
	}
	decodeFn, ok := decodeMethod.Func.Interface().(func(*V, io.Reader) error)
	if !ok {
		return nil, nil, nil
	}

	return encodeFn, decodeFn, nil
}

func (db *DB[K, V]) decode(b []byte) (*V, error) {
	v := db.recPool.Get()

	reader := decodeReaderPool.Get()
	defer decodeReaderPool.Put(reader)

	reader.Reset(b)

	if db.decodeFn != nil {
		if err := db.decodeFn(v, reader); err != nil {
			db.ReleaseRecords(v)
			return nil, err
		}
		return v, nil
	}

	dec := msgpackDecPool.Get()
	defer msgpackDecPool.Put(dec)

	dec.Reset(reader)

	if err := dec.Decode(v); err != nil {
		db.ReleaseRecords(v)
		return nil, err
	}
	return v, nil
}

func (db *DB[K, V]) encode(v *V, b *bytes.Buffer) error {
	if db.encodeFn != nil {
		return db.encodeFn(v, b)
	}
	enc := msgpackEncPool.Get()
	enc.Reset(b)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidBucketName)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
				continue
			}
			return fmt.Errorf(
				"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
				ErrInvalidBucketName,
				name,
			)
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return fmt.Errorf(
			"%w %q: allowed pattern is [A-Za-z_][A-Za-z0-9_]*",
			ErrInvalidBucketName,
			name,
		)
	}
	return nil
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

func compareFloat64QuerySemantics(a, b float64) int {
	a = keycodec.CanonicalizeFloat64ForIndex(a)
	b = keycodec.CanonicalizeFloat64ForIndex(b)

	aNaN := math.IsNaN(a)
	bNaN := math.IsNaN(b)
	switch {
	case aNaN && bNaN:
		return 0
	case aNaN:
		return 1
	case bNaN:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

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
		return fmt.Errorf("rbi is already open for \"%v\" at %v", bucket, dbPath)
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

const randStreamMix uint64 = 0x9e3779b97f4a7c15

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
}
