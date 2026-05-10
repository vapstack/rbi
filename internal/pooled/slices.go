package pooled

import (
	"cmp"
	"math/bits"
	"slices"
	"sync"
	"unsafe"
)

type SlicePool[T any] struct {
	maxCap    int
	maxShift  int
	maxRetain int

	policy  SliceClearPolicy
	cleanup func([]T)

	pools []sync.Pool
}

type SliceClearPolicy byte

const (
	sliceMinShift    = 5
	sliceMinCap      = 1 << sliceMinShift
	sliceMinCapMask  = sliceMinCap - 1
	sliceMaxDistance = 3
)

const (
	NoClear SliceClearPolicy = iota
	ClearLen
	ClearCap
)

func NewSlicePool[T any](maxCap uint, clearPolicy SliceClearPolicy, cleanup ...func([]T)) *SlicePool[T] {
	maxShift := bits.Len(max(maxCap, 32) - 1)
	maxCap2 := 1 << maxShift
	p := &SlicePool[T]{
		maxCap:    maxCap2,
		maxShift:  maxShift,
		maxRetain: max(64, maxCap2+maxCap2/4),
		policy:    clearPolicy,
		pools:     make([]sync.Pool, maxShift+1),
	}
	if len(cleanup) > 0 {
		p.cleanup = cleanup[0]
	}
	return p
}

func (s *SlicePool[T]) Get(capHint int) []T {
	if capHint > s.maxCap {
		return make([]T, 0, capHint)
	}
	shift := bits.Len(uint(max(capHint-1, sliceMinCapMask))) // ceil(log2(cap))
	search := min(shift+sliceMaxDistance, s.maxShift)
	for sh := shift; sh <= search; sh++ {
		if v := s.pools[sh].Get(); v != nil {
			n := 1 << sh
			r := unsafe.Slice(v.(*T), n)
			return r[:0:n]
		}
	}
	return make([]T, 0, 1<<shift)
}

func (s *SlicePool[T]) Put(v []T) {
	if s.cleanup != nil {
		s.cleanup(v)
	}
	c := cap(v)
	if c < sliceMinCap || c > s.maxRetain {
		return
	}
	switch s.policy {
	case ClearLen:
		clear(v)
	case ClearCap:
		clear(v[:c])
	}
	shift := min(s.maxShift, bits.Len(uint(c))-1) // floor(log2(cap))
	n := 1 << shift
	s.pools[shift].Put(unsafe.SliceData(v[:n:n]))
}

const (
	maxNumericPooledCap = 1 << 20
	maxStringPooledCap  = 64 << 10
)

var (
	bytePool    = NewSlicePool[byte](maxNumericPooledCap, NoClear)
	boolPool    = NewSlicePool[bool](maxNumericPooledCap, NoClear)
	intPool     = NewSlicePool[int](maxNumericPooledCap, NoClear)
	int32Pool   = NewSlicePool[int32](maxNumericPooledCap, NoClear)
	int64Pool   = NewSlicePool[int64](maxNumericPooledCap, NoClear)
	uintPool    = NewSlicePool[uint](maxNumericPooledCap, NoClear)
	uint32Pool  = NewSlicePool[uint32](maxNumericPooledCap, NoClear)
	uint64Pool  = NewSlicePool[uint64](maxNumericPooledCap, NoClear)
	float32Pool = NewSlicePool[float32](maxNumericPooledCap, NoClear)
	float64Pool = NewSlicePool[float64](maxNumericPooledCap, NoClear)
	stringPool  = NewSlicePool[string](maxStringPooledCap, ClearCap)
)

func GetBoolSlice(capHint int) []bool { return boolPool.Get(capHint) }
func PutBoolSlice(s []bool)           { boolPool.Put(s) }

func GetIntSlice(capHint int) []int { return intPool.Get(capHint) }
func PutIntSlice(s []int)           { intPool.Put(s) }

func GetInt32Slice(capHint int) []int32 { return int32Pool.Get(capHint) }
func PutInt32Slice(s []int32)           { int32Pool.Put(s) }

func GetInt64Slice(capHint int) []int64 { return int64Pool.Get(capHint) }
func PutInt64Slice(s []int64)           { int64Pool.Put(s) }

func GetUintSlice(capHint int) []uint { return uintPool.Get(capHint) }
func PutUintSlice(s []uint)           { uintPool.Put(s) }

func GetUint32Slice(capHint int) []uint32 { return uint32Pool.Get(capHint) }
func PutUint32Slice(s []uint32)           { uint32Pool.Put(s) }

func GetUint64Slice(capHint int) []uint64 { return uint64Pool.Get(capHint) }
func PutUint64Slice(s []uint64)           { uint64Pool.Put(s) }

func GetStringSlice(capHint int) []string { return stringPool.Get(capHint) }
func PutStringSlice(s []string)           { stringPool.Put(s) }

func GetByteSlice(capHint int) []byte { return bytePool.Get(capHint) }
func PutByteSlice(s []byte)           { bytePool.Put(s) }

func GetFloat32Slice(capHint int) []float32 { return float32Pool.Get(capHint) }
func PutFloat32Slice(s []float32)           { float32Pool.Put(s) }

func GetFloat64Slice(capHint int) []float64 { return float64Pool.Get(capHint) }
func PutFloat64Slice(s []float64)           { float64Pool.Put(s) }

/**/

// Slices pools []T backing arrays.
// Deprecated: use NewSlicePool.
type Slices[T any] struct {
	MinCap  int             // minimum capacity
	MaxCap  int             // maximum capacity (drop if greater)
	Cleanup func(*Slice[T]) // function that is called before returning Slice to the pool
	Clear   bool            // clears contents with clear(values[:cap(values)])

	pool sync.Pool
}

// Get returns a pooled slice buffer or allocates a new one.
func (s *Slices[T]) Get() *Slice[T] {
	if v := s.pool.Get(); v != nil {
		return v.(*Slice[T])
	}
	return &Slice[T]{
		values: make([]T, 0, max(s.MinCap, 8)),
	}
}

// Put releases v back to the pool after applying configured cleanup.
func (s *Slices[T]) Put(v *Slice[T]) {
	if s.Cleanup != nil {
		s.Cleanup(v)
	}
	if s.MaxCap > 0 && cap(v.values) > s.MaxCap {
		return
	}
	if s.Clear {
		clear(v.values[:cap(v.values)])
	}
	v.values = v.values[:0]
	s.pool.Put(v)
}

/**/

// Slice owns a pooled []T backing array.
// Deprecated: use NewSlicePool.
type Slice[T any] struct{ values []T }

// Len returns the current slice length.
func (b *Slice[T]) Len() int { return len(b.values) }

// Cap returns the current slice capacity.
func (b *Slice[T]) Cap() int { return cap(b.values) }

// Truncate resets the slice length to zero.
func (b *Slice[T]) Truncate() { b.values = b.values[:0] }

// Clear calls clear on the underlying slice.
func (b *Slice[T]) Clear() { clear(b.values) }

// Append appends values to the buffer.
func (b *Slice[T]) Append(v T) { b.values = append(b.values, v) }

// AppendAll appends a slice of values to the buffer.
func (b *Slice[T]) AppendAll(s []T) { b.values = append(b.values, s...) }

// SetLen sets the slice length, growing capacity if needed.
func (b *Slice[T]) SetLen(l int) {
	if cap(b.values) < l {
		b.values = slices.Grow(b.values, l-len(b.values))
	}
	b.values = b.values[:l]
}

// Grow ensures capacity for another l elements without changing the current length.
func (b *Slice[T]) Grow(l int) { b.values = slices.Grow(b.values, l) }

// Get returns the element at index i.
func (b *Slice[T]) Get(i int) T { return b.values[i] }

// GetPtr returns a pointer to the element at index i.
func (b *Slice[T]) GetPtr(i int) *T { return &b.values[i] }

// Set sets the element at index i.
func (b *Slice[T]) Set(i int, v T) { b.values[i] = v }

func SortSlice[T cmp.Ordered](s *Slice[T]) {
	slices.Sort(s.values)
}
