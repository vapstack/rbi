package pooled

import (
	"cmp"
	"iter"
	"slices"
	"sync"
)

// Slices pools SliceBuf owners for []T backing arrays.
type Slices[T any] struct {
	MinCap  int                // minimum capacity
	MaxCap  int                // maximum capacity (drop if greater)
	Cleanup func(*SliceBuf[T]) // function that is called before returning SliceBuf to the pool
	Clear   bool               // clears contents with clear(values[:cap(values)])

	pool sync.Pool
}

// Get returns a pooled slice buffer or allocates a new one.
func (s *Slices[T]) Get() *SliceBuf[T] {
	if v := s.pool.Get(); v != nil {
		return v.(*SliceBuf[T])
	}
	return &SliceBuf[T]{
		values: make([]T, 0, max(s.MinCap, 8)),
	}
}

// Put releases v back to the pool after applying configured cleanup.
func (s *Slices[T]) Put(v *SliceBuf[T]) {
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

// SliceBuf owns a pooled []T backing array.
type SliceBuf[T any] struct{ values []T }

// Len returns the current slice length.
func (b *SliceBuf[T]) Len() int { return len(b.values) }

// Cap returns the current slice capacity.
func (b *SliceBuf[T]) Cap() int { return cap(b.values) }

// Truncate resets the slice length to zero.
func (b *SliceBuf[T]) Truncate() { b.values = b.values[:0] }

// Clear calls clear on the underlying slice.
func (b *SliceBuf[T]) Clear() { clear(b.values) }

// Append appends values to the buffer.
func (b *SliceBuf[T]) Append(v T) { b.values = append(b.values, v) }

// AppendAll appends a slice of values to the buffer.
func (b *SliceBuf[T]) AppendAll(s []T) { b.values = append(b.values, s...) }

// SetLen sets the slice length, growing capacity if needed.
func (b *SliceBuf[T]) SetLen(l int) {
	if cap(b.values) < l {
		b.values = slices.Grow(b.values, l-len(b.values))
	}
	b.values = b.values[:l]
}

// Grow ensures capacity for another l elements without changing the current length.
func (b *SliceBuf[T]) Grow(l int) { b.values = slices.Grow(b.values, l) }

// Get returns the element at index i.
func (b *SliceBuf[T]) Get(i int) T { return b.values[i] }

// GetPtr returns a pointer to the element at index i.
func (b *SliceBuf[T]) GetPtr(i int) *T { return &b.values[i] }

// Set sets the element at index i.
func (b *SliceBuf[T]) Set(i int, v T) { b.values[i] = v }

// Values returns an iterator over the current elements.
func (b *SliceBuf[T]) Values() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i, v := range b.values {
			if !yield(i, v) {
				return
			}
		}
	}
}

func SortSlice[T cmp.Ordered](s *SliceBuf[T]) {
	slices.Sort(s.values)
}

/*
func (b *SliceBuf[T]) Scan(start, end int) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i, v := range b.values {
			if i >= start && i <= end {
				if !yield(i, v) {
					return
				}
			}
		}
	}
}
*/
