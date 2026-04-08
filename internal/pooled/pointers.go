package pooled

import (
	"sync"
)

// Pointers pools *T values.
type Pointers[T any] struct {
	New     func() *T // creates new T on Get
	Init    func(*T)  // initializes T on Get
	Cleanup func(*T)  // function that is called before returning T to the pool
	Clear   bool      // zeroes T before returning it to the pool

	pool sync.Pool
}

// Get returns a pooled pointer, creating and initializing one if needed.
func (p *Pointers[T]) Get() *T {
	if pv := p.pool.Get(); pv != nil {
		v := pv.(*T)
		if p.Init != nil {
			p.Init(v)
		}
		return v
	}
	var v *T
	if p.New != nil {
		v = p.New()
	} else {
		v = new(T)
	}
	if p.Init != nil {
		p.Init(v)
	}
	return v
}

// Put returns v to the pool.
// If Cleanup is set, it is called on v before returning it to the pool.
// If Clear is set, v is zeroed before returning to the pool.
func (p *Pointers[T]) Put(v *T) {
	if v != nil {
		if p.Cleanup != nil {
			p.Cleanup(v)
		}
		if p.Clear {
			var zero T
			*v = zero
		}
		p.pool.Put(v)
	}
}
