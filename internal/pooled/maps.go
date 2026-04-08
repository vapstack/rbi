package pooled

import "sync"

// Maps pools map[K]V values.
type Maps[K comparable, V any] struct {
	NewCap  int           // initial capacity
	MaxLen  int           // maximum items count (drop if greater)
	Cleanup func(map[K]V) // function that is called before returning the map to the pool

	pool sync.Pool
}

// Get returns a pooled map or allocates a new one.
func (p *Maps[K, V]) Get(capHint ...int) map[K]V {
	if pv := p.pool.Get(); pv != nil {
		return pv.(map[K]V)
	}
	if len(capHint) > 0 {
		return make(map[K]V, max(p.NewCap, capHint[0]))
	}
	return make(map[K]V, max(p.NewCap, 16))
}

// Put returns m to the pool.
// If Cleanup is set it is called on m before returning it to the pool.
func (p *Maps[K, V]) Put(m map[K]V) {
	if m != nil {
		l := len(m)
		if p.Cleanup != nil {
			p.Cleanup(m)
		}
		if p.MaxLen > 0 && l > p.MaxLen {
			return
		}
		clear(m)
		p.pool.Put(m)
	}
}
