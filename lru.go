package rbi

import (
	"container/list"
	"sync"

	"github.com/vapstack/qx"
)

type LRUCache[K ~string | ~uint64, V any] struct {
	*DB[K, V]
	mu           sync.Mutex
	list         *list.List
	cache        map[K]*list.Element
	capacity     int
	queryToKeys  map[string][]K
	keyToQueries map[K]map[string]struct{}
}

func NewLRUCache[K ~string | ~uint64, V any](db *DB[K, V], capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		DB:           db,
		capacity:     capacity,
		list:         list.New(),
		cache:        make(map[K]*list.Element),
		queryToKeys:  make(map[string][]K),
		keyToQueries: make(map[K]map[string]struct{}),
	}
}

type entry[K ~string | ~uint64, V any] struct {
	key   K
	value *V
}

func (c *LRUCache[K, V]) moveToFront(e *list.Element) {
	c.list.MoveToFront(e)
}

func (c *LRUCache[K, V]) removeOldest() {
	e := c.list.Back()
	if e == nil {
		return
	}
	c.list.Remove(e)
	ent := e.Value.(*entry[K, V])
	delete(c.cache, ent.key)
}

func (c *LRUCache[K, V]) add(key K, value *V) {
	if ee, ok := c.cache[key]; ok {
		c.list.MoveToFront(ee)
		ee.Value.(*entry[K, V]).value = value
		return
	}
	e := &entry[K, V]{key: key, value: value}
	elem := c.list.PushFront(e)
	c.cache[key] = elem
	if c.list.Len() > c.capacity {
		c.removeOldest()
	}
}

func (c *LRUCache[K, V]) delete(key K) {
	if e, ok := c.cache[key]; ok {
		c.list.Remove(e)
		delete(c.cache, key)
	}
}

func (c *LRUCache[K, V]) Get(id K) (*V, error) {
	c.mu.Lock()
	if e, ok := c.cache[id]; ok {
		c.moveToFront(e)
		val := e.Value.(*entry[K, V]).value
		c.mu.Unlock()
		return val, nil
	}
	c.mu.Unlock()

	v, err := c.DB.Get(id)
	if err != nil || v == nil {
		return v, err
	}

	c.mu.Lock()
	c.add(id, v)
	c.mu.Unlock()

	return v, nil
}

func (c *LRUCache[K, V]) GetMany(ids ...K) ([]*V, error) {
	results := make([]*V, len(ids))
	var missing []K
	var missIdx []int

	c.mu.Lock()
	for i, id := range ids {
		if e, ok := c.cache[id]; ok {
			c.moveToFront(e)
			results[i] = e.Value.(*entry[K, V]).value
		} else {
			missing = append(missing, id)
			missIdx = append(missIdx, i)
		}
	}
	c.mu.Unlock()

	if len(missing) == 0 {
		return results, nil
	}

	dbresults, err := c.DB.GetMany(missing...)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	for i, id := range missing {
		val := dbresults[i]
		if val != nil {
			c.add(id, val)
		}
		results[missIdx[i]] = val
	}
	c.mu.Unlock()

	return results, nil
}

func (c *LRUCache[K, V]) Set(id K, v *V, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Set(id, v, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) SetMany(ids []K, values []*V, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.SetMany(ids, values, fns...)
	if err == nil {
		c.mu.Lock()
		for _, id := range ids {
			c.invalidateKey(id)
		}
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) Patch(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Patch(id, patch, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) PatchStrict(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.PatchStrict(id, patch, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) PatchMany(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.PatchMany(ids, patch, fns...)
	if err == nil {
		c.mu.Lock()
		for _, id := range ids {
			c.invalidateKey(id)
		}
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) PatchManyStrict(ids []K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.PatchManyStrict(ids, patch, fns...)
	if err == nil {
		c.mu.Lock()
		for _, id := range ids {
			c.invalidateKey(id)
		}
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) Delete(id K, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Delete(id, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) DeleteMany(ids []K, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.DeleteMany(ids, fns...)
	if err == nil {
		c.mu.Lock()
		for _, id := range ids {
			c.invalidateKey(id)
		}
		c.mu.Unlock()
	}
	return err
}

func (c *LRUCache[K, V]) QueryItems(q *qx.QX) ([]*V, error) {

	if q.Key == "" {
		keys, err := c.DB.QueryKeys(q)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, nil
		}
		return c.GetMany(keys...)
	}

	c.mu.Lock()

	if keys, ok := c.queryToKeys[q.Key]; ok {

		results := make([]*V, len(keys))
		var missing []K
		var missIdx []int

		for i, id := range keys {
			if e, ok := c.cache[id]; ok {
				c.moveToFront(e)
				results[i] = e.Value.(*entry[K, V]).value
			} else {
				missing = append(missing, id)
				missIdx = append(missIdx, i)
			}
		}
		c.mu.Unlock()

		if len(missing) == 0 {
			return results, nil
		}

		dbresults, err := c.DB.GetMany(missing...)
		if err != nil {
			return nil, err
		}

		c.mu.Lock()
		for i, id := range missing {
			val := dbresults[i]
			if val != nil {
				c.add(id, val)
			}
			results[missIdx[i]] = val
		}
		c.mu.Unlock()

		return results, nil

	} else {

		c.mu.Unlock()

		var err error

		keys, err = c.DB.QueryKeys(q)
		if err != nil {
			return nil, err
		}

		var items []*V
		if len(keys) > 0 {
			items, err = c.GetMany(keys...)
			if err != nil {
				return nil, err
			}
		}

		c.mu.Lock()
		c.queryToKeys[q.Key] = append(make([]K, 0, len(keys)), keys...)
		for _, id := range keys {
			m, ok := c.keyToQueries[id]
			if !ok {
				m = make(map[string]struct{})
				c.keyToQueries[id] = m
			}
			m[q.Key] = struct{}{}
		}
		c.mu.Unlock()

		return items, nil
	}
}

func (c *LRUCache[K, V]) QueryKeys(q *qx.QX) ([]K, error) {

	if q.Key == "" {
		return c.DB.QueryKeys(q)
	}

	c.mu.Lock()
	if keys, ok := c.queryToKeys[q.Key]; ok {
		out := append(make([]K, 0, len(keys)), keys...)
		c.mu.Unlock()
		return out, nil
	}
	c.mu.Unlock()

	keys, err := c.DB.QueryKeys(q)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.queryToKeys[q.Key] = append(make([]K, 0, len(keys)), keys...)
	for _, id := range keys {
		m, ok := c.keyToQueries[id]
		if !ok {
			m = make(map[string]struct{})
			c.keyToQueries[id] = m
		}
		m[q.Key] = struct{}{}
	}
	c.mu.Unlock()

	return keys, nil
}

func (c *LRUCache[K, V]) invalidateKey(id K) {
	c.delete(id)
	if qs, ok := c.keyToQueries[id]; ok {
		for qk := range qs {
			delete(c.queryToKeys, qk)
		}
		delete(c.keyToQueries, id)
	}
}
