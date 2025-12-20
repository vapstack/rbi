package rbi

import (
	"container/list"
	"strings"
	"sync"

	"github.com/vapstack/qx"
)

// LRUCache wraps a DB instance and adds two layers of caching:
//   - An in-memory LRU cache for individual items (Get/GetMany).
//   - An invalidation-based cache for QueryKeys results.
//
// Technical limitations:
//  1. Phantom Reads: The query cache maps a query signature (q.Key) to a static list of keys.
//     It relies on "reactive invalidation": the cache entry is cleared only if
//     one of the keys already inside that entry is modified or deleted.
//     If a new record is inserted that matches the query criteria, the cached result
//     will not be updated/invalidated. Use this only for static datasets or ID-list lookups.
//  2. Memory Usage: While the item cache is bounded by 'capacity', the query cache
//     is unbounded. Heavy usage of unique, non-repeating queries (random q.Key) may lead to memory growth.
type LRUCache[K ~string | ~uint64, V any] struct {
	*DB[K, V]
	mu           sync.Mutex
	list         *list.List
	cache        map[K]*list.Element
	capacity     int
	queryToKeys  map[string][]K
	keyToQueries map[K]map[string]struct{}
}

// NewLRUCache creates a new cached wrapper around db.
// capacity determines the maximum number of items stored in the LRU cache.
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

func (c *LRUCache[K, V]) removeOldest() {
	e := c.list.Back()
	if e == nil {
		return
	}
	c.list.Remove(e)
	ent := e.Value.(*entry[K, V])
	delete(c.cache, ent.key)
	// do not remove from keyToQueries here, even if the item is evicted
	// we still want to track it for query invalidation in case a write happens later
}

func (c *LRUCache[K, V]) delete(key K) {
	if e, ok := c.cache[key]; ok {
		c.list.Remove(e)
		delete(c.cache, key)
	}
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

// InvalidateQuery removes a cached query result identified by its query key.
//
// This only invalidates the cached list of keys for the given query.
// It does not affect cached individual records and does not touch the underlying DB.
func (c *LRUCache[K, V]) InvalidateQuery(key string) {
	c.mu.Lock()
	delete(c.queryToKeys, key)
	c.mu.Unlock()
}

// InvalidateQueryPrefix removes all cached query results whose keys
// start with the specified prefix.
//
// This is useful for bulk invalidation of logically related queries
// that share a common query key namespace.
//
// Only query result caches are affected; item cache entries remain intact.
func (c *LRUCache[K, V]) InvalidateQueryPrefix(prefix string) {
	c.mu.Lock()
	for k := range c.queryToKeys {
		if strings.HasPrefix(k, prefix) {
			delete(c.queryToKeys, k)
		}
	}
	c.mu.Unlock()
}

// Invalidate removes all cache entries associated with the given record ID
// and all cached query results that included this ID.
// It does not modify the underlying database.
func (c *LRUCache[K, V]) Invalidate(id K) {
	c.mu.Lock()
	c.invalidateKey(id)
	c.mu.Unlock()
}

// Get retrieves an item from the cache or falls back to the underlying DB.
// On a cache miss, the item is fetched from DB and added to the LRU cache.
func (c *LRUCache[K, V]) Get(id K) (*V, error) {
	c.mu.Lock()
	if e, ok := c.cache[id]; ok {
		c.list.MoveToFront(e)
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

// GetMany retrieves multiple items, optimizing for cache hits.
// It only queries the underlying DB for keys missing from the cache.
func (c *LRUCache[K, V]) GetMany(ids ...K) ([]*V, error) {
	results := make([]*V, len(ids))
	var missing []K
	var missIdx []int

	c.mu.Lock()
	for i, id := range ids {
		if e, ok := c.cache[id]; ok {
			c.list.MoveToFront(e)
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

	dbResults, err := c.DB.GetMany(missing...)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	for i, id := range missing {
		val := dbResults[i]
		if val != nil {
			c.add(id, val)
		}
		results[missIdx[i]] = val
	}
	c.mu.Unlock()

	return results, nil
}

// QueryKeys executes a query and caches the resulting list of keys.
// If q.Key is empty, caching is skipped.
//
// Warning: This cache is susceptible to "Phantom Reads". New records inserted
// into the DB that match the query criteria will not invalidate this cache entry.
// Invalidation only occurs if a key already present in the result set is modified.
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

	cachedKeys := append(make([]K, 0, len(keys)), keys...)
	c.queryToKeys[q.Key] = cachedKeys

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

// QueryItems executes a query and retrieves the actual values.
// It leverages QueryKeys (for caching result IDs) and GetMany (for caching items).
//
// Warning: This operation is not strictly atomic.
// A concurrent Delete/Update can occur between the resolution of keys (from cache or DB)
// and the fetching of items. In such cases, the returned slice may contain nil values
// where records existed during QueryKeys but vanished before GetMany.
func (c *LRUCache[K, V]) QueryItems(q *qx.QX) ([]*V, error) {
	keys, err := c.QueryKeys(q)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return c.GetMany(keys...)
}

// Set writes a value to the DB and invalidates both the item cache for this ID
// and any cached queries that contained this ID.
func (c *LRUCache[K, V]) Set(id K, v *V, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Set(id, v, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

// SetMany writes multiple values and invalidates related caches.
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

// Patch updates a value and invalidates related caches.
func (c *LRUCache[K, V]) Patch(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Patch(id, patch, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

// PatchStrict updates a value with strict type checking and invalidates related caches.
func (c *LRUCache[K, V]) PatchStrict(id K, patch []Field, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.PatchStrict(id, patch, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

// PatchMany updates multiple values and invalidates related caches.
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

// PatchManyStrict updates multiple values with strict checks and invalidates related caches.
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

// Delete removes a value and invalidates related caches.
func (c *LRUCache[K, V]) Delete(id K, fns ...PreCommitFunc[K, V]) error {
	err := c.DB.Delete(id, fns...)
	if err == nil {
		c.mu.Lock()
		c.invalidateKey(id)
		c.mu.Unlock()
	}
	return err
}

// DeleteMany removes multiple values and invalidates related caches.
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

// Reset clears the entire cache state.
// It purges all stored items (LRU) and cached query results, releasing the underlying memory for garbage collection.
// This operation is thread-safe and atomic with respect to other cache operations.
func (c *LRUCache[K, V]) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list = list.New()
	c.cache = make(map[K]*list.Element)
	c.queryToKeys = make(map[string][]K)
	c.keyToQueries = make(map[K]map[string]struct{})
}
