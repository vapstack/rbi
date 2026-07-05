package rbi

import (
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/mathutil"
	"go.etcd.io/bbolt"
)

func (c *Collection[K, V]) disableSync() { c.root.bolt.NoSync = true }

func (c *Collection[K, V]) enableSync() {
	c.root.bolt.NoSync = false
	_ = c.root.bolt.Sync()
}

func newRand(seed int64) *rand.Rand {
	return mathutil.NewRand(seed)
}

var testDiscardLogger = log.New(io.Discard, "", 0)

func testOptions(opts Options) Options {
	if opts.Logger == nil {
		opts.Logger = testDiscardLogger
	}
	return opts
}

func enableStoreStatsForTest(tb testing.TB) {
	tb.Helper()
	old := EnableStoreStats
	EnableStoreStats = true
	tb.Cleanup(func() { EnableStoreStats = old })
}

type Product struct {
	SKU   string   `db:"sku"   rbi:"index"`
	Price float64  `db:"price" rbi:"index"`
	Tags  []string `db:"tags"  rbi:"index"`
}

func openTempCollectionStringProduct(t *testing.T, options ...Options) (*Collection[string, Product], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_product.db")
	c, bolt := openBoltAndCollection[string, Product](t, path, options...)
	t.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c, path
}

func openRawBolt(t *testing.T) (*bbolt.DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.db")
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	return db, path
}
func readBucketSequence(tb testing.TB, raw *bbolt.DB, bucket []byte) uint64 {
	tb.Helper()

	var seq uint64
	if err := raw.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		seq = b.Sequence()
		return nil
	}); err != nil {
		tb.Fatalf("read bucket sequence: %v", err)
	}
	return seq
}

func assertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, c *Collection[K, V]) {
	tb.Helper()

	stats := c.SnapshotStats()
	if stats.Sequence == 0 {
		return
	}
	rr := &c.root.registry
	rr.mu.RLock()
	refs, staged := len(rr.byEpoch), len(rr.staged)
	rr.mu.RUnlock()
	if refs != 1 || staged != 0 {
		tb.Fatalf("snapshot registry contains staged or retired refs: refs=%d staged=%d", refs, staged)
	}
}

func readGet[K ~string | ~uint64, V any](c *Collection[K, V], id K) (*V, error) {
	tx := BeginView()
	defer tx.Release()
	v, err := c.Get(tx, id)
	return v, err
}

func readValues[K ~string | ~uint64, V any](c *Collection[K, V], ids ...K) ([]*V, error) {
	tx := BeginView()
	defer tx.Release()
	vals := make([]*V, len(ids))
	for i, id := range ids {
		v, err := c.Get(tx, id)
		if err != nil {
			return vals, err
		}
		vals[i] = v
	}
	return vals, nil
}

func readScanKeys[K ~string | ~uint64, V any](c *Collection[K, V], seek K, fn func(K) (bool, error)) error {
	tx := BeginView()
	defer tx.Release()
	err := c.ScanKeys(tx, seek, fn)
	return err
}

func readSeqScan[K ~string | ~uint64, V any](c *Collection[K, V], seek K, fn func(K, *V) (bool, error)) error {
	tx := BeginView()
	defer tx.Release()
	err := c.SeqScan(tx, seek, fn)
	return err
}

func readQuery[K ~string | ~uint64, V any](c *Collection[K, V], q *qx.QX) ([]*V, error) {
	tx := BeginView()
	defer tx.Release()
	vals, err := c.Query(tx, q)
	return vals, err
}

func readQueryKeys[K ~string | ~uint64, V any](c *Collection[K, V], q *qx.QX) ([]K, error) {
	tx := BeginView()
	defer tx.Release()
	keys, err := c.QueryKeys(tx, q)
	return keys, err
}

func readIndexQueryKeys[V any](c *Collection[uint64, V], q *qx.QX) ([]uint64, error) {
	tx := BeginIndexView()
	defer tx.Release()
	keys, err := c.QueryKeys(tx, q)
	return keys, err
}

func readCount[K ~string | ~uint64, V any](c *Collection[K, V], exprs ...qx.Expr) (uint64, error) {
	tx := BeginIndexView()
	defer tx.Release()
	count, err := c.Count(tx, exprs...)
	return count, err
}

func readAggregate[K ~string | ~uint64, V any](c *Collection[K, V], q *qx.QX) (Result, error) {
	tx := BeginIndexView()
	defer tx.Release()
	result, err := c.Aggregate(tx, q)
	return result, err
}

func writeSet[K ~string | ~uint64, V any](c *Collection[K, V], id K, val *V, opts ...ExecOption[K, V]) error {
	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Set(tx, id, val, opts...); err != nil {
		return err
	}
	return tx.Commit()
}

func writeSets[K ~string | ~uint64, V any](c *Collection[K, V], ids []K, vals []*V, opts ...ExecOption[K, V]) error {
	if len(ids) != len(vals) {
		return fmt.Errorf("different slice lengths")
	}
	tx := BeginUpdate()
	defer tx.Release()
	for i := range ids {
		if err := c.Set(tx, ids[i], vals[i], opts...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func writePatch[K ~string | ~uint64, V any](c *Collection[K, V], id K, patch []Field, opts ...ExecOption[K, V]) error {
	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Patch(tx, id, patch, opts...); err != nil {
		return err
	}
	return tx.Commit()
}

func writePatches[K ~string | ~uint64, V any](c *Collection[K, V], ids []K, patch []Field, opts ...ExecOption[K, V]) error {
	tx := BeginUpdate()
	defer tx.Release()
	for i := range ids {
		if err := c.Patch(tx, ids[i], patch, opts...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func writeDelete[K ~string | ~uint64, V any](c *Collection[K, V], id K, opts ...ExecOption[K, V]) error {
	tx := BeginUpdate()
	defer tx.Release()
	if err := c.Delete(tx, id, opts...); err != nil {
		return err
	}
	return tx.Commit()
}

func writeDeletes[K ~string | ~uint64, V any](c *Collection[K, V], ids []K, opts ...ExecOption[K, V]) error {
	tx := BeginUpdate()
	defer tx.Release()
	for i := range ids {
		if err := c.Delete(tx, ids[i], opts...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func releaseUniqueRecords[K ~string | ~uint64, V any](c *Collection[K, V], vals ...*V) {
	if c == nil || len(vals) == 0 {
		return
	}
	seen := make(map[*V]struct{}, len(vals))
	unique := make([]*V, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		unique = append(unique, v)
	}
	c.ReleaseRecords(unique...)
}

func mustSetAPIRec(tb testing.TB, c *Collection[uint64, Rec], id uint64, rec *Rec) {
	tb.Helper()
	if err := writeSet(c, id, rec); err != nil {
		tb.Fatalf("Set(%d): %v", id, err)
	}
}

func mustSetAPIRecs(tb testing.TB, c *Collection[uint64, Rec], recs map[uint64]*Rec) {
	tb.Helper()
	for id, rec := range recs {
		mustSetAPIRec(tb, c, id, rec)
	}
}
func ioExtCopyRec(v *Rec) Rec {
	if v == nil {
		return Rec{}
	}
	cp := *v
	cp.Tags = slices.Clone(v.Tags)
	if v.Opt != nil {
		s := *v.Opt
		cp.Opt = &s
	}
	return cp
}

func ioExtCopyProduct(v *Product) Product {
	if v == nil {
		return Product{}
	}
	cp := *v
	cp.Tags = slices.Clone(v.Tags)
	return cp
}

func ioExtMustSetRec(t *testing.T, c *Collection[uint64, Rec], id uint64, v *Rec) {
	t.Helper()
	if err := writeSet(c, id, v); err != nil {
		t.Fatalf("Set(%d): %v", id, err)
	}
}

func ioExtMustSetProduct(t *testing.T, c *Collection[string, Product], id string, v *Product) {
	t.Helper()
	if err := writeSet(c, id, v); err != nil {
		t.Fatalf("Set(%q): %v", id, err)
	}
}

func ioExtMustGetRec(t *testing.T, c *Collection[uint64, Rec], id uint64) *Rec {
	t.Helper()
	v, err := readGet(c, id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	if v == nil {
		t.Fatalf("Get(%d): nil", id)
	}
	return v
}

func ioExtMustReadUint64Raw(t *testing.T, c *Collection[uint64, Rec], id uint64) []byte {
	t.Helper()
	var raw []byte
	if err := c.root.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("missing raw value for id=%d", id)
		}
		raw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("read raw(%d): %v", id, err)
	}
	return raw
}

func ioExtMustReadStringRaw(t *testing.T, c *Collection[string, Product], id string) []byte {
	t.Helper()
	var raw []byte
	if err := c.root.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf))
		if v == nil {
			return fmt.Errorf("missing raw value for id=%q", id)
		}
		raw = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("read raw(%q): %v", id, err)
	}
	return raw
}

func ioExtMustCorruptUint64Raw(t *testing.T, c *Collection[uint64, Rec], id uint64, raw []byte) {
	t.Helper()
	if err := c.root.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(id, c.strKey, &keyBuf), raw)
	}); err != nil {
		t.Fatalf("corrupt raw(%d): %v", id, err)
	}
}

func scanRawBolt[K ~string | ~uint64, V any](tb testing.TB, c *Collection[K, V], seek K, fn func(K, []byte) (bool, error)) error {
	tb.Helper()
	return c.root.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(c.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		cur := b.Cursor()
		var keyBuf [8]byte
		for key, value := cur.Seek(keycodec.UserKeyBytesWithBuf(seek, c.strKey, &keyBuf)); key != nil; key, value = cur.Next() {
			more, err := fn(keycodec.UserKeyFromBytes[K](key, c.strKey), value)
			if err != nil || !more {
				return err
			}
		}
		return nil
	})
}

func rawPayloadForTest[K ~string | ~uint64, V any](c *Collection[K, V], raw []byte) ([]byte, error) {
	if c.strKey {
		if len(raw) < 8 {
			return nil, fmt.Errorf("string storage format: value shorter than %d bytes", 8)
		}
		if keycodec.U64FromBytes(raw[:8]) == 0 {
			return nil, fmt.Errorf("string storage format: zero string id")
		}
		return raw[8:], nil
	}
	return raw, nil
}

func ioExtReadBucketValue(t testing.TB, raw *bbolt.DB, bucketName, key string) ([]byte, bool) {
	t.Helper()
	var out []byte
	var ok bool
	if err := raw.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v == nil {
			return nil
		}
		out = append([]byte(nil), v...)
		ok = true
		return nil
	}); err != nil {
		t.Fatalf("read bucket %q key %q: %v", bucketName, key, err)
	}
	return out, ok
}

func ioExtMustQueryName(t *testing.T, c *Collection[uint64, Rec], name string) []uint64 {
	t.Helper()
	ids, err := readQueryKeys(c, qx.Query(qx.EQ("name", name)))
	if err != nil {
		t.Fatalf("QueryKeys(name=%q): %v", name, err)
	}
	return ids
}

func ioExtMustCountUint64(t *testing.T, c *Collection[uint64, Rec]) uint64 {
	t.Helper()
	cnt, err := readCount(c)
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	return cnt
}

func ioExtCollectSeqScanRec(t *testing.T, c *Collection[uint64, Rec], seek uint64) map[uint64]Rec {
	t.Helper()
	out := make(map[uint64]Rec)
	if err := readSeqScan(c, seek, func(id uint64, v *Rec) (bool, error) {
		out[id] = ioExtCopyRec(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%d): %v", seek, err)
	}
	return out
}

func ioExtCollectSeqScanProduct(t *testing.T, c *Collection[string, Product], seek string) map[string]Product {
	t.Helper()
	out := make(map[string]Product)
	if err := readSeqScan(c, seek, func(id string, v *Product) (bool, error) {
		out[id] = ioExtCopyProduct(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%q): %v", seek, err)
	}
	return out
}
