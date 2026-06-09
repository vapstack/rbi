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

type Product struct {
	SKU   string   `db:"sku"   rbi:"index"`
	Price float64  `db:"price" rbi:"index"`
	Tags  []string `db:"tags"  rbi:"index"`
}

func openTempDBStringProduct(t *testing.T, options ...Options) (*DB[string, Product], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_product.db")
	db, raw := openBoltAndNew[string, Product](t, path, options...)
	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})
	return db, path
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

func assertNoFutureSnapshotRefs[K ~uint64 | ~string, V any](tb testing.TB, db *DB[K, V]) {
	tb.Helper()

	stats := db.SnapshotStats()
	if stats.Sequence == 0 {
		return
	}
	if stats.RegistrySize != 1 {
		tb.Fatalf("snapshot registry contains staged or retired refs: %+v", stats)
	}
}
func releaseUniqueRecords[K ~string | ~uint64, V any](db *DB[K, V], vals ...*V) {
	if db == nil || len(vals) == 0 {
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
	db.ReleaseRecords(unique...)
}

func mustSetAPIRec(tb testing.TB, db *DB[uint64, Rec], id uint64, rec *Rec) {
	tb.Helper()
	if err := db.Set(id, rec); err != nil {
		tb.Fatalf("Set(%d): %v", id, err)
	}
}

func mustSetAPIRecs(tb testing.TB, db *DB[uint64, Rec], recs map[uint64]*Rec) {
	tb.Helper()
	for id, rec := range recs {
		mustSetAPIRec(tb, db, id, rec)
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

func ioExtMustSetRec(t *testing.T, db *DB[uint64, Rec], id uint64, v *Rec) {
	t.Helper()
	if err := db.Set(id, v); err != nil {
		t.Fatalf("Set(%d): %v", id, err)
	}
}

func ioExtMustSetProduct(t *testing.T, db *DB[string, Product], id string, v *Product) {
	t.Helper()
	if err := db.Set(id, v); err != nil {
		t.Fatalf("Set(%q): %v", id, err)
	}
}

func ioExtMustGetRec(t *testing.T, db *DB[uint64, Rec], id uint64) *Rec {
	t.Helper()
	v, err := db.Get(id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	if v == nil {
		t.Fatalf("Get(%d): nil", id)
	}
	return v
}

func ioExtMustReadUint64Raw(t *testing.T, db *DB[uint64, Rec], id uint64) []byte {
	t.Helper()
	var raw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
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

func ioExtMustReadStringRaw(t *testing.T, db *DB[string, Product], id string) []byte {
	t.Helper()
	var raw []byte
	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		v := b.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
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

func ioExtMustCorruptUint64Raw(t *testing.T, db *DB[uint64, Rec], id uint64, raw []byte) {
	t.Helper()
	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.dataBucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf), raw)
	}); err != nil {
		t.Fatalf("corrupt raw(%d): %v", id, err)
	}
}

func scanRawBolt[K ~string | ~uint64, V any](tb testing.TB, db *DB[K, V], seek K, fn func(K, []byte) (bool, error)) error {
	tb.Helper()
	return db.Bolt().View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.BucketName())
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		c := b.Cursor()
		var keyBuf [8]byte
		for key, value := c.Seek(keycodec.UserKeyBytesWithBuf(seek, db.strKey, &keyBuf)); key != nil; key, value = c.Next() {
			more, err := fn(keycodec.UserKeyFromBytes[K](key, db.strKey), value)
			if err != nil || !more {
				return err
			}
		}
		return nil
	})
}

func rawPayloadForTest[K ~string | ~uint64, V any](db *DB[K, V], raw []byte) ([]byte, error) {
	if db.strKey {
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

func ioExtMustQueryName(t *testing.T, db *DB[uint64, Rec], name string) []uint64 {
	t.Helper()
	ids, err := db.QueryKeys(qx.Query(qx.EQ("name", name)))
	if err != nil {
		t.Fatalf("QueryKeys(name=%q): %v", name, err)
	}
	return ids
}

func ioExtMustCountUint64(t *testing.T, db *DB[uint64, Rec]) uint64 {
	t.Helper()
	cnt, err := db.Count()
	if err != nil {
		t.Fatalf("Count(): %v", err)
	}
	return cnt
}

func ioExtCollectSeqScanRec(t *testing.T, db *DB[uint64, Rec], seek uint64) map[uint64]Rec {
	t.Helper()
	out := make(map[uint64]Rec)
	if err := db.SeqScan(seek, func(id uint64, v *Rec) (bool, error) {
		out[id] = ioExtCopyRec(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%d): %v", seek, err)
	}
	return out
}

func ioExtCollectSeqScanProduct(t *testing.T, db *DB[string, Product], seek string) map[string]Product {
	t.Helper()
	out := make(map[string]Product)
	if err := db.SeqScan(seek, func(id string, v *Product) (bool, error) {
		out[id] = ioExtCopyProduct(v)
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(%q): %v", seek, err)
	}
	return out
}
