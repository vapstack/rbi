package rbi

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"go.etcd.io/bbolt"
)

var (
	benchTxUserSink   *UserBench
	benchTxUintSink   uint64
	benchTxResultSink Result
)

func txBenchOptions() Options {
	opts := benchOptions()
	opts.AnalyzeInterval = -1
	return opts
}

func openTxBenchCollection(b *testing.B, n int, opts Options) (*Collection[uint64, UserBench], *bbolt.DB) {
	b.Helper()
	c, bolt := openBoltAndCollection[uint64, UserBench](b, filepath.Join(b.TempDir(), "tx_bench.db"), opts)
	b.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	seedTxBenchDB(b, c, n)
	return c, bolt
}

func seedTxBenchDB(b *testing.B, c *Collection[uint64, UserBench], n int) {
	b.Helper()
	if n == 0 {
		return
	}
	c.disableSync()
	defer c.enableSync()

	ids := make([]uint64, n)
	vals := make([]*UserBench, n)
	for i := 0; i < n; i++ {
		ids[i] = uint64(i + 1)
		vals[i] = &UserBench{
			Country: "US",
			Plan:    "pro",
			Status:  "active",
			Age:     20 + i%40,
			Score:   float64(i),
			Name:    "user-" + strconv.Itoa(i+1),
			Email:   "user-" + strconv.Itoa(i+1) + "@example.test",
			Tags:    []string{"go", "db"},
			Roles:   []string{"user"},
		}
	}
	if err := writeSets(c, ids, vals); err != nil {
		b.Fatalf("seed tx bench: %v", err)
	}
}

func runFixedParallel(b *testing.B, goroutines int, fn func()) {
	b.Helper()
	var next atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	b.ResetTimer()
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for {
				if next.Add(1) > uint64(b.N) {
					return
				}
				fn()
			}
		}()
	}
	wg.Wait()
}

func Benchmark_Tx_BeginView_NoOp(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		tx := BeginView()
		tx.Release()
	}
}

func Benchmark_Tx_BeginRelease_Parallel(b *testing.B) {
	for _, goroutines := range [...]int{1, 16, 64, 256} {
		goroutines := goroutines
		b.Run("View/"+strconv.Itoa(goroutines), func(b *testing.B) {
			b.ReportAllocs()
			runFixedParallel(b, goroutines, func() {
				tx := BeginView()
				tx.Release()
			})
		})
		b.Run("IndexView/"+strconv.Itoa(goroutines), func(b *testing.B) {
			b.ReportAllocs()
			runFixedParallel(b, goroutines, func() {
				tx := BeginIndexView()
				tx.Release()
			})
		})
	}
}

func Benchmark_Tx_Read_BindGet(b *testing.B) {
	c, _ := openTxBenchCollection(b, 1, txBenchOptions())
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tx := BeginView()
		rec, err := c.Get(tx, 1)
		if err != nil {
			b.Fatal(err)
		}
		tx.Release()
		benchTxUserSink = rec
		c.ReleaseRecords(rec)
	}
}

func Benchmark_Tx_Read_BoundGet(b *testing.B) {
	c, _ := openTxBenchCollection(b, 1, txBenchOptions())
	tx := BeginView()
	if rec, err := c.Get(tx, 1); err != nil {
		b.Fatal(err)
	} else {
		c.ReleaseRecords(rec)
	}
	b.Cleanup(func() { tx.Release() })

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		rec, err := c.Get(tx, 1)
		if err != nil {
			b.Fatal(err)
		}
		benchTxUserSink = rec
		c.ReleaseRecords(rec)
	}
}

func Benchmark_Tx_Read_BoundCrossCollectionGet(b *testing.B) {
	opts := txBenchOptions()
	raw, err := bbolt.Open(filepath.Join(b.TempDir(), "tx_cross_read.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	db1, err := Open[uint64, UserBench](raw, opts)
	if err != nil {
		_ = raw.Close()
		b.Fatalf("New db1: %v", err)
	}
	opts.BucketName = "UserBenchAudit"
	db2, err := Open[uint64, UserBench](raw, opts)
	if err != nil {
		_ = db1.Close()
		_ = raw.Close()
		b.Fatalf("New db2: %v", err)
	}
	b.Cleanup(func() {
		_ = db1.Close()
		_ = db2.Close()
		_ = raw.Close()
	})
	seedTxBenchDB(b, db1, 1)
	seedTxBenchDB(b, db2, 1)

	tx := BeginView()
	if rec, err := db1.Get(tx, 1); err != nil {
		b.Fatal(err)
	} else {
		db1.ReleaseRecords(rec)
	}
	b.Cleanup(func() { tx.Release() })

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		rec, err := db2.Get(tx, 1)
		if err != nil {
			b.Fatal(err)
		}
		benchTxUserSink = rec
		db2.ReleaseRecords(rec)
	}
}

func Benchmark_Tx_Read_ViewGet(b *testing.B) {
	c, _ := openTxBenchCollection(b, 1, txBenchOptions())
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := View(func(tx *Tx) error {
			rec, err := c.Get(tx, 1)
			if err != nil {
				return err
			}
			benchTxUserSink = rec
			c.ReleaseRecords(rec)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Tx_Index_BoundCount(b *testing.B) {
	c, _ := openTxBenchCollection(b, 128, txBenchOptions())
	tx := BeginIndexView()
	if _, err := c.Count(tx, qx.EQ("country", "US")); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { tx.Release() })

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		n, err := c.Count(tx, qx.EQ("country", "US"))
		if err != nil {
			b.Fatal(err)
		}
		benchTxUintSink = n
	}
}

func Benchmark_Tx_Index_BoundAggregate(b *testing.B) {
	c, _ := openTxBenchCollection(b, 128, txBenchOptions())
	q := qx.Aggregate(qx.ROWCOUNT().AS("rows"))
	tx := BeginIndexView()
	if _, err := c.Aggregate(tx, q); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { tx.Release() })

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		result, err := c.Aggregate(tx, q)
		if err != nil {
			b.Fatal(err)
		}
		benchTxResultSink = result
	}
}

func Benchmark_Tx_Write_Set_NoHook(b *testing.B) {
	opts := txBenchOptions()
	opts.BatchSoftLimit = 1
	c, _ := openTxBenchCollection(b, 0, opts)
	rec := &UserBench{Country: "US", Plan: "pro", Status: "active", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		if err := writeSet(c, uint64(i+1), rec); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Tx_Write_MultiSet_MultiBlock(b *testing.B) {
	opts := txBenchOptions()
	opts.BatchSoftLimit = 1
	c, _ := openTxBenchCollection(b, 0, opts)

	const batchSize = 1024
	ids := make([]uint64, batchSize)
	vals := make([]*UserBench, batchSize)
	for i := range vals {
		vals[i] = &UserBench{Country: "US", Plan: "pro", Status: "active", Age: 20 + i%40}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		base := uint64(i * batchSize)
		for j := range ids {
			ids[j] = base + uint64(j) + 1
		}
		if err := writeSets(c, ids, vals); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Tx_Write_OnChangeGeneratedAudit(b *testing.B) {
	opts := txBenchOptions()
	opts.BatchSoftLimit = 1
	raw, err := bbolt.Open(filepath.Join(b.TempDir(), "tx_generated.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	mainDB, err := Open[uint64, UserBench](raw, opts)
	if err != nil {
		_ = raw.Close()
		b.Fatalf("New main: %v", err)
	}
	opts.BucketName = "UserBenchAudit"
	auditDB, err := Open[uint64, UserBench](raw, opts)
	if err != nil {
		_ = mainDB.Close()
		_ = raw.Close()
		b.Fatalf("New audit: %v", err)
	}
	b.Cleanup(func() {
		_ = mainDB.Close()
		_ = auditDB.Close()
		_ = raw.Close()
	})

	rec := &UserBench{Country: "US", Plan: "pro", Status: "active", Age: 30}
	auditRec := &UserBench{Country: "US", Plan: "audit", Status: "active", Age: 30}
	hook := OnChange(func(tx *Tx, id uint64, _, _ *UserBench) error {
		return auditDB.Set(tx, id, auditRec)
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		if err := writeSet(mainDB, uint64(i+1), rec, hook); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Tx_Write_OnChangeMakePatchOutbox(b *testing.B) {
	type outboxRecord struct {
		Key   uint64
		Patch []Field
	}

	opts := txBenchOptions()
	opts.BatchSoftLimit = 1
	raw, err := bbolt.Open(filepath.Join(b.TempDir(), "tx_outbox.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	orders, err := Open[uint64, UserBench](raw, opts)
	if err != nil {
		_ = raw.Close()
		b.Fatalf("New orders: %v", err)
	}
	outboxOpts := txBenchOptions()
	outboxOpts.BatchSoftLimit = 1
	outboxOpts.BucketName = "UserBenchOutbox"
	outboxOpts.Index = map[string]IndexKind{}
	outbox, err := Open[uint64, outboxRecord](raw, outboxOpts)
	if err != nil {
		_ = orders.Close()
		_ = raw.Close()
		b.Fatalf("New outbox: %v", err)
	}
	b.Cleanup(func() {
		_ = orders.Close()
		_ = outbox.Close()
		_ = raw.Close()
	})

	const orderRing = 4096
	seedTxBenchDB(b, orders, orderRing)

	recA, recB := writeBenchUpdateRecords()
	var outboxSeq uint64
	hook := OnChange(func(tx *Tx, key uint64, oldValue, newValue *UserBench) error {
		patch, err := orders.MakePatch(oldValue, newValue)
		if err != nil {
			return err
		}
		outboxSeq++
		return outbox.Set(tx, outboxSeq, &outboxRecord{
			Key:   key,
			Patch: patch,
		})
	})

	id := uint64(1)
	rec := recB
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tx := BeginUpdate()
		if err := orders.Set(tx, id, rec, hook); err != nil {
			tx.Release()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			tx.Release()
			b.Fatal(err)
		}
		tx.Release()

		id++
		if id > orderRing {
			id = 1
			if rec == recB {
				rec = recA
			} else {
				rec = recB
			}
		}
	}

	writeBenchHookSink = outboxSeq
	b.ReportMetric(1, "outbox_rows/op")
}

func Benchmark_Tx_Write_ParallelOneCollection(b *testing.B) {
	c, _ := openTxBenchCollection(b, 0, txBenchOptions())
	rec := &UserBench{Country: "US", Plan: "pro", Status: "active", Age: 30}
	var id atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := writeSet(c, id.Add(1), rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Tx_Write_ParallelManyCollections(b *testing.B) {
	opts := txBenchOptions()
	raw, err := bbolt.Open(filepath.Join(b.TempDir(), "tx_many_writers.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	const collectionCount = 16
	collections := make([]*Collection[uint64, UserBench], collectionCount)
	for i := range collections {
		opts.BucketName = "UserBenchP" + strconv.Itoa(i)
		c, err := Open[uint64, UserBench](raw, opts)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = collections[j].Close()
			}
			_ = raw.Close()
			b.Fatalf("New collection %d: %v", i, err)
		}
		collections[i] = c
	}
	b.Cleanup(func() {
		for i := range collections {
			_ = collections[i].Close()
		}
		_ = raw.Close()
	})

	rec := &UserBench{Country: "US", Plan: "pro", Status: "active", Age: 30}
	var id atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			next := id.Add(1)
			c := collections[next%collectionCount]
			if err := writeSet(c, next, rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Tx_Truncate(b *testing.B) {
	opts := txBenchOptions()
	opts.BatchSoftLimit = 1
	c, _ := openTxBenchCollection(b, 128, opts)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := c.Truncate(); err != nil {
			b.Fatal(err)
		}
	}
}

func buildRootRegistryDirtyScanBench(b *testing.B, entryCount int, dirtyCount int) (*rootStore, []*collectionReadState) {
	b.Helper()
	if dirtyCount > entryCount {
		dirtyCount = entryCount
	}
	r := &rootStore{}
	r.registry.init(1)
	gen := r.registry.buildFromCurrent(2, entryCount)
	reads := make([]*collectionReadState, 0, dirtyCount)
	for i := 0; i < entryCount; i++ {
		c := &collection{root: r, ordinal: uint32(i)}
		if i < dirtyCount {
			read := newCollectionReadState(c, 1, newRootRegistryDirtySnapshot(b, &r.registry))
			reads = append(reads, read)
			gen.setEntry(uint32(i), c, read)
		} else {
			gen.setEntry(uint32(i), c, newCollectionReadState(c, 1, nil))
		}
	}
	r.registry.stage(2, gen)
	r.registry.publish(2)
	return r, reads
}

func Benchmark_RootRegistry_UnpinCurrentDirtyCaches(b *testing.B) {
	for _, tc := range [...]struct {
		name    string
		entries int
		dirty   int
	}{
		{name: "Entries16Dirty1", entries: 16, dirty: 1},
		{name: "Entries256Dirty16", entries: 256, dirty: 16},
		{name: "Entries1024Dirty64", entries: 1024, dirty: 64},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			r, reads := buildRootRegistryDirtyScanBench(b, tc.entries, tc.dirty)
			b.Cleanup(func() { _ = r.registry.reap() })

			keys := [2][]qcache.MaterializedPredKey{
				make([]qcache.MaterializedPredKey, len(reads)),
				make([]qcache.MaterializedPredKey, len(reads)),
			}
			for i := range reads {
				keys[0][i] = qcache.MaterializedPredKeyFromOpaque("dirty-a-" + strconv.Itoa(i))
				keys[1][i] = qcache.MaterializedPredKeyFromOpaque("dirty-b-" + strconv.Itoa(i))
			}

			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; b.Loop(); n++ {
				active := keys[n&1]
				for i := range reads {
					reads[i].snap.StoreMaterializedPredKey(active[i], (posting.List{}).BuildAdded(uint64(n+i+1)))
				}
				_, epoch, pin := r.registry.pinCurrent()
				r.registry.unpin(epoch, pin)
			}
		})
	}
}

func Benchmark_RootGeneration_PublishForcedDrainHighWater(b *testing.B) {
	for _, entries := range [...]int{1, 16, 64, 256, 1024, 10000} {
		entries := entries
		b.Run(strconv.Itoa(entries), func(b *testing.B) {
			var rr rootRegistry
			rr.init(1)
			collections := make([]*collection, 8)
			for i := range collections {
				collections[i] = &collection{ordinal: uint32(i)}
			}
			b.Cleanup(func() { _ = rr.reap() })

			epoch := uint64(1)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				epoch++
				gen := rr.buildFromCurrent(epoch, entries)
				touched := len(collections)
				if entries < touched {
					touched = entries
				}
				for i := 0; i < touched; i++ {
					gen.entries[i].collection = collections[i]
				}
				rr.stage(epoch, gen)
				rr.publish(epoch)
			}
		})
	}
}

var benchBoltReadTxSink uint64

func openBenchBoltEpochBucket(b *testing.B) (*bbolt.DB, []byte, []byte) {
	b.Helper()

	raw, err := bbolt.Open(filepath.Join(b.TempDir(), "read_tx.db"), 0o600, nil)
	if err != nil {
		b.Fatalf("bbolt.Open: %v", err)
	}
	b.Cleanup(func() { _ = raw.Close() })

	bucketName := []byte("root_epoch")
	key := []byte("epoch")
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], 729)

	if err = raw.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		if err = bucket.SetSequence(729); err != nil {
			return err
		}
		return bucket.Put(key, value[:])
	}); err != nil {
		b.Fatalf("seed epoch bucket: %v", err)
	}

	return raw, bucketName, key
}

func Benchmark_Bolt_ReadTx_BeginRollback(b *testing.B) {
	raw, _, _ := openBenchBoltEpochBucket(b)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tx, err := raw.Begin(false)
		if err != nil {
			b.Fatalf("Begin(false): %v", err)
		}
		_ = tx.Rollback()
	}
}

func Benchmark_Bolt_ReadTx_BucketSequence(b *testing.B) {
	raw, bucketName, _ := openBenchBoltEpochBucket(b)

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tx, err := raw.Begin(false)
		if err != nil {
			b.Fatalf("Begin(false): %v", err)
		}
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			_ = tx.Rollback()
			b.Fatal("epoch bucket missing")
		}
		seq += bucket.Sequence()
		_ = tx.Rollback()
	}

	benchBoltReadTxSink = seq
}

func Benchmark_Bolt_ReadTx_BucketGet(b *testing.B) {
	raw, bucketName, key := openBenchBoltEpochBucket(b)

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		tx, err := raw.Begin(false)
		if err != nil {
			b.Fatalf("Begin(false): %v", err)
		}
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			_ = tx.Rollback()
			b.Fatal("epoch bucket missing")
		}
		value := bucket.Get(key)
		if len(value) != 8 {
			_ = tx.Rollback()
			b.Fatalf("epoch value len=%d", len(value))
		}
		seq += binary.LittleEndian.Uint64(value)
		_ = tx.Rollback()
	}

	benchBoltReadTxSink = seq
}

func Benchmark_Bolt_OpenReadTx_BucketSequence(b *testing.B) {
	raw, bucketName, _ := openBenchBoltEpochBucket(b)

	tx, err := raw.Begin(false)
	if err != nil {
		b.Fatalf("Begin(false): %v", err)
	}
	b.Cleanup(func() { _ = tx.Rollback() })

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		b.Fatal("epoch bucket missing")
	}

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		seq += bucket.Sequence()
	}

	benchBoltReadTxSink = seq
}

func Benchmark_Bolt_OpenReadTx_BucketGet(b *testing.B) {
	raw, bucketName, key := openBenchBoltEpochBucket(b)

	tx, err := raw.Begin(false)
	if err != nil {
		b.Fatalf("Begin(false): %v", err)
	}
	b.Cleanup(func() { _ = tx.Rollback() })

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		b.Fatal("epoch bucket missing")
	}

	var seq uint64
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		value := bucket.Get(key)
		if len(value) != 8 {
			b.Fatalf("epoch value len=%d", len(value))
		}
		seq += binary.LittleEndian.Uint64(value)
	}

	benchBoltReadTxSink = seq
}
