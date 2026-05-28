package wexec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/pooled"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

type benchmarkWexecRec struct {
	Unique uint64 `db:"unique" rbi:"unique"`
	Group  uint64 `db:"group" rbi:"index"`
	Value  uint64 `db:"value"`
}

type benchmarkExecutorConfig struct {
	indexed  bool
	unique   bool
	strKey   bool
	stats    bool
	maxOps   int
	window   time.Duration
	maxQueue int
}

var (
	benchmarkWexecRecPool = pooled.Pointers[benchmarkWexecRec]{Clear: true}
	benchmarkUniqueErr    = errors.New("unique violation")
	benchmarkStatsSink    Stats
	benchmarkUintSink     uint64
	benchmarkRecordOps    = RecordOps{
		Encode:        benchmarkEncodeRec,
		Decode:        benchmarkDecodeRec,
		Release:       benchmarkReleaseRec,
		ValidateIndex: benchmarkValidateRec,
	}
)

func benchmarkEncodeRec(ptr unsafe.Pointer, buf *bytes.Buffer) error {
	rec := (*benchmarkWexecRec)(ptr)
	var tmp [24]byte
	binary.LittleEndian.PutUint64(tmp[0:8], rec.Unique)
	binary.LittleEndian.PutUint64(tmp[8:16], rec.Group)
	binary.LittleEndian.PutUint64(tmp[16:24], rec.Value)
	_, _ = buf.Write(tmp[:])
	return nil
}

func benchmarkDecodeRec(data []byte) (unsafe.Pointer, error) {
	rec := benchmarkWexecRecPool.Get()
	rec.Unique = binary.LittleEndian.Uint64(data[0:8])
	rec.Group = binary.LittleEndian.Uint64(data[8:16])
	rec.Value = binary.LittleEndian.Uint64(data[16:24])
	return unsafe.Pointer(rec), nil
}

func benchmarkReleaseRec(ptr unsafe.Pointer) {
	benchmarkWexecRecPool.Put((*benchmarkWexecRec)(ptr))
}

func benchmarkValidateRec(unsafe.Pointer) error {
	return nil
}

func benchmarkUnavailable() error {
	return nil
}

func benchmarkCommit(tx *bbolt.Tx, _ string) error {
	return tx.Commit()
}

func benchmarkSnapshotCacheConfig() snapshot.CacheConfig {
	return snapshot.CacheConfig{}
}

func benchmarkBeforeProcess(keycodec.DataKey, unsafe.Pointer) error {
	return nil
}

func benchmarkBeforeStore(keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
	return nil
}

func benchmarkBeforeCommit(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error {
	return nil
}

func benchmarkRuntime(tb testing.TB) *schema.Runtime {
	tb.Helper()

	rt, err := schema.Compile(reflect.TypeFor[benchmarkWexecRec](), schema.Config{})
	if err != nil {
		tb.Fatalf("Compile: %v", err)
	}
	return rt
}

func newBenchmarkExecutor(tb testing.TB, cfg benchmarkExecutorConfig) *Batcher {
	tb.Helper()

	if cfg.unique {
		cfg.indexed = true
	}
	maxOps := cfg.maxOps
	if maxOps == 0 {
		maxOps = 1
	}

	rt := benchmarkRuntime(tb)
	path := filepath.Join(tb.TempDir(), "wexec-bench.db")
	raw, err := bbolt.Open(path, 0o600, &bbolt.Options{
		NoSync:          true,
		NoGrowSync:      true,
		NoFreelistSync:  true,
		InitialMmapSize: 64 << 20,
	})
	if err != nil {
		tb.Fatalf("bbolt.Open: %v", err)
	}
	tb.Cleanup(func() {
		_ = raw.Close()
	})

	bucket := []byte("records")
	if err = raw.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	}); err != nil {
		tb.Fatalf("create bucket: %v", err)
	}

	var rootMu sync.RWMutex
	var sm *strmap.Mapper
	if cfg.strKey {
		sm = strmap.New(0, 64)
	}

	var snapOps SnapshotOps
	var publishOps IndexPublishOps
	var unique UniqueContext
	errs := ErrorSet{}
	if cfg.indexed {
		manager := snapshot.NewManager(false)
		storage := snapshot.Storage{}
		if sm != nil {
			storage.StrMap = sm.Snapshot()
		}
		current := snapshot.NewView(0, nil, rt, snapshot.CacheConfig{}, storage)
		manager.Publish(current)
		if sm != nil {
			sm.MarkCommittedPublished(current.StrMap)
		}

		snapOps = SnapshotOps{
			Enabled:     true,
			Manager:     manager,
			Schema:      rt,
			CacheConfig: benchmarkSnapshotCacheConfig,
			StrMap:      sm,
			PatchFields: rt.Patch.Fields,
		}
		publishOps = IndexPublishOps{
			PublishCommitted: func(_ uint64, _ string, snap *snapshot.View) error {
				manager.Publish(snap)
				if sm != nil {
					sm.MarkCommittedPublished(snap.StrMap)
				}
				return nil
			},
		}
		if cfg.unique {
			errs.UniqueViolation = benchmarkUniqueErr
			unique = UniqueContext{
				Schema:          rt,
				Current:         manager.Current,
				UniqueViolation: benchmarkUniqueErr,
			}
		}
	}

	return NewBatcher(Config{
		MaxOps:             maxOps,
		Window:             cfg.window,
		MaxQueue:           cfg.maxQueue,
		StatsEnabled:       cfg.stats,
		Unavailable:        benchmarkUnavailable,
		Errors:             errs,
		Bolt:               raw,
		Bucket:             bucket,
		BucketFillPercent:  0.8,
		RejectEmptyPayload: true,
		RootMu:             &rootMu,
		Commit:             benchmarkCommit,
		StrKey:             cfg.strKey,
		StrMap:             sm,
		Indexed:            cfg.indexed,
		Ops:                &benchmarkRecordOps,
		Schema:             rt,
		Unique:             unique,
		SnapshotOps:        snapOps,
		IndexPublishOps:    publishOps,
	})
}

func benchmarkSeedSet(tb testing.TB, ex *Batcher, key keycodec.DataKey, rec *benchmarkWexecRec) {
	tb.Helper()

	batch := ex.NewBatch(1)
	if err := batch.AddSet(key, unsafe.Pointer(rec), nil, nil, nil); err != nil {
		tb.Fatalf("seed AddSet: %v", err)
	}
	if err := batch.Submit(true); err != nil {
		tb.Fatalf("seed Submit: %v", err)
	}
}

func benchmarkSubmitSet(b *testing.B, cfg benchmarkExecutorConfig, key keycodec.DataKey, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := uint64(i + 2)
		rec.Unique = v
		rec.Group = v & 15
		rec.Value = v
		batch := ex.NewBatch(1)
		if err := batch.AddSet(key, unsafe.Pointer(&rec), beforeStore, beforeCommit, nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Submit(false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitSetNew(b *testing.B, cfg benchmarkExecutorConfig) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
	var strKeys []keycodec.DataKey
	if cfg.strKey {
		strKeys = make([]keycodec.DataKey, b.N)
		for i := 0; i < b.N; i++ {
			strKeys[i] = keycodec.DataKeyFromUserKey("new-"+strconv.Itoa(i+1), true)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := uint64(i + 1)
		rec.Unique = v
		rec.Group = v & 15
		rec.Value = v
		key := keycodec.DataKeyFromUserKey(v, false)
		if cfg.strKey {
			key = strKeys[i]
		}
		batch := ex.NewBatch(1)
		if err := batch.AddSet(key, unsafe.Pointer(&rec), nil, nil, nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Submit(false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitSetBatch(b *testing.B, cfg benchmarkExecutorConfig, n int, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, n)
	recs := make([]benchmarkWexecRec, n)
	for i := 0; i < n; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		recs[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i*n + n + 1)
		batch := ex.NewBatch(n)
		for j := 0; j < n; j++ {
			rec := &recs[j]
			v := base + uint64(j)
			rec.Unique = v
			rec.Group = v & 15
			rec.Value = v
			if err := batch.AddSet(keys[j], unsafe.Pointer(rec), beforeStore, beforeCommit, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Submit(false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitParallel(b *testing.B, cfg benchmarkExecutorConfig, keyCount int) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, keyCount)
	seeds := make([]benchmarkWexecRec, keyCount)
	batch := ex.NewBatch(keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		seeds[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
		if err := batch.AddSet(keys[i], unsafe.Pointer(&seeds[i]), nil, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Submit(true); err != nil {
		b.Fatal(err)
	}

	beforeStore := []BeforeStoreHook{benchmarkBeforeStore}
	beforeCommit := []BeforeCommitHook{benchmarkBeforeCommit}
	var seq atomic.Uint64
	var errMu sync.Mutex
	var firstErr error

	b.SetParallelism(4)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var rec benchmarkWexecRec
		for pb.Next() {
			n := seq.Add(1)
			rec.Unique = n + uint64(keyCount)
			rec.Group = n & 15
			rec.Value = n
			batch := ex.NewBatch(1)
			if err := batch.AddSet(keys[int(n)%keyCount], unsafe.Pointer(&rec), beforeStore, beforeCommit, nil); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			if err := batch.Submit(false); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
		}
	})
	b.StopTimer()

	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		b.Fatal(err)
	}
	st := ex.Stats()
	b.ReportMetric(float64(st.MultiRequestBatches), "multi_request_batches")
	b.ReportMetric(st.AvgBatchSize, "avg_batch")
}

func benchmarkSubmitPatch(b *testing.B, cfg benchmarkExecutorConfig, key keycodec.DataKey, field string, beforeProcess []BeforeProcessHook, beforeStore []BeforeStoreHook, beforeCommit []BeforeCommitHook) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	seed := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
	benchmarkSeedSet(b, ex, key, &seed)
	patch := []schema.PatchItem{{Name: field, Value: uint64(2)}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		patch[0].Value = uint64(i + 2)
		batch := ex.NewBatch(1)
		batch.AddPatch(key, patch, true, beforeProcess, beforeStore, beforeCommit)
		if err := batch.Submit(false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitPatchBatch(b *testing.B, cfg benchmarkExecutorConfig, field string, n int) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, n)
	seeds := make([]benchmarkWexecRec, n)
	for i := 0; i < n; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		seeds[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
		benchmarkSeedSet(b, ex, keys[i], &seeds[i])
	}
	patch := []schema.PatchItem{{Name: field, Value: uint64(0)}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i*n + n + 1)
		batch := ex.NewBatch(n)
		for j := 0; j < n; j++ {
			patch[0].Value = base + uint64(j)
			batch.AddPatch(keys[j], patch, true, nil, nil, nil)
		}
		if err := batch.Submit(false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitDeleteRestore(b *testing.B, cfg benchmarkExecutorConfig, key keycodec.DataKey, beforeCommit []BeforeCommitHook) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
	benchmarkSeedSet(b, ex, key, &rec)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := uint64(i + 2)
		rec.Unique = v
		rec.Group = v & 15
		rec.Value = v
		batch := ex.NewBatch(2)
		batch.AddDelete(key, beforeCommit)
		if err := batch.AddSet(key, unsafe.Pointer(&rec), nil, nil, nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Submit(true); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkSubmitDeleteRestoreBatch(b *testing.B, cfg benchmarkExecutorConfig, n int) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, n)
	recs := make([]benchmarkWexecRec, n)
	for i := 0; i < n; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		recs[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
		benchmarkSeedSet(b, ex, keys[i], &recs[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i*n + n + 1)
		batch := ex.NewBatch(n * 2)
		for j := 0; j < n; j++ {
			rec := &recs[j]
			v := base + uint64(j)
			rec.Unique = v
			rec.Group = v & 15
			rec.Value = v
			batch.AddDelete(keys[j], nil)
			if err := batch.AddSet(keys[j], unsafe.Pointer(rec), nil, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Submit(true); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkRunSharedSetBatch(b *testing.B, cfg benchmarkExecutorConfig, n int) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, n)
	recs := make([]benchmarkWexecRec, n)
	for i := 0; i < n; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		recs[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i*n + n + 1)
		reqs := requestScratchPool.Get(n)
		for j := 0; j < n; j++ {
			rec := &recs[j]
			v := base + uint64(j)
			rec.Unique = v
			rec.Group = v & 15
			rec.Value = v
			req, err := ex.buildSetRequest(keys[j], unsafe.Pointer(rec), nil, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			reqs = append(reqs, req)
		}
		ex.runShared(reqs)
		for k := 0; k < len(reqs); k++ {
			req := reqs[k]
			if req.Err != nil {
				b.Fatal(req.Err)
			}
			requestPool.Put(req)
		}
		requestScratchPool.Put(reqs)
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func benchmarkRunSharedPatchBatch(b *testing.B, cfg benchmarkExecutorConfig, field string, n int) {
	b.Helper()

	ex := newBenchmarkExecutor(b, cfg)
	keys := make([]keycodec.DataKey, n)
	seeds := make([]benchmarkWexecRec, n)
	for i := 0; i < n; i++ {
		keys[i] = keycodec.DataKeyFromUserKey(uint64(i+1), false)
		seeds[i] = benchmarkWexecRec{Unique: uint64(i + 1), Group: uint64(i & 15), Value: uint64(i)}
		benchmarkSeedSet(b, ex, keys[i], &seeds[i])
	}
	patch := []schema.PatchItem{{Name: field, Value: uint64(0)}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := uint64(i*n + n + 1)
		reqs := requestScratchPool.Get(n)
		for j := 0; j < n; j++ {
			patch[0].Value = base + uint64(j)
			reqs = append(reqs, ex.buildPatchRequest(keys[j], patch, true, nil, nil, nil))
		}
		ex.runShared(reqs)
		for k := 0; k < len(reqs); k++ {
			req := reqs[k]
			if req.Err != nil {
				b.Fatal(req.Err)
			}
			requestPool.Put(req)
		}
		requestScratchPool.Put(reqs)
	}
	b.StopTimer()
	benchmarkStatsSink = ex.Stats()
}

func BenchmarkWriteBuildRequest(b *testing.B) {
	key := keycodec.DataKeyFromUserKey(uint64(1), false)
	strKey := keycodec.DataKeyFromUserKey("bench-key", true)
	beforeStore := []BeforeStoreHook{benchmarkBeforeStore}
	beforeCommit := []BeforeCommitHook{benchmarkBeforeCommit}

	b.Run("Set", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{})
		rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Unique = uint64(i + 1)
			req, err := ex.buildSetRequest(key, unsafe.Pointer(&rec), nil, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			requestPool.Put(req)
		}
	})
	b.Run("SetBeforeStore", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{})
		rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Unique = uint64(i + 1)
			req, err := ex.buildSetRequest(key, unsafe.Pointer(&rec), beforeStore, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			requestPool.Put(req)
		}
	})
	b.Run("SetBeforeCommit", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{})
		rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Unique = uint64(i + 1)
			req, err := ex.buildSetRequest(key, unsafe.Pointer(&rec), nil, beforeCommit, nil)
			if err != nil {
				b.Fatal(err)
			}
			requestPool.Put(req)
		}
	})
	b.Run("Patch", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{indexed: true})
		patch := []schema.PatchItem{{Name: "group", Value: uint64(1)}}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			patch[0].Value = uint64(i + 1)
			req := ex.buildPatchRequest(key, patch, true, nil, nil, nil)
			requestPool.Put(req)
		}
	})
	b.Run("Delete", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := ex.buildDeleteRequest(key, nil)
			requestPool.Put(req)
		}
	})
	b.Run("StringSet", func(b *testing.B) {
		ex := newBenchmarkExecutor(b, benchmarkExecutorConfig{strKey: true})
		rec := benchmarkWexecRec{Unique: 1, Group: 1, Value: 1}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Unique = uint64(i + 1)
			req, err := ex.buildSetRequest(strKey, unsafe.Pointer(&rec), nil, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			requestPool.Put(req)
		}
	})
}

func BenchmarkWriteSet(b *testing.B) {
	key := keycodec.DataKeyFromUserKey(uint64(1), false)
	strKey := keycodec.DataKeyFromUserKey("bench-key", true)

	b.Run("UintNoIndex", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{}, key, nil, nil)
	})
	b.Run("UintNoIndexStats", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{stats: true}, key, nil, nil)
	})
	b.Run("UintIndexed", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{indexed: true}, key, nil, nil)
	})
	b.Run("NewUintIndexed", func(b *testing.B) {
		benchmarkSubmitSetNew(b, benchmarkExecutorConfig{indexed: true})
	})
	b.Run("UintUnique", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{unique: true}, key, nil, nil)
	})
	b.Run("StringIndexed", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{indexed: true, strKey: true}, strKey, nil, nil)
	})
	b.Run("NewStringIndexed", func(b *testing.B) {
		benchmarkSubmitSetNew(b, benchmarkExecutorConfig{indexed: true, strKey: true})
	})
	b.Run("StringUnique", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{unique: true, strKey: true}, strKey, nil, nil)
	})
	b.Run("AtomicBatch8NoIndex", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{}, 8, nil, nil)
	})
	b.Run("AtomicBatch8Indexed", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{indexed: true}, 8, nil, nil)
	})
	b.Run("AtomicBatch8Unique", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{unique: true}, 8, nil, nil)
	})
	b.Run("AtomicBatch1000Indexed", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{indexed: true}, 1000, nil, nil)
	})
	b.Run("AtomicBatch1000Unique", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{unique: true}, 1000, nil, nil)
	})
}

func BenchmarkWritePatch(b *testing.B) {
	key := keycodec.DataKeyFromUserKey(uint64(1), false)
	beforeProcess := []BeforeProcessHook{benchmarkBeforeProcess}
	beforeStore := []BeforeStoreHook{benchmarkBeforeStore}
	beforeCommit := []BeforeCommitHook{benchmarkBeforeCommit}

	b.Run("ValueNoIndex", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{}, key, "value", nil, nil, nil)
	})
	b.Run("GroupIndexedPatchOnly", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{indexed: true}, key, "group", nil, nil, nil)
	})
	b.Run("UniqueIndexed", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{unique: true}, key, "unique", nil, nil, nil)
	})
	b.Run("BeforeProcess", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{}, key, "value", beforeProcess, nil, nil)
	})
	b.Run("BeforeStore", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{indexed: true}, key, "group", nil, beforeStore, nil)
	})
	b.Run("BeforeCommit", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{indexed: true}, key, "group", nil, nil, beforeCommit)
	})
	b.Run("AtomicBatch8Indexed", func(b *testing.B) {
		benchmarkSubmitPatchBatch(b, benchmarkExecutorConfig{indexed: true}, "group", 8)
	})
	b.Run("AtomicBatch8Unique", func(b *testing.B) {
		benchmarkSubmitPatchBatch(b, benchmarkExecutorConfig{unique: true}, "unique", 8)
	})
	b.Run("AtomicBatch1000Indexed", func(b *testing.B) {
		benchmarkSubmitPatchBatch(b, benchmarkExecutorConfig{indexed: true}, "group", 1000)
	})
	b.Run("AtomicBatch1000Unique", func(b *testing.B) {
		benchmarkSubmitPatchBatch(b, benchmarkExecutorConfig{unique: true}, "unique", 1000)
	})
}

func BenchmarkWriteDelete(b *testing.B) {
	key := keycodec.DataKeyFromUserKey(uint64(1), false)
	strKey := keycodec.DataKeyFromUserKey("bench-key", true)
	beforeCommit := []BeforeCommitHook{benchmarkBeforeCommit}

	b.Run("UintNoIndexDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{}, key, nil)
	})
	b.Run("UintIndexedDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{indexed: true}, key, nil)
	})
	b.Run("UintUniqueDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{unique: true}, key, nil)
	})
	b.Run("StringIndexedDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{indexed: true, strKey: true}, strKey, nil)
	})
	b.Run("BeforeCommitDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{indexed: true}, key, beforeCommit)
	})
	b.Run("AtomicBatch8IndexedDeleteRestore", func(b *testing.B) {
		benchmarkSubmitDeleteRestoreBatch(b, benchmarkExecutorConfig{indexed: true}, 8)
	})
}

func BenchmarkWriteHooks(b *testing.B) {
	key := keycodec.DataKeyFromUserKey(uint64(1), false)
	beforeProcess := []BeforeProcessHook{benchmarkBeforeProcess}
	beforeStore := []BeforeStoreHook{benchmarkBeforeStore}
	beforeCommit := []BeforeCommitHook{benchmarkBeforeCommit}

	b.Run("SetBeforeStore", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{}, key, beforeStore, nil)
	})
	b.Run("SetBeforeCommit", func(b *testing.B) {
		benchmarkSubmitSet(b, benchmarkExecutorConfig{}, key, nil, beforeCommit)
	})
	b.Run("PatchBeforeProcess", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{}, key, "value", beforeProcess, nil, nil)
	})
	b.Run("PatchBeforeStore", func(b *testing.B) {
		benchmarkSubmitPatch(b, benchmarkExecutorConfig{indexed: true}, key, "group", nil, beforeStore, nil)
	})
	b.Run("DeleteBeforeCommit", func(b *testing.B) {
		benchmarkSubmitDeleteRestore(b, benchmarkExecutorConfig{}, key, beforeCommit)
	})
	b.Run("Batch1000BeforeStoreBeforeCommit", func(b *testing.B) {
		benchmarkSubmitSetBatch(b, benchmarkExecutorConfig{indexed: true}, 1000, beforeStore, beforeCommit)
	})
}

func BenchmarkWriteSharedBatch(b *testing.B) {
	b.Run("Set8NoIndex", func(b *testing.B) {
		benchmarkRunSharedSetBatch(b, benchmarkExecutorConfig{}, 8)
	})
	b.Run("Set8Indexed", func(b *testing.B) {
		benchmarkRunSharedSetBatch(b, benchmarkExecutorConfig{indexed: true}, 8)
	})
	b.Run("Set8Unique", func(b *testing.B) {
		benchmarkRunSharedSetBatch(b, benchmarkExecutorConfig{unique: true}, 8)
	})
	b.Run("Patch8Indexed", func(b *testing.B) {
		benchmarkRunSharedPatchBatch(b, benchmarkExecutorConfig{indexed: true}, "group", 8)
	})
	b.Run("Patch8Unique", func(b *testing.B) {
		benchmarkRunSharedPatchBatch(b, benchmarkExecutorConfig{unique: true}, "unique", 8)
	})
}

func BenchmarkWriteParallel(b *testing.B) {
	const keyCount = 4096

	b.Run("SingleRequestBatches", func(b *testing.B) {
		benchmarkSubmitParallel(b, benchmarkExecutorConfig{indexed: true, stats: true, maxOps: 1}, keyCount)
	})
	b.Run("AutoBatchMax16", func(b *testing.B) {
		benchmarkSubmitParallel(b, benchmarkExecutorConfig{
			indexed:  true,
			stats:    true,
			maxOps:   16,
			window:   200 * time.Microsecond,
			maxQueue: 1024,
		}, keyCount)
	})
}

func BenchmarkWriteScheduler(b *testing.B) {
	b.Run("PopUniqueUintSetDelete64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, false, benchmarkSchedulerUnique, opSet, reqSetDeleteCoalescible)
	})
	b.Run("PopRepeatedUintSetDelete64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, false, benchmarkSchedulerRepeated, opSet, reqSetDeleteCoalescible)
	})
	b.Run("PopRepeatedUintPatchSafe64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, false, benchmarkSchedulerRepeated, opPatch, reqRepeatIDSafeShared)
	})
	b.Run("PopRepeatedUintPatchUnsafe64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, false, benchmarkSchedulerRepeated, opPatch, 0)
	})
	b.Run("PopUniqueStringSetDelete64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, true, benchmarkSchedulerUnique, opSet, reqSetDeleteCoalescible)
	})
	b.Run("PopRepeatedStringSetDelete64", func(b *testing.B) {
		benchmarkSchedulerPop(b, 64, true, benchmarkSchedulerRepeated, opSet, reqSetDeleteCoalescible)
	})
}

type benchmarkSchedulerMode uint8

const (
	benchmarkSchedulerUnique benchmarkSchedulerMode = iota
	benchmarkSchedulerRepeated
)

func benchmarkSchedulerPop(b *testing.B, n int, strKey bool, mode benchmarkSchedulerMode, op Op, policy reqPolicy) {
	b.Helper()

	s := newScheduler(n, time.Microsecond, 0, false)
	reqs := make([]request, n)
	reqLists := make([][]*request, n)
	jobs := make([]writeJob, n)
	jobPtrs := make([]*writeJob, n)
	for i := 0; i < n; i++ {
		id := i + 1
		if mode == benchmarkSchedulerRepeated {
			id = i&7 + 1
		}
		req := &reqs[i]
		req.op = op
		req.policy = policy
		if strKey {
			req.id = keycodec.DataKeyFromUserKey("key-"+strconv.Itoa(id), true)
		} else {
			req.id = keycodec.DataKeyFromUserKey(uint64(id), false)
		}
		reqLists[i] = []*request{req}
		jobs[i].reqs = reqLists[i]
		jobPtrs[i] = &jobs[i]
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			reqs[j].replacedBy = nil
			s.queue[j] = jobPtrs[j]
		}
		s.queueHead = 0
		s.queueLen = n
		batch := s.popBatch(strKey)
		benchmarkUintSink += uint64(len(batch))
		clear(batch)
		s.mu.Lock()
		s.recycleBatchScratchLocked(batch)
		s.mu.Unlock()
	}
}
