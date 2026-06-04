package snapshot

import (
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qcache"
	"github.com/vapstack/rbi/internal/qir"
	"github.com/vapstack/rbi/internal/schema"
)

type benchRec struct {
	Name  string   `rbi:"index"`
	Age   int      `rbi:"index"`
	Tags  []string `rbi:"index"`
	Score float64  `rbi:"measure"`
}

func benchRuntime(b testing.TB) *schema.Schema {
	b.Helper()
	rt, err := schema.Compile(reflect.TypeOf(benchRec{}), schema.Config{})
	if err != nil {
		b.Fatalf("schema.Compile: %v", err)
	}
	return rt
}

func benchRecords(n int, seed int) []benchRec {
	out := make([]benchRec, n)
	for i := range out {
		out[i] = benchRec{
			Name:  fmt.Sprintf("u_%d_%d", seed, i),
			Age:   seed + i,
			Tags:  []string{"go", fmt.Sprintf("t_%d", i&15)},
			Score: float64(seed + i),
		}
	}
	return out
}

func benchEntries(records []benchRec, idBase uint64) []BatchEntry {
	entries := make([]BatchEntry, len(records))
	for i := range records {
		entries[i] = BatchEntry{
			ID:  idBase + uint64(i),
			New: unsafe.Pointer(&records[i]),
		}
	}
	return entries
}

func benchFixedStorage(n int) indexdata.FieldStorage {
	m := indexdata.GetFixedPostingMap()
	for i := 0; i < n; i++ {
		m[uint64(i*3+1)] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(m)
}

func BenchmarkBuildPreparedEmptyBase(b *testing.B) {
	rt := benchRuntime(b)
	records := benchRecords(256, 0)
	entries := benchEntries(records, 1)
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+1), nil, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkNewViewRetainSharedStorage(b *testing.B) {
	rt := benchRuntime(b)
	records := benchRecords(256, 0)
	base := BuildPrepared(1, nil, rt, CacheConfig{MatPredMaxEntries: 64}, nil, rt.Patch.Fields, benchEntries(records, 1))
	defer base.releaseRuntimeCaches()
	defer base.releaseStorage()

	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := NewView(uint64(i+2), base, rt, cfg, Storage{
			Index:             indexdata.CloneFieldStorageSlots(base.Index, len(rt.Indexed)),
			NilIndex:          indexdata.CloneFieldStorageSlots(base.NilIndex, len(rt.Indexed)),
			LenIndex:          indexdata.CloneFieldStorageSlots(base.LenIndex, len(rt.Indexed)),
			LenZeroComplement: cloneFieldIndexBoolSlots(base.LenZeroComplement, len(rt.Indexed)),
			Measure:           indexdata.CloneMeasureStorageSlots(base.Measure, len(rt.Measures)),
			Universe:          base.Universe,
		})
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedInsertOnly(b *testing.B) {
	rt := benchRuntime(b)
	baseRecords := benchRecords(256, 0)
	base := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(baseRecords, 1))
	defer base.releaseRuntimeCaches()
	defer base.releaseStorage()

	records := benchRecords(64, 10_000)
	entries := benchEntries(records, 1_000_000)
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), base, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedUpdate(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRecords := benchRecords(256, 1_000)
	entries := make([]BatchEntry, len(newRecords))
	for i := range newRecords {
		entries[i] = BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
			New: unsafe.Pointer(&newRecords[i]),
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedUpdateWithWarmCaches(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("Name", qir.OpPREFIX, "u")
	ids := testPosting(1, 2, 3)
	defer ids.Release()
	prev.StoreMaterializedPredKey(key, ids.Borrow())
	ageOrdinal := rt.IndexedByName["Age"].Ordinal
	prev.NumericRangeBucketCacheEntry("Age", ageOrdinal, prev.Index[ageOrdinal], 16, 1)

	newRecords := benchRecords(256, 1_000)
	entries := make([]BatchEntry, len(newRecords))
	for i := range newRecords {
		entries[i] = BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
			New: unsafe.Pointer(&newRecords[i]),
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedPartialUpdate(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRecords := benchRecords(64, 1_000)
	entries := make([]BatchEntry, len(newRecords))
	for i := range newRecords {
		entries[i] = BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
			New: unsafe.Pointer(&newRecords[i]),
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedDelete(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	entries := make([]BatchEntry, len(oldRecords))
	for i := range oldRecords {
		entries[i] = BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedPartialDelete(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	entries := make([]BatchEntry, 64)
	for i := range entries {
		entries[i] = BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedPatchOnly(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(256, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	newRecords := benchRecords(256, 1_000)
	entries := make([]BatchEntry, len(newRecords))
	for i := range newRecords {
		entries[i] = BatchEntry{
			ID:        uint64(i + 1),
			Old:       unsafe.Pointer(&oldRecords[i]),
			New:       unsafe.Pointer(&newRecords[i]),
			Patch:     []schema.PatchItem{{Name: "Age", Value: newRecords[i].Age}, {Name: "Score", Value: newRecords[i].Score}},
			PatchOnly: true,
		}
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkBuildPreparedAggregatedRepeatedID(b *testing.B) {
	rt := benchRuntime(b)
	oldRecords := benchRecords(128, 0)
	prev := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(oldRecords, 1))
	defer prev.releaseRuntimeCaches()
	defer prev.releaseStorage()

	first := benchRecords(128, 1_000)
	last := benchRecords(128, 2_000)
	entries := make([]BatchEntry, 0, len(first)*2)
	for i := range first {
		entries = append(entries, BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&oldRecords[i]),
			New: unsafe.Pointer(&first[i]),
		})
		entries = append(entries, BatchEntry{
			ID:  uint64(i + 1),
			Old: unsafe.Pointer(&first[i]),
			New: unsafe.Pointer(&last[i]),
		})
	}
	cfg := CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := BuildPrepared(uint64(i+2), prev, rt, cfg, nil, rt.Patch.Fields, entries)
		snap.releaseRuntimeCaches()
		snap.releaseStorage()
	}
}

func BenchmarkManagerPinUnpinRetired(b *testing.B) {
	m := NewRegistry(true)
	current := &View{Seq: 1}
	m.Publish(current)
	_, ref, ok := m.PinBySeq(current.Seq)
	if !ok {
		b.Fatal("pin current")
	}
	m.Publish(&View{Seq: 2})
	defer m.Unpin(current.Seq, ref)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap, ref, ok := m.PinBySeq(2)
		if !ok || snap == nil {
			b.Fatal("pin latest")
		}
		m.Unpin(2, ref)
	}
}

func BenchmarkManagerPinCurrentStatsUnpin(b *testing.B) {
	m := NewRegistry(true)
	m.Publish(&View{Seq: 1})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap, seq, ref := m.PinCurrent()
		if snap == nil || seq != 1 || ref == nil {
			b.Fatal("pin current")
		}
		if stats := m.Stats(snap, ref); stats.Sequence != 1 || stats.RegistrySize != 1 {
			b.Fatalf("stats=%+v", stats)
		}
		m.Unpin(seq, ref)
	}
}

func BenchmarkManagerStageDropStaged(b *testing.B) {
	m := NewRegistry(true)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		snap := &View{Seq: uint64(i + 1)}
		m.Stage(snap)
		m.DropStaged(snap.Seq)
	}
}

func BenchmarkManagerPublishSameSeqReplace(b *testing.B) {
	m := NewRegistry(true)
	m.Publish(&View{Seq: 1})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Publish(&View{Seq: 1})
	}
}

func BenchmarkMaterializedCacheInheritRelease(b *testing.B) {
	prev := testMatPredView(128, 0)
	for i := 0; i < 128; i++ {
		testStoreMaterializedPred(prev, qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, fmt.Sprintf("%d", i)), testPosting(uint64(i+1)))
	}
	defer prev.releaseRuntimeCaches()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		next := testMatPredView(128, 0)
		inheritMaterializedPredCache(next, prev, nil, nil)
		if got := next.matPredCache.EntryCount(); got != 128 {
			b.Fatalf("inherited entries=%d", got)
		}
		next.releaseRuntimeCaches()
	}
}

func BenchmarkViewMaterializedPredMethods(b *testing.B) {
	key := qcache.MaterializedPredKeyForScalar("email", qir.OpPREFIX, "user")
	ids := testPosting(1, 2, 3)
	large := testLargePosting()
	defer ids.Release()
	defer large.Release()

	b.Run("StoreLoad", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := testMatPredView(64, 512)
			v.StoreMaterializedPredKey(key, ids.Borrow())
			got, ok := v.LoadMaterializedPredKey(key)
			if !ok || got.IsEmpty() {
				b.Fatal("load")
			}
			got.Release()
			v.releaseRuntimeCaches()
		}
	})

	b.Run("LoadOrStoreHit", func(b *testing.B) {
		v := testMatPredView(64, 512)
		defer v.releaseRuntimeCaches()
		v.StoreMaterializedPredKey(key, ids)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, ok := v.LoadOrStoreMaterializedPredKey(key, ids.Borrow())
			if !ok || got.IsEmpty() {
				b.Fatal("load or store")
			}
			got.Release()
		}
	})

	b.Run("TryLoadOrStoreOversizedHit", func(b *testing.B) {
		v := testMatPredView(64, 1)
		defer v.releaseRuntimeCaches()
		if !v.TryStoreMaterializedPredOversizedKey(key, large.Borrow()) {
			b.Fatal("seed oversized")
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, ok := v.TryLoadOrStoreMaterializedPredOversizedKey(key, large.Borrow())
			if !ok || got.IsEmpty() {
				b.Fatal("oversized load or store")
			}
			got.Release()
		}
	})

	b.Run("Promotion", func(b *testing.B) {
		v := testMatPredView(64, 512)
		defer v.releaseRuntimeCaches()

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if i&1 == 0 {
				v.ShouldPromoteRuntimeMaterializedPredKey(key)
			} else {
				v.ShouldPromoteObservedMaterializedPredKey(key, 32, 64)
			}
		}
	})
}

func BenchmarkNumericRangeCacheEntry(b *testing.B) {
	storage := benchFixedStorage(512)
	defer storage.Release()

	b.Run("Build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := &View{numericRangeBucketCache: qcache.GetNumericRangeBucketCache(1, 0)}
			entry := v.NumericRangeBucketCacheEntry("age", 0, storage, 16, 1)
			if entry == nil {
				b.Fatal("entry")
			}
			v.releaseRuntimeCaches()
		}
	})

	b.Run("Hit", func(b *testing.B) {
		v := &View{numericRangeBucketCache: qcache.GetNumericRangeBucketCache(1, 0)}
		defer v.releaseRuntimeCaches()
		if entry := v.NumericRangeBucketCacheEntry("age", 0, storage, 16, 1); entry == nil {
			b.Fatal("seed entry")
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if entry := v.NumericRangeBucketCacheEntry("age", 0, storage, 16, 1); entry == nil {
				b.Fatal("entry")
			}
		}
	})
}

func BenchmarkRuntimeCacheClearRelease(b *testing.B) {
	rt := benchRuntime(b)
	records := benchRecords(256, 0)
	snap := BuildPrepared(1, nil, rt, CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}, nil, rt.Patch.Fields, benchEntries(records, 1))
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	key := qcache.MaterializedPredKeyForScalar("Name", qir.OpPREFIX, "u")
	ids := testPosting(1, 2, 3)
	defer ids.Release()
	ageOrdinal := rt.IndexedByName["Age"].Ordinal
	ageStorage := snap.Index[ageOrdinal]

	b.Run("Clear", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			snap.StoreMaterializedPredKey(key, ids.Borrow())
			snap.NumericRangeBucketCacheEntry("Age", ageOrdinal, ageStorage, 16, 1)
			snap.ShouldPromoteRuntimeMaterializedPredKey(key)
			snap.ClearRuntimeCaches()
		}
	})

	b.Run("Release", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			v := NewView(uint64(i+2), snap, rt, CacheConfig{MatPredMaxEntries: 64, MatPredMaxCard: 512}, Storage{
				Index:             indexdata.CloneFieldStorageSlots(snap.Index, len(rt.Indexed)),
				NilIndex:          indexdata.CloneFieldStorageSlots(snap.NilIndex, len(rt.Indexed)),
				LenIndex:          indexdata.CloneFieldStorageSlots(snap.LenIndex, len(rt.Indexed)),
				LenZeroComplement: cloneFieldIndexBoolSlots(snap.LenZeroComplement, len(rt.Indexed)),
				Measure:           indexdata.CloneMeasureStorageSlots(snap.Measure, len(rt.Measures)),
				Universe:          snap.Universe,
			})
			v.StoreMaterializedPredKey(key, ids.Borrow())
			v.releaseRuntimeCaches()
			v.releaseStorage()
		}
	})
}

func BenchmarkViewStorageHelpers(b *testing.B) {
	rt := benchRuntime(b)
	records := benchRecords(256, 0)
	snap := BuildPrepared(1, nil, rt, CacheConfig{}, nil, rt.Patch.Fields, benchEntries(records, 1))
	defer snap.releaseRuntimeCaches()
	defer snap.releaseStorage()

	b.Run("FieldIndexStorage", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := snap.FieldIndexStorage("Name"); !ok {
				b.Fatal("field")
			}
		}
	})

	b.Run("FieldLookupPostingRetained", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ids := testFieldLookupPostingRetained(snap, "Name", "u_0_0")
			if ids.IsEmpty() {
				b.Fatal("lookup")
			}
			ids.Release()
		}
	})

	b.Run("NameSets", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fields := snap.FieldNameSet()
			nilFields := snap.NilFieldNameSet()
			lenFields := snap.LenFieldNameSet()
			if len(fields) == 0 || len(lenFields) == 0 || len(nilFields) != 0 {
				b.Fatal("sets")
			}
		}
	})

	b.Run("UniverseCardinality", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if snap.UniverseCardinality() != 256 {
				b.Fatal("universe")
			}
		}
	})
}
