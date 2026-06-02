package persist

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/posting"
	"github.com/vapstack/rbi/internal/qexec"
	"github.com/vapstack/rbi/internal/schema"
	"github.com/vapstack/rbi/internal/snapshot"
	"github.com/vapstack/rbi/internal/strmap"
)

const (
	benchPersistSeq             = 77
	benchPersistStrMapCompactAt = 256
	benchPersistBufferSize      = 64 << 10
)

var (
	benchPersistStaleErr   = errors.New("bench persisted index is stale")
	benchPersistInvalidErr = errors.New("bench persisted index is invalid")
	benchPersistSink       uint64
)

type persistBenchFixture struct {
	rt    *schema.Schema
	snap  *snapshot.View
	stats *qexec.PlannerStatsSnapshot
}

func BenchmarkSidecarStringCodec(b *testing.B) {
	cases := [...]struct {
		name  string
		value string
	}{
		{name: "Short", value: "field-name"},
		{name: "Long4K", value: strings.Repeat("index-key/", 512)},
	}

	for _, tc := range cases {
		b.Run("Write/"+tc.name, func(b *testing.B) {
			writer := bufio.NewWriterSize(io.Discard, benchPersistBufferSize)
			b.SetBytes(int64(len(tc.value)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeSidecarString(writer, tc.value); err != nil {
					b.Fatalf("writeSidecarString: %v", err)
				}
				if err := writer.Flush(); err != nil {
					b.Fatalf("Flush: %v", err)
				}
			}
		})

		b.Run("Read/"+tc.name, func(b *testing.B) {
			payload := benchPersistSidecarStringBytes(b, tc.value)
			reader := bytes.NewReader(payload)
			br := bufio.NewReaderSize(reader, benchPersistBufferSize)
			var total uint64
			b.SetBytes(int64(len(tc.value)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.Reset(payload)
				br.Reset(reader)
				got, err := readSidecarString(br)
				if err != nil {
					b.Fatalf("readSidecarString: %v", err)
				}
				total += uint64(len(got))
			}
			benchPersistSink = total
		})
	}
}

func BenchmarkFieldCompatibilityCodec(b *testing.B) {
	const fields = 256
	storedAll, currentAll := benchPersistFieldMaps(fields, 0)
	storedHalf, currentHalf := benchPersistFieldMaps(fields, 2)

	b.Run("WriteFields", func(b *testing.B) {
		size := len(benchPersistFieldsBytes(b, storedAll))
		writer := bufio.NewWriterSize(io.Discard, benchPersistBufferSize)
		benchPersistSetSidecarBytes(b, int64(size))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := writeFields(writer, storedAll); err != nil {
				b.Fatalf("writeFields: %v", err)
			}
			if err := writer.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	})

	b.Run("ReadAllCompatible", func(b *testing.B) {
		payload := benchPersistFieldsBytes(b, storedAll)
		benchmarkReadFieldCompatibility(b, payload, currentAll)
	})

	b.Run("ReadHalfCompatible", func(b *testing.B) {
		payload := benchPersistFieldsBytes(b, storedHalf)
		benchmarkReadFieldCompatibility(b, payload, currentHalf)
	})
}

func BenchmarkPlannerStatsSnapshotCodec(b *testing.B) {
	stats, all, half := benchPersistPlannerStats(256)

	b.Run("Write", func(b *testing.B) {
		size := len(benchPersistPlannerStatsBytes(b, stats))
		writer := bufio.NewWriterSize(io.Discard, benchPersistBufferSize)
		benchPersistSetSidecarBytes(b, int64(size))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := writePlannerStatsSnapshot(writer, stats); err != nil {
				b.Fatalf("writePlannerStatsSnapshot: %v", err)
			}
			if err := writer.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	})

	b.Run("ReadAllCompatible", func(b *testing.B) {
		payload := benchPersistPlannerStatsBytes(b, stats)
		benchmarkReadPlannerStatsSnapshot(b, payload, all)
	})

	b.Run("ReadHalfCompatible", func(b *testing.B) {
		payload := benchPersistPlannerStatsBytes(b, stats)
		benchmarkReadPlannerStatsSnapshot(b, payload, half)
	})
}

func BenchmarkStorePayload(b *testing.B) {
	cases := [...]struct {
		name   string
		rows   int
		strMap bool
	}{
		{name: "Flat64", rows: 64},
		{name: "Chunked4096", rows: 4096},
		{name: "Chunked4096StringKeys", rows: 4096, strMap: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			fx := newPersistBenchFixture(b, tc.rows, tc.strMap)
			defer fx.release()
			size := len(benchPersistPayloadBytes(b, fx))
			writer := bufio.NewWriterSize(io.Discard, benchPersistBufferSize)
			benchPersistSetSidecarBytes(b, int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := storePayload(writer, fx.rt, fx.snap, fx.stats); err != nil {
					b.Fatalf("storePayload: %v", err)
				}
				if err := writer.Flush(); err != nil {
					b.Fatalf("Flush: %v", err)
				}
			}
		})
	}
}

func BenchmarkLoadPayload(b *testing.B) {
	cases := [...]struct {
		name   string
		rows   int
		strMap bool
	}{
		{name: "Flat64", rows: 64},
		{name: "Chunked4096", rows: 4096},
		{name: "Chunked4096StringKeys", rows: 4096, strMap: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			fx := newPersistBenchFixture(b, tc.rows, tc.strMap)
			defer fx.release()
			payload := benchPersistPayloadBytes(b, fx)
			benchmarkLoadPayloadBytes(b, payload, fx.rt)
		})
	}
}

func BenchmarkLoadPayloadSkipIncompatible(b *testing.B) {
	fx := newPersistBenchFixture(b, 4096, true)
	defer fx.release()
	payload := benchPersistPayloadBytes(b, fx)
	current := benchPersistRuntime("tags_v2", "amount_v2")
	benchmarkLoadPayloadBytes(b, payload, current)
}

func BenchmarkStore(b *testing.B) {
	cases := [...]struct {
		name   string
		rows   int
		strMap bool
	}{
		{name: "Flat64", rows: 64},
		{name: "Chunked4096", rows: 4096},
		{name: "Chunked4096StringKeys", rows: 4096, strMap: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			fx := newPersistBenchFixture(b, tc.rows, tc.strMap)
			defer fx.release()
			file := benchPersistTempFile(b, "bench.rbi")
			size := benchPersistStoreFile(b, fx, file)
			benchPersistSetSidecarBytes(b, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := Store(benchPersistStoreConfig(fx, file)); err != nil {
					b.Fatalf("Store: %v", err)
				}
			}
		})
	}
}

func BenchmarkLoad(b *testing.B) {
	cases := [...]struct {
		name   string
		rows   int
		strMap bool
	}{
		{name: "Flat64", rows: 64},
		{name: "Chunked4096", rows: 4096},
		{name: "Chunked4096StringKeys", rows: 4096, strMap: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			fx := newPersistBenchFixture(b, tc.rows, tc.strMap)
			defer fx.release()
			file := benchPersistTempFile(b, "bench.rbi")
			size := benchPersistStoreFile(b, fx, file)
			var total uint64
			benchPersistSetSidecarBytes(b, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := Load(benchPersistLoadConfig(fx.rt, file, benchPersistSeq))
				if err != nil {
					b.Fatalf("Load: %v", err)
				}
				total += benchPersistLoadResultMetric(result)
				result.Storage.Release()
			}
			benchPersistSink = total
		})
	}
}

func BenchmarkStoreLoadRoundTrip(b *testing.B) {
	fx := newPersistBenchFixture(b, 4096, true)
	defer fx.release()
	file := benchPersistTempFile(b, "bench.rbi")
	size := benchPersistStoreFile(b, fx, file)
	benchPersistSetSidecarBytes(b, size)
	var total uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Store(benchPersistStoreConfig(fx, file)); err != nil {
			b.Fatalf("Store: %v", err)
		}
		result, err := Load(benchPersistLoadConfig(fx.rt, file, benchPersistSeq))
		if err != nil {
			b.Fatalf("Load: %v", err)
		}
		total += benchPersistLoadResultMetric(result)
		result.Storage.Release()
	}
	benchPersistSink = total
}

func BenchmarkPersistMemory(b *testing.B) {
	const rows = 65536

	b.Run("StorePayload", func(b *testing.B) {
		fx := newPersistBenchFixture(b, rows, true)
		defer fx.release()
		size := len(benchPersistPayloadBytes(b, fx))
		writer := bufio.NewWriterSize(io.Discard, benchPersistBufferSize)
		var maxLiveDelta uint64
		var maxAfterGCDelta uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			debug.FreeOSMemory()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			b.StartTimer()
			if err := storePayload(writer, fx.rt, fx.snap, fx.stats); err != nil {
				b.Fatalf("storePayload: %v", err)
			}
			if err := writer.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
			b.StopTimer()

			var afterStore runtime.MemStats
			runtime.ReadMemStats(&afterStore)
			benchPersistUpdateMaxHeapDelta(&maxLiveDelta, before.HeapAlloc, afterStore.HeapAlloc)
			debug.FreeOSMemory()
			var afterGC runtime.MemStats
			runtime.ReadMemStats(&afterGC)
			benchPersistUpdateMaxHeapDelta(&maxAfterGCDelta, before.HeapAlloc, afterGC.HeapAlloc)
		}
		benchPersistSetSidecarBytes(b, int64(size))
		b.ReportMetric(float64(maxLiveDelta), "max_live_heap_B")
		b.ReportMetric(float64(maxAfterGCDelta), "max_after_gc_heap_B")
	})

	b.Run("StoreFile", func(b *testing.B) {
		fx := newPersistBenchFixture(b, rows, true)
		defer fx.release()
		file := benchPersistTempFile(b, "bench.rbi")
		size := benchPersistStoreFile(b, fx, file)
		var maxLiveDelta uint64
		var maxAfterGCDelta uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			debug.FreeOSMemory()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			b.StartTimer()
			if err := Store(benchPersistStoreConfig(fx, file)); err != nil {
				b.Fatalf("Store: %v", err)
			}
			b.StopTimer()

			var afterStore runtime.MemStats
			runtime.ReadMemStats(&afterStore)
			benchPersistUpdateMaxHeapDelta(&maxLiveDelta, before.HeapAlloc, afterStore.HeapAlloc)
			debug.FreeOSMemory()
			var afterGC runtime.MemStats
			runtime.ReadMemStats(&afterGC)
			benchPersistUpdateMaxHeapDelta(&maxAfterGCDelta, before.HeapAlloc, afterGC.HeapAlloc)
		}
		benchPersistSetSidecarBytes(b, size)
		b.ReportMetric(float64(maxLiveDelta), "max_live_heap_B")
		b.ReportMetric(float64(maxAfterGCDelta), "max_after_gc_heap_B")
	})

	b.Run("LoadPayload", func(b *testing.B) {
		fx := newPersistBenchFixture(b, rows, true)
		defer fx.release()
		payload := benchPersistPayloadBytes(b, fx)
		reader := bytes.NewReader(payload)
		br := bufio.NewReaderSize(reader, benchPersistBufferSize)
		var total uint64
		var maxLiveDelta uint64
		var maxAfterReleaseDelta uint64
		var maxAfterGCDelta uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			debug.FreeOSMemory()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			reader.Reset(payload)
			br.Reset(reader)

			b.StartTimer()
			result, err := loadPayload(br, fx.rt, benchPersistStrMapCompactAt)
			b.StopTimer()
			if err != nil {
				b.Fatalf("loadPayload: %v", err)
			}

			var afterLoad runtime.MemStats
			runtime.ReadMemStats(&afterLoad)
			benchPersistUpdateMaxHeapDelta(&maxLiveDelta, before.HeapAlloc, afterLoad.HeapAlloc)
			total += benchPersistLoadResultMetric(result)
			result.Storage.Release()
			result = LoadResult{}
			var afterRelease runtime.MemStats
			runtime.ReadMemStats(&afterRelease)
			benchPersistUpdateMaxHeapDelta(&maxAfterReleaseDelta, before.HeapAlloc, afterRelease.HeapAlloc)
			debug.FreeOSMemory()
			var afterGC runtime.MemStats
			runtime.ReadMemStats(&afterGC)
			benchPersistUpdateMaxHeapDelta(&maxAfterGCDelta, before.HeapAlloc, afterGC.HeapAlloc)
		}
		benchPersistSetSidecarBytes(b, int64(len(payload)))
		b.ReportMetric(float64(maxLiveDelta), "max_live_heap_B")
		b.ReportMetric(float64(maxAfterReleaseDelta), "max_after_release_heap_B")
		b.ReportMetric(float64(maxAfterGCDelta), "max_after_gc_heap_B")
		benchPersistSink = total
	})

	b.Run("LoadFile", func(b *testing.B) {
		fx := newPersistBenchFixture(b, rows, true)
		defer fx.release()
		file := benchPersistTempFile(b, "bench.rbi")
		size := benchPersistStoreFile(b, fx, file)
		var total uint64
		var maxLiveDelta uint64
		var maxAfterReleaseDelta uint64
		var maxAfterGCDelta uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for i := 0; i < b.N; i++ {
			debug.FreeOSMemory()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			b.StartTimer()
			result, err := Load(benchPersistLoadConfig(fx.rt, file, benchPersistSeq))
			b.StopTimer()
			if err != nil {
				b.Fatalf("Load: %v", err)
			}

			var afterLoad runtime.MemStats
			runtime.ReadMemStats(&afterLoad)
			benchPersistUpdateMaxHeapDelta(&maxLiveDelta, before.HeapAlloc, afterLoad.HeapAlloc)
			total += benchPersistLoadResultMetric(result)
			result.Storage.Release()
			result = LoadResult{}
			var afterRelease runtime.MemStats
			runtime.ReadMemStats(&afterRelease)
			benchPersistUpdateMaxHeapDelta(&maxAfterReleaseDelta, before.HeapAlloc, afterRelease.HeapAlloc)
			debug.FreeOSMemory()
			var afterGC runtime.MemStats
			runtime.ReadMemStats(&afterGC)
			benchPersistUpdateMaxHeapDelta(&maxAfterGCDelta, before.HeapAlloc, afterGC.HeapAlloc)
		}
		benchPersistSetSidecarBytes(b, size)
		b.ReportMetric(float64(maxLiveDelta), "max_live_heap_B")
		b.ReportMetric(float64(maxAfterReleaseDelta), "max_after_release_heap_B")
		b.ReportMetric(float64(maxAfterGCDelta), "max_after_gc_heap_B")
		benchPersistSink = total
	})
}

func BenchmarkLoadRejects(b *testing.B) {
	b.Run("StaleSequence", func(b *testing.B) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		if err := writer.WriteByte(persistedIndexVersion); err != nil {
			b.Fatalf("write version: %v", err)
		}
		if err := writeSidecarUvarint(writer, benchPersistSeq); err != nil {
			b.Fatalf("write seq: %v", err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
		file := benchPersistTempFile(b, "stale.rbi")
		if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
			b.Fatalf("WriteFile: %v", err)
		}
		cfg := LoadConfig{
			File:       file,
			DBPath:     "bench.db",
			Bucket:     []byte("bench"),
			CurrentSeq: benchPersistSeq + 1,
			Schema:     &schema.Schema{},
			Errors:     Errors{Stale: benchPersistStaleErr, Invalid: benchPersistInvalidErr},
		}
		b.SetBytes(int64(buf.Len()))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Load(cfg)
			if !errors.Is(err, benchPersistStaleErr) {
				b.Fatalf("Load err=%v, want stale sentinel", err)
			}
		}
	})

	b.Run("InvalidVersion", func(b *testing.B) {
		file := benchPersistTempFile(b, "invalid.rbi")
		if err := os.WriteFile(file, []byte{99}, 0o600); err != nil {
			b.Fatalf("WriteFile: %v", err)
		}
		cfg := LoadConfig{
			File:       file,
			DBPath:     "bench.db",
			Bucket:     []byte("bench"),
			CurrentSeq: benchPersistSeq,
			Schema:     &schema.Schema{},
			Errors:     Errors{Stale: benchPersistStaleErr, Invalid: benchPersistInvalidErr},
		}
		b.SetBytes(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := Load(cfg)
			if !errors.Is(err, benchPersistInvalidErr) {
				b.Fatalf("Load err=%v, want invalid sentinel", err)
			}
		}
	})
}

func benchmarkReadFieldCompatibility(b *testing.B, payload []byte, current map[string]*schema.Field) {
	reader := bytes.NewReader(payload)
	br := bufio.NewReaderSize(reader, benchPersistBufferSize)
	var total uint64
	benchPersistSetSidecarBytes(b, int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		br.Reset(reader)
		compatible, err := readFieldCompatibility(br, current)
		if err != nil {
			b.Fatalf("readFieldCompatibility: %v", err)
		}
		total += uint64(len(compatible))
	}
	benchPersistSink = total
}

func benchmarkReadPlannerStatsSnapshot(b *testing.B, payload []byte, compatible map[string]bool) {
	reader := bytes.NewReader(payload)
	br := bufio.NewReaderSize(reader, benchPersistBufferSize)
	var total uint64
	benchPersistSetSidecarBytes(b, int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		br.Reset(reader)
		stats, err := readPlannerStatsSnapshot(br, compatible)
		if err != nil {
			b.Fatalf("readPlannerStatsSnapshot: %v", err)
		}
		total += stats.Version + uint64(len(stats.Fields))
	}
	benchPersistSink = total
}

func benchmarkLoadPayloadBytes(b *testing.B, payload []byte, s *schema.Schema) {
	reader := bytes.NewReader(payload)
	br := bufio.NewReaderSize(reader, benchPersistBufferSize)
	var total uint64
	benchPersistSetSidecarBytes(b, int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		br.Reset(reader)
		result, err := loadPayload(br, s, benchPersistStrMapCompactAt)
		if err != nil {
			b.Fatalf("loadPayload: %v", err)
		}
		total += benchPersistLoadResultMetric(result)
		result.Storage.Release()
	}
	benchPersistSink = total
}

func newPersistBenchFixture(tb testing.TB, rows int, withStrMap bool) *persistBenchFixture {
	tb.Helper()
	rt := benchPersistRuntime("tags", "amount")

	index := indexdata.GetFieldStorageSlice(len(rt.Indexed))[:len(rt.Indexed)]
	nilIndex := indexdata.GetFieldStorageSlice(len(rt.Indexed))[:len(rt.Indexed)]
	lenIndex := indexdata.GetFieldStorageSlice(len(rt.Indexed))[:len(rt.Indexed)]
	measure := indexdata.GetMeasureStorageSlice(len(rt.Measures))[:len(rt.Measures)]

	universe := benchPersistUniverse(rows)
	index[0] = benchPersistStringStorage(rows, "name")
	index[1] = benchPersistNumericStorage(rows)
	index[2] = benchPersistTagStorage(rows)
	nilIndex[0] = benchPersistNilStorage(rows)
	lenIndex[2] = benchPersistLenStorage(rows, universe)
	measure[0] = benchPersistMeasureStorage(rows)

	var sm *strmap.Snapshot
	if withStrMap {
		sm = benchPersistStrMapSnapshot(rows)
	}

	snap := &snapshot.View{
		Seq:                benchPersistSeq,
		Index:              index,
		NilIndex:           nilIndex,
		LenIndex:           lenIndex,
		Measure:            measure,
		IndexedFieldByName: rt.IndexedByName,
		Universe:           universe,
		StrMap:             sm,
	}

	return &persistBenchFixture{
		rt:    rt,
		snap:  snap,
		stats: benchPersistStats(rows, index[0].KeyCount(), index[1].KeyCount(), index[2].KeyCount()),
	}
}

func (fx *persistBenchFixture) release() {
	indexdata.ReleaseFieldStorageSlots(fx.snap.Index)
	indexdata.ReleaseFieldStorageSlots(fx.snap.NilIndex)
	indexdata.ReleaseFieldStorageSlots(fx.snap.LenIndex)
	indexdata.ReleaseMeasureStorageSlots(fx.snap.Measure)
	fx.snap.Universe.Release()
}

func benchPersistRuntime(tagsDBName, amountDBName string) *schema.Schema {
	name := benchPersistField("name", "name", 0, reflect.String, false, schema.IndexDefault)
	age := benchPersistField("age", "age", 1, reflect.Int, false, schema.IndexDefault)
	tags := benchPersistField("tags", tagsDBName, 2, reflect.String, true, schema.IndexDefault)
	amount := benchPersistField("amount", amountDBName, 0, reflect.Uint64, false, schema.IndexMeasure)

	fields := map[string]*schema.Field{
		"name": name,
		"age":  age,
		"tags": tags,
	}
	measureFields := map[string]*schema.Field{
		"amount": amount,
	}
	indexed := []schema.IndexedFieldAccessor{
		{Ordinal: 0, Name: "name", Field: name},
		{Ordinal: 1, Name: "age", Field: age},
		{Ordinal: 2, Name: "tags", Field: tags},
	}
	indexedByName := schema.IndexedFieldMap{
		"name": {Ordinal: 0, Name: "name", Field: name},
		"age":  {Ordinal: 1, Name: "age", Field: age},
		"tags": {Ordinal: 2, Name: "tags", Field: tags},
	}
	measures := []schema.MeasureFieldAccessor{
		{Ordinal: 0, Name: "amount", Field: amount},
	}
	measuresByName := schema.MeasureFieldMap{
		"amount": {Ordinal: 0, Name: "amount", Field: amount},
	}
	return &schema.Schema{
		Fields:         fields,
		MeasureFields:  measureFields,
		Indexed:        indexed,
		IndexedByName:  indexedByName,
		Measures:       measures,
		MeasuresByName: measuresByName,
	}
}

func benchPersistField(name, dbName string, ordinal int, kind reflect.Kind, slice bool, indexKind schema.IndexKind) *schema.Field {
	keyKind := schema.FieldWriteKeysString
	if !slice {
		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			keyKind = schema.FieldWriteKeysOrderedU64
		}
	}
	return &schema.Field{
		Name:      name,
		IndexKind: indexKind,
		Kind:      kind,
		Slice:     slice,
		KeyKind:   keyKind,
		DBName:    dbName,
		Index:     []int{ordinal},
	}
}

func benchPersistUniverse(rows int) posting.List {
	ids := make([]uint64, rows)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return posting.BuildFromSorted(ids)
}

func benchPersistStringStorage(rows int, prefix string) indexdata.FieldStorage {
	m := indexdata.GetPostingMap()
	for i := 0; i < rows; i++ {
		m[fmt.Sprintf("%s/%06d", prefix, i)] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return indexdata.NewRegularFieldStorageFromPostingMapOwned(m, false)
}

func benchPersistNumericStorage(rows int) indexdata.FieldStorage {
	m := indexdata.GetFixedPostingMap()
	for i := 0; i < rows; i++ {
		m[uint64(i*2)] = (posting.List{}).BuildAdded(uint64(i + 1))
	}
	return indexdata.NewRegularFieldStorageFromFixedPostingMapOwned(m)
}

func benchPersistTagStorage(rows int) indexdata.FieldStorage {
	keys := rows / 4
	if keys < 1 {
		keys = 1
	}
	m := indexdata.GetPostingMap()
	ids := make([]uint64, 0, rows/keys+1)
	for key := 0; key < keys; key++ {
		ids = ids[:0]
		for id := key + 1; id <= rows; id += keys {
			ids = append(ids, uint64(id))
		}
		m[fmt.Sprintf("tag/%05d", key)] = posting.BuildFromSorted(ids)
	}
	return indexdata.NewRegularFieldStorageFromPostingMapOwned(m, false)
}

func benchPersistNilStorage(rows int) indexdata.FieldStorage {
	n := (rows + 16) / 17
	ids := make([]uint64, 0, n)
	for id := 17; id <= rows; id += 17 {
		ids = append(ids, uint64(id))
	}
	if len(ids) == 0 && rows > 0 {
		ids = append(ids, 1)
	}
	return indexdata.NewNilFieldStorageOwned(posting.BuildFromSorted(ids))
}

func benchPersistLenStorage(rows int, universe posting.List) indexdata.FieldStorage {
	lengths := indexdata.GetLenPostingMap()
	var buckets [4][]uint64
	for i := 0; i < len(buckets); i++ {
		buckets[i] = make([]uint64, 0, rows/4+1)
	}
	for id := 1; id <= rows; id++ {
		bucket := id & 3
		buckets[bucket] = append(buckets[bucket], uint64(id))
	}
	for i := 0; i < len(buckets); i++ {
		lengths[uint32(i)] = posting.BuildFromSorted(buckets[i])
	}
	storage, _ := indexdata.NewLenFieldStorageFromMapOwned(universe, lengths)
	indexdata.ReleaseLenPostingMap(lengths)
	return storage
}

func benchPersistMeasureStorage(rows int) indexdata.MeasureStorage {
	entries := indexdata.GetMeasureEntrySlice(rows)
	for i := 0; i < rows; i++ {
		entries = append(entries, indexdata.MeasureEntry{
			ID:    uint64(i + 1),
			Value: uint64(i*11 + 3),
		})
	}
	return indexdata.NewMeasureStorageFromEntriesOwned(entries)
}

func benchPersistStrMapSnapshot(rows int) *strmap.Snapshot {
	m := strmap.New(uint64(rows), benchPersistStrMapCompactAt)
	for i := 0; i < rows; i++ {
		m.Create(fmt.Sprintf("pk/%08d", i+1))
	}
	return m.Snapshot()
}

func benchPersistStats(rows, nameKeys, ageKeys, tagKeys int) *qexec.PlannerStatsSnapshot {
	return &qexec.PlannerStatsSnapshot{
		Version:             11,
		GeneratedAt:         time.Unix(1700000000, int64(rows)).UTC(),
		UniverseCardinality: uint64(rows),
		Fields: map[string]qexec.PlannerFieldStats{
			"name": benchPersistFieldStats(rows, nameKeys),
			"age":  benchPersistFieldStats(rows, ageKeys),
			"tags": benchPersistFieldStats(rows, tagKeys),
		},
	}
}

func benchPersistFieldStats(rows, keys int) qexec.PlannerFieldStats {
	maxCard := uint64(1)
	if keys > 0 {
		if avg := rows / keys; avg > 1 {
			maxCard = uint64(avg + 1)
		}
	}
	return qexec.PlannerFieldStats{
		DistinctKeys:    uint64(keys),
		NonEmptyKeys:    uint64(keys),
		TotalBucketCard: uint64(rows),
		MaxBucketCard:   maxCard,
		P50BucketCard:   maxCard,
		P95BucketCard:   maxCard,
		AvgBucketCard:   float64(rows) / float64(max(1, keys)),
	}
}

func benchPersistFieldMaps(n int, incompatibleEvery int) (map[string]*schema.Field, map[string]*schema.Field) {
	stored := make(map[string]*schema.Field, n)
	current := make(map[string]*schema.Field, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("field_%04d", i)
		stored[name] = benchPersistField(name, name, i, reflect.Int, false, schema.IndexDefault)
		dbName := name
		if incompatibleEvery > 0 && i%incompatibleEvery == 0 {
			dbName += "_v2"
		}
		current[name] = benchPersistField(name, dbName, i, reflect.Int, false, schema.IndexDefault)
	}
	return stored, current
}

func benchPersistPlannerStats(n int) (*qexec.PlannerStatsSnapshot, map[string]bool, map[string]bool) {
	fields := make(map[string]qexec.PlannerFieldStats, n)
	all := make(map[string]bool, n)
	half := make(map[string]bool, n/2+1)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("field_%04d", i)
		fields[name] = qexec.PlannerFieldStats{
			DistinctKeys:    uint64(i + 1),
			NonEmptyKeys:    uint64(i + 1),
			TotalBucketCard: uint64((i + 1) * 3),
			MaxBucketCard:   3,
			P50BucketCard:   2,
			P95BucketCard:   3,
			AvgBucketCard:   3,
		}
		all[name] = true
		if i&1 == 0 {
			half[name] = true
		}
	}
	return &qexec.PlannerStatsSnapshot{
		Version:             19,
		GeneratedAt:         time.Unix(1700000001, 2).UTC(),
		UniverseCardinality: uint64(n * 3),
		Fields:              fields,
	}, all, half
}

func benchPersistSidecarStringBytes(tb testing.TB, value string) []byte {
	tb.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := writeSidecarString(writer, value); err != nil {
		tb.Fatalf("writeSidecarString: %v", err)
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func benchPersistFieldsBytes(tb testing.TB, fields map[string]*schema.Field) []byte {
	tb.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchPersistBufferSize)
	if err := writeFields(writer, fields); err != nil {
		tb.Fatalf("writeFields: %v", err)
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func benchPersistPlannerStatsBytes(tb testing.TB, stats *qexec.PlannerStatsSnapshot) []byte {
	tb.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchPersistBufferSize)
	if err := writePlannerStatsSnapshot(writer, stats); err != nil {
		tb.Fatalf("writePlannerStatsSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func benchPersistPayloadBytes(tb testing.TB, fx *persistBenchFixture) []byte {
	tb.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchPersistBufferSize)
	if err := storePayload(writer, fx.rt, fx.snap, fx.stats); err != nil {
		tb.Fatalf("storePayload: %v", err)
	}
	if err := writer.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func benchPersistStoreFile(tb testing.TB, fx *persistBenchFixture, file string) int64 {
	tb.Helper()
	if err := Store(benchPersistStoreConfig(fx, file)); err != nil {
		tb.Fatalf("Store: %v", err)
	}
	info, err := os.Stat(file)
	if err != nil {
		tb.Fatalf("Stat: %v", err)
	}
	return info.Size()
}

func benchPersistTempFile(tb testing.TB, name string) string {
	tb.Helper()
	base := filepath.Join("..", "..", ".tmp")
	if err := os.MkdirAll(base, 0o755); err != nil {
		tb.Fatalf("MkdirAll: %v", err)
	}
	dir, err := os.MkdirTemp(base, "persist-bench-*")
	if err != nil {
		tb.Fatalf("MkdirTemp: %v", err)
	}
	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatalf("RemoveAll: %v", err)
		}
	})
	return filepath.Join(dir, name)
}

func benchPersistStoreConfig(fx *persistBenchFixture, file string) StoreConfig {
	return StoreConfig{
		File:         file,
		BucketSeq:    benchPersistSeq,
		Schema:       fx.rt,
		Snapshot:     fx.snap,
		PlannerStats: fx.stats,
	}
}

func benchPersistLoadConfig(s *schema.Schema, file string, seq uint64) LoadConfig {
	return LoadConfig{
		File:            file,
		DBPath:          "bench.db",
		Bucket:          []byte("bench"),
		CurrentSeq:      seq,
		Schema:          s,
		StrMapCompactAt: benchPersistStrMapCompactAt,
		Errors:          Errors{Stale: benchPersistStaleErr, Invalid: benchPersistInvalidErr},
	}
}

func benchPersistLoadResultMetric(result LoadResult) uint64 {
	total := result.Storage.Universe.Cardinality()
	total += uint64(len(result.SkipFields) + len(result.SkipMeasureFields))
	if result.LenLoaded {
		total++
	}
	if result.StrMap != nil {
		total++
	}
	if result.PlannerStats != nil {
		total += result.PlannerStats.Version + uint64(len(result.PlannerStats.Fields))
	}
	return total
}

func benchPersistUpdateMaxHeapDelta(maxDelta *uint64, before, after uint64) {
	if after <= before {
		return
	}
	delta := after - before
	if delta > *maxDelta {
		*maxDelta = delta
	}
}

func benchPersistSetSidecarBytes(b *testing.B, size int64) {
	b.SetBytes(size)
	b.ReportMetric(float64(size)/(1<<20), "sidecar_MiB")
}
