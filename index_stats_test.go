package rbi

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/vapstack/rbi/internal/pooled"
)

type indexStatsTestRec struct {
	Name string `db:"name" dbi:"default"`
	Rank *int   `db:"rank" dbi:"default"`
}

type indexStatsTestFieldStats struct {
	unique             uint64
	postingBytes       uint64
	keyBytes           uint64
	postingCardinality uint64
	entryCount         uint64
	approxStructBytes  uint64
}

func openTempDBUint64IndexStats(t *testing.T, options ...Options) *DB[uint64, indexStatsTestRec] {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "index_stats.db")
	db, raw := openBoltAndNew[uint64, indexStatsTestRec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db
}

func TestIndexStats_MatchesSnapshotStorage(t *testing.T) {
	db := openTempDBUint64IndexStats(t)

	total := fieldIndexChunkThreshold + 17
	for i := 0; i < total; i++ {
		var rank *int
		if i%5 != 0 {
			rank = new(int)
			*rank = i % 7
		}
		if err := db.Set(uint64(i+1), &indexStatsTestRec{
			Name: fmt.Sprintf("user_%04d", i),
			Rank: rank,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	snap := db.getSnapshot()
	nameStorage, ok := snap.fieldIndexStorage("name")
	if !ok || !nameStorage.isChunked() {
		t.Fatalf("expected chunked name storage, got ok=%t chunked=%t", ok, ok && nameStorage.isChunked())
	}
	if nilSlice := snap.nilFieldIndexSlice("rank"); nilSlice == nil || len(*nilSlice) != 1 {
		t.Fatalf("expected rank nil storage to contain a single synthetic entry")
	}

	want := indexStatsTestExpected(db)
	got := db.IndexStats()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IndexStats mismatch:\n got=%+v\nwant=%+v", got, want)
	}

	var structSum uint64
	var heapSum uint64
	for _, acc := range db.indexedFieldAccess {
		name := acc.name
		fieldStruct := got.FieldApproxStructBytes[name]
		fieldHeap := got.FieldApproxHeapBytes[name]
		if fieldHeap != got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct {
			t.Fatalf("field heap mismatch for %q: got=%d want=%d", name, fieldHeap, got.FieldSize[name]+got.FieldKeyBytes[name]+fieldStruct)
		}
		structSum += fieldStruct
		heapSum += fieldHeap
	}
	if structSum != got.ApproxStructBytes {
		t.Fatalf("FieldApproxStructBytes sum mismatch: got=%d want=%d", structSum, got.ApproxStructBytes)
	}
	if heapSum != got.ApproxHeapBytes {
		t.Fatalf("FieldApproxHeapBytes sum mismatch: got=%d want=%d", heapSum, got.ApproxHeapBytes)
	}
}

func indexStatsTestExpected[K ~string | ~uint64, V any](db *DB[K, V]) IndexStats {
	snap := db.getSnapshot()
	out := IndexStats{
		UniqueFieldKeys:        make(map[string]uint64),
		FieldSize:              make(map[string]uint64),
		FieldKeyBytes:          make(map[string]uint64),
		FieldTotalCardinality:  make(map[string]uint64),
		FieldApproxStructBytes: make(map[string]uint64),
		FieldApproxHeapBytes:   make(map[string]uint64),
	}
	sharedStructBytes := indexStatsTestSliceBufBytes(snap.index) + indexStatsTestSliceBufBytes(snap.nilIndex)
	fieldCount := len(db.indexedFieldAccess)
	sharedStructPerField := uint64(0)
	sharedStructRemainder := uint64(0)
	if fieldCount > 0 {
		sharedStructPerField = sharedStructBytes / uint64(fieldCount)
		sharedStructRemainder = sharedStructBytes % uint64(fieldCount)
	}

	for i, acc := range db.indexedFieldAccess {
		fieldStats := indexStatsTestStorageStats(snap.index.Get(acc.ordinal), true)
		nilStats := indexStatsTestStorageStats(snap.nilIndex.Get(acc.ordinal), false)

		out.UniqueFieldKeys[acc.name] = fieldStats.unique + nilStats.unique
		out.FieldSize[acc.name] = fieldStats.postingBytes + nilStats.postingBytes
		out.FieldKeyBytes[acc.name] = fieldStats.keyBytes + nilStats.keyBytes
		out.FieldTotalCardinality[acc.name] = fieldStats.postingCardinality + nilStats.postingCardinality
		fieldStructBytes := fieldStats.approxStructBytes + nilStats.approxStructBytes + sharedStructPerField
		if uint64(i) < sharedStructRemainder {
			fieldStructBytes++
		}
		out.FieldApproxStructBytes[acc.name] = fieldStructBytes
		out.FieldApproxHeapBytes[acc.name] = out.FieldSize[acc.name] + out.FieldKeyBytes[acc.name] + fieldStructBytes

		out.Size += out.FieldSize[acc.name]
		out.EntryCount += fieldStats.entryCount + nilStats.entryCount
		out.KeyBytes += out.FieldKeyBytes[acc.name]
		out.PostingCardinality += out.FieldTotalCardinality[acc.name]
		out.ApproxStructBytes += fieldStructBytes
	}

	out.ApproxHeapBytes = out.Size + out.KeyBytes + out.ApproxStructBytes
	return out
}

func indexStatsTestStorageStats(storage fieldIndexStorage, countDistinct bool) indexStatsTestFieldStats {
	stats := indexStatsTestFieldStats{
		approxStructBytes: indexStatsTestStorageStructBytes(storage),
	}
	ov := newFieldOverlayStorage(storage)
	if ov.keyCount() == 0 {
		return stats
	}

	br := ov.rangeByRanks(0, ov.keyCount())
	cur := ov.newCursor(br, false)
	for {
		key, ids, ok := cur.next()
		if !ok {
			return stats
		}
		if ids.IsEmpty() {
			continue
		}
		if countDistinct {
			stats.unique++
			stats.keyBytes += uint64(key.byteLen())
		}
		stats.postingBytes += ids.SizeInBytes()
		card := ids.Cardinality()
		stats.postingCardinality += card
		stats.entryCount++
	}
}

func indexStatsTestStorageStructBytes(storage fieldIndexStorage) uint64 {
	if storage.flat != nil {
		return indexStatsTestFlatStructBytes(storage.flat)
	}
	if storage.chunked != nil {
		return indexStatsTestChunkedStructBytes(storage.chunked)
	}
	return 0
}

func indexStatsTestFlatStructBytes(root *fieldIndexFlatRoot) uint64 {
	if root == nil {
		return 0
	}
	total := uint64(unsafe.Sizeof(fieldIndexFlatRoot{})) +
		uint64(cap(root.entries))*uint64(unsafe.Sizeof(index{}))
	for i := range root.entries {
		if root.entries[i].Key.isNumeric() {
			total -= uint64(unsafe.Sizeof(uint64(0)))
		}
	}
	return total
}

func indexStatsTestChunkedStructBytes(root *fieldIndexChunkedRoot) uint64 {
	if root == nil {
		return 0
	}
	total := uint64(unsafe.Sizeof(fieldIndexChunkedRoot{})) +
		indexStatsTestSliceBufBytes(root.pages) +
		indexStatsTestSliceBufBytes(root.chunkPrefix) +
		indexStatsTestSliceBufBytes(root.prefix) +
		indexStatsTestSliceBufBytes(root.rowPrefix)

	for i := 0; i < root.pages.Len(); i++ {
		page := root.pages.Get(i)
		if page == nil {
			continue
		}
		total += uint64(unsafe.Sizeof(fieldIndexChunkDirPage{})) +
			indexStatsTestSliceBufBytes(page.refs) +
			indexStatsTestSliceBufBytes(page.prefix) +
			indexStatsTestSliceBufBytes(page.rowPrefix)

		for j := 0; j < page.refsLen(); j++ {
			chunk := page.refAt(j).chunk
			if chunk == nil {
				continue
			}
			total += uint64(unsafe.Sizeof(fieldIndexChunk{})) +
				uint64(cap(chunk.posts))*uint64(unsafe.Sizeof(index{}.IDs))
			if chunk.numeric == nil {
				total += uint64(cap(chunk.stringRefs)) * uint64(unsafe.Sizeof(fieldIndexStringRef{}))
			}
		}
	}

	return total
}

func indexStatsTestSliceBufBytes[T any](buf *pooled.SliceBuf[T]) uint64 {
	if buf == nil {
		return 0
	}
	var zero T
	return uint64(unsafe.Sizeof(pooled.SliceBuf[T]{})) +
		uint64(buf.Cap())*uint64(unsafe.Sizeof(zero))
}
