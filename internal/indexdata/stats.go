package indexdata

import (
	"unsafe"

	"github.com/vapstack/rbi/internal/posting"
)

type FieldStats struct {
	Unique             uint64
	PostingBytes       uint64
	KeyBytes           uint64
	PostingCardinality uint64
	EntryCount         uint64
	ApproxStructBytes  uint64
}

func (s FieldStorage) Stats(countDistinct bool) FieldStats {
	if s.flat != nil {
		return s.flat.stats(countDistinct)
	}
	if s.chunked != nil {
		return s.chunked.stats(countDistinct)
	}
	return FieldStats{}
}

func (root *fieldIndexFlatRoot) stats(countDistinct bool) FieldStats {
	stats := FieldStats{
		ApproxStructBytes: uint64(unsafe.Sizeof(fieldIndexFlatRoot{})) +
			uint64(cap(root.entries))*uint64(unsafe.Sizeof(Entry{})),
	}
	for i := range root.entries {
		entry := root.entries[i]
		if entry.Key.IsNumeric() {
			stats.ApproxStructBytes -= uint64(unsafe.Sizeof(uint64(0)))
		}
		stats.accumulateEntry(entry, countDistinct)
	}
	return stats
}

func (root *fieldIndexChunkedRoot) stats(countDistinct bool) FieldStats {
	stats := FieldStats{
		ApproxStructBytes: uint64(unsafe.Sizeof(fieldIndexChunkedRoot{})) +
			uint64(cap(root.pages))*uint64(unsafe.Sizeof((*fieldIndexChunkDirPage)(nil))) +
			uint64(cap(root.chunkPrefix))*uint64(unsafe.Sizeof(int(0))) +
			uint64(cap(root.prefix))*uint64(unsafe.Sizeof(int(0))) +
			uint64(cap(root.rowPrefix))*uint64(unsafe.Sizeof(uint64(0))),
	}
	for i := range root.pages {
		page := root.pages[i]
		stats.ApproxStructBytes += uint64(unsafe.Sizeof(fieldIndexChunkDirPage{})) +
			uint64(cap(page.refs))*uint64(unsafe.Sizeof(fieldIndexChunkRef{})) +
			uint64(cap(page.prefix))*uint64(unsafe.Sizeof(int(0))) +
			uint64(cap(page.rowPrefix))*uint64(unsafe.Sizeof(uint64(0)))

		for j := range page.refs {
			chunk := page.refs[j].chunk
			stats.ApproxStructBytes += uint64(unsafe.Sizeof(fieldIndexChunk{}))
			if chunk.hasStringKeys() {
				stats.ApproxStructBytes += uint64(cap(chunk.stringRefs)) * uint64(unsafe.Sizeof(fieldIndexStringRef(0)))
				if chunk.hasUniqueStringOwners() {
					stats.ApproxStructBytes += uint64(cap(chunk.numeric)) * uint64(unsafe.Sizeof(uint64(0)))
				} else {
					stats.ApproxStructBytes += uint64(cap(chunk.posts)) * uint64(unsafe.Sizeof(posting.List{}))
				}
			} else if chunk.hasUniqueNumericOwners() {
				stats.ApproxStructBytes += uint64(cap(chunk.numeric)>>1) * uint64(unsafe.Sizeof(uint64(0)))
			} else {
				stats.ApproxStructBytes += uint64(cap(chunk.posts)) * uint64(unsafe.Sizeof(posting.List{}))
			}
			for k := 0; k < chunk.keyCount(); k++ {
				stats.accumulateEntry(Entry{Key: chunk.keyAt(k), IDs: chunk.postingAt(k)}, countDistinct)
			}
		}
	}
	return stats
}

func (stats *FieldStats) accumulateEntry(entry Entry, countDistinct bool) {
	if entry.IDs.IsEmpty() {
		return
	}
	if countDistinct {
		stats.Unique++
		stats.KeyBytes += uint64(entry.Key.ByteLen())
	}
	stats.PostingBytes += entry.IDs.SizeInBytes()
	stats.PostingCardinality += entry.IDs.Cardinality()
	stats.EntryCount++
}
