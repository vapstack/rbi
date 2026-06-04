package indexdata

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

var benchUint64 uint64
var benchBool bool

const benchWriterSize = 64 << 10

func benchmarkFieldEntries(n int, numeric bool) []Entry {
	entries := make([]Entry, n)
	for i := range entries {
		key := keycodec.FromStoredString(fmt.Sprintf("k/%06d", i), false)
		if numeric {
			key = keycodec.FromU64(uint64(i * 2))
		}
		entries[i] = Entry{
			Key: key,
			IDs: (posting.List{}).BuildAdded(uint64(i + 1)),
		}
	}
	return entries
}

func benchmarkFieldEntriesWithPostingCardinality(n int, numeric bool, card int) []Entry {
	entries := make([]Entry, n)
	ids := make([]uint64, card)
	for i := range entries {
		key := keycodec.FromStoredString(fmt.Sprintf("k/%06d", i), false)
		if numeric {
			key = keycodec.FromU64(uint64(i * 2))
		}
		if card == 1 {
			entries[i] = Entry{
				Key: key,
				IDs: (posting.List{}).BuildAdded(uint64(i + 1)),
			}
			continue
		}
		base := uint64(i*card*3 + 1)
		for j := range ids {
			ids[j] = base + uint64(j*3)
		}
		entries[i] = Entry{
			Key: key,
			IDs: (posting.List{}).BuildAddedMany(ids),
		}
	}
	return entries
}

func benchmarkFieldStorage(rows int, numeric bool) FieldStorage {
	entries := benchmarkFieldEntries(rows, numeric)
	return newRegularFieldStorage(entries)
}

func benchmarkFieldStorageWithPostingCardinality(rows int, numeric bool, card int) FieldStorage {
	entries := benchmarkFieldEntriesWithPostingCardinality(rows, numeric, card)
	return newRegularFieldStorage(entries)
}

func benchmarkMeasureStorage(rows int) MeasureStorage {
	entries := GetMeasureEntrySlice(rows)
	for i := 0; i < rows; i++ {
		entries = append(entries, MeasureEntry{
			ID:    uint64(i*2 + 1),
			Value: uint64(i * 11),
		})
	}
	return NewMeasureStorageFromEntriesOwned(entries)
}

func benchmarkFieldStorageBytes(storage FieldStorage) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := storage.WriteInto(writer); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkMeasureStorageBytes(storage MeasureStorage) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := storage.WriteInto(writer); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkEntryBytes(entry Entry) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := entry.WriteInto(writer); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkKeyBytes(key keycodec.IndexKey) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := writeKey(writer, key); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkStringBytes(s string) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := writeString(writer, s); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkUvarintBytes(v uint64) []byte {
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, benchWriterSize)
	if err := writeUvarint(writer, v); err != nil {
		panic(err)
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func benchmarkFirstFieldChunk(storage FieldStorage) *fieldIndexChunk {
	if storage.chunked == nil || len(storage.chunked.pages) == 0 || len(storage.chunked.pages[0].refs) == 0 {
		panic("benchmark storage is not chunked")
	}
	return storage.chunked.pages[0].refs[0].chunk
}

func benchmarkResetReader(src *bytes.Reader, reader *bufio.Reader, payload []byte) {
	src.Reset(payload)
	reader.Reset(src)
}

func BenchmarkWriteUvarint(b *testing.B) {
	tests := []struct {
		name  string
		value uint64
	}{
		{name: "OneByte", value: 42},
		{name: "TenBytes", value: ^uint64(0)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeUvarint(writer, tc.value); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkWriteString(b *testing.B) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "Short", value: "k/000001"},
		{name: "MaxIndexed", value: strings.Repeat("x", FieldStringRefMax)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeString(writer, tc.value); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkWriteKey(b *testing.B) {
	tests := []struct {
		name string
		key  keycodec.IndexKey
	}{
		{name: "String", key: keycodec.FromStoredString("k/000001", false)},
		{name: "Numeric", key: keycodec.FromU64(42)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeKey(writer, tc.key); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkEntryWriteInto(b *testing.B) {
	multi := (posting.List{}).BuildAddedMany([]uint64{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46})
	defer multi.Release()

	tests := []struct {
		name  string
		entry Entry
	}{
		{
			name: "StringSingleton",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "NumericSingleton",
			entry: Entry{
				Key: keycodec.FromU64(42),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "StringMultiPosting",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: multi,
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := tc.entry.WriteInto(writer); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkWriteEntries(b *testing.B) {
	tests := []struct {
		name    string
		numeric bool
		card    int
	}{
		{name: "StringSingleton", card: 1},
		{name: "NumericSingleton", numeric: true, card: 1},
		{name: "StringMultiPosting", card: 16},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			entries := benchmarkFieldEntriesWithPostingCardinality(FieldChunkTargetEntries, tc.numeric, tc.card)
			defer func() {
				for i := range entries {
					entries[i].IDs.Release()
				}
			}()
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := WriteEntries(writer, entries); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkWriteFieldIndexChunk(b *testing.B) {
	tests := []struct {
		name    string
		numeric bool
		card    int
	}{
		{name: "StringSingleton", card: 1},
		{name: "NumericSingleton", numeric: true, card: 1},
		{name: "StringMultiPosting", card: 16},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			storage := benchmarkFieldStorageWithPostingCardinality(FieldChunkThreshold*2, tc.numeric, tc.card)
			defer storage.Release()
			chunk := benchmarkFirstFieldChunk(storage)
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := chunk.writeInto(writer); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkFieldStorageWriteInto(b *testing.B) {
	tests := []struct {
		name    string
		rows    int
		numeric bool
		card    int
	}{
		{name: "FlatStringSingleton", rows: FieldChunkTargetEntries / 2, card: 1},
		{name: "FlatNumericSingleton", rows: FieldChunkTargetEntries / 2, numeric: true, card: 1},
		{name: "ChunkedStringSingleton", rows: FieldChunkThreshold * 8, card: 1},
		{name: "ChunkedNumericSingleton", rows: FieldChunkThreshold * 8, numeric: true, card: 1},
		{name: "ChunkedStringMultiPosting", rows: FieldChunkThreshold * 8, card: 16},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			storage := benchmarkFieldStorageWithPostingCardinality(tc.rows, tc.numeric, tc.card)
			defer storage.Release()
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := storage.WriteInto(writer); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkMeasureStorageWriteInto(b *testing.B) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: MeasureChunkTargetRows},
		{name: "Chunked", rows: MeasureChunkThreshold * 8},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			storage := benchmarkMeasureStorage(tc.rows)
			defer storage.Release()
			writer := bufio.NewWriterSize(io.Discard, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := storage.WriteInto(writer); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkReadString(b *testing.B) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "Short", value: "k/000001"},
		{name: "MaxIndexed", value: strings.Repeat("x", FieldStringRefMax)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkStringBytes(tc.value)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				s, err := readString(reader)
				if err != nil {
					b.Fatal(err)
				}
				total += len(s)
			}
			benchUint64 = uint64(total)
		})
	}
}

func BenchmarkSkipString(b *testing.B) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "Short", value: "k/000001"},
		{name: "MaxIndexed", value: strings.Repeat("x", FieldStringRefMax)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkStringBytes(tc.value)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				if err := skipString(reader); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadUvarint(b *testing.B) {
	tests := []struct {
		name  string
		value uint64
	}{
		{name: "OneByte", value: 42},
		{name: "TenBytes", value: ^uint64(0)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkUvarintBytes(tc.value)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				v, err := binary.ReadUvarint(reader)
				if err != nil {
					b.Fatal(err)
				}
				total += v
			}
			benchUint64 = total
		})
	}
}

func BenchmarkReadKey(b *testing.B) {
	tests := []struct {
		name string
		key  keycodec.IndexKey
	}{
		{name: "String", key: keycodec.FromStoredString("k/000001", false)},
		{name: "Numeric", key: keycodec.FromU64(42)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkKeyBytes(tc.key)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				key, err := readKey(reader)
				if err != nil {
					b.Fatal(err)
				}
				total += key.ByteLen()
			}
			benchUint64 = uint64(total)
		})
	}
}

func BenchmarkSkipKey(b *testing.B) {
	tests := []struct {
		name string
		key  keycodec.IndexKey
	}{
		{name: "String", key: keycodec.FromStoredString("k/000001", false)},
		{name: "Numeric", key: keycodec.FromU64(42)},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkKeyBytes(tc.key)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				if err := skipKey(reader); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadEntry(b *testing.B) {
	multi := (posting.List{}).BuildAddedMany([]uint64{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46})
	defer multi.Release()

	tests := []struct {
		name  string
		entry Entry
	}{
		{
			name: "StringSingleton",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "NumericSingleton",
			entry: Entry{
				Key: keycodec.FromU64(42),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "StringMultiPosting",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: multi,
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkEntryBytes(tc.entry)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				entry, err := readEntry(reader)
				if err != nil {
					b.Fatal(err)
				}
				total += entry.IDs.Cardinality()
				entry.IDs.Release()
			}
			benchUint64 = total
		})
	}
}

func BenchmarkSkipEntry(b *testing.B) {
	multi := (posting.List{}).BuildAddedMany([]uint64{1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46})
	defer multi.Release()

	tests := []struct {
		name  string
		entry Entry
	}{
		{
			name: "StringSingleton",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "NumericSingleton",
			entry: Entry{
				Key: keycodec.FromU64(42),
				IDs: (posting.List{}).BuildAdded(1),
			},
		},
		{
			name: "StringMultiPosting",
			entry: Entry{
				Key: keycodec.FromStoredString("k/000001", false),
				IDs: multi,
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			payload := benchmarkEntryBytes(tc.entry)
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				if err := skipEntry(reader); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadFieldStorage(b *testing.B) {
	tests := []struct {
		name    string
		rows    int
		numeric bool
		card    int
		keep    bool
	}{
		{name: "FlatStringSingletonKeep", rows: FieldChunkTargetEntries / 2, card: 1, keep: true},
		{name: "FlatStringSingletonSkip", rows: FieldChunkTargetEntries / 2, card: 1},
		{name: "ChunkedStringSingletonKeep", rows: FieldChunkThreshold * 8, card: 1, keep: true},
		{name: "ChunkedStringSingletonSkip", rows: FieldChunkThreshold * 8, card: 1},
		{name: "ChunkedNumericSingletonKeep", rows: FieldChunkThreshold * 8, numeric: true, card: 1, keep: true},
		{name: "ChunkedStringMultiPostingKeep", rows: FieldChunkThreshold * 8, card: 16, keep: true},
		{name: "ChunkedStringMultiPostingSkip", rows: FieldChunkThreshold * 8, card: 16},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			source := benchmarkFieldStorageWithPostingCardinality(tc.rows, tc.numeric, tc.card)
			payload := benchmarkFieldStorageBytes(source)
			source.Release()
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				storage, err := ReadFieldStorage(reader, tc.keep, "bench", "field")
				if err != nil {
					b.Fatal(err)
				}
				total += storage.KeyCount()
				storage.Release()
			}
			benchUint64 = uint64(total)
		})
	}
}

func BenchmarkReadMeasureStorage(b *testing.B) {
	tests := []struct {
		name string
		rows int
		keep bool
	}{
		{name: "FlatKeep", rows: MeasureChunkTargetRows, keep: true},
		{name: "FlatSkip", rows: MeasureChunkTargetRows},
		{name: "ChunkedKeep", rows: MeasureChunkThreshold * 8, keep: true},
		{name: "ChunkedSkip", rows: MeasureChunkThreshold * 8},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			source := benchmarkMeasureStorage(tc.rows)
			payload := benchmarkMeasureStorageBytes(source)
			source.Release()
			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, benchWriterSize)
			b.ReportAllocs()
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				benchmarkResetReader(&src, reader, payload)
				storage, err := ReadMeasureStorage(reader, tc.keep)
				if err != nil {
					b.Fatal(err)
				}
				total += storage.Rows()
				storage.Release()
			}
			benchUint64 = uint64(total)
		})
	}
}

func BenchmarkFieldIndexViewLookupFlat(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkTargetEntries/2, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	if storage.IsChunked() {
		b.Fatalf("flat benchmark built chunked storage")
	}
	ov := NewFieldIndexViewFromStorage(storage)
	key := entries[len(entries)/2].Key.UnsafeString()

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		total += ov.LookupCardinality(key)
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewLookupChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	key := entries[len(entries)/2].Key.UnsafeString()

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		total += ov.LookupCardinality(key)
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewCursorChunkedRange(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	br := ov.RangeByRanks(FieldChunkTargetEntries, FieldChunkTargetEntries*3)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		cur := ov.NewCursor(br, false)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			total += ids.Cardinality()
		}
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewCursorFlatRange(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkTargetEntries/2, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	if storage.IsChunked() {
		b.Fatalf("flat benchmark built chunked storage")
	}
	ov := NewFieldIndexViewFromStorage(storage)
	br := ov.RangeByRanks(16, FieldChunkTargetEntries/2-16)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		cur := ov.NewCursor(br, false)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			total += ids.Cardinality()
		}
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewCursorChunkedRangeDesc(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	br := ov.RangeByRanks(FieldChunkTargetEntries, FieldChunkTargetEntries*3)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		cur := ov.NewCursor(br, true)
		for {
			_, ids, ok := cur.Next()
			if !ok {
				break
			}
			total += ids.Cardinality()
		}
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewRangeForBounds(b *testing.B) {
	tests := []struct {
		name    string
		rows    int
		numeric bool
	}{
		{name: "FlatString", rows: FieldChunkTargetEntries / 2},
		{name: "ChunkedString", rows: FieldChunkThreshold * 8},
		{name: "FlatNumeric", rows: FieldChunkTargetEntries / 2, numeric: true},
		{name: "ChunkedNumeric", rows: FieldChunkThreshold * 8, numeric: true},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			entries := benchmarkFieldEntries(tc.rows, tc.numeric)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)
			rank := len(entries) / 2
			bounds := Bounds{
				HasLo: true,
				LoKey: fmt.Sprintf("k/%06d", rank),
			}
			if tc.numeric {
				bounds.LoNumeric = true
				bounds.LoIndex = keycodec.FromU64(uint64(rank * 2))
			}

			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				br := ov.RangeForBounds(bounds)
				total += uint64(br.BaseStart + br.BaseEnd)
			}
			benchUint64 = total
		})
	}
}

func BenchmarkFieldIndexViewRangeStats(b *testing.B) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: FieldChunkTargetEntries / 2},
		{name: "Chunked", rows: FieldChunkThreshold * 8},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			entries := benchmarkFieldEntriesWithPostingCardinality(tc.rows, false, 4)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)
			br := ov.RangeByRanks(tc.rows/4, tc.rows*3/4)

			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				buckets, rows := ov.RangeStats(br)
				total += uint64(buckets) + rows
			}
			benchUint64 = total
		})
	}
}

func BenchmarkBoundsApply(b *testing.B) {
	b.Run("StringRangePrefix", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			var bounds Bounds
			bounds.ApplyLo("k/000010", true)
			bounds.ApplyLo("k/000020", false)
			bounds.ApplyHi("k/000100", true)
			bounds.ApplyHi("k/000090", false)
			bounds.ApplyPrefix("k/000")
			if bounds.Empty {
				total++
			} else {
				total += uint64(len(bounds.LoKey) + len(bounds.HiKey) + len(bounds.Prefix))
			}
		}
		benchUint64 = total
	})

	b.Run("NumericRange", func(b *testing.B) {
		loA := keycodec.FromU64(10)
		loB := keycodec.FromU64(20)
		hiA := keycodec.FromU64(100)
		hiB := keycodec.FromU64(90)
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			var bounds Bounds
			bounds.ApplyLoIndex(loA, true)
			bounds.ApplyLoIndex(loB, false)
			bounds.ApplyHiIndex(hiA, true)
			bounds.ApplyHiIndex(hiB, false)
			if bounds.Empty {
				total++
			} else {
				total += bounds.LoIndex.U64() + bounds.HiIndex.U64()
			}
		}
		benchUint64 = total
	})
}

func BenchmarkFieldIndexViewLookupPostingsChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	keys := make([]string, 0, 64)
	base := len(entries) / 3
	for i := 0; i < 64; i++ {
		keys = append(keys, entries[base+i*3].Key.UnsafeString())
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		posts, est := ov.LookupPostings(keys)
		total += est + uint64(len(posts))
		posting.ReleaseSlice(posts)
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewUnionRangePostingsChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	first := ov.RangeByRanks(FieldChunkTargetEntries, FieldChunkTargetEntries*3)
	second := ov.RangeByRanks(FieldChunkTargetEntries*2, FieldChunkTargetEntries*4)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		ids := ov.UnionRangePostings(first, second)
		total += ids.Cardinality()
		ids.Release()
	}
	benchUint64 = total
}

func BenchmarkFieldIndexViewMergeRangePostingsChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)
	first := ov.RangeByRanks(FieldChunkTargetEntries, FieldChunkTargetEntries*3)
	second := ov.RangeByRanks(FieldChunkTargetEntries*2, FieldChunkTargetEntries*4)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		ids := ov.MergeRangePostingsInto((posting.List{}).BuildAdded(uint64(i+1)<<48), first, second)
		total += ids.Cardinality()
		ids.Release()
	}
	benchUint64 = total
}

func BenchmarkFieldStorageBuilderChunked(b *testing.B) {
	tests := []struct {
		name    string
		numeric bool
	}{
		{name: "String"},
		{name: "Numeric", numeric: true},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			const rows = FieldChunkThreshold * 4
			entries := benchmarkFieldEntries(rows, tc.numeric)
			source := newRegularFieldStorage(entries)
			defer source.Release()
			ov := NewFieldIndexViewFromStorage(source)
			keys := make([]keycodec.IndexKey, rows)
			posts := posting.GetSlice(rows)
			for i := 0; i < rows; i++ {
				keys[i] = ov.KeyAt(i)
				posts = append(posts, ov.PostingAt(i))
			}
			defer posting.ReleaseSlice(posts)

			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				var builder fieldStorageBuilder
				builder.init(rows, tc.numeric)
				for j := 0; j < rows; j++ {
					builder.append(keys[j], posts[j])
				}
				storage := builder.finish()
				total += uint64(storage.KeyCount())
				storage.Release()
			}
			benchUint64 = total
		})
	}
}

func BenchmarkNewRegularFieldStorageFromFixedPostingMapOwnedChunked(b *testing.B) {
	const rows = FieldChunkThreshold * 4

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := GetFixedPostingMap()
		for j := 0; j < rows; j++ {
			m[uint64(j*2)] = (posting.List{}).BuildAdded(uint64(j + 1))
		}
		b.StartTimer()

		storage := NewRegularFieldStorageFromFixedPostingMapOwned(m)
		total += uint64(storage.KeyCount())

		b.StopTimer()
		storage.Release()
		b.StartTimer()
	}
	benchUint64 = total
}

func BenchmarkNewRegularFieldStorageFromPostingMapOwnedChunked(b *testing.B) {
	const rows = FieldChunkThreshold * 4

	tests := []struct {
		name   string
		fixed8 bool
	}{
		{name: "String"},
		{name: "Fixed8", fixed8: true},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := GetPostingMap()
				for j := 0; j < rows; j++ {
					key := fmt.Sprintf("k/%06d", j*2)
					if tc.fixed8 {
						key = fmt.Sprintf("%08d", j*2)
					}
					m[key] = (posting.List{}).BuildAdded(uint64(j + 1))
				}
				b.StartTimer()

				storage := NewRegularFieldStorageFromPostingMapOwned(m, tc.fixed8)
				total += uint64(storage.KeyCount())

				b.StopTimer()
				storage.Release()
				b.StartTimer()
			}
			benchUint64 = total
		})
	}
}

func BenchmarkNewRegularFieldStorageFromRunsOwned(b *testing.B) {
	const (
		rows    = FieldChunkThreshold * 2
		runSize = 4
	)

	b.Run("String", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			runs := make([]FieldStorageRun, runSize)
			for r := 0; r < runSize; r++ {
				m := GetPostingMap()
				base := uint64(r*rows + 1)
				for j := 0; j < rows; j++ {
					m[fmt.Sprintf("k/%06d", j)] = (posting.List{}).BuildAdded(base + uint64(j))
				}
				runs[r] = NewStringFieldStorageRunFromPostingMap(m)
				ReleasePostingMap(m)
			}
			b.StartTimer()

			storage := NewRegularFieldStorageFromRunsOwned(runs)
			total += uint64(storage.KeyCount())

			b.StopTimer()
			storage.Release()
			b.StartTimer()
		}
		benchUint64 = total
	})

	b.Run("Fixed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			runs := make([]FieldStorageRun, runSize)
			for r := 0; r < runSize; r++ {
				m := GetFixedPostingMap()
				base := uint64(r*rows + 1)
				for j := 0; j < rows; j++ {
					m[uint64(j*2)] = (posting.List{}).BuildAdded(base + uint64(j))
				}
				runs[r] = NewFixedFieldStorageRunFromPostingMap(m)
				ReleaseFixedPostingMap(m)
			}
			b.StartTimer()

			storage := NewRegularFieldStorageFromRunsOwned(runs)
			total += uint64(storage.KeyCount())

			b.StopTimer()
			storage.Release()
			b.StartTimer()
		}
		benchUint64 = total
	})
}

func BenchmarkFieldStorageRunFromPostingMap(b *testing.B) {
	const rows = FieldChunkThreshold * 4

	b.Run("String", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := GetPostingMap()
			for j := 0; j < rows; j++ {
				m[fmt.Sprintf("k/%06d", j*2)] = (posting.List{}).BuildAdded(uint64(j + 1))
			}
			b.StartTimer()

			run := NewStringFieldStorageRunFromPostingMap(m)
			total += uint64(run.KeyCount())

			b.StopTimer()
			run.ReleaseOwned()
			ReleasePostingMap(m)
			b.StartTimer()
		}
		benchUint64 = total
	})

	b.Run("Fixed", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := GetFixedPostingMap()
			for j := 0; j < rows; j++ {
				m[uint64(j*2)] = (posting.List{}).BuildAdded(uint64(j + 1))
			}
			b.StartTimer()

			run := NewFixedFieldStorageRunFromPostingMap(m)
			total += uint64(run.KeyCount())

			b.StopTimer()
			run.ReleaseOwned()
			ReleaseFixedPostingMap(m)
			b.StartTimer()
		}
		benchUint64 = total
	})
}

func BenchmarkNewNilFieldStorageOwned(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		ids := fieldStoragePosting(1, 3, 5, uint64(i+10))
		storage := NewNilFieldStorageOwned(ids)
		total += uint64(storage.KeyCount())
		storage.Release()
	}
	benchUint64 = total
}

func BenchmarkNewLenFieldStorageFromMapOwned(b *testing.B) {
	const rows = 16_384

	ids := make([]uint64, rows)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	universe := posting.BuildFromSorted(ids)
	defer universe.Release()

	var base [4]posting.List
	for i := 1; i <= rows; i++ {
		ln := uint32(0)
		switch {
		case i%23 == 0:
			ln = 3
		case i%11 == 0:
			ln = 2
		case i%7 == 0:
			ln = 1
		}
		base[ln] = base[ln].BuildAdded(uint64(i))
	}
	defer func() {
		for i := range base {
			base[i].Release()
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	var used bool
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lengths := lenPostingMapPool.Get()
		for ln := range base {
			lengths[uint32(ln)] = base[ln].Clone()
		}
		b.StartTimer()

		storage, useZeroComplement := NewLenFieldStorageFromMapOwned(universe, lengths)
		total += uint64(storage.KeyCount())
		used = used || useZeroComplement

		b.StopTimer()
		storage.Release()
		lenPostingMapPool.Put(lengths)
		b.StartTimer()
	}
	benchUint64 = total
	benchBool = used
}

func BenchmarkRebuildLenFieldStorageFromIndexView(b *testing.B) {
	const rows = 16_384
	const values = FieldChunkThreshold * 4

	ids := make([]uint64, rows)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	universe := posting.BuildFromSorted(ids)
	defer universe.Release()

	posts := make([]posting.List, values)
	for id := 1; id <= rows; id++ {
		if id%5 == 0 {
			continue
		}
		u64 := uint64(id)
		idx := id % values
		posts[idx] = posts[idx].BuildAdded(u64)
		if id%3 == 0 {
			posts[(idx+97)%values] = posts[(idx+97)%values].BuildAdded(u64)
		}
		if id%11 == 0 {
			posts[(idx+211)%values] = posts[(idx+211)%values].BuildAdded(u64)
		}
	}
	entries := make([]Entry, values)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromU64(uint64(i)),
			IDs: posts[i],
		}
	}
	base := newRegularFieldStorage(entries)
	defer base.Release()
	ov := NewFieldIndexViewFromStorage(base)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	var used bool
	for i := 0; i < b.N; i++ {
		storage, useZeroComplement := RebuildLenFieldStorageFromIndexView(universe, ov)
		total += uint64(storage.KeyCount())
		used = used || useZeroComplement

		b.StopTimer()
		storage.Release()
		b.StartTimer()
	}
	benchUint64 = total
	benchBool = used
}

func BenchmarkSortPostingDeltasBuf(b *testing.B) {
	const deltaCount = 256
	keys := make([]keycodec.IndexKey, deltaCount)
	for i := 0; i < deltaCount; i++ {
		keys[i] = keycodec.FromU64(uint64((deltaCount - i) * 2))
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		buf := GetPostingDeltaSlice(deltaCount)
		for j := 0; j < deltaCount; j++ {
			buf = append(buf, PostingDelta{Key: keys[j]})
		}
		sortPostingDeltasBuf(buf)
		total += buf[0].Key.U64()
		ReleasePostingDeltaSlice(buf)
	}
	benchUint64 = total
}

func BenchmarkFieldStorageStats(b *testing.B) {
	tests := []struct {
		name          string
		rows          int
		numeric       bool
		card          int
		countDistinct bool
	}{
		{name: "FlatString", rows: FieldChunkTargetEntries / 2, card: 1, countDistinct: true},
		{name: "ChunkedString", rows: FieldChunkThreshold * 8, card: 1, countDistinct: true},
		{name: "ChunkedNumeric", rows: FieldChunkThreshold * 8, numeric: true, card: 1, countDistinct: true},
		{name: "ChunkedStringMultiPosting", rows: FieldChunkThreshold * 8, card: 16, countDistinct: true},
		{name: "ChunkedStringCardinalityOnly", rows: FieldChunkThreshold * 8, card: 16},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			storage := benchmarkFieldStorageWithPostingCardinality(tc.rows, tc.numeric, tc.card)
			defer storage.Release()

			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				stats := storage.Stats(tc.countDistinct)
				total += stats.EntryCount + stats.PostingCardinality + stats.KeyBytes
			}
			benchUint64 = total
		})
	}
}

func BenchmarkFieldStorageKeyCount(b *testing.B) {
	tests := []struct {
		name    string
		rows    int
		numeric bool
	}{
		{name: "Empty"},
		{name: "FlatString", rows: FieldChunkTargetEntries / 2},
		{name: "ChunkedString", rows: FieldChunkThreshold * 8},
		{name: "ChunkedNumeric", rows: FieldChunkThreshold * 8, numeric: true},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			var storage FieldStorage
			if tc.rows > 0 {
				storage = benchmarkFieldStorage(tc.rows, tc.numeric)
				defer storage.Release()
			}

			b.ReportAllocs()
			b.ResetTimer()
			var total uint64
			for i := 0; i < b.N; i++ {
				total += uint64(storage.KeyCount())
			}
			benchUint64 = total
		})
	}
}

func BenchmarkFieldStorageSlotsLifecycle(b *testing.B) {
	const fieldCount = 32

	prev := GetFieldStorageSlice(fieldCount)[:fieldCount]
	for i := 0; i < fieldCount; i++ {
		prev[i] = benchmarkFieldStorage(FieldChunkThreshold*2, i&1 == 0)
	}
	defer ReleaseFieldStorageSlots(prev)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		slots := CloneFieldStorageSlots(prev, fieldCount)
		RetainSharedFieldStorageSlots(slots, prev)
		total += uint64(len(slots))
		ReleaseFieldStorageSlots(slots)
	}
	benchUint64 = total
}

func BenchmarkApplySingleFieldPostingDiffChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	key := keycodec.FromU64(uint64((len(entries) / 2) * 2))

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		next := storage.applyPostingDiff([]PostingDelta{{
			Key: key,
			Delta: BatchPostingDelta{
				Add: (posting.List{}).BuildAdded(uint64(10_000_000 + i)),
			},
		}}, true)
		total += next.KeyCount()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkApplyFieldPostingDiffStorageBufOwnedFlat(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkTargetEntries/2, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	const deltaCount = 8
	keys := make([]keycodec.IndexKey, deltaCount)
	base := len(entries) / 2
	for i := 0; i < deltaCount; i++ {
		keys[i] = keycodec.FromU64(uint64((base + i) * 2))
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		buf := GetPostingDeltaSlice(deltaCount)
		for j := 0; j < deltaCount; j++ {
			buf = append(buf, PostingDelta{
				Key: keys[j],
				Delta: BatchPostingDelta{
					Add: (posting.List{}).BuildAdded(20_000_000 + uint64(i)*deltaCount + uint64(j)),
				},
			})
		}
		next := storage.applyPostingDiffBufOwned(buf, false)
		total += next.KeyCount()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkApplyFieldPostingDiffStorageBufOwnedChunked(b *testing.B) {
	entries := benchmarkFieldEntries(FieldChunkThreshold*8, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	const deltaCount = 64
	keys := make([]keycodec.IndexKey, deltaCount)
	base := len(entries) / 2
	for i := 0; i < deltaCount; i++ {
		keys[i] = keycodec.FromU64(uint64((base + i) * 2))
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		buf := GetPostingDeltaSlice(deltaCount)
		for j := 0; j < deltaCount; j++ {
			buf = append(buf, PostingDelta{
				Key: keys[j],
				Delta: BatchPostingDelta{
					Add: (posting.List{}).BuildAdded(10_000_000 + uint64(i)*deltaCount + uint64(j)),
				},
			})
		}
		next := storage.applyPostingDiffBufOwned(buf, true)
		total += next.KeyCount()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkFieldStorageMergeStringPostingAdds(b *testing.B) {
	const deltaCount = 128
	keys := make([]string, deltaCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("k/%06d", i*2)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	var arena PostingAddArena
	var adds map[string]uint32
	for i := 0; i < b.N; i++ {
		for j := 0; j < deltaCount; j++ {
			adds = AddStringPostingAdd(adds, &arena, keys[j], uint64(i*deltaCount+j+1), deltaCount)
		}
		storage := FieldStorage{}.MergeStringPostingAdds(adds, &arena, false, true)
		total += storage.KeyCount()
		storage.Release()
		clear(adds)
		arena.Reset()
	}
	benchUint64 = uint64(total)
}

func BenchmarkFieldStorageMergeFixedPostingAdds(b *testing.B) {
	const deltaCount = 128
	keys := make([]uint64, deltaCount)
	for i := range keys {
		keys[i] = uint64(i * 2)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	var arena PostingAddArena
	var adds map[uint64]uint32
	for i := 0; i < b.N; i++ {
		for j := 0; j < deltaCount; j++ {
			adds = AddFixedPostingAdd(adds, &arena, keys[j], uint64(i*deltaCount+j+1), deltaCount)
		}
		storage := FieldStorage{}.MergeFixedPostingAdds(adds, &arena, true)
		total += storage.KeyCount()
		storage.Release()
		clear(adds)
		arena.Reset()
	}
	benchUint64 = uint64(total)
}

func BenchmarkFieldStorageApplyStringPostingDiff(b *testing.B) {
	const rows = FieldChunkThreshold * 4
	const deltaCount = 64

	entries := benchmarkFieldEntries(rows, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	base := rows / 2
	keys := make([]string, deltaCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("k/%06d", base+i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	var arena PostingDiffArena
	var deltas map[string]uint32
	for i := 0; i < b.N; i++ {
		for j := 0; j < deltaCount; j++ {
			deltas = AddStringPostingDiff(deltas, &arena, keys[j], uint64(base+j+1), false)
			deltas = AddStringPostingDiff(deltas, &arena, keys[j], uint64(10_000_000+i*deltaCount+j), true)
		}
		next := storage.ApplyStringPostingDiff(deltas, &arena, false, true)
		total += next.KeyCount()
		next.Release()
		clear(deltas)
		arena.Reset()
	}
	benchUint64 = uint64(total)
}

func BenchmarkFieldStorageApplyLenPostingDiff(b *testing.B) {
	const rows = 512
	const deltaCount = 64

	lenOne := posting.List{}
	nonEmpty := posting.List{}
	for i := uint64(1); i <= rows; i++ {
		lenOne = lenOne.BuildAdded(i)
		nonEmpty = nonEmpty.BuildAdded(i)
	}
	entries := []Entry{
		{Key: keycodec.FromU64(1), IDs: lenOne},
		{Key: keycodec.FromString(LenIndexNonEmptyKey), IDs: nonEmpty},
	}
	storage := newRegularFieldStorage(entries)
	defer storage.Release()

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	var deltas *LenPostingDiff
	for i := 0; i < b.N; i++ {
		base := uint64(rows + i*deltaCount + 1)
		for j := 0; j < deltaCount; j++ {
			id := base + uint64(j)
			deltas = AddLenPostingBucket(deltas, id, 1, true)
			deltas = AddLenPostingNonEmpty(deltas, id, true)
		}
		next := storage.ApplyLenPostingDiff(deltas)
		total += next.KeyCount()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkMeasureStorageBuildChunked(b *testing.B) {
	const rows = MeasureChunkThreshold * 8

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		entries := GetMeasureEntrySlice(rows)
		for j := rows - 1; j >= 0; j-- {
			entries = append(entries, MeasureEntry{
				ID:    uint64(j*2 + 1),
				Value: uint64(j * 11),
			})
		}
		storage := NewMeasureStorageFromEntriesOwned(entries)
		total += uint64(storage.Rows())
		storage.Release()
	}
	benchUint64 = total
}

func BenchmarkMeasureStorageLookupFlat(b *testing.B) {
	storage := benchmarkMeasureStorage(MeasureChunkTargetRows)
	defer storage.Release()
	key := uint64(MeasureChunkTargetRows/2*2 + 1)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	var found bool
	for i := 0; i < b.N; i++ {
		value, ok := storage.Lookup(key)
		total += value
		found = found || ok
	}
	benchUint64 = total
	benchBool = found
}

func BenchmarkMeasureStorageLookupChunked(b *testing.B) {
	storage := benchmarkMeasureStorage(MeasureChunkThreshold * 8)
	defer storage.Release()
	key := uint64(MeasureChunkThreshold*4*2 + 1)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	var found bool
	for i := 0; i < b.N; i++ {
		value, ok := storage.Lookup(key)
		total += value
		found = found || ok
	}
	benchUint64 = total
	benchBool = found
}

func BenchmarkMeasureStorageAggregateAccessors(b *testing.B) {
	b.Run("Flat", func(b *testing.B) {
		storage := benchmarkMeasureStorage(MeasureChunkTargetRows)
		defer storage.Release()

		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		var found bool
		for i := 0; i < b.N; i++ {
			ids, values, ok := storage.FlatSlices()
			total += uint64(storage.Rows()) + storage.LookupSteps() + uint64(len(ids)+len(values))
			found = found || ok
		}
		benchUint64 = total
		benchBool = found
	})

	b.Run("Chunked", func(b *testing.B) {
		storage := benchmarkMeasureStorage(MeasureChunkThreshold * 8)
		defer storage.Release()

		b.ReportAllocs()
		b.ResetTimer()
		var total uint64
		for i := 0; i < b.N; i++ {
			total += uint64(storage.Rows()+storage.ChunkCount()) + storage.LookupSteps()
			for chunkPos := 0; chunkPos < storage.ChunkCount(); chunkPos++ {
				ids, values := storage.ChunkSlices(chunkPos)
				total += uint64(len(ids) + len(values))
			}
		}
		benchUint64 = total
	})
}

func BenchmarkApplyMeasureDeltasFlat(b *testing.B) {
	storage := benchmarkMeasureStorage(MeasureChunkTargetRows)
	defer storage.Release()
	key := uint64(MeasureChunkTargetRows/2*2 + 1)

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		deltas := GetMeasureDeltaSlice(0)
		deltas = append(deltas, MeasureDelta{
			ID:    key,
			NewOK: true,
			New:   uint64(i + 1),
		})
		next := storage.ApplyDeltasOwned(deltas)
		total += next.Rows()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkApplyMeasureDeltasChunked(b *testing.B) {
	storage := benchmarkMeasureStorage(MeasureChunkThreshold * 8)
	defer storage.Release()
	key := uint64(MeasureChunkThreshold*4*2 + 1)

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		deltas := GetMeasureDeltaSlice(0)
		deltas = append(deltas, MeasureDelta{
			ID:    key,
			NewOK: true,
			New:   uint64(i + 1),
		})
		next := storage.ApplyDeltasOwned(deltas)
		total += next.Rows()
		next.Release()
	}
	benchUint64 = uint64(total)
}

func BenchmarkMeasureDeltaBatchApply(b *testing.B) {
	const fieldCount = 8
	const deltaCount = 4

	prev := GetMeasureStorageSlice(fieldCount)[:fieldCount]
	for i := 0; i < fieldCount; i++ {
		prev[i] = benchmarkMeasureStorage(MeasureChunkTargetRows)
	}
	defer ReleaseMeasureStorageSlots(prev)

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		slots := CloneMeasureStorageSlots(prev, fieldCount)
		deltas := NewMeasureDeltaBatch(fieldCount)
		base := uint64(i * fieldCount * deltaCount)
		for field := 0; field < fieldCount; field++ {
			for delta := 0; delta < deltaCount; delta++ {
				deltas.Append(field, uint64(delta*2+1), true, base+uint64(field*deltaCount+delta+1))
			}
		}
		deltas.ApplyToMeasureStorageSlotsOwned(slots)
		total += slots[0].Rows()
		deltas.Release()
		ReleaseMeasureStorageSlots(slots)
	}
	benchUint64 = uint64(total)
}

func BenchmarkMeasureStorageSlotsLifecycle(b *testing.B) {
	const fieldCount = 32

	prev := GetMeasureStorageSlice(fieldCount)[:fieldCount]
	for i := 0; i < fieldCount; i++ {
		prev[i] = benchmarkMeasureStorage(MeasureChunkThreshold * 4)
	}
	defer ReleaseMeasureStorageSlots(prev)

	b.ReportAllocs()
	b.ResetTimer()
	var total uint64
	for i := 0; i < b.N; i++ {
		slots := CloneMeasureStorageSlots(prev, fieldCount)
		RetainSharedMeasureStorageSlots(slots, prev)
		total += uint64(len(slots))
		ReleaseMeasureStorageSlots(slots)
	}
	benchUint64 = total
}

func BenchmarkMeasureDeltaBatchAppendRelease(b *testing.B) {
	const fieldCount = 16
	const deltaCount = 8

	b.ReportAllocs()
	b.ResetTimer()
	var total int
	for i := 0; i < b.N; i++ {
		deltas := NewMeasureDeltaBatch(fieldCount)
		base := uint64(i * fieldCount * deltaCount)
		for field := 0; field < fieldCount; field++ {
			for delta := 0; delta < deltaCount; delta++ {
				deltas.Append(field, base+uint64(field*deltaCount+delta+1), true, uint64(delta+1))
			}
		}
		total += len(deltas.touched)
		deltas.Release()
	}
	benchUint64 = uint64(total)
}
