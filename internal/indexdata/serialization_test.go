package indexdata

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func TestFieldIndexChunk_StringSingletonRoundTripUsesOwnerLayout(t *testing.T) {
	keys := []keycodec.IndexKey{
		keycodec.FromString("alpha"),
		keycodec.FromString("beta"),
	}
	posts := []posting.List{
		fieldStorageSingleton(11),
		fieldStorageSingleton(22),
	}
	chunk := newFieldIndexChunkFromKeys(posts, keys, 2)
	if chunk == nil {
		t.Fatalf("expected string chunk")
	}
	if !chunk.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout before serialization")
	}

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := chunk.writeInto(writer); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush chunk: %v", err)
	}

	roundTrip, err := readFieldIndexChunk(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if roundTrip == nil {
		t.Fatalf("expected round-trip chunk")
	}
	if !roundTrip.hasUniqueStringOwners() {
		t.Fatalf("expected owner layout after serialization round-trip")
	}
	if got := roundTrip.keyAt(0).UnsafeString(); got != "alpha" {
		t.Fatalf("unexpected first Key: got %q want %q", got, "alpha")
	}
	if ids := roundTrip.postingAt(1); ids.Cardinality() != 1 || !ids.Contains(22) {
		t.Fatalf("unexpected round-trip posting: %v", ids)
	}
}

func TestFieldIndexChunk_NumericSingletonRoundTripUsesOwnerLayout(t *testing.T) {
	chunk := newNumericFieldIndexChunk([]posting.List{
		fieldStorageSingleton(11),
		fieldStorageSingleton(22),
	}, []uint64{111, 222}, 2)
	if chunk == nil {
		t.Fatalf("expected numeric chunk")
	}
	if !chunk.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout before serialization")
	}

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := chunk.writeInto(writer); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush chunk: %v", err)
	}

	roundTrip, err := readFieldIndexChunk(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if roundTrip == nil {
		t.Fatalf("expected round-trip chunk")
	}
	if !roundTrip.hasUniqueNumericOwners() {
		t.Fatalf("expected owner layout after serialization round-trip")
	}
	if got := roundTrip.keyAt(0).U64(); got != 111 {
		t.Fatalf("unexpected first Key: got %d want 111", got)
	}
	if ids := roundTrip.postingAt(1); ids.Cardinality() != 1 || !ids.Contains(22) {
		t.Fatalf("unexpected round-trip posting: %v", ids)
	}
}

func TestReadIndexKeyRejectsTooLongString(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writer.WriteByte(indexKeyEncodingString); err != nil {
		t.Fatalf("write tag: %v", err)
	}
	if err := writeString(writer, strings.Repeat("x", fieldIndexStringRefMax+1)); err != nil {
		t.Fatalf("write string: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readKey(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err == nil {
		t.Fatalf("expected indexed string validation error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoredStringSerializationRejectsStoredStringLimit(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writeUvarint(writer, MaxStoredStringLen+1); err != nil {
		t.Fatalf("write len: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, err := readString(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err == nil {
		t.Fatalf("expected readString length error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected readString error: %v", err)
	}

	err = skipString(bufio.NewReader(bytes.NewReader(raw.Bytes())))
	if err == nil {
		t.Fatalf("expected skipString length error")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("unexpected skipString error: %v", err)
	}
}

func TestFieldStorageSerializationRoundTrip_FlatAndChunked(t *testing.T) {
	fixedFlat := GetFixedPostingMap()
	fixedFlat[3] = fieldStorageOwnedTestPosting(30)
	fixedFlat[9] = fieldStorageOwnedTestPosting(90)
	flat := NewRegularFieldStorageFromFixedPostingMapOwned(fixedFlat)
	if flat.IsChunked() {
		t.Fatalf("expected flat storage")
	}
	flatRound, err := fieldStorageRoundTripForTest(flat)
	if err != nil {
		flat.Release()
		t.Fatalf("flat round trip: %v", err)
	}
	if flatRound.IsChunked() {
		flat.Release()
		flatRound.Release()
		t.Fatalf("expected flat storage after round trip")
	}
	fieldStorageAssertPostingContainsKey(t, flatRound, keycodec.FromU64(3), 30, 1_000_030)
	flat.Release()
	flatRound.Release()

	stringMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold+17; i++ {
		stringMap[fmt.Sprintf("k/%04d", i)] = fieldStorageOwnedTestPosting(uint64(i + 1))
	}
	chunkedString := NewRegularFieldStorageFromPostingMapOwned(stringMap)
	if !chunkedString.IsChunked() {
		t.Fatalf("expected string storage to be chunked")
	}
	stringRound, err := fieldStorageRoundTripForTest(chunkedString)
	if err != nil {
		chunkedString.Release()
		t.Fatalf("string chunked round trip: %v", err)
	}
	if !stringRound.IsChunked() {
		chunkedString.Release()
		stringRound.Release()
		t.Fatalf("expected string storage to stay chunked")
	}
	stringProbe := FieldChunkTargetEntries
	fieldStorageAssertPostingContains(
		t,
		stringRound,
		fmt.Sprintf("k/%04d", stringProbe),
		uint64(stringProbe+1),
		uint64(stringProbe+1_000_001),
	)
	chunkedString.Release()
	stringRound.Release()

	fixedChunked := GetFixedPostingMap()
	for i := 0; i < FieldChunkThreshold+23; i++ {
		fixedChunked[uint64(i*2)] = fieldStorageOwnedTestPosting(uint64(i + 10_000))
	}
	chunkedNumeric := NewRegularFieldStorageFromFixedPostingMapOwned(fixedChunked)
	if !chunkedNumeric.IsChunked() {
		t.Fatalf("expected numeric storage to be chunked")
	}
	numericRound, err := fieldStorageRoundTripForTest(chunkedNumeric)
	if err != nil {
		chunkedNumeric.Release()
		t.Fatalf("numeric chunked round trip: %v", err)
	}
	if !numericRound.IsChunked() {
		chunkedNumeric.Release()
		numericRound.Release()
		t.Fatalf("expected numeric storage to stay chunked")
	}
	numericProbe := uint64(FieldChunkTargetEntries * 2)
	fieldStorageAssertPostingContainsKey(t, numericRound, keycodec.FromU64(numericProbe), 10_000+uint64(FieldChunkTargetEntries), 1_010_000+uint64(FieldChunkTargetEntries))
	chunkedNumeric.Release()
	numericRound.Release()
}

func TestFieldStorageReadCopiesInputBufferStringKeys(t *testing.T) {
	flatMap := GetPostingMap()
	flatMap["ser-flat/a"] = fieldStorageSingleton(1)
	flatMap["ser-flat/b"] = fieldStorageSingleton(2)
	flat := NewRegularFieldStorageFromPostingMapOwned(flatMap)
	if flat.IsChunked() {
		flat.Release()
		t.Fatalf("expected flat string storage")
	}

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := flat.WriteInto(writer); err != nil {
		flat.Release()
		t.Fatalf("write flat storage: %v", err)
	}
	if err := writer.Flush(); err != nil {
		flat.Release()
		t.Fatalf("flush flat storage: %v", err)
	}
	flatRound, err := ReadFieldStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true, "test", "field")
	if err != nil {
		flat.Release()
		t.Fatalf("read flat storage: %v", err)
	}
	flat.Release()
	poisonBytes(raw.Bytes())
	fieldStorageAssertPostingContains(t, flatRound, "ser-flat/a", 1)
	fieldStorageAssertPostingContains(t, flatRound, "ser-flat/b", 2)
	flatRound.Release()

	chunkedMap := GetPostingMap()
	for i := 0; i < FieldChunkThreshold+5; i++ {
		chunkedMap[fmt.Sprintf("ser-chunk/%04d", i)] = fieldStorageSingleton(uint64(i + 1))
	}
	chunked := NewRegularFieldStorageFromPostingMapOwned(chunkedMap)
	if !chunked.IsChunked() {
		chunked.Release()
		t.Fatalf("expected chunked string storage")
	}

	raw.Reset()
	writer.Reset(&raw)
	if err := chunked.WriteInto(writer); err != nil {
		chunked.Release()
		t.Fatalf("write chunked storage: %v", err)
	}
	if err := writer.Flush(); err != nil {
		chunked.Release()
		t.Fatalf("flush chunked storage: %v", err)
	}
	chunkedRound, err := ReadFieldStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true, "test", "field")
	if err != nil {
		chunked.Release()
		t.Fatalf("read chunked storage: %v", err)
	}
	chunked.Release()
	poisonBytes(raw.Bytes())
	fieldStorageAssertPostingContains(t, chunkedRound, "ser-chunk/0000", 1)
	fieldStorageAssertPostingContains(t, chunkedRound, "ser-chunk/0192", 193)
	fieldStorageAssertPostingContains(t, chunkedRound, "ser-chunk/0388", 389)
	chunkedRound.Release()
}

func TestSerializationSkipEntryAndKey(t *testing.T) {
	first := Entry{
		Key: keycodec.FromString("skip"),
		IDs: fieldStorageOwnedTestPosting(1),
	}
	second := Entry{
		Key: keycodec.FromString("read"),
		IDs: fieldStorageOwnedTestPosting(2),
	}
	defer first.IDs.Release()
	defer second.IDs.Release()

	var entriesRaw bytes.Buffer
	writer := bufio.NewWriter(&entriesRaw)
	if err := first.WriteInto(writer); err != nil {
		t.Fatalf("write first entry: %v", err)
	}
	if err := second.WriteInto(writer); err != nil {
		t.Fatalf("write second entry: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush entries: %v", err)
	}
	reader := bufio.NewReader(bytes.NewReader(entriesRaw.Bytes()))
	if err := skipEntry(reader); err != nil {
		t.Fatalf("skip entry: %v", err)
	}
	gotEntry, err := readEntry(reader)
	if err != nil {
		t.Fatalf("read entry after skip: %v", err)
	}
	defer gotEntry.IDs.Release()
	if !keycodec.EqualsString(gotEntry.Key, "read") || gotEntry.IDs.Cardinality() != 2 || !gotEntry.IDs.Contains(2) || !gotEntry.IDs.Contains(1_000_002) {
		t.Fatalf("unexpected entry after skip: key=%q ids=%v", gotEntry.Key.UnsafeString(), gotEntry.IDs)
	}

	var keysRaw bytes.Buffer
	writer = bufio.NewWriter(&keysRaw)
	if err := writeKey(writer, keycodec.FromString("skip-key")); err != nil {
		t.Fatalf("write first key: %v", err)
	}
	if err := writeKey(writer, keycodec.FromString("read-key")); err != nil {
		t.Fatalf("write second key: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush keys: %v", err)
	}
	reader = bufio.NewReader(bytes.NewReader(keysRaw.Bytes()))
	if err := skipKey(reader); err != nil {
		t.Fatalf("skip key: %v", err)
	}
	gotKey, err := readKey(reader)
	if err != nil {
		t.Fatalf("read key after skip: %v", err)
	}
	if !keycodec.EqualsString(gotKey, "read-key") {
		t.Fatalf("unexpected key after skip: got %q want read-key", gotKey.UnsafeString())
	}
}

func TestReadStorageSkipConsumesExactlyOneStorage(t *testing.T) {
	fieldTests := []struct {
		name string
		rows int
	}{
		{name: "FieldFlat", rows: 8},
		{name: "FieldChunked", rows: fieldIndexChunkThreshold + 17},
	}
	for _, tc := range fieldTests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, false)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()

			var raw bytes.Buffer
			writer := bufio.NewWriter(&raw)
			if err := storage.WriteInto(writer); err != nil {
				t.Fatalf("write field storage: %v", err)
			}
			if err := writer.WriteByte(0xA7); err != nil {
				t.Fatalf("write sentinel: %v", err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			reader := bufio.NewReader(bytes.NewReader(raw.Bytes()))
			skipped, err := ReadFieldStorage(reader, false, "test", "field")
			if err != nil {
				t.Fatalf("skip field storage: %v", err)
			}
			if skipped.KeyCount() != 0 {
				skipped.Release()
				t.Fatalf("skip returned non-empty field storage")
			}
			b, err := reader.ReadByte()
			if err != nil || b != 0xA7 {
				t.Fatalf("field skip consumed wrong bytes: byte=%x err=%v", b, err)
			}
		})
	}

	measureTests := []struct {
		name string
		rows int
	}{
		{name: "MeasureFlat", rows: 8},
		{name: "MeasureChunked", rows: MeasureChunkThreshold + 17},
	}
	for _, tc := range measureTests {
		t.Run(tc.name, func(t *testing.T) {
			storage := measureStorageForTest(tc.rows)
			defer storage.Release()

			var raw bytes.Buffer
			writer := bufio.NewWriter(&raw)
			if err := storage.WriteInto(writer); err != nil {
				t.Fatalf("write measure storage: %v", err)
			}
			if err := writer.WriteByte(0xA8); err != nil {
				t.Fatalf("write sentinel: %v", err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			reader := bufio.NewReader(bytes.NewReader(raw.Bytes()))
			skipped, err := ReadMeasureStorage(reader, false)
			if err != nil {
				t.Fatalf("skip measure storage: %v", err)
			}
			if skipped.Rows() != 0 {
				skipped.Release()
				t.Fatalf("skip returned non-empty measure storage")
			}
			b, err := reader.ReadByte()
			if err != nil || b != 0xA8 {
				t.Fatalf("measure skip consumed wrong bytes: byte=%x err=%v", b, err)
			}
		})
	}
}

func TestMeasureStorageSerializationRoundTrip_FlatAndChunked(t *testing.T) {
	flat := measureStorageForTest(8)
	flatRound, err := measureStorageRoundTripForTest(flat)
	if err != nil {
		flat.Release()
		t.Fatalf("flat measure round trip: %v", err)
	}
	if flatRound.IsChunked() {
		flat.Release()
		flatRound.Release()
		t.Fatalf("expected flat measure storage after round trip")
	}
	if flatRound.Rows() != 8 {
		flat.Release()
		flatRound.Release()
		t.Fatalf("flat rows: got %d want 8", flatRound.Rows())
	}
	measureStorageAssertValue(t, flatRound, 7, 30)
	flat.Release()
	flatRound.Release()

	chunked := measureStorageForTest(MeasureChunkThreshold + 11)
	chunkedRound, err := measureStorageRoundTripForTest(chunked)
	if err != nil {
		chunked.Release()
		t.Fatalf("chunked measure round trip: %v", err)
	}
	if !chunkedRound.IsChunked() {
		chunked.Release()
		chunkedRound.Release()
		t.Fatalf("expected chunked measure storage after round trip")
	}
	if chunkedRound.Rows() != MeasureChunkThreshold+11 {
		chunked.Release()
		chunkedRound.Release()
		t.Fatalf("chunked rows: got %d want %d", chunkedRound.Rows(), MeasureChunkThreshold+11)
	}
	measureProbe := uint64((MeasureChunkThreshold/2)*2 + 1)
	measureStorageAssertValue(t, chunkedRound, measureProbe, uint64(MeasureChunkThreshold/2*10))
	chunked.Release()
	chunkedRound.Release()
}

func TestReadFieldStorageRejectsUnsortedFlatKeys(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writer.WriteByte(fieldStorageEncodingFlat); err != nil {
		t.Fatalf("write storage tag: %v", err)
	}
	if err := writeUvarint(writer, 2); err != nil {
		t.Fatalf("write count: %v", err)
	}
	if err := writeKey(writer, keycodec.FromString("b")); err != nil {
		t.Fatalf("write first key: %v", err)
	}
	if err := posting.WriteSingleton(writer, 1); err != nil {
		t.Fatalf("write first posting: %v", err)
	}
	if err := writeKey(writer, keycodec.FromString("a")); err != nil {
		t.Fatalf("write second key: %v", err)
	}
	if err := posting.WriteSingleton(writer, 2); err != nil {
		t.Fatalf("write second posting: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	storage, err := ReadFieldStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true, "test", "field")
	if storage.KeyCount() != 0 {
		storage.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "flat entry keys must be strictly increasing") {
		t.Fatalf("ReadFieldStorage err=%v, want sorted-key error", err)
	}
}

func TestReadFieldStorageRejectsUnsortedChunkKeys(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writer.WriteByte(fieldStorageEncodingChunked); err != nil {
		t.Fatalf("write storage tag: %v", err)
	}
	if err := writeUvarint(writer, 1); err != nil {
		t.Fatalf("write page count: %v", err)
	}
	if err := writeUvarint(writer, 1); err != nil {
		t.Fatalf("write ref count: %v", err)
	}
	writeNumericFieldChunkForTest(t, writer, 2, 1)
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	storage, err := ReadFieldStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true, "test", "field")
	if storage.KeyCount() != 0 {
		storage.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "numeric chunk keys must be strictly increasing") {
		t.Fatalf("ReadFieldStorage err=%v, want chunk sorted-key error", err)
	}
}

func TestReadFieldStorageRejectsUnsortedChunkRefs(t *testing.T) {
	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := writer.WriteByte(fieldStorageEncodingChunked); err != nil {
		t.Fatalf("write storage tag: %v", err)
	}
	if err := writeUvarint(writer, 1); err != nil {
		t.Fatalf("write page count: %v", err)
	}
	if err := writeUvarint(writer, 2); err != nil {
		t.Fatalf("write ref count: %v", err)
	}
	writeNumericFieldChunkForTest(t, writer, 2)
	writeNumericFieldChunkForTest(t, writer, 1)
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	storage, err := ReadFieldStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true, "test", "field")
	if storage.KeyCount() != 0 {
		storage.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "chunk keys must be strictly increasing") {
		t.Fatalf("ReadFieldStorage err=%v, want chunk-ref sorted-key error", err)
	}
}

func TestReadMeasureStorageRejectsUnsortedRows(t *testing.T) {
	t.Run("Flat", func(t *testing.T) {
		var raw bytes.Buffer
		writer := bufio.NewWriter(&raw)
		if err := writeUvarint(writer, 2); err != nil {
			t.Fatalf("write count: %v", err)
		}
		if err := writeUvarint(writer, 2); err != nil {
			t.Fatalf("write first id: %v", err)
		}
		if err := writeUvarint(writer, 20); err != nil {
			t.Fatalf("write first value: %v", err)
		}
		if err := writeUvarint(writer, 1); err != nil {
			t.Fatalf("write second id: %v", err)
		}
		if err := writeUvarint(writer, 10); err != nil {
			t.Fatalf("write second value: %v", err)
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		storage, err := ReadMeasureStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true)
		if storage.Rows() != 0 {
			storage.Release()
		}
		if err == nil || !strings.Contains(err.Error(), "measure rows must be strictly increasing") {
			t.Fatalf("ReadMeasureStorage err=%v, want sorted-row error", err)
		}
	})

	t.Run("Chunked", func(t *testing.T) {
		rows := MeasureChunkThreshold + 1
		var raw bytes.Buffer
		writer := bufio.NewWriter(&raw)
		if err := writeUvarint(writer, uint64(rows)); err != nil {
			t.Fatalf("write count: %v", err)
		}
		for i := 0; i < rows; i++ {
			id := uint64(i + 1)
			if i == MeasureChunkTargetRows {
				id = uint64(MeasureChunkTargetRows)
			}
			if err := writeUvarint(writer, id); err != nil {
				t.Fatalf("write id %d: %v", i, err)
			}
			if err := writeUvarint(writer, uint64(i*10)); err != nil {
				t.Fatalf("write value %d: %v", i, err)
			}
		}
		if err := writer.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		storage, err := ReadMeasureStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true)
		if storage.Rows() != 0 {
			storage.Release()
		}
		if err == nil || !strings.Contains(err.Error(), "measure rows must be strictly increasing") {
			t.Fatalf("ReadMeasureStorage err=%v, want sorted-row error", err)
		}
	})
}

func fieldStorageRoundTripForTest(storage FieldStorage) (FieldStorage, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := storage.WriteInto(writer); err != nil {
		return FieldStorage{}, err
	}
	if err := writer.Flush(); err != nil {
		return FieldStorage{}, err
	}
	return ReadFieldStorage(bufio.NewReader(bytes.NewReader(buf.Bytes())), true, "test", "field")
}

func measureStorageRoundTripForTest(storage MeasureStorage) (MeasureStorage, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := storage.WriteInto(writer); err != nil {
		return MeasureStorage{}, err
	}
	if err := writer.Flush(); err != nil {
		return MeasureStorage{}, err
	}
	return ReadMeasureStorage(bufio.NewReader(bytes.NewReader(buf.Bytes())), true)
}

func writeNumericFieldChunkForTest(t *testing.T, writer *bufio.Writer, keys ...uint64) {
	t.Helper()
	if err := writer.WriteByte(fieldIndexChunkEncodingRaw8); err != nil {
		t.Fatalf("write chunk tag: %v", err)
	}
	if err := writeUvarint(writer, uint64(len(keys))); err != nil {
		t.Fatalf("write chunk count: %v", err)
	}
	for i, key := range keys {
		if err := writeBEUint64(writer, key); err != nil {
			t.Fatalf("write chunk key %d: %v", i, err)
		}
		if err := posting.WriteSingleton(writer, uint64(i+1)); err != nil {
			t.Fatalf("write chunk posting %d: %v", i, err)
		}
	}
}
