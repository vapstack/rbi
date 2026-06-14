package indexdata

import (
	"bufio"
	"bytes"
	"testing"
)

// Measure storage ownership matrix:
// - NewMeasureStorageFromEntriesOwned consumes entry buffers and copies ID/value data into storage-owned slices.
// - NewMeasureStorageFromSortedRunsOwned consumes run buffers and copies ID/value data into storage-owned chunks.
// - ApplyDeltasOwned retains untouched chunks and rebuilds touched flat/chunk data into new owned storage.
func poisonMeasureEntries(entries []MeasureEntry) {
	for i := range entries {
		entries[i] = MeasureEntry{ID: ^uint64(0), Value: ^uint64(0)}
	}
}

func poisonUint64s(bufs ...[]uint64) {
	for i := range bufs {
		buf := bufs[i]
		for j := range buf {
			buf[j] = ^uint64(0)
		}
	}
}

func roundTripMeasureStorage(t *testing.T, storage MeasureStorage) MeasureStorage {
	t.Helper()

	var raw bytes.Buffer
	writer := bufio.NewWriter(&raw)
	if err := storage.WriteInto(writer); err != nil {
		t.Fatalf("WriteInto: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	round, err := ReadMeasureStorage(bufio.NewReader(bytes.NewReader(raw.Bytes())), true)
	if err != nil {
		t.Fatalf("ReadMeasureStorage: %v", err)
	}
	return round
}

func TestMeasureStorageFromEntriesOwnedCopiesEntryBuffer(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 8},
		{name: "Chunked", rows: MeasureChunkThreshold + 17},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := GetMeasureEntrySlice(tc.rows)
			for i := 0; i < tc.rows; i++ {
				id := uint64(i*2 + 1)
				entries = append(entries, MeasureEntry{ID: id, Value: id * 10})
			}

			storage := NewMeasureStorageFromEntriesOwned(entries)
			defer storage.Release()
			if storage.IsChunked() != (tc.rows > MeasureChunkThreshold) {
				t.Fatalf("unexpected storage shape")
			}

			poisonMeasureEntries(entries)

			measureStorageAssertValue(t, storage, 1, 10)
			mid := uint64((tc.rows/2)*2 + 1)
			measureStorageAssertValue(t, storage, mid, mid*10)
			last := uint64((tc.rows-1)*2 + 1)
			measureStorageAssertValue(t, storage, last, last*10)
		})
	}
}

func TestMeasureStorageApplyDeltasFlatAndChunked(t *testing.T) {
	flat := measureStorageForTest(8)
	if flat.IsChunked() {
		flat.Release()
		t.Fatalf("expected flat measure storage")
	}
	flatDeltas := GetMeasureDeltaSlice(3)
	flatDeltas = append(flatDeltas, MeasureDelta{ID: 17, NewOK: true, New: 1_700})
	flatDeltas = append(flatDeltas, MeasureDelta{ID: 5})
	flatDeltas = append(flatDeltas, MeasureDelta{ID: 3, NewOK: true, New: 333})
	flatOut := flat.ApplyDeltasOwned(flatDeltas)
	if flatOut.Rows() != 8 {
		flat.Release()
		flatOut.Release()
		t.Fatalf("flat rows: got %d want 8", flatOut.Rows())
	}
	measureStorageAssertValue(t, flatOut, 3, 333)
	measureStorageAssertMissing(t, flatOut, 5)
	measureStorageAssertValue(t, flatOut, 17, 1_700)
	flat.Release()
	flatOut.Release()

	chunked := measureStorageForTest(MeasureChunkThreshold + 19)
	if !chunked.IsChunked() {
		chunked.Release()
		t.Fatalf("expected chunked measure storage")
	}
	chunkedDeltas := GetMeasureDeltaSlice(3)
	chunkedDeltas = append(chunkedDeltas, MeasureDelta{ID: 1, NewOK: true, New: 111})
	chunkedDeltas = append(chunkedDeltas, MeasureDelta{ID: 9})
	chunkedDeltas = append(chunkedDeltas, MeasureDelta{ID: uint64((MeasureChunkThreshold+19)*2 + 1), NewOK: true, New: 9_999})
	chunkedOut := chunked.ApplyDeltasOwned(chunkedDeltas)
	if !chunkedOut.IsChunked() {
		chunked.Release()
		chunkedOut.Release()
		t.Fatalf("expected chunked result")
	}
	if chunkedOut.Rows() != MeasureChunkThreshold+19 {
		chunked.Release()
		chunkedOut.Release()
		t.Fatalf("chunked rows: got %d want %d", chunkedOut.Rows(), MeasureChunkThreshold+19)
	}
	measureStorageAssertValue(t, chunkedOut, 1, 111)
	measureStorageAssertMissing(t, chunkedOut, 9)
	measureStorageAssertValue(t, chunkedOut, uint64((MeasureChunkThreshold+19)*2+1), 9_999)
	chunked.Release()
	chunkedOut.Release()
}

func TestMeasureStorageFromEntriesOwnedCoalescesDuplicateIDs(t *testing.T) {
	entries := GetMeasureEntrySlice(0)
	entries = append(entries, MeasureEntry{ID: 5, Value: 50})
	entries = append(entries, MeasureEntry{ID: 1, Value: 10})
	entries = append(entries, MeasureEntry{ID: 5, Value: 55})
	entries = append(entries, MeasureEntry{ID: 3, Value: 30})

	storage := NewMeasureStorageFromEntriesOwned(entries)
	defer storage.Release()

	if storage.Rows() != 3 {
		t.Fatalf("rows: got %d want 3", storage.Rows())
	}
	measureStorageAssertValue(t, storage, 1, 10)
	measureStorageAssertValue(t, storage, 3, 30)
	measureStorageAssertValue(t, storage, 5, 55)
}

func TestMeasureStorageApplyDeltasOwnedCoalescesDuplicateIDs(t *testing.T) {
	deltas := GetMeasureDeltaSlice(0)
	deltas = append(deltas, MeasureDelta{ID: 5, NewOK: true, New: 50})
	deltas = append(deltas, MeasureDelta{ID: 5, NewOK: true, New: 55})

	storage := MeasureStorage{}.ApplyDeltasOwned(deltas)
	defer storage.Release()

	if storage.Rows() != 1 {
		t.Fatalf("rows: got %d want 1", storage.Rows())
	}
	measureStorageAssertValue(t, storage, 5, 55)

	round, err := measureStorageRoundTripForTest(storage)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	defer round.Release()
	measureStorageAssertValue(t, round, 5, 55)
}

func TestMeasureStorageApplyDeltasOwnedSurvivesBaseReleaseAndPoison(t *testing.T) {
	flat := measureStorageForTest(8)
	if flat.IsChunked() {
		flat.Release()
		t.Fatalf("expected flat measure storage")
	}
	oldFlatIDs := flat.flat.ids
	oldFlatValues := flat.flat.values
	flatDeltas := GetMeasureDeltaSlice(2)
	flatDeltas = append(flatDeltas, MeasureDelta{ID: 1, NewOK: true, New: 111})
	flatDeltas = append(flatDeltas, MeasureDelta{ID: 3})
	flatOut := flat.ApplyDeltasOwned(flatDeltas)
	flat.Release()
	poisonUint64s(oldFlatIDs, oldFlatValues)
	defer flatOut.Release()

	measureStorageAssertValue(t, flatOut, 1, 111)
	measureStorageAssertMissing(t, flatOut, 3)
	measureStorageAssertValue(t, flatOut, 5, 20)

	chunked := measureStorageForTest(MeasureChunkThreshold + 19)
	if !chunked.IsChunked() {
		chunked.Release()
		t.Fatalf("expected chunked measure storage")
	}
	touched := chunked.chunked.refsByID[0].chunk
	oldChunkIDs := touched.ids
	oldChunkValues := touched.values
	chunkedDeltas := GetMeasureDeltaSlice(2)
	chunkedDeltas = append(chunkedDeltas, MeasureDelta{ID: 1, NewOK: true, New: 111})
	chunkedDeltas = append(chunkedDeltas, MeasureDelta{ID: 3})
	chunkedOut := chunked.ApplyDeltasOwned(chunkedDeltas)
	if !chunkedOut.IsChunked() {
		chunkedOut.Release()
		chunked.Release()
		t.Fatalf("expected chunked output")
	}
	chunked.Release()
	poisonUint64s(oldChunkIDs, oldChunkValues)
	defer chunkedOut.Release()

	measureStorageAssertValue(t, chunkedOut, 1, 111)
	measureStorageAssertMissing(t, chunkedOut, 3)
	measureStorageAssertValue(t, chunkedOut, 5, 20)
	probe := uint64(MeasureChunkTargetRows*2 + 1)
	measureStorageAssertValue(t, chunkedOut, probe, uint64(MeasureChunkTargetRows*10))
}

func TestMeasureStorageFromSortedRunsOwned(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: MeasureChunkThreshold},
		{name: "Chunked", rows: MeasureChunkThreshold + MeasureChunkTargetRows + 17},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const runCount = 4
			runs := GetMeasureEntrySlots(runCount)
			for i := 0; i < runCount; i++ {
				run := GetMeasureEntrySlice(0)
				for id := i + 1; id <= tc.rows; id += runCount {
					run = append(run, MeasureEntry{ID: uint64(id), Value: uint64(id * 10)})
				}
				runs[i] = run
			}

			storage := NewMeasureStorageFromSortedRunsOwned(runs)
			defer storage.Release()

			if got := storage.Rows(); got != tc.rows {
				t.Fatalf("rows: got %d want %d", got, tc.rows)
			}
			measureStorageAssertValue(t, storage, 1, 10)
			measureStorageAssertValue(t, storage, uint64(tc.rows/2), uint64((tc.rows/2)*10))
			measureStorageAssertValue(t, storage, uint64(tc.rows), uint64(tc.rows*10))
		})
	}
}

func TestMeasureStorageFromSortedRunsOwnedCoalescesDuplicateIDsFlat(t *testing.T) {
	runs := GetMeasureEntrySlots(3)
	runs[0] = append(GetMeasureEntrySlice(0),
		MeasureEntry{ID: 1, Value: 10},
		MeasureEntry{ID: 3, Value: 30},
		MeasureEntry{ID: 5, Value: 50},
		MeasureEntry{ID: 5, Value: 55},
	)
	runs[1] = append(GetMeasureEntrySlice(0),
		MeasureEntry{ID: 2, Value: 20},
		MeasureEntry{ID: 3, Value: 300},
		MeasureEntry{ID: 6, Value: 60},
	)
	runs[2] = append(GetMeasureEntrySlice(0),
		MeasureEntry{ID: 3, Value: 3_000},
		MeasureEntry{ID: 7, Value: 70},
	)

	storage := NewMeasureStorageFromSortedRunsOwned(runs)
	defer storage.Release()
	if storage.IsChunked() {
		t.Fatalf("expected flat storage")
	}
	if got := storage.Rows(); got != 6 {
		t.Fatalf("rows: got %d want 6", got)
	}
	measureStorageAssertValue(t, storage, 3, 3_000)
	measureStorageAssertValue(t, storage, 5, 55)

	round := roundTripMeasureStorage(t, storage)
	defer round.Release()
	if got := round.Rows(); got != 6 {
		t.Fatalf("round rows: got %d want 6", got)
	}
	measureStorageAssertValue(t, round, 3, 3_000)
	measureStorageAssertValue(t, round, 5, 55)
}

func TestMeasureStorageFromSortedRunsOwnedCoalescesDuplicateIDsToFlat(t *testing.T) {
	const rows = MeasureChunkThreshold

	runs := GetMeasureEntrySlots(2)
	first := GetMeasureEntrySlice(rows)
	second := GetMeasureEntrySlice(rows)
	for id := 1; id <= rows; id++ {
		first = append(first, MeasureEntry{ID: uint64(id), Value: uint64(id * 10)})
		second = append(second, MeasureEntry{ID: uint64(id), Value: uint64(id * 100)})
	}
	runs[0] = first
	runs[1] = second

	storage := NewMeasureStorageFromSortedRunsOwned(runs)
	defer storage.Release()
	if storage.IsChunked() {
		t.Fatalf("expected flat storage")
	}
	if got := storage.Rows(); got != rows {
		t.Fatalf("rows: got %d want %d", got, rows)
	}
	measureStorageAssertValue(t, storage, 1, 100)
	measureStorageAssertValue(t, storage, rows, rows*100)
}

func TestMeasureStorageFromSortedRunsOwnedCoalescesDuplicateIDsChunked(t *testing.T) {
	const rows = MeasureChunkThreshold + 17

	runs := GetMeasureEntrySlots(2)
	run := GetMeasureEntrySlice(rows)
	for id := 1; id <= rows; id++ {
		run = append(run, MeasureEntry{ID: uint64(id), Value: uint64(id * 10)})
	}
	runs[0] = run
	runs[1] = append(GetMeasureEntrySlice(0),
		MeasureEntry{ID: 7, Value: 7_000},
		MeasureEntry{ID: MeasureChunkTargetRows + 1, Value: 88_000},
		MeasureEntry{ID: rows, Value: 99_000},
	)

	storage := NewMeasureStorageFromSortedRunsOwned(runs)
	defer storage.Release()
	if !storage.IsChunked() {
		t.Fatalf("expected chunked storage")
	}
	if got := storage.Rows(); got != rows {
		t.Fatalf("rows: got %d want %d", got, rows)
	}
	measureStorageAssertValue(t, storage, 7, 7_000)
	measureStorageAssertValue(t, storage, MeasureChunkTargetRows+1, 88_000)
	measureStorageAssertValue(t, storage, rows, 99_000)

	round := roundTripMeasureStorage(t, storage)
	defer round.Release()
	if got := round.Rows(); got != rows {
		t.Fatalf("round rows: got %d want %d", got, rows)
	}
	measureStorageAssertValue(t, round, 7, 7_000)
	measureStorageAssertValue(t, round, MeasureChunkTargetRows+1, 88_000)
	measureStorageAssertValue(t, round, rows, 99_000)
}

func TestMeasureStorageFromSortedRunsOwnedCopiesRunBuffers(t *testing.T) {
	const runCount = 3
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 9},
		{name: "Chunked", rows: MeasureChunkThreshold + 9},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runs := GetMeasureEntrySlots(runCount)
			runCopies := make([][]MeasureEntry, runCount)
			for i := 0; i < runCount; i++ {
				run := GetMeasureEntrySlice(0)
				for id := i + 1; id <= tc.rows; id += runCount {
					run = append(run, MeasureEntry{ID: uint64(id), Value: uint64(id * 10)})
				}
				runs[i] = run
				runCopies[i] = run
			}

			storage := NewMeasureStorageFromSortedRunsOwned(runs)
			defer storage.Release()
			if storage.IsChunked() != (tc.rows > MeasureChunkThreshold) {
				t.Fatalf("unexpected storage shape")
			}

			for i := range runCopies {
				poisonMeasureEntries(runCopies[i])
			}

			measureStorageAssertValue(t, storage, 1, 10)
			mid := uint64(tc.rows/2 + 1)
			measureStorageAssertValue(t, storage, mid, mid*10)
			measureStorageAssertValue(t, storage, uint64(tc.rows), uint64(tc.rows*10))
		})
	}
}

func TestMeasureDeltaBatchApplyToMeasureStorageSlotsOwned(t *testing.T) {
	prev := GetMeasureStorageSlice(3)[:3]
	prev[0] = measureStorageForTest(4)
	prev[1] = measureStorageForTest(4)
	prev[2] = measureStorageForTest(4)
	next := CloneMeasureStorageSlots(prev, 3)

	deltas := NewMeasureDeltaBatch(3)
	deltas.Append(1, 3, true, 333)
	deltas.Append(1, 5, false, 0)
	deltas.Append(2, 99, true, 9_900)
	deltas.ApplyToMeasureStorageSlotsOwned(next)
	deltas.Release()
	RetainSharedMeasureStorageSlots(next, prev)
	defer ReleaseMeasureStorageSlots(next)
	defer ReleaseMeasureStorageSlots(prev)

	if deltas.fields != nil || deltas.touched != nil {
		t.Fatalf("expected released batch")
	}
	if next[0] != prev[0] {
		t.Fatalf("untouched slot changed")
	}
	if next[1] == prev[1] {
		t.Fatalf("updated slot reused base storage")
	}
	if next[2] == prev[2] {
		t.Fatalf("appended slot reused base storage")
	}
	if next[1].Rows() != 3 {
		t.Fatalf("updated rows: got %d want 3", next[1].Rows())
	}
	measureStorageAssertValue(t, next[1], 3, 333)
	measureStorageAssertMissing(t, next[1], 5)
	measureStorageAssertValue(t, next[2], 99, 9_900)
}

func TestMeasureDeltaBatchAppendTracksTouchedSlots(t *testing.T) {
	deltas := NewMeasureDeltaBatch(3)
	deltas.Append(2, 5, true, 500)
	deltas.Append(2, 7, true, 700)
	deltas.Append(0, 1, false, 0)

	if len(deltas.touched) != 2 || deltas.touched[0] != 2 || deltas.touched[1] != 0 {
		deltas.Release()
		t.Fatalf("touched slots mismatch: %v", deltas.touched)
	}
	if len(deltas.fields[2]) != 2 || len(deltas.fields[0]) != 1 || deltas.fields[1] != nil {
		deltas.Release()
		t.Fatalf("field delta slots mismatch")
	}

	deltas.Release()
	if deltas.fields != nil || deltas.touched != nil {
		t.Fatalf("expected released batch")
	}
}

func TestMeasureStorageAccessorsFlatAndChunked(t *testing.T) {
	flat := measureStorageForTest(8)
	ids, values, ok := flat.FlatSlices()
	if !ok || len(ids) != 8 || len(values) != 8 || ids[3] != 7 || values[3] != 30 {
		flat.Release()
		t.Fatalf("flat slices mismatch: ids=%v values=%v ok=%v", ids, values, ok)
	}
	if flat.LookupSteps() == 0 {
		flat.Release()
		t.Fatalf("expected flat lookup steps")
	}
	flat.Release()

	chunked := measureStorageForTest(MeasureChunkThreshold + 11)
	if chunked.ChunkCount() != 3 {
		chunked.Release()
		t.Fatalf("chunk count: got %d want 3", chunked.ChunkCount())
	}
	if chunked.TailChunkRows() != 11 {
		chunked.Release()
		t.Fatalf("tail rows: got %d want 11", chunked.TailChunkRows())
	}
	chunkIDs, chunkValues := chunked.ChunkSlices(0)
	if len(chunkIDs) != MeasureChunkTargetRows || len(chunkValues) != MeasureChunkTargetRows || chunkIDs[3] != 7 || chunkValues[3] != 30 {
		chunked.Release()
		t.Fatalf("chunk slices mismatch")
	}
	if chunked.LookupSteps() == 0 {
		chunked.Release()
		t.Fatalf("expected chunked lookup steps")
	}
	chunked.Release()
}

func TestMeasureStorageSlotsRetainSharedStorage(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 8},
		{name: "Chunked", rows: MeasureChunkThreshold + 11},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			storage := measureStorageForTest(tc.rows)
			prev := GetMeasureStorageSlice(1)
			prev = append(prev, storage)
			next := CloneMeasureStorageSlots(prev, 1)
			RetainSharedMeasureStorageSlots(next, prev)
			ReleaseMeasureStorageSlots(prev)

			measureStorageAssertValue(t, next[0], 1, 0)
			ReleaseMeasureStorageSlots(next)
		})
	}
}
