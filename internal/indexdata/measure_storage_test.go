package indexdata

import "testing"

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
