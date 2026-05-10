package strmap

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%08d", i)
	}
	return keys
}

func benchDenseSnapshot(n int) (*Snapshot, []string) {
	keys := benchKeys(n)
	keyMap := make(map[string]uint64, n)
	strs := make([]string, n+1)
	used := make([]bool, n+1)
	for i, key := range keys {
		idx := uint64(i + 1)
		keyMap[key] = idx
		strs[idx] = key
		used[idx] = true
	}
	m := New(uint64(n), 256)
	m.replaceAllDenseNoLock(keyMap, strs, used, uint64(n))
	return m.Snapshot(), keys
}

func benchReadDirSnapshot(n int) (*Snapshot, []string) {
	keys := benchKeys(n)
	m := New(uint64(n), 256)
	for _, key := range keys {
		m.Create(key)
	}
	return m.Snapshot(), keys
}

func benchSparseSnapshot(n int, gap uint64) (*Snapshot, []string, []uint64) {
	keys := benchKeys(n)
	keyMap := make(map[string]uint64, n)
	strs := make(map[uint64]string, n)
	idxs := make([]uint64, n)
	for i, key := range keys {
		idx := uint64(i+1) * gap
		keyMap[key] = idx
		strs[idx] = key
		idxs[i] = idx
	}
	m := New(uint64(n), 256)
	m.replaceAllSparseNoLock(keyMap, strs, uint64(n)*gap)
	return m.Snapshot(), keys, idxs
}

func benchSnapshotPayload(b *testing.B, snap *Snapshot) []byte {
	b.Helper()
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, snap); err != nil {
		b.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
	return payload.Bytes()
}

func BenchmarkCreateExisting(b *testing.B) {
	m := New(0, 256)
	m.Create("key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, created := m.Create("key")
		if idx != 1 || created {
			b.Fatalf("Create = %d/%v", idx, created)
		}
	}
}

func BenchmarkWriterCreateExisting(b *testing.B) {
	m := New(0, 256)
	m.Create("key")
	w := m.LockWriter()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, created := w.Create("key")
		if idx != 1 || created {
			b.Fatalf("Create = %d/%v", idx, created)
		}
	}
	b.StopTimer()
	w.Unlock()
}

func BenchmarkCreateNewDense(b *testing.B) {
	keys := benchKeys(b.N)
	m := New(uint64(b.N), 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Create(keys[i])
	}
}

func BenchmarkWriterCreateNewDense(b *testing.B) {
	keys := benchKeys(b.N)
	m := New(uint64(b.N), 256)
	w := m.LockWriter()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Create(keys[i])
	}
	b.StopTimer()
	w.Unlock()
}

func BenchmarkRollbackLastCreated(b *testing.B) {
	keys := benchKeys(b.N)
	m := New(uint64(b.N), 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, _ := m.Create(keys[i])
		m.RollbackCreated(keys[i], idx)
	}
}

func BenchmarkSnapshotSmallDeltaOverBase(b *testing.B) {
	m := New(1024, 256)
	for _, key := range benchKeys(1024) {
		m.Create(key)
	}
	base := m.Snapshot()
	m.MarkCommittedPublished(base)
	keys := benchKeys(b.N + 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, _ := m.Create(keys[1024+i])
		_ = m.Snapshot()
		m.RollbackCreated(keys[1024+i], idx)
	}
}

func BenchmarkPublishedLookupByIdx(b *testing.B) {
	m := New(4096, 256)
	keys := benchKeys(4096)
	for _, key := range keys {
		m.Create(key)
	}
	snap := m.Snapshot()
	lookup := snap.Lookup()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := uint64(i&4095) + 1
		if _, ok := lookup.String(idx); !ok {
			b.Fatalf("missing idx %d", idx)
		}
	}
}

func BenchmarkPublishedLookupByIdxDense(b *testing.B) {
	snap, _ := benchDenseSnapshot(4096)
	lookup := snap.Lookup()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := uint64(i&4095) + 1
		if _, ok := lookup.String(idx); !ok {
			b.Fatalf("missing idx %d", idx)
		}
	}
}

func BenchmarkPublishedLookupByIdxSparse(b *testing.B) {
	snap, _, idxs := benchSparseSnapshot(512, 32)
	lookup := snap.Lookup()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := idxs[i&511]
		if _, ok := lookup.String(idx); !ok {
			b.Fatalf("missing idx %d", idx)
		}
	}
}

func BenchmarkPublishedLookupByString(b *testing.B) {
	m := New(4096, 256)
	keys := benchKeys(4096)
	for _, key := range keys {
		m.Create(key)
	}
	snap := m.Snapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i&4095]
		if _, ok := snap.Index(key); !ok {
			b.Fatalf("missing key %q", key)
		}
	}
}

func BenchmarkPublishedLookupByStringWarmDense(b *testing.B) {
	snap, keys := benchDenseSnapshot(4096)
	if _, ok := snap.Index(keys[0]); !ok {
		b.Fatalf("missing key %q", keys[0])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i&4095]
		if _, ok := snap.Index(key); !ok {
			b.Fatalf("missing key %q", key)
		}
	}
}

func BenchmarkPublishedLookupByStringColdReadDirs(b *testing.B) {
	snap, keys := benchReadDirSnapshot(4096)
	if len(snap.readDirs) == 0 {
		b.Fatal("expected readDirs snapshot")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cold := *snap
		key := keys[i&4095]
		if _, ok := cold.Index(key); !ok {
			b.Fatalf("missing key %q", key)
		}
	}
}

func BenchmarkPublishedLookupByStringWarmSparse(b *testing.B) {
	snap, keys, _ := benchSparseSnapshot(512, 32)
	if _, ok := snap.Index(keys[0]); !ok {
		b.Fatalf("missing key %q", keys[0])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i&511]
		if _, ok := snap.Index(key); !ok {
			b.Fatalf("missing key %q", key)
		}
	}
}

func BenchmarkWriteDense(b *testing.B) {
	m := New(512, 256)
	for _, key := range benchKeys(512) {
		m.Create(key)
	}
	snap := m.Snapshot()
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload.Reset()
		writer.Reset(&payload)
		if err := WriteSnapshot(writer, snap); err != nil {
			b.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteDirectDense(b *testing.B) {
	snap, _ := benchDenseSnapshot(512)
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload.Reset()
		writer.Reset(&payload)
		if err := WriteSnapshot(writer, snap); err != nil {
			b.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteSparse(b *testing.B) {
	strs := map[uint64]string{
		1:     "k00000001",
		4096:  "k00004096",
		16384: "k00016384",
	}
	keys := map[string]uint64{
		"k00000001": 1,
		"k00004096": 4096,
		"k00016384": 16384,
	}
	m := New(uint64(len(keys)), 256)
	m.replaceAllSparseNoLock(keys, strs, 16384)
	snap := m.Snapshot()
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload.Reset()
		writer.Reset(&payload)
		if err := WriteSnapshot(writer, snap); err != nil {
			b.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteSparseLarge(b *testing.B) {
	snap, _, _ := benchSparseSnapshot(512, 32)
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload.Reset()
		writer.Reset(&payload)
		if err := WriteSnapshot(writer, snap); err != nil {
			b.Fatal(err)
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDense(b *testing.B) {
	snap, _ := benchDenseSnapshot(512)
	payload := benchSnapshotPayload(b, snap)
	var src bytes.Reader
	reader := bufio.NewReader(&src)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.Reset(payload)
		reader.Reset(&src)
		m, err := Read(reader, 256)
		if err != nil {
			b.Fatal(err)
		}
		if m.next != 512 {
			b.Fatalf("next=%d", m.next)
		}
	}
}

func BenchmarkReadSparse(b *testing.B) {
	snap, _, _ := benchSparseSnapshot(512, 32)
	payload := benchSnapshotPayload(b, snap)
	var src bytes.Reader
	reader := bufio.NewReader(&src)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src.Reset(payload)
		reader.Reset(&src)
		m, err := Read(reader, 256)
		if err != nil {
			b.Fatal(err)
		}
		if m.next != 16384 {
			b.Fatalf("next=%d", m.next)
		}
	}
}
