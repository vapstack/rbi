package strmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
)

func TestReadRoundTripDense(t *testing.T) {
	sm := &Snapshot{
		next:      5,
		keys:      map[string]uint64{"alpha": 1, "charlie": 3, "echo": 5},
		denseStrs: []string{"", "alpha", "", "charlie", "", "echo"},
		denseUsed: []bool{false, true, false, true, false, true},
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, sm); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := Read(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.next != sm.next {
		t.Fatalf("unexpected next: got=%d want=%d", got.next, sm.next)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "charlie", idx: 3},
		{key: "echo", idx: 5},
	} {
		gotIdx, ok := got.keys[tc.key]
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		if got.strs[tc.idx] != tc.key {
			t.Fatalf("unexpected dense string at %d: got=%q want=%q", tc.idx, got.strs[tc.idx], tc.key)
		}
		if !got.strsUsed[tc.idx] {
			t.Fatalf("expected used flag at %d", tc.idx)
		}
	}
}

func TestReadRoundTripSparseWithHoles(t *testing.T) {
	denseStrs := make([]string, 1001)
	denseUsed := make([]bool, 1001)
	denseStrs[1] = "alpha"
	denseUsed[1] = true
	denseStrs[1000] = "omega"
	denseUsed[1000] = true

	sm := &Snapshot{
		next:      1000,
		keys:      map[string]uint64{"alpha": 1, "omega": 1000},
		denseStrs: denseStrs,
		denseUsed: denseUsed,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, sm); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := Read(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.next != sm.next {
		t.Fatalf("unexpected next: got=%d want=%d", got.next, sm.next)
	}
	if len(got.strs) != 0 || len(got.strsUsed) != 0 {
		t.Fatalf("expected sparse reverse map after load, got dense lens strs=%d used=%d", len(got.strs), len(got.strsUsed))
	}
	if len(got.sparseStrs) != 2 {
		t.Fatalf("unexpected sparse reverse size: got=%d want=2", len(got.sparseStrs))
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "omega", idx: 1000},
	} {
		gotIdx, ok := got.keys[tc.key]
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		if gotKey, ok := got.snapshotNoLock().String(tc.idx); !ok || gotKey != tc.key {
			t.Fatalf("unexpected reverse mapping at %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
}

func TestReadSnapshotCopiesInputBufferStringBytes(t *testing.T) {
	sm := &Snapshot{
		next:      5,
		keys:      map[string]uint64{"alpha": 1, "charlie": 3, "echo": 5},
		denseStrs: []string{"", "alpha", "", "charlie", "", "echo"},
		denseUsed: []bool{false, true, false, true, false, true},
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, sm); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	input := payload.Bytes()
	got, err := Read(bufio.NewReader(bytes.NewReader(input)), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for i := range input {
		input[i] = 0xa5
	}

	snap := got.snapshotNoLock()
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "charlie", idx: 3},
		{key: "echo", idx: 5},
	} {
		gotIdx, ok := snap.Index(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("forward mapping aliases input buffer for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := snap.String(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("reverse mapping aliases input buffer at %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
}

func TestSnapshotStringJumpsToAnchor(t *testing.T) {
	base := &Snapshot{
		next:      100,
		denseStrs: make([]string, 101),
		denseUsed: make([]bool, 101),
		depth:     1,
	}
	base.denseStrs[1] = "alpha"
	base.denseUsed[1] = true

	cur := base
	for i := uint64(101); i <= 110; i++ {
		cur = &Snapshot{
			next:   i,
			strs:   map[uint64]string{i: fmt.Sprintf("k%d", i)},
			base:   cur,
			anchor: base,
			depth:  cur.depth + 1,
		}
	}

	if got, ok := cur.String(1); !ok || got != "alpha" {
		t.Fatalf("base lookup mismatch: got=%q ok=%v", got, ok)
	}
	if got, ok := cur.String(110); !ok || got != "k110" {
		t.Fatalf("delta lookup mismatch: got=%q ok=%v", got, ok)
	}
	if _, ok := cur.String(111); ok {
		t.Fatal("expected missing lookup above next to fail")
	}
}

func TestWriteDeltaChainChoosesDenseByEffectiveSnapshot(t *testing.T) {
	base := &Snapshot{
		next:      2,
		keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		denseStrs: []string{"", "alpha", "bravo"},
		denseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &Snapshot{
		next:  4,
		keys:  map[string]uint64{"charlie": 3, "delta": 4},
		strs:  map[uint64]string{3: "charlie", 4: "delta"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, sm); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(payload.Bytes()))
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if next != sm.next {
		t.Fatalf("unexpected next: got=%d want=%d", next, sm.next)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read encoding: %v", err)
	}
	if enc != encodingDense {
		t.Fatalf("unexpected encoding: got=%d want=%d", enc, encodingDense)
	}

	got, err := Read(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "bravo", idx: 2},
		{key: "charlie", idx: 3},
		{key: "delta", idx: 4},
	} {
		gotIdx, ok := got.snapshotNoLock().Index(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
	}
}

func TestWriteSparseDeltaChainRoundTrip(t *testing.T) {
	base := &Snapshot{
		next:      2,
		keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		denseStrs: []string{"", "alpha", "bravo"},
		denseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &Snapshot{
		next:  1000,
		keys:  map[string]uint64{"omega": 1000},
		strs:  map[uint64]string{1000: "omega"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, sm); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(payload.Bytes()))
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if next != sm.next {
		t.Fatalf("unexpected next: got=%d want=%d", next, sm.next)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read encoding: %v", err)
	}
	if enc != encodingSparse {
		t.Fatalf("unexpected encoding: got=%d want=%d", enc, encodingSparse)
	}

	got, err := Read(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "bravo", idx: 2},
		{key: "omega", idx: 1000},
	} {
		gotIdx, ok := got.snapshotNoLock().Index(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := got.snapshotNoLock().String(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("missing reverse mapping for %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
	if _, ok := got.snapshotNoLock().String(999); ok {
		t.Fatal("expected gap before sparse delta entry to stay absent after round-trip")
	}
}

func TestPublishedReadPagesRoundTrip(t *testing.T) {
	const baseNext = 250

	baseDenseStrs := make([]string, baseNext+1)
	baseDenseUsed := make([]bool, baseNext+1)
	for i := 1; i <= baseNext; i++ {
		key := fmt.Sprintf("k%03d", i)
		baseDenseStrs[i] = key
		baseDenseUsed[i] = true
	}

	base := &Snapshot{
		next:      baseNext,
		denseStrs: baseDenseStrs,
		denseUsed: baseDenseUsed,
		depth:     1,
	}
	sm := &Snapshot{
		next: 260,
		strs: map[uint64]string{
			251: "k251",
			253: "k253",
			257: "k257",
			260: "k260",
		},
		base:  base,
		depth: 2,
	}

	published := buildPublishedSnapshot(sm, nil, nil)
	if len(published.readDirs) == 0 {
		t.Fatal("expected published read pages for delta snapshot")
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := WriteSnapshot(writer, published); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := Read(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	for _, tc := range []struct {
		idx uint64
		key string
	}{
		{idx: 250, key: "k250"},
		{idx: 251, key: "k251"},
		{idx: 253, key: "k253"},
		{idx: 257, key: "k257"},
		{idx: 260, key: "k260"},
	} {
		gotKey, ok := got.snapshotNoLock().String(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("reverse mapping mismatch at %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
		gotIdx, ok := got.snapshotNoLock().Index(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("forward mapping mismatch for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
	}

	for _, hole := range []uint64{252, 254, 255, 256, 258, 259} {
		if gotKey, ok := got.snapshotNoLock().String(hole); ok {
			t.Fatalf("expected hole at %d, got %q", hole, gotKey)
		}
	}
}

func TestSnapshotConcurrentIndexStringLookup(t *testing.T) {
	m := New(0, 256)

	const n = 64
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%02d", i)
		keys = append(keys, key)
		m.Create(key)
	}

	snap := m.Snapshot()
	start := make(chan struct{})
	errCh := make(chan error, len(keys))
	var wg sync.WaitGroup
	for i := range keys {
		key := keys[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			idx, ok := snap.Index(key)
			if !ok {
				errCh <- fmt.Errorf("missing idx for %q", key)
				return
			}
			back, ok := snap.String(idx)
			if !ok || back != key {
				errCh <- fmt.Errorf("round-trip mismatch for %q: idx=%d back=%q ok=%v", key, idx, back, ok)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotKeepsOriginalMappingAfterMapperMutation(t *testing.T) {
	m := New(0, 256)
	if idx, created := m.Create("snap-key"); idx != 1 || !created {
		t.Fatalf("Create(snap-key) = %d/%v, want 1/true", idx, created)
	}
	snap := m.Snapshot()
	if idx, created := m.Create("live-key"); idx != 2 || !created {
		t.Fatalf("Create(live-key) = %d/%v, want 2/true", idx, created)
	}

	if got, ok := snap.String(1); !ok || got != "snap-key" {
		t.Fatalf("snapshot reverse mismatch: got=%q ok=%v want snap-key", got, ok)
	}
	if _, ok := snap.Index("live-key"); ok {
		t.Fatalf("old snapshot saw future key")
	}

	latest := m.Snapshot()
	if got, ok := latest.String(2); !ok || got != "live-key" {
		t.Fatalf("latest reverse mismatch: got=%q ok=%v want live-key", got, ok)
	}
}

func TestSnapshotSurvivesMapperTruncateAndIDReuse(t *testing.T) {
	m := New(0, 256)
	for i, key := range []string{"seed-a", "seed-b", "seed-c"} {
		idx, created := m.Create(key)
		if idx != uint64(i+1) || !created {
			t.Fatalf("Create(%q) = %d/%v, want %d/true", key, idx, created, i+1)
		}
	}
	snap := m.Snapshot()
	if idx, ok := snap.Index("seed-a"); !ok || idx != 1 {
		t.Fatalf("snapshot Index(seed-a) before truncate = %d/%v, want 1/true", idx, ok)
	}

	m.Truncate()
	if idx, created := m.Create("new-a"); idx != 1 || !created {
		t.Fatalf("Create(new-a) after truncate = %d/%v, want 1/true", idx, created)
	}

	if idx, ok := snap.Index("seed-a"); !ok || idx != 1 {
		t.Fatalf("old snapshot Index(seed-a) after truncate = %d/%v, want 1/true", idx, ok)
	}
	if got, ok := snap.String(1); !ok || got != "seed-a" {
		t.Fatalf("old snapshot String(1) after id reuse = %q/%v, want seed-a/true", got, ok)
	}
	if idx, ok := snap.Index("new-a"); ok {
		t.Fatalf("old snapshot saw key created after truncate: idx=%d", idx)
	}

	latest := m.Snapshot()
	if got, ok := latest.String(1); !ok || got != "new-a" {
		t.Fatalf("latest String(1) = %q/%v, want new-a/true", got, ok)
	}
	if _, ok := latest.Index("seed-a"); ok {
		t.Fatalf("latest snapshot retained old key after truncate")
	}
}

func TestPublishedSnapshotsSurviveCompactionAndLiveStoragePoison(t *testing.T) {
	m := New(0, 2)
	for i, key := range []string{"seed-a", "seed-b"} {
		idx, created := m.Create(key)
		if idx != uint64(i+1) || !created {
			t.Fatalf("Create(%q) = %d/%v, want %d/true", key, idx, created, i+1)
		}
	}
	pinned := m.Snapshot()

	if idx, created := m.Create("delta-c"); idx != 3 || !created {
		t.Fatalf("Create(delta-c) = %d/%v, want 3/true", idx, created)
	}
	mid := m.Snapshot()

	if idx, created := m.Create("compact-d"); idx != 4 || !created {
		t.Fatalf("Create(compact-d) = %d/%v, want 4/true", idx, created)
	}
	latest := m.Snapshot()

	m.Lock()
	if m.snap == nil || m.snap.base != nil || m.snap.depth != 1 || m.snap.next != 4 {
		t.Fatalf("expected compacted full snapshot, got snap=%#v", m.snap)
	}
	for key := range m.keys {
		delete(m.keys, key)
	}
	for i := range m.strs {
		m.strs[i] = "poison"
	}
	m.Unlock()

	type mapping struct {
		key string
		idx uint64
	}
	for _, tc := range []struct {
		name       string
		snap       *Snapshot
		present    []mapping
		absentKeys []string
		absentIdxs []uint64
	}{
		{
			name:       "pinned",
			snap:       pinned,
			present:    []mapping{{key: "seed-a", idx: 1}, {key: "seed-b", idx: 2}},
			absentKeys: []string{"delta-c", "compact-d"},
			absentIdxs: []uint64{3, 4},
		},
		{
			name:       "mid",
			snap:       mid,
			present:    []mapping{{key: "seed-a", idx: 1}, {key: "seed-b", idx: 2}, {key: "delta-c", idx: 3}},
			absentKeys: []string{"compact-d"},
			absentIdxs: []uint64{4},
		},
		{
			name:    "latest",
			snap:    latest,
			present: []mapping{{key: "seed-a", idx: 1}, {key: "seed-b", idx: 2}, {key: "delta-c", idx: 3}, {key: "compact-d", idx: 4}},
		},
	} {
		for _, want := range tc.present {
			gotIdx, ok := tc.snap.Index(want.key)
			if !ok || gotIdx != want.idx {
				t.Fatalf("%s Index(%q) = %d/%v, want %d/true", tc.name, want.key, gotIdx, ok, want.idx)
			}
			gotKey, ok := tc.snap.String(want.idx)
			if !ok || gotKey != want.key {
				t.Fatalf("%s String(%d) = %q/%v, want %q/true", tc.name, want.idx, gotKey, ok, want.key)
			}
		}
		for _, key := range tc.absentKeys {
			if idx, ok := tc.snap.Index(key); ok {
				t.Fatalf("%s Index(%q) = %d/true, want absent", tc.name, key, idx)
			}
		}
		for _, idx := range tc.absentIdxs {
			if key, ok := tc.snap.String(idx); ok {
				t.Fatalf("%s String(%d) = %q/true, want absent", tc.name, idx, key)
			}
		}
	}
}

func TestSparseBasePagesStayPageLocalAfterDelta(t *testing.T) {
	baseStrs := map[uint64]string{
		1:   "k001",
		255: "k255",
		260: "k260",
		511: "k511",
		515: "k515",
	}
	baseKeys := map[string]uint64{
		"k001": 1,
		"k255": 255,
		"k260": 260,
		"k511": 511,
		"k515": 515,
	}

	m := New(uint64(len(baseStrs)), 256)
	m.replaceAllSparseNoLock(baseKeys, baseStrs, 600)

	if idx, created := m.createNoLock("k601"); idx != 601 || !created {
		t.Fatalf("unexpected appended idx: got=%d created=%v want=601/true", idx, created)
	}

	published := m.snapshotNoLock()
	if len(published.readDirs) == 0 {
		t.Fatal("expected published read pages after sparse-base delta publish")
	}

	base := m.snap.base
	if base == nil || base.strs == nil {
		t.Fatalf("expected sparse base snapshot after append: %#v", base)
	}

	for _, tc := range []struct {
		page      int
		wantLen   int
		absentIdx uint64
	}{
		{page: 0, wantLen: 2, absentIdx: 260},
		{page: 1, wantLen: 2, absentIdx: 1},
	} {
		readPage := published.readPageAtNoLock(tc.page)
		if readPage == nil || readPage.strs == nil {
			t.Fatalf("page %d missing sparse read page", tc.page)
		}
		if len(readPage.strs) != tc.wantLen {
			t.Fatalf("page %d sparse len = %d, want %d", tc.page, len(readPage.strs), tc.wantLen)
		}
		if _, ok := readPage.strs[tc.absentIdx]; ok {
			t.Fatalf("page %d unexpectedly retained idx %d from another page", tc.page, tc.absentIdx)
		}
	}

	if got := snapshotUsedCountNoLock(published); got != 6 {
		t.Fatalf("published used count = %d, want 6", got)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "k001", idx: 1},
		{key: "k260", idx: 260},
		{key: "k515", idx: 515},
		{key: "k601", idx: 601},
	} {
		gotIdx, ok := published.Index(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("forward mismatch for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := published.String(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("reverse mismatch for %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
}

func TestRollbackCreatedRestoresCommittedSnapshotBase(t *testing.T) {
	m := New(0, 256)
	for _, key := range []string{"seed-a", "seed-b", "seed-c"} {
		m.Create(key)
	}
	before := m.Snapshot()
	m.MarkCommittedPublished(before)
	if before.next != 3 {
		t.Fatalf("unexpected committed base before reject: next=%d", before.next)
	}
	beforeState := m.committed
	beforePublished := m.committedPub

	idx, created := m.Create("ghost-dup")
	if !created {
		t.Fatal("expected ghost key to be created")
	}
	m.RollbackCreated("ghost-dup", idx)
	if m.next != 3 {
		t.Fatalf("rollback next = %d, want 3", m.next)
	}
	if _, ok := m.keys["ghost-dup"]; ok {
		t.Fatalf("rollback retained ghost key")
	}
	if m.snap != beforeState || m.published != beforePublished || m.pubSource != beforeState || m.dirty {
		t.Fatalf("rollback did not restore committed state: snap=%p published=%p pubSource=%p dirty=%v", m.snap, m.published, m.pubSource, m.dirty)
	}

	idx, created = m.Create("real-ok")
	if idx != 4 || !created {
		t.Fatalf("real Create = %d/%v, want 4/true", idx, created)
	}
	m.Lock()
	latest := m.stateSnapshotNoLock()
	m.Unlock()
	if latest.base != beforeState {
		t.Fatalf("next snapshot did not reuse committed base: got=%p want=%p", latest.base, beforeState)
	}
	if latest.baseNextNoLock() != beforeState.next {
		t.Fatalf("latest delta base next = %d, want %d", latest.baseNextNoLock(), beforeState.next)
	}
	if got := snapshotOwnUsedCount(latest); got != 1 {
		t.Fatalf("latest delta own used count = %d, want 1", got)
	}
	if got, ok := latest.String(beforeState.next + 1); !ok || got != "real-ok" {
		t.Fatalf("latest delta reverse mapping mismatch: got=%q ok=%v", got, ok)
	}
}
