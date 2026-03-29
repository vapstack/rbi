package rbi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/posting"
	"go.etcd.io/bbolt"
)

func TestReadString_RoundTrip(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	const want = "plain-string"
	if err := writeString(writer, want); err != nil {
		t.Fatalf("writeString: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := readString(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("readString: %v", err)
	}
	if got != want {
		t.Fatalf("readString mismatch: got %q", got)
	}
}

func TestAddDistinctStrings_SinglePass(t *testing.T) {
	values := []string{"alpha", "beta", "alpha", "gamma", "beta"}
	var got []string
	calls := 0
	distinct := addDistinctStrings(len(values), func(i int) string {
		calls++
		return values[i]
	}, func(v string) {
		got = append(got, v)
	})
	if calls != len(values) {
		t.Fatalf("valueAt calls mismatch: got=%d want=%d", calls, len(values))
	}
	if distinct != 3 {
		t.Fatalf("distinct mismatch: got=%d want=3", distinct)
	}
	if !slices.Equal(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("unexpected distinct order: got=%v", got)
	}
}

func TestAddDistinctFixedKeys_SinglePass(t *testing.T) {
	values := []int64{5, 7, 5, 11, 7}
	var got []uint64
	calls := 0
	distinct := addDistinctFixedKeys(len(values), func(i int) uint64 {
		calls++
		return buildInt64Key(values[i])
	}, func(v uint64) {
		got = append(got, v)
	})
	if calls != len(values) {
		t.Fatalf("encode calls mismatch: got=%d want=%d", calls, len(values))
	}
	if distinct != 3 {
		t.Fatalf("distinct mismatch: got=%d want=3", distinct)
	}
	want := []uint64{buildInt64Key(5), buildInt64Key(7), buildInt64Key(11)}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected distinct order: got=%v want=%v", got, want)
	}
}

func TestReadStrMap_RoundTripDense(t *testing.T) {
	sm := &strMapSnapshot{
		Next:      5,
		Keys:      map[string]uint64{"alpha": 1, "charlie": 3, "echo": 5},
		DenseStrs: []string{"", "alpha", "", "charlie", "", "echo"},
		DenseUsed: []bool{false, true, false, true, false, true},
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
	}
	if got.Next != sm.Next {
		t.Fatalf("unexpected next: got=%d want=%d", got.Next, sm.Next)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "charlie", idx: 3},
		{key: "echo", idx: 5},
	} {
		gotIdx, ok := got.Keys[tc.key]
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		if got.Strs[tc.idx] != tc.key {
			t.Fatalf("unexpected dense string at %d: got=%q want=%q", tc.idx, got.Strs[tc.idx], tc.key)
		}
		if !got.strsUsed[tc.idx] {
			t.Fatalf("expected used flag at %d", tc.idx)
		}
	}
}

func TestQueryViewIdxMapping_UsesPinnedStrMapSnapshot(t *testing.T) {
	db := &DB[string, UserBench]{
		strkey:  true,
		strmap:  newStrMapper(0, defaultSnapshotStrMapCompactDepth),
		fields:  map[string]*field{},
		options: &Options{},
	}
	snap := &strMapSnapshot{
		Next: 1,
		Strs: map[uint64]string{1: "snap-key"},
	}
	viewSnap := &indexSnapshot{
		strmap:            snap,
		lenZeroComplement: map[string]bool{},
	}
	view := &queryView[string, UserBench]{
		root:              db,
		snap:              viewSnap,
		strkey:            true,
		strmapView:        snap,
		fields:            db.fields,
		planner:           &db.planner,
		options:           db.options,
		lenZeroComplement: viewSnap.lenZeroComplement,
	}

	db.strmap.Lock()
	db.strmap.createIdxNoLock("live-key")
	db.strmap.Unlock()

	if got := view.idFromIdxNoLock(1); got != "snap-key" {
		t.Fatalf("idFromIdxNoLock mismatch: got=%q want=%q", got, "snap-key")
	}
	if got, ok := view.keyFromIdx(1); !ok || got != "snap-key" {
		t.Fatalf("keyFromIdx mismatch: got=%q ok=%v want=%q", got, ok, "snap-key")
	}
}

func TestStrMapSnapshotGetStringNoLock_JumpsToAnchor(t *testing.T) {
	base := &strMapSnapshot{
		Next:      100,
		DenseStrs: make([]string, 101),
		DenseUsed: make([]bool, 101),
		depth:     1,
	}
	base.DenseStrs[1] = "alpha"
	base.DenseUsed[1] = true

	cur := base
	for i := uint64(101); i <= 110; i++ {
		cur = &strMapSnapshot{
			Next:   i,
			Strs:   map[uint64]string{i: fmt.Sprintf("k%d", i)},
			base:   cur,
			anchor: base,
			depth:  cur.depth + 1,
		}
	}

	if got, ok := cur.getStringNoLock(1); !ok || got != "alpha" {
		t.Fatalf("getStringNoLock(base lookup) mismatch: got=%q ok=%v", got, ok)
	}
	if got, ok := cur.getStringNoLock(110); !ok || got != "k110" {
		t.Fatalf("getStringNoLock(delta lookup) mismatch: got=%q ok=%v", got, ok)
	}
	if _, ok := cur.getStringNoLock(111); ok {
		t.Fatal("expected missing lookup above Next to fail")
	}
}

func TestReadStrMap_RoundTripSparseWithHoles(t *testing.T) {
	denseStrs := make([]string, 1001)
	denseUsed := make([]bool, 1001)
	denseStrs[1] = "alpha"
	denseUsed[1] = true
	denseStrs[1000] = "omega"
	denseUsed[1000] = true

	sm := &strMapSnapshot{
		Next:      1000,
		Keys:      map[string]uint64{"alpha": 1, "omega": 1000},
		DenseStrs: denseStrs,
		DenseUsed: denseUsed,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
	}
	if got.Next != sm.Next {
		t.Fatalf("unexpected next: got=%d want=%d", got.Next, sm.Next)
	}
	if len(got.Strs) != 0 || len(got.strsUsed) != 0 {
		t.Fatalf("expected sparse reverse map after load, got dense lens strs=%d used=%d", len(got.Strs), len(got.strsUsed))
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
		gotIdx, ok := got.Keys[tc.key]
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		if gotKey, ok := got.snapshotNoLock().getStringNoLock(tc.idx); !ok || gotKey != tc.key {
			t.Fatalf("unexpected reverse mapping at %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
}

func TestReadStrMap_RoundTripDeltaChain(t *testing.T) {
	base := &strMapSnapshot{
		Next:      2,
		Keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		DenseStrs: []string{"", "alpha", "bravo"},
		DenseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &strMapSnapshot{
		Next:  4,
		Keys:  map[string]uint64{"charlie": 3, "delta": 4},
		Strs:  map[uint64]string{3: "charlie", 4: "delta"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
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
		gotIdx, ok := got.snapshotNoLock().getIdxNoLock(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := got.snapshotNoLock().getStringNoLock(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("missing reverse mapping for %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
}

func TestWriteStrMapSnapshot_DeltaChainChoosesDenseByEffectiveSnapshot(t *testing.T) {
	base := &strMapSnapshot{
		Next:      2,
		Keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		DenseStrs: []string{"", "alpha", "bravo"},
		DenseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &strMapSnapshot{
		Next:  4,
		Keys:  map[string]uint64{"charlie": 3, "delta": 4},
		Strs:  map[uint64]string{3: "charlie", 4: "delta"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(payload.Bytes()))
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if next != sm.Next {
		t.Fatalf("unexpected next: got=%d want=%d", next, sm.Next)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read encoding: %v", err)
	}
	if enc != strMapEncodingDense {
		t.Fatalf("unexpected encoding: got=%d want=%d", enc, strMapEncodingDense)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
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
		gotIdx, ok := got.snapshotNoLock().getIdxNoLock(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
	}
}

func TestReadStrMap_RoundTripDeltaChainWithHole(t *testing.T) {
	base := &strMapSnapshot{
		Next:      2,
		Keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		DenseStrs: []string{"", "alpha", "bravo"},
		DenseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &strMapSnapshot{
		Next:  4,
		Keys:  map[string]uint64{"delta": 4},
		Strs:  map[uint64]string{4: "delta"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
	}
	if got.Next != sm.Next {
		t.Fatalf("unexpected next: got=%d want=%d", got.Next, sm.Next)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "bravo", idx: 2},
		{key: "delta", idx: 4},
	} {
		gotIdx, ok := got.snapshotNoLock().getIdxNoLock(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := got.snapshotNoLock().getStringNoLock(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("missing reverse mapping for %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
	if _, ok := got.snapshotNoLock().getStringNoLock(3); ok {
		t.Fatal("expected hole to stay absent after round-trip")
	}
}

func TestWriteStrMapSnapshot_SparseDeltaChainRoundTrip(t *testing.T) {
	base := &strMapSnapshot{
		Next:      2,
		Keys:      map[string]uint64{"alpha": 1, "bravo": 2},
		DenseStrs: []string{"", "alpha", "bravo"},
		DenseUsed: []bool{false, true, true},
		depth:     1,
	}
	sm := &strMapSnapshot{
		Next:  1000,
		Keys:  map[string]uint64{"omega": 1000},
		Strs:  map[uint64]string{1000: "omega"},
		base:  base,
		depth: 2,
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeStrMapSnapshot(writer, sm); err != nil {
		t.Fatalf("writeStrMapSnapshot: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(payload.Bytes()))
	next, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Fatalf("read next: %v", err)
	}
	if next != sm.Next {
		t.Fatalf("unexpected next: got=%d want=%d", next, sm.Next)
	}
	enc, err := reader.ReadByte()
	if err != nil {
		t.Fatalf("read encoding: %v", err)
	}
	if enc != strMapEncodingSparse {
		t.Fatalf("unexpected encoding: got=%d want=%d", enc, strMapEncodingSparse)
	}

	got, err := readStrMap(bufio.NewReader(bytes.NewReader(payload.Bytes())), 0)
	if err != nil {
		t.Fatalf("readStrMap: %v", err)
	}
	for _, tc := range []struct {
		key string
		idx uint64
	}{
		{key: "alpha", idx: 1},
		{key: "bravo", idx: 2},
		{key: "omega", idx: 1000},
	} {
		gotIdx, ok := got.snapshotNoLock().getIdxNoLock(tc.key)
		if !ok || gotIdx != tc.idx {
			t.Fatalf("missing key mapping for %q: got=%d ok=%v want=%d", tc.key, gotIdx, ok, tc.idx)
		}
		gotKey, ok := got.snapshotNoLock().getStringNoLock(tc.idx)
		if !ok || gotKey != tc.key {
			t.Fatalf("missing reverse mapping for %d: got=%q ok=%v want=%q", tc.idx, gotKey, ok, tc.key)
		}
	}
	if _, ok := got.snapshotNoLock().getStringNoLock(999); ok {
		t.Fatal("expected gap before sparse delta entry to stay absent after round-trip")
	}
}

func toPosting(ids ...uint64) posting.List {
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func indexTestSingleton(id uint64) posting.List {
	return (posting.List{}).BuildAdded(id)
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "alice", Age: 10, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 20, Tags: []string{"java"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1], got %v", ids)
	}

	st := db2.Stats()
	if st.KeyCount != 2 {
		t.Fatalf("expected Stats.KeyCount=2, got %d", st.KeyCount)
	}
}

func TestIndexPersistence_ChunkedFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_chunked.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	for i := 0; i < fieldIndexChunkThreshold+64; i++ {
		if err := db.Set(uint64(i+1), &Rec{
			Name:  fmt.Sprintf("user_%04d", i),
			Email: fmt.Sprintf("user_%04d@example.test", i),
			Age:   i,
		}); err != nil {
			t.Fatalf("Set(%d): %v", i+1, err)
		}
	}

	if storage, ok := db.index["name"]; !ok || !storage.isChunked() {
		t.Fatalf("expected chunked name index before close")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if storage, ok := db2.index["name"]; !ok || !storage.isChunked() {
		t.Fatalf("expected chunked name index after reopen")
	}

	ids, err := db2.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "name", Value: "user_0007"}})
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	if len(ids) != 1 || ids[0] != 8 {
		t.Fatalf("expected [8], got %v", ids)
	}
}

func TestIndexPersistence_LenZeroComplement_AllEmptyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist_len_zero_complement_all_empty.db")

	db, raw := openBoltAndNew[uint64, Rec](t, path)
	for i := 1; i <= 90; i++ {
		rec := &Rec{
			Name:  fmt.Sprintf("u_%d", i),
			Email: fmt.Sprintf("u_%d@example.test", i),
			Age:   i,
		}
		if i%5 == 0 {
			rec.Tags = []string{"go"}
		}
		if err := db.Set(uint64(i), rec); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if !db.isLenZeroComplementField("tags") {
		t.Fatalf("expected zero-complement mode before patching to empty")
	}

	for i := 1; i <= 90; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string(nil)}}); err != nil {
			t.Fatalf("Patch(%d): %v", i, err)
		}
	}

	want := make([]uint64, 0, 90)
	for i := 1; i <= 90; i++ {
		want = append(want, uint64(i))
	}
	gotBeforeClose, err := db.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) before close: %v", err)
	}
	if !slices.Equal(gotBeforeClose, want) {
		t.Fatalf("unexpected empty-tags ids before reopen: got=%v want=%v", gotBeforeClose, want)
	}

	if err = db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err = raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	db2, raw2 := openBoltAndNew[uint64, Rec](t, path)
	t.Cleanup(func() {
		if err = db2.Close(); err != nil {
			t.Fatal(err)
		}
		if err = raw2.Close(); err != nil {
			t.Fatal(err)
		}
	})

	gotAfterReopen, err := db2.QueryKeys(qx.Query(qx.EQ("tags", []string{})))
	if err != nil {
		t.Fatalf("QueryKeys(empty tags) after reopen: %v", err)
	}
	if !slices.Equal(gotAfterReopen, want) {
		t.Fatalf("unexpected empty-tags ids after reopen: got=%v want=%v", gotAfterReopen, want)
	}
}

func TestRebuildIndex_CleanState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(2, &Rec{Name: "bob", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(3, &Rec{Name: "charlie", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(2); err != nil {
		t.Fatal(err)
	}
	cnt, err := db.Count(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("before rebuild: expected 2, got %d", cnt)
	}

	if err = db.RebuildIndex(); err != nil {
		t.Fatal(err)
	}

	// verify state (1, 3)
	ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpEQ, Field: "age", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Fatalf("after rebuild: expected [1, 3], got %v", ids)
	}
}

func TestRebuildIndex_ClosedReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
}

func TestReadMethods_ReturnErrorWhenBucketMissing(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	expectBucketErr := func(op string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error, got nil", op)
		}
		if !strings.Contains(err.Error(), "bucket does not exist") {
			t.Fatalf("%s: unexpected error: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBucketErr("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBucketErr("BatchGet", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScan", err)

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBucketErr("SeqScanRaw", err)

	_, err = db.Query(qx.Query())
	expectBucketErr("Query", err)
}

func TestQuery_MissingBucket_EmptyIndexResultStillRequiresSequenceTx(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(db.BucketName()) == nil {
			return nil
		}
		return tx.DeleteBucket(db.BucketName())
	}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	items, err := db.Query(qx.Query(qx.EQ("age", 999_999)))
	if err == nil {
		t.Fatalf("expected missing bucket error, got items=%#v", items)
	}
}

func TestRebuildIndex_ScanErrorClearsActiveFlag(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.Bolt().Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.BucketName())
		if b == nil {
			return fmt.Errorf("bucket missing")
		}
		return b.Put(db.keyFromID(1), []byte{0xff})
	}); err != nil {
		t.Fatalf("corrupt value: %v", err)
	}

	err := db.RebuildIndex()
	if err == nil {
		t.Fatal("expected rebuild error on corrupted value")
	}
	if !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("expected scan error, got: %v", err)
	}

	// Rebuild must clear active flag even when build fails.
	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set after failed rebuild should not see busy flag, got: %v", err)
	}

	err = db.RebuildIndex()
	if err == nil || !strings.Contains(err.Error(), "scan error") {
		t.Fatalf("second RebuildIndex: expected scan error, got: %v", err)
	}
}

func TestRebuildIndex_StopTheWorld(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawRebuildBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawRebuildBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawRebuildBusy {
		t.Fatal("expected ErrRebuildInProgress while rebuild waits for in-flight reader")
	}

	select {
	case err := <-rebuildDone:
		t.Fatalf("rebuild must wait for in-flight reader, got early result: %v", err)
	default:
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
}

func TestRebuildIndex_ConcurrentCallReturnsErrRebuildInProgress(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := db.RebuildIndex(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from concurrent rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}

func TestRebuildIndex_StopsTruncateWhileActive(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Truncate(); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Truncate during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Truncate(); err != nil {
		t.Fatalf("Truncate(after rebuild): %v", err)
	}
}

func TestRebuildIndex_RejectsSetWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); !errors.Is(err, ErrRebuildInProgress) {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("expected ErrRebuildInProgress from Set during rebuild, got: %v", err)
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if err := db.Set(2, &Rec{Name: "bob", Age: 31}); err != nil {
		t.Fatalf("Set(after rebuild): %v", err)
	}
}

func TestRebuildIndex_ConcurrentCloseReturnsErrClosed(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
		scanDone <- err
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	closedDeadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(closedDeadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		close(releaseScan)
		<-scanDone
		<-closeDone
		<-rebuildDone
		t.Fatal("db.closed was not set by Close in time")
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out")
	}

	select {
	case err := <-rebuildDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed from rebuild racing with close, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RebuildIndex timed out")
	}
}

func TestRebuildIndex_WaitsForInFlightBatchedSet(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1024,
	})

	setStarted := make(chan struct{})
	releaseSet := make(chan struct{})
	setDone := make(chan error, 1)

	go func() {
		setDone <- db.Set(1, &Rec{Name: "alice", Age: 30}, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
			close(setStarted)
			<-releaseSet
			return nil
		}))
	}()

	select {
	case <-setStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("batched set callback did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseSet)
		<-setDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active while batched set is in flight")
	}

	select {
	case err := <-rebuildDone:
		close(releaseSet)
		<-setDone
		t.Fatalf("rebuild must wait for in-flight batched set, got early result: %v", err)
	default:
	}

	close(releaseSet)
	if err := <-setDone; err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	if st := db.AutoBatchStats(); st.Enqueued == 0 {
		t.Fatalf("expected auto-batch enqueue for Set path, got stats: %+v", st)
	}

	v, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v == nil || v.Age != 30 || v.Name != "alice" {
		t.Fatalf("unexpected value after rebuild: %#v", v)
	}

	ids, err := db.QueryKeys(qx.Query(qx.EQ("age", 30)))
	if err != nil {
		t.Fatalf("QueryKeys(age=30): %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected [1] after batched set + rebuild, got %v", ids)
	}
}

func TestRebuildIndex_StormConcurrentMixedOps_FinalConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   2 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 2048,
	})

	countries := []string{"NL", "PL", "DE", "US"}
	for i := 1; i <= 220; i++ {
		id := uint64(i)
		if err := db.Set(id, &Rec{
			Meta:     Meta{Country: countries[i%len(countries)]},
			Name:     fmt.Sprintf("seed-%03d", i),
			Age:      18 + (i % 60),
			Score:    float64(i%1000) / 10.0,
			Active:   i%2 == 0,
			Tags:     []string{fmt.Sprintf("grp-%d", i%7), "seed"},
			FullName: fmt.Sprintf("FN-%05d", i),
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	const (
		writers   = 4
		readers   = 3
		writerOps = 220
		readerOps = 320
		rebuilds  = 48
	)

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 25)).By("age", qx.ASC).Skip(2).Max(40),
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayCount("tags", qx.DESC).Skip(1).Max(55),
		qx.Query(qx.HASANY("tags", []string{"seed", "w0", "w1", "grp-2"})).ByArrayPos("country", []string{"NL", "DE", "PL", "US"}, qx.ASC).Max(70),
		qx.Query(qx.PREFIX("name", "rw-")).By("name", qx.ASC).Max(80),
		qx.Query(qx.IN("country", []string{"NL", "DE"})).ByArrayPos("tags", []string{"seed", "w0", "grp-1"}, qx.DESC).Skip(3).Max(60),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1+writers+readers)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rebuilds; i++ {
			if err := db.RebuildIndex(); err != nil {
				errCh <- fmt.Errorf("rebuild iter=%d: %w", i, err)
				return
			}
			if i%3 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writerOps; i++ {
				id := uint64((w*997+i*31+i*i)%260 + 1)
				switch (w + i) % 3 {
				case 0:
					rec := &Rec{
						Meta:     Meta{Country: countries[(w+i)%len(countries)]},
						Name:     fmt.Sprintf("rw-%02d-%04d-%03d", w, i, id),
						Age:      18 + ((w*13 + i) % 65),
						Score:    float64((w*1000+i*17)%1200) / 10.0,
						Active:   (w+i)%2 == 0,
						Tags:     []string{fmt.Sprintf("w%d", w%4), fmt.Sprintf("grp-%d", i%7)},
						FullName: fmt.Sprintf("FN-%05d", id),
					}
					if err := db.Set(id, rec); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Set(id=%d): %w", w, id, err)
						return
					}
				case 1:
					patch := []Field{
						{Name: "age", Value: 21 + ((w*7 + i) % 60)},
						{Name: "active", Value: i%2 == 0},
						{Name: "country", Value: countries[(w+i)%len(countries)]},
					}
					if err := db.Patch(id, patch); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Patch(id=%d): %w", w, id, err)
						return
					}
				default:
					if err := db.Delete(id); err != nil && !errors.Is(err, ErrRebuildInProgress) {
						errCh <- fmt.Errorf("writer=%d Delete(id=%d): %w", w, id, err)
						return
					}
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readerOps; i++ {
				q := queries[(r+i)%len(queries)]
				if _, err := db.QueryKeys(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d QueryKeys(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Query(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Query(i=%d): %w", r, i, err)
					return
				}
				if _, err := db.Count(q); err != nil && !errors.Is(err, ErrRebuildInProgress) {
					errCh <- fmt.Errorf("reader=%d Count(i=%d): %w", r, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(final): %v", err)
	}

	checkQueries := []*qx.QX{
		qx.Query(qx.GTE("age", 18)).By("age", qx.DESC).Skip(5).Max(70),
		qx.Query(qx.HASANY("tags", []string{"w0", "w1", "grp-3", "seed"})).ByArrayCount("tags", qx.ASC).Skip(4).Max(90),
		qx.Query(qx.IN("country", []string{"NL", "DE", "US"})).ByArrayPos("country", []string{"DE", "NL", "US", "PL"}, qx.ASC).Max(120),
		qx.Query(qx.NOT(qx.EQ("active", false))).ByArrayPos("tags", []string{"w0", "w1", "seed", "grp-1"}, qx.DESC).Skip(2).Max(85),
	}
	for i, q := range checkQueries {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("check QueryKeys(%d): %v", i, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("check expectedKeysUint64(%d): %v", i, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("check mismatch(%d): got=%v want=%v q=%+v", i, got, want, q)
		}
	}

	var seqCount uint64
	if err := db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		seqCount++
		return true, nil
	}); err != nil {
		t.Fatalf("SeqScan(final): %v", err)
	}
	cnt, err := db.Count(nil)
	if err != nil {
		t.Fatalf("Count(nil): %v", err)
	}
	if cnt != seqCount {
		t.Fatalf("final count mismatch: count=%d seqscan=%d", cnt, seqCount)
	}
}

func TestRebuildIndex_StatsBlockUntilRebuildCompletes(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	statsDone := make(chan IndexStats, 1)
	go func() {
		statsDone <- db.IndexStats()
	}()

	select {
	case st := <-statsDone:
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatalf("Stats returned while rebuild still active: %+v", st)
	case <-time.After(80 * time.Millisecond):
	}

	close(releaseScan)
	if err := <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err := <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	select {
	case st := <-statsDone:
		if st.Size == 0 {
			t.Fatalf("expected IndexStats.Size > 0, got %+v", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("IndexStats did not return after rebuild completion")
	}
}

func TestRebuildIndex_RejectsCoreOpsWhileActive(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "alice", Age: 30, Tags: []string{"go"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	scanStarted := make(chan struct{})
	releaseScan := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
			close(scanStarted)
			<-releaseScan
			return false, nil
		})
	}()

	select {
	case <-scanStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("scan did not start in time")
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- db.RebuildIndex()
	}()

	deadline := time.Now().Add(2 * time.Second)
	sawBusy := false
	for time.Now().Before(deadline) {
		_, err := db.Get(1)
		if errors.Is(err, ErrRebuildInProgress) {
			sawBusy = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawBusy {
		close(releaseScan)
		<-scanDone
		<-rebuildDone
		t.Fatal("expected rebuild to become active")
	}

	expectBusy := func(op string, err error) {
		t.Helper()
		if !errors.Is(err, ErrRebuildInProgress) {
			t.Fatalf("%s: expected ErrRebuildInProgress, got: %v", op, err)
		}
	}

	_, err := db.Get(1)
	expectBusy("Get", err)

	_, err = db.BatchGet(1, 2)
	expectBusy("BatchGet", err)

	err = db.ScanKeys(0, func(_ uint64) (bool, error) {
		return true, nil
	})
	expectBusy("ScanKeys", err)

	err = db.SeqScan(0, func(_ uint64, _ *Rec) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScan", err)

	err = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) {
		return true, nil
	})
	expectBusy("SeqScanRaw", err)

	_, err = db.QueryKeys(qx.Query())
	expectBusy("QueryKeys", err)

	_, err = db.Query(qx.Query())
	expectBusy("Query", err)

	_, err = db.Count(nil)
	expectBusy("Count", err)

	err = db.Set(2, &Rec{Name: "bob", Age: 31})
	expectBusy("Set", err)

	err = db.BatchSet(
		[]uint64{2, 3},
		[]*Rec{
			{Name: "setmany-2", Age: 21},
			{Name: "setmany-3", Age: 22},
		},
	)
	expectBusy("BatchSet", err)

	err = db.Patch(1, []Field{{Name: "age", Value: 35}})
	expectBusy("Patch", err)

	err = db.BatchPatch([]uint64{1, 2}, []Field{{Name: "age", Value: 37}})
	expectBusy("BatchPatch", err)

	err = db.Delete(1)
	expectBusy("Delete", err)

	err = db.BatchDelete([]uint64{1, 2})
	expectBusy("BatchDelete", err)

	err = db.Truncate()
	expectBusy("Truncate", err)

	err = db.RebuildIndex()
	expectBusy("RebuildIndex(second)", err)

	close(releaseScan)
	if err = <-scanDone; err != nil {
		t.Fatalf("SeqScan: %v", err)
	}
	if err = <-rebuildDone; err != nil {
		t.Fatalf("RebuildIndex(first): %v", err)
	}
}

/**/

type PtrIntRec struct {
	Name   string `db:"name"`
	Rank   *int   `db:"rank"`
	Active bool   `db:"active"`
}

func intPtr(v int) *int {
	return &v
}

func strPtr(v string) *string {
	return &v
}

func openTempDBUint64PtrInt(t *testing.T, options ...Options) (*DB[uint64, PtrIntRec], string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_uint64_ptrint.db")
	db, raw := openBoltAndNew[uint64, PtrIntRec](t, path, options...)

	t.Cleanup(func() {
		_ = db.Close()
		_ = raw.Close()
	})

	return db, path
}

func sortedIDsCopy(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	slices.Sort(out)
	return out
}

func assertSameSet(t *testing.T, got, want []uint64) {
	t.Helper()
	assertSameSlice(t, sortedIDsCopy(got), sortedIDsCopy(want))
}

func TestPointerNil_StringQueriesAndOrder(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	gotNil, err := db.QueryKeys(qx.Query(qx.EQ("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	assertSameSlice(t, gotNil, []uint64{1})

	gotIn, err := db.QueryKeys(qx.Query(qx.IN("opt", []any{nil, "alpha"})))
	if err != nil {
		t.Fatalf("QueryKeys(IN nil,alpha): %v", err)
	}
	assertSameSet(t, gotIn, []uint64{1, 3})

	gotNE, err := db.QueryKeys(qx.Query(qx.NE("opt", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(NE nil): %v", err)
	}
	assertSameSet(t, gotNE, []uint64{2, 3, 4, 5})

	gotPrefix, err := db.QueryKeys(qx.Query(qx.PREFIX("opt", "")))
	if err != nil {
		t.Fatalf("QueryKeys(PREFIX empty): %v", err)
	}
	assertSameSet(t, gotPrefix, []uint64{2, 3, 4, 5})

	gotSuffix, err := db.QueryKeys(qx.Query(qx.SUFFIX("opt", "ha")))
	if err != nil {
		t.Fatalf("QueryKeys(SUFFIX ha): %v", err)
	}
	assertSameSlice(t, gotSuffix, []uint64{3})

	gotContains, err := db.QueryKeys(qx.Query(qx.CONTAINS("opt", "il")))
	if err != nil {
		t.Fatalf("QueryKeys(CONTAINS il): %v", err)
	}
	assertSameSlice(t, gotContains, []uint64{4})

	for _, q := range []*qx.QX{
		qx.Query().By("opt", qx.ASC),
		qx.Query().By("opt", qx.DESC),
		qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(2),
		qx.Query(qx.EQ("active", true)).By("opt", qx.DESC).Skip(1).Max(2),
	} {
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("QueryKeys(%+v): %v", q, err)
		}
		want, err := expectedKeysUint64(t, db, q)
		if err != nil {
			t.Fatalf("expectedKeysUint64(%+v): %v", q, err)
		}
		assertSameSlice(t, got, want)

		_, prepared, _, _ := assertPreparedRouteEquivalence(t, db, q)
		assertSameSlice(t, prepared, want)
	}
}

func TestPointerNil_IntQueriesCountRebuildAndReopen(t *testing.T) {
	db, path := openTempDBUint64PtrInt(t)
	opts := Options{
		EnableAutoBatchStats: true,
		EnableSnapshotStats:  true,
	}

	rows := map[uint64]*PtrIntRec{
		1: {Name: "nil", Rank: nil, Active: true},
		2: {Name: "zero", Rank: intPtr(0), Active: true},
		3: {Name: "ten", Rank: intPtr(10), Active: false},
		4: {Name: "twenty", Rank: intPtr(20), Active: true},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	check := func(stage string, wantAsc, wantDesc, wantNil, wantIn []uint64, wantGT, wantCountNil, wantCountIn uint64) {
		t.Helper()

		gotNil, err := db.QueryKeys(qx.Query(qx.EQ("rank", nil)))
		if err != nil {
			t.Fatalf("%s QueryKeys(EQ nil): %v", stage, err)
		}
		assertSameSlice(t, gotNil, wantNil)

		gotIn, err := db.QueryKeys(qx.Query(qx.IN("rank", []any{nil, 10})))
		if err != nil {
			t.Fatalf("%s QueryKeys(IN nil,10): %v", stage, err)
		}
		assertSameSet(t, gotIn, wantIn)

		gotGT, err := db.QueryKeys(qx.Query(qx.GT("rank", 0)))
		if err != nil {
			t.Fatalf("%s QueryKeys(GT 0): %v", stage, err)
		}
		if uint64(len(gotGT)) != wantGT {
			t.Fatalf("%s GT count mismatch: got=%v wantCount=%d", stage, gotGT, wantGT)
		}

		gotAsc, err := db.QueryKeys(qx.Query().By("rank", qx.ASC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC): %v", stage, err)
		}
		assertSameSlice(t, gotAsc, wantAsc)

		gotDesc, err := db.QueryKeys(qx.Query().By("rank", qx.DESC))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank DESC): %v", stage, err)
		}
		assertSameSlice(t, gotDesc, wantDesc)

		gotAscPage, err := db.QueryKeys(qx.Query().By("rank", qx.ASC).Skip(1).Max(3))
		if err != nil {
			t.Fatalf("%s QueryKeys(By rank ASC page): %v", stage, err)
		}
		assertSameSlice(t, gotAscPage, wantAsc[1:])

		cntNil, err := db.Count(qx.Query(qx.EQ("rank", nil)))
		if err != nil {
			t.Fatalf("%s Count(EQ nil): %v", stage, err)
		}
		if cntNil != wantCountNil {
			t.Fatalf("%s Count(EQ nil) mismatch: got=%d want=%d", stage, cntNil, wantCountNil)
		}

		cntIn, err := db.Count(qx.Query(qx.IN("rank", []any{nil, 10})))
		if err != nil {
			t.Fatalf("%s Count(IN nil,10): %v", stage, err)
		}
		if cntIn != wantCountIn {
			t.Fatalf("%s Count(IN nil,10) mismatch: got=%d want=%d", stage, cntIn, wantCountIn)
		}
	}

	check("base", []uint64{2, 3, 4, 1}, []uint64{4, 3, 2, 1}, []uint64{1}, []uint64{1, 3}, 2, 1, 2)

	if err := db.Patch(1, []Field{{Name: "rank", Value: 15}}); err != nil {
		t.Fatalf("Patch(1 rank=15): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "rank", Value: (*int)(nil)}}); err != nil {
		t.Fatalf("Patch(2 rank=nil): %v", err)
	}
	if err := db.Patch(4, []Field{{Name: "rank", Value: 5}}); err != nil {
		t.Fatalf("Patch(4 rank=5): %v", err)
	}

	check("delta", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)

	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	check("rebuild", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)

	plannerStats := db.PlannerStats()
	fs, ok := plannerStats.Fields["rank"]
	if !ok {
		t.Fatalf("planner stats missing rank field")
	}
	if fs.DistinctKeys != 3 {
		t.Fatalf("planner stats distinct keys mismatch: got=%d want=3", fs.DistinctKeys)
	}

	indexStats := db.IndexStats()
	if got := indexStats.UniqueFieldKeys["rank"]; got != 3 {
		t.Fatalf("index stats unique keys mismatch: got=%d want=3", got)
	}
	if got := indexStats.FieldTotalCardinality["rank"]; got != 4 {
		t.Fatalf("index stats total cardinality mismatch: got=%d want=4", got)
	}
	if got := indexStats.FieldSize["rank"]; got == 0 {
		t.Fatalf("index stats field size should include nil family")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.bolt.Close(); err != nil {
		t.Fatalf("bolt.Close: %v", err)
	}
	db = nil

	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen raw: %v", err)
	}
	defer func() {
		if err := raw.Close(); err != nil {
			t.Fatalf("raw.Close: %v", err)
		}
	}()

	reopened, err := New[uint64, PtrIntRec](raw, opts)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("reopened.Close: %v", err)
		}
	}()
	db = reopened

	check("reopen", []uint64{4, 3, 1, 2}, []uint64{1, 3, 4, 2}, []uint64{2}, []uint64{2, 3}, 3, 1, 2)
}

func TestPointerNil_RebuildClearsStaleNilBase(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t)

	if err := db.Set(1, &PtrIntRec{Name: "nil", Rank: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &PtrIntRec{Name: "ten", Rank: intPtr(10)}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(initial): %v", err)
	}

	if err := db.Patch(1, []Field{{Name: "rank", Value: 20}}); err != nil {
		t.Fatalf("Patch(1 rank=20): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex(after patch): %v", err)
	}

	gotNil, err := db.QueryKeys(qx.Query(qx.EQ("rank", nil)))
	if err != nil {
		t.Fatalf("QueryKeys(EQ nil): %v", err)
	}
	if len(gotNil) != 0 {
		t.Fatalf("expected no nil rows after rebuild, got %v", gotNil)
	}

	gotIn, err := db.QueryKeys(qx.Query(qx.IN("rank", []any{nil, 20})))
	if err != nil {
		t.Fatalf("QueryKeys(IN nil,20): %v", err)
	}
	assertSameSlice(t, gotIn, []uint64{1})
}

func TestPlanCandidateOrder_SkipsPointerSortField(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Meta: Meta{Country: "NL"}, Name: "a", Opt: nil, Active: true},
		2: {Meta: Meta{Country: "DE"}, Name: "b", Opt: strPtr("beta"), Active: true},
		3: {Meta: Meta{Country: "PL"}, Name: "c", Opt: strPtr("alpha"), Active: true},
		4: {Meta: Meta{Country: "NL"}, Name: "d", Opt: strPtr("gamma"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	q := qx.Query(
		qx.NOT(qx.EQ("active", false)),
		qx.NOT(qx.EQ("country", "PL")),
	).By("opt", qx.ASC).Max(10)

	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)

	nq := normalizeQueryForTest(q)
	planOut, ok, err := db.tryPlan(nq, nil)
	if err != nil {
		t.Fatalf("tryPlan: %v", err)
	}
	if ok {
		t.Fatalf("expected tryPlan to skip pointer ORDER fast paths, got %v", planOut)
	}
}

func TestPointerNil_OrderExecutionFastPaths(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}

	qLimit := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Max(3)
	limitLeaves := mustExtractAndLeaves(t, qLimit.Expr)
	out, used, err := db.tryLimitQueryOrderBasic(qLimit, limitLeaves, nil)
	if err != nil {
		t.Fatalf("tryLimitQueryOrderBasic: %v", err)
	}
	if !used {
		t.Fatalf("expected tryLimitQueryOrderBasic to be used")
	}
	want, err := expectedKeysUint64(t, db, qLimit)
	if err != nil {
		t.Fatalf("expectedKeysUint64(limit): %v", err)
	}
	assertSameSlice(t, out, want)

	qOffset := qx.Query(qx.EQ("active", true)).By("opt", qx.ASC).Skip(1).Max(2)
	out, used, err = db.tryQueryOrderBasicWithLimit(qOffset, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderBasicWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected tryQueryOrderBasicWithLimit to be used")
	}
	want, err = expectedKeysUint64(t, db, qOffset)
	if err != nil {
		t.Fatalf("expectedKeysUint64(offset): %v", err)
	}
	assertSameSlice(t, out, want)

	qPrefix := qx.Query(qx.PREFIX("opt", "")).By("opt", qx.ASC).Skip(1).Max(2)
	out, used, err = db.tryQueryOrderPrefixWithLimit(qPrefix, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderPrefixWithLimit: %v", err)
	}
	if !used {
		t.Fatalf("expected tryQueryOrderPrefixWithLimit to be used")
	}
	want, err = expectedKeysUint64(t, db, qPrefix)
	if err != nil {
		t.Fatalf("expectedKeysUint64(prefix): %v", err)
	}
	assertSameSlice(t, out, want)

	qPrefixDesc := qx.Query(qx.PREFIX("opt", "")).By("opt", qx.DESC).Skip(1).Max(2)
	out, used, err = db.tryQueryOrderPrefixWithLimit(qPrefixDesc, nil)
	if err != nil {
		t.Fatalf("tryQueryOrderPrefixWithLimit(desc): %v", err)
	}
	if !used {
		t.Fatalf("expected tryQueryOrderPrefixWithLimit(desc) to be used")
	}
	want, err = expectedKeysUint64(t, db, qPrefixDesc)
	if err != nil {
		t.Fatalf("expectedKeysUint64(prefix desc): %v", err)
	}
	assertSameSlice(t, out, want)
}

func TestPointerNil_OrderSmallSlice_AllNilField(t *testing.T) {
	db, _ := openTempDBUint64PtrInt(t)

	if err := db.Set(1, &PtrIntRec{Name: "a", Rank: nil}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &PtrIntRec{Name: "b", Rank: nil}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := qx.Query().By("rank", qx.ASC).Max(2)
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	assertSameSlice(t, got, []uint64{1, 2})
}

func TestPointerNil_ExecPlanOrderedBasic_BaseNilTail(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	q := normalizeQueryForTest(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).By("opt", qx.ASC).Skip(1).Max(3))

	leaves, ok := collectAndLeaves(q.Expr)
	if !ok {
		t.Fatalf("collectAndLeaves: ok=false")
	}
	window, _ := orderWindow(q)
	preds, ok := db.buildPredicatesOrderedWithMode(leaves, "opt", false, window, true, true)
	if !ok {
		t.Fatalf("buildPredicatesOrderedWithMode: ok=false")
	}
	defer releasePredicates(preds)

	got, ok := db.execPlanOrderedBasic(q, preds, nil)
	if !ok {
		t.Fatalf("execPlanOrderedBasic: ok=false")
	}
	want, err := db.execPreparedQuery(q)
	if err != nil {
		t.Fatalf("execPreparedQuery: %v", err)
	}
	assertSameSlice(t, got, want)
}

func TestPointerNil_TryPlanOrdered_AllowsPointerSortField(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:        -1,
		CalibrationEnabled:     true,
		CalibrationSampleEvery: -1,
	})

	rows := map[uint64]*Rec{
		1: {Name: "nil", Opt: nil, Active: true},
		2: {Name: "empty", Opt: strPtr(""), Active: true},
		3: {Name: "alpha", Opt: strPtr("alpha"), Active: false},
		4: {Name: "nilish", Opt: strPtr("nilish"), Active: true},
		5: {Name: "beta", Opt: strPtr("beta"), Active: false},
	}
	for id, rec := range rows {
		if err := db.Set(id, rec); err != nil {
			t.Fatalf("Set(%d): %v", id, err)
		}
	}
	if err := db.RebuildIndex(); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "opt", Value: "zeta"}}); err != nil {
		t.Fatalf("Patch(1 opt=zeta): %v", err)
	}
	if err := db.Patch(2, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
		t.Fatalf("Patch(2 opt=nil): %v", err)
	}

	if err := db.SetCalibrationSnapshot(CalibrationSnapshot{
		UpdatedAt: time.Now(),
		Multipliers: map[string]float64{
			string(PlanOrdered):         0.01,
			string(PlanLimitOrderBasic): 100,
		},
		Samples: map[string]uint64{
			string(PlanOrdered):         1,
			string(PlanLimitOrderBasic): 1,
		},
	}); err != nil {
		t.Fatalf("SetCalibrationSnapshot: %v", err)
	}

	q := normalizeQueryForTest(qx.Query(
		qx.NOT(qx.EQ("active", false)),
	).By("opt", qx.DESC).Skip(1).Max(2))

	got, ok, err := db.tryPlan(q, nil)
	if err != nil {
		t.Fatalf("tryPlan: %v", err)
	}
	if !ok {
		t.Fatalf("expected tryPlan to use ordered planner path for pointer sort field")
	}
	want, err := db.execPreparedQuery(q)
	if err != nil {
		t.Fatalf("execPreparedQuery: %v", err)
	}
	assertSameSlice(t, got, want)
}

/**/

type indexExtOverlayFixture struct {
	keys    []string
	entries []index
	flat    fieldOverlay
	chunked fieldOverlay
	root    *fieldIndexChunkedRoot
	fixed8  bool
}

type indexExtDeltaOp struct {
	key    string
	add    []uint64
	remove []uint64
}

func indexExtPosting(start uint64, card int) posting.List {
	if card <= 0 {
		return posting.List{}
	}
	var out posting.List
	for i := 0; i < card; i++ {
		out = out.BuildAdded(start + uint64(i))
	}
	return out
}

func indexExtPostingFromIDs(ids []uint64) posting.List {
	if len(ids) == 0 {
		return posting.List{}
	}
	var out posting.List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func indexExtPostingIDs(ids posting.List) []uint64 {
	return ids.ToArray()
}

func indexExtAssertSameSlice[T comparable](t *testing.T, got, want []T) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("slice mismatch\ngot=%v\nwant=%v", got, want)
	}
}

func indexExtSortedStringKeys() []string {
	keys := make([]string, 0, fieldIndexChunkThreshold+32)
	for i := 0; i < 140; i++ {
		keys = append(keys, fmt.Sprintf("aa/%03d", i))
	}
	for i := 0; i < 132; i++ {
		keys = append(keys, fmt.Sprintf("ab/%03d", i))
	}
	for i := 0; i < 126; i++ {
		keys = append(keys, fmt.Sprintf("b/%03d", i))
	}
	keys = append(keys, "\xff", "\xff\x00", "\xff\xff")
	slices.Sort(keys)
	return keys
}

func indexExtSortedNumericKeys() []string {
	values := make([]uint64, 0, fieldIndexChunkThreshold+64)
	for i := 0; i < 150; i++ {
		values = append(values, uint64(0x10)<<56|uint64(i))
	}
	for i := 0; i < 140; i++ {
		values = append(values, uint64(0x20)<<56|uint64(i))
	}
	for i := 0; i < 120; i++ {
		values = append(values, uint64(0x20)<<56|uint64(0x8000+i))
	}
	for i := 0; i < 12; i++ {
		values = append(values, uint64(0xff)<<56|uint64(i))
	}
	slices.Sort(values)
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = uint64ByteStr(v)
	}
	return out
}

func indexExtNewOverlayFixture(t *testing.T, keys []string, fixed8 bool) indexExtOverlayFixture {
	t.Helper()

	entries := make([]index, len(keys))
	for i, key := range keys {
		entries[i] = index{
			Key: indexKeyFromStoredString(key, fixed8),
			IDs: indexExtPosting(uint64(i*64+1), i%11+1),
		}
	}
	slices.SortFunc(entries, func(a, b index) int {
		return compareIndexKeys(a.Key, b.Key)
	})

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root for %d keys", len(entries))
	}

	flatEntries := append([]index(nil), entries...)
	sortedKeys := make([]string, len(entries))
	for i := range entries {
		sortedKeys[i] = entries[i].Key.asUnsafeString()
	}

	return indexExtOverlayFixture{
		keys:    sortedKeys,
		entries: flatEntries,
		flat:    newFieldOverlay(&flatEntries),
		chunked: fieldOverlay{chunked: root},
		root:    root,
		fixed8:  fixed8,
	}
}

func indexExtNewSingletonOverlayFixture(t *testing.T, keys []string, fixed8 bool) indexExtOverlayFixture {
	t.Helper()

	entries := make([]index, len(keys))
	for i, key := range keys {
		entries[i] = index{
			Key: indexKeyFromStoredString(key, fixed8),
			IDs: indexTestSingleton(uint64(i + 1)),
		}
	}
	slices.SortFunc(entries, func(a, b index) int {
		return compareIndexKeys(a.Key, b.Key)
	})

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root for %d keys", len(entries))
	}

	flatEntries := append([]index(nil), entries...)
	sortedKeys := make([]string, len(entries))
	for i := range entries {
		sortedKeys[i] = entries[i].Key.asUnsafeString()
	}

	return indexExtOverlayFixture{
		keys:    sortedKeys,
		entries: flatEntries,
		flat:    newFieldOverlay(&flatEntries),
		chunked: fieldOverlay{chunked: root},
		root:    root,
		fixed8:  fixed8,
	}
}

func indexExtStringFixture(t *testing.T) indexExtOverlayFixture {
	t.Helper()
	return indexExtNewOverlayFixture(t, indexExtSortedStringKeys(), false)
}

func indexExtNumericFixture(t *testing.T) indexExtOverlayFixture {
	t.Helper()
	return indexExtNewOverlayFixture(t, indexExtSortedNumericKeys(), true)
}

func indexExtMultiPageStringFixture(t *testing.T) indexExtOverlayFixture {
	t.Helper()

	total := fieldIndexChunkTargetEntries*(fieldIndexDirPageTargetRefs+1) + fieldIndexChunkTargetEntries/2 + 17
	keys := make([]string, 0, total)
	for i := 0; i < total; i++ {
		switch {
		case i < total/3:
			keys = append(keys, fmt.Sprintf("mp/aa/%05d", i))
		case i < 2*total/3:
			keys = append(keys, fmt.Sprintf("mp/ab/%05d", i))
		default:
			keys = append(keys, fmt.Sprintf("mp/b/%05d", i))
		}
	}

	fx := indexExtNewSingletonOverlayFixture(t, keys, false)
	if len(fx.root.pages) < 2 {
		t.Fatalf("expected multi-page root, got %d pages", len(fx.root.pages))
	}
	return fx
}

func indexExtCollectCursor(ov fieldOverlay, br overlayRange, desc bool) ([]string, []uint64) {
	cur := ov.newCursor(br, desc)
	keys := make([]string, 0, max(0, br.baseEnd-br.baseStart))
	cards := make([]uint64, 0, max(0, br.baseEnd-br.baseStart))
	for {
		key, ids, ok := cur.next()
		if !ok {
			return keys, cards
		}
		keys = append(keys, key.asUnsafeString())
		cards = append(cards, ids.Cardinality())
	}
}

func indexExtRangeWindows(n int) [][2]int {
	return [][2]int{
		{0, 0},
		{0, 1},
		{0, 5},
		{1, 7},
		{fieldIndexChunkTargetEntries - 1, fieldIndexChunkTargetEntries + 3},
		{fieldIndexChunkTargetEntries + 11, fieldIndexChunkThreshold - 5},
		{n/2 - 7, n/2 + 7},
		{n - 5, n},
		{0, n},
		{n, n},
	}
}

func indexExtNormalizeWindow(start, end, n int) (int, int) {
	start = max(0, min(start, n))
	end = max(0, min(end, n))
	if start > end {
		start = end
	}
	return start, end
}

func indexExtAssertRangeEquivalent(
	t *testing.T,
	flat, chunked fieldOverlay,
	brFlat, brChunk overlayRange,
	desc bool,
) {
	t.Helper()
	if brFlat.baseStart != brChunk.baseStart || brFlat.baseEnd != brChunk.baseEnd {
		t.Fatalf(
			"range rank mismatch: flat=[%d,%d) chunked=[%d,%d)",
			brFlat.baseStart,
			brFlat.baseEnd,
			brChunk.baseStart,
			brChunk.baseEnd,
		)
	}

	wantKeys, wantCards := indexExtCollectCursor(flat, brFlat, desc)
	gotKeys, gotCards := indexExtCollectCursor(chunked, brChunk, desc)
	indexExtAssertSameSlice(t, gotKeys, wantKeys)
	indexExtAssertSameSlice(t, gotCards, wantCards)
}

func indexExtBoundCase(t *testing.T, ops ...struct {
	op  qx.Op
	key string
}) rangeBounds {
	t.Helper()
	rb := rangeBounds{has: true}
	for _, op := range ops {
		if !rb.applyOp(op.op, op.key) {
			t.Fatalf("applyOp(%v, %q) failed", op.op, op.key)
		}
	}
	return rb
}

func indexExtStorageKeysAndPostings(storage fieldIndexStorage) ([]string, [][]uint64) {
	ov := newFieldOverlayStorage(storage)
	br := ov.rangeByRanks(0, ov.keyCount())
	keys, _ := indexExtCollectCursor(ov, br, false)
	posts := make([][]uint64, 0, ov.keyCount())
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			return keys, posts
		}
		posts = append(posts, indexExtPostingIDs(ids))
	}
}

func indexExtDeltaKeys(ops []indexExtDeltaOp, fixed8 bool) []keyedBatchPostingDelta {
	out := make([]keyedBatchPostingDelta, 0, len(ops))
	for _, op := range ops {
		out = append(out, keyedBatchPostingDelta{
			key: indexKeyFromStoredString(op.key, fixed8),
			delta: batchPostingDelta{
				add:    indexExtPostingFromIDs(op.add),
				remove: indexExtPostingFromIDs(op.remove),
			},
		})
	}
	slices.SortFunc(out, func(a, b keyedBatchPostingDelta) int {
		return compareIndexKeys(a.key, b.key)
	})
	return out
}

func indexExtExpectedMapFromEntries(entries []index) map[string][]uint64 {
	out := make(map[string][]uint64, len(entries))
	for i := range entries {
		out[entries[i].Key.asUnsafeString()] = indexExtPostingIDs(entries[i].IDs)
	}
	return out
}

func indexExtApplyOpsExpected(expected map[string][]uint64, ops []indexExtDeltaOp) {
	for _, op := range ops {
		switch {
		case len(op.remove) != 0 && len(op.add) == 0:
			delete(expected, op.key)
		case len(op.add) != 0:
			expected[op.key] = append([]uint64(nil), op.add...)
		}
	}
}

func indexExtAssertStorageMatchesExpected(
	t *testing.T,
	storage fieldIndexStorage,
	expected map[string][]uint64,
) {
	t.Helper()

	keys, postings := indexExtStorageKeysAndPostings(storage)
	wantKeys := make([]string, 0, len(expected))
	for key := range expected {
		wantKeys = append(wantKeys, key)
	}
	sort.Strings(wantKeys)
	indexExtAssertSameSlice(t, keys, wantKeys)
	if len(postings) != len(wantKeys) {
		t.Fatalf("posting count mismatch: got=%d want=%d", len(postings), len(wantKeys))
	}
	for i, key := range wantKeys {
		indexExtAssertSameSlice(t, postings[i], expected[key])
	}
}

func indexExtAssertStorageMatchesEntries(t *testing.T, storage fieldIndexStorage, entries []index) {
	t.Helper()

	keys, postings := indexExtStorageKeysAndPostings(storage)
	wantKeys := make([]string, len(entries))
	wantPosts := make([][]uint64, len(entries))
	for i := range entries {
		wantKeys[i] = entries[i].Key.asUnsafeString()
		wantPosts[i] = indexExtPostingIDs(entries[i].IDs)
	}

	indexExtAssertSameSlice(t, keys, wantKeys)
	if len(postings) != len(wantPosts) {
		t.Fatalf("posting count mismatch: got=%d want=%d", len(postings), len(wantPosts))
	}
	for i := range wantPosts {
		indexExtAssertSameSlice(t, postings[i], wantPosts[i])
	}
}

func indexExtAssertOverlayMembership(t *testing.T, ov fieldOverlay, key string, id uint64, want bool) {
	t.Helper()
	if got := ov.lookupPostingRetained(key).Contains(id); got != want {
		t.Fatalf("lookup(%q).Contains(%d): got=%v want=%v", key, id, got, want)
	}
}

func indexExtBatchSetGenerated(t *testing.T, db *DB[uint64, Rec], start, end int, gen func(i int) *Rec) {
	t.Helper()
	if start > end {
		return
	}

	batchIDs := make([]uint64, 0, min(128, end-start+1))
	batchVals := make([]*Rec, 0, cap(batchIDs))
	flush := func() {
		if len(batchIDs) == 0 {
			return
		}
		if err := db.BatchSet(batchIDs, batchVals); err != nil {
			t.Fatalf("BatchSet(start=%d,end=%d,size=%d): %v", start, end, len(batchIDs), err)
		}
		batchIDs = batchIDs[:0]
		batchVals = batchVals[:0]
	}

	for i := start; i <= end; i++ {
		batchIDs = append(batchIDs, uint64(i))
		batchVals = append(batchVals, gen(i))
		if len(batchIDs) == cap(batchIDs) {
			flush()
		}
	}
	flush()
}

func indexExtAssertQueryKeysExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) []uint64 {
	t.Helper()
	got, err := db.QueryKeys(q)
	if err != nil {
		t.Fatalf("QueryKeys: %v", err)
	}
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	assertSameSlice(t, got, want)
	return want
}

func indexExtAssertCountExpected(t *testing.T, db *DB[uint64, Rec], q *qx.QX) {
	t.Helper()
	want, err := expectedKeysUint64(t, db, q)
	if err != nil {
		t.Fatalf("expectedKeysUint64: %v", err)
	}
	got, err := db.Count(q)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != uint64(len(want)) {
		t.Fatalf("Count mismatch: got=%d want=%d", got, len(want))
	}
}

func indexExtFloatSignedZeroChunkedDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("fzero/%03d", i),
			Age:    i,
			Score:  float64(i + 10),
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id    uint64
		score float64
	}{
		{id: 1, score: 0.0},
		{id: 2, score: math.Copysign(0, -1)},
		{id: 3, score: -1.0},
		{id: 4, score: 1.0},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "score", Value: tc.score}}); err != nil {
			t.Fatalf("Patch(score %d): %v", tc.id, err)
		}
	}

	if db.fieldOverlay("score").chunked == nil {
		t.Fatalf("expected chunked score overlay")
	}

	return db
}

func indexExtFloatNaNChunkedDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("fnan/%03d", i),
			Age:    i,
			Score:  float64(i + 1000),
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id    uint64
		score float64
	}{
		{id: 1, score: math.NaN()},
		{id: 2, score: math.Inf(-1)},
		{id: 3, score: -1.0},
		{id: 4, score: 0.0},
		{id: 5, score: 1.0},
		{id: 6, score: math.Inf(1)},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "score", Value: tc.score}}); err != nil {
			t.Fatalf("Patch(score %d): %v", tc.id, err)
		}
	}

	if db.fieldOverlay("score").chunked == nil {
		t.Fatalf("expected chunked score overlay")
	}

	return db
}

func indexExtNumericCoercionChunkedDB(t *testing.T) *DB[uint64, Rec] {
	t.Helper()

	db, _ := openTempDBUint64(t)
	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("num/%03d", i),
			Age:    i,
			Score:  float64(i),
			Active: i%2 == 0,
		}
	})

	if db.fieldOverlay("age").chunked == nil {
		t.Fatalf("expected chunked age overlay")
	}
	if db.fieldOverlay("score").chunked == nil {
		t.Fatalf("expected chunked score overlay")
	}

	return db
}

func TestIndexExt_StringLowerBoundMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	probes := []string{
		"",
		"aa/",
		"aa/000",
		"aa/071",
		"aa/999",
		"ab/000",
		"ab/131",
		"ab/999",
		"b/000",
		"b/999",
		"c/",
		"\xff",
		"\xff\x00",
		"\xff\xff",
		"\xff\xff\xff",
	}
	for _, probe := range probes {
		if got, want := fx.chunked.lowerBound(probe), fx.flat.lowerBound(probe); got != want {
			t.Fatalf("lowerBound(%q): got=%d want=%d", probe, got, want)
		}
	}
}

func TestIndexExt_StringUpperBoundMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	probes := []string{
		"",
		"aa/",
		"aa/000",
		"aa/071",
		"aa/999",
		"ab/000",
		"ab/131",
		"ab/999",
		"b/000",
		"b/999",
		"c/",
		"\xff",
		"\xff\x00",
		"\xff\xff",
		"\xff\xff\xff",
	}
	for _, probe := range probes {
		if got, want := fx.chunked.upperBound(probe), fx.flat.upperBound(probe); got != want {
			t.Fatalf("upperBound(%q): got=%d want=%d", probe, got, want)
		}
	}
}

func TestIndexExt_StringPrefixRangeEndMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	cases := []struct {
		prefix string
		start  int
	}{
		{prefix: "", start: 0},
		{prefix: "", start: fx.flat.keyCount() / 2},
		{prefix: "aa/", start: 0},
		{prefix: "aa/", start: fx.flat.lowerBound("aa/050")},
		{prefix: "aa/", start: fx.flat.lowerBound("ab/000")},
		{prefix: "ab/09", start: fx.flat.lowerBound("ab/090")},
		{prefix: "ab/09", start: fx.flat.lowerBound("b/000")},
		{prefix: "b/", start: fx.flat.lowerBound("b/010")},
		{prefix: "missing/", start: 0},
		{prefix: "\xff", start: fx.flat.lowerBound("\xff")},
		{prefix: "\xff\xff", start: fx.flat.lowerBound("\xff\xff")},
	}
	for _, tc := range cases {
		if got, want := fx.chunked.prefixRangeEnd(tc.prefix, tc.start), fx.flat.prefixRangeEnd(tc.prefix, tc.start); got != want {
			t.Fatalf("prefixRangeEnd(prefix=%q,start=%d): got=%d want=%d", tc.prefix, tc.start, got, want)
		}
	}
}

func TestIndexExt_StringRangeForBoundsMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	cases := []rangeBounds{
		{has: true, empty: true},
		indexExtBoundCase(t, struct {
			op  qx.Op
			key string
		}{op: qx.OpPREFIX, key: "aa/"}),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpPREFIX, key: "aa/"},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpGTE, key: "aa/050"},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLT, key: "aa/101"},
		),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpGT, key: "ab/010"},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLTE, key: "b/003"},
		),
		indexExtBoundCase(t, struct {
			op  qx.Op
			key string
		}{op: qx.OpEQ, key: "\xff"}),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpPREFIX, key: "\xff"},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLTE, key: "\xff\xff"},
		),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpGTE, key: "zzzz"},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLTE, key: "zzzz"},
		),
	}

	for _, rb := range cases {
		indexExtAssertRangeEquivalent(t, fx.flat, fx.chunked, fx.flat.rangeForBounds(rb), fx.chunked.rangeForBounds(rb), false)
	}
}

func TestIndexExt_StringCursorAscMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	for _, window := range indexExtRangeWindows(len(fx.keys)) {
		start, end := indexExtNormalizeWindow(window[0], window[1], len(fx.keys))
		indexExtAssertRangeEquivalent(t, fx.flat, fx.chunked, fx.flat.rangeByRanks(start, end), fx.chunked.rangeByRanks(start, end), false)
	}
}

func TestIndexExt_StringCursorDescMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	for _, window := range indexExtRangeWindows(len(fx.keys)) {
		start, end := indexExtNormalizeWindow(window[0], window[1], len(fx.keys))
		indexExtAssertRangeEquivalent(t, fx.flat, fx.chunked, fx.flat.rangeByRanks(start, end), fx.chunked.rangeByRanks(start, end), true)
	}
}

func TestIndexExt_StringPostingAtMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	for i := range fx.keys {
		indexExtAssertSameSlice(t, indexExtPostingIDs(fx.chunked.postingAt(i)), indexExtPostingIDs(fx.flat.postingAt(i)))
		indexExtAssertSameSlice(
			t,
			indexExtPostingIDs(fx.chunked.lookupPostingRetained(fx.keys[i])),
			indexExtPostingIDs(fx.flat.lookupPostingRetained(fx.keys[i])),
		)
	}
	if !fx.chunked.lookupPostingRetained("missing/key").IsEmpty() {
		t.Fatalf("expected empty lookup for missing key")
	}
}

func TestIndexExt_StringOverlayRangeStatsMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	for _, window := range indexExtRangeWindows(len(fx.keys)) {
		start, end := indexExtNormalizeWindow(window[0], window[1], len(fx.keys))
		gotBuckets, gotRows := overlayRangeStats(fx.chunked, fx.chunked.rangeByRanks(start, end))
		wantBuckets, wantRows := overlayRangeStats(fx.flat, fx.flat.rangeByRanks(start, end))
		if gotBuckets != wantBuckets || gotRows != wantRows {
			t.Fatalf(
				"overlayRangeStats(%d,%d): got=(%d,%d) want=(%d,%d)",
				start,
				end,
				gotBuckets,
				gotRows,
				wantBuckets,
				wantRows,
			)
		}
	}
}

func TestIndexExt_StringOverlayUnionRangeMatchesFlat(t *testing.T) {
	fx := indexExtStringFixture(t)
	for _, window := range indexExtRangeWindows(len(fx.keys)) {
		start, end := indexExtNormalizeWindow(window[0], window[1], len(fx.keys))
		got := overlayUnionRange(fx.chunked, fx.chunked.rangeByRanks(start, end))
		want := overlayUnionRange(fx.flat, fx.flat.rangeByRanks(start, end))
		indexExtAssertSameSlice(t, got.ToArray(), want.ToArray())
		got.Release()
		want.Release()
	}
}

func TestIndexExt_NumericBoundsMatchFlat(t *testing.T) {
	fx := indexExtNumericFixture(t)
	probes := []string{
		fx.keys[0],
		fx.keys[17],
		fx.keys[149],
		fx.keys[150],
		fx.keys[len(fx.keys)-1],
		uint64ByteStr(uint64(0x10)<<56 | uint64(999)),
		uint64ByteStr(uint64(0x20)<<56 | uint64(0x4000)),
		uint64ByteStr(uint64(0xff)<<56 | uint64(999)),
	}
	for _, probe := range probes {
		if got, want := fx.chunked.lowerBound(probe), fx.flat.lowerBound(probe); got != want {
			t.Fatalf("lowerBound(%x): got=%d want=%d", []byte(probe), got, want)
		}
		if got, want := fx.chunked.upperBound(probe), fx.flat.upperBound(probe); got != want {
			t.Fatalf("upperBound(%x): got=%d want=%d", []byte(probe), got, want)
		}
	}
	for _, key := range []string{fx.keys[0], fx.keys[27], fx.keys[191], fx.keys[len(fx.keys)-1]} {
		indexExtAssertSameSlice(
			t,
			indexExtPostingIDs(fx.chunked.lookupPostingRetained(key)),
			indexExtPostingIDs(fx.flat.lookupPostingRetained(key)),
		)
	}
}

func TestIndexExt_NumericPrefixRangeEndMatchesFlat(t *testing.T) {
	fx := indexExtNumericFixture(t)
	cases := []struct {
		prefix string
		start  int
	}{
		{prefix: string([]byte{0x10}), start: 0},
		{prefix: string([]byte{0x20}), start: fx.flat.lowerBound(string([]byte{0x20}))},
		{prefix: string([]byte{0x20}), start: fx.flat.lowerBound(fx.keys[155])},
		{prefix: string([]byte{0x20, 0x00}), start: fx.flat.lowerBound(fx.keys[150])},
		{prefix: string([]byte{0x20, 0x80}), start: fx.flat.lowerBound(uint64ByteStr(uint64(0x20)<<56 | uint64(0x8000)))},
		{prefix: string([]byte{0x20, 0x80}), start: fx.flat.lowerBound(uint64ByteStr(uint64(0xff) << 56))},
		{prefix: string([]byte{0xff}), start: fx.flat.lowerBound(uint64ByteStr(uint64(0xff) << 56))},
	}
	for _, tc := range cases {
		if got, want := fx.chunked.prefixRangeEnd(tc.prefix, tc.start), fx.flat.prefixRangeEnd(tc.prefix, tc.start); got != want {
			t.Fatalf("prefixRangeEnd(prefix=%x,start=%d): got=%d want=%d", []byte(tc.prefix), tc.start, got, want)
		}
	}
}

func TestIndexExt_NumericRangeForBoundsMatchesFlat(t *testing.T) {
	fx := indexExtNumericFixture(t)
	cases := []rangeBounds{
		indexExtBoundCase(t, struct {
			op  qx.Op
			key string
		}{op: qx.OpPREFIX, key: string([]byte{0x10})}),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpGTE, key: fx.keys[40]},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLT, key: fx.keys[155]},
		),
		indexExtBoundCase(t,
			struct {
				op  qx.Op
				key string
			}{op: qx.OpPREFIX, key: string([]byte{0x20})},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpGT, key: fx.keys[150]},
			struct {
				op  qx.Op
				key string
			}{op: qx.OpLTE, key: fx.keys[320]},
		),
		indexExtBoundCase(t, struct {
			op  qx.Op
			key string
		}{op: qx.OpEQ, key: fx.keys[len(fx.keys)-1]}),
	}

	for _, rb := range cases {
		indexExtAssertRangeEquivalent(t, fx.flat, fx.chunked, fx.flat.rangeForBounds(rb), fx.chunked.rangeForBounds(rb), false)
	}
}

func TestIndexExt_ChunkedRootPosForRankRoundTrip(t *testing.T) {
	fx := indexExtStringFixture(t)
	for rank := 0; rank <= len(fx.entries); rank++ {
		pos := fx.root.posForRank(rank)
		if rank == len(fx.entries) {
			if !fx.root.isEndPos(pos) {
				t.Fatalf("expected end pos for rank=%d, got=%+v", rank, pos)
			}
			continue
		}
		key, ok := fx.root.posKey(pos)
		if !ok {
			t.Fatalf("posKey(rank=%d) failed", rank)
		}
		if compareIndexKeys(key, fx.entries[rank].Key) != 0 {
			t.Fatalf("rank=%d key mismatch", rank)
		}
		if got := fx.root.entryPrefixForChunk(pos.chunk) + pos.entry; got != rank {
			t.Fatalf("rank=%d prefix+entry mismatch: got=%d", rank, got)
		}
	}
}

func TestIndexExt_ChunkedRootRangeRowsMatchesCursor(t *testing.T) {
	fx := indexExtStringFixture(t)
	for _, window := range indexExtRangeWindows(len(fx.keys)) {
		start, end := indexExtNormalizeWindow(window[0], window[1], len(fx.keys))
		br := fx.chunked.rangeByRanks(start, end)
		got := fx.root.rangeRows(br.startPos, br.endPos)
		_, want := overlayRangeStats(fx.flat, fx.flat.rangeByRanks(start, end))
		if got != want {
			t.Fatalf("rangeRows(%d,%d): got=%d want=%d", start, end, got, want)
		}
	}
}

func TestIndexExt_ChunkedRootTouchChunkIndexFromFindsTargetChunk(t *testing.T) {
	fx := indexExtStringFixture(t)
	for i := 0; i < len(fx.keys); i += 17 {
		expectedChunk := fx.root.posForRank(i).chunk
		for _, start := range []int{0, max(0, expectedChunk-1), expectedChunk} {
			if got := fx.root.touchChunkIndexFrom(start, fx.entries[i].Key); got != expectedChunk {
				t.Fatalf("touchChunkIndexFrom(start=%d,rank=%d): got=%d want=%d", start, i, got, expectedChunk)
			}
		}
	}
	if got := fx.root.touchChunkIndexFrom(0, indexKeyFromBytes([]byte{0xff, 0xff, 0xff})); got != fx.root.chunkCount-1 {
		t.Fatalf("touchChunkIndexFrom(after-last): got=%d want=%d", got, fx.root.chunkCount-1)
	}
}

func TestIndexExt_MultiPageStringDirectoryMatchesFlat(t *testing.T) {
	fx := indexExtMultiPageStringFixture(t)

	ranks := map[int]struct{}{
		0:                {},
		fx.root.keyCount: {},
	}
	addRank := func(rank int) {
		if rank < 0 {
			rank = 0
		}
		if rank > fx.root.keyCount {
			rank = fx.root.keyCount
		}
		ranks[rank] = struct{}{}
	}

	for _, prefix := range fx.root.prefix {
		addRank(prefix - 1)
		addRank(prefix)
	}
	for chunk := 0; chunk <= fx.root.chunkCount; chunk++ {
		rank := fx.root.entryPrefixForChunk(chunk)
		addRank(rank - 1)
		addRank(rank)
	}

	probes := make([]string, 0, len(ranks)*3+2)
	probes = append(probes, "", "mp/zzzz")
	for rank := range ranks {
		if rank >= 0 && rank < len(fx.keys) {
			key := fx.keys[rank]
			probes = append(probes, key, key+"/tail")
		}
		if rank > 0 && rank <= len(fx.keys) {
			probes = append(probes, fx.keys[rank-1]+"/tail")
		}
	}
	sort.Strings(probes)
	probes = slices.Compact(probes)

	for _, probe := range probes {
		if got, want := fx.chunked.lowerBound(probe), fx.flat.lowerBound(probe); got != want {
			t.Fatalf("lowerBound(%q): got=%d want=%d", probe, got, want)
		}
		if got, want := fx.chunked.upperBound(probe), fx.flat.upperBound(probe); got != want {
			t.Fatalf("upperBound(%q): got=%d want=%d", probe, got, want)
		}
	}

	for rank := range ranks {
		start, end := indexExtNormalizeWindow(rank-5, rank+5, len(fx.keys))
		indexExtAssertRangeEquivalent(
			t,
			fx.flat,
			fx.chunked,
			fx.flat.rangeByRanks(start, end),
			fx.chunked.rangeByRanks(start, end),
			false,
		)
		indexExtAssertRangeEquivalent(
			t,
			fx.flat,
			fx.chunked,
			fx.flat.rangeByRanks(start, end),
			fx.chunked.rangeByRanks(start, end),
			true,
		)
	}
}

func TestIndexExt_ApplyFieldPostingDiffChunkedMultiPageBoundariesMatchFlat(t *testing.T) {
	fx := indexExtMultiPageStringFixture(t)

	boundaryRanks := make([]int, 0, 16)
	addBoundary := func(rank int) {
		if rank < 0 || rank >= len(fx.keys) {
			return
		}
		boundaryRanks = append(boundaryRanks, rank)
	}
	addBoundary(0)
	addBoundary(fieldIndexChunkTargetEntries - 1)
	addBoundary(fieldIndexChunkTargetEntries)
	for _, prefix := range fx.root.prefix {
		addBoundary(prefix - 1)
		addBoundary(prefix)
	}
	addBoundary(len(fx.keys) - 1)
	slices.Sort(boundaryRanks)
	boundaryRanks = slices.Compact(boundaryRanks)

	ops := make([]indexExtDeltaOp, 0, len(boundaryRanks)+4)
	for i, rank := range boundaryRanks {
		key := fx.keys[rank]
		switch i % 3 {
		case 0:
			ops = append(ops, indexExtDeltaOp{
				key:    key,
				remove: indexExtPostingIDs(fx.entries[rank].IDs),
			})
		case 1:
			ops = append(ops, indexExtDeltaOp{
				key:    key,
				remove: indexExtPostingIDs(fx.entries[rank].IDs),
				add:    []uint64{900_000 + uint64(i), 950_000 + uint64(i)},
			})
		default:
			ops = append(ops, indexExtDeltaOp{
				key: fmt.Sprintf("%s/x", key),
				add: []uint64{990_000 + uint64(i)},
			})
		}
	}
	ops = append(ops,
		indexExtDeltaOp{key: "mp/00-before", add: []uint64{1_100_001}},
		indexExtDeltaOp{key: fx.keys[fx.root.prefix[1]-1] + "/after", add: []uint64{1_100_002}},
		indexExtDeltaOp{key: fx.keys[fx.root.prefix[1]] + "/after", add: []uint64{1_100_003}},
		indexExtDeltaOp{key: "mp/z-after", add: []uint64{1_100_004}},
	)

	wantFlat := applyFieldPostingDiffSorted(&fx.entries, indexExtDeltaKeys(ops, false))
	if wantFlat == nil {
		t.Fatalf("expected non-empty flat diff result")
	}

	got := applyFieldPostingDiffChunked(fx.root, indexExtDeltaKeys(ops, false))
	if !got.isChunked() {
		t.Fatalf("expected multi-page diff to remain chunked")
	}
	indexExtAssertStorageMatchesEntries(t, got, *wantFlat)
}

func TestIndexExt_ApplyFieldPostingDiffFlatMaybeChunkedPromotes(t *testing.T) {
	allKeys := indexExtSortedStringKeys()
	baseFix := indexExtNewOverlayFixture(t, allKeys[:260], false)
	baseEntries := append([]index(nil), baseFix.entries...)
	expected := indexExtExpectedMapFromEntries(baseFix.entries)

	var ops []indexExtDeltaOp
	for i, key := range allKeys[260:] {
		ids := indexExtPostingIDs(indexExtPosting(uint64(100_000+i*16), i%7+1))
		ops = append(ops, indexExtDeltaOp{key: key, add: ids})
	}
	indexExtApplyOpsExpected(expected, ops)

	out := applyFieldPostingDiffFlatMaybeChunked(&baseEntries, indexExtDeltaKeys(ops, false))
	if !out.isChunked() {
		t.Fatalf("expected promotion to chunked storage")
	}
	indexExtAssertStorageMatchesExpected(t, out, expected)
}

func TestIndexExt_ApplyFieldPostingDiffChunkedDemotes(t *testing.T) {
	baseFix := indexExtNewOverlayFixture(t, indexExtSortedStringKeys()[:396], false)
	expected := indexExtExpectedMapFromEntries(baseFix.entries)

	var ops []indexExtDeltaOp
	for _, rank := range []int{0, 7, 14, 21, 28, 35, 120, 121, 122, 123, 191, 192, 193, 250, 251, 252, 320, 321, 322, 395} {
		key := baseFix.keys[rank]
		ops = append(ops, indexExtDeltaOp{key: key, remove: expected[key]})
	}
	indexExtApplyOpsExpected(expected, ops)

	out := applyFieldPostingDiffChunked(baseFix.root, indexExtDeltaKeys(ops, false))
	if out.isChunked() {
		t.Fatalf("expected demotion to flat storage")
	}
	indexExtAssertStorageMatchesExpected(t, out, expected)
}

func TestIndexExt_ApplyFieldPostingDiffChunkedMixedAddRemovePreservesOrder(t *testing.T) {
	baseFix := indexExtNewOverlayFixture(t, indexExtSortedStringKeys()[:396], false)
	expected := indexExtExpectedMapFromEntries(baseFix.entries)

	var ops []indexExtDeltaOp
	replaceRanks := []int{10, 191, 192, 193, 381, 395}
	for i, rank := range replaceRanks {
		key := baseFix.keys[rank]
		ops = append(ops, indexExtDeltaOp{
			key:    key,
			remove: expected[key],
			add:    indexExtPostingIDs(indexExtPosting(uint64(200_000+i*32), 4+i%3)),
		})
	}
	for _, rank := range []int{0, 1, 2, 180, 194, 195, 196, 300, 301, 302, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359} {
		key := baseFix.keys[rank]
		ops = append(ops, indexExtDeltaOp{key: key, remove: expected[key]})
	}
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("az/new/%03d", i)
		ops = append(ops, indexExtDeltaOp{
			key: key,
			add: indexExtPostingIDs(indexExtPosting(uint64(300_000+i*16), 2+i%5)),
		})
	}
	indexExtApplyOpsExpected(expected, ops)

	out := applyFieldPostingDiffChunked(baseFix.root, indexExtDeltaKeys(ops, false))
	if !out.isChunked() {
		t.Fatalf("expected mixed diff to remain chunked")
	}
	indexExtAssertStorageMatchesExpected(t, out, expected)
}

func TestIndexExt_MergeInsertOnlyFieldStorageOwnedUnionsExistingKeysAndKeepsSorted(t *testing.T) {
	baseFix := indexExtNewOverlayFixture(t, indexExtSortedStringKeys()[:390], false)
	baseEntries := append([]index(nil), baseFix.entries...)
	baseStorage := newRegularFieldIndexStorage(&baseEntries)
	expected := indexExtExpectedMapFromEntries(baseFix.entries)

	adds := getPostingMap()
	for i, rank := range []int{1, 189, 190, 191, 389} {
		key := baseFix.keys[rank]
		addIDs := indexExtPostingIDs(indexExtPosting(uint64(400_000+i*16), 3+i))
		adds[key] = indexExtPostingFromIDs(addIDs)
		expected[key] = append(append([]uint64(nil), expected[key]...), addIDs...)
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("bb/new/%03d", i)
		addIDs := indexExtPostingIDs(indexExtPosting(uint64(410_000+i*16), 1+i%4))
		adds[key] = indexExtPostingFromIDs(addIDs)
		expected[key] = addIDs
	}
	for key := range expected {
		slices.Sort(expected[key])
	}

	out := mergeInsertOnlyFieldStorageOwned(baseStorage, adds, false, true)
	if !out.isChunked() {
		t.Fatalf("expected chunked storage after insert-only merge")
	}
	indexExtAssertStorageMatchesExpected(t, out, expected)
}

func TestIndexExt_StorageFromPostingMapOwnedDropsEmptyPostings(t *testing.T) {
	adds := getPostingMap()
	adds["drop/1"] = posting.List{}
	adds["keep/1"] = indexExtPostingFromIDs([]uint64{10})
	adds["keep/2"] = indexExtPostingFromIDs([]uint64{20, 21, 22})
	adds["drop/2"] = posting.List{}

	out := newRegularFieldIndexStorageFromPostingMapOwned(adds, false)
	expected := map[string][]uint64{
		"keep/1": {10},
		"keep/2": {20, 21, 22},
	}
	indexExtAssertStorageMatchesExpected(t, out, expected)
}

func TestIndexExt_DBFieldOverlayPromotesOnDistinctGrowth(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 300, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("prom/%03d", i), Age: i}
	})
	if db.fieldOverlay("name").chunked != nil {
		t.Fatalf("expected flat name overlay below threshold")
	}

	indexExtBatchSetGenerated(t, db, 301, 420, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("prom/%03d", i), Age: i}
	})
	if db.fieldOverlay("name").chunked == nil {
		t.Fatalf("expected chunked name overlay above threshold")
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.PREFIX("name", "prom/")).By("name", qx.ASC))
}

func TestIndexExt_DBFieldOverlayDemotesOnDistinctCollapse(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("dem/%03d", i), Age: i}
	})
	if db.fieldOverlay("name").chunked == nil {
		t.Fatalf("expected chunked name overlay before deletes")
	}

	for i := 1; i <= 50; i++ {
		if err := db.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}
	if db.fieldOverlay("name").chunked != nil {
		t.Fatalf("expected flat name overlay after deletes")
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.PREFIX("name", "dem/")).By("name", qx.ASC))
}

func TestIndexExt_DBPrefixQueryAfterSetPatchDeleteMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 450, func(i int) *Rec {
		name := fmt.Sprintf("b/%03d", i)
		switch {
		case i <= 170:
			name = fmt.Sprintf("aa/%03d", i)
		case i <= 330:
			name = fmt.Sprintf("ab/%03d", i)
		}
		return &Rec{Name: name, Age: i}
	})

	for _, id := range []uint64{171, 172, 173, 331, 332, 333} {
		if err := db.Patch(id, []Field{{Name: "name", Value: fmt.Sprintf("aa/mut/%03d", id)}}); err != nil {
			t.Fatalf("Patch(%d): %v", id, err)
		}
	}
	for _, id := range []uint64{10, 11, 12, 13, 14, 15} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}
	indexExtBatchSetGenerated(t, db, 451, 470, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("aa/new/%03d", i), Age: i}
	})

	q := qx.Query(qx.PREFIX("name", "aa/")).By("name", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBRangeQueryAfterBoundaryPatchesMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{Name: fmt.Sprintf("rng/%03d", i), Age: i}
	})

	for _, tc := range []struct {
		id  uint64
		age int
	}{
		{id: 50, age: 200},
		{id: 51, age: 199},
		{id: 52, age: 350},
		{id: 53, age: 349},
		{id: 54, age: 200},
		{id: 55, age: 349},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "age", Value: tc.age}}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 200), qx.LT("age", 350)).By("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBCountAfterBoundaryPatchesAndDeletesMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("cnt/%03d", i),
			Age:    1000 + i,
			Active: i%3 == 0,
		}
	})

	for _, id := range []uint64{110, 111, 112, 113, 114, 115} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}
	for _, tc := range []struct {
		id     uint64
		age    int
		active bool
	}{
		{id: 30, age: 1100, active: true},
		{id: 31, age: 1099, active: true},
		{id: 32, age: 1260, active: true},
		{id: 33, age: 1259, active: false},
		{id: 34, age: 1180, active: true},
	} {
		if err := db.Patch(tc.id, []Field{
			{Name: "age", Value: tc.age},
			{Name: "active", Value: tc.active},
		}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}

	q := qx.Query(qx.GTE("age", 1100), qx.LT("age", 1260), qx.EQ("active", true))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBOrderAscAfterChurnMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("orda/%03d", i),
			Age:  i % 40,
		}
	})

	for _, tc := range []struct {
		id  uint64
		age int
	}{
		{id: 7, age: 0},
		{id: 8, age: 0},
		{id: 9, age: 99},
		{id: 10, age: 99},
		{id: 191, age: 17},
		{id: 192, age: 17},
		{id: 193, age: 17},
	} {
		if err := db.Patch(tc.id, []Field{{Name: "age", Value: tc.age}}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}
	for _, id := range []uint64{44, 45, 46, 47, 48} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.PREFIX("name", "orda/")).By("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBOrderDescLimitOffsetAfterChurnMatchesExpected(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 520, func(i int) *Rec {
		return &Rec{
			Name:   fmt.Sprintf("ordd/%03d", i),
			Age:    100 + i%70,
			Active: i%2 == 0,
		}
	})

	for _, tc := range []struct {
		id     uint64
		age    int
		active bool
	}{
		{id: 5, age: 500, active: true},
		{id: 6, age: 499, active: true},
		{id: 7, age: 101, active: false},
		{id: 8, age: 500, active: true},
		{id: 250, age: 450, active: true},
		{id: 251, age: 450, active: true},
	} {
		if err := db.Patch(tc.id, []Field{
			{Name: "age", Value: tc.age},
			{Name: "active", Value: tc.active},
		}); err != nil {
			t.Fatalf("Patch(%d): %v", tc.id, err)
		}
	}
	for _, id := range []uint64{60, 61, 62, 63, 64, 65, 66} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	q := qx.Query(qx.EQ("active", true)).By("age", qx.DESC).Skip(17).Max(61)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBNilPointerTransitionsPreserveIndexes(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 500, func(i int) *Rec {
		var opt *string
		if i%7 != 0 {
			s := fmt.Sprintf("opt-%03d", i)
			opt = &s
		}
		return &Rec{
			Name: fmt.Sprintf("opt/%03d", i),
			Opt:  opt,
		}
	})

	for _, id := range []uint64{7, 14, 21, 28, 35, 42} {
		v := fmt.Sprintf("opt-hot-%03d", id)
		if err := db.Patch(id, []Field{{Name: "opt", Value: v}}); err != nil {
			t.Fatalf("Patch(%d nil->value): %v", id, err)
		}
	}
	for _, id := range []uint64{1, 2, 3, 4, 5, 6} {
		if err := db.Patch(id, []Field{{Name: "opt", Value: (*string)(nil)}}); err != nil {
			t.Fatalf("Patch(%d value->nil): %v", id, err)
		}
	}
	if db.fieldOverlay("opt").chunked == nil {
		t.Fatalf("expected opt overlay to remain chunked")
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.PREFIX("opt", "opt-hot-")).By("opt", qx.ASC))
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", nil)))
}

func TestIndexExt_DBSliceReplaceRemovesStaleTermsAndLenBuckets(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 450, func(i int) *Rec {
		return &Rec{
			Name: fmt.Sprintf("tag/%03d", i),
			Tags: []string{"shared", fmt.Sprintf("tag-%03d", i)},
		}
	})

	for i := 1; i <= 30; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{}}}); err != nil {
			t.Fatalf("Patch(empty %d): %v", i, err)
		}
	}
	for i := 31; i <= 60; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{"hot", "shared", "hot"}}}); err != nil {
			t.Fatalf("Patch(hot %d): %v", i, err)
		}
	}
	for i := 61; i <= 75; i++ {
		if err := db.Patch(uint64(i), []Field{{Name: "tags", Value: []string{"solo"}}}); err != nil {
			t.Fatalf("Patch(solo %d): %v", i, err)
		}
	}
	for _, id := range []uint64{76, 77, 78, 79, 80} {
		if err := db.Delete(id); err != nil {
			t.Fatalf("Delete(%d): %v", id, err)
		}
	}

	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"hot"})))
	indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HAS("tags", []string{"shared", "hot"})))

	var wantZero []uint64
	for i := 1; i <= 30; i++ {
		wantZero = append(wantZero, uint64(i))
	}
	indexExtAssertSameSlice(
		t,
		indexExtPostingIDs(db.lenFieldOverlay("tags").lookupPostingRetained(uint64ByteStr(0))),
		wantZero,
	)

	if db.fieldOverlay("tags").lookupPostingRetained("tag-005").Contains(5) {
		t.Fatalf("stale tag posting retained id=5 after slice replacement")
	}
}

func TestIndexExt_SnapshotOverlayStableDuringConcurrentWrites(t *testing.T) {
	db, _ := openTempDBUint64(t)

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		name := fmt.Sprintf("zz/%03d", i)
		if i <= 180 {
			name = fmt.Sprintf("aa/%03d", i)
		} else if i <= 320 {
			name = fmt.Sprintf("ab/%03d", i)
		}
		return &Rec{Name: name, Age: i}
	})

	snap := db.getSnapshot()
	oldOV := newFieldOverlayStorage(snap.index["name"])
	br := oldOV.rangeForBounds(indexExtBoundCase(t, struct {
		op  qx.Op
		key string
	}{op: qx.OpPREFIX, key: "aa/"}))
	wantKeys, wantCards := indexExtCollectCursor(oldOV, br, false)
	wantBuckets, wantRows := overlayRangeStats(oldOV, br)

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				gotKeys, gotCards := indexExtCollectCursor(oldOV, br, false)
				if !slices.Equal(gotKeys, wantKeys) || !slices.Equal(gotCards, wantCards) {
					setFailed("old snapshot cursor changed under concurrent writes")
					return
				}
				gotBuckets, gotRows := overlayRangeStats(oldOV, br)
				if gotBuckets != wantBuckets || gotRows != wantRows {
					setFailed("old snapshot stats changed under concurrent writes")
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := uint64(i%420 + 1)
			if err := db.Patch(id, []Field{{Name: "name", Value: fmt.Sprintf("mut/%03d/%03d", i, id)}}); err != nil {
				setFailed(fmt.Sprintf("Patch(%d): %v", id, err))
				return
			}
		}
	}()

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}

func TestIndexExt_DuplicateIDBatchPatchNetDiffKeepsIndexesConsistent(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	indexExtBatchSetGenerated(t, db, 1, 420, func(i int) *Rec {
		opt := fmt.Sprintf("seed-opt/%03d", i)
		return &Rec{
			Name: fmt.Sprintf("seed/%03d", i),
			Age:  i,
			Tags: []string{"shared", fmt.Sprintf("seed-tag/%03d", i)},
			Opt:  &opt,
		}
	})

	if db.fieldOverlay("name").chunked == nil || db.fieldOverlay("tags").chunked == nil {
		t.Fatalf("expected chunked overlays before duplicate-id patch")
	}

	var step atomic.Int32
	err := db.BatchPatch(
		[]uint64{77, 77, 77},
		[]Field{{Name: "age", Value: 9_001}},
		BeforeProcess(func(_ uint64, v *Rec) error {
			switch step.Add(1) {
			case 1:
				v.Name = "dup/first"
				v.Age = 9_001
				v.Tags = []string{"first", "shared", "shared"}
				v.Opt = nil
			case 2:
				live := "dup-live"
				v.Name = "dup/second"
				v.Age = 9_002
				v.Tags = []string{}
				v.Opt = &live
			case 3:
				v.Name = "dup/final"
				v.Age = 9_003
				v.Tags = []string{"final", "shared", "final"}
				v.Opt = nil
			default:
				t.Fatalf("unexpected duplicate patch step")
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch duplicate ids: %v", err)
	}

	assertState := func(stage string) {
		t.Helper()

		got, err := db.Get(77)
		if err != nil {
			t.Fatalf("%s Get(77): %v", stage, err)
		}
		if got == nil {
			t.Fatalf("%s Get(77): nil", stage)
		}
		if got.Name != "dup/final" || got.Age != 9_003 || !slices.Equal(got.Tags, []string{"final", "shared", "final"}) || got.Opt != nil {
			t.Fatalf("%s unexpected final value: %#v", stage, got)
		}

		indexExtAssertOverlayMembership(t, db.fieldOverlay("name"), "seed/077", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("name"), "dup/first", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("name"), "dup/second", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("name"), "dup/final", 77, true)

		indexExtAssertOverlayMembership(t, db.fieldOverlay("tags"), "seed-tag/077", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("tags"), "first", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("tags"), "final", 77, true)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("tags"), "shared", 77, true)

		indexExtAssertOverlayMembership(t, db.fieldOverlay("opt"), "seed-opt/077", 77, false)
		indexExtAssertOverlayMembership(t, db.fieldOverlay("opt"), "dup-live", 77, false)
		indexExtAssertOverlayMembership(t, db.nilFieldOverlay("opt"), nilIndexEntryKey, 77, true)

		indexExtAssertOverlayMembership(t, db.lenFieldOverlay("tags"), uint64ByteStr(0), 77, false)
		indexExtAssertOverlayMembership(t, db.lenFieldOverlay("tags"), uint64ByteStr(1), 77, false)
		indexExtAssertOverlayMembership(t, db.lenFieldOverlay("tags"), uint64ByteStr(2), 77, true)

		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("name", "dup/final")))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.HASANY("tags", []string{"final"})))
		indexExtAssertQueryKeysExpected(t, db, qx.Query(qx.EQ("opt", nil)))
	}

	assertState("incremental")

	if err := db.buildIndex(nil); err != nil {
		t.Fatalf("buildIndex: %v", err)
	}
	assertState("rebuilt")
}

func TestIndexExt_DBFloatSignedZeroBetweenBoundsMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(
		qx.GTE("score", 0.0),
		qx.LTE("score", 0.0),
	).By("score", qx.ASC)

	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroOrderAscMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(
		qx.GTE("score", -1.0),
	).By("score", qx.ASC).Max(8)

	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroLookupMatchesNumericEquality(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	indexExtAssertSameSlice(
		t,
		indexExtPostingIDs(db.fieldOverlay("score").lookupPostingRetained(float64ByteStr(0.0))),
		[]uint64{1, 2},
	)
}

func TestIndexExt_DBFloatSignedZeroEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(qx.EQ("score", 0.0))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroINMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(qx.IN("score", []float64{0.0}))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNegativeZeroEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(qx.EQ("score", math.Copysign(0, -1)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatSignedZeroNotEqualCountMatchesExpected(t *testing.T) {
	db := indexExtFloatSignedZeroChunkedDB(t)

	q := qx.Query(qx.NOT(qx.EQ("score", 0.0)))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNEqualityMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.EQ("score", math.NaN()))
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNCountMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.EQ("score", math.NaN()))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNLessEqualMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.LTE("score", math.NaN())).By("score", qx.ASC).Max(16)
	indexExtAssertQueryKeysExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNGreaterEqualMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.GTE("score", math.NaN())).By("score", qx.ASC).Max(16)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNNotEqualCountMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.NOT(qx.EQ("score", math.NaN())))
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatNaNINMatchesExpected(t *testing.T) {
	db := indexExtFloatNaNChunkedDB(t)

	q := qx.Query(qx.IN("score", []float64{math.NaN(), 1.0}))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldFloatEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.EQ("age", 42.0))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldFloatRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.GTE("age", 200.0), qx.LT("age", 205.0)).By("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldIntEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.EQ("score", 42))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldIntRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.GTE("score", 200), qx.LT("score", 205)).By("score", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldUintEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.EQ("age", uint64(42)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldUintRangeMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.GTE("age", uint64(200)), qx.LT("age", uint64(205))).By("age", qx.ASC)
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBIntFieldBinaryStringEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.EQ("age", int64ByteStr(42)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

func TestIndexExt_DBFloatFieldBinaryStringEqualityMatchesExpected(t *testing.T) {
	db := indexExtNumericCoercionChunkedDB(t)

	q := qx.Query(qx.EQ("score", float64ByteStr(42.0)))
	indexExtAssertQueryKeysExpected(t, db, q)
	indexExtAssertCountExpected(t, db, q)
}

/**/

func postingConsumerExpected(ids posting.List) []uint64 {
	return ids.ToArray()
}

func TestFieldIndexChunkPostingAtBorrowedDetachUnderConcurrency(t *testing.T) {
	base := snapshotExtPosting()
	for i := uint64(1); i <= 48; i++ {
		base.Add(i * 3)
	}

	chunk := &fieldIndexChunk{posts: []posting.List{base}}
	defer posting.ReleaseSliceOwned(chunk.posts)
	want := postingConsumerExpected(chunk.posts[0])

	remove := snapshotExtPosting(3, 6, 9, 12)
	add := snapshotExtPosting(1<<32|7, 1<<32|9, 2<<32|11)
	defer remove.Release()
	defer add.Release()

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				ids := chunk.postingAt(0)
				if ids.Cardinality() != uint64(len(want)) {
					setFailed(fmt.Sprintf("cardinality mismatch: got=%d want=%d", ids.Cardinality(), len(want)))
					return
				}
				if !slices.Equal(ids.ToArray(), want) {
					setFailed(fmt.Sprintf("reader view mismatch: got=%v want=%v", ids.ToArray(), want))
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 300; i++ {
			ids := chunk.postingAt(0)
			ids.AndNotInPlace(remove)
			ids.OrInPlace(add)
			ids.Add(5<<32 | uint64(i))
			ids.Optimize()
			if chunk.posts[0].Contains(5<<32 | uint64(i)) {
				setFailed("mutated borrowed chunk view changed source posting")
				return
			}
			ids.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	assertPostingConsumerSet(t, chunk.posts[0], want)
}
