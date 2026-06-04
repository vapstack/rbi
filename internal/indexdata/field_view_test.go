package indexdata

import (
	"fmt"
	"testing"

	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/posting"
)

func TestFieldIndexViewFlatCursorReturnsBorrowedPostings(t *testing.T) {
	entries := []Entry{
		{
			Key: keycodec.FromString("a"),
			IDs: fieldStorageOwnedTestPosting(10),
		},
		{
			Key: keycodec.FromString("b"),
			IDs: fieldStorageOwnedTestPosting(20),
		},
	}
	defer entries[0].IDs.Release()
	defer entries[1].IDs.Release()

	ov := NewFieldIndexView(&entries)
	br := ov.RangeByRanks(0, 2)

	cur := ov.NewCursor(br, false)
	_, ids, ok := cur.Next()
	if !ok {
		t.Fatalf("ascending cursor returned no posting")
	}
	if !ids.IsBorrowed() {
		t.Fatalf("ascending cursor posting must be borrowed")
	}
	changed := ids.BuildAdded(999)
	changed.Release()
	if entries[0].IDs.Contains(999) {
		t.Fatalf("ascending cursor posting mutation changed source storage")
	}

	cur = ov.NewCursor(br, true)
	_, ids, ok = cur.Next()
	if !ok {
		t.Fatalf("descending cursor returned no posting")
	}
	if !ids.IsBorrowed() {
		t.Fatalf("descending cursor posting must be borrowed")
	}
	changed = ids.BuildAdded(999)
	changed.Release()
	if entries[1].IDs.Contains(999) {
		t.Fatalf("descending cursor posting mutation changed source storage")
	}
}

func TestFieldIndexViewCursorRangeOrderFlatAndChunked(t *testing.T) {
	tests := []struct {
		name      string
		rows      int
		start     int
		end       int
		wantChunk bool
	}{
		{name: "Flat", rows: 64, start: 7, end: 25},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 17, start: fieldIndexChunkTargetEntries - 3, end: fieldIndexChunkTargetEntries + 5, wantChunk: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, false)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			if storage.IsChunked() != tc.wantChunk {
				t.Fatalf("storage chunked: got %v want %v", storage.IsChunked(), tc.wantChunk)
			}
			ov := NewFieldIndexViewFromStorage(storage)
			br := ov.RangeByRanks(tc.start, tc.end)

			cur := ov.NewCursor(br, false)
			for rank := tc.start; rank < tc.end; rank++ {
				key, ids, ok := cur.Next()
				if !ok {
					t.Fatalf("ascending cursor ended at rank %d", rank)
				}
				if keycodec.Compare(key, entries[rank].Key) != 0 {
					t.Fatalf("ascending key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
				}
				if ids.Cardinality() != 1 || !ids.Contains(uint64(rank+1)) {
					t.Fatalf("ascending posting[%d]: %v", rank, ids)
				}
			}
			if _, _, ok := cur.Next(); ok {
				t.Fatalf("ascending cursor returned extra row")
			}

			cur = ov.NewCursor(br, true)
			for rank := tc.end - 1; rank >= tc.start; rank-- {
				key, ids, ok := cur.Next()
				if !ok {
					t.Fatalf("descending cursor ended at rank %d", rank)
				}
				if keycodec.Compare(key, entries[rank].Key) != 0 {
					t.Fatalf("descending key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
				}
				if ids.Cardinality() != 1 || !ids.Contains(uint64(rank+1)) {
					t.Fatalf("descending posting[%d]: %v", rank, ids)
				}
			}
			if _, _, ok := cur.Next(); ok {
				t.Fatalf("descending cursor returned extra row")
			}
		})
	}
}

func TestFieldIndexViewCursorNextPostingOrSingle(t *testing.T) {
	entries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+17, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	br := ov.RangeByRanks(fieldIndexChunkTargetEntries-3, fieldIndexChunkTargetEntries+5)

	cur := ov.NewCursor(br, false)
	for rank := br.BaseStart; rank < br.BaseEnd; rank++ {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			t.Fatalf("ascending cursor ended at rank %d", rank)
		}
		if !single || !ids.IsEmpty() || idx != uint64(rank+1) {
			t.Fatalf("ascending item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, ok := cur.NextPostingOrSingle(); ok {
		t.Fatalf("ascending cursor returned extra row")
	}

	cur = ov.NewCursor(br, true)
	for rank := br.BaseEnd - 1; rank >= br.BaseStart; rank-- {
		ids, idx, single, ok := cur.NextPostingOrSingle()
		if !ok {
			t.Fatalf("descending cursor ended at rank %d", rank)
		}
		if !single || !ids.IsEmpty() || idx != uint64(rank+1) {
			t.Fatalf("descending item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, ok := cur.NextPostingOrSingle(); ok {
		t.Fatalf("descending cursor returned extra row")
	}

	multi := []Entry{
		{Key: keycodec.FromString("a"), IDs: fieldStorageOwnedTestPosting(10)},
		{Key: keycodec.FromString("b"), IDs: fieldStorageOwnedTestPosting(20)},
	}
	defer multi[0].IDs.Release()
	defer multi[1].IDs.Release()

	ov = NewFieldIndexView(&multi)
	cur = ov.NewCursor(ov.RangeByRanks(0, 2), false)
	ids, _, single, ok := cur.NextPostingOrSingle()
	if !ok || single || !ids.IsBorrowed() || ids.Cardinality() != 2 || !ids.Contains(10) {
		t.Fatalf("flat multi posting mismatch: ok=%v single=%v ids=%v", ok, single, ids)
	}
}

func TestFieldIndexViewCursorNextKeyPostingOrSingle(t *testing.T) {
	entries := fieldStorageEntriesForTest(fieldIndexChunkThreshold+17, true)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()

	ov := NewFieldIndexViewFromStorage(storage)
	br := ov.RangeByRanks(fieldIndexChunkTargetEntries-3, fieldIndexChunkTargetEntries+5)

	cur := ov.NewCursor(br, false)
	for rank := br.BaseStart; rank < br.BaseEnd; rank++ {
		key, ids, idx, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			t.Fatalf("ascending cursor ended at rank %d", rank)
		}
		if keycodec.Compare(key, entries[rank].Key) != 0 {
			t.Fatalf("ascending key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
		}
		if !single || !ids.IsEmpty() || idx != uint64(rank+1) {
			t.Fatalf("ascending item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, _, ok := cur.NextKeyPostingOrSingle(); ok {
		t.Fatalf("ascending cursor returned extra row")
	}

	cur = ov.NewCursor(br, true)
	for rank := br.BaseEnd - 1; rank >= br.BaseStart; rank-- {
		key, ids, idx, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			t.Fatalf("descending cursor ended at rank %d", rank)
		}
		if keycodec.Compare(key, entries[rank].Key) != 0 {
			t.Fatalf("descending key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
		}
		if !single || !ids.IsEmpty() || idx != uint64(rank+1) {
			t.Fatalf("descending item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, _, ok := cur.NextKeyPostingOrSingle(); ok {
		t.Fatalf("descending cursor returned extra row")
	}

	multi := []Entry{
		{Key: keycodec.FromString("a"), IDs: fieldStorageOwnedTestPosting(10)},
		{Key: keycodec.FromString("b"), IDs: fieldStorageOwnedTestPosting(20)},
	}
	defer multi[0].IDs.Release()
	defer multi[1].IDs.Release()

	ov = NewFieldIndexView(&multi)
	cur = ov.NewCursor(ov.RangeByRanks(0, 2), false)
	key, ids, _, single, ok := cur.NextKeyPostingOrSingle()
	if !ok || single || !ids.IsBorrowed() || ids.Cardinality() != 2 || !ids.Contains(10) || keycodec.Compare(key, multi[0].Key) != 0 {
		t.Fatalf("flat multi posting mismatch: ok=%v single=%v key=%q ids=%v", ok, single, key.UnsafeString(), ids)
	}

	entries = make([]Entry, fieldIndexChunkThreshold+17)
	for i := range entries {
		entries[i] = Entry{
			Key: keycodec.FromU64(uint64(i * 2)),
			IDs: fieldStorageOwnedTestPosting(uint64(i + 1)),
		}
	}
	storage = newRegularFieldStorage(entries)
	defer storage.Release()

	ov = NewFieldIndexViewFromStorage(storage)
	br = ov.RangeByRanks(fieldIndexChunkTargetEntries-2, fieldIndexChunkTargetEntries+3)
	cur = ov.NewCursor(br, false)
	for rank := br.BaseStart; rank < br.BaseEnd; rank++ {
		key, ids, idx, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			t.Fatalf("ascending multi cursor ended at rank %d", rank)
		}
		if keycodec.Compare(key, entries[rank].Key) != 0 {
			t.Fatalf("ascending multi key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
		}
		want := uint64(rank + 1)
		if single || idx != 0 || !ids.IsBorrowed() || ids.Cardinality() != 2 || !ids.Contains(want) || !ids.Contains(want+1000000) {
			t.Fatalf("ascending multi item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, _, ok := cur.NextKeyPostingOrSingle(); ok {
		t.Fatalf("ascending multi cursor returned extra row")
	}

	cur = ov.NewCursor(br, true)
	for rank := br.BaseEnd - 1; rank >= br.BaseStart; rank-- {
		key, ids, idx, single, ok := cur.NextKeyPostingOrSingle()
		if !ok {
			t.Fatalf("descending multi cursor ended at rank %d", rank)
		}
		if keycodec.Compare(key, entries[rank].Key) != 0 {
			t.Fatalf("descending multi key[%d]: got %q want %q", rank, key.UnsafeString(), entries[rank].Key.UnsafeString())
		}
		want := uint64(rank + 1)
		if single || idx != 0 || !ids.IsBorrowed() || ids.Cardinality() != 2 || !ids.Contains(want) || !ids.Contains(want+1000000) {
			t.Fatalf("descending multi item[%d]: ids=%v idx=%d single=%v", rank, ids, idx, single)
		}
	}
	if _, _, _, _, ok := cur.NextKeyPostingOrSingle(); ok {
		t.Fatalf("descending multi cursor returned extra row")
	}
}

func TestFieldIndexViewRangeByRanksClampsFlatAndChunked(t *testing.T) {
	tests := []struct {
		name      string
		rows      int
		wantChunk bool
	}{
		{name: "Flat", rows: 64},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 17, wantChunk: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, false)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			if storage.IsChunked() != tc.wantChunk {
				t.Fatalf("storage chunked: got %v want %v", storage.IsChunked(), tc.wantChunk)
			}
			ov := NewFieldIndexViewFromStorage(storage)

			br := ov.RangeByRanks(-7, 5)
			if br.BaseStart != 0 || br.BaseEnd != 5 || br.Len() != 5 {
				t.Fatalf("low-clamped range: got [%d,%d) len=%d", br.BaseStart, br.BaseEnd, br.Len())
			}
			br = ov.RangeByRanks(tc.rows-5, tc.rows+99)
			if br.BaseStart != tc.rows-5 || br.BaseEnd != tc.rows || br.Len() != 5 {
				t.Fatalf("high-clamped range: got [%d,%d) len=%d", br.BaseStart, br.BaseEnd, br.Len())
			}
			br = ov.RangeByRanks(tc.rows+10, tc.rows+20)
			if br.BaseStart != tc.rows || br.BaseEnd != tc.rows || !br.Empty() {
				t.Fatalf("empty high-clamped range: got [%d,%d)", br.BaseStart, br.BaseEnd)
			}
		})
	}
}

func TestIndexViewRangeStats_ChunkedMatchesPostingCardinality(t *testing.T) {
	entries := make([]Entry, 0, fieldIndexChunkThreshold+37)
	var expected uint64
	start := 17
	end := fieldIndexChunkThreshold + 29
	for i := 0; i < fieldIndexChunkThreshold+37; i++ {
		card := uint64(i%5 + 1)
		var ids posting.List
		for j := uint64(0); j < card; j++ {
			ids = ids.BuildAdded(uint64(i*10) + j + 1)
		}
		if i >= start && i < end {
			expected += ids.Cardinality()
		}
		entries = append(entries, Entry{
			Key: keycodec.FromString(fmt.Sprintf("k%04d", i)),
			IDs: ids,
		})
	}

	root := buildChunkedFieldIndexRoot(entries)
	if root == nil {
		t.Fatalf("expected chunked root")
	}
	ov := FieldIndexView{chunked: root}
	br := ov.RangeByRanks(start, end)
	buckets, rows := ov.RangeStats(br)
	if buckets != end-start {
		t.Fatalf("unexpected bucket count: got %d want %d", buckets, end-start)
	}
	if rows != expected {
		t.Fatalf("unexpected row estimate: got %d want %d", rows, expected)
	}
}

func TestBoundsApplyKeepsTightestEdges(t *testing.T) {
	var lo Bounds
	lo.ApplyLo("m", true)
	lo.ApplyLo("m", false)
	lo.ApplyLo("a", true)
	if !lo.HasLo || lo.LoKey != "m" || lo.LoInc {
		t.Fatalf("string lower bound mismatch: %+v", lo)
	}
	lo.ApplyLo("z", true)
	if lo.LoKey != "z" || !lo.LoInc {
		t.Fatalf("string lower bound did not move to higher key: %+v", lo)
	}

	var loReverse Bounds
	loReverse.ApplyLo("m", false)
	loReverse.ApplyLo("m", true)
	if !loReverse.HasLo || loReverse.LoKey != "m" || loReverse.LoInc {
		t.Fatalf("string lower bound loosened on equal inclusive update: %+v", loReverse)
	}

	var hi Bounds
	hi.ApplyHi("m", true)
	hi.ApplyHi("m", false)
	hi.ApplyHi("z", true)
	if !hi.HasHi || hi.HiKey != "m" || hi.HiInc {
		t.Fatalf("string upper bound mismatch: %+v", hi)
	}
	hi.ApplyHi("a", true)
	if hi.HiKey != "a" || !hi.HiInc {
		t.Fatalf("string upper bound did not move to lower key: %+v", hi)
	}

	var numLo Bounds
	numLo.ApplyLoIndex(keycodec.FromU64(10), true)
	numLo.ApplyLoIndex(keycodec.FromU64(10), false)
	numLo.ApplyLoIndex(keycodec.FromU64(8), true)
	if !numLo.HasLo || !numLo.LoNumeric || numLo.LoIndex.U64() != 10 || numLo.LoInc {
		t.Fatalf("numeric lower bound mismatch: %+v", numLo)
	}
	numLo.ApplyLoIndex(keycodec.FromU64(12), true)
	if numLo.LoIndex.U64() != 12 || !numLo.LoInc {
		t.Fatalf("numeric lower bound did not move to higher key: %+v", numLo)
	}

	var numHi Bounds
	numHi.ApplyHiIndex(keycodec.FromU64(10), true)
	numHi.ApplyHiIndex(keycodec.FromU64(10), false)
	numHi.ApplyHiIndex(keycodec.FromU64(12), true)
	if !numHi.HasHi || !numHi.HiNumeric || numHi.HiIndex.U64() != 10 || numHi.HiInc {
		t.Fatalf("numeric upper bound mismatch: %+v", numHi)
	}
	numHi.ApplyHiIndex(keycodec.FromU64(8), true)
	if numHi.HiIndex.U64() != 8 || !numHi.HiInc {
		t.Fatalf("numeric upper bound did not move to lower key: %+v", numHi)
	}
}

func TestBoundsApplyPrefixIntersectionAndContradiction(t *testing.T) {
	var narrowed Bounds
	narrowed.ApplyPrefix("user")
	narrowed.ApplyPrefix("user1")
	if narrowed.Empty || !narrowed.HasPrefix || narrowed.Prefix != "user1" {
		t.Fatalf("prefix did not narrow: %+v", narrowed)
	}

	var disjoint Bounds
	disjoint.ApplyPrefix("user1")
	disjoint.ApplyPrefix("user2")
	if !disjoint.Empty {
		t.Fatalf("disjoint prefixes must empty bounds: %+v", disjoint)
	}

	var abovePrefix Bounds
	abovePrefix.ApplyPrefix("a")
	abovePrefix.ApplyLo("b", true)
	if !abovePrefix.Empty {
		t.Fatalf("lower bound above prefix range must empty bounds: %+v", abovePrefix)
	}

	var belowPrefix Bounds
	belowPrefix.ApplyPrefix("b")
	belowPrefix.ApplyHi("aa", true)
	if !belowPrefix.Empty {
		t.Fatalf("upper bound below prefix range must empty bounds: %+v", belowPrefix)
	}
}

func TestFieldIndexViewRangeForBounds_PrefixIntersectsRange(t *testing.T) {
	entries := []Entry{
		{Key: keycodec.FromString("aa")},
		{Key: keycodec.FromString("ab")},
		{Key: keycodec.FromString("ac")},
		{Key: keycodec.FromString("ad")},
		{Key: keycodec.FromString("b0")},
	}
	ov := NewFieldIndexView(&entries)

	bounds := Bounds{Has: true}
	bounds.ApplyPrefix("a")
	bounds.ApplyLo("ab", true)
	bounds.ApplyHi("ad", false)
	br := ov.RangeForBounds(bounds)
	if br.BaseStart != 1 || br.BaseEnd != 3 {
		t.Fatalf("unexpected prefix/range intersection: got [%d,%d) want [1,3)", br.BaseStart, br.BaseEnd)
	}

	empty := Bounds{Has: true}
	empty.ApplyPrefix("b")
	empty.ApplyHi("aa", true)
	br = ov.RangeForBounds(empty)
	if !br.Empty() {
		t.Fatalf("expected empty contradicted prefix range, got [%d,%d)", br.BaseStart, br.BaseEnd)
	}
}

func TestFieldIndexViewRangeForBounds_ExclusiveLowerExactAndMissing(t *testing.T) {
	tests := []struct {
		name    string
		numeric bool
		rows    int
	}{
		{name: "FlatString", rows: 64},
		{name: "ChunkedString", rows: fieldIndexChunkThreshold + 17},
		{name: "FlatNumeric", numeric: true, rows: 64},
		{name: "ChunkedNumeric", numeric: true, rows: fieldIndexChunkThreshold + 17},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, tc.numeric)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)
			rank := tc.rows / 2

			var exact Bounds
			var missing Bounds
			var wantKey keycodec.IndexKey
			if tc.numeric {
				exact = Bounds{
					HasLo:     true,
					LoIndex:   keycodec.FromU64(uint64(rank * 2)),
					LoNumeric: true,
				}
				missing = Bounds{
					HasLo:     true,
					LoIndex:   keycodec.FromU64(uint64(rank*2 + 1)),
					LoNumeric: true,
				}
				wantKey = keycodec.FromU64(uint64((rank + 1) * 2))
			} else {
				key := fmt.Sprintf("k/%06d", rank)
				exact = Bounds{HasLo: true, LoKey: key}
				missing = Bounds{HasLo: true, LoKey: key + "~"}
				wantKey = keycodec.FromString(fmt.Sprintf("k/%06d", rank+1))
			}

			br := ov.RangeForBounds(exact)
			if br.BaseStart != rank+1 {
				t.Fatalf("exact exclusive lower start: got %d want %d", br.BaseStart, rank+1)
			}
			cur := ov.NewCursor(br, false)
			gotKey, ids, ok := cur.Next()
			if !ok {
				t.Fatalf("expected first row after exact lower bound")
			}
			if keycodec.Compare(gotKey, wantKey) != 0 {
				t.Fatalf("first key after exact lower: got %q want %q", gotKey.UnsafeString(), wantKey.UnsafeString())
			}
			if ids.Cardinality() != 1 || !ids.Contains(uint64(rank+2)) {
				t.Fatalf("first posting after exact lower: %v", ids)
			}

			br = ov.RangeForBounds(missing)
			if br.BaseStart != rank+1 {
				t.Fatalf("missing exclusive lower start: got %d want %d", br.BaseStart, rank+1)
			}
		})
	}
}

func TestFieldIndexViewRangeForBounds_HighPrefixAndDescendingCursor(t *testing.T) {
	tests := []struct {
		name string
		rows int
	}{
		{name: "Flat", rows: 64},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 17},
	}

	for _, tc := range tests {
		t.Run(tc.name+"String", func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, false)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)

			exclusive := ov.RangeForBounds(Bounds{HasHi: true, HiKey: "k/000020"})
			if exclusive.BaseEnd != 20 {
				t.Fatalf("exclusive high end: got %d want 20", exclusive.BaseEnd)
			}
			inclusive := ov.RangeForBounds(Bounds{HasHi: true, HiKey: "k/000020", HiInc: true})
			if inclusive.BaseEnd != 21 {
				t.Fatalf("inclusive high end: got %d want 21", inclusive.BaseEnd)
			}

			prefixed := ov.RangeForBounds(Bounds{HasPrefix: true, Prefix: "k/00001"})
			if prefixed.BaseStart != 10 || prefixed.BaseEnd != 20 {
				t.Fatalf("prefix range: got [%d,%d) want [10,20)", prefixed.BaseStart, prefixed.BaseEnd)
			}
			cur := ov.NewCursor(prefixed, true)
			key, ids, ok := cur.Next()
			if !ok {
				t.Fatalf("expected descending prefix cursor row")
			}
			if !keycodec.EqualsString(key, "k/000019") {
				t.Fatalf("descending prefix first key: got %q want k/000019", key.UnsafeString())
			}
			if ids.Cardinality() != 1 || !ids.Contains(20) {
				t.Fatalf("descending prefix first posting: %v", ids)
			}
		})

		t.Run(tc.name+"Numeric", func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, true)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			ov := NewFieldIndexViewFromStorage(storage)

			exclusive := ov.RangeForBounds(Bounds{
				HasHi:     true,
				HiIndex:   keycodec.FromU64(40),
				HiNumeric: true,
			})
			if exclusive.BaseEnd != 20 {
				t.Fatalf("numeric exclusive high end: got %d want 20", exclusive.BaseEnd)
			}
			inclusive := ov.RangeForBounds(Bounds{
				HasHi:     true,
				HiIndex:   keycodec.FromU64(40),
				HiNumeric: true,
				HiInc:     true,
			})
			if inclusive.BaseEnd != 21 {
				t.Fatalf("numeric inclusive high end: got %d want 21", inclusive.BaseEnd)
			}
		})
	}
}

func TestBoundsNormalizeComparesEndpoints(t *testing.T) {
	stringBounds := Bounds{
		HasLo: true,
		LoKey: "a",
		LoInc: true,
		HasHi: true,
		HiKey: "b",
		HiInc: true,
	}
	stringBounds.Normalize()
	if stringBounds.Empty {
		t.Fatalf("string bounds marked empty")
	}

	pointBounds := Bounds{
		HasLo: true,
		LoKey: "a",
		LoInc: true,
		HasHi: true,
		HiKey: "a",
		HiInc: true,
	}
	if !pointBounds.IsPointRange() {
		t.Fatalf("equal inclusive bounds not recognized as point range")
	}

	numericBounds := Bounds{
		HasLo:     true,
		LoIndex:   keycodec.FromU64(7),
		LoNumeric: true,
		LoInc:     true,
		HasHi:     true,
		HiIndex:   keycodec.FromU64(3),
		HiNumeric: true,
		HiInc:     true,
	}
	numericBounds.Normalize()
	if !numericBounds.Empty {
		t.Fatalf("numeric bounds not marked empty")
	}

	defer func() {
		if recover() == nil {
			t.Fatalf("expected mixed bound representation panic")
		}
	}()
	mixedBounds := Bounds{
		HasLo:     true,
		LoKey:     "a",
		LoInc:     true,
		HasHi:     true,
		HiIndex:   keycodec.FromU64(1),
		HiNumeric: true,
		HiInc:     true,
	}
	mixedBounds.Normalize()
}

func TestFieldIndexViewRangeStats_ChunkedMatchesCursorScanForBounds(t *testing.T) {
	const rows = fieldIndexChunkThreshold + 113
	tests := []struct {
		name    string
		numeric bool
		bounds  []Bounds
	}{
		{
			name: "String",
			bounds: []Bounds{
				{Has: true, HasLo: true, LoKey: "k/000081", LoInc: false},
				{Has: true, HasLo: true, LoKey: "k/000004", LoInc: false},
				{Has: true, HasLo: true, LoKey: "k/000040", LoInc: true},
				{Has: true, HasHi: true, HiKey: "k/000050", HiInc: false},
				{Has: true, HasLo: true, LoKey: "k/000010", LoInc: true, HasHi: true, HiKey: "k/000020", HiInc: true},
			},
		},
		{
			name:    "Numeric",
			numeric: true,
			bounds: []Bounds{
				{Has: true, HasLo: true, LoIndex: keycodec.FromU64(162), LoNumeric: true, LoInc: false},
				{Has: true, HasLo: true, LoIndex: keycodec.FromU64(8), LoNumeric: true, LoInc: false},
				{Has: true, HasLo: true, LoIndex: keycodec.FromU64(80), LoNumeric: true, LoInc: true},
				{Has: true, HasHi: true, HiIndex: keycodec.FromU64(100), HiNumeric: true, HiInc: false},
				{Has: true, HasLo: true, LoIndex: keycodec.FromU64(20), LoNumeric: true, LoInc: true, HasHi: true, HiIndex: keycodec.FromU64(40), HiNumeric: true, HiInc: true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := make([]Entry, 0, rows)
			for i := 0; i < rows; i++ {
				key := keycodec.FromString(fmt.Sprintf("k/%06d", i))
				if tc.numeric {
					key = keycodec.FromU64(uint64(i * 2))
				}
				var ids posting.List
				for j := 0; j <= i%5; j++ {
					ids = ids.BuildAdded(uint64(i*10 + j + 1))
				}
				entries = append(entries, Entry{Key: key, IDs: ids})
			}
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			if !storage.IsChunked() {
				t.Fatalf("expected chunked storage")
			}
			ov := NewFieldIndexViewFromStorage(storage)
			for i, bounds := range tc.bounds {
				br := ov.RangeForBounds(bounds)
				gotBuckets, gotRows := ov.RangeStats(br)
				wantBuckets, wantRows := fieldIndexViewRangeStats(ov, br)
				if gotBuckets != wantBuckets || gotRows != wantRows {
					t.Fatalf("case %d range stats mismatch: got buckets=%d rows=%d want buckets=%d rows=%d", i, gotBuckets, gotRows, wantBuckets, wantRows)
				}
			}
		})
	}
}

func TestFieldIndexViewRangeStats_ChunkedRankOnlyRangeUsesRankBounds(t *testing.T) {
	entries := make([]Entry, 0, fieldIndexChunkThreshold+17)
	for i := 0; i < fieldIndexChunkThreshold+17; i++ {
		entries = append(entries, Entry{
			Key: keycodec.FromString(fmt.Sprintf("k/%04d", i)),
			IDs: posting.BuildFromSorted([]uint64{uint64(i), uint64(i + 100_000)}),
		})
	}
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	if !storage.IsChunked() {
		t.Fatalf("expected chunked storage")
	}
	ov := NewFieldIndexViewFromStorage(storage)
	br := FieldIndexRange{
		BaseStart: 13,
		BaseEnd:   fieldIndexChunkThreshold + 7,
	}
	gotBuckets, gotRows := ov.RangeStats(br)
	wantBuckets, wantRows := fieldIndexViewRangeStats(ov, br)
	if gotBuckets != wantBuckets || gotRows != wantRows {
		t.Fatalf("rank-only range stats mismatch: got buckets=%d rows=%d want buckets=%d rows=%d", gotBuckets, gotRows, wantBuckets, wantRows)
	}
}

func TestFieldIndexViewLookupPostingsSkipsMissingAndReturnsEstimate(t *testing.T) {
	entries := fieldStorageEntriesForTest(8, false)
	storage := newRegularFieldStorage(entries)
	defer storage.Release()
	ov := NewFieldIndexViewFromStorage(storage)

	posts, est := ov.LookupPostings([]string{"k/000001", "missing", "k/000004"})
	defer posting.ReleaseSlice(posts)
	if len(posts) != 2 {
		t.Fatalf("lookup postings len: got %d want 2", len(posts))
	}
	if est != 2 {
		t.Fatalf("lookup postings estimate: got %d want 2", est)
	}
	if !posts[0].Contains(2) || !posts[1].Contains(5) {
		t.Fatalf("lookup postings returned wrong postings")
	}
}

func TestFieldIndexViewAccessorsFlatAndChunked(t *testing.T) {
	tests := []struct {
		name      string
		rows      int
		wantChunk bool
	}{
		{name: "Flat", rows: 64},
		{name: "Chunked", rows: fieldIndexChunkThreshold + 17, wantChunk: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := fieldStorageEntriesForTest(tc.rows, true)
			storage := newRegularFieldStorage(entries)
			defer storage.Release()
			if storage.KeyCount() != tc.rows {
				t.Fatalf("storage key count: got %d want %d", storage.KeyCount(), tc.rows)
			}
			ov := NewFieldIndexViewFromStorage(storage)
			if !ov.HasData() {
				t.Fatalf("overlay must report data")
			}
			if ov.IsChunked() != tc.wantChunk {
				t.Fatalf("overlay chunked: got %v want %v", ov.IsChunked(), tc.wantChunk)
			}
			if ov.Rows() != uint64(tc.rows) {
				t.Fatalf("overlay rows: got %d want %d", ov.Rows(), tc.rows)
			}
			if ov.RangeRows(3, 17) != 14 {
				t.Fatalf("overlay range rows: got %d want 14", ov.RangeRows(3, 17))
			}
			br := ov.RangeByRanks(3, 17)
			buckets, rows := ov.RangeStats(br)
			if buckets != 14 || rows != 14 {
				t.Fatalf("overlay range stats: buckets=%d rows=%d want 14/14", buckets, rows)
			}
			if key := ov.KeyAt(9); key.U64() != 18 {
				t.Fatalf("overlay key at rank: got %d want 18", key.U64())
			}
			if ids := ov.PostingAt(9); ids.Cardinality() != 1 || !ids.Contains(10) {
				t.Fatalf("overlay posting at rank: %v", ids)
			}
			if got := ov.LookupCardinalityKey(keycodec.FromU64(18)); got != 1 {
				t.Fatalf("overlay lookup cardinality: got %d want 1", got)
			}
		})
	}
}

func fieldIndexViewRangeStats(ov FieldIndexView, br FieldIndexRange) (int, uint64) {
	if br.BaseStart >= br.BaseEnd {
		return 0, 0
	}
	cur := ov.NewCursor(br, false)
	n := 0
	rows := uint64(0)
	for {
		_, ids, ok := cur.Next()
		if !ok {
			break
		}
		card := ids.Cardinality()
		if card == 0 {
			continue
		}
		n++
		rows += card
	}
	return n, rows
}
