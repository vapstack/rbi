package posting

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

var forbiddenListMethodNames = map[string]struct{}{
	"Add":           {},
	"CheckedAdd":    {},
	"AddMany":       {},
	"Remove":        {},
	"OrInPlace":     {},
	"AndInPlace":    {},
	"AndNotInPlace": {},
	"Optimize":      {},
	"Clear":         {},
	"MergeOwned":    {},
	"ReadFrom":      {},
	"OrInto":        {},
	"AndNotFrom":    {},
}

func TestListRemainsSmallValueHandle(t *testing.T) {
	if got := unsafe.Sizeof(List{}); got != 16 {
		t.Fatalf("posting.List size changed: got=%d want=16", got)
	}
}

func TestListForbiddenMethodsGuard(t *testing.T) {
	dir := testPackageDir(t)
	fset := token.NewFileSet()
	var failures []string

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			if !isListReceiver(fn.Recv.List[0].Type) {
				continue
			}
			if _, forbidden := forbiddenListMethodNames[fn.Name.Name]; forbidden {
				failures = append(failures, fset.Position(fn.Pos()).String()+": forbidden production method on posting.List: "+fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk posting package: %v", err)
	}
	if len(failures) > 0 {
		t.Fatalf("posting.List architecture guard failed:\n%s", strings.Join(failures, "\n"))
	}
}

func testPackageDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func isListReceiver(expr ast.Expr) bool {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name == "List"
	case *ast.StarExpr:
		id, ok := v.X.(*ast.Ident)
		return ok && id.Name == "List"
	default:
		return false
	}
}

/**/

func postingFromIDs(ids ...uint64) List {
	var out List
	for _, id := range ids {
		out = out.BuildAdded(id)
	}
	return out
}

func TestCloneOwnedSmallIsIndependent(t *testing.T) {
	var base List
	base = base.BuildAdded(3)
	base = base.BuildAdded(9)
	base = base.BuildAdded(17)
	clone := base.Clone()

	clone = clone.BuildAdded(40)

	if base.Cardinality() != 3 {
		t.Fatalf("base cardinality changed: got %d want 3", base.Cardinality())
	}
	if base.Contains(40) {
		t.Fatalf("base unexpectedly contains cloned add")
	}
	if clone.Cardinality() != 4 {
		t.Fatalf("clone cardinality mismatch: got %d want 4", clone.Cardinality())
	}
	if !clone.Contains(40) {
		t.Fatalf("clone missing added value")
	}

	base.Release()
	clone.Release()
}

func TestCloneIntoLargeKeepsSharedContainerClone(t *testing.T) {
	var (
		src  List
		want []uint64
	)
	for i := uint64(0); i < 96; i++ {
		id := (i << 1) + 1
		src = src.BuildAdded(id)
		want = append(want, id)
	}
	defer src.Release()

	dst := postingFromIDs(999, 1001, 1003)
	defer dst.Release()

	dst = src.CloneInto(dst)

	srcLarge := src.largeRef()
	dstLarge := dst.largeRef()
	if srcLarge == nil || dstLarge == nil {
		t.Fatalf("expected large postings on CloneInto path")
	}
	if len(srcLarge.highlowcontainer.containers) == 0 || len(dstLarge.highlowcontainer.containers) == 0 {
		t.Fatalf("expected populated large posting containers")
	}
	if srcLarge.highlowcontainer.containers[0] != dstLarge.highlowcontainer.containers[0] {
		t.Fatalf("large CloneInto must preserve shared/COW container clone semantics")
	}

	dst = dst.BuildAdded(513)

	assertSameListSet(t, src, want)
	if src.Contains(513) {
		t.Fatalf("CloneInto mutation leaked into source")
	}
	if !dst.Contains(513) {
		t.Fatalf("CloneInto destination lost local mutation")
	}
}

func TestBuildRemovedTwoElementPostingBecomesSingleton(t *testing.T) {
	ids := postingFromIDs(7, 11)
	ids = ids.BuildRemoved(7)

	if !ids.isSingleton() {
		t.Fatalf("expected singleton after removing one of two elements")
	}
	if id, ok := ids.TrySingle(); !ok || id != 11 {
		t.Fatalf("unexpected singleton contents: got=%d ok=%v", id, ok)
	}
}

func TestBuildRemovedOwnedCompactReusesPayload(t *testing.T) {
	t.Run("Small", func(t *testing.T) {
		ids := postingFromIDs(3, 7, 11, 19)
		defer ids.Release()

		before := ids.small()
		ids = ids.BuildRemoved(11)

		after := ids.small()
		if after == nil {
			t.Fatalf("expected small representation after removal")
		}
		if after != before {
			t.Fatalf("owned small removal must reuse payload")
		}
		assertSameListSet(t, ids, []uint64{3, 7, 19})
	})

	t.Run("Mid", func(t *testing.T) {
		ids := postingFromIDs(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
		defer ids.Release()

		before := ids.mid()
		ids = ids.BuildRemoved(10)

		after := ids.mid()
		if after == nil {
			t.Fatalf("expected mid representation after removal")
		}
		if after != before {
			t.Fatalf("owned mid removal must reuse payload")
		}
		assertSameListSet(t, ids, []uint64{2, 4, 6, 8, 12, 14, 16, 18, 20})
	})
}

func TestAdaptiveLifecycle(t *testing.T) {
	var ids List
	for _, id := range []uint64{10, 20, 30, 40, 50, 60, 70, 80} {
		ids = ids.BuildAdded(id)
	}
	if ids.small() == nil || ids.mid() != nil || ids.largeRef() != nil {
		t.Fatalf("expected small representation at threshold")
	}
	if ids.Cardinality() != SmallCap {
		t.Fatalf("unexpected small cardinality: got %d want %d", ids.Cardinality(), SmallCap)
	}

	ids = ids.BuildAdded(90)
	if ids.small() != nil || ids.mid() == nil || ids.largeRef() != nil {
		t.Fatalf("expected mid representation after small spill")
	}
	if ids.Cardinality() != SmallCap+1 {
		t.Fatalf("unexpected mid cardinality: got %d want %d", ids.Cardinality(), SmallCap+1)
	}

	for id := uint64(100); id < 100+uint64(MidCap-SmallCap); id++ {
		ids = ids.BuildAdded(id)
	}
	if ids.largeRef() == nil || ids.small() != nil || ids.mid() != nil {
		t.Fatalf("expected large-backed representation after mid spill")
	}
	if ids.Cardinality() != MidCap+1 {
		t.Fatalf("unexpected bitmap cardinality: got %d want %d", ids.Cardinality(), MidCap+1)
	}

	ids = ids.BuildRemoved(123)
	if ids.largeRef() == nil || ids.mid() != nil {
		t.Fatalf("expected large representation to stay after lazy downshift")
	}
	ids = ids.BuildOptimized()
	if ids.largeRef() != nil || ids.mid() == nil {
		t.Fatalf("expected explicit optimize to collapse large representation into mid")
	}

	for id := uint64(100); id < 123; id++ {
		ids = ids.BuildRemoved(id)
	}
	ids = ids.BuildRemoved(90)
	if ids.small() == nil || ids.largeRef() != nil || ids.mid() != nil {
		t.Fatalf("expected small representation after collapse")
	}
	if ids.Cardinality() != SmallCap {
		t.Fatalf("unexpected collapsed cardinality: got %d want %d", ids.Cardinality(), SmallCap)
	}
	for _, id := range []uint64{10, 20, 30, 40, 50, 60, 70, 80} {
		if !ids.Contains(id) {
			t.Fatalf("collapsed posting missing id=%d", id)
		}
	}
}

func TestTrySingleOnLargeBackedSingleton(t *testing.T) {
	var ids List
	for i := uint64(1); i <= MidCap+1; i++ {
		ids = ids.BuildAdded(i)
	}
	for i := uint64(1); i < MidCap+1; i++ {
		ids = ids.BuildRemoved(i)
	}
	if ids.largeRef() == nil {
		t.Fatalf("expected lazy downshift to keep large representation")
	}
	if got, ok := ids.TrySingle(); !ok || got != MidCap+1 {
		t.Fatalf("unexpected TrySingle result: got=%d ok=%v want=%d", got, ok, MidCap+1)
	}
}

func TestWriteReadPostingCompactRoundTrip(t *testing.T) {
	ids := postingFromIDs(3, 9, 17, 40)

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := ids.WriteTo(writer); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if got.small() == nil || got.largeRef() != nil {
		t.Fatalf("expected compact representation after round-trip")
	}
	if got.Cardinality() != ids.Cardinality() {
		t.Fatalf("cardinality mismatch: got %d want %d", got.Cardinality(), ids.Cardinality())
	}
	for _, id := range []uint64{3, 9, 17, 40} {
		if !got.Contains(id) {
			t.Fatalf("round-trip posting missing id=%d", id)
		}
	}
}

func TestWriteReadPostingEmptyRoundTrip(t *testing.T) {
	var ids List

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := ids.WriteTo(writer); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if !got.IsEmpty() {
		t.Fatalf("expected empty posting after round-trip")
	}
}

func TestReadSmallPostingSingletonCanonicalizes(t *testing.T) {
	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writer.WriteByte(encodingSmall); err != nil {
		t.Fatalf("WriteByte: %v", err)
	}
	if err := writeUvarint(writer, 1); err != nil {
		t.Fatalf("write len: %v", err)
	}
	if err := writeUvarint(writer, 17); err != nil {
		t.Fatalf("write id: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if !got.isSingleton() {
		t.Fatalf("expected singleton canonicalization for small payload len=1")
	}
	if got.small() != nil || got.mid() != nil || got.largeRef() != nil {
		t.Fatalf("unexpected non-singleton representation after canonicalization")
	}
	if id, ok := got.TrySingle(); !ok || id != 17 {
		t.Fatalf("unexpected singleton contents: got=%d ok=%v", id, ok)
	}
}

func TestReadFromReusedReceiverReleasesPreviousSmallPayload(t *testing.T) {
	ids := postingFromIDs(3, 9, 17, 40)
	prev := ids.small()
	if prev == nil {
		t.Fatalf("expected small-backed receiver")
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writer.WriteByte(encodingSingleton); err != nil {
		t.Fatalf("WriteByte: %v", err)
	}
	if err := writeUvarint(writer, 77); err != nil {
		t.Fatalf("write id: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	next, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	prevValue := ids
	ids = next
	prevValue.Release()
	if got, ok := ids.TrySingle(); !ok || got != 77 {
		t.Fatalf("unexpected singleton contents after reuse: got=%d ok=%v", got, ok)
	}

	reused := postingFromIDs(1, 2, 3, 4)
	defer reused.Release()
	if !testRaceEnabled && reused.small() != prev {
		t.Fatalf("reused receiver payload was not returned to the pool")
	}
}

func TestCompactPostingPools_ReacquireCanonicalState(t *testing.T) {
	t.Run("SmallPosting", func(t *testing.T) {
		sp := smallSetPool.Get()
		sp.n = 3
		sp.ids[0] = 11
		sp.ids[1] = 22
		sp.ids[2] = 33
		sp.Release()

		reused := smallSetPool.Get()
		if !testRaceEnabled && reused != sp {
			t.Fatalf("small posting pool did not reuse instance")
		}
		if reused.n != 0 {
			t.Fatalf("reacquired small posting not in canonical state: n=%d", reused.n)
		}
		reused.Release()
	})

	t.Run("MidPosting", func(t *testing.T) {
		mp := midSetPool.Get()
		mp.n = 5
		mp.ids[0] = 5
		mp.ids[1] = 10
		mp.ids[2] = 15
		mp.ids[3] = 20
		mp.ids[4] = 25
		mp.Release()

		reused := midSetPool.Get()
		if !testRaceEnabled && reused != mp {
			t.Fatalf("mid posting pool did not reuse instance")
		}
		if reused.n != 0 {
			t.Fatalf("reacquired mid posting not in canonical state: n=%d", reused.n)
		}
		reused.Release()
	})
}

func TestCompactPostingIteratorPool_ReacquireCanonicalState(t *testing.T) {
	it := compactPostingIterPool.Get()
	it.ids = []uint64{3, 7, 11}
	it.i = 2
	it.Release()

	reused := compactPostingIterPool.Get()
	if !testRaceEnabled && reused != it {
		t.Fatalf("compact posting iterator pool did not reuse iterator")
	}
	if reused.ids != nil {
		t.Fatalf("reacquired iterator leaked previous slice")
	}
	if reused.i != 0 {
		t.Fatalf("reacquired iterator leaked previous position: %d", reused.i)
	}
	reused.Release()
}

func TestWriteReadPostingMidRoundTrip(t *testing.T) {
	var ids List
	for i := uint64(1); i <= MidCap; i++ {
		ids = ids.BuildAdded(i * 3)
	}
	if ids.mid() == nil || ids.largeRef() != nil {
		t.Fatalf("expected compact mid representation")
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := ids.WriteTo(writer); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if got.Cardinality() != ids.Cardinality() {
		t.Fatalf("cardinality mismatch: got %d want %d", got.Cardinality(), ids.Cardinality())
	}
	for i := uint64(1); i <= MidCap; i++ {
		if !got.Contains(i * 3) {
			t.Fatalf("round-trip posting missing id=%d", i*3)
		}
	}
}

func TestWriteReadPostingLargeRoundTrip(t *testing.T) {
	var ids List

	for i := uint64(0); i < 256; i++ {
		ids = ids.BuildAdded(i)
	}
	ids = ids.BuildAdded(1<<32 | 3)
	ids = ids.BuildAdded(1<<32 | 9)
	ids = ids.BuildAdded(1<<32 | 15)
	for i := uint64(0); i < 5000; i++ {
		ids = ids.BuildAdded(2<<32 | (i << 1))
	}
	ids = ids.BuildOptimized()

	if ids.kind() != postingKindLarge {
		t.Fatalf("expected large-backed posting before round-trip")
	}

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := ids.WriteTo(writer); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload.Bytes())))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if got.kind() != postingKindLarge {
		t.Fatalf("expected large-backed representation after round-trip")
	}
	if got.Cardinality() != ids.Cardinality() {
		t.Fatalf("cardinality mismatch: got %d want %d", got.Cardinality(), ids.Cardinality())
	}

	for _, id := range []uint64{0, 255, 1<<32 | 9, 2 << 32, 2<<32 | ((5000 - 1) << 1)} {
		if !got.Contains(id) {
			t.Fatalf("round-trip posting missing id=%d", id)
		}
	}
	if got.Contains(1<<32|7) || got.Contains(2<<32|1) {
		t.Fatalf("round-trip posting gained values that were never written")
	}
}

func TestSmallPostingIterationConcurrentStable(t *testing.T) {
	ids := postingFromIDs(3, 7, 11, 19, 23, 31, 41, 43)
	want := []uint64{3, 7, 11, 19, 23, 31, 41, 43}

	var failed atomic.Pointer[string]
	setFailed := func(msg string) {
		if failed.Load() != nil {
			return
		}
		copyMsg := msg
		failed.CompareAndSwap(nil, &copyMsg)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2_000; i++ {
				iter := ids.Iter()
				var got []uint64
				for iter.HasNext() {
					got = append(got, iter.Next())
				}
				if !slices.Equal(got, want) {
					setFailed(fmt.Sprintf("iter ids mismatch: got=%v want=%v", got, want))
					return
				}
			}
		}()
	}

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}
}

func TestBorrowedSmallMutationDetaches(t *testing.T) {
	base := postingFromIDs(3, 7, 11, 19)
	borrowed := base.Borrow()
	if !borrowed.IsBorrowed() {
		t.Fatalf("expected borrowed handle")
	}
	if !base.SharesPayload(borrowed) {
		t.Fatalf("expected borrowed alias to share payload with base")
	}

	borrowed = borrowed.BuildAdded(23)

	if base.Contains(23) {
		t.Fatalf("borrowed add mutated base payload")
	}
	if !borrowed.Contains(23) {
		t.Fatalf("borrowed add missing detached value")
	}
	if base.SharesPayload(borrowed) {
		t.Fatalf("detached borrowed handle still shares payload with base")
	}

	borrowed.Release()
	base.Release()
}

func TestBorrowedLargeMutationDetaches(t *testing.T) {
	var base List
	for i := uint64(1); i <= MidCap+1; i++ {
		base = base.BuildAdded(i)
	}
	if base.largeRef() == nil {
		t.Fatalf("expected large-backed base posting")
	}

	borrowed := base.Borrow()
	if !borrowed.IsBorrowed() {
		t.Fatalf("expected borrowed large handle")
	}
	if !base.SharesPayload(borrowed) {
		t.Fatalf("expected borrowed large handle to share payload with base")
	}

	mask := postingFromIDs(1, 2, 3)
	borrowed = borrowed.BuildAndNot(mask)
	mask.Release()

	for _, id := range []uint64{1, 2, 3} {
		if !base.Contains(id) {
			t.Fatalf("borrowed bitmap mutation changed base id=%d", id)
		}
		if borrowed.Contains(id) {
			t.Fatalf("borrowed bitmap mutation kept removed id=%d", id)
		}
	}
	if base.SharesPayload(borrowed) {
		t.Fatalf("detached large handle still shares payload with base")
	}

	borrowed.Release()
	base.Release()
}

func TestBorrowedMutationInvariant_SourceRemainsStable(t *testing.T) {
	buildLarge := func() List {
		var out List
		for i := uint64(1); i <= MidCap+8; i++ {
			out = out.BuildAdded(i)
		}
		return out
	}

	cases := []struct {
		name         string
		makeBase     func() List
		mutate       func(List) List
		wantBorrowed []uint64
	}{
		{
			name:     "SmallAdd",
			makeBase: func() List { return postingFromIDs(3, 7, 11, 19) },
			mutate: func(ids List) List {
				return ids.BuildAdded(23)
			},
			wantBorrowed: []uint64{3, 7, 11, 19, 23},
		},
		{
			name:     "MidAndNotInPlace",
			makeBase: func() List { return postingFromIDs(2, 4, 6, 8, 10, 12, 14, 16, 18, 20) },
			mutate: func(ids List) List {
				mask := postingFromIDs(6, 8, 20)
				ids = ids.BuildAndNot(mask)
				mask.Release()
				return ids
			},
			wantBorrowed: []uint64{2, 4, 10, 12, 14, 16, 18},
		},
		{
			name:     "LargeAndInPlace",
			makeBase: buildLarge,
			mutate: func(ids List) List {
				keep := postingFromIDs(2, 4, 6, 8, 10, 12)
				ids = ids.BuildAnd(keep)
				keep.Release()
				return ids
			},
			wantBorrowed: []uint64{2, 4, 6, 8, 10, 12},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			base := tc.makeBase()
			wantBase := base.ToArray()

			borrowed := base.Borrow()
			if !borrowed.IsBorrowed() {
				t.Fatalf("expected borrowed handle")
			}
			if !borrowed.SharesPayload(base) {
				t.Fatalf("borrowed handle must start as an alias")
			}

			borrowed = tc.mutate(borrowed)

			if gotBase := base.ToArray(); !slices.Equal(gotBase, wantBase) {
				t.Fatalf("borrowed mutation changed source: got=%v want=%v", gotBase, wantBase)
			}
			if gotBorrowed := borrowed.ToArray(); !slices.Equal(gotBorrowed, tc.wantBorrowed) {
				t.Fatalf("borrowed mutation result mismatch: got=%v want=%v", gotBorrowed, tc.wantBorrowed)
			}
			if borrowed.SharesPayload(base) {
				t.Fatalf("mutated borrowed handle still shares source payload")
			}

			borrowed.Release()
			base.Release()
		})
	}
}

func TestLargePostingPoolReleaseDoesNotCorruptSharedClone(t *testing.T) {
	src := largePostingPool.Get()
	src.add(1)
	src.add(2)
	src.add(uint64(1)<<32 | 7)
	src.add(uint64(2)<<32 | 11)

	clone := src.cloneSharedInto(largePostingPool.Get())
	src.Release()

	if !clone.contains(1) || !clone.contains(2) || !clone.contains(uint64(1)<<32|7) || !clone.contains(uint64(2)<<32|11) {
		t.Fatalf("shared clone lost data after source was returned to pool")
	}

	clone.add(uint64(9) << 32)
	if !clone.contains(uint64(9) << 32) {
		t.Fatalf("clone mutation failed after pooled source release")
	}

	clone.Release()

	reused := largePostingPool.Get()
	defer reused.Release()
	if !reused.isEmpty() {
		t.Fatalf("reused large posting is not empty")
	}
}

func TestCompactAndIntoLargeIntersection(t *testing.T) {
	ids := postingFromIDs(3, 9, 17, 40)
	dst := largePostingOf(1, 3, 5, 9, 17, 100)
	ids.andIntoLarge(dst)
	if dst.cardinality() != 3 {
		t.Fatalf("unexpected cardinality after intersection: got %d want 3", dst.cardinality())
	}
	for _, id := range []uint64{3, 9, 17} {
		if !dst.contains(id) {
			t.Fatalf("intersection missing id=%d", id)
		}
	}
	if dst.contains(40) {
		t.Fatalf("unexpected id=40 after intersection")
	}
}

func encodeUvarint(v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return buf[:n]
}

func assertListRepresentation(t *testing.T, list List, want string) {
	t.Helper()
	switch want {
	case "empty":
		if !list.IsEmpty() {
			t.Fatalf("expected empty representation")
		}
	case "singleton":
		if !list.isSingleton() {
			t.Fatalf("expected singleton representation")
		}
	case "small":
		if list.small() == nil || list.mid() != nil || list.largeRef() != nil {
			t.Fatalf("expected small representation")
		}
	case "mid":
		if list.small() != nil || list.mid() == nil || list.largeRef() != nil {
			t.Fatalf("expected mid representation")
		}
	case "large":
		if list.small() != nil || list.mid() != nil || list.largeRef() == nil {
			t.Fatalf("expected large representation")
		}
	default:
		t.Fatalf("unknown representation %q", want)
	}
}

func TestListReadOnlyAPIAcrossRepresentations(t *testing.T) {
	largeIDs := make([]uint64, 0, MidCap+8)
	for i := uint64(0); i < MidCap+4; i++ {
		largeIDs = append(largeIDs, i*3+1)
	}
	largeIDs = append(largeIDs, 1<<32|5, 1<<32|9, 1<<32|17, 2<<32|33)

	cases := []struct {
		name string
		ids  []uint64
		repr string
	}{
		{name: "empty", repr: "empty"},
		{name: "singleton", ids: []uint64{42}, repr: "singleton"},
		{name: "small", ids: []uint64{3, 7, 11, 19}, repr: "small"},
		{name: "mid", ids: []uint64{2, 5, 8, 13, 21, 34, 55, 89, 144}, repr: "mid"},
		{name: "large", ids: largeIDs, repr: "large"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := postingFromIDs(tc.ids...)
			defer list.Release()

			want := canonicalUint64s(tc.ids)
			assertListRepresentation(t, list, tc.repr)
			assertSameListSet(t, list, want)

			single, singleOK := list.TrySingle()
			if len(want) == 1 {
				if !singleOK || single != want[0] {
					t.Fatalf("TrySingle mismatch: got=%d ok=%v want=%d", single, singleOK, want[0])
				}
			} else if singleOK {
				t.Fatalf("TrySingle unexpectedly succeeded: %d", single)
			}

			minimum, minOK := list.Minimum()
			maximum, maxOK := list.Maximum()
			if len(want) == 0 {
				if minOK || maxOK {
					t.Fatalf("empty list returned minimum/maximum")
				}
			} else {
				if !minOK || minimum != want[0] {
					t.Fatalf("Minimum mismatch: got=%d ok=%v want=%d", minimum, minOK, want[0])
				}
				if !maxOK || maximum != want[len(want)-1] {
					t.Fatalf("Maximum mismatch: got=%d ok=%v want=%d", maximum, maxOK, want[len(want)-1])
				}
			}

			switch tc.repr {
			case "empty":
				if got := list.SizeInBytes(); got != 0 {
					t.Fatalf("SizeInBytes mismatch: got=%d want=0", got)
				}
			case "singleton":
				if got := list.SizeInBytes(); got != 8 {
					t.Fatalf("SizeInBytes mismatch: got=%d want=8", got)
				}
			case "small", "mid":
				if got := list.SizeInBytes(); got != uint64(len(want))*8 {
					t.Fatalf("SizeInBytes mismatch: got=%d want=%d", got, len(want)*8)
				}
			case "large":
				if got := list.SizeInBytes(); got != list.largeRef().sizeInBytes() {
					t.Fatalf("SizeInBytes mismatch: got=%d want=%d", got, list.largeRef().sizeInBytes())
				}
			}

			for _, id := range want {
				if !list.Contains(id) {
					t.Fatalf("Contains(%d)=false", id)
				}
			}
			for _, id := range []uint64{0, 1, 4, 1<<32 | 7, 9<<32 | 1} {
				if slices.Contains(want, id) {
					continue
				}
				if list.Contains(id) {
					t.Fatalf("Contains(%d)=true for missing id", id)
				}
			}

			var foreach []uint64
			if ok := list.ForEach(func(id uint64) bool {
				foreach = append(foreach, id)
				return true
			}); !ok {
				t.Fatalf("ForEach unexpectedly returned false")
			}
			if !slices.Equal(foreach, want) {
				t.Fatalf("ForEach mismatch: got=%v want=%v", foreach, want)
			}

			iter := list.Iter()
			defer iter.Release()
			var iterIDs []uint64
			for iter.HasNext() {
				iterIDs = append(iterIDs, iter.Next())
			}
			if !slices.Equal(iterIDs, want) {
				t.Fatalf("Iter mismatch: got=%v want=%v", iterIDs, want)
			}

			other := postingFromIDs(0, 3, 8, 21, 42, 1<<32|9, 2<<32|33)
			defer other.Release()
			wantIntersect := intersectUint64(want, listToSlice(other))

			var intersected []uint64
			stopped := list.ForEachIntersecting(other, func(id uint64) bool {
				intersected = append(intersected, id)
				return false
			})
			if stopped {
				t.Fatalf("ForEachIntersecting unexpectedly reported stop")
			}
			if !slices.Equal(intersected, wantIntersect) {
				t.Fatalf("ForEachIntersecting mismatch: got=%v want=%v", intersected, wantIntersect)
			}

			var firstOnly []uint64
			stopped = list.ForEachIntersecting(other, func(id uint64) bool {
				firstOnly = append(firstOnly, id)
				return true
			})
			if len(wantIntersect) == 0 {
				if stopped || len(firstOnly) != 0 {
					t.Fatalf("ForEachIntersecting stop mismatch on empty intersection")
				}
			} else {
				if !stopped || !slices.Equal(firstOnly, wantIntersect[:1]) {
					t.Fatalf("ForEachIntersecting early stop mismatch: got=%v want=%v", firstOnly, wantIntersect[:1])
				}
			}
		})
	}
}

func TestListCheckedAddAddManyAndIterators(t *testing.T) {
	var list List
	var added bool
	list, added = list.BuildAddedChecked(7)
	if !added {
		t.Fatalf("CheckedAdd must report newly added id")
	}
	list, added = list.BuildAddedChecked(7)
	if added {
		t.Fatalf("CheckedAdd must reject duplicate id")
	}
	list = list.BuildAddedMany([]uint64{9, 3, 5, 7, 9, 11})
	defer list.Release()
	assertSameListSet(t, list, []uint64{3, 5, 7, 9, 11})

	emptyIt := (List{}).Iter()
	if emptyIt.HasNext() {
		t.Fatalf("empty iterator must have no elements")
	}
	if got := emptyIt.Next(); got != 0 {
		t.Fatalf("empty iterator Next mismatch: got=%d want=0", got)
	}
	emptyIt.Release()

	single := postingFromIDs(42)
	defer single.Release()
	singleIter := single.Iter()
	if !singleIter.HasNext() {
		t.Fatalf("singleton iterator must have one element")
	}
	if got := singleIter.Next(); got != 42 {
		t.Fatalf("singleton iterator Next mismatch: got=%d want=42", got)
	}
	if singleIter.HasNext() {
		t.Fatalf("singleton iterator must be exhausted")
	}
	if got := singleIter.Next(); got != 0 {
		t.Fatalf("exhausted singleton iterator Next mismatch: got=%d want=0", got)
	}
	singleIter.Release()

	emptyIter{}.Release()
	(&singletonIter{v: 1, has: true}).Release()
}

func TestListCheckedAddBorrowedDuplicateNoDetach(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		base := postingFromIDs(3, 7, 11, 19)
		defer base.Release()

		borrowed := base.Borrow()
		defer borrowed.Release()

		var added bool
		borrowed, added = borrowed.BuildAddedChecked(11)
		if added {
			t.Fatalf("CheckedAdd must reject duplicate id")
		}
		if !borrowed.SharesPayload(base) {
			t.Fatalf("duplicate CheckedAdd detached borrowed small payload")
		}
		borrowed, added = borrowed.BuildAddedChecked(23)
		if !added {
			t.Fatalf("CheckedAdd must report newly added borrowed small id")
		}
		if borrowed.SharesPayload(base) {
			t.Fatalf("borrowed small add must detach on miss")
		}
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, borrowed, []uint64{3, 7, 11, 19, 23})
	})

	t.Run("large", func(t *testing.T) {
		var base List
		for i := uint64(1); i <= MidCap+4; i++ {
			base = base.BuildAdded(i * 2)
		}
		defer base.Release()

		borrowed := base.Borrow()
		defer borrowed.Release()

		var added bool
		borrowed, added = borrowed.BuildAddedChecked(8)
		if added {
			t.Fatalf("CheckedAdd must reject duplicate large id")
		}
		if !borrowed.SharesPayload(base) {
			t.Fatalf("duplicate CheckedAdd detached borrowed large payload")
		}
		extra := uint64(9)<<32 | 1
		borrowed, added = borrowed.BuildAddedChecked(extra)
		if !added {
			t.Fatalf("CheckedAdd must report newly added borrowed large id")
		}
		if borrowed.SharesPayload(base) {
			t.Fatalf("borrowed large add must detach on miss")
		}
		if base.Contains(extra) {
			t.Fatalf("borrowed large CheckedAdd mutated base payload")
		}
		if !borrowed.Contains(extra) {
			t.Fatalf("borrowed large CheckedAdd lost inserted id")
		}
	})
}

func TestListAddManySortedLargeBatchDetachesBorrowedLarge(t *testing.T) {
	var base List
	for i := uint64(0); i < MidCap+8; i++ {
		base = base.BuildAdded(i*2 + 1)
	}
	defer base.Release()

	borrowed := base.Borrow()
	extra := make([]uint64, 96)
	for i := range extra {
		extra[i] = (1 << 32) | uint64(i)
	}

	borrowed = borrowed.BuildAddedMany(extra)
	defer borrowed.Release()

	if borrowed.SharesPayload(base) {
		t.Fatalf("sorted AddMany must detach borrowed large payload")
	}
	if base.Contains(1 << 32) {
		t.Fatalf("borrowed sorted AddMany mutated base payload")
	}

	want := append(base.ToArray(), extra...)
	assertSameListSet(t, borrowed, want)
}

func TestListAddManyUnsortedLargeBatchDetachesBorrowedLarge(t *testing.T) {
	var base List
	for i := uint64(0); i < MidCap+8; i++ {
		base = base.BuildAdded(i*2 + 1)
	}
	defer base.Release()

	borrowed := base.Borrow()
	extra := []uint64{
		3<<32 | 9,
		1<<32 | 7,
		3<<32 | 5,
		7,
		1<<32 | 3,
		7,
	}

	borrowed = borrowed.BuildAddedMany(extra)
	defer borrowed.Release()

	if borrowed.SharesPayload(base) {
		t.Fatalf("unsorted AddMany must detach borrowed large payload")
	}
	if base.Contains(1<<32|3) || base.Contains(3<<32|5) || base.Contains(3<<32|9) {
		t.Fatalf("borrowed unsorted AddMany mutated base payload")
	}

	want := append(base.ToArray(), extra...)
	assertSameListSet(t, borrowed, want)
}

func TestListFromLargeOwnedHelpers(t *testing.T) {
	emptyLarge := largePostingPool.Get()
	if got := fromLargeOwned(emptyLarge); !got.IsEmpty() {
		t.Fatalf("fromLargeOwned(empty) must return empty list")
	}
	reusedEmpty := largePostingPool.Get()
	defer reusedEmpty.Release()
	if !testRaceEnabled && reusedEmpty != emptyLarge {
		t.Fatalf("fromLargeOwned(empty) did not return large payload to pool")
	}

	singleLarge := largePostingPool.Get()
	singleLarge.add(77)
	gotSingle := fromLargeOwned(singleLarge)
	if id, ok := gotSingle.TrySingle(); !ok || id != 77 {
		t.Fatalf("fromLargeOwned(single) mismatch: got=%d ok=%v", id, ok)
	}

	smallLarge := largePostingPool.Get()
	smallLarge.addMany([]uint64{1, 3, 5, 7})
	gotSmall := fromLargeOwned(smallLarge)
	defer gotSmall.Release()
	assertSameListSet(t, gotSmall, []uint64{1, 3, 5, 7})
	assertListRepresentation(t, gotSmall, "small")
}

func TestListBorrowDoesNotAllocate(t *testing.T) {
	small := postingFromIDs(3, 7, 11, 19)
	defer small.Release()

	large := postingFromIDs()
	for i := uint64(0); i < MidCap+8; i++ {
		large = large.BuildAdded(i*3 + 1)
	}
	defer large.Release()

	for _, tc := range []struct {
		name string
		list List
	}{
		{name: "empty", list: List{}},
		{name: "singleton", list: postingFromIDs(42)},
		{name: "small", list: small},
		{name: "large", list: large},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "singleton" {
				defer tc.list.Release()
			}
			allocs := testing.AllocsPerRun(1000, func() {
				b := tc.list.Borrow()
				if !tc.list.IsEmpty() && !tc.list.isSingleton() && !b.SharesPayload(tc.list) {
					t.Fatalf("Borrow must share payload")
				}
				b.Release()
			})
			if allocs != 0 {
				t.Fatalf("Borrow allocated: got=%v want=0", allocs)
			}
		})
	}
}

func TestListSetOpsMatrix(t *testing.T) {
	cases := []struct {
		name string
		ids  []uint64
	}{
		{name: "empty"},
		{name: "singleton", ids: []uint64{7}},
		{name: "small", ids: []uint64{1, 3, 5, 9}},
		{name: "mid", ids: []uint64{2, 4, 6, 8, 10, 12, 14, 16, 18}},
		{name: "large", ids: func() []uint64 {
			out := make([]uint64, 0, MidCap+3)
			for i := uint64(0); i < MidCap+1; i++ {
				out = append(out, i*2)
			}
			out = append(out, 1<<32|5, 1<<32|11)
			return out
		}()},
	}

	for _, lhs := range cases {
		for _, rhs := range cases {
			name := fmt.Sprintf("%s_x_%s", lhs.name, rhs.name)
			t.Run(name, func(t *testing.T) {
				wantUnion := unionUint64(lhs.ids, rhs.ids)
				wantIntersection := intersectUint64(lhs.ids, rhs.ids)
				wantDiff := differenceUint64(lhs.ids, rhs.ids)

				left := postingFromIDs(lhs.ids...)
				right := postingFromIDs(rhs.ids...)
				defer left.Release()
				defer right.Release()

				if got := left.Intersects(right); got != (len(wantIntersection) > 0) {
					t.Fatalf("Intersects mismatch: got=%v want=%v", got, len(wantIntersection) > 0)
				}
				if got := right.Intersects(left); got != (len(wantIntersection) > 0) {
					t.Fatalf("reverse Intersects mismatch: got=%v want=%v", got, len(wantIntersection) > 0)
				}
				if got := left.AndCardinality(right); got != uint64(len(wantIntersection)) {
					t.Fatalf("AndCardinality mismatch: got=%d want=%d", got, len(wantIntersection))
				}
				if got := right.AndCardinality(left); got != uint64(len(wantIntersection)) {
					t.Fatalf("reverse AndCardinality mismatch: got=%d want=%d", got, len(wantIntersection))
				}

				unionList := postingFromIDs(lhs.ids...)
				unionRight := postingFromIDs(rhs.ids...)
				unionList = unionList.BuildOr(unionRight)
				assertSameListSet(t, unionList, wantUnion)
				unionList.Release()
				unionRight.Release()

				intersectionList := postingFromIDs(lhs.ids...)
				intersectionRight := postingFromIDs(rhs.ids...)
				intersectionList = intersectionList.BuildAnd(intersectionRight)
				assertSameListSet(t, intersectionList, wantIntersection)
				intersectionList.Release()
				intersectionRight.Release()

				diffList := postingFromIDs(lhs.ids...)
				diffRight := postingFromIDs(rhs.ids...)
				diffList = diffList.BuildAndNot(diffRight)
				assertSameListSet(t, diffList, wantDiff)
				diffList.Release()
				diffRight.Release()

				var dst List
				src := postingFromIDs(lhs.ids...)
				dst = dst.BuildOr(src)
				assertSameListSet(t, dst, lhs.ids)
				src.Release()
				dst.Release()

				dst = postingFromIDs(lhs.ids...)
				src = postingFromIDs(rhs.ids...)
				dst = dst.BuildAndNot(src)
				assertSameListSet(t, dst, wantDiff)
				src.Release()
				dst.Release()
			})
		}
	}
}

func TestListAlgebraicExpectations(t *testing.T) {
	aIDs := []uint64{1, 3, 5, 7, 9, 1<<32 | 5}
	bIDs := []uint64{3, 4, 5, 8, 13, 1<<32 | 5}

	a := postingFromIDs(aIDs...)
	b := postingFromIDs(bIDs...)
	defer a.Release()
	defer b.Release()

	unionAB := postingFromIDs(aIDs...)
	unionABRight := postingFromIDs(bIDs...)
	unionAB = unionAB.BuildOr(unionABRight)
	unionABRight.Release()
	defer unionAB.Release()

	unionBA := postingFromIDs(bIDs...)
	unionBARight := postingFromIDs(aIDs...)
	unionBA = unionBA.BuildOr(unionBARight)
	unionBARight.Release()
	defer unionBA.Release()

	assertSameListSet(t, unionAB, unionUint64(aIDs, bIDs))
	assertSameListSet(t, unionBA, unionUint64(aIDs, bIDs))

	interAB := postingFromIDs(aIDs...)
	interABRight := postingFromIDs(bIDs...)
	interAB = interAB.BuildAnd(interABRight)
	interABRight.Release()
	defer interAB.Release()

	interBA := postingFromIDs(bIDs...)
	interBARight := postingFromIDs(aIDs...)
	interBA = interBA.BuildAnd(interBARight)
	interBARight.Release()
	defer interBA.Release()

	assertSameListSet(t, interAB, intersectUint64(aIDs, bIDs))
	assertSameListSet(t, interBA, intersectUint64(aIDs, bIDs))

	diff := postingFromIDs(aIDs...)
	diffRight := postingFromIDs(aIDs...)
	diff = diff.BuildAndNot(diffRight)
	diffRight.Release()
	defer diff.Release()
	if !diff.IsEmpty() {
		t.Fatalf("A - A must be empty, got %v", diff.ToArray())
	}

	var intersected []uint64
	a.ForEachIntersecting(b, func(id uint64) bool {
		intersected = append(intersected, id)
		return false
	})
	if !slices.Equal(intersected, intersectUint64(aIDs, bIDs)) {
		t.Fatalf("ForEachIntersecting mismatch: got=%v want=%v", intersected, intersectUint64(aIDs, bIDs))
	}
}

func TestListForEachIntersectingLargeLargeFastPath(t *testing.T) {
	leftIDs := make([]uint64, 0, MidCap+48)
	rightIDs := make([]uint64, 0, MidCap+48)

	for i := uint64(0); i < MidCap+8; i++ {
		leftIDs = append(leftIDs, i*2)
	}
	for i := uint64(10); i < MidCap+18; i++ {
		rightIDs = append(rightIDs, i*2)
	}

	base := uint64(1) << 32
	for i := uint64(0); i < 20; i++ {
		leftIDs = append(leftIDs, base|i*3)
	}
	for i := uint64(5); i < 25; i++ {
		rightIDs = append(rightIDs, base|i*3)
	}

	base = uint64(7) << 32
	for i := uint64(0); i < 18; i++ {
		leftIDs = append(leftIDs, base|(i*5+1))
	}
	for i := uint64(0); i < 18; i++ {
		if i%2 == 0 {
			rightIDs = append(rightIDs, base|(i*5+1))
			continue
		}
		rightIDs = append(rightIDs, base|(i*5+2))
	}

	left := postingFromIDs(leftIDs...)
	right := postingFromIDs(rightIDs...)
	defer left.Release()
	defer right.Release()

	assertListRepresentation(t, left, "large")
	assertListRepresentation(t, right, "large")

	want := intersectUint64(leftIDs, rightIDs)
	if len(want) < 3 {
		t.Fatalf("test setup must produce at least 3 intersections, got %d", len(want))
	}

	var got []uint64
	stopped := left.ForEachIntersecting(right, func(id uint64) bool {
		got = append(got, id)
		return false
	})
	if stopped {
		t.Fatalf("ForEachIntersecting unexpectedly reported stop")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ForEachIntersecting mismatch: got=%v want=%v", got, want)
	}

	var first []uint64
	stopped = left.ForEachIntersecting(right, func(id uint64) bool {
		first = append(first, id)
		return len(first) == 3
	})
	if !stopped {
		t.Fatalf("ForEachIntersecting must report early stop")
	}
	if !slices.Equal(first, want[:3]) {
		t.Fatalf("ForEachIntersecting early stop mismatch: got=%v want=%v", first, want[:3])
	}
}

func TestListMixedIntersectionLazyLargeDownshift(t *testing.T) {
	baseIDs := make([]uint64, 0, MidCap+12)
	for i := uint64(0); i < MidCap+12; i++ {
		baseIDs = append(baseIDs, (i<<32)|(i*3+1))
	}

	large := postingFromIDs(baseIDs...)
	defer large.Release()

	keep := []uint64{baseIDs[1], baseIDs[9], baseIDs[17]}
	keepSet := make(map[uint64]struct{}, len(keep))
	for _, id := range keep {
		keepSet[id] = struct{}{}
	}
	for _, id := range baseIDs {
		if _, ok := keepSet[id]; ok {
			continue
		}
		large = large.BuildRemoved(id)
	}

	assertListRepresentation(t, large, "large")
	if got := large.Cardinality(); got != uint64(len(keep)) {
		t.Fatalf("lazy large cardinality mismatch: got=%d want=%d", got, len(keep))
	}

	otherIDs := []uint64{
		baseIDs[1],
		baseIDs[2],
		baseIDs[5],
		baseIDs[9],
		baseIDs[13],
		baseIDs[17],
		baseIDs[19],
		baseIDs[23],
		baseIDs[31],
	}
	other := postingFromIDs(otherIDs...)
	defer other.Release()

	assertListRepresentation(t, other, "mid")
	if got, want := large.Cardinality(), other.Cardinality(); got >= want {
		t.Fatalf("test setup must keep large cardinality below compact side: got=%d want<%d", got, want)
	}

	want := intersectUint64(keep, otherIDs)
	if !large.Intersects(other) || !other.Intersects(large) {
		t.Fatalf("Intersects must report mixed lazy-large overlap")
	}
	if got := large.AndCardinality(other); got != uint64(len(want)) {
		t.Fatalf("AndCardinality mismatch: got=%d want=%d", got, len(want))
	}
	if got := other.AndCardinality(large); got != uint64(len(want)) {
		t.Fatalf("reverse AndCardinality mismatch: got=%d want=%d", got, len(want))
	}

	var got []uint64
	stopped := large.ForEachIntersecting(other, func(id uint64) bool {
		got = append(got, id)
		return false
	})
	if stopped {
		t.Fatalf("ForEachIntersecting unexpectedly reported stop")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ForEachIntersecting mismatch: got=%v want=%v", got, want)
	}

	got = got[:0]
	stopped = other.ForEachIntersecting(large, func(id uint64) bool {
		got = append(got, id)
		return false
	})
	if stopped {
		t.Fatalf("reverse ForEachIntersecting unexpectedly reported stop")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("reverse ForEachIntersecting mismatch: got=%v want=%v", got, want)
	}
}

func TestListLifecycleHelpers(t *testing.T) {
	t.Run("BorrowAndMutateDetach", func(t *testing.T) {
		base := postingFromIDs(3, 7, 11, 19, 23, 29, 31, 37, 41)
		defer base.Release()

		borrowed := base.Borrow()
		if !borrowed.IsBorrowed() {
			t.Fatalf("expected borrowed handle")
		}
		if !base.SharesPayload(borrowed) {
			t.Fatalf("borrowed list must share payload with base")
		}

		borrowed = borrowed.BuildAdded(43)
		assertSameListSet(t, base, []uint64{3, 7, 11, 19, 23, 29, 31, 37, 41})
		assertSameListSet(t, borrowed, []uint64{3, 7, 11, 19, 23, 29, 31, 37, 41, 43})
		if borrowed.SharesPayload(base) {
			t.Fatalf("mutated borrowed list still shares payload with base")
		}
		borrowed.Release()
	})

	t.Run("CloneOwnedSmallIsIndependent", func(t *testing.T) {
		base := postingFromIDs(5, 9, 17, 33)
		defer base.Release()

		clone := base.Clone()
		defer clone.Release()
		if base.SharesPayload(clone) {
			t.Fatalf("small clone unexpectedly shares payload")
		}
		clone = clone.BuildAdded(99)
		assertSameListSet(t, base, []uint64{5, 9, 17, 33})
		assertSameListSet(t, clone, []uint64{5, 9, 17, 33, 99})
	})

	t.Run("ClearReleasesAndZeroesHandle", func(t *testing.T) {
		list := postingFromIDs(1, 2, 3, 4)
		prev := list.small()
		list.Release()
		list = List{}
		if !list.IsEmpty() {
			t.Fatalf("Clear must reset list to empty")
		}

		reused := postingFromIDs(10, 11, 12, 13)
		defer reused.Release()
		if !testRaceEnabled && reused.small() != prev {
			t.Fatalf("Clear did not return small payload to pool")
		}
	})

	t.Run("ReleaseSliceOwned", func(t *testing.T) {
		values := []List{
			postingFromIDs(1, 3, 5, 7),
			postingFromIDs(9, 11, 13, 15),
		}
		prev := values[0].small()
		ReleaseSliceOwned(values)
		for i, value := range values {
			if !value.IsEmpty() {
				t.Fatalf("slice value #%d not cleared", i)
			}
		}

		reused := postingFromIDs(21, 23, 25, 27)
		defer reused.Release()
		if !testRaceEnabled && reused.small() != prev {
			t.Fatalf("ReleaseSliceOwned did not release pool-backed payload")
		}
	})

	t.Run("ClearMapOwned", func(t *testing.T) {
		values := map[string]List{
			"a": postingFromIDs(1, 2, 3, 4),
			"b": postingFromIDs(8, 9, 10, 11),
		}
		prevA := values["a"].small()
		prevB := values["b"].small()
		ClearMapOwned(values)
		if len(values) != 0 {
			t.Fatalf("ClearMapOwned did not empty map")
		}

		reused := postingFromIDs(31, 33, 35, 37)
		defer reused.Release()
		if !testRaceEnabled && reused.small() != prevA && reused.small() != prevB {
			t.Fatalf("ClearMapOwned did not release pool-backed payload")
		}
	})

	t.Run("MergeOwnedAdoptsEmptyAndReleasesMergedPayload", func(t *testing.T) {
		var dst List
		add := postingFromIDs(4, 8, 12, 16)
		addPtr := add.small()
		dst = dst.BuildMergedOwned(add)
		assertSameListSet(t, dst, []uint64{4, 8, 12, 16})
		if dst.small() != addPtr {
			t.Fatalf("empty MergeOwned must adopt payload directly")
		}
		dst.Release()

		dst = postingFromIDs(1, 3, 5, 7)
		add = postingFromIDs(9, 11, 13, 15)
		addPtr = add.small()
		dst = dst.BuildMergedOwned(add)
		assertSameListSet(t, dst, []uint64{1, 3, 5, 7, 9, 11, 13, 15})

		reused := postingFromIDs(21, 23, 25, 27)
		defer reused.Release()
		if !testRaceEnabled && reused.small() != addPtr {
			t.Fatalf("merged payload was not released after MergeOwned")
		}
		dst.Release()
	})
}

func TestBorrowedReceiverSetOpsAndMergeOwnedDetach(t *testing.T) {
	t.Run("BorrowedSmallSetOpsDetach", func(t *testing.T) {
		base := postingFromIDs(3, 7, 11, 19)
		defer base.Release()

		orAdd := postingFromIDs(23, 29)
		defer orAdd.Release()
		orDst := base.Borrow()
		orDst = orDst.BuildOr(orAdd)
		defer orDst.Release()
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, orDst, []uint64{3, 7, 11, 19, 23, 29})
		if orDst.SharesPayload(base) {
			t.Fatalf("borrowed small OrInPlace still shares payload with base")
		}

		andMask := postingFromIDs(7, 19, 23)
		defer andMask.Release()
		andDst := base.Borrow()
		andDst = andDst.BuildAnd(andMask)
		defer andDst.Release()
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, andDst, []uint64{7, 19})
		if andDst.SharesPayload(base) {
			t.Fatalf("borrowed small AndInPlace still shares payload with base")
		}

		diffMask := postingFromIDs(7, 19)
		defer diffMask.Release()
		diffDst := base.Borrow()
		diffDst = diffDst.BuildAndNot(diffMask)
		defer diffDst.Release()
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, diffDst, []uint64{3, 11})
		if diffDst.SharesPayload(base) {
			t.Fatalf("borrowed small AndNotInPlace still shares payload with base")
		}
	})

	t.Run("BorrowedLargeSetOpsDetach", func(t *testing.T) {
		baseIDs := priorityLargeBaseIDs()
		base := postingFromIDs(baseIDs...)
		defer base.Release()

		orAddIDs := make([]uint64, 0, MidCap+8)
		for i := uint64(0); i < MidCap+4; i++ {
			orAddIDs = append(orAddIDs, 9<<32|i)
		}
		orAddIDs = append(orAddIDs, 10<<32|1, 10<<32|3, 11<<32|5, 11<<32|7)
		orAdd := postingFromIDs(orAddIDs...)
		defer orAdd.Release()
		orDst := base.Borrow()
		orDst = orDst.BuildOr(orAdd)
		defer orDst.Release()
		assertSameListSet(t, base, baseIDs)
		assertSameListSet(t, orDst, unionUint64(baseIDs, orAddIDs))
		if orDst.SharesPayload(base) {
			t.Fatalf("borrowed large OrInPlace still shares payload with base")
		}

		andMaskIDs := append(slices.Clone(baseIDs[:SmallCap]), 12<<32|1, 12<<32|3, 13<<32|5)
		andMask := postingFromIDs(andMaskIDs...)
		defer andMask.Release()
		andDst := base.Borrow()
		andDst = andDst.BuildAnd(andMask)
		defer andDst.Release()
		assertSameListSet(t, base, baseIDs)
		assertSameListSet(t, andDst, intersectUint64(baseIDs, andMaskIDs))
		if andDst.SharesPayload(base) {
			t.Fatalf("borrowed large AndInPlace still shares payload with base")
		}

		diffMaskIDs := append(slices.Clone(baseIDs[:SmallCap]), baseIDs[len(baseIDs)-2:]...)
		diffMask := postingFromIDs(diffMaskIDs...)
		defer diffMask.Release()
		diffDst := base.Borrow()
		diffDst = diffDst.BuildAndNot(diffMask)
		defer diffDst.Release()
		assertSameListSet(t, base, baseIDs)
		assertSameListSet(t, diffDst, differenceUint64(baseIDs, diffMaskIDs))
		if diffDst.SharesPayload(base) {
			t.Fatalf("borrowed large AndNotInPlace still shares payload with base")
		}
	})

	t.Run("MergeOwnedBorrowedSmallDetachesAndReleasesAdd", func(t *testing.T) {
		base := postingFromIDs(1, 3, 5, 7)
		defer base.Release()

		dst := base.Borrow()
		add := postingFromIDs(9, 11, 13, 15)
		addPtr := add.small()
		dst = dst.BuildMergedOwned(add)
		defer dst.Release()

		assertSameListSet(t, base, []uint64{1, 3, 5, 7})
		assertSameListSet(t, dst, []uint64{1, 3, 5, 7, 9, 11, 13, 15})
		if dst.SharesPayload(base) {
			t.Fatalf("MergeOwned on borrowed small dst still shares payload with base")
		}

		if addPtr.n != 0 {
			t.Fatalf("MergeOwned did not clear merged small payload")
		}
	})

	t.Run("MergeOwnedBorrowedLargeDetachesAndReleasesAdd", func(t *testing.T) {
		baseIDs := priorityLargeBaseIDs()
		base := postingFromIDs(baseIDs...)
		defer base.Release()

		addIDs := make([]uint64, 0, MidCap+8)
		for i := uint64(0); i < MidCap+4; i++ {
			addIDs = append(addIDs, 14<<32|i)
		}
		addIDs = append(addIDs, 15<<32|1, 15<<32|3, 16<<32|5, 16<<32|7)
		dst := base.Borrow()
		add := postingFromIDs(addIDs...)
		addPtr := add.largeRef()
		dst = dst.BuildMergedOwned(add)
		defer dst.Release()

		assertSameListSet(t, base, baseIDs)
		assertSameListSet(t, dst, unionUint64(baseIDs, addIDs))
		if dst.SharesPayload(base) {
			t.Fatalf("MergeOwned on borrowed large dst still shares payload with base")
		}

		if !addPtr.isEmpty() {
			t.Fatalf("MergeOwned did not clear merged large payload")
		}
	})
}

func TestBorrowedCompactSetOpsAgainstLargeDetach(t *testing.T) {
	largeIDs := make([]uint64, 0, MidCap+16)
	for i := uint64(0); i < MidCap+10; i++ {
		largeIDs = append(largeIDs, 1<<32|i*2)
	}
	largeIDs = append(largeIDs, 4, 7, 8, 12, 16, 19, 24, 28, 32, 36, 40)

	large := postingFromIDs(largeIDs...)
	defer large.Release()
	assertListRepresentation(t, large, "large")

	t.Run("Small", func(t *testing.T) {
		base := postingFromIDs(3, 7, 11, 19)
		defer base.Release()

		andDst := base.Borrow()
		andDst = andDst.BuildAnd(large)
		defer andDst.Release()
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, andDst, []uint64{7, 19})
		if andDst.SharesPayload(base) {
			t.Fatalf("borrowed small AndInPlace against large still shares payload")
		}

		diffDst := base.Borrow()
		diffDst = diffDst.BuildAndNot(large)
		defer diffDst.Release()
		assertSameListSet(t, base, []uint64{3, 7, 11, 19})
		assertSameListSet(t, diffDst, []uint64{3, 11})
		if diffDst.SharesPayload(base) {
			t.Fatalf("borrowed small AndNotInPlace against large still shares payload")
		}
	})

	t.Run("Mid", func(t *testing.T) {
		base := postingFromIDs(4, 8, 12, 16, 20, 24, 28, 32, 36)
		defer base.Release()

		andDst := base.Borrow()
		andDst = andDst.BuildAnd(large)
		defer andDst.Release()
		assertSameListSet(t, base, []uint64{4, 8, 12, 16, 20, 24, 28, 32, 36})
		assertSameListSet(t, andDst, []uint64{4, 8, 12, 16, 24, 28, 32, 36})
		assertListRepresentation(t, andDst, "small")
		if andDst.SharesPayload(base) {
			t.Fatalf("borrowed mid AndInPlace against large still shares payload")
		}

		diffDst := base.Borrow()
		diffDst = diffDst.BuildAndNot(large)
		defer diffDst.Release()
		assertSameListSet(t, base, []uint64{4, 8, 12, 16, 20, 24, 28, 32, 36})
		assertSameListSet(t, diffDst, []uint64{20})
		if diffDst.SharesPayload(base) {
			t.Fatalf("borrowed mid AndNotInPlace against large still shares payload")
		}
	})
}

func TestListSerializationRoundTripSkipAndReuse(t *testing.T) {
	largeIDs := make([]uint64, 0, MidCap+5)
	for i := uint64(0); i < MidCap+2; i++ {
		largeIDs = append(largeIDs, i*7+1)
	}
	largeIDs = append(largeIDs, 1<<32|9, 2<<32|11, 2<<32|13)

	cases := []struct {
		name string
		ids  []uint64
	}{
		{name: "empty"},
		{name: "singleton", ids: []uint64{77}},
		{name: "small", ids: []uint64{3, 7, 11, 19}},
		{name: "mid", ids: []uint64{2, 5, 8, 13, 21, 34, 55, 89, 144}},
		{name: "large", ids: largeIDs},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := postingFromIDs(tc.ids...)
			defer list.Release()

			payload := mustWriteListPayload(t, list)
			got := mustReadListPayload(t, payload)
			defer got.Release()
			assertSameListSet(t, got, tc.ids)

			receiver := postingFromIDs(100, 101, 102, 103)
			next, err := ReadFrom(bufio.NewReader(bytes.NewReader(payload)))
			if err != nil {
				t.Fatalf("ReadFrom(reuse): %v", err)
			}
			receiver.Release()
			receiver = next
			assertSameListSet(t, receiver, tc.ids)
			receiver.Release()
		})
	}

	var stream bytes.Buffer
	for _, tc := range cases {
		list := postingFromIDs(tc.ids...)
		payload := mustWriteListPayload(t, list)
		_, _ = stream.Write(payload)
		list.Release()
	}

	reader := bufio.NewReader(bytes.NewReader(stream.Bytes()))
	for i, tc := range cases {
		if i%2 == 0 {
			if err := Skip(reader); err != nil {
				t.Fatalf("Skip(%s): %v", tc.name, err)
			}
			continue
		}
		got, err := ReadFrom(reader)
		if err != nil {
			t.Fatalf("ReadFrom(%s): %v", tc.name, err)
		}
		assertSameListSet(t, got, tc.ids)
		got.Release()
	}
}

func TestListReadFromRejectsInvalidPayloads(t *testing.T) {
	validLarge := func() []byte {
		list := postingFromIDs(func() []uint64 {
			out := make([]uint64, 0, MidCap+4)
			for i := uint64(0); i < MidCap+2; i++ {
				out = append(out, i*5+1)
			}
			return out
		}()...)
		defer list.Release()
		return mustWriteListPayload(t, list)
	}()

	validLargeTruncated := validLarge[:len(validLarge)-1]
	largeSize, n := binary.Uvarint(validLarge[1:])
	if n <= 0 {
		t.Fatalf("failed to decode large size")
	}

	largeSizeMismatch := append([]byte{encodingLarge}, encodeUvarint(largeSize-1)...)
	largeSizeMismatch = append(largeSizeMismatch, validLarge[1+n:]...)

	corruptedLarge := slices.Clone(validLarge)
	kindOffset := 1 + n + 8 + 4 + 4 + 2
	corruptedLarge[kindOffset] = 99

	nonMonotonicSmall := []byte{encodingSmall}
	nonMonotonicSmall = appendTestUvarint(nonMonotonicSmall, 2)
	nonMonotonicSmall = appendTestUvarint(nonMonotonicSmall, 7)
	nonMonotonicSmall = appendTestUvarint(nonMonotonicSmall, 0)

	nonMonotonicMid := []byte{encodingMid}
	nonMonotonicMid = appendTestUvarint(nonMonotonicMid, SmallCap+1)
	nonMonotonicMid = appendTestUvarint(nonMonotonicMid, 1)
	for i := 0; i < SmallCap-1; i++ {
		nonMonotonicMid = appendTestUvarint(nonMonotonicMid, 1)
	}
	nonMonotonicMid = appendTestUvarint(nonMonotonicMid, 0)

	tests := []struct {
		name       string
		payload    []byte
		wantErrSub string
	}{
		{name: "InvalidTag", payload: []byte{99}, wantErrSub: "invalid posting encoding tag"},
		{name: "InvalidSmallLenZero", payload: append([]byte{encodingSmall}, encodeUvarint(0)...), wantErrSub: "invalid small posting len"},
		{name: "InvalidSmallLenTooLarge", payload: append([]byte{encodingSmall}, encodeUvarint(SmallCap+1)...), wantErrSub: "invalid small posting len"},
		{name: "InvalidMidLenSmall", payload: append([]byte{encodingMid}, encodeUvarint(SmallCap)...), wantErrSub: "invalid mid posting len"},
		{name: "InvalidMidLenTooLarge", payload: append([]byte{encodingMid}, encodeUvarint(MidCap+1)...), wantErrSub: "invalid mid posting len"},
		{name: "NonMonotonicSmall", payload: nonMonotonicSmall, wantErrSub: "strictly increasing"},
		{name: "NonMonotonicMid", payload: nonMonotonicMid, wantErrSub: "strictly increasing"},
		{name: "TruncatedSingletonVarint", payload: []byte{encodingSingleton, 0x80}, wantErrSub: "EOF"},
		{name: "TruncatedSmallPayload", payload: append(append(append([]byte{encodingSmall}, encodeUvarint(2)...), encodeUvarint(7)...), 0x80), wantErrSub: "EOF"},
		{name: "LargeSizeMismatch", payload: largeSizeMismatch, wantErrSub: "size mismatch"},
		{name: "CorruptedLargePayload", payload: corruptedLarge, wantErrSub: "invalid bitmap32 container kind"},
		{name: "TruncatedLargePayload", payload: validLargeTruncated, wantErrSub: "EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			receiver := postingFromIDs(3, 9, 17, 40)
			defer receiver.Release()
			next, err := ReadFrom(bufio.NewReader(bytes.NewReader(tc.payload)))
			if err == nil {
				next.Release()
				t.Fatalf("expected error")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(tc.wantErrSub)) {
				t.Fatalf("unexpected error: %v, want substring %q", err, tc.wantErrSub)
			}
			assertSameListSet(t, receiver, []uint64{3, 9, 17, 40})
		})
	}
}

func TestListConcurrentReadOnlyAndDetachedMutation(t *testing.T) {
	baseIDs := make([]uint64, 0, MidCap+8)
	for i := uint64(0); i < MidCap+4; i++ {
		baseIDs = append(baseIDs, i*3+1)
	}
	baseIDs = append(baseIDs, 1<<32|5, 1<<32|11, 2<<32|17, 2<<32|19)

	base := postingFromIDs(baseIDs...)
	defer base.Release()

	borrowed := base.Borrow()
	defer borrowed.Release()

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

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				if got := borrowed.Cardinality(); got != uint64(len(baseIDs)) {
					setFailed(fmt.Sprintf("Cardinality mismatch: got=%d want=%d", got, len(baseIDs)))
					return
				}
				minimum, ok := borrowed.Minimum()
				if !ok || minimum != baseIDs[0] {
					setFailed(fmt.Sprintf("Minimum mismatch: got=%d ok=%v want=%d", minimum, ok, baseIDs[0]))
					return
				}
				maximum, ok := borrowed.Maximum()
				if !ok || maximum != baseIDs[len(baseIDs)-1] {
					setFailed(fmt.Sprintf("Maximum mismatch: got=%d ok=%v want=%d", maximum, ok, baseIDs[len(baseIDs)-1]))
					return
				}
				if !borrowed.Contains(baseIDs[j%len(baseIDs)]) {
					setFailed("Contains lost existing id")
					return
				}
				iter := borrowed.Iter()
				count := 0
				for iter.HasNext() {
					iter.Next()
					count++
				}
				iter.Release()
				if count != len(baseIDs) {
					setFailed(fmt.Sprintf("Iter count mismatch: got=%d want=%d", count, len(baseIDs)))
					return
				}
			}
		}()
	}

	writerDone := make(chan List, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		mutated := borrowed
		added := postingFromIDs(2, 8, 13, 21, 34, 1<<32|9, 3<<32|1)
		defer added.Release()
		removed := postingFromIDs(1, 7, 10, 1<<32|5)
		defer removed.Release()
		mask := postingFromIDs(2, 4, 8, 13, 16, 21, 34, 1<<32|9, 2<<32|17, 3<<32|1)
		defer mask.Release()

		mutated = mutated.BuildOr(added)
		mutated = mutated.BuildAndNot(removed)
		mutated = mutated.BuildAnd(mask)
		mutated = mutated.BuildAdded(5<<32 | 7)
		mutated = mutated.BuildOptimized()
		writerDone <- mutated
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	mutated := <-writerDone
	defer mutated.Release()

	assertSameListSet(t, base, baseIDs)
	if mutated.SharesPayload(base) {
		t.Fatalf("detached mutation still shares payload with base")
	}
	if base.Contains(5<<32 | 7) {
		t.Fatalf("base changed after detached mutation")
	}
	if !mutated.Contains(5<<32 | 7) {
		t.Fatalf("mutated copy lost added id")
	}
}

func TestSkipRejectsInvalidAndTruncatedInputs(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "InvalidTag", payload: []byte{255}},
		{name: "TruncatedSingleton", payload: []byte{encodingSingleton, 0x80}},
		{name: "TruncatedLarge", payload: []byte{encodingLarge, 4, 1, 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Skip(bufio.NewReader(bytes.NewReader(tc.payload)))
			if err == nil {
				t.Fatalf("expected error")
			}
		})
	}

	err := Skip(bufio.NewReader(bytes.NewReader([]byte{encodingEmpty})))
	if err != nil {
		t.Fatalf("Skip(empty): %v", err)
	}

	err = Skip(bufio.NewReader(bytes.NewReader(append([]byte{encodingSingleton}, encodeUvarint(17)...))))
	if err != nil {
		t.Fatalf("Skip(singleton): %v", err)
	}
}

func TestWriteToReadFromMultiplePayloadsAlternatingSkip(t *testing.T) {
	values := []List{
		postingFromIDs(1),
		postingFromIDs(3, 7, 11, 19),
		postingFromIDs(1, 2, 3, 4, 5, 6, 7, 8, 9),
		postingFromIDs(func() []uint64 {
			out := make([]uint64, 0, MidCap+2)
			for i := uint64(0); i < MidCap+1; i++ {
				out = append(out, i*2)
			}
			return out
		}()...),
	}
	defer func() {
		for i := range values {
			values[i].Release()
		}
	}()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	for i := range values {
		if err := values[i].WriteTo(writer); err != nil {
			t.Fatalf("WriteTo #%d: %v", i, err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	for i := range values {
		if i%2 == 0 {
			if err := Skip(reader); err != nil {
				t.Fatalf("Skip #%d: %v", i, err)
			}
			continue
		}
		got, err := ReadFrom(reader)
		if err != nil {
			t.Fatalf("ReadFrom #%d: %v", i, err)
		}
		assertSameListSet(t, got, values[i].ToArray())
		got.Release()
	}

	if _, err := reader.Peek(1); err != io.EOF {
		t.Fatalf("expected EOF after consuming stream, got %v", err)
	}
}

/**/

type priorityPostingSlot struct {
	ids List
}

func (s *priorityPostingSlot) postingAt() List {
	if s == nil {
		return List{}
	}
	return s.ids.Borrow()
}

type priorityMaterializedPredCacheEntry struct {
	ids List
}

type priorityMaterializedPredCache struct {
	entries sync.Map
}

func (c *priorityMaterializedPredCache) load(key string) (List, bool) {
	if c == nil || key == "" {
		return List{}, false
	}
	v, ok := c.entries.Load(key)
	if !ok {
		return List{}, false
	}
	entry, _ := v.(*priorityMaterializedPredCacheEntry)
	if entry == nil {
		return List{}, true
	}
	return entry.ids.Borrow(), true
}

func (c *priorityMaterializedPredCache) store(key string, ids List) {
	if c == nil || key == "" {
		return
	}
	stored := ids
	if stored.IsBorrowed() {
		stored = stored.Clone()
	}
	c.entries.Store(key, &priorityMaterializedPredCacheEntry{ids: stored})
}

func (c *priorityMaterializedPredCache) clear() {
	if c == nil {
		return
	}
	c.entries.Range(func(key, value any) bool {
		entry, _ := value.(*priorityMaterializedPredCacheEntry)
		if entry != nil {
			entry.ids.Release()
		}
		c.entries.Delete(key)
		return true
	})
}

type priorityBatchPostingDelta struct {
	add    List
	remove List
}

func priorityApplyBatchPostingDeltaOwned(base List, delta *priorityBatchPostingDelta) List {
	if delta == nil {
		return base
	}
	deltaValue := *delta
	out := base
	changed := false
	releaseRemove := !deltaValue.remove.IsEmpty()
	releaseAdd := !deltaValue.add.IsEmpty()

	if !deltaValue.remove.IsEmpty() && !out.IsEmpty() {
		out = out.Clone()
		out = out.BuildAndNot(deltaValue.remove)
		changed = true
	}

	if !deltaValue.add.IsEmpty() {
		if out.IsEmpty() {
			out = deltaValue.add
			releaseAdd = false
			changed = true
		} else {
			if !changed {
				out = out.Clone()
				changed = true
			}
			out = out.BuildOr(deltaValue.add)
		}
	}

	if changed {
		out = out.BuildOptimized()
	}
	if releaseRemove {
		deltaValue.remove.Release()
	}
	if releaseAdd {
		deltaValue.add.Release()
	}
	delta.remove = List{}
	delta.add = List{}
	return out
}

func priorityLinearPostingUnion(posts []List) List {
	bestIdx := 0
	maxCard := posts[0].Cardinality()
	for i := 1; i < len(posts); i++ {
		card := posts[i].Cardinality()
		if card > maxCard {
			maxCard = card
			bestIdx = i
		}
	}

	out := posts[bestIdx].Clone()
	for i := range posts {
		if i == bestIdx {
			continue
		}
		out = out.BuildOr(posts[i])
	}
	return out.BuildOptimized()
}

func priorityParallelBatchedPostingUnion(posts []List) List {
	n := len(posts)

	workers := 8
	if n < workers*2 {
		workers = n / 2
	}
	if workers < 2 {
		return priorityLinearPostingUnion(posts)
	}

	chunkSize := (n + workers - 1) / workers
	results := make([]List, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		start := i * chunkSize
		if start >= n {
			break
		}
		end := start + chunkSize
		if end > n {
			end = n
		}

		wg.Add(1)
		go func(idx int, part []List) {
			defer wg.Done()
			results[idx] = priorityLinearPostingUnion(part)
		}(i, posts[start:end])
	}

	wg.Wait()

	final := results[0]
	for i := 1; i < len(results); i++ {
		final = final.BuildMergedOwned(results[i])
	}
	return final.BuildOptimized()
}

func prioritySetFailed(failed *atomic.Pointer[string], msg string) {
	if failed.Load() != nil {
		return
	}
	copyMsg := msg
	failed.CompareAndSwap(nil, &copyMsg)
}

func priorityLargeBaseIDs() []uint64 {
	out := make([]uint64, 0, MidCap+16)
	for i := uint64(0); i < MidCap+8; i++ {
		out = append(out, i*3+1)
	}
	out = append(out, 1<<32|5, 1<<32|9, 2<<32|11, 2<<32|13)
	return out
}

func priorityUnionAll(posts []List) []uint64 {
	seen := make(map[uint64]struct{}, 1024)
	out := make([]uint64, 0, 1024)
	for i := range posts {
		for _, id := range posts[i].ToArray() {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

func TestFieldPostingBorrowPatternDetachedUnderConcurrency(t *testing.T) {
	slot := &priorityPostingSlot{ids: postingFromIDs(priorityLargeBaseIDs()...)}
	defer slot.ids.Release()

	want := slot.ids.ToArray()
	remove := postingFromIDs(want[0], want[1], want[2])
	add := postingFromIDs(3<<32|5, 3<<32|7, 4<<32|9)
	defer remove.Release()
	defer add.Release()

	var failed atomic.Pointer[string]
	start := make(chan struct{})
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				ids := slot.postingAt()
				if ids.Cardinality() != uint64(len(want)) {
					prioritySetFailed(&failed, fmt.Sprintf("cardinality mismatch: got=%d want=%d", ids.Cardinality(), len(want)))
					ids.Release()
					return
				}
				if _, ok := ids.TrySingle(); ok {
					prioritySetFailed(&failed, "TrySingle unexpectedly succeeded for shared large posting")
					ids.Release()
					return
				}
				var got []uint64
				if ok := ids.ForEach(func(id uint64) bool {
					got = append(got, id)
					return true
				}); !ok {
					prioritySetFailed(&failed, "ForEach unexpectedly stopped")
					ids.Release()
					return
				}
				if !slices.Equal(got, want) {
					prioritySetFailed(&failed, fmt.Sprintf("ForEach mismatch: got=%v want=%v", got, want))
					ids.Release()
					return
				}

				iter := ids.Iter()
				count := 0
				for iter.HasNext() {
					iter.Next()
					count++
				}
				iter.Release()
				if count != len(want) {
					prioritySetFailed(&failed, fmt.Sprintf("Iter count mismatch: got=%d want=%d", count, len(want)))
					ids.Release()
					return
				}
				ids.Release()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 300; i++ {
			ids := slot.postingAt()
			ids = ids.BuildAndNot(remove)
			ids = ids.BuildOr(add)
			ids = ids.BuildAdded(6<<32 | uint64(i))
			ids = ids.BuildOptimized()
			if slot.ids.Contains(6<<32 | uint64(i)) {
				prioritySetFailed(&failed, "mutated borrowed slot view changed source")
				ids.Release()
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

	assertSameListSet(t, slot.ids, want)
}

func TestMaterializedPredCacheBorrowPatternDetachedUnderConcurrency(t *testing.T) {
	cache := &priorityMaterializedPredCache{}
	defer cache.clear()

	base := postingFromIDs(priorityLargeBaseIDs()...)
	want := base.ToArray()
	cache.store("email\x1f1\xfal", base.Borrow())
	base.Release()

	remove := postingFromIDs(want[0], want[1], want[2], want[3])
	add := postingFromIDs(5<<32|3, 5<<32|7)
	defer remove.Release()
	defer add.Release()

	var failed atomic.Pointer[string]
	start := make(chan struct{})
	var wg sync.WaitGroup

	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				cached, ok := cache.load("email\x1f1\xfal")
				if !ok {
					prioritySetFailed(&failed, "cache entry unexpectedly missing")
					return
				}
				if !slices.Equal(cached.ToArray(), want) {
					prioritySetFailed(&failed, fmt.Sprintf("cached view mismatch: got=%v want=%v", cached.ToArray(), want))
					cached.Release()
					return
				}
				cached.Release()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 300; i++ {
			cached, ok := cache.load("email\x1f1\xfal")
			if !ok {
				prioritySetFailed(&failed, "writer unexpectedly missed cache entry")
				return
			}
			cached = cached.BuildAndNot(remove)
			cached = cached.BuildOr(add)
			cached = cached.BuildAdded(7<<32 | uint64(i))
			cached = cached.BuildOptimized()
			cached.Release()
		}
	}()

	close(start)
	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	cached, ok := cache.load("email\x1f1\xfal")
	if !ok {
		t.Fatalf("cache entry missing after concurrent mutations")
	}
	defer cached.Release()
	assertSameListSet(t, cached, want)
}

func TestParallelBatchedPostingUnionBorrowedInputsStayStable(t *testing.T) {
	sources := make([]List, 0, 320)
	sourceWant := make([][]uint64, 0, 320)
	for i := 0; i < 320; i++ {
		var ids List
		base := uint64(i * 16)
		ids = ids.BuildAdded(base + 1)
		ids = ids.BuildAdded(base + 3)
		ids = ids.BuildAdded(base + 5)
		if i%3 == 0 {
			ids = ids.BuildAdded(1 << 32)
		}
		if i%7 == 0 {
			ids = ids.BuildAdded(2<<32 | uint64(i))
		}
		sourceWant = append(sourceWant, ids.ToArray())
		sources = append(sources, ids)
	}
	defer ReleaseSliceOwned(sources)

	posts := make([]List, len(sources))
	for i := range sources {
		posts[i] = sources[i].Borrow()
	}
	defer ReleaseSliceOwned(posts)

	want := priorityUnionAll(func() []List {
		out := make([]List, len(sources))
		copy(out, sources)
		return out
	}())

	var failed atomic.Pointer[string]
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			out := priorityParallelBatchedPostingUnion(posts)
			if !slices.Equal(out.ToArray(), want) {
				prioritySetFailed(&failed, fmt.Sprintf("union mismatch: got=%v want=%v", out.ToArray(), want))
				out.Release()
				return
			}
			out = out.BuildAdded(9<<32 | id)
			out.Release()
		}(uint64(g))
	}

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	for i := range sources {
		assertSameListSet(t, sources[i], sourceWant[i])
	}
}

func TestApplyBatchPostingDeltaOwnedBorrowedBaseStaysStable(t *testing.T) {
	base := postingFromIDs()
	for i := uint64(1); i <= 48; i++ {
		base = base.BuildAdded(i * 2)
	}
	defer base.Release()

	wantBase := base.ToArray()
	removeIDs := []uint64{wantBase[0], wantBase[1], wantBase[2], wantBase[3]}
	addIDs := []uint64{1<<32 | 5, 1<<32 | 9, 2<<32 | 11}
	expected := unionUint64(differenceUint64(wantBase, removeIDs), addIDs)

	var failed atomic.Pointer[string]
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				delta := priorityBatchPostingDelta{
					remove: postingFromIDs(removeIDs...),
					add:    postingFromIDs(addIDs...),
				}
				out := priorityApplyBatchPostingDeltaOwned(base.Borrow(), &delta)
				if !slices.Equal(out.ToArray(), expected) {
					prioritySetFailed(&failed, fmt.Sprintf("delta result mismatch: got=%v want=%v", out.ToArray(), expected))
					out.Release()
					return
				}
				if !delta.add.IsEmpty() || !delta.remove.IsEmpty() {
					prioritySetFailed(&failed, "applyBatchPostingDeltaOwned must consume delta buffers")
					out.Release()
					return
				}
				out.Release()
			}
		}()
	}

	wg.Wait()
	if msg := failed.Load(); msg != nil {
		t.Fatal(*msg)
	}

	assertSameListSet(t, base, wantBase)
}

func TestListAddManySortedLargeFastPaths(t *testing.T) {
	batch := make([]uint64, 0, MidCap+8)
	for i := uint64(0); i < MidCap+4; i++ {
		batch = append(batch, i*2+1)
	}
	batch = append(batch, 1<<32|5, 1<<32|7, 2<<32|9, 2<<32|11)

	cases := []struct {
		name string
		base []uint64
	}{
		{name: "empty"},
		{name: "singleton", base: []uint64{42}},
		{name: "small", base: []uint64{2, 4, 6, 8}},
		{name: "mid", base: []uint64{12, 14, 16, 18, 20, 22, 24, 26, 28}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := postingFromIDs(tc.base...)
			defer list.Release()

			list = list.BuildAddedMany(batch)

			want := unionUint64(tc.base, batch)
			assertSameListSet(t, list, want)
			assertListRepresentation(t, list, "large")
		})
	}
}

func TestListAddManyUnsortedLargeFastPaths(t *testing.T) {
	batch := make([]uint64, 0, MidCap+8)
	for i := uint64(0); i < MidCap+4; i++ {
		batch = append(batch, i*2+1)
	}
	batch = append(batch, 1<<32|5, 1<<32|7, 2<<32|9, 2<<32|11)
	slices.Reverse(batch)

	cases := []struct {
		name string
		base []uint64
	}{
		{name: "empty"},
		{name: "singleton", base: []uint64{42}},
		{name: "small", base: []uint64{2, 4, 6, 8}},
		{name: "mid", base: []uint64{12, 14, 16, 18, 20, 22, 24, 26, 28}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list := postingFromIDs(tc.base...)
			defer list.Release()

			list = list.BuildAddedMany(batch)

			want := unionUint64(tc.base, batch)
			assertSameListSet(t, list, want)
			assertListRepresentation(t, list, "large")
		})
	}
}

func TestListOptimizeBorrowedLargeCollapsesToCompactRepresentations(t *testing.T) {
	base := postingFromIDs(priorityLargeBaseIDs()...)
	for i := uint64(0); i < MidCap+16; i++ {
		base = base.BuildAdded(8<<32 | i)
	}
	defer base.Release()
	baseWant := base.ToArray()

	midKeep := postingFromIDs(base.ToArray()[:MidCap]...)
	defer midKeep.Release()
	midBorrowed := base.Borrow()
	midBorrowed = midBorrowed.BuildAnd(midKeep)
	midBorrowed = midBorrowed.BuildOptimized()
	defer midBorrowed.Release()

	assertListRepresentation(t, midBorrowed, "mid")
	if midBorrowed.SharesPayload(base) {
		t.Fatalf("Optimize must detach borrowed large posting before collapsing to mid")
	}
	assertSameListSet(t, base, baseWant)

	smallKeep := postingFromIDs(base.ToArray()[:SmallCap]...)
	defer smallKeep.Release()
	smallBorrowed := base.Borrow()
	smallBorrowed = smallBorrowed.BuildAnd(smallKeep)
	smallBorrowed = smallBorrowed.BuildOptimized()
	defer smallBorrowed.Release()

	assertListRepresentation(t, smallBorrowed, "small")
	if smallBorrowed.SharesPayload(base) {
		t.Fatalf("Optimize must detach borrowed large posting before collapsing to small")
	}
}

func TestSkipLargeRejectsInvalidPayloads(t *testing.T) {
	lp := largePostingOf(1, 3, 5, 7, 1<<32|9)
	defer lp.Release()

	var payload bytes.Buffer
	writer := bufio.NewWriter(&payload)
	if err := writeLarge(writer, lp); err != nil {
		t.Fatalf("writeLarge: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	valid := payload.Bytes()
	size, n := binary.Uvarint(valid)
	if n <= 0 {
		t.Fatalf("failed to parse large payload size")
	}

	tests := []struct {
		name    string
		payload []byte
		wantSub string
	}{
		{name: "SizeMismatch", payload: append(encodeUvarint(size+1), valid[n:]...), wantSub: "EOF"},
		{name: "Oversized", payload: encodeUvarint(^uint64(0)), wantSub: "overflows int64"},
		{name: "Truncated", payload: valid[:len(valid)-1], wantSub: "EOF"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := skipLarge(bufio.NewReader(bytes.NewReader(tc.payload)))
			if err == nil || !bytes.Contains([]byte(err.Error()), []byte(tc.wantSub)) {
				t.Fatalf("unexpected error: %v want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestByteSliceAdaptersShareBackingAndHandleEmpty(t *testing.T) {
	if got := uint64SliceAsByteSlice(nil); got != nil {
		t.Fatalf("uint64SliceAsByteSlice(nil) must return nil")
	}
	if got := uint16SliceAsByteSlice(nil); got != nil {
		t.Fatalf("uint16SliceAsByteSlice(nil) must return nil")
	}
	if got := interval16SliceAsByteSlice(nil); got != nil {
		t.Fatalf("interval16SliceAsByteSlice(nil) must return nil")
	}

	u64 := make([]uint64, 2)
	u64Bytes := uint64SliceAsByteSlice(u64)
	if len(u64Bytes) != 16 {
		t.Fatalf("uint64 adapter size mismatch: got=%d want=16", len(u64Bytes))
	}
	u64Bytes[0] = 1
	if u64[0] == 0 {
		t.Fatalf("uint64 adapter must share backing storage")
	}

	u16 := make([]uint16, 3)
	u16Bytes := uint16SliceAsByteSlice(u16)
	if len(u16Bytes) != 6 {
		t.Fatalf("uint16 adapter size mismatch: got=%d want=6", len(u16Bytes))
	}
	u16Bytes[0] = 1
	if u16[0] == 0 {
		t.Fatalf("uint16 adapter must share backing storage")
	}

	iv := make([]interval16, 2)
	ivBytes := interval16SliceAsByteSlice(iv)
	if len(ivBytes) != 8 {
		t.Fatalf("interval16 adapter size mismatch: got=%d want=8", len(ivBytes))
	}
	ivBytes[0] = 1
	if iv[0] == (interval16{}) {
		t.Fatalf("interval16 adapter must share backing storage")
	}
}
