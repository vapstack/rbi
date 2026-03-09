package rbi

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func TestPatchStrict_StructTags_NumConversion(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{
		Name:     "alice",
		Age:      10,
		Tags:     []string{"go"},
		FullName: "Alice A.",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "age", Value: 42.0}}); err != nil {
		t.Fatalf("PatchStrict age float->int: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age not patched: got %d", v.Age)
	}

	if err = db.PatchStrict(1, []Field{{Name: "fullName", Value: "Alice Alpha"}}); err != nil {
		t.Fatalf("PatchStrict json tag: %v", err)
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.FullName != "Alice Alpha" {
		t.Fatalf("full name not patched: got %q", v.FullName)
	}

	err = db.PatchStrict(1, []Field{{Name: "age", Value: 1.25}})
	if err == nil {
		t.Fatalf("expected error on float->int with fraction")
	}
	v, err = db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Age != 42 {
		t.Fatalf("age changed despite failed patch: got %d", v.Age)
	}
}

func TestPatchStrict_NilRules(t *testing.T) {
	db, _ := openTempDBUint64(t)

	s := "opt"
	if err := db.Set(1, &Rec{
		Name: "alice", Age: 10, Opt: &s,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := db.PatchStrict(1, []Field{{Name: "opt", Value: nil}}); err != nil {
		t.Fatalf("PatchStrict opt=nil: %v", err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if v.Opt != nil {
		t.Fatalf("expected opt=nil after patch")
	}

	err = db.PatchStrict(1, []Field{{Name: "age", Value: nil}})
	if err == nil {
		t.Fatalf("expected error for age=nil")
	}
}

func TestMakePatch_PreCommit_DeepCopy_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	rec := &Rec{
		Name: "alice",
		Age:  10,
		Tags: []string{"a"},
	}
	if err := db.Set(1, rec); err != nil {
		t.Fatalf("Set: %v", err)
	}

	patch := make([]Field, 0, 8)
	// makePatch := db.CollectPatch(&patch)

	origTags := []string{"x", "y"} // will be mutated later to validate deep copy
	updated := &Rec{
		Name: "bob", // changed
		Age:  10,    // unchanged
		Tags: origTags,
	}

	if err := db.Set(1, updated, func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
		patch = db.MakePatch(oldValue, newValue)
		return nil
	}); err != nil {
		t.Fatalf("Set(update): %v", err)
	}

	got := make(map[string]any, len(patch))
	for _, f := range patch {
		got[f.Name] = f.Value
	}

	// expect changed fields: name and tags

	if _, ok := got["Name"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "Name", patch)
	}
	if _, ok := got["Tags"]; !ok {
		t.Fatalf("expected patch to include %q, got %#v", "Tags", patch)
	}
	if _, ok := got["Age"]; ok {
		t.Fatalf("did not expect patch to include unchanged field %q, got %#v", "Age", patch)
	}

	if v, _ := got["Name"].(string); v != "bob" {
		t.Fatalf("expected patched name %q, got %#v", "bob", got["Name"])
	}

	gotTags, _ := got["Tags"].([]string)
	if len(gotTags) != 2 || gotTags[0] != "x" || gotTags[1] != "y" {
		t.Fatalf("expected patched tags [x y], got %#v", gotTags)
	}

	origTags[0] = "MUTATED"

	gotTags2, _ := got["Tags"].([]string)
	if len(gotTags2) != 2 || gotTags2[0] != "x" || gotTags2[1] != "y" {
		t.Fatalf("expected deep-copied tags [x y], got %#v", gotTags2)
	}
}

func TestCollectBatchPatch_PreCommit_CollectsAndDeepCopies_SliceValues(t *testing.T) {
	db, _ := openTempDBUint64(t)

	ids := []uint64{1, 2}
	base := []*Rec{
		{Name: "alice", Age: 10, Tags: []string{"a"}},
		{Name: "carol", Age: 20, Tags: []string{"c"}},
	}
	if err := db.BatchSet(ids, base); err != nil {
		t.Fatalf("BatchSet(base): %v", err)
	}

	patchByID := make(map[uint64][]Field)
	// makePatchMany := db.CollectPatchMany(patchByID)

	origTags1 := []string{"x", "y"} // will mutate later
	origTags2 := []string{"p", "q"} // will mutate later

	updated := []*Rec{
		{Name: "bob", Age: 10, Tags: origTags1},   // name+tags changed, age unchanged
		{Name: "carol", Age: 21, Tags: origTags2}, // age+tags changed, name unchanged
	}

	if err := db.BatchSet(ids, updated, func(_ *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		patchByID[key] = db.MakePatch(oldValue, newValue)
		return nil
	}); err != nil {
		t.Fatalf("BatchSet(update): %v", err)
	}

	// to map []Field -> map[name]value for assertions.
	toMap := func(fs []Field) map[string]any {
		m := make(map[string]any, len(fs))
		for _, f := range fs {
			m[f.Name] = f.Value
		}
		return m
	}

	p1, ok := patchByID[1]
	if !ok {
		t.Fatalf("expected patch for id=1, got keys: %#v", keysOfMap(patchByID))
	}
	m1 := toMap(p1)

	if _, ok = m1["Name"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "Name", p1)
	}
	if _, ok = m1["Tags"]; !ok {
		t.Fatalf("id=1: expected patch to include %q, got %#v", "Tags", p1)
	}
	if _, ok = m1["Age"]; ok {
		t.Fatalf("id=1: did not expect patch to include unchanged field %q, got %#v", "Age", p1)
	}

	if v, _ := m1["Name"].(string); v != "bob" {
		t.Fatalf("id=1: expected patched name %q, got %#v", "bob", m1["Name"])
	}
	tags1, _ := m1["Tags"].([]string)
	if len(tags1) != 2 || tags1[0] != "x" || tags1[1] != "y" {
		t.Fatalf("id=1: expected patched tags [x y], got %#v", tags1)
	}

	p2, ok := patchByID[2]
	if !ok {
		t.Fatalf("expected patch for id=2, got keys: %#v", keysOfMap(patchByID))
	}
	m2 := toMap(p2)

	if _, ok = m2["Age"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "Age", p2)
	}
	if _, ok = m2["Tags"]; !ok {
		t.Fatalf("id=2: expected patch to include %q, got %#v", "Tags", p2)
	}
	if _, ok = m2["Name"]; ok {
		t.Fatalf("id=2: did not expect patch to include unchanged field %q, got %#v", "Name", p2)
	}

	if v, _ := m2["Age"].(int); v != 21 {
		t.Fatalf("id=2: expected patched age %d, got %#v", 21, m2["Age"])
	}
	tags2, _ := m2["Tags"].([]string)
	if len(tags2) != 2 || tags2[0] != "p" || tags2[1] != "q" {
		t.Fatalf("id=2: expected patched tags [p q], got %#v", tags2)
	}

	origTags1[0] = "MUTATED1"
	origTags2[0] = "MUTATED2"

	tags1b, _ := m1["Tags"].([]string)
	if len(tags1b) != 2 || tags1b[0] != "x" || tags1b[1] != "y" {
		t.Fatalf("id=1: expected deep-copied tags [x y], got %#v", tags1b)
	}
	tags2b, _ := m2["Tags"].([]string)
	if len(tags2b) != 2 || tags2b[0] != "p" || tags2b[1] != "q" {
		t.Fatalf("id=2: expected deep-copied tags [p q], got %#v", tags2b)
	}
}

/**/

func TestTruncate(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"t1"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"t2"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	if cnt, err := db.Count(nil); err != nil || cnt != 2 {
		t.Fatalf("Count(before): cnt=%d err=%v", cnt, err)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("QueryKeys(before): ids=%v err=%v", ids, err)
	}

	// concurrent readers while truncate happens should not panic
	done := make(chan struct{})
	errCh := make(chan error, 16)

	var wg sync.WaitGroup
	reader := func(name string) {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic in %s: %v", name, r)
			}
		}()
		for {
			select {
			case <-done:
				return
			default:
			}
			_, _ = db.Get(1)
			_, _ = db.BatchGet(1, 2, 999)

			_ = db.SeqScan(0, func(_ uint64, _ *UniqueTestRec) (bool, error) { return true, nil })
			_ = db.SeqScanRaw(0, func(_ uint64, _ []byte) (bool, error) { return true, nil })

			_, _ = db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}})
			_, _ = db.Count(nil)
		}
	}

	wg.Add(2)
	go reader("r1")
	go reader("r2")

	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #1: %v", err)
	}
	if err := db.Truncate(); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("Truncate #2: %v", err)
	}

	close(done)
	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Errorf("%v", e)
	}
	if t.Failed() {
		t.FailNow()
	}

	if v, err := db.Get(1); err != nil {
		t.Fatalf("Get(after): %v", err)
	} else if v != nil {
		t.Fatalf("expected Get(1)==nil after truncate, got %#v", v)
	}
	if vals, err := db.BatchGet(1, 2); err != nil {
		t.Fatalf("BatchGet(after): %v", err)
	} else if len(vals) != 2 || vals[0] != nil || vals[1] != nil {
		t.Fatalf("expected BatchGet to return [nil nil], got %#v", vals)
	}

	if cnt, err := db.Count(nil); err != nil {
		t.Fatalf("Count(after): %v", err)
	} else if cnt != 0 {
		t.Fatalf("expected 0 records after truncate, got %d", cnt)
	}
	if ids, err := db.QueryKeys(&qx.QX{Expr: qx.Expr{Op: qx.OpNOOP}}); err != nil {
		t.Fatalf("QueryKeys(NOOP after): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("expected no keys after truncate, got %v", ids)
	}

	st := db.Stats()
	if st.Index.KeyCount != 0 {
		t.Fatalf("expected Stats.Index.KeyCount=0 after truncate, got %d", st.Index.KeyCount)
	}
	var zero uint64
	if st.Index.LastKey != zero {
		t.Fatalf("expected Stats.Index.LastKey=%v after truncate, got %v", zero, st.Index.LastKey)
	}

	if err := db.Set(10, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("expected unique values reusable after truncate, got: %v", err)
	}
	if err := db.Set(11, &UniqueTestRec{Email: "a@x", Code: 2}); err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected ErrUniqueViolation after truncate+reuse, got: %v", err)
	}

	if ids, err := db.QueryKeys(qx.Query(qx.EQ("email", "a@x"))); err != nil {
		t.Fatalf("QueryKeys(after reinsert): %v", err)
	} else if len(ids) != 1 || ids[0] != 10 {
		t.Fatalf("expected [10] after reinsert, got %v", ids)
	}
}

func TestTruncate_NoDeadlock_WithConcurrentExecuteBatchWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncate_deadlock.db")
	db, raw := openBoltAndNew[uint64, Rec](t, path)

	if err := db.Set(1, &Rec{Name: "seed", Age: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	newVal := &Rec{Name: "after", Age: 2}
	enc := getEncodeBuf()
	if err := db.encode(newVal, enc); err != nil {
		releaseEncodeBuf(enc)
		t.Fatalf("encode payload: %v", err)
	}
	payload := append([]byte(nil), enc.Bytes()...)
	releaseEncodeBuf(enc)

	req := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         1,
		setValue:   newVal,
		setPayload: payload,
		done:       make(chan error, 1),
	}

	// Force lock-order contention window:
	// 1) truncate goroutine waits on db.mu
	// 2) executeCombinedBatch goroutine acquires writer tx and then waits on db.mu
	// 3) release db.mu and assert both operations complete.
	db.mu.Lock()
	truncateDone := make(chan error, 1)
	go func() {
		truncateDone <- db.Truncate()
	}()
	go db.executeCombinedBatch([]*combineRequest[uint64, Rec]{req})
	time.Sleep(20 * time.Millisecond)
	db.mu.Unlock()

	select {
	case err := <-truncateDone:
		if err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("truncate timed out (possible deadlock)")
	}

	select {
	case err := <-req.done:
		if err != nil && !errors.Is(err, ErrRecordNotFound) {
			t.Fatalf("executeCombinedBatch req error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("executeCombinedBatch timed out (possible deadlock)")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw.Close: %v", err)
	}
}
