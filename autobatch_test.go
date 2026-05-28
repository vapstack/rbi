package rbi

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"github.com/vapstack/rbi/internal/strmap"
	"go.etcd.io/bbolt"
)

func testStrMapCount[V any](tb testing.TB, db *DB[string, V]) int {
	tb.Helper()
	return testStrMapSnapshotCount(db.strMap.Snapshot())
}

func testStrMapSnapshotCount(snap *strmap.Snapshot) int {
	lookup := snap.Lookup()
	count := 0
	for idx, next := uint64(1), snap.Next(); idx <= next; idx++ {
		if _, ok := lookup.String(idx); ok {
			count++
		}
	}
	return count
}

func TestAutoBatchMissingBucketRollsBackWriteTx(t *testing.T) {
	db, _ := openTempDBUint64(t)
	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(db.bucket)
	}); err != nil {
		t.Fatalf("delete bucket: %v", err)
	}

	err := db.Set(1, &Rec{Name: "missing"}, NoBatch[uint64, Rec])
	if err == nil || !strings.Contains(err.Error(), "bucket does not exist") {
		t.Fatalf("expected missing bucket error, got %v", err)
	}

	done := make(chan error, 1)
	go func() {
		tx, err := db.bolt.Begin(true)
		if err == nil {
			err = tx.Rollback()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("begin write tx after missing bucket: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("write tx blocked after missing bucket error")
	}
}

func TestBatch_BeforeCommit_CallbacksRunForSetPatchDelete(t *testing.T) {
	db, _ := openTempDBUint64(t)

	var (
		mu     sync.Mutex
		events []string
	)
	cb := func(_ *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		oldName := "<nil>"
		newName := "<nil>"
		if oldValue != nil {
			oldName = oldValue.Name
		}
		if newValue != nil {
			newName = newValue.Name
		}
		mu.Lock()
		events = append(events, fmt.Sprintf("%d:%s->%s", key, oldName, newName))
		mu.Unlock()
		return nil
	}

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}, BeforeCommit(cb)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "name", Value: "bob"}}, BeforeCommit(cb)); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if err := db.Delete(1, BeforeCommit(cb)); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	want := []string{
		"1:<nil>->alice",
		"1:alice->bob",
		"1:bob-><nil>",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("unexpected callback events: got=%v want=%v", events, want)
	}

	bs := db.AutoBatchStats()
	if bs.CallbackOps < 3 {
		t.Fatalf("expected at least 3 callback ops in auto-batcher stats, got %d", bs.CallbackOps)
	}
}

func TestAutoBatchMixedQueuedWritesMatchCommitOrderModel(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:   -1,
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	cloneRec := func(src *Rec) Rec {
		out := *src
		out.Tags = slices.Clone(src.Tags)
		if src.Opt != nil {
			opt := *src.Opt
			out.Opt = &opt
		}
		return out
	}
	makeRec := func(i int) Rec {
		countries := [...]string{"US", "DE", "NL", "PL"}
		names := [...]string{"alice", "bob", "carol", "dave"}
		tags := []string{"base", fmt.Sprintf("g-%d", i%5)}
		if i%3 == 0 {
			tags = append(tags, "hot")
		}
		return Rec{
			Meta:     Meta{Country: countries[i%len(countries)]},
			Name:     names[i%len(names)],
			Email:    fmt.Sprintf("mixed-%03d@example.test", i),
			Age:      20 + i%50,
			Score:    float64(i) + 0.25,
			Active:   i%2 == 0,
			Tags:     tags,
			FullName: fmt.Sprintf("Mixed %03d", i),
		}
	}
	patchFor := func(rec Rec) []Field {
		return []Field{
			{Name: "country", Value: rec.Country},
			{Name: "name", Value: rec.Name},
			{Name: "email", Value: rec.Email},
			{Name: "age", Value: rec.Age},
			{Name: "score", Value: rec.Score},
			{Name: "active", Value: rec.Active},
			{Name: "tags", Value: slices.Clone(rec.Tags)},
			{Name: "full_name", Value: rec.FullName},
			{Name: "opt", Value: nil},
		}
	}

	model := make(map[uint64]Rec, 48)
	for i := 1; i <= 24; i++ {
		rec := makeRec(i)
		id := uint64(i)
		if err := db.Set(id, &rec, NoBatch[uint64, Rec]); err != nil {
			t.Fatalf("seed Set(%d): %v", id, err)
		}
		model[id] = cloneRec(&rec)
	}

	type event struct {
		id     uint64
		delete bool
		rec    Rec
	}
	var (
		mu     sync.Mutex
		events []event
	)
	recordCommit := func(_ *bbolt.Tx, id uint64, _, newValue *Rec) error {
		mu.Lock()
		if newValue == nil {
			events = append(events, event{id: id, delete: true})
		} else {
			events = append(events, event{id: id, rec: cloneRec(newValue)})
		}
		mu.Unlock()
		return nil
	}

	type op struct {
		kind  int
		id    uint64
		rec   Rec
		patch []Field
	}
	ops := make([]op, 96)
	for i := range ops {
		id := uint64(1 + (i*7)%40)
		rec := makeRec(1000 + i)
		switch i % 6 {
		case 0, 3:
			ops[i] = op{kind: 0, id: id, rec: rec}
		case 1, 4:
			ops[i] = op{kind: 1, id: id, patch: patchFor(rec)}
		default:
			ops[i] = op{kind: 2, id: id}
		}
	}

	before := db.AutoBatchStats()
	start := make(chan struct{})
	errCh := make(chan error, len(ops))
	var wg sync.WaitGroup
	for i := range ops {
		op := ops[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var err error
			switch op.kind {
			case 0:
				rec := op.rec
				err = db.Set(op.id, &rec, BeforeCommit(recordCommit))
			case 1:
				err = db.Patch(op.id, op.patch, BeforeCommit(recordCommit))
			default:
				err = db.Delete(op.id, BeforeCommit(recordCommit))
			}
			if err != nil {
				errCh <- fmt.Errorf("kind=%d id=%d: %w", op.kind, op.id, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := range events {
		ev := events[i]
		if ev.delete {
			delete(model, ev.id)
		} else {
			model[ev.id] = ev.rec
		}
	}

	if got, err := db.Count(); err != nil {
		t.Fatalf("Count: %v", err)
	} else if got != uint64(len(model)) {
		t.Fatalf("Count = %d, want %d", got, len(model))
	}
	for id := uint64(1); id <= 40; id++ {
		got, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		want, ok := model[id]
		if !ok {
			if got != nil {
				t.Fatalf("Get(%d) = %#v, want nil", id, got)
			}
			continue
		}
		if got == nil || !reflect.DeepEqual(*got, want) {
			t.Fatalf("Get(%d) = %#v, want %#v", id, got, want)
		}
	}

	assertQuery := func(label string, q *qx.QX, match func(Rec) bool) {
		t.Helper()
		got, err := db.QueryKeys(q)
		if err != nil {
			t.Fatalf("%s QueryKeys: %v", label, err)
		}
		want := make([]uint64, 0, len(model))
		for id, rec := range model {
			if match(rec) {
				want = append(want, id)
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s QueryKeys = %v, want %v", label, got, want)
		}
	}
	assertQuery("active", qx.Query(qx.EQ("active", true)), func(rec Rec) bool {
		return rec.Active
	})
	assertQuery("country", qx.Query(qx.EQ("country", "DE")), func(rec Rec) bool {
		return rec.Country == "DE"
	})
	assertQuery("tags", qx.Query(qx.HASANY("tags", []string{"hot"})), func(rec Rec) bool {
		return slices.Contains(rec.Tags, "hot")
	})

	after := db.AutoBatchStats()
	if after.MultiRequestBatches <= before.MultiRequestBatches {
		t.Fatalf("expected queued writes to form a multi-request batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_SequentialSet_DoesNotProduceMultiRequestBatches(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	before := db.AutoBatchStats()
	for i := 1; i <= 64; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name: fmt.Sprintf("seq-%03d", i),
			Age:  18 + (i % 50),
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	after := db.AutoBatchStats()

	enqueuedDelta := after.Enqueued - before.Enqueued
	if enqueuedDelta != 64 {
		t.Fatalf("expected all sequential Set writes to be enqueued, delta=%d before=%+v after=%+v", enqueuedDelta, before, after)
	}
	if after.MultiRequestBatches != before.MultiRequestBatches {
		t.Fatalf("expected no multi-request batches for sequential Set calls, before=%+v after=%+v", before, after)
	}
	if after.MaxBatchSeen > 1 {
		t.Fatalf("expected max seen batch size to stay 1 for sequential Set calls, stats=%+v", after)
	}
	if after.BatchSize1 == 0 {
		t.Fatalf("expected single-request batch distribution bucket to be tracked, stats=%+v", after)
	}
}

func TestBatch_RepeatedPatchIDMaintainsIndexConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})

	if err := db.Set(1, &Rec{
		Meta:     Meta{Country: "NL"},
		Name:     "alice",
		Email:    "alice@example.test",
		Age:      30,
		Score:    10.5,
		Active:   false,
		Tags:     []string{"go"},
		FullName: "ID-001",
	}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	if err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{
			{Name: "age", Value: 31},
			{Name: "tags", Value: []string{"rust", "db"}},
		},
	); err != nil {
		t.Fatalf("BatchPatch repeated id: %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil")
	}
	if got.Age != 31 {
		t.Fatalf("unexpected age: got=%d want=31", got.Age)
	}
	if !slices.Equal(got.Tags, []string{"rust", "db"}) {
		t.Fatalf("unexpected tags: got=%v want=%v", got.Tags, []string{"rust", "db"})
	}

	assertContains := func(q *qx.QX, desc string) {
		t.Helper()
		ids, qerr := db.QueryKeys(q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%s): %v", desc, qerr)
		}
		if !slices.Contains(ids, uint64(1)) {
			t.Fatalf("%s missing id=1, got=%v", desc, ids)
		}
	}
	assertOmits := func(q *qx.QX, desc string) {
		t.Helper()
		ids, qerr := db.QueryKeys(q)
		if qerr != nil {
			t.Fatalf("QueryKeys(%s): %v", desc, qerr)
		}
		if slices.Contains(ids, uint64(1)) {
			t.Fatalf("stale %s still contains id=1, got=%v", desc, ids)
		}
	}

	assertContains(qx.Query(qx.EQ("age", 31)), "age=31")
	assertContains(qx.Query(qx.HASALL("tags", []string{"rust"})), `tag="rust"`)
	assertContains(qx.Query(qx.HASALL("tags", []string{"db"})), `tag="db"`)
	assertOmits(qx.Query(qx.EQ("age", 30)), "age=30")
	assertOmits(qx.Query(qx.HASALL("tags", []string{"go"})), `tag="go"`)
}

func TestBatch_MaxOne_StillUsesBatcher(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow: 5 * time.Millisecond,
		AutoBatchMax:    1,
	})

	before := db.AutoBatchStats()

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.AutoBatchStats()
	if after.Submitted != before.Submitted+1 || after.Enqueued != before.Enqueued+1 || after.Dequeued != before.Dequeued+1 {
		t.Fatalf("expected AutoBatchMax=1 write to still use queue path, before=%+v after=%+v", before, after)
	}
	if after.BatchSize1 != before.BatchSize1+1 || after.MultiRequestBatches != before.MultiRequestBatches {
		t.Fatalf("expected AutoBatchMax=1 to execute as a single-request internal batch, before=%+v after=%+v", before, after)
	}
}

func TestBatch_NoBatch_IsolatesRequestInsideBatcher(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	before := db.AutoBatchStats()

	calls := 0
	err := db.Set(1, &Rec{Name: "alice", Age: 10}, NoBatch[uint64, Rec], BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
		calls++
		return nil
	}))
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected callback to run exactly once, got %d", calls)
	}

	after := db.AutoBatchStats()
	if after.Submitted != before.Submitted+1 || after.Enqueued != before.Enqueued+1 || after.Dequeued != before.Dequeued+1 {
		t.Fatalf("expected NoBatch write to use queued internal path, before=%+v after=%+v", before, after)
	}
	if after.BatchSize1 != before.BatchSize1+1 || after.MultiRequestBatches != before.MultiRequestBatches {
		t.Fatalf("expected NoBatch request to execute as a single-request batch, before=%+v after=%+v", before, after)
	}
	if after.CallbackOps != before.CallbackOps+1 {
		t.Fatalf("expected NoBatch callback to run through internal batcher, before=%+v after=%+v", before, after)
	}
}

func TestBatch_PatchUnique_QueuedIntoBatch(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := db.AutoBatchStats()
	if err := db.Patch(1, []Field{{Name: "email", Value: "c@x"}}); err != nil {
		t.Fatalf("Patch unique field should use auto-batcher path: %v", err)
	}
	mid := db.AutoBatchStats()
	if mid.Enqueued <= before.Enqueued {
		t.Fatalf("expected patch to be enqueued into auto-batcher, before=%+v after=%+v", before, mid)
	}

	err := db.Patch(1, []Field{{Name: "email", Value: "b@x"}})
	if err == nil || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("expected unique violation for conflicting email patch, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "c@x" {
		t.Fatalf("id=1 must keep last successful value, got: %#v", v1)
	}
}

func TestBatch_DuplicatePatchSameID_DecodeFailurePropagatesToLaterRequests(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		var keyBuf [8]byte
		return b.Put(keycodec.UserKeyBytesWithBuf(uint64(1), db.strKey, &keyBuf), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	err := db.BatchPatch([]uint64{1, 1}, []Field{{Name: "age", Value: 31}})
	if err == nil || !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("BatchPatch error = %v, want decode existing value", err)
	}
}

func TestBatch_BeforeCommitError_RollsBack(t *testing.T) {
	db, _ := openTempDBUint64(t)

	wantErr := errors.New("before commit failed")
	err := db.Set(1, &Rec{Name: "alice", Age: 10}, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
		return wantErr
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected BeforeCommit error %v, got %v", wantErr, err)
	}

	got, gerr := db.Get(1)
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != nil {
		t.Fatalf("expected rollback on BeforeCommit error, got %#v", got)
	}
}

func TestBatch_StringKeyBeforeStoreError_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := testStrMapCount(t, db)

	hookErr := errors.New("before store fail")
	err := db.Set(
		"ghost-before-store",
		&Product{SKU: "ghost-before-store", Price: 11},
		BeforeStore(func(_ string, _ *Product, _ *Product) error { return hookErr }),
	)
	if !errors.Is(err, hookErr) {
		t.Fatalf("Set must fail with BeforeStore error, got: %v", err)
	}
	if got, err := db.Get("ghost-before-store"); err != nil {
		t.Fatalf("Get(ghost-before-store): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-before-store must not persist after BeforeStore failure, got %#v", got)
	}
	if after := testStrMapCount(t, db); after != initial {
		t.Fatalf("strmap grew after BeforeStore failure: initial=%d after=%d", initial, after)
	}
}

func keysOfMap[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestAutoBatchExt_BatchAtomic_DuplicatePatchSameID_BeforeStoreSeesSteppedState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	call := 0
	var seen []string
	err := db.BatchPatch(
		[]uint64{1, 1},
		[]Field{{Name: "age", Value: 99}},
		BeforeProcess(func(_ uint64, value *Rec) error {
			call++
			value.Name = fmt.Sprintf("step-%d", call)
			return nil
		}),
		BeforeStore(func(_ uint64, oldValue, newValue *Rec) error {
			seen = append(seen, fmt.Sprintf("%s->%s", oldValue.Name, newValue.Name))
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchPatch: %v", err)
	}

	want := []string{"seed->step-1", "step-1->step-2"}
	if !slices.Equal(seen, want) {
		t.Fatalf("BeforeStore sequence = %v, want %v", seen, want)
	}
	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "step-2" || got.Age != 99 {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
}

func TestAutoBatchExt_BatchAtomic_DuplicateDeleteSameID_BeforeCommitRunsOnce(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	calls := 0
	err := db.BatchDelete(
		[]uint64{1, 1},
		BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
			calls++
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}
	if calls != 1 {
		t.Fatalf("BeforeCommit calls = %d, want 1", calls)
	}
	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must be deleted, got %#v", got)
	}
}

func TestAutoBatchExt_BatchAtomic_PatchStrictDuplicateSameID_RollsBackBothSteps(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	err := db.BatchPatch([]uint64{1, 1}, []Field{{Name: "missing", Value: 1}}, PatchStrict)
	if err == nil || !strings.Contains(err.Error(), "cannot patch field") {
		t.Fatalf("BatchPatch error = %v, want strict patch error", err)
	}

	if got, gerr := db.Get(1); gerr != nil {
		t.Fatalf("Get(1): %v", gerr)
	} else if got == nil || got.Name != "seed" || got.Age != 10 {
		t.Fatalf("id=1 changed after failed BatchPatch: %#v", got)
	}
}

func TestAutoBatchExt_Race_HotSameID_AutoBatchQueryConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:   -1,
		AutoBatchWindow:   200 * time.Microsecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	for i := 1; i <= 4; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:   fmt.Sprintf("seed-%d", i),
			Age:    20 + i,
			Active: i%2 == 0,
			Tags:   []string{"base"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(4))
				switch r.IntN(3) {
				case 0:
					if err := db.Set(id, &Rec{
						Name:   []string{"alice", "bob", "carol"}[r.IntN(3)],
						Age:    18 + r.IntN(50),
						Active: r.IntN(2) == 0,
						Tags:   []string{"go", "db", "ops"}[:1+r.IntN(3)],
					}); err != nil {
						reportErr(fmt.Errorf("Set(%d): %w", id, err))
						return
					}
				case 1:
					patch := []Field{{Name: "age", Value: float64(20 + r.IntN(40))}}
					if err := db.Patch(id, patch); err != nil {
						reportErr(fmt.Errorf("Patch(%d): %w", id, err))
						return
					}
				default:
					if err := db.Delete(id); err != nil {
						reportErr(fmt.Errorf("Delete(%d): %w", id, err))
						return
					}
				}
			}
		}(int64(1000 + w))
	}

	queries := []*qx.QX{
		qx.Query(qx.GTE("age", 30)),
		qx.Query(qx.PREFIX("name", "a")),
		qx.Query(qx.HASANY("tags", []string{"go", "db"})),
		qx.Query(qx.EQ("active", true)),
	}
	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				q := queries[r.IntN(len(queries))]
				items, err := db.Query(q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					ok, evalErr := evalExprBool(item, q.Filter)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Filter))
						return
					}
				}
			}
		}(int64(2000 + rr))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}
}

func TestAutoBatchExt_Race_HotUniqueContention_NoInvariantBreak(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   200 * time.Microsecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	for i := 1; i <= 8; i++ {
		if err := db.Set(uint64(i), &UniqueTestRec{
			Email: fmt.Sprintf("seed-%d@x", i),
			Code:  i,
			Tags:  []string{"seed"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(8))
				switch r.IntN(3) {
				case 0:
					err := db.Set(id, &UniqueTestRec{
						Email: fmt.Sprintf("u%d@x", r.IntN(6)),
						Code:  1 + r.IntN(6),
						Tags:  []string{fmt.Sprintf("w%d", r.IntN(3))},
					})
					if err != nil && !errors.Is(err, ErrUniqueViolation) {
						reportErr(fmt.Errorf("Set(%d): %w", id, err))
						return
					}
				case 1:
					var patch []Field
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "email", Value: fmt.Sprintf("u%d@x", r.IntN(6))}}
					} else {
						patch = []Field{{Name: "tags", Value: []string{fmt.Sprintf("p%d", r.IntN(4))}}}
					}
					err := db.Patch(id, patch)
					if err != nil && !errors.Is(err, ErrUniqueViolation) {
						reportErr(fmt.Errorf("Patch(%d): %w", id, err))
						return
					}
				default:
					if err := db.Delete(id); err != nil && !errors.Is(err, ErrUniqueViolation) {
						reportErr(fmt.Errorf("Delete(%d): %w", id, err))
						return
					}
				}
			}
		}(int64(3000 + w))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}

	seenEmail := make(map[string]uint64)
	seenCode := make(map[int]uint64)
	for id := uint64(1); id <= 8; id++ {
		got, err := db.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		if got == nil {
			continue
		}
		if prev, exists := seenEmail[got.Email]; exists {
			t.Fatalf("duplicate email %q for ids %d and %d", got.Email, prev, id)
		}
		seenEmail[got.Email] = id
		if prev, exists := seenCode[got.Code]; exists {
			t.Fatalf("duplicate code %d for ids %d and %d", got.Code, prev, id)
		}
		seenCode[got.Code] = id
	}

	for email, id := range seenEmail {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("email", email)))
		if err != nil {
			t.Fatalf("QueryKeys(email=%s): %v", email, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("email=%s ids = %v, want [%d]", email, ids, id)
		}
	}
	for code, id := range seenCode {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("code", code)))
		if err != nil {
			t.Fatalf("QueryKeys(code=%d): %v", code, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("code=%d ids = %v, want [%d]", code, ids, id)
		}
	}
}

func TestAutoBatchExt_New_Race_HotPatchHooks_QueryConsistency(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AnalyzeInterval:   -1,
		AutoBatchWindow:   200 * time.Microsecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	for i := 1; i <= 2; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name:     fmt.Sprintf("seed-%d", i),
			Age:      20 + i,
			Tags:     []string{"seed"},
			FullName: fmt.Sprintf("seed-full-%d", i),
			Meta:     Meta{Country: "NL"},
		}); err != nil {
			t.Fatalf("seed Set(%d): %v", i, err)
		}
	}

	stop := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var wg sync.WaitGroup
	countries := []string{"NL", "DE", "PL", "ES"}

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				id := uint64(1 + r.IntN(2))
				name := fmt.Sprintf("writer-%d", r.IntN(4))
				fullName := fmt.Sprintf("full-%d", r.IntN(4))
				country := countries[r.IntN(len(countries))]

				err := db.Patch(
					id,
					[]Field{{Name: "age", Value: float64(20 + r.IntN(50))}},
					BeforeProcess(func(_ uint64, v *Rec) error {
						v.Name = name
						return nil
					}),
					BeforeStore(func(_ uint64, _ *Rec, v *Rec) error {
						v.FullName = fullName
						v.Country = country
						return nil
					}),
				)
				if err != nil {
					reportErr(fmt.Errorf("Patch(%d): %w", id, err))
					return
				}
			}
		}(int64(4100 + w))
	}

	queries := []*qx.QX{
		qx.Query(qx.PREFIX("name", "writer-")),
		qx.Query(qx.PREFIX("full_name", "full-")),
		qx.Query(qx.GTE("age", 20)),
		qx.Query(qx.EQ("country", "NL")),
		qx.Query(qx.EQ("country", "DE")),
		qx.Query(qx.EQ("country", "PL")),
		qx.Query(qx.EQ("country", "ES")),
	}

	for rr := 0; rr < 4; rr++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := newRand(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}

				q := queries[r.IntN(len(queries))]
				items, err := db.Query(q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					ok, evalErr := evalExprBool(item, q.Filter)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Filter))
						return
					}
				}
			}
		}(int64(5100 + rr))
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}
}
