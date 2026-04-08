package rbi

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/pooled"
	"go.etcd.io/bbolt"
)

func testAutoBatchRequestBuf[K ~string | ~uint64, V any](reqs ...*autoBatchRequest[K, V]) *pooled.SliceBuf[*autoBatchRequest[K, V]] {
	buf := new(pooled.SliceBuf[*autoBatchRequest[K, V]])
	buf.AppendAll(reqs)
	return buf
}

func queuedSingleJob[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) *autoBatchJob[K, V] {
	return &autoBatchJob[K, V]{
		reqs: testAutoBatchRequestBuf(req),
		done: req.done,
	}
}

func mustEncodeAutoBatchPayload[K ~string | ~uint64, V any](tb testing.TB, db *DB[K, V], v *V) *bytes.Buffer {
	tb.Helper()
	b := encodePool.Get()
	if err := db.encode(v, b); err != nil {
		encodePool.Put(b)
		tb.Fatalf("encode: %v", err)
	}
	return b
}

func rawAutoBatchPayload(tb testing.TB, payload []byte) *bytes.Buffer {
	tb.Helper()
	b := encodePool.Get()
	if _, err := b.Write(payload); err != nil {
		encodePool.Put(b)
		tb.Fatalf("write payload: %v", err)
	}
	return b
}

func replaceAutoBatchPayloadForTest[K ~string | ~uint64, V any](req *autoBatchRequest[K, V], payload *bytes.Buffer) {
	if req == nil {
		encodePool.Put(payload)
		return
	}
	if req.setPayload != nil {
		encodePool.Put(req.setPayload)
	}
	req.setPayload = payload
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

	reqAge := db.buildPatchAutoBatchRequest(1, []Field{{Name: "age", Value: 31}}, false, nil, nil, nil)
	reqTags := db.buildPatchAutoBatchRequest(1, []Field{{Name: "tags", Value: []string{"rust", "db"}}}, false, nil, nil, nil)

	_, done, fatalErr := db.executeAutoBatchAttempt(testAutoBatchRequestBuf(reqAge, reqTags), false)
	if fatalErr != nil {
		t.Fatalf("executeAutoBatchAttempt: %v", fatalErr)
	}
	if !done {
		t.Fatal("executeAutoBatchAttempt returned done=false")
	}
	if reqAge.err != nil {
		t.Fatalf("age patch err: %v", reqAge.err)
	}
	if reqTags.err != nil {
		t.Fatalf("tags patch err: %v", reqTags.err)
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
	assertContains(qx.Query(qx.HAS("tags", []string{"rust"})), `tag="rust"`)
	assertContains(qx.Query(qx.HAS("tags", []string{"db"})), `tag="db"`)
	assertOmits(qx.Query(qx.EQ("age", 30)), "age=30")
	assertOmits(qx.Query(qx.HAS("tags", []string{"go"})), `tag="go"`)
}

func TestBatch_StatsTrackBatchSizeDistribution(t *testing.T) {
	db, _ := openTempDBUint64(t)

	makeSetReq := func(id uint64, name string) *autoBatchRequest[uint64, Rec] {
		rec := &Rec{Name: name, Age: int(id)}
		return &autoBatchRequest[uint64, Rec]{
			op:         autoBatchSet,
			id:         id,
			setValue:   rec,
			setPayload: mustEncodeAutoBatchPayload(t, db, rec),
			done:       make(chan error, 1),
		}
	}

	single := makeSetReq(1, "single")
	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{single})
	if err := <-single.done; err != nil {
		t.Fatalf("single batch failed: %v", err)
	}

	reqs := []*autoBatchRequest[uint64, Rec]{
		makeSetReq(2, "a"),
		makeSetReq(3, "b"),
		makeSetReq(4, "c"),
		makeSetReq(5, "d"),
	}
	db.executeAutoBatch(reqs)
	for i, req := range reqs {
		if err := <-req.done; err != nil {
			t.Fatalf("batch req #%d failed: %v", i+1, err)
		}
	}

	st := db.AutoBatchStats()
	if st.BatchSize1 == 0 {
		t.Fatalf("expected single-request batch counter > 0, stats=%+v", st)
	}
	if st.BatchSize2To4 == 0 {
		t.Fatalf("expected 2..4 batch counter > 0, stats=%+v", st)
	}
	if st.ExecutedBatches < st.BatchSize1+st.BatchSize2To4+st.BatchSize5To8+st.BatchSize9Plus {
		t.Fatalf("batch size distribution exceeds executed batches, stats=%+v", st)
	}
}

func TestBatch_OpNamesMatchExecutionMode(t *testing.T) {
	makeReq := func(op autoBatchOp) *autoBatchRequest[uint64, Rec] {
		return &autoBatchRequest[uint64, Rec]{op: op, id: 1}
	}

	sharedDB, _ := openTempDBUint64(t)
	if got := sharedDB.autoBatchOpName(testAutoBatchRequestBuf(makeReq(autoBatchSet)), false); got != "batch" {
		t.Fatalf("shared single request op = %q, want batch", got)
	}
	if got := sharedDB.autoBatchOpName(testAutoBatchRequestBuf(makeReq(autoBatchSet)), true); got != "set" {
		t.Fatalf("isolated single request op = %q, want set", got)
	}
	if got := sharedDB.autoBatchOpName(testAutoBatchRequestBuf(makeReq(autoBatchSet), makeReq(autoBatchSet)), true); got != "batch_set" {
		t.Fatalf("isolated multi set op = %q, want batch_set", got)
	}

	soloDB, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	if got := soloDB.autoBatchOpName(testAutoBatchRequestBuf(makeReq(autoBatchPatch)), false); got != "patch" {
		t.Fatalf("single-only patch op = %q, want patch", got)
	}
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

	seed := &Rec{Name: "seed", Age: 7}
	b := encodePool.Get()
	if err := db.encode(seed, b); err != nil {
		encodePool.Put(b)
		t.Fatalf("encode(seed): %v", err)
	}
	seedReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         100,
		setValue:   seed,
		setPayload: b,
		done:       make(chan error, 1),
	}

	before := db.AutoBatchStats()
	db.autoBatcher.mu.Lock()
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(seedReq))
	db.autoBatcher.running = false
	db.autoBatcher.mu.Unlock()

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
	if after.Submitted != before.Submitted+1 || after.Enqueued != before.Enqueued+1 || after.Dequeued != before.Dequeued+2 {
		t.Fatalf("expected NoBatch write to use queued internal path and stay isolated, before=%+v after=%+v", before, after)
	}
	if after.BatchSize1 != before.BatchSize1+2 || after.MultiRequestBatches != before.MultiRequestBatches {
		t.Fatalf("expected queued seed request and NoBatch request to execute as separate single-request batches, before=%+v after=%+v", before, after)
	}
	if after.CallbackOps != before.CallbackOps+1 {
		t.Fatalf("expected NoBatch callback to run through internal batcher, before=%+v after=%+v", before, after)
	}
}

func TestBatch_PopSkipsCoalescingWindowForIsolatedHead(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   25 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	isolatedReq := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 1, done: make(chan error, 1)}
	followerReq := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 2, done: make(chan error, 1)}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(db,
		&autoBatchJob[uint64, Rec]{
			reqs:     testAutoBatchRequestBuf(isolatedReq),
			isolated: true,
			done:     isolatedReq.done,
		},
		queuedSingleJob(followerReq),
	)
	db.autoBatcher.mu.Unlock()

	before := db.AutoBatchStats()
	batch := db.popAutoBatch()
	after := db.AutoBatchStats()

	if len(batch) != 1 || batch[0].reqs.Len() != 1 || batch[0].reqs.Get(0) != isolatedReq {
		t.Fatalf("expected isolated head to pop immediately alone, got=%+v", batch)
	}
	if after.CoalesceWaits != before.CoalesceWaits {
		t.Fatalf("isolated head must skip coalescing wait, before=%+v after=%+v", before, after)
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if !db.autoBatcher.hotUntil.IsZero() {
		t.Fatalf("isolated head must not arm hot window, hotUntil=%v", db.autoBatcher.hotUntil)
	}
}

func TestBatch_PopSkipsCoalescingWindowBeforeIsolatedFollower(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   25 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	headReq := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 1, done: make(chan error, 1)}
	isolatedReq := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 2, done: make(chan error, 1)}
	tailReq := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 3, done: make(chan error, 1)}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(headReq),
		&autoBatchJob[uint64, Rec]{
			reqs:     testAutoBatchRequestBuf(isolatedReq),
			isolated: true,
			done:     isolatedReq.done,
		},
		queuedSingleJob(tailReq),
	)
	db.autoBatcher.mu.Unlock()

	before := db.AutoBatchStats()
	batch := db.popAutoBatch()
	after := db.AutoBatchStats()

	if len(batch) != 1 || batch[0].reqs.Len() != 1 || batch[0].reqs.Get(0) != headReq {
		t.Fatalf("expected head request before isolated follower to pop immediately alone, got=%+v", batch)
	}
	if after.CoalesceWaits != before.CoalesceWaits {
		t.Fatalf("isolated follower must prevent coalescing wait, before=%+v after=%+v", before, after)
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if !db.autoBatcher.hotUntil.IsZero() {
		t.Fatalf("isolated follower must not arm hot window for the preceding request, hotUntil=%v", db.autoBatcher.hotUntil)
	}
}

func TestBatch_CallbackRequestsStayBatchable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	cb := func(*bbolt.Tx, uint64, *Rec, *Rec) error { return nil }
	makeReq := func(id uint64, withCB bool) *autoBatchRequest[uint64, Rec] {
		req := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: id}
		if withCB {
			req.beforeCommit = []beforeCommitFunc[uint64, Rec]{cb}
		}
		return req
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(makeReq(1, false)),
		queuedSingleJob(makeReq(2, true)),
		queuedSingleJob(makeReq(3, false)),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 3 {
		t.Fatalf("expected callbacks to remain batchable, got %d", len(batch))
	}
	if batch[1].reqs.Len() != 1 || len(batch[1].reqs.Get(0).beforeCommit) == 0 {
		t.Fatalf("expected middle request with callback to stay in a multi-request batch")
	}
}

func TestBatch_RepeatIDPool_ClearsBetweenPops(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      1024,
		AutoBatchMaxQueue: 2048,
	})

	req1 := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 1, done: make(chan error, 1)}
	req2 := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 2, done: make(chan error, 1)}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(req1), queuedSingleJob(req2))
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 2 {
		t.Fatalf("unexpected popped batch size: got=%d want=2", len(batch))
	}

	req3 := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 3, done: make(chan error, 1)}
	req4 := &autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 4, done: make(chan error, 1)}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(req3), queuedSingleJob(req4))
	db.autoBatcher.mu.Unlock()

	next := db.popAutoBatch()
	if len(next) != 2 {
		t.Fatalf("unexpected next popped batch size: got=%d want=2", len(next))
	}
}

func TestBatch_RepeatIDPool_RemainsReusableAfterLargeBurst(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      128,
		AutoBatchMaxQueue: 256,
	})

	burst := make([]*autoBatchJob[uint64, Rec], 64)
	for i := range burst {
		req := &autoBatchRequest[uint64, Rec]{
			op:   autoBatchSet,
			id:   uint64(i + 1),
			done: make(chan error, 1),
		}
		burst[i] = queuedSingleJob(req)
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db, burst...)
	db.autoBatcher.mu.Unlock()

	if batch := db.popAutoBatch(); len(batch) != len(burst) {
		t.Fatalf("unexpected burst batch size: got=%d want=%d", len(batch), len(burst))
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(
		db,
		queuedSingleJob(&autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 1001, done: make(chan error, 1)}),
		queuedSingleJob(&autoBatchRequest[uint64, Rec]{op: autoBatchSet, id: 1002, done: make(chan error, 1)}),
	)
	db.autoBatcher.mu.Unlock()

	if batch := db.popAutoBatch(); len(batch) != 2 {
		t.Fatalf("unexpected small batch size after burst: got=%d want=2", len(batch))
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if db.autoBatcher.queueLen() != 0 {
		t.Fatalf("unexpected leftover queue after reuse batch: got=%d want=0", db.autoBatcher.queueLen())
	}
}

func TestBatch_RepeatIDPool_StringKeysClearedAfterPop(t *testing.T) {
	db, _ := openTempDBString(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      64,
		AutoBatchMaxQueue: 128,
	})

	req1 := &autoBatchRequest[string, Rec]{
		op:   autoBatchSet,
		id:   strings.Repeat("a", 256),
		done: make(chan error, 1),
	}
	req2 := &autoBatchRequest[string, Rec]{
		op:   autoBatchSet,
		id:   strings.Repeat("b", 256),
		done: make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(req1), queuedSingleJob(req2))
	db.autoBatcher.mu.Unlock()

	if batch := db.popAutoBatch(); len(batch) != 2 {
		t.Fatalf("unexpected popped batch size: got=%d want=2", len(batch))
	}

	req3 := &autoBatchRequest[string, Rec]{
		op:   autoBatchSet,
		id:   strings.Repeat("c", 256),
		done: make(chan error, 1),
	}
	req4 := &autoBatchRequest[string, Rec]{
		op:   autoBatchSet,
		id:   strings.Repeat("d", 256),
		done: make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(req3), queuedSingleJob(req4))
	db.autoBatcher.mu.Unlock()

	if next := db.popAutoBatch(); len(next) != 2 {
		t.Fatalf("unexpected string batch size after reuse: got=%d want=2", len(next))
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

func TestBatch_DuplicatePatchSameID_NonUniqueFieldsStayBatched(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"seed"}}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"seed-2"}}); err != nil {
		t.Fatalf("seed Set(2): %v", err)
	}

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"x"}}},
		patchIgnoreUnknown: true,
		policy:             db.autoBatchPatchRequestPolicy([]Field{{Name: "tags", Value: []string{"x"}}}, nil, nil),
		done:               make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"y"}}},
		patchIgnoreUnknown: true,
		policy:             db.autoBatchPatchRequestPolicy([]Field{{Name: "tags", Value: []string{"y"}}}, nil, nil),
		done:               make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 2,
		patch:              []Field{{Name: "tags", Value: []string{"z"}}},
		patchIgnoreUnknown: true,
		policy:             db.autoBatchPatchRequestPolicy([]Field{{Name: "tags", Value: []string{"z"}}}, nil, nil),
		done:               make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 3 {
		t.Fatalf("expected repeated non-unique patches to stay batched, got=%d", len(batch))
	}

	db.executeAutoBatchJobs(batch)

	for i, req := range []*autoBatchRequest[uint64, UniqueTestRec]{req1, req2, req3} {
		if err := <-req.done; err != nil {
			t.Fatalf("request #%d failed: %v", i+1, err)
		}
	}

	got1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got1 == nil || !slices.Equal(got1.Tags, []string{"y"}) {
		t.Fatalf("id=1 must reflect sequential repeated patches, got: %#v", got1)
	}

	got2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got2 == nil || !slices.Equal(got2.Tags, []string{"z"}) {
		t.Fatalf("id=2 must persist independent patch, got: %#v", got2)
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
		return b.Put(db.keyFromID(1), []byte{0xc1})
	}); err != nil {
		t.Fatalf("seed invalid payload: %v", err)
	}

	req1 := &autoBatchRequest[uint64, Rec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "age", Value: 31}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, Rec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "age", Value: 32}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{req1, req2})

	if err := <-req1.done; err == nil || !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("req1 error = %v, want decode existing value", err)
	}
	if err := <-req2.done; err == nil || !strings.Contains(err.Error(), "failed to decode existing value") {
		t.Fatalf("req2 error = %v, want decode existing value", err)
	}
}

func TestBatch_DuplicatePatchSameID_UniqueFieldCutsBatch(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "email", Value: "next@x"}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"y"}}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 2,
		patch:              []Field{{Name: "tags", Value: []string{"z"}}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected repeated unique-touching patches to cut batch, got=%d", len(batch))
	}
	if batch[0].reqs.Len() != 1 || batch[0].reqs.Get(0) != req1 {
		t.Fatalf("expected first request to stay alone in batch")
	}
}

func TestBatch_DuplicatePatchSameID_BeforeStoreOnUniqueDBCutsBatch(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	cb := func(_ uint64, _ *UniqueTestRec, newValue *UniqueTestRec) error {
		newValue.Email = "mutated@x"
		return nil
	}

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"x"}}},
		patchIgnoreUnknown: true,
		beforeStore:        []beforeStoreFunc[uint64, UniqueTestRec]{cb},
		done:               make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"y"}}},
		patchIgnoreUnknown: true,
		beforeStore:        []beforeStoreFunc[uint64, UniqueTestRec]{cb},
		done:               make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 2,
		patch:              []Field{{Name: "email", Value: "other@x"}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected BeforeStore-bearing repeated same-id patch to cut batch on unique DB, got=%d", len(batch))
	}
	if batch[0].reqs.Len() != 1 || batch[0].reqs.Get(0) != req1 {
		t.Fatalf("expected first request to stay alone in batch")
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if db.autoBatcher.queueLen() != 2 ||
		db.autoBatcher.queueAt(0).reqs.Len() != 1 || db.autoBatcher.queueAt(0).reqs.Get(0) != req2 ||
		db.autoBatcher.queueAt(1).reqs.Len() != 1 || db.autoBatcher.queueAt(1).reqs.Get(0) != req3 {
		t.Fatalf("expected remaining requests to stay queued in order")
	}
}

func TestBatch_DuplicatePatchSameID_BeforeProcessOnUniqueDBCutsBatch(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	cb := func(_ uint64, newValue *UniqueTestRec) error {
		newValue.Email = "mutated@x"
		return nil
	}

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"x"}}},
		patchIgnoreUnknown: true,
		beforeProcess:      []beforeProcessFunc[uint64, UniqueTestRec]{cb},
		done:               make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 1,
		patch:              []Field{{Name: "tags", Value: []string{"y"}}},
		patchIgnoreUnknown: true,
		beforeProcess:      []beforeProcessFunc[uint64, UniqueTestRec]{cb},
		done:               make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:                 autoBatchPatch,
		id:                 2,
		patch:              []Field{{Name: "email", Value: "other@x"}},
		patchIgnoreUnknown: true,
		done:               make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected BeforeProcess-bearing repeated same-id patch to cut batch on unique DB, got=%d", len(batch))
	}
	if batch[0].reqs.Len() != 1 || batch[0].reqs.Get(0) != req1 {
		t.Fatalf("expected first request to stay alone in batch")
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if db.autoBatcher.queueLen() != 2 ||
		db.autoBatcher.queueAt(0).reqs.Len() != 1 || db.autoBatcher.queueAt(0).reqs.Get(0) != req2 ||
		db.autoBatcher.queueAt(1).reqs.Len() != 1 || db.autoBatcher.queueAt(1).reqs.Get(0) != req3 {
		t.Fatalf("expected remaining requests to stay queued in order")
	}
}

func TestBatch_SetDeleteSameID_CoalescedToLastWrite(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &Rec{Name: "seed", Age: 11}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}

	setA := &Rec{Name: "A", Age: 20}
	setB := &Rec{Name: "B", Age: 30}
	setOther := &Rec{Name: "X", Age: 40}

	req1 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   setA,
		setPayload: mustEncodeAutoBatchPayload(t, db, setA),
		done:       make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, Rec]{
		op:     autoBatchDelete,
		id:     1,
		policy: db.autoBatchDeleteRequestPolicy(nil),
		done:   make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   setB,
		setPayload: mustEncodeAutoBatchPayload(t, db, setB),
		done:       make(chan error, 1),
	}
	req4 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         2,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   setOther,
		setPayload: mustEncodeAutoBatchPayload(t, db, setOther),
		done:       make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
		queuedSingleJob(req4),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req3 || req3.replacedBy != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.replacedBy, req2.replacedBy, req3.replacedBy)
	}

	db.executeAutoBatchJobs(batch)

	for i, req := range []*autoBatchRequest[uint64, Rec]{req1, req2, req3, req4} {
		if err := <-req.done; err != nil {
			t.Fatalf("request #%d failed: %v", i+1, err)
		}
	}

	got1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got1 == nil || got1.Name != "B" || got1.Age != 30 {
		t.Fatalf("id=1 must match last write in coalesced chain, got: %#v", got1)
	}
	got2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got2 == nil || got2.Name != "X" || got2.Age != 40 {
		t.Fatalf("id=2 unexpected value: %#v", got2)
	}

	if st := db.AutoBatchStats(); st.CoalescedSetDelete < 2 {
		t.Fatalf("expected at least 2 coalesced set/delete ops, got %d (stats=%+v)", st.CoalescedSetDelete, st)
	}
}

func TestBatch_CoalescedChain_PropagatesTerminalError(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &UniqueTestRec{Email: "seed@x", Code: 1}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "taken@x", Code: 2}); err != nil {
		t.Fatalf("seed Set(2): %v", err)
	}

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   &UniqueTestRec{Email: "mid@x", Code: 10},
		setPayload: mustEncodeAutoBatchPayload(t, db, &UniqueTestRec{Email: "mid@x", Code: 10}),
		done:       make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:     autoBatchDelete,
		id:     1,
		policy: db.autoBatchDeleteRequestPolicy(nil),
		done:   make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   &UniqueTestRec{Email: "taken@x", Code: 11},
		setPayload: mustEncodeAutoBatchPayload(t, db, &UniqueTestRec{Email: "taken@x", Code: 11}),
		done:       make(chan error, 1),
	}
	req4 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         3,
		policy:     db.autoBatchSetRequestPolicy(nil, nil),
		setValue:   &UniqueTestRec{Email: "ok@x", Code: 3},
		setPayload: mustEncodeAutoBatchPayload(t, db, &UniqueTestRec{Email: "ok@x", Code: 3}),
		done:       make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	setAutoBatchQueueJobsForTest(db,
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
		queuedSingleJob(req4),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req3 || req3.replacedBy != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.replacedBy, req2.replacedBy, req3.replacedBy)
	}

	db.executeAutoBatchJobs(batch)

	if err := <-req1.done; !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("req1 expected ErrUniqueViolation via coalesced chain, got: %v", err)
	}
	if err := <-req2.done; !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("req2 expected ErrUniqueViolation via coalesced chain, got: %v", err)
	}
	if err := <-req3.done; !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("req3 expected ErrUniqueViolation, got: %v", err)
	}
	if err := <-req4.done; err != nil {
		t.Fatalf("req4 must succeed despite coalesced chain failure, got: %v", err)
	}

	v1, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if v1 == nil || v1.Email != "seed@x" || v1.Code != 1 {
		t.Fatalf("id=1 must stay unchanged after failed terminal coalesced op, got: %#v", v1)
	}
	v2, err := db.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if v2 == nil || v2.Email != "taken@x" {
		t.Fatalf("id=2 unexpected value: %#v", v2)
	}
	v3, err := db.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if v3 == nil || v3.Email != "ok@x" || v3.Code != 3 {
		t.Fatalf("id=3 must persist successful independent op, got: %#v", v3)
	}
}

func TestBatch_PatchFailures_IsolateFailedRequest(t *testing.T) {
	type tc struct {
		name       string
		makeBadReq func() *autoBatchRequest[uint64, Rec]
		checkErr   func(error) bool
	}

	cases := []tc{
		{
			name: "missing_record",
			makeBadReq: func() *autoBatchRequest[uint64, Rec] {
				return &autoBatchRequest[uint64, Rec]{
					op:                 autoBatchPatch,
					id:                 999,
					patch:              []Field{{Name: "age", Value: 77}},
					patchIgnoreUnknown: true,
					done:               make(chan error, 1),
				}
			},
			checkErr: func(err error) bool { return err == nil },
		},
		{
			name: "invalid_patch",
			makeBadReq: func() *autoBatchRequest[uint64, Rec] {
				return &autoBatchRequest[uint64, Rec]{
					op:                 autoBatchPatch,
					id:                 1,
					patch:              []Field{{Name: "age", Value: "not-an-int"}},
					patchIgnoreUnknown: true,
					done:               make(chan error, 1),
				}
			},
			checkErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "failed to apply patch") },
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, Options{
				AutoBatchWindow:   5 * time.Millisecond,
				AutoBatchMax:      16,
				AutoBatchMaxQueue: 64,
			})

			if err := db.Set(1, &Rec{Name: "seed-1", Age: 10, Meta: Meta{Country: "NL"}}); err != nil {
				t.Fatalf("seed Set(1): %v", err)
			}
			if err := db.Set(2, &Rec{Name: "seed-2", Age: 20, Meta: Meta{Country: "DE"}}); err != nil {
				t.Fatalf("seed Set(2): %v", err)
			}

			badReq := c.makeBadReq()
			goodVal := &Rec{Name: "good-" + c.name, Age: 99, Meta: Meta{Country: "US"}}
			goodReq := &autoBatchRequest[uint64, Rec]{
				op:         autoBatchSet,
				id:         2,
				setValue:   goodVal,
				setPayload: mustEncodeAutoBatchPayload(t, db, goodVal),
				done:       make(chan error, 1),
			}

			db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

			if err := <-badReq.done; !c.checkErr(err) {
				t.Fatalf("unexpected bad request error: %v", err)
			}
			if err := <-goodReq.done; err != nil {
				t.Fatalf("good request must succeed, got: %v", err)
			}

			got1, err := db.Get(1)
			if err != nil {
				t.Fatalf("Get(1): %v", err)
			}
			if got1 == nil || got1.Name != "seed-1" || got1.Age != 10 {
				t.Fatalf("id=1 must remain unchanged after bad patch request, got: %#v", got1)
			}

			got2, err := db.Get(2)
			if err != nil {
				t.Fatalf("Get(2): %v", err)
			}
			if got2 == nil || got2.Name != goodVal.Name || got2.Age != goodVal.Age || got2.Country != goodVal.Country {
				t.Fatalf("id=2 must persist successful request, got: %#v", got2)
			}
		})
	}
}

func TestBatch_QueueFull_WaitsAndStaysQueued(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1,
	})

	blocked := &Rec{Name: "queued", Age: 11, Meta: Meta{Country: "NL"}}
	b := encodePool.Get()
	if err := db.encode(blocked, b); err != nil {
		encodePool.Put(b)
		t.Fatalf("encode(blocked): %v", err)
	}
	blockedReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         123,
		setValue:   blocked,
		setPayload: b,
		done:       make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(blockedReq))
	db.autoBatcher.running = false
	db.autoBatcher.mu.Unlock()
	defer func() {
		db.autoBatcher.mu.Lock()
		db.autoBatcher.queue = nil
		db.autoBatcher.queueHead = 0
		db.autoBatcher.queueSize = 0
		db.autoBatcher.running = false
		db.autoBatcher.mu.Unlock()
	}()

	before := db.AutoBatchStats()
	if err := db.Set(1, &Rec{Name: "fallback", Age: 42, Meta: Meta{Country: "US"}}); err != nil {
		t.Fatalf("Set must succeed after queued wait, got: %v", err)
	}
	after := db.AutoBatchStats()

	if after.Submitted != before.Submitted+1 || after.Enqueued != before.Enqueued+1 {
		t.Fatalf("expected queue-full path to remain queued instead of falling back, before=%+v after=%+v", before, after)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "fallback" || got.Age != 42 || got.Country != "US" {
		t.Fatalf("unexpected value after queue-full queued write: %#v", got)
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

func TestBatch_BeforeCommitError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	cbErr := errors.New("callback fail")
	badVal := &Rec{Name: "bad", Age: 1}
	goodVal := &Rec{Name: "good", Age: 2}

	badReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   badVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, badVal),
		beforeCommit: []beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   goodVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, goodVal),
		done:       make(chan error, 1),
	}

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := <-badReq.done; !errors.Is(err, cbErr) {
		t.Fatalf("bad request must fail with callback error, got: %v", err)
	}
	if err := <-goodReq.done; err != nil {
		t.Fatalf("good request must succeed, got: %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must not persist after callback failure, got: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "good" {
		t.Fatalf("id=2 must persist after isolation retry, got: %#v", got)
	}
}

func TestBatch_BeforeCommitError_Isolation_RechecksUniqueAfterRetry(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	cbErr := errors.New("callback fail")
	badVal := &UniqueTestRec{Email: "shared@x", Code: 1}
	goodVal := &UniqueTestRec{Email: "shared@x", Code: 2}

	badReq := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   badVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, badVal),
		beforeCommit: []beforeCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *UniqueTestRec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   goodVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, goodVal),
		done:       make(chan error, 1),
	}

	db.executeAutoBatch([]*autoBatchRequest[uint64, UniqueTestRec]{badReq, goodReq})

	if err := <-badReq.done; !errors.Is(err, cbErr) {
		t.Fatalf("bad request must fail with callback error, got: %v", err)
	}
	if err := <-goodReq.done; err != nil {
		t.Fatalf("good request must succeed after callback isolation, got: %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must not persist after callback failure, got: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Email != "shared@x" || got.Code != 2 {
		t.Fatalf("id=2 must persist with shared unique value after retry, got: %#v", got)
	}
}

func TestBatch_BeforeStore_RerunsFromBaselineOnRetry(t *testing.T) {
	db, _ := openTempDBUint64(t)

	beforeStoreCalls := 0
	goodVal := &Rec{Name: "good", Age: 10}
	goodReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   goodVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, goodVal),
		beforeStore: []beforeStoreFunc[uint64, Rec]{
			func(_ uint64, _ *Rec, newValue *Rec) error {
				beforeStoreCalls++
				newValue.Age++
				return nil
			},
		},
		done: make(chan error, 1),
	}

	cbErr := errors.New("before commit fail")
	badVal := &Rec{Name: "bad", Age: 20}
	badReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   badVal,
		setPayload: mustEncodeAutoBatchPayload(t, db, badVal),
		beforeCommit: []beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error { return cbErr },
		},
		done: make(chan error, 1),
	}

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := <-badReq.done; !errors.Is(err, cbErr) {
		t.Fatalf("bad request must fail with BeforeCommit error, got: %v", err)
	}
	if err := <-goodReq.done; err != nil {
		t.Fatalf("good request must succeed after retry, got: %v", err)
	}
	if beforeStoreCalls != 2 {
		t.Fatalf("expected BeforeStore to run twice due to retry, got %d", beforeStoreCalls)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Age != 11 {
		t.Fatalf("expected BeforeStore to restart from baseline and persist age=11, got %#v", got)
	}
}

func TestBatch_StringKeyBeforeStoreError_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	hookErr := errors.New("before store fail")
	req := &autoBatchRequest[string, Product]{
		op:         autoBatchSet,
		id:         "ghost-before-store",
		setValue:   &Product{SKU: "ghost-before-store", Price: 11},
		setPayload: mustEncodeAutoBatchPayload(t, db, &Product{SKU: "ghost-before-store", Price: 11}),
		beforeStore: []beforeStoreFunc[string, Product]{
			func(_ string, _ *Product, _ *Product) error { return hookErr },
		},
		done: make(chan error, 1),
	}

	db.executeAutoBatch([]*autoBatchRequest[string, Product]{req})

	if err := <-req.done; !errors.Is(err, hookErr) {
		t.Fatalf("request must fail with BeforeStore error, got: %v", err)
	}
	if got, err := db.Get("ghost-before-store"); err != nil {
		t.Fatalf("Get(ghost-before-store): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-before-store must not persist after BeforeStore failure, got %#v", got)
	}
	if after := len(db.strmap.Keys); after != initial {
		t.Fatalf("strmap grew after BeforeStore failure: initial=%d after=%d", initial, after)
	}
}

func TestBatch_CompletedRequest_ReleasesResources(t *testing.T) {
	db, _ := openTempDBUint64(t)

	req := mustBuildSetAutoReq(
		t,
		db,
		1,
		&Rec{Name: "cleanup", Age: 11},
		[]beforeStoreFunc[uint64, Rec]{
			func(_ uint64, _ *Rec, _ *Rec) error { return nil },
		},
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return nil },
		},
		nil,
	)
	if req.setPayload == nil {
		t.Fatal("expected prepared payload before execution")
	}

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{req})

	if err := mustAutoBatchErr(t, req); err != nil {
		t.Fatalf("req error = %v", err)
	}
	if req.setPayload != nil {
		t.Fatal("set payload buffer must be released after completion")
	}
	if req.setValue != nil || req.setBaseline != nil {
		t.Fatalf("set values must be cleared after completion: value=%#v baseline=%#v", req.setValue, req.setBaseline)
	}
	if req.patch != nil {
		t.Fatalf("patch slice must be cleared after completion: %#v", req.patch)
	}
	if req.beforeProcess != nil || req.beforeStore != nil || req.beforeCommit != nil {
		t.Fatalf("callbacks must be cleared after completion: beforeProcess=%v beforeStore=%v beforeCommit=%v", req.beforeProcess, req.beforeStore, req.beforeCommit)
	}
	if req.cloneValue != nil {
		t.Fatal("cloneValue must be cleared after completion")
	}
	if req.replacedBy != nil {
		t.Fatalf("replacedBy must be cleared after completion: %p", req.replacedBy)
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

/**/

func mustAutoBatchErr[K ~string | ~uint64, V any](tb testing.TB, req *autoBatchRequest[K, V]) error {
	tb.Helper()
	if req == nil || req.done == nil {
		tb.Fatal("request has no done channel")
	}
	select {
	case err := <-req.done:
		return err
	case <-time.After(2 * time.Second):
		tb.Fatal("timeout waiting for auto-batch request")
		return nil
	}
}

func mustBuildSetAutoReq[K ~string | ~uint64, V any](
	tb testing.TB,
	db *DB[K, V],
	id K,
	newVal *V,
	beforeStore []beforeStoreFunc[K, V],
	beforeCommit []beforeCommitFunc[K, V],
	cloneValue func(K, *V) *V,
) *autoBatchRequest[K, V] {
	tb.Helper()
	req, err := db.buildSetAutoBatchRequest(id, newVal, beforeStore, beforeCommit, cloneValue)
	if err != nil {
		tb.Fatalf("buildSetAutoBatchRequest(%v): %v", id, err)
	}
	return req
}

func mustBuildPatchAutoReq[K ~string | ~uint64, V any](
	_ testing.TB,
	db *DB[K, V],
	id K,
	patch []Field,
	ignoreUnknown bool,
	beforeProcess []beforeProcessFunc[K, V],
	beforeStore []beforeStoreFunc[K, V],
	beforeCommit []beforeCommitFunc[K, V],
) *autoBatchRequest[K, V] {
	req := db.buildPatchAutoBatchRequest(id, patch, ignoreUnknown, beforeProcess, beforeStore, beforeCommit)
	req.done = make(chan error, 1)
	return req
}

func mustBuildDeleteAutoReq[K ~string | ~uint64, V any](
	_ testing.TB,
	db *DB[K, V],
	id K,
	beforeCommit []beforeCommitFunc[K, V],
) *autoBatchRequest[K, V] {
	req := db.buildDeleteAutoBatchRequest(id, beforeCommit)
	req.done = make(chan error, 1)
	return req
}

func setAutoBatchQueueJobsForTest[K ~string | ~uint64, V any](db *DB[K, V], jobs ...*autoBatchJob[K, V]) {
	db.autoBatcher.queueHead = 0
	db.autoBatcher.queueSize = len(jobs)
	db.autoBatcher.queue = make([]*autoBatchJob[K, V], len(jobs))
	copy(db.autoBatcher.queue, jobs)
}

func popQueuedSingleReqBatch[K ~string | ~uint64, V any](tb testing.TB, db *DB[K, V], reqs ...*autoBatchRequest[K, V]) []*autoBatchJob[K, V] {
	tb.Helper()
	limit := len(reqs)
	if limit < 16 {
		limit = 16
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = limit
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	db.autoBatcher.queueHead = 0
	db.autoBatcher.queueSize = len(reqs)
	db.autoBatcher.queue = make([]*autoBatchJob[K, V], len(reqs))
	for i, req := range reqs {
		db.autoBatcher.queue[i] = queuedSingleJob(req)
	}
	db.autoBatcher.mu.Unlock()

	return db.popAutoBatch()
}

func TestAutoBatchExt_SharedSet_BeforeStoreError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	hookErr := errors.New("before store failed")
	badReq := mustBuildSetAutoReq(
		t,
		db,
		1,
		&Rec{Name: "bad", Age: 11},
		[]beforeStoreFunc[uint64, Rec]{
			func(_ uint64, _ *Rec, _ *Rec) error { return hookErr },
		},
		nil,
		nil,
	)
	goodReq := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "good", Age: 22}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want %v", err, hookErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must stay absent, got %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "good" || got.Age != 22 {
		t.Fatalf("unexpected id=2 value: %#v", got)
	}
}

func TestAutoBatchExt_SharedSet_DecodePreparedValueError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	badReq := mustBuildSetAutoReq(
		t,
		db,
		1,
		&Rec{Name: "bad", Age: 11},
		[]beforeStoreFunc[uint64, Rec]{
			func(_ uint64, _ *Rec, _ *Rec) error { return nil },
		},
		nil,
		nil,
	)
	replaceAutoBatchPayloadForTest(badReq, rawAutoBatchPayload(t, []byte{0xc1}))

	goodReq := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "good", Age: 22}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); err == nil || !strings.Contains(err.Error(), "decode prepared value") {
		t.Fatalf("bad request error = %v, want decode prepared value", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must stay absent, got %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "good" || got.Age != 22 {
		t.Fatalf("unexpected id=2 value: %#v", got)
	}
}

func TestAutoBatchExt_SharedPatch_BeforeProcessError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	hookErr := errors.New("before process failed")
	badReq := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 55}},
		true,
		[]beforeProcessFunc[uint64, Rec]{
			func(_ uint64, _ *Rec) error { return hookErr },
		},
		nil,
		nil,
	)
	goodReq := mustBuildPatchAutoReq(t, db, 2, []Field{{Name: "age", Value: 66}}, true, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want %v", err, hookErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Age != 10 {
		t.Fatalf("id=1 changed after failed patch: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Age != 66 {
		t.Fatalf("id=2 not patched: %#v", got)
	}
}

func TestAutoBatchExt_SharedPatch_BeforeStoreError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	hookErr := errors.New("before store failed")
	badReq := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 55}},
		true,
		nil,
		[]beforeStoreFunc[uint64, Rec]{
			func(_ uint64, _ *Rec, _ *Rec) error { return hookErr },
		},
		nil,
	)
	goodReq := mustBuildPatchAutoReq(t, db, 2, []Field{{Name: "age", Value: 66}}, true, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want %v", err, hookErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Age != 10 {
		t.Fatalf("id=1 changed after failed patch: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Age != 66 {
		t.Fatalf("id=2 not patched: %#v", got)
	}
}

func TestAutoBatchExt_SharedPatch_PatchStrictUnknownField_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	badReq := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "missing", Value: 1}}, false, nil, nil, nil)
	goodReq := mustBuildPatchAutoReq(t, db, 2, []Field{{Name: "age", Value: 66}}, true, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); err == nil || !strings.Contains(err.Error(), "cannot patch field") {
		t.Fatalf("bad request error = %v, want strict patch error", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Age != 10 {
		t.Fatalf("id=1 changed after failed patch: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Age != 66 {
		t.Fatalf("id=2 not patched: %#v", got)
	}
}

func TestAutoBatchExt_SharedDelete_BeforeCommitError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	cbErr := errors.New("before commit failed")
	badReq := mustBuildDeleteAutoReq(
		t,
		db,
		1,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return cbErr },
		},
	)
	goodReq := mustBuildDeleteAutoReq(t, db, 2, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, cbErr) {
		t.Fatalf("bad request error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil {
		t.Fatal("id=1 must remain after failed delete")
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got != nil {
		t.Fatalf("id=2 must be deleted, got %#v", got)
	}
}

func TestAutoBatchExt_SharedDelete_DecodeError_IsolatesFailedRequest(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.bolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		return b.Put(db.keyFromID(1), []byte{0xff, 0x00, 0x7f, 0x42})
	}); err != nil {
		t.Fatalf("corrupt id=1: %v", err)
	}

	badReq := mustBuildDeleteAutoReq(t, db, 1, nil)
	goodReq := mustBuildDeleteAutoReq(t, db, 2, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("bad request error = %v, want decode error", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}

	if err := db.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(db.bucket)
		if b == nil {
			return fmt.Errorf("bucket does not exist")
		}
		if raw := b.Get(db.keyFromID(1)); raw == nil {
			return fmt.Errorf("id=1 was deleted despite decode failure")
		}
		return nil
	}); err != nil {
		t.Fatalf("verify raw id=1: %v", err)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got != nil {
		t.Fatalf("id=2 must be deleted, got %#v", got)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_FirstBeforeCommitError_RetriesFollowerFromOriginalState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10, Tags: []string{"base"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("first callback failed")
	req1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 20}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return cbErr },
		},
	)
	req2 := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "tags", Value: []string{"ok"}}}, true, nil, nil, nil)

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); !errors.Is(err, cbErr) {
		t.Fatalf("req1 error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Age != 10 || !slices.Equal(got.Tags, []string{"ok"}) {
		t.Fatalf("id=1 final state = %#v, want original age + retried tags", got)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_MiddleBeforeCommitError_RetriesNeighborsSequentially(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10, Tags: []string{"base"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("middle callback failed")
	req1 := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "age", Value: 20}}, true, nil, nil, nil)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "tags", Value: []string{"bad"}}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return cbErr },
		},
	)
	req3 := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "name", Value: "tail"}}, true, nil, nil, nil)

	batch := popQueuedSingleReqBatch(t, db, req1, req2, req3)
	if len(batch) != 3 {
		t.Fatalf("popped batch size = %d, want 3", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); !errors.Is(err, cbErr) {
		t.Fatalf("req2 error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, req3); err != nil {
		t.Fatalf("req3 error = %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Age != 20 || got.Name != "tail" || !slices.Equal(got.Tags, []string{"base"}) {
		t.Fatalf("id=1 final state = %#v, want age=20 name=tail tags=[base]", got)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_BeforeStoreSeesSteppedOldValue(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var seen []string
	hook := func(_ uint64, oldValue, newValue *Rec) error {
		seen = append(seen, fmt.Sprintf("%s/%d->%s/%d", oldValue.Name, oldValue.Age, newValue.Name, newValue.Age))
		return nil
	}

	req1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 20}},
		true,
		nil,
		[]beforeStoreFunc[uint64, Rec]{hook},
		nil,
	)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "name", Value: "next"}},
		true,
		nil,
		[]beforeStoreFunc[uint64, Rec]{hook},
		nil,
	)

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
	}

	want := []string{"seed/10->seed/20", "seed/20->next/20"}
	if !slices.Equal(seen, want) {
		t.Fatalf("BeforeStore sequence = %v, want %v", seen, want)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_BeforeCommitSeesSteppedOldValue(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var seen []string
	hook := func(tx *bbolt.Tx, key uint64, oldValue, newValue *Rec) error {
		raw := tx.Bucket(db.bucket).Get(db.keyFromID(key))
		if raw == nil {
			return fmt.Errorf("missing raw value in BeforeCommit")
		}
		current, err := db.decode(raw)
		if err != nil {
			return fmt.Errorf("decode current: %w", err)
		}
		seen = append(seen, fmt.Sprintf("%s/%d->%s/%d|tx=%s/%d", oldValue.Name, oldValue.Age, newValue.Name, newValue.Age, current.Name, current.Age))
		return nil
	}

	req1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 20}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{hook},
	)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "name", Value: "next"}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{hook},
	)

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
	}

	want := []string{"seed/10->seed/20|tx=seed/20", "seed/20->next/20|tx=next/20"}
	if !slices.Equal(seen, want) {
		t.Fatalf("BeforeCommit sequence = %v, want %v", seen, want)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_BeforeProcessMutationsAreSequential(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10, Tags: []string{"base"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	call := 0
	hook := func(_ uint64, newValue *Rec) error {
		call++
		switch call {
		case 1:
			newValue.Name = "step-1"
			newValue.Tags = append(newValue.Tags, "bp1")
		case 2:
			if !slices.Equal(newValue.Tags, []string{"base", "bp1"}) {
				return fmt.Errorf("second BeforeProcess saw tags=%v, want [base bp1]", newValue.Tags)
			}
			newValue.Name = "step-2"
		default:
			return fmt.Errorf("unexpected BeforeProcess call %d", call)
		}
		return nil
	}

	req1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 20}},
		true,
		[]beforeProcessFunc[uint64, Rec]{hook},
		nil,
		nil,
	)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "score", Value: 5.5}},
		true,
		[]beforeProcessFunc[uint64, Rec]{hook},
		nil,
		nil,
	)

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "step-2" || got.Age != 20 || got.Score != 5.5 || !slices.Equal(got.Tags, []string{"base", "bp1"}) {
		t.Fatalf("unexpected final value: %#v", got)
	}
}

func TestAutoBatchExt_RepeatedDeleteSameID_BeforeCommitRunsOnce(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed", Age: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	calls := 0
	hook := func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
		calls++
		return nil
	}

	req1 := mustBuildDeleteAutoReq(t, db, 1, []beforeCommitFunc[uint64, Rec]{hook})
	req2 := mustBuildDeleteAutoReq(t, db, 1, []beforeCommitFunc[uint64, Rec]{hook})

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
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

func TestAutoBatchExt_CoalescedInterleavedChains_PreserveFinalState(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	req1 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "A", Age: 30}, nil, nil, nil)
	req2 := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "B", Age: 40}, nil, nil, nil)
	req3 := mustBuildDeleteAutoReq(t, db, 1, nil)
	req4 := mustBuildDeleteAutoReq(t, db, 2, nil)
	req5 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "C", Age: 50}, nil, nil, nil)
	req6 := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "D", Age: 60}, nil, nil, nil)

	batch := popQueuedSingleReqBatch(t, db, req1, req2, req3, req4, req5, req6)
	if len(batch) != 6 {
		t.Fatalf("popped batch size = %d, want 6", len(batch))
	}
	if req1.replacedBy != req3 || req3.replacedBy != req5 {
		t.Fatalf("unexpected id=1 coalesce chain: req1->%p req3->%p", req1.replacedBy, req3.replacedBy)
	}
	if req2.replacedBy != req4 || req4.replacedBy != req6 {
		t.Fatalf("unexpected id=2 coalesce chain: req2->%p req4->%p", req2.replacedBy, req4.replacedBy)
	}

	db.executeAutoBatchJobs(batch)

	for i, req := range []*autoBatchRequest[uint64, Rec]{req1, req2, req3, req4, req5, req6} {
		if err := mustAutoBatchErr(t, req); err != nil {
			t.Fatalf("req%d error = %v", i+1, err)
		}
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "C" || got.Age != 50 {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "D" || got.Age != 60 {
		t.Fatalf("unexpected id=2 value: %#v", got)
	}

	if ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "A"))); err != nil {
		t.Fatalf("QueryKeys(name=A): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("stale name=A ids: %v", ids)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "B"))); err != nil {
		t.Fatalf("QueryKeys(name=B): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("stale name=B ids: %v", ids)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "C"))); err != nil {
		t.Fatalf("QueryKeys(name=C): %v", err)
	} else if !slices.Equal(ids, []uint64{1}) {
		t.Fatalf("name=C ids = %v, want [1]", ids)
	}
	if ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "D"))); err != nil {
		t.Fatalf("QueryKeys(name=D): %v", err)
	} else if !slices.Equal(ids, []uint64{2}) {
		t.Fatalf("name=D ids = %v, want [2]", ids)
	}
}

func TestAutoBatchExt_CoalescedInterleavedChains_TerminalCommitErrorPropagatesToAllMembers(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	db.testHooks.beforeCommit = func(op string) error {
		if op == "batch" {
			return fmt.Errorf("failpoint: commit batch")
		}
		return nil
	}
	t.Cleanup(func() { db.testHooks.beforeCommit = nil })

	req1 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "A", Age: 30}, nil, nil, nil)
	req2 := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "B", Age: 40}, nil, nil, nil)
	req3 := mustBuildDeleteAutoReq(t, db, 1, nil)
	req4 := mustBuildDeleteAutoReq(t, db, 2, nil)

	batch := popQueuedSingleReqBatch(t, db, req1, req2, req3, req4)
	if len(batch) != 4 {
		t.Fatalf("popped batch size = %d, want 4", len(batch))
	}

	db.executeAutoBatchJobs(batch)

	for i, req := range []*autoBatchRequest[uint64, Rec]{req1, req2, req3, req4} {
		err := mustAutoBatchErr(t, req)
		if err == nil || !strings.Contains(err.Error(), "failpoint: commit batch") {
			t.Fatalf("req%d error = %v, want commit failpoint", i+1, err)
		}
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "seed-1" {
		t.Fatalf("id=1 changed after failed commit: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "seed-2" {
		t.Fatalf("id=2 changed after failed commit: %#v", got)
	}

	assertNoFutureSnapshotRefs(t, db)
}

func TestAutoBatchExt_CoalescedChain_NeighborCallbackFailureDoesNotPoisonChain(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}

	cbErr := errors.New("neighbor callback failed")
	req1 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "next", Age: 20}, nil, nil, nil)
	req2 := mustBuildSetAutoReq(
		t,
		db,
		2,
		&Rec{Name: "bad", Age: 30},
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return cbErr },
		},
		nil,
	)
	req3 := mustBuildDeleteAutoReq(t, db, 1, nil)

	batch := popQueuedSingleReqBatch(t, db, req1, req2, req3)
	if len(batch) != 3 {
		t.Fatalf("popped batch size = %d, want 3", len(batch))
	}
	if req1.replacedBy != req3 {
		t.Fatalf("expected req1 to coalesce to req3, got %p", req1.replacedBy)
	}

	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); err != nil {
		t.Fatalf("req1 error = %v", err)
	}
	if err := mustAutoBatchErr(t, req2); !errors.Is(err, cbErr) {
		t.Fatalf("req2 error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, req3); err != nil {
		t.Fatalf("req3 error = %v", err)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got != nil {
		t.Fatalf("id=1 must be deleted by surviving coalesced chain, got %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got != nil {
		t.Fatalf("id=2 must not persist after callback failure, got %#v", got)
	}
}

func TestAutoBatchExt_SharedStringSet_BeforeStoreErrorWithNeighbor_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	hookErr := errors.New("before store failed")
	badReq := mustBuildSetAutoReq(
		t,
		db,
		"ghost-before-store",
		&Product{SKU: "ghost-before-store", Price: 11},
		[]beforeStoreFunc[string, Product]{
			func(_ string, _ *Product, _ *Product) error { return hookErr },
		},
		nil,
		nil,
	)
	goodReq := mustBuildSetAutoReq(t, db, "p2", &Product{SKU: "p2", Price: 22}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[string, Product]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, hookErr) {
		t.Fatalf("bad request error = %v, want %v", err, hookErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got, err := db.Get("ghost-before-store"); err != nil {
		t.Fatalf("Get(ghost-before-store): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-before-store must stay absent, got %#v", got)
	}
	if got, err := db.Get("p2"); err != nil {
		t.Fatalf("Get(p2): %v", err)
	} else if got == nil || got.Price != 22 {
		t.Fatalf("unexpected p2 value: %#v", got)
	}
	if after := len(db.strmap.Keys); after != initial+1 {
		t.Fatalf("strmap size = %d, want %d", after, initial+1)
	}
}

func TestAutoBatchExt_SharedStringSet_DecodePreparedValueErrorWithNeighbor_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	badReq := mustBuildSetAutoReq(
		t,
		db,
		"ghost-decode",
		&Product{SKU: "ghost-decode", Price: 11},
		[]beforeStoreFunc[string, Product]{
			func(_ string, _ *Product, _ *Product) error { return nil },
		},
		nil,
		nil,
	)
	replaceAutoBatchPayloadForTest(badReq, rawAutoBatchPayload(t, []byte{0xc1}))
	goodReq := mustBuildSetAutoReq(t, db, "p2", &Product{SKU: "p2", Price: 22}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[string, Product]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); err == nil || !strings.Contains(err.Error(), "decode prepared value") {
		t.Fatalf("bad request error = %v, want decode prepared value", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got, err := db.Get("ghost-decode"); err != nil {
		t.Fatalf("Get(ghost-decode): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-decode must stay absent, got %#v", got)
	}
	if after := len(db.strmap.Keys); after != initial+1 {
		t.Fatalf("strmap size = %d, want %d", after, initial+1)
	}
}

func TestAutoBatchExt_SharedStringSet_UniqueRejectWithNeighbor_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringUnique(t)

	if err := db.Set("u1", &StringUniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	badReq := mustBuildSetAutoReq(t, db, "u-dup", &StringUniqueTestRec{Email: "a@x", Code: 2}, nil, nil, nil)
	goodReq := mustBuildSetAutoReq(t, db, "u-ok", &StringUniqueTestRec{Email: "c@x", Code: 3}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[string, StringUniqueTestRec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want ErrUniqueViolation", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got, err := db.Get("u-dup"); err != nil {
		t.Fatalf("Get(u-dup): %v", err)
	} else if got != nil {
		t.Fatalf("u-dup must stay absent, got %#v", got)
	}
	if got, err := db.Get("u-ok"); err != nil {
		t.Fatalf("Get(u-ok): %v", err)
	} else if got == nil || got.Email != "c@x" || got.Code != 3 {
		t.Fatalf("unexpected u-ok value: %#v", got)
	}
	if after := len(db.strmap.Keys); after != initial+1 {
		t.Fatalf("strmap size = %d, want %d", after, initial+1)
	}
}

func TestAutoBatchExt_SharedStringSet_CallbackFailureWithNeighbor_DoesNotGrowStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("p1", &Product{SKU: "p1", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	initial := len(db.strmap.Keys)

	cbErr := errors.New("callback failed")
	badReq := mustBuildSetAutoReq(
		t,
		db,
		"ghost-cb",
		&Product{SKU: "ghost-cb", Price: 11},
		nil,
		[]beforeCommitFunc[string, Product]{
			func(_ *bbolt.Tx, _ string, _ *Product, _ *Product) error { return cbErr },
		},
		nil,
	)
	goodReq := mustBuildSetAutoReq(t, db, "p2", &Product{SKU: "p2", Price: 22}, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[string, Product]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, cbErr) {
		t.Fatalf("bad request error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got, err := db.Get("ghost-cb"); err != nil {
		t.Fatalf("Get(ghost-cb): %v", err)
	} else if got != nil {
		t.Fatalf("ghost-cb must stay absent, got %#v", got)
	}
	if got, err := db.Get("p2"); err != nil {
		t.Fatalf("Get(p2): %v", err)
	} else if got == nil || got.Price != 22 {
		t.Fatalf("unexpected p2 value: %#v", got)
	}
	if after := len(db.strmap.Keys); after != initial+1 {
		t.Fatalf("strmap size = %d, want %d", after, initial+1)
	}
}

func TestAutoBatchExt_BatchAtomic_CloneFuncRetryStartsFromFreshClone(t *testing.T) {
	db := openTempDBUint64BeforeStoreCloneRec(t)

	cbErr := errors.New("callback failed")
	cloneValue := func(_ uint64, v *beforeStoreCloneRec) *beforeStoreCloneRec {
		cp := *v
		return &cp
	}

	goodInput := &beforeStoreCloneRec{}
	goodBeforeStoreCalls := 0
	goodReq := mustBuildSetAutoReq(
		t,
		db,
		1,
		goodInput,
		[]beforeStoreFunc[uint64, beforeStoreCloneRec]{
			func(_ uint64, _ *beforeStoreCloneRec, newValue *beforeStoreCloneRec) error {
				goodBeforeStoreCalls++
				newValue.Name = "good"
				newValue.Ready = true
				return nil
			},
		},
		nil,
		cloneValue,
	)

	badInput := &beforeStoreCloneRec{}
	badReq := mustBuildSetAutoReq(
		t,
		db,
		2,
		badInput,
		[]beforeStoreFunc[uint64, beforeStoreCloneRec]{
			func(_ uint64, _ *beforeStoreCloneRec, newValue *beforeStoreCloneRec) error {
				newValue.Name = "bad"
				newValue.Ready = true
				return nil
			},
		},
		[]beforeCommitFunc[uint64, beforeStoreCloneRec]{
			func(_ *bbolt.Tx, _ uint64, _ *beforeStoreCloneRec, _ *beforeStoreCloneRec) error { return cbErr },
		},
		cloneValue,
	)

	db.executeAutoBatch([]*autoBatchRequest[uint64, beforeStoreCloneRec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, cbErr) {
		t.Fatalf("bad request error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if goodBeforeStoreCalls != 2 {
		t.Fatalf("good BeforeStore calls = %d, want 2", goodBeforeStoreCalls)
	}
	if goodInput.Name != "" || goodInput.Ready {
		t.Fatalf("good input mutated: %#v", goodInput)
	}
	if badInput.Name != "" || badInput.Ready {
		t.Fatalf("bad input mutated: %#v", badInput)
	}
	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "good" || !got.Ready {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got != nil {
		t.Fatalf("id=2 must stay absent, got %#v", got)
	}
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

func TestAutoBatchExt_SharedPatch_MissingThenPresent_OnlyExistingBeforeCommit(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(2, &Rec{Name: "seed", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var calls []uint64
	hook := func(_ *bbolt.Tx, key uint64, _ *Rec, _ *Rec) error {
		calls = append(calls, key)
		return nil
	}

	missingReq := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "age", Value: 11}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{hook},
	)
	presentReq := mustBuildPatchAutoReq(
		t,
		db,
		2,
		[]Field{{Name: "age", Value: 99}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{hook},
	)

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{missingReq, presentReq})

	if err := mustAutoBatchErr(t, missingReq); err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := mustAutoBatchErr(t, presentReq); err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !slices.Equal(calls, []uint64{2}) {
		t.Fatalf("BeforeCommit keys = %v, want [2]", calls)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Age != 99 {
		t.Fatalf("id=2 not patched: %#v", got)
	}
}

func TestAutoBatchExt_SharedDelete_MissingThenPresent_OnlyExistingBeforeCommit(t *testing.T) {
	db, _ := openTempDBUint64(t)

	if err := db.Set(2, &Rec{Name: "seed", Age: 20}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	var calls []uint64
	hook := func(_ *bbolt.Tx, key uint64, _ *Rec, _ *Rec) error {
		calls = append(calls, key)
		return nil
	}

	missingReq := mustBuildDeleteAutoReq(t, db, 1, []beforeCommitFunc[uint64, Rec]{hook})
	presentReq := mustBuildDeleteAutoReq(t, db, 2, []beforeCommitFunc[uint64, Rec]{hook})

	db.executeAutoBatch([]*autoBatchRequest[uint64, Rec]{missingReq, presentReq})

	if err := mustAutoBatchErr(t, missingReq); err != nil {
		t.Fatalf("missing request error = %v", err)
	}
	if err := mustAutoBatchErr(t, presentReq); err != nil {
		t.Fatalf("present request error = %v", err)
	}
	if !slices.Equal(calls, []uint64{2}) {
		t.Fatalf("BeforeCommit keys = %v, want [2]", calls)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got != nil {
		t.Fatalf("id=2 must be deleted, got %#v", got)
	}
}

func TestAutoBatchExt_SharedPatch_UniqueConflict_SelectsOnlyConflictingRequest(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"seed"}}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2, Tags: []string{"seed"}}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}
	if err := db.Set(3, &UniqueTestRec{Email: "c@x", Code: 3, Tags: []string{"seed"}}); err != nil {
		t.Fatalf("Set(3): %v", err)
	}

	badReq := mustBuildPatchAutoReq(t, db, 3, []Field{{Name: "email", Value: "b@x"}}, true, nil, nil, nil)
	goodReq := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "tags", Value: []string{"ok"}}}, true, nil, nil, nil)

	db.executeAutoBatch([]*autoBatchRequest[uint64, UniqueTestRec]{badReq, goodReq})

	if err := mustAutoBatchErr(t, badReq); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bad request error = %v, want ErrUniqueViolation", err)
	}
	if err := mustAutoBatchErr(t, goodReq); err != nil {
		t.Fatalf("good request error = %v", err)
	}
	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || !slices.Equal(got.Tags, []string{"ok"}) {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
	if got, err := db.Get(3); err != nil {
		t.Fatalf("Get(3): %v", err)
	} else if got == nil || got.Email != "c@x" {
		t.Fatalf("id=3 changed after unique reject: %#v", got)
	}
}

func TestAutoBatchExt_RepeatedPatchSameID_UniqueDB_FirstBeforeCommitError_RetriesFollowerFromOriginalState(t *testing.T) {
	db, _ := openTempDBUint64Unique(t)

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1, Tags: []string{"base"}}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	cbErr := errors.New("first callback failed")
	var seenOldTags [][]string
	req1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "tags", Value: []string{"mid"}}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, _ *UniqueTestRec, _ *UniqueTestRec) error { return cbErr },
		},
	)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "tags", Value: []string{"tail"}}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, _ *UniqueTestRec) error {
				if oldValue != nil {
					seenOldTags = append(seenOldTags, append([]string(nil), oldValue.Tags...))
				}
				return nil
			},
		},
	)

	batch := popQueuedSingleReqBatch(t, db, req1, req2)
	if len(batch) != 2 {
		t.Fatalf("popped batch size = %d, want 2", len(batch))
	}
	db.executeAutoBatchJobs(batch)

	if err := mustAutoBatchErr(t, req1); !errors.Is(err, cbErr) {
		t.Fatalf("req1 error = %v, want %v", err, cbErr)
	}
	if err := mustAutoBatchErr(t, req2); err != nil {
		t.Fatalf("req2 error = %v", err)
	}
	if len(seenOldTags) != 1 || !slices.Equal(seenOldTags[0], []string{"base"}) {
		t.Fatalf("req2 old tags = %v, want [[base]]", seenOldTags)
	}
	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || !slices.Equal(got.Tags, []string{"tail"}) {
		t.Fatalf("unexpected id=1 value: %#v", got)
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
					ok, evalErr := evalExprBool(item, q.Expr)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Expr))
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

func TestAutoBatchExt_New_CoalescedSetDeleteUnique_MatchesCollapsedSequentialModel(t *testing.T) {
	dbBatch, _ := openTempDBUint64Unique(t)
	dbSeq, _ := openTempDBUint64Unique(t, Options{AutoBatchMax: 1})

	cloneRec := func(v *UniqueTestRec) *UniqueTestRec {
		if v == nil {
			return nil
		}
		cp := *v
		cp.Tags = slices.Clone(v.Tags)
		if v.Opt != nil {
			s := *v.Opt
			cp.Opt = &s
		}
		return &cp
	}
	sameRec := func(a, b *UniqueTestRec) bool {
		switch {
		case a == nil || b == nil:
			return a == b
		case a.Email != b.Email || a.Code != b.Code:
			return false
		case (a.Opt == nil) != (b.Opt == nil):
			return false
		case a.Opt != nil && *a.Opt != *b.Opt:
			return false
		default:
			return slices.Equal(a.Tags, b.Tags)
		}
	}
	errClass := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case errors.Is(err, ErrUniqueViolation):
			return "unique"
		default:
			return err.Error()
		}
	}
	compareState := func(step int, emailPool []string, codeMax int) {
		t.Helper()

		cntBatch, err := dbBatch.Count(nil)
		if err != nil {
			t.Fatalf("step=%d batch Count: %v", step, err)
		}
		cntSeq, err := dbSeq.Count(nil)
		if err != nil {
			t.Fatalf("step=%d seq Count: %v", step, err)
		}
		if cntBatch != cntSeq {
			t.Fatalf("step=%d count mismatch: batch=%d seq=%d", step, cntBatch, cntSeq)
		}

		for id := uint64(1); id <= 6; id++ {
			vb, err := dbBatch.Get(id)
			if err != nil {
				t.Fatalf("step=%d batch Get(%d): %v", step, id, err)
			}
			vs, err := dbSeq.Get(id)
			if err != nil {
				t.Fatalf("step=%d seq Get(%d): %v", step, id, err)
			}
			if !sameRec(vb, vs) {
				t.Fatalf("step=%d id=%d value mismatch\nbatch=%#v\nseq=%#v", step, id, vb, vs)
			}
		}

		for _, email := range emailPool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("email", email)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(email=%q): %v", step, email, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("email", email)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(email=%q): %v", step, email, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d email index mismatch for %q: batch=%v seq=%v", step, email, ib, is)
			}
		}
		for code := 1; code <= codeMax; code++ {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("code", code)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(code=%d): %v", step, code, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("code", code)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(code=%d): %v", step, code, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d code index mismatch for %d: batch=%v seq=%v", step, code, ib, is)
			}
		}
	}

	seed := map[uint64]*UniqueTestRec{
		1: {Email: "seed-1@x", Code: 1, Tags: []string{"seed-1"}},
		2: {Email: "seed-2@x", Code: 2, Tags: []string{"seed-2"}},
		3: {Email: "seed-3@x", Code: 3, Tags: []string{"seed-3"}},
		4: {Email: "seed-4@x", Code: 4, Tags: []string{"seed-4"}},
	}
	for id, rec := range seed {
		if err := dbBatch.Set(id, cloneRec(rec)); err != nil {
			t.Fatalf("seed batch Set(%d): %v", id, err)
		}
		if err := dbSeq.Set(id, cloneRec(rec)); err != nil {
			t.Fatalf("seed seq Set(%d): %v", id, err)
		}
	}

	emailPool := []string{
		"seed-1@x",
		"seed-2@x",
		"seed-3@x",
		"seed-4@x",
		"u0@x",
		"u1@x",
		"u2@x",
		"u3@x",
		"u4@x",
		"u5@x",
	}
	const codeMax = 8

	type spec struct {
		op  autoBatchOp
		id  uint64
		val *UniqueTestRec
	}

	r := newRand(20260324)
	for step := 0; step < 160; step++ {
		n := 3 + r.IntN(5)
		specs := make([]spec, n)
		reqs := make([]*autoBatchRequest[uint64, UniqueTestRec], n)
		last := make(map[uint64]int, n)

		for i := 0; i < n; i++ {
			id := uint64(1 + r.IntN(6))
			if r.IntN(4) == 0 {
				specs[i] = spec{op: autoBatchDelete, id: id}
				reqs[i] = mustBuildDeleteAutoReq(t, dbBatch, id, nil)
			} else {
				rec := &UniqueTestRec{
					Email: emailPool[r.IntN(len(emailPool))],
					Code:  1 + r.IntN(codeMax),
					Tags: []string{
						fmt.Sprintf("tg-%d", r.IntN(3)),
					},
				}
				specs[i] = spec{op: autoBatchSet, id: id, val: rec}
				reqs[i] = mustBuildSetAutoReq(t, dbBatch, id, cloneRec(rec), nil, nil, nil)
			}
			last[id] = i
		}

		batch := popQueuedSingleReqBatch(t, dbBatch, reqs...)
		if len(batch) != len(reqs) {
			t.Fatalf("step=%d popped batch size=%d want=%d", step, len(batch), len(reqs))
		}
		dbBatch.executeAutoBatchJobs(batch)

		gotErrs := make([]error, len(reqs))
		for i, req := range reqs {
			gotErrs[i] = mustAutoBatchErr(t, req)
		}

		wantErrs := make([]error, len(reqs))
		for i, s := range specs {
			if last[s.id] != i {
				continue
			}
			switch s.op {
			case autoBatchSet:
				wantErrs[i] = dbSeq.Set(s.id, cloneRec(s.val))
			case autoBatchDelete:
				wantErrs[i] = dbSeq.Delete(s.id)
			default:
				t.Fatalf("step=%d unknown spec op=%v", step, s.op)
			}
		}

		for i, s := range specs {
			want := wantErrs[last[s.id]]
			if gotClass, wantClass := errClass(gotErrs[i]), errClass(want); gotClass != wantClass {
				t.Fatalf(
					"step=%d req=%d id=%d op=%v error mismatch: got=%v want=%v",
					step,
					i,
					s.id,
					s.op,
					gotErrs[i],
					want,
				)
			}
		}

		compareState(step, emailPool, codeMax)
	}
}

func TestAutoBatchExt_New_SharedPatchRetry_MatchesSequentialModel(t *testing.T) {
	dbBatch, _ := openTempDBUint64(t)
	dbSeq, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})

	clonePatch := func(in []Field) []Field {
		out := make([]Field, len(in))
		for i := range in {
			out[i].Name = in[i].Name
			switch v := in[i].Value.(type) {
			case []string:
				out[i].Value = slices.Clone(v)
			default:
				out[i].Value = v
			}
		}
		return out
	}
	sameRec := func(a, b *Rec) bool {
		switch {
		case a == nil || b == nil:
			return a == b
		default:
			return a.Name == b.Name &&
				a.Email == b.Email &&
				a.Age == b.Age &&
				a.Score == b.Score &&
				a.Active == b.Active &&
				a.Country == b.Country &&
				a.FullName == b.FullName &&
				slices.Equal(a.Tags, b.Tags)
		}
	}
	errClass := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case strings.Contains(err.Error(), "callback failed"):
			return "callback"
		default:
			return err.Error()
		}
	}

	namePool := map[string]struct{}{}
	countryPool := map[string]struct{}{}
	tagPool := map[string]struct{}{}

	rememberTags := func(tags []string) {
		for _, tag := range tags {
			tagPool[tag] = struct{}{}
		}
	}
	compareState := func(step int) {
		t.Helper()

		cntBatch, err := dbBatch.Count(nil)
		if err != nil {
			t.Fatalf("step=%d batch Count: %v", step, err)
		}
		cntSeq, err := dbSeq.Count(nil)
		if err != nil {
			t.Fatalf("step=%d seq Count: %v", step, err)
		}
		if cntBatch != cntSeq {
			t.Fatalf("step=%d count mismatch: batch=%d seq=%d", step, cntBatch, cntSeq)
		}

		for id := uint64(1); id <= 4; id++ {
			vb, err := dbBatch.Get(id)
			if err != nil {
				t.Fatalf("step=%d batch Get(%d): %v", step, id, err)
			}
			vs, err := dbSeq.Get(id)
			if err != nil {
				t.Fatalf("step=%d seq Get(%d): %v", step, id, err)
			}
			if !sameRec(vb, vs) {
				t.Fatalf("step=%d id=%d value mismatch\nbatch=%#v\nseq=%#v", step, id, vb, vs)
			}
		}

		for name := range namePool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("name", name)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(name=%q): %v", step, name, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("name", name)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(name=%q): %v", step, name, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d name index mismatch for %q: batch=%v seq=%v", step, name, ib, is)
			}
		}
		for country := range countryPool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("country", country)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(country=%q): %v", step, country, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("country", country)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(country=%q): %v", step, country, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d country index mismatch for %q: batch=%v seq=%v", step, country, ib, is)
			}
		}
		for tag := range tagPool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.HASANY("tags", []string{tag})))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(tag=%q): %v", step, tag, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.HASANY("tags", []string{tag})))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(tag=%q): %v", step, tag, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d tag index mismatch for %q: batch=%v seq=%v", step, tag, ib, is)
			}
		}
	}

	seed := map[uint64]*Rec{
		1: {Name: "seed-1", Age: 21, Score: 1.5, Tags: []string{"seed-1"}, FullName: "full-1", Meta: Meta{Country: "NL"}},
		2: {Name: "seed-2", Age: 22, Score: 2.5, Tags: []string{"seed-2"}, FullName: "full-2", Meta: Meta{Country: "DE"}},
		3: {Name: "seed-3", Age: 23, Score: 3.5, Tags: []string{"seed-3"}, FullName: "full-3", Meta: Meta{Country: "PL"}},
		4: {Name: "seed-4", Age: 24, Score: 4.5, Tags: []string{"seed-4"}, FullName: "full-4", Meta: Meta{Country: "ES"}},
	}
	for id, rec := range seed {
		cpBatch := *rec
		cpBatch.Tags = slices.Clone(rec.Tags)
		if err := dbBatch.Set(id, &cpBatch); err != nil {
			t.Fatalf("seed batch Set(%d): %v", id, err)
		}
		cpSeq := *rec
		cpSeq.Tags = slices.Clone(rec.Tags)
		if err := dbSeq.Set(id, &cpSeq); err != nil {
			t.Fatalf("seed seq Set(%d): %v", id, err)
		}
		namePool[rec.Name] = struct{}{}
		countryPool[rec.Country] = struct{}{}
		rememberTags(rec.Tags)
	}

	type spec struct {
		id         uint64
		patch      []Field
		useBP      bool
		bpName     string
		useBS      bool
		bsTag      string
		failCommit bool
	}

	countries := []string{"NL", "DE", "PL", "ES", "SE", "US"}
	r := newRand(20260325)
	for step := 0; step < 140; step++ {
		n := 3 + r.IntN(4)
		failIdx := -1
		if r.IntN(3) == 0 {
			failIdx = r.IntN(n)
		}

		specs := make([]spec, n)
		reqs := make([]*autoBatchRequest[uint64, Rec], n)

		for i := 0; i < n; i++ {
			id := uint64(1 + r.IntN(4))
			var patch []Field
			switch r.IntN(4) {
			case 0:
				patch = []Field{{Name: "age", Value: float64(20 + r.IntN(50))}}
			case 1:
				patch = []Field{{Name: "score", Value: float64(10*r.IntN(10) + 5)}}
			case 2:
				country := countries[r.IntN(len(countries))]
				countryPool[country] = struct{}{}
				patch = []Field{{Name: "country", Value: country}}
			default:
				tags := []string{
					fmt.Sprintf("pt-%d", r.IntN(4)),
					fmt.Sprintf("pt-%d", r.IntN(4)),
				}
				rememberTags(tags)
				patch = []Field{{Name: "tags", Value: tags}}
			}

			specs[i] = spec{
				id:         id,
				patch:      patch,
				useBP:      r.IntN(2) == 0,
				bpName:     fmt.Sprintf("bp-%d-%d", step, i),
				useBS:      r.IntN(2) == 0,
				bsTag:      fmt.Sprintf("bs-%d-%d", step, i),
				failCommit: i == failIdx,
			}
			if specs[i].useBP {
				namePool[specs[i].bpName] = struct{}{}
			}
			if specs[i].useBS {
				tagPool[specs[i].bsTag] = struct{}{}
			}

			var beforeProcess []beforeProcessFunc[uint64, Rec]
			if specs[i].useBP {
				name := specs[i].bpName
				beforeProcess = []beforeProcessFunc[uint64, Rec]{
					func(_ uint64, v *Rec) error {
						v.Name = name
						return nil
					},
				}
			}
			var beforeStore []beforeStoreFunc[uint64, Rec]
			if specs[i].useBS {
				tag := specs[i].bsTag
				beforeStore = []beforeStoreFunc[uint64, Rec]{
					func(_ uint64, _ *Rec, v *Rec) error {
						v.Tags = append(v.Tags, tag)
						return nil
					},
				}
			}
			var beforeCommit []beforeCommitFunc[uint64, Rec]
			if specs[i].failCommit {
				beforeCommit = []beforeCommitFunc[uint64, Rec]{
					func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
						return fmt.Errorf("callback failed step=%d req=%d", step, i)
					},
				}
			}

			reqs[i] = mustBuildPatchAutoReq(t, dbBatch, id, clonePatch(patch), true, beforeProcess, beforeStore, beforeCommit)
		}

		batch := popQueuedSingleReqBatch(t, dbBatch, reqs...)
		if len(batch) != len(reqs) {
			t.Fatalf("step=%d popped batch size=%d want=%d", step, len(batch), len(reqs))
		}
		dbBatch.executeAutoBatchJobs(batch)

		gotErrs := make([]error, len(reqs))
		for i, req := range reqs {
			gotErrs[i] = mustAutoBatchErr(t, req)
		}

		for i, s := range specs {
			var opts []ExecOption[uint64, Rec]
			if s.useBP {
				name := s.bpName
				opts = append(opts, BeforeProcess(func(_ uint64, v *Rec) error {
					v.Name = name
					return nil
				}))
			}
			if s.useBS {
				tag := s.bsTag
				opts = append(opts, BeforeStore(func(_ uint64, _ *Rec, v *Rec) error {
					v.Tags = append(v.Tags, tag)
					return nil
				}))
			}
			if s.failCommit {
				opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
					return fmt.Errorf("callback failed step=%d req=%d", step, i)
				}))
			}

			wantErr := dbSeq.Patch(s.id, clonePatch(s.patch), opts...)
			if gotClass, wantClass := errClass(gotErrs[i]), errClass(wantErr); gotClass != wantClass {
				t.Fatalf(
					"step=%d req=%d id=%d error mismatch: got=%v want=%v patch=%v",
					step,
					i,
					s.id,
					gotErrs[i],
					wantErr,
					s.patch,
				)
			}
		}

		compareState(step)
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
					ok, evalErr := evalExprBool(item, q.Expr)
					if evalErr != nil {
						reportErr(fmt.Errorf("eval: %w", evalErr))
						return
					}
					if !ok {
						reportErr(fmt.Errorf("query returned inconsistent item %#v for %v", item, q.Expr))
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
