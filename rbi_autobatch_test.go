package rbi

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func queuedSingleJob[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) *autoBatchJob[K, V] {
	return &autoBatchJob[K, V]{
		reqs: []*autoBatchRequest[K, V]{req},
		done: req.done,
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

func TestBatch_StatsTrackBatchSizeDistribution(t *testing.T) {
	db, _ := openTempDBUint64(t)

	encodePayload := func(v *Rec) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	makeSetReq := func(id uint64, name string) *autoBatchRequest[uint64, Rec] {
		rec := &Rec{Name: name, Age: int(id)}
		return &autoBatchRequest[uint64, Rec]{
			op:         autoBatchSet,
			id:         id,
			setValue:   rec,
			setPayload: encodePayload(rec),
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
	if got := sharedDB.autoBatchOpName([]*autoBatchRequest[uint64, Rec]{makeReq(autoBatchSet)}, false); got != "batch" {
		t.Fatalf("shared single request op = %q, want batch", got)
	}
	if got := sharedDB.autoBatchOpName([]*autoBatchRequest[uint64, Rec]{makeReq(autoBatchSet)}, true); got != "set" {
		t.Fatalf("isolated single request op = %q, want set", got)
	}
	if got := sharedDB.autoBatchOpName([]*autoBatchRequest[uint64, Rec]{makeReq(autoBatchSet), makeReq(autoBatchSet)}, true); got != "batch_set" {
		t.Fatalf("isolated multi set op = %q, want batch_set", got)
	}

	soloDB, _ := openTempDBUint64(t, Options{AutoBatchMax: 1})
	if got := soloDB.autoBatchOpName([]*autoBatchRequest[uint64, Rec]{makeReq(autoBatchPatch)}, false); got != "patch" {
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
	b := getEncodeBuf()
	if err := db.encode(seed, b); err != nil {
		releaseEncodeBuf(b)
		t.Fatalf("encode(seed): %v", err)
	}
	seedReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         100,
		setValue:   seed,
		setPayload: append([]byte(nil), b.Bytes()...),
		done:       make(chan error, 1),
	}
	releaseEncodeBuf(b)

	before := db.AutoBatchStats()
	db.autoBatcher.mu.Lock()
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{queuedSingleJob(seedReq)}
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{
		{
			reqs:     []*autoBatchRequest[uint64, Rec]{isolatedReq},
			isolated: true,
			done:     isolatedReq.done,
		},
		queuedSingleJob(followerReq),
	}
	db.autoBatcher.mu.Unlock()

	before := db.AutoBatchStats()
	batch := db.popAutoBatch()
	after := db.AutoBatchStats()

	if len(batch) != 1 || len(batch[0].reqs) != 1 || batch[0].reqs[0] != isolatedReq {
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{
		queuedSingleJob(headReq),
		{
			reqs:     []*autoBatchRequest[uint64, Rec]{isolatedReq},
			isolated: true,
			done:     isolatedReq.done,
		},
		queuedSingleJob(tailReq),
	}
	db.autoBatcher.mu.Unlock()

	before := db.AutoBatchStats()
	batch := db.popAutoBatch()
	after := db.AutoBatchStats()

	if len(batch) != 1 || len(batch[0].reqs) != 1 || batch[0].reqs[0] != headReq {
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{
		queuedSingleJob(makeReq(1, false)),
		queuedSingleJob(makeReq(2, true)),
		queuedSingleJob(makeReq(3, false)),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 3 {
		t.Fatalf("expected callbacks to remain batchable, got %d", len(batch))
	}
	if len(batch[1].reqs) != 1 || len(batch[1].reqs[0].beforeCommit) == 0 {
		t.Fatalf("expected middle request with callback to stay in a multi-request batch")
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, UniqueTestRec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	}
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, UniqueTestRec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected repeated unique-touching patches to cut batch, got=%d", len(batch))
	}
	if len(batch[0].reqs) != 1 || batch[0].reqs[0] != req1 {
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, UniqueTestRec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected BeforeStore-bearing repeated same-id patch to cut batch on unique DB, got=%d", len(batch))
	}
	if len(batch[0].reqs) != 1 || batch[0].reqs[0] != req1 {
		t.Fatalf("expected first request to stay alone in batch")
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if len(db.autoBatcher.queue) != 2 ||
		len(db.autoBatcher.queue[0].reqs) != 1 || db.autoBatcher.queue[0].reqs[0] != req2 ||
		len(db.autoBatcher.queue[1].reqs) != 1 || db.autoBatcher.queue[1].reqs[0] != req3 {
		t.Fatalf("expected remaining requests to stay queued in order, queue=%+v", db.autoBatcher.queue)
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
	db.autoBatcher.queue = []*autoBatchJob[uint64, UniqueTestRec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 1 {
		t.Fatalf("expected BeforeProcess-bearing repeated same-id patch to cut batch on unique DB, got=%d", len(batch))
	}
	if len(batch[0].reqs) != 1 || batch[0].reqs[0] != req1 {
		t.Fatalf("expected first request to stay alone in batch")
	}

	db.autoBatcher.mu.Lock()
	defer db.autoBatcher.mu.Unlock()
	if len(db.autoBatcher.queue) != 2 ||
		len(db.autoBatcher.queue[0].reqs) != 1 || db.autoBatcher.queue[0].reqs[0] != req2 ||
		len(db.autoBatcher.queue[1].reqs) != 1 || db.autoBatcher.queue[1].reqs[0] != req3 {
		t.Fatalf("expected remaining requests to stay queued in order, queue=%+v", db.autoBatcher.queue)
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

	encodePayload := func(v *Rec) []byte {
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode payload: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	setA := &Rec{Name: "A", Age: 20}
	setB := &Rec{Name: "B", Age: 30}
	setOther := &Rec{Name: "X", Age: 40}

	req1 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   setA,
		setPayload: encodePayload(setA),
		done:       make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, Rec]{
		op:   autoBatchDelete,
		id:   1,
		done: make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   setB,
		setPayload: encodePayload(setB),
		done:       make(chan error, 1),
	}
	req4 := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   setOther,
		setPayload: encodePayload(setOther),
		done:       make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
		queuedSingleJob(req4),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.coalescedTo != req2 || req2.coalescedTo != req3 || req3.coalescedTo != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.coalescedTo, req2.coalescedTo, req3.coalescedTo)
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

	encode := func(v *UniqueTestRec) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	req1 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   &UniqueTestRec{Email: "mid@x", Code: 10},
		setPayload: encode(&UniqueTestRec{Email: "mid@x", Code: 10}),
		done:       make(chan error, 1),
	}
	req2 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:   autoBatchDelete,
		id:   1,
		done: make(chan error, 1),
	}
	req3 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   &UniqueTestRec{Email: "taken@x", Code: 11},
		setPayload: encode(&UniqueTestRec{Email: "taken@x", Code: 11}),
		done:       make(chan error, 1),
	}
	req4 := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         3,
		setValue:   &UniqueTestRec{Email: "ok@x", Code: 3},
		setPayload: encode(&UniqueTestRec{Email: "ok@x", Code: 3}),
		done:       make(chan error, 1),
	}

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.queue = []*autoBatchJob[uint64, UniqueTestRec]{
		queuedSingleJob(req1),
		queuedSingleJob(req2),
		queuedSingleJob(req3),
		queuedSingleJob(req4),
	}
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.coalescedTo != req2 || req2.coalescedTo != req3 || req3.coalescedTo != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.coalescedTo, req2.coalescedTo, req3.coalescedTo)
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

			encode := func(v *Rec) []byte {
				t.Helper()
				b := getEncodeBuf()
				if err := db.encode(v, b); err != nil {
					releaseEncodeBuf(b)
					t.Fatalf("encode: %v", err)
				}
				out := append([]byte(nil), b.Bytes()...)
				releaseEncodeBuf(b)
				return out
			}

			badReq := c.makeBadReq()
			goodVal := &Rec{Name: "good-" + c.name, Age: 99, Meta: Meta{Country: "US"}}
			goodReq := &autoBatchRequest[uint64, Rec]{
				op:         autoBatchSet,
				id:         2,
				setValue:   goodVal,
				setPayload: encode(goodVal),
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
	b := getEncodeBuf()
	if err := db.encode(blocked, b); err != nil {
		releaseEncodeBuf(b)
		t.Fatalf("encode(blocked): %v", err)
	}
	blockedReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         123,
		setValue:   blocked,
		setPayload: append([]byte(nil), b.Bytes()...),
		done:       make(chan error, 1),
	}
	releaseEncodeBuf(b)

	db.autoBatcher.mu.Lock()
	db.autoBatcher.queue = []*autoBatchJob[uint64, Rec]{queuedSingleJob(blockedReq)}
	db.autoBatcher.running = false
	db.autoBatcher.mu.Unlock()
	defer func() {
		db.autoBatcher.mu.Lock()
		db.autoBatcher.queue = nil
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

	encode := func(v *Rec) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	cbErr := errors.New("callback fail")
	badVal := &Rec{Name: "bad", Age: 1}
	goodVal := &Rec{Name: "good", Age: 2}

	badReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   badVal,
		setPayload: encode(badVal),
		beforeCommit: []beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   goodVal,
		setPayload: encode(goodVal),
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

	encode := func(v *UniqueTestRec) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	cbErr := errors.New("callback fail")
	badVal := &UniqueTestRec{Email: "shared@x", Code: 1}
	goodVal := &UniqueTestRec{Email: "shared@x", Code: 2}

	badReq := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   badVal,
		setPayload: encode(badVal),
		beforeCommit: []beforeCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *UniqueTestRec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &autoBatchRequest[uint64, UniqueTestRec]{
		op:         autoBatchSet,
		id:         2,
		setValue:   goodVal,
		setPayload: encode(goodVal),
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

	encode := func(v *Rec) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	beforeStoreCalls := 0
	goodVal := &Rec{Name: "good", Age: 10}
	goodReq := &autoBatchRequest[uint64, Rec]{
		op:         autoBatchSet,
		id:         1,
		setValue:   goodVal,
		setPayload: encode(goodVal),
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
		setPayload: encode(badVal),
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

	encode := func(v *Product) []byte {
		t.Helper()
		b := getEncodeBuf()
		if err := db.encode(v, b); err != nil {
			releaseEncodeBuf(b)
			t.Fatalf("encode: %v", err)
		}
		out := append([]byte(nil), b.Bytes()...)
		releaseEncodeBuf(b)
		return out
	}

	hookErr := errors.New("before store fail")
	req := &autoBatchRequest[string, Product]{
		op:         autoBatchSet,
		id:         "ghost-before-store",
		setValue:   &Product{SKU: "ghost-before-store", Price: 11},
		setPayload: encode(&Product{SKU: "ghost-before-store", Price: 11}),
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

func keysOfMap[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
