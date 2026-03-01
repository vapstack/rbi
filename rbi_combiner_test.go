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

func TestBatch_PreCommit_CallbacksRunForSetPatchDelete(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{BatchAllowCallbacks: true})

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

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}, cb); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Patch(1, []Field{{Name: "name", Value: "bob"}}, cb); err != nil {
		t.Fatalf("Patch: %v", err)
	}
	if err := db.Delete(1, cb); err != nil {
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

	bs := db.BatchStats()
	if bs.CallbackOps < 3 {
		t.Fatalf("expected at least 3 callback ops in combiner stats, got %d", bs.CallbackOps)
	}
}

func TestBatch_SequentialSet_DoesNotProduceCombinedBatches(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 64,
	})

	before := db.BatchStats()
	for i := 1; i <= 64; i++ {
		if err := db.Set(uint64(i), &Rec{
			Name: fmt.Sprintf("seq-%03d", i),
			Age:  18 + (i % 50),
		}); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	after := db.BatchStats()

	enqueuedDelta := after.Enqueued - before.Enqueued
	if enqueuedDelta != 64 {
		t.Fatalf("expected all sequential Set writes to be enqueued, delta=%d before=%+v after=%+v", enqueuedDelta, before, after)
	}
	if after.CombinedBatches != before.CombinedBatches {
		t.Fatalf("expected no multi-request combined batches for sequential Set calls, before=%+v after=%+v", before, after)
	}
	if after.MaxBatchSeen > 1 {
		t.Fatalf("expected max seen batch size to stay 1 for sequential Set calls, stats=%+v", after)
	}
}

func TestBatch_DisabledCombiner_SkipsCombinerPath(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow: 5 * time.Millisecond,
		BatchMax:    1,
	})

	before := db.BatchStats()
	if before.Enabled {
		t.Fatalf("expected combiner to be disabled")
	}

	if err := db.Set(1, &Rec{Name: "alice", Age: 10}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	after := db.BatchStats()
	if after.Submitted != before.Submitted || after.Enqueued != before.Enqueued || after.Dequeued != before.Dequeued {
		t.Fatalf("expected disabled combiner to skip queue path, before=%+v after=%+v", before, after)
	}
	if after.FallbackDisabled != before.FallbackDisabled {
		t.Fatalf("expected no disabled-fallback accounting when combiner is fully off, before=%+v after=%+v", before, after)
	}
}

func TestBatch_CallbacksBypassCombinerWhenDisabled(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 64,
	})

	before := db.BatchStats()
	calls := 0
	err := db.Set(1, &Rec{Name: "alice", Age: 10}, func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected callback to run exactly once, got %d", calls)
	}

	after := db.BatchStats()
	if after.Enqueued != before.Enqueued {
		t.Fatalf("expected callback write to bypass combiner queue, before=%+v after=%+v", before, after)
	}
	if after.FallbackCallbacks <= before.FallbackCallbacks {
		t.Fatalf("expected fallback-callback counter increment, before=%+v after=%+v", before, after)
	}
	if after.CallbackOps != before.CallbackOps {
		t.Fatalf("expected no combiner callback executions on bypass path, before=%+v after=%+v", before, after)
	}
}

func TestBatch_CallbackBarrier_RespectsBatchAllowCallbacks(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 64,
	})

	cb := func(*bbolt.Tx, uint64, *Rec, *Rec) error { return nil }
	makeReq := func(id uint64, withCB bool) *combineRequest[uint64, Rec] {
		req := &combineRequest[uint64, Rec]{op: combineSet, id: id}
		if withCB {
			req.fns = []PreCommitFunc[uint64, Rec]{cb}
		}
		return req
	}

	loadQueue := func(allow bool) {
		db.combiner.mu.Lock()
		db.combiner.allowCallbacks = allow
		db.combiner.window = 0
		db.combiner.maxOps = 16
		db.combiner.running = true
		db.combiner.queue = []*combineRequest[uint64, Rec]{
			makeReq(1, false),
			makeReq(2, true),
			makeReq(3, false),
		}
		db.combiner.mu.Unlock()
	}

	loadQueue(false)
	batch := db.popCombinedBatch()
	if len(batch) != 1 {
		t.Fatalf("expected callback barrier to cut batch to 1 when disabled, got %d", len(batch))
	}
	if len(batch[0].fns) != 0 {
		t.Fatalf("expected first request without callbacks, got callbacks=%d", len(batch[0].fns))
	}

	loadQueue(true)
	batch = db.popCombinedBatch()
	if len(batch) != 3 {
		t.Fatalf("expected callbacks to be batchable when enabled, got %d", len(batch))
	}
	if len(batch[1].fns) == 0 {
		t.Fatalf("expected middle request with callback to stay in combined batch")
	}
}

func TestBatch_PatchUnique_QueuedIntoCombiner(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 256,
	})

	if err := db.Set(1, &UniqueTestRec{Email: "a@x", Code: 1}); err != nil {
		t.Fatalf("Set(1): %v", err)
	}
	if err := db.Set(2, &UniqueTestRec{Email: "b@x", Code: 2}); err != nil {
		t.Fatalf("Set(2): %v", err)
	}

	before := db.BatchStats()
	if err := db.Patch(1, []Field{{Name: "email", Value: "c@x"}}); err != nil {
		t.Fatalf("Patch unique field should use combiner path: %v", err)
	}
	mid := db.BatchStats()
	if mid.Enqueued <= before.Enqueued {
		t.Fatalf("expected patch to be enqueued into combiner, before=%+v after=%+v", before, mid)
	}
	if mid.FallbackPatchUnique != before.FallbackPatchUnique {
		t.Fatalf("unique-patch fallback should not trigger, before=%+v after=%+v", before, mid)
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

func TestBatch_SetDeleteSameID_CoalescedToLastWrite(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 64,
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

	req1 := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         1,
		setValue:   setA,
		setPayload: encodePayload(setA),
		done:       make(chan error, 1),
	}
	req2 := &combineRequest[uint64, Rec]{
		op:   combineDelete,
		id:   1,
		done: make(chan error, 1),
	}
	req3 := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         1,
		setValue:   setB,
		setPayload: encodePayload(setB),
		done:       make(chan error, 1),
	}
	req4 := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         2,
		setValue:   setOther,
		setPayload: encodePayload(setOther),
		done:       make(chan error, 1),
	}

	db.combiner.mu.Lock()
	db.combiner.window = 0
	db.combiner.maxOps = 16
	db.combiner.running = true
	db.combiner.queue = []*combineRequest[uint64, Rec]{req1, req2, req3, req4}
	db.combiner.mu.Unlock()

	batch := db.popCombinedBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.coalescedTo != req2 || req2.coalescedTo != req3 || req3.coalescedTo != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.coalescedTo, req2.coalescedTo, req3.coalescedTo)
	}

	db.executeCombinedBatch(batch)

	for i, req := range []*combineRequest[uint64, Rec]{req1, req2, req3, req4} {
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

	if st := db.BatchStats(); st.CoalescedSetDelete < 2 {
		t.Fatalf("expected at least 2 coalesced set/delete ops, got %d (stats=%+v)", st.CoalescedSetDelete, st)
	}
}

func TestBatch_CoalescedChain_PropagatesTerminalError(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 64,
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

	req1 := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         1,
		setValue:   &UniqueTestRec{Email: "mid@x", Code: 10},
		setPayload: encode(&UniqueTestRec{Email: "mid@x", Code: 10}),
		done:       make(chan error, 1),
	}
	req2 := &combineRequest[uint64, UniqueTestRec]{
		op:   combineDelete,
		id:   1,
		done: make(chan error, 1),
	}
	req3 := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         1,
		setValue:   &UniqueTestRec{Email: "taken@x", Code: 11},
		setPayload: encode(&UniqueTestRec{Email: "taken@x", Code: 11}),
		done:       make(chan error, 1),
	}
	req4 := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         3,
		setValue:   &UniqueTestRec{Email: "ok@x", Code: 3},
		setPayload: encode(&UniqueTestRec{Email: "ok@x", Code: 3}),
		done:       make(chan error, 1),
	}

	db.combiner.mu.Lock()
	db.combiner.window = 0
	db.combiner.maxOps = 16
	db.combiner.running = true
	db.combiner.queue = []*combineRequest[uint64, UniqueTestRec]{req1, req2, req3, req4}
	db.combiner.mu.Unlock()

	batch := db.popCombinedBatch()
	if len(batch) != 4 {
		t.Fatalf("unexpected popped batch size: got=%d want=4", len(batch))
	}
	if req1.coalescedTo != req2 || req2.coalescedTo != req3 || req3.coalescedTo != nil {
		t.Fatalf("unexpected coalesce chain: req1->%p req2->%p req3->%p", req1.coalescedTo, req2.coalescedTo, req3.coalescedTo)
	}

	db.executeCombinedBatch(batch)

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

func TestBatch_CombinedPatchFailures_IsolateFailedRequest(t *testing.T) {
	type tc struct {
		name       string
		makeBadReq func() *combineRequest[uint64, Rec]
		checkErr   func(error) bool
	}

	cases := []tc{
		{
			name: "missing_record",
			makeBadReq: func() *combineRequest[uint64, Rec] {
				return &combineRequest[uint64, Rec]{
					op:                 combinePatch,
					id:                 999,
					patch:              []Field{{Name: "age", Value: 77}},
					patchIgnoreUnknown: true,
					patchAllowMissing:  false,
					done:               make(chan error, 1),
				}
			},
			checkErr: func(err error) bool { return errors.Is(err, ErrRecordNotFound) },
		},
		{
			name: "invalid_patch",
			makeBadReq: func() *combineRequest[uint64, Rec] {
				return &combineRequest[uint64, Rec]{
					op:                 combinePatch,
					id:                 1,
					patch:              []Field{{Name: "age", Value: "not-an-int"}},
					patchIgnoreUnknown: true,
					patchAllowMissing:  false,
					done:               make(chan error, 1),
				}
			},
			checkErr: func(err error) bool { return err != nil && strings.Contains(err.Error(), "failed to apply patch") },
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db, _ := openTempDBUint64(t, &Options{
				BatchWindow:   5 * time.Millisecond,
				BatchMax:      16,
				BatchMaxQueue: 64,
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
			goodReq := &combineRequest[uint64, Rec]{
				op:         combineSet,
				id:         2,
				setValue:   goodVal,
				setPayload: encode(goodVal),
				done:       make(chan error, 1),
			}

			db.executeCombinedBatch([]*combineRequest[uint64, Rec]{badReq, goodReq})

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

func TestBatch_QueueFullFallback_UsesDirectPath(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{
		BatchWindow:   5 * time.Millisecond,
		BatchMax:      16,
		BatchMaxQueue: 1,
	})

	if st := db.BatchStats(); !st.Enabled {
		t.Fatalf("expected combiner enabled")
	}

	db.combiner.mu.Lock()
	db.combiner.queue = []*combineRequest[uint64, Rec]{
		{op: combineSet, id: 123, done: make(chan error, 1)},
	}
	db.combiner.running = false
	db.combiner.mu.Unlock()
	defer func() {
		db.combiner.mu.Lock()
		db.combiner.queue = nil
		db.combiner.running = false
		db.combiner.mu.Unlock()
	}()

	before := db.BatchStats()
	if err := db.Set(1, &Rec{Name: "fallback", Age: 42, Meta: Meta{Country: "US"}}); err != nil {
		t.Fatalf("Set must succeed via direct fallback path, got: %v", err)
	}
	after := db.BatchStats()

	if after.FallbackQueueFull <= before.FallbackQueueFull {
		t.Fatalf("expected queue-full fallback accounting increment, before=%+v after=%+v", before, after)
	}
	if after.Enqueued != before.Enqueued {
		t.Fatalf("queue-full fallback must bypass queue enqueue, before=%+v after=%+v", before, after)
	}

	got, err := db.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil || got.Name != "fallback" || got.Age != 42 || got.Country != "US" {
		t.Fatalf("unexpected value after queue-full direct fallback write: %#v", got)
	}
}

func TestBatch_PreCommitError_RollsBack(t *testing.T) {
	db, _ := openTempDBUint64(t, nil)

	wantErr := errors.New("precommit failed")
	err := db.Set(1, &Rec{Name: "alice", Age: 10}, func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected precommit error %v, got %v", wantErr, err)
	}

	got, gerr := db.Get(1)
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != nil {
		t.Fatalf("expected rollback on precommit error, got %#v", got)
	}
}

func TestBatch_PreCommitError_IsolatesFailedRequest_WhenCallbacksAllowed(t *testing.T) {
	db, _ := openTempDBUint64(t, &Options{BatchAllowCallbacks: true})

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

	badReq := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         1,
		setValue:   badVal,
		setPayload: encode(badVal),
		fns: []PreCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *Rec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &combineRequest[uint64, Rec]{
		op:         combineSet,
		id:         2,
		setValue:   goodVal,
		setPayload: encode(goodVal),
		done:       make(chan error, 1),
	}

	db.executeCombinedBatch([]*combineRequest[uint64, Rec]{badReq, goodReq})

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

func TestBatch_PreCommitError_Isolation_RechecksUniqueAfterRetry(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, &Options{BatchAllowCallbacks: true})

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

	badReq := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         1,
		setValue:   badVal,
		setPayload: encode(badVal),
		fns: []PreCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, _, _ *UniqueTestRec) error { return cbErr },
		},
		done: make(chan error, 1),
	}
	goodReq := &combineRequest[uint64, UniqueTestRec]{
		op:         combineSet,
		id:         2,
		setValue:   goodVal,
		setPayload: encode(goodVal),
		done:       make(chan error, 1),
	}

	db.executeCombinedBatch([]*combineRequest[uint64, UniqueTestRec]{badReq, goodReq})

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

func keysOfMap[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
