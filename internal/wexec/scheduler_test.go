package wexec

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

func TestSchedulerSnapshotReadsQueueCapacityUnderLock(t *testing.T) {
	s := newScheduler(8, 0, 0, true)
	s.queue = make([]*writeJob, 1)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 4096; i++ {
			s.mu.Lock()
			if len(s.queue) >= 64 {
				s.queue = make([]*writeJob, 1)
			} else {
				s.queueLen = len(s.queue)
				s.growQueue()
				s.queueLen = 0
			}
			s.mu.Unlock()
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			_ = s.getStats()
		}
	}
}

func TestWakeWaitersInterruptsMultiJobCoalesceWindow(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:       8,
		Window:       time.Hour,
		StatsEnabled: true,
		Unavailable:  func() error { return nil },
	})
	ex.sched.mu.Lock()
	ex.sched.enqueue(schedulerJob(schedulerReq(1, 0), false))
	ex.sched.enqueue(schedulerJob(schedulerReq(2, 0), false))
	ex.sched.mu.Unlock()

	started := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		ex.sched.mu.Lock()
		close(started)
		ok := ex.sched.waitWindowLocked(2)
		if ok {
			ex.sched.mu.Unlock()
		}
		done <- ok
	}()

	<-started
	ex.WakeWaiters()

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("waitWindowLocked returned false")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("WakeWaiters did not interrupt multi-job coalesce wait")
	}
}

func TestNormalNotifyDoesNotInterruptMultiJobCoalesceWindow(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:       8,
		Window:       time.Hour,
		StatsEnabled: true,
		Unavailable:  func() error { return nil },
	})
	ex.sched.mu.Lock()
	ex.sched.enqueue(schedulerJob(schedulerReq(1, 0), false))
	ex.sched.enqueue(schedulerJob(schedulerReq(2, 0), false))
	ex.sched.mu.Unlock()

	started := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		ex.sched.mu.Lock()
		close(started)
		ok := ex.sched.waitWindowLocked(2)
		if ok {
			ex.sched.mu.Unlock()
		}
		done <- ok
	}()

	<-started
	select {
	case ex.sched.waitNotify <- struct{}{}:
	default:
	}

	select {
	case ok := <-done:
		t.Fatalf("normal notify interrupted multi-job coalesce wait: %v", ok)
	case <-time.After(10 * time.Millisecond):
	}

	ex.WakeWaiters()

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("waitWindowLocked returned false")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("WakeWaiters did not interrupt multi-job coalesce wait")
	}
}

func TestWakeWaitersInterruptsHotWindowCoalesce(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:       8,
		Window:       time.Hour,
		StatsEnabled: true,
		Unavailable:  func() error { return nil },
	})
	ex.sched.mu.Lock()
	ex.sched.hotUntil = time.Now().Add(time.Hour)
	ex.sched.hotBatchSize = 3
	ex.sched.enqueue(schedulerJob(schedulerReq(1, reqRepeatIDSafeShared), false))
	ex.sched.mu.Unlock()

	done := make(chan []*writeJob, 1)
	go func() {
		done <- ex.sched.popBatch(false)
	}()

	select {
	case batch := <-done:
		t.Fatalf("hot-window pop returned before wake: %v", schedulerBatchIDs(batch))
	case <-time.After(10 * time.Millisecond):
	}

	ex.WakeWaiters()

	select {
	case batch := <-done:
		if got := schedulerBatchIDs(batch); !reflect.DeepEqual(got, []uint64{1}) {
			t.Fatalf("batch ids = %v, want [1]", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("WakeWaiters did not interrupt hot-window coalescing")
	}
}

func TestSchedulerMarkSupersededSetDeleteJobsBuildsReplacementChain(t *testing.T) {
	s := newScheduler(8, 0, 0, true)
	req1 := coalescibleReq(1, opSet)
	req2 := coalescibleReq(2, opSet)
	req3 := coalescibleReq(1, opDelete)
	req4 := coalescibleReq(1, opSet)
	batch := []*writeJob{
		{reqs: []*request{req1}},
		{reqs: []*request{req2}},
		{reqs: []*request{req3}},
		{reqs: []*request{req4}},
	}

	s.markSupersededSetDeleteJobs(batch, false)

	if req1.replacedBy != req3 {
		t.Fatalf("req1 replacement = %p, want req3 %p", req1.replacedBy, req3)
	}
	if req3.replacedBy != req4 {
		t.Fatalf("req3 replacement = %p, want req4 %p", req3.replacedBy, req4)
	}
	if req2.replacedBy != nil || req4.replacedBy != nil {
		t.Fatalf("unrelated/latest requests must not be replaced: req2=%p req4=%p", req2.replacedBy, req4.replacedBy)
	}
	if got := s.stats.CoalescedSetDelete.Load(); got != 2 {
		t.Fatalf("coalesced counter = %d, want 2", got)
	}
}

func TestNewStringKeyModeDrivesSchedulerCoalescing(t *testing.T) {
	ex := NewBatcher(Config{
		MaxOps:       8,
		StatsEnabled: true,
		Unavailable:  func() error { return nil },
		StrKey:       true,
		Ops:          &RecordOps{},
	})
	ex.sched.maxOps = 8

	req1 := schedulerStringReq("alpha", reqRepeatIDSafeShared|reqSetDeleteCoalescible)
	req2 := schedulerStringReq("bravo", reqRepeatIDSafeShared|reqSetDeleteCoalescible)
	ex.sched.mu.Lock()
	setSchedulerQueue(&ex.sched, schedulerJob(req1, false), schedulerJob(req2, false))
	ex.sched.mu.Unlock()

	batch := ex.sched.popBatch(ex.strKey)
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}
	if req1.replacedBy != nil || req2.replacedBy != nil {
		t.Fatalf("different string keys were coalesced: req1.replaced=%p req2.replaced=%p", req1.replacedBy, req2.replacedBy)
	}
	if got := ex.sched.stats.CoalescedSetDelete.Load(); got != 0 {
		t.Fatalf("coalesced counter = %d, want 0", got)
	}
}

func TestSchedulerPopBatchStopsBeforeUnsafeRepeatedID(t *testing.T) {
	s := newScheduler(8, 0, 0, false)
	s.maxOps = 8
	req1 := schedulerReq(1, 0)
	req2 := schedulerReq(1, 0)
	req3 := schedulerReq(2, 0)
	s.enqueue(schedulerJob(req1, false))
	s.enqueue(schedulerJob(req2, false))
	s.enqueue(schedulerJob(req3, false))

	batch := s.popBatch(false)
	if len(batch) != 1 || batch[0].reqs[0] != req1 {
		t.Fatalf("first batch = %v, want only first repeated-id request", schedulerBatchIDs(batch))
	}
	next := s.popBatch(false)
	if len(next) != 2 || next[0].reqs[0] != req2 || next[1].reqs[0] != req3 {
		t.Fatalf("second batch = %v, want remaining requests", schedulerBatchIDs(next))
	}
}

func TestSchedulerPopBatchCoalescesSafeRepeatedSetDelete(t *testing.T) {
	s := newScheduler(8, 0, 0, true)
	s.maxOps = 8
	req1 := schedulerReq(1, reqSetDeleteCoalescible)
	req2 := schedulerReq(1, reqSetDeleteCoalescible)
	s.enqueue(schedulerJob(req1, false))
	s.enqueue(schedulerJob(req2, false))

	batch := s.popBatch(false)
	if len(batch) != 2 {
		t.Fatalf("batch size = %d, want 2", len(batch))
	}
	if req1.replacedBy != req2 {
		t.Fatalf("req1 replacement = %p, want req2 %p", req1.replacedBy, req2)
	}
	if got := s.stats.CoalescedSetDelete.Load(); got != 1 {
		t.Fatalf("coalesced counter = %d, want 1", got)
	}
}

func TestSchedulerCallbackRequestsStayBatchable(t *testing.T) {
	s := newScheduler(16, 0, 0, false)
	s.maxOps = 16
	req1 := schedulerReq(1, 0)
	req2 := schedulerReq(2, 0)
	req3 := schedulerReq(3, 0)
	req2.beforeCommit = append(req2.beforeCommit, func(*bbolt.Tx, keycodec.DataKey, unsafe.Pointer, unsafe.Pointer) error { return nil })
	s.enqueue(schedulerJob(req1, false))
	s.enqueue(schedulerJob(req2, false))
	s.enqueue(schedulerJob(req3, false))

	batch := s.popBatch(false)
	if len(batch) != 3 {
		t.Fatalf("batch size = %d, want 3", len(batch))
	}
	if len(batch[1].reqs) != 1 || len(batch[1].reqs[0].beforeCommit) == 0 {
		t.Fatalf("middle callback request was not preserved")
	}
}

func TestSchedulerPopBatchSkipsWindowForIsolatedHead(t *testing.T) {
	s := newScheduler(8, time.Hour, 0, false)
	req1 := schedulerReq(1, 0)
	req2 := schedulerReq(2, 0)
	s.enqueue(schedulerJob(req1, true))
	s.enqueue(schedulerJob(req2, false))

	start := time.Now()
	batch := s.popBatch(false)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("isolated head waited %v", elapsed)
	}
	if len(batch) != 1 || batch[0].reqs[0] != req1 {
		t.Fatalf("batch = %v, want isolated head only", schedulerBatchIDs(batch))
	}
	if !s.hotUntil.IsZero() {
		t.Fatalf("isolated head armed hot window: %v", s.hotUntil)
	}
}

func TestSchedulerPopBatchSkipsWindowBeforeIsolatedFollower(t *testing.T) {
	s := newScheduler(8, time.Hour, 0, false)
	head := schedulerReq(1, 0)
	isolated := schedulerReq(2, 0)
	tail := schedulerReq(3, 0)
	s.enqueue(schedulerJob(head, false))
	s.enqueue(schedulerJob(isolated, true))
	s.enqueue(schedulerJob(tail, false))

	start := time.Now()
	batch := s.popBatch(false)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("head before isolated follower waited %v", elapsed)
	}
	if len(batch) != 1 || batch[0].reqs[0] != head {
		t.Fatalf("batch = %v, want head only", schedulerBatchIDs(batch))
	}
	if !s.hotUntil.IsZero() {
		t.Fatalf("head before isolated follower armed hot window: %v", s.hotUntil)
	}
}

func TestSchedulerSnapshotHotWindowActiveGate(t *testing.T) {
	s := newScheduler(8, 25*time.Millisecond, 0, true)

	s.hotUntil = time.Now().Add(time.Second)
	s.hotBatchSize = 2
	if st := s.getStats(); st.HotWindowActive {
		t.Fatalf("hot window active for 2-request hot batch: %+v", st)
	}

	s.hotBatchSize = 4
	s.window = 0
	if st := s.getStats(); st.HotWindowActive {
		t.Fatalf("hot window active while window disabled: %+v", st)
	}

	s.window = 25 * time.Millisecond
	if st := s.getStats(); !st.HotWindowActive {
		t.Fatalf("hot window inactive for singleton hot wait gate: %+v", st)
	}
}

func TestSchedulerRepeatIDPoolClearsBetweenPops(t *testing.T) {
	s := newScheduler(1024, 0, 0, false)
	s.maxOps = 1024
	s.enqueue(schedulerJob(schedulerReq(1, 0), false))
	s.enqueue(schedulerJob(schedulerReq(2, 0), false))
	if batch := s.popBatch(false); len(batch) != 2 {
		t.Fatalf("first batch size = %d, want 2", len(batch))
	}

	s.running = true
	s.enqueue(schedulerJob(schedulerReq(3, 0), false))
	s.enqueue(schedulerJob(schedulerReq(4, 0), false))
	if batch := s.popBatch(false); len(batch) != 2 {
		t.Fatalf("second batch size = %d, want 2", len(batch))
	}
	if s.queueLen != 0 {
		t.Fatalf("queue len after second pop = %d, want 0", s.queueLen)
	}
}

func TestSchedulerRepeatIDPoolReusableAfterLargeBurst(t *testing.T) {
	s := newScheduler(128, 0, 0, false)
	s.maxOps = 128
	for i := 0; i < 64; i++ {
		s.enqueue(schedulerJob(schedulerReq(uint64(i+1), 0), false))
	}
	if batch := s.popBatch(false); len(batch) != 64 {
		t.Fatalf("burst batch size = %d, want 64", len(batch))
	}

	s.running = true
	s.enqueue(schedulerJob(schedulerReq(1001, 0), false))
	s.enqueue(schedulerJob(schedulerReq(1002, 0), false))
	if batch := s.popBatch(false); len(batch) != 2 {
		t.Fatalf("small batch size after burst = %d, want 2", len(batch))
	}
}

func TestSchedulerRepeatIDPoolStringKeysClearedAfterPop(t *testing.T) {
	s := newScheduler(64, 0, 0, false)
	s.maxOps = 64
	s.enqueue(schedulerJob(schedulerStringReq(strings.Repeat("a", 256), 0), false))
	s.enqueue(schedulerJob(schedulerStringReq(strings.Repeat("b", 256), 0), false))
	if batch := s.popBatch(true); len(batch) != 2 {
		t.Fatalf("first string batch size = %d, want 2", len(batch))
	}

	s.running = true
	s.enqueue(schedulerJob(schedulerStringReq(strings.Repeat("c", 256), 0), false))
	s.enqueue(schedulerJob(schedulerStringReq(strings.Repeat("d", 256), 0), false))
	if batch := s.popBatch(true); len(batch) != 2 {
		t.Fatalf("second string batch size = %d, want 2", len(batch))
	}
}

func TestSchedulerGrowQueuePreservesRingOrder(t *testing.T) {
	s := newScheduler(16, 0, 64, false)
	s.queue = make([]*writeJob, 4)
	s.queueHead = 3
	s.queueLen = 4
	s.queue[3] = schedulerJob(schedulerReq(1, 0), false)
	s.queue[0] = schedulerJob(schedulerReq(2, 0), false)
	s.queue[1] = schedulerJob(schedulerReq(3, 0), false)
	s.queue[2] = schedulerJob(schedulerReq(4, 0), false)

	s.enqueue(schedulerJob(schedulerReq(5, 0), false))

	if len(s.queue) <= 4 {
		t.Fatalf("queue did not grow: len=%d", len(s.queue))
	}
	got := []uint64{
		s.queueAt(0).reqs[0].id.Uint(),
		s.queueAt(1).reqs[0].id.Uint(),
		s.queueAt(2).reqs[0].id.Uint(),
		s.queueAt(3).reqs[0].id.Uint(),
		s.queueAt(4).reqs[0].id.Uint(),
	}
	want := []uint64{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queue order = %v, want %v", got, want)
	}
}

func TestSchedulerGrowQueueCoalescesAcrossWrappedRing(t *testing.T) {
	s := newScheduler(16, 0, 64, false)
	s.maxOps = 16
	req1 := schedulerReq(1, reqSetDeleteCoalescible)
	req2 := schedulerReq(1, reqSetDeleteCoalescible)
	req3 := schedulerReq(2, reqSetDeleteCoalescible)
	req4 := schedulerReq(3, reqSetDeleteCoalescible)
	req5 := schedulerReq(1, reqSetDeleteCoalescible)
	req2.op = opDelete

	s.queue = make([]*writeJob, 4)
	s.queueHead = 3
	s.queueLen = 4
	s.queue[3] = schedulerJob(req1, false)
	s.queue[0] = schedulerJob(req2, false)
	s.queue[1] = schedulerJob(req3, false)
	s.queue[2] = schedulerJob(req4, false)
	s.enqueue(schedulerJob(req5, false))

	batch := s.popBatch(false)
	if len(batch) != 5 {
		t.Fatalf("batch size = %d, want 5", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req5 || req5.replacedBy != nil {
		t.Fatalf("unexpected replacement chain: req1->%p req2->%p req5->%p", req1.replacedBy, req2.replacedBy, req5.replacedBy)
	}
}

func TestSchedulerGroupedJobBetweenSharedRequestsPopSequenceStable(t *testing.T) {
	s := newScheduler(16, 0, 64, false)
	for i := 0; i < 20000; i++ {
		configureSchedulerGroupedBetweenShared(&s)
		assertSchedulerGroupedBetweenSharedPopSequence(t, &s, i)
	}
}

func TestSchedulerGroupedJobBetweenSharedRequestsStableAfterScratchReuse(t *testing.T) {
	s := newScheduler(16, 0, 64, false)
	for i := 0; i < 12000; i++ {
		warmSchedulerBatchScratch(&s)
		configureSchedulerGroupedBetweenShared(&s)
		assertSchedulerGroupedBetweenSharedPopSequence(t, &s, i)
	}
}

func coalescibleReq(id uint64, op Op) *request {
	return &request{
		op:     op,
		id:     keycodec.DataKeyFromUserKey(id, false),
		policy: reqSetDeleteCoalescible,
	}
}

func schedulerReq(id uint64, policy reqPolicy) *request {
	return &request{
		op:     opSet,
		id:     keycodec.DataKeyFromUserKey(id, false),
		policy: policy,
	}
}

func schedulerStringReq(id string, policy reqPolicy) *request {
	return &request{
		op:     opSet,
		id:     keycodec.DataKeyFromUserKey(id, true),
		policy: policy,
	}
}

func schedulerJob(req *request, isolated bool) *writeJob {
	return &writeJob{
		reqs:     []*request{req},
		isolated: isolated,
	}
}

func schedulerGroupedJob(ids ...uint64) *writeJob {
	reqs := make([]*request, len(ids))
	for i, id := range ids {
		reqs[i] = schedulerReq(id, 0)
	}
	return &writeJob{
		reqs:     reqs,
		isolated: true,
		done:     make(chan error, 1),
	}
}

func setSchedulerQueue(s *scheduler, jobs ...*writeJob) {
	s.queueHead = 0
	s.queueLen = len(jobs)
	s.queue = make([]*writeJob, len(jobs))
	copy(s.queue, jobs)
}

func configureSchedulerGroupedBetweenShared(s *scheduler) {
	s.window = 0
	s.maxOps = 16
	s.running = true
	s.hotUntil = time.Time{}
	setSchedulerQueue(
		s,
		schedulerJob(schedulerReq(1, 0), false),
		schedulerGroupedJob(2, 3),
		schedulerJob(schedulerReq(4, 0), false),
	)
}

func warmSchedulerBatchScratch(s *scheduler) {
	s.window = 0
	s.maxOps = 16
	s.running = true
	s.hotUntil = time.Time{}
	setSchedulerQueue(
		s,
		schedulerJob(schedulerReq(10, 0), false),
		schedulerJob(schedulerReq(11, 0), false),
		schedulerJob(schedulerReq(12, 0), false),
	)
	batch := s.popBatch(false)
	recycleSchedulerBatch(s, batch)
}

func assertSchedulerGroupedBetweenSharedPopSequence(tb testing.TB, s *scheduler, iteration int) {
	tb.Helper()

	first := s.popBatch(false)
	if len(first) != 1 || first[0].isolated || len(first[0].reqs) != 1 || first[0].reqs[0].id.Uint() != 1 {
		tb.Fatalf("iter=%d first batch = %v", iteration, schedulerBatchIDs(first))
	}
	recycleSchedulerBatch(s, first)

	second := s.popBatch(false)
	if len(second) != 1 || !second[0].isolated || len(second[0].reqs) != 2 ||
		second[0].reqs[0].id.Uint() != 2 || second[0].reqs[1].id.Uint() != 3 {
		tb.Fatalf("iter=%d second batch = %v", iteration, schedulerBatchIDs(second))
	}
	recycleSchedulerBatch(s, second)

	third := s.popBatch(false)
	if len(third) != 1 || third[0].isolated || len(third[0].reqs) != 1 || third[0].reqs[0].id.Uint() != 4 {
		tb.Fatalf("iter=%d third batch = %v", iteration, schedulerBatchIDs(third))
	}
	recycleSchedulerBatch(s, third)

	if batch := s.popBatch(false); len(batch) != 0 {
		tb.Fatalf("iter=%d trailing batch = %v", iteration, schedulerBatchIDs(batch))
	}
}

func recycleSchedulerBatch(s *scheduler, batch []*writeJob) {
	clear(batch)
	s.mu.Lock()
	s.recycleBatchScratchLocked(batch)
	s.mu.Unlock()
}

func schedulerBatchIDs(batch []*writeJob) []uint64 {
	out := make([]uint64, len(batch))
	for i := range batch {
		out[i] = batch[i].reqs[0].id.Uint()
	}
	return out
}
