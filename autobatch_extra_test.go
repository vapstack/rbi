package rbi

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"go.etcd.io/bbolt"
)

func drainQueuedAutoBatchStep[K ~string | ~uint64, V any](
	tb testing.TB,
	db *DB[K, V],
	reqs []*autoBatchRequest[K, V],
	onBatch func([]*autoBatchJob[K, V]),
) {
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

	for {
		batch := db.popAutoBatch()
		if len(batch) == 0 {
			return
		}
		if onBatch != nil {
			onBatch(batch)
		}
		db.executeAutoBatchJobs(batch)
	}
}

func drainCurrentAutoBatchQueue[K ~string | ~uint64, V any](
	tb testing.TB,
	db *DB[K, V],
	onBatch func([]*autoBatchJob[K, V]),
) {
	tb.Helper()

	for {
		batch := db.popAutoBatch()
		if len(batch) == 0 {
			return
		}
		if onBatch != nil {
			onBatch(batch)
		}
		db.executeAutoBatchJobs(batch)
		clear(batch)
		db.autoBatcher.mu.Lock()
		if cap(batch) > cap(db.autoBatcher.batchScratch) {
			db.autoBatcher.batchScratch = batch[:0]
		} else if db.autoBatcher.batchScratch == nil {
			db.autoBatcher.batchScratch = batch[:0]
		}
		db.autoBatcher.mu.Unlock()
	}
}

func describeAutoBatchJobForTest[K ~string | ~uint64, V any](job *autoBatchJob[K, V]) string {
	if job == nil {
		return "<nil>"
	}
	ids := make([]K, job.reqs.Len())
	for i := 0; i < job.reqs.Len(); i++ {
		ids[i] = job.reqs.Get(i).id
	}
	kind := "shared"
	if job.isolated {
		kind = "isolated"
	}
	return fmt.Sprintf("%s:reqs=%d ids=%v", kind, job.reqs.Len(), ids)
}

func describeAutoBatchBatchForTest[K ~string | ~uint64, V any](batch []*autoBatchJob[K, V]) string {
	if len(batch) == 0 {
		return "[]"
	}
	parts := make([]string, len(batch))
	for i := range batch {
		parts[i] = describeAutoBatchJobForTest(batch[i])
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func describeAutoBatchQueueForTest[K ~string | ~uint64, V any](db *DB[K, V]) string {
	db.autoBatcher.mu.Lock()
	size := db.autoBatcher.queueLen()
	head := db.autoBatcher.queueHead
	parts := make([]string, size)
	for i := 0; i < size; i++ {
		parts[i] = describeAutoBatchJobForTest(db.autoBatcher.queueAt(i))
	}
	db.autoBatcher.mu.Unlock()
	return fmt.Sprintf("head=%d size=%d jobs=[%s]", head, size, strings.Join(parts, ", "))
}

func recycleAutoBatchScratchForTest[K ~string | ~uint64, V any](db *DB[K, V], batch []*autoBatchJob[K, V]) {
	clear(batch)
	db.autoBatcher.mu.Lock()
	if cap(batch) > cap(db.autoBatcher.batchScratch) {
		db.autoBatcher.batchScratch = batch[:0]
	} else if db.autoBatcher.batchScratch == nil {
		db.autoBatcher.batchScratch = batch[:0]
	}
	db.autoBatcher.mu.Unlock()
}

func terminalAutoBatchReq[K ~string | ~uint64, V any](req *autoBatchRequest[K, V]) *autoBatchRequest[K, V] {
	for req != nil && req.replacedBy != nil {
		req = req.replacedBy
	}
	return req
}

func sameAutoBatchExtraErr(got, want error) bool {
	switch {
	case got == nil || want == nil:
		return got == want
	case errors.Is(got, ErrUniqueViolation) || errors.Is(want, ErrUniqueViolation):
		return errors.Is(got, ErrUniqueViolation) && errors.Is(want, ErrUniqueViolation)
	default:
		return got.Error() == want.Error()
	}
}

func waitAutoBatchExtraStats(
	tb testing.TB,
	db interface {
		AutoBatchStats() AutoBatchStats
	},
	desc string,
	ok func(AutoBatchStats) bool,
) AutoBatchStats {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := db.AutoBatchStats()
		if ok(stats) {
			return stats
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timeout waiting for autobatch stats: %s last=%+v", desc, stats)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func readAutoBatchExtraRawValue[K ~string | ~uint64, V any](tb testing.TB, db *DB[K, V], id K) *V {
	tb.Helper()

	var got *V
	err := db.bolt.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(db.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		raw := bucket.Get(db.keyFromID(id))
		if raw == nil {
			return nil
		}
		value, err := db.decode(raw)
		if err != nil {
			return err
		}
		got = value
		return nil
	})
	if err != nil {
		tb.Fatalf("raw read %v: %v", id, err)
	}
	return got
}

type autoBatchExtraStrMapState struct {
	next         uint64
	keyCount     int
	dirty        bool
	snap         *strMapSnapshot
	published    *strMapSnapshot
	pubSource    *strMapSnapshot
	committed    *strMapSnapshot
	committedPub *strMapSnapshot
}

func captureAutoBatchExtraStrMapState[V any](db *DB[string, V]) autoBatchExtraStrMapState {
	db.strmap.Lock()
	defer db.strmap.Unlock()

	return autoBatchExtraStrMapState{
		next:         db.strmap.Next,
		keyCount:     len(db.strmap.Keys),
		dirty:        db.strmap.dirty,
		snap:         db.strmap.snap,
		published:    db.strmap.published,
		pubSource:    db.strmap.pubSource,
		committed:    db.strmap.committed,
		committedPub: db.strmap.committedPub,
	}
}

func waitAutoBatchExtraStrMapHasKeys[V any](tb testing.TB, db *DB[string, V], keys ...string) autoBatchExtraStrMapState {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		db.strmap.Lock()
		ok := true
		for _, key := range keys {
			if _, exists := db.strmap.Keys[key]; !exists {
				ok = false
				break
			}
		}
		state := autoBatchExtraStrMapState{
			next:         db.strmap.Next,
			keyCount:     len(db.strmap.Keys),
			dirty:        db.strmap.dirty,
			snap:         db.strmap.snap,
			published:    db.strmap.published,
			pubSource:    db.strmap.pubSource,
			committed:    db.strmap.committed,
			committedPub: db.strmap.committedPub,
		}
		db.strmap.Unlock()

		if ok {
			return state
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timeout waiting for strmap keys %v last=%+v", keys, state)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func newAutoBatchPopReqForTest(id uint64) *autoBatchRequest[uint64, Rec] {
	return &autoBatchRequest[uint64, Rec]{
		op:   autoBatchPatch,
		id:   id,
		done: make(chan error, 1),
	}
}

func configureGroupedBetweenSharedQueueForTest(
	db *DB[uint64, Rec],
	headID uint64,
	groupIDs [2]uint64,
	tailID uint64,
) {
	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(
		db,
		queuedSingleJob(newAutoBatchPopReqForTest(headID)),
		&autoBatchJob[uint64, Rec]{
			reqs:     testAutoBatchRequestBuf(newAutoBatchPopReqForTest(groupIDs[0]), newAutoBatchPopReqForTest(groupIDs[1])),
			isolated: true,
			done:     make(chan error, 1),
		},
		queuedSingleJob(newAutoBatchPopReqForTest(tailID)),
	)
	db.autoBatcher.mu.Unlock()
}

func assertGroupedBetweenSharedPopSequenceForTest(tb testing.TB, db *DB[uint64, Rec], iteration int) {
	tb.Helper()

	first := db.popAutoBatch()
	if len(first) != 1 || first[0].isolated || first[0].reqs.Len() != 1 || first[0].reqs.Get(0).id != 1 {
		tb.Fatalf(
			"iter=%d pop1=%s queue=%s",
			iteration,
			describeAutoBatchBatchForTest(first),
			describeAutoBatchQueueForTest(db),
		)
	}
	recycleAutoBatchScratchForTest(db, first)

	second := db.popAutoBatch()
	if len(second) != 1 || !second[0].isolated || second[0].reqs.Len() != 2 ||
		second[0].reqs.Get(0).id != 2 || second[0].reqs.Get(1).id != 3 {
		tb.Fatalf(
			"iter=%d pop2=%s queue=%s",
			iteration,
			describeAutoBatchBatchForTest(second),
			describeAutoBatchQueueForTest(db),
		)
	}
	recycleAutoBatchScratchForTest(db, second)

	third := db.popAutoBatch()
	if len(third) != 1 || third[0].isolated || third[0].reqs.Len() != 1 || third[0].reqs.Get(0).id != 4 {
		tb.Fatalf(
			"iter=%d pop3=%s queue=%s",
			iteration,
			describeAutoBatchBatchForTest(third),
			describeAutoBatchQueueForTest(db),
		)
	}
	recycleAutoBatchScratchForTest(db, third)

	empty := db.popAutoBatch()
	if len(empty) != 0 {
		tb.Fatalf(
			"iter=%d trailing-pop=%s queue=%s",
			iteration,
			describeAutoBatchBatchForTest(empty),
			describeAutoBatchQueueForTest(db),
		)
	}
}

func warmAutoBatchScratchForTest(db *DB[uint64, Rec]) {
	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(
		db,
		queuedSingleJob(newAutoBatchPopReqForTest(10)),
		queuedSingleJob(newAutoBatchPopReqForTest(11)),
		queuedSingleJob(newAutoBatchPopReqForTest(12)),
	)
	db.autoBatcher.mu.Unlock()

	batch := db.popAutoBatch()
	recycleAutoBatchScratchForTest(db, batch)
}

func cloneAutoBatchExtraFields(in []Field) []Field {
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

func cloneAutoBatchExtraRec(v *Rec) *Rec {
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

func sameAutoBatchExtraRec(a, b *Rec) bool {
	switch {
	case a == nil || b == nil:
		return a == b
	case a.Name != b.Name || a.Email != b.Email || a.Age != b.Age || a.Score != b.Score:
		return false
	case a.Active != b.Active || a.Country != b.Country || a.FullName != b.FullName:
		return false
	case (a.Opt == nil) != (b.Opt == nil):
		return false
	case a.Opt != nil && *a.Opt != *b.Opt:
		return false
	default:
		return slices.Equal(a.Tags, b.Tags)
	}
}

func cloneAutoBatchExtraUniqueRec(v *UniqueTestRec) *UniqueTestRec {
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

func sameAutoBatchExtraUniqueRec(a, b *UniqueTestRec) bool {
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

type autoBatchExtraRecSpec struct {
	step int
	pos  int
	op   autoBatchOp
	id   uint64

	set   *Rec
	patch []Field

	useBeforeProcess bool
	beforeProcess    string

	useBeforeStore bool
	beforeStoreTag string
	beforeStoreCC  string

	failCommit bool
}

func applyAutoBatchExtraRecSpec(db *DB[uint64, Rec], spec autoBatchExtraRecSpec) error {
	cbMsg := fmt.Sprintf("callback failed step=%d req=%d", spec.step, spec.pos)
	switch spec.op {
	case autoBatchSet:
		var opts []ExecOption[uint64, Rec]
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			cc := spec.beforeStoreCC
			opts = append(opts, BeforeStore(func(_ uint64, _ *Rec, v *Rec) error {
				v.Tags = append(v.Tags, tag)
				v.Country = cc
				return nil
			}))
		}
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Set(spec.id, cloneAutoBatchExtraRec(spec.set), opts...)
	case autoBatchPatch:
		var opts []ExecOption[uint64, Rec]
		if spec.useBeforeProcess {
			name := spec.beforeProcess
			opts = append(opts, BeforeProcess(func(_ uint64, v *Rec) error {
				v.Name = name
				return nil
			}))
		}
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			cc := spec.beforeStoreCC
			opts = append(opts, BeforeStore(func(_ uint64, _ *Rec, v *Rec) error {
				v.Tags = append(v.Tags, tag)
				v.Country = cc
				return nil
			}))
		}
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Patch(spec.id, cloneAutoBatchExtraFields(spec.patch), opts...)
	case autoBatchDelete:
		var opts []ExecOption[uint64, Rec]
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Delete(spec.id, opts...)
	default:
		return fmt.Errorf("unknown rec spec op: %v", spec.op)
	}
}

func buildAutoBatchExtraRecReq(
	tb testing.TB,
	db *DB[uint64, Rec],
	spec autoBatchExtraRecSpec,
) *autoBatchRequest[uint64, Rec] {
	tb.Helper()

	var beforeCommit []beforeCommitFunc[uint64, Rec]
	if spec.failCommit {
		cbMsg := fmt.Sprintf("callback failed step=%d req=%d", spec.step, spec.pos)
		beforeCommit = []beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, _ *Rec, _ *Rec) error { return errors.New(cbMsg) },
		}
	}

	switch spec.op {
	case autoBatchSet:
		var beforeStore []beforeStoreFunc[uint64, Rec]
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			cc := spec.beforeStoreCC
			beforeStore = []beforeStoreFunc[uint64, Rec]{
				func(_ uint64, _ *Rec, v *Rec) error {
					v.Tags = append(v.Tags, tag)
					v.Country = cc
					return nil
				},
			}
		}
		return mustBuildSetAutoReq(tb, db, spec.id, cloneAutoBatchExtraRec(spec.set), beforeStore, beforeCommit, nil)
	case autoBatchPatch:
		var beforeProcess []beforeProcessFunc[uint64, Rec]
		if spec.useBeforeProcess {
			name := spec.beforeProcess
			beforeProcess = []beforeProcessFunc[uint64, Rec]{
				func(_ uint64, v *Rec) error {
					v.Name = name
					return nil
				},
			}
		}
		var beforeStore []beforeStoreFunc[uint64, Rec]
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			cc := spec.beforeStoreCC
			beforeStore = []beforeStoreFunc[uint64, Rec]{
				func(_ uint64, _ *Rec, v *Rec) error {
					v.Tags = append(v.Tags, tag)
					v.Country = cc
					return nil
				},
			}
		}
		return mustBuildPatchAutoReq(tb, db, spec.id, cloneAutoBatchExtraFields(spec.patch), true, beforeProcess, beforeStore, beforeCommit)
	case autoBatchDelete:
		return mustBuildDeleteAutoReq(tb, db, spec.id, beforeCommit)
	default:
		tb.Fatalf("unknown rec spec op: %v", spec.op)
		return nil
	}
}

type autoBatchExtraUniqueSpec struct {
	step int
	pos  int
	op   autoBatchOp
	id   uint64

	set   *UniqueTestRec
	patch []Field

	useBeforeProcess bool
	beforeProcessEM  string
	beforeProcessCD  int

	useBeforeStore bool
	beforeStoreEM  string
	beforeStoreCD  int
	beforeStoreTag string

	failCommit bool
}

func applyAutoBatchExtraUniqueSpec(db *DB[uint64, UniqueTestRec], spec autoBatchExtraUniqueSpec) error {
	cbMsg := fmt.Sprintf("callback failed step=%d req=%d", spec.step, spec.pos)
	switch spec.op {
	case autoBatchSet:
		var opts []ExecOption[uint64, UniqueTestRec]
		if spec.useBeforeStore {
			email := spec.beforeStoreEM
			code := spec.beforeStoreCD
			tag := spec.beforeStoreTag
			opts = append(opts, BeforeStore(func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
				v.Email = email
				v.Code = code
				v.Tags = append(v.Tags, tag)
				return nil
			}))
		}
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *UniqueTestRec, _ *UniqueTestRec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Set(spec.id, cloneAutoBatchExtraUniqueRec(spec.set), opts...)
	case autoBatchPatch:
		var opts []ExecOption[uint64, UniqueTestRec]
		if spec.useBeforeProcess {
			email := spec.beforeProcessEM
			code := spec.beforeProcessCD
			opts = append(opts, BeforeProcess(func(_ uint64, v *UniqueTestRec) error {
				v.Email = email
				v.Code = code
				return nil
			}))
		}
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			opts = append(opts, BeforeStore(func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
				v.Tags = append(v.Tags, tag)
				return nil
			}))
		}
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *UniqueTestRec, _ *UniqueTestRec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Patch(spec.id, cloneAutoBatchExtraFields(spec.patch), opts...)
	case autoBatchDelete:
		var opts []ExecOption[uint64, UniqueTestRec]
		if spec.failCommit {
			opts = append(opts, BeforeCommit(func(_ *bbolt.Tx, _ uint64, _ *UniqueTestRec, _ *UniqueTestRec) error {
				return errors.New(cbMsg)
			}))
		}
		return db.Delete(spec.id, opts...)
	default:
		return fmt.Errorf("unknown unique spec op: %v", spec.op)
	}
}

func buildAutoBatchExtraUniqueReq(
	tb testing.TB,
	db *DB[uint64, UniqueTestRec],
	spec autoBatchExtraUniqueSpec,
) *autoBatchRequest[uint64, UniqueTestRec] {
	tb.Helper()

	var beforeCommit []beforeCommitFunc[uint64, UniqueTestRec]
	if spec.failCommit {
		cbMsg := fmt.Sprintf("callback failed step=%d req=%d", spec.step, spec.pos)
		beforeCommit = []beforeCommitFunc[uint64, UniqueTestRec]{
			func(_ *bbolt.Tx, _ uint64, _ *UniqueTestRec, _ *UniqueTestRec) error { return errors.New(cbMsg) },
		}
	}

	switch spec.op {
	case autoBatchSet:
		var beforeStore []beforeStoreFunc[uint64, UniqueTestRec]
		if spec.useBeforeStore {
			email := spec.beforeStoreEM
			code := spec.beforeStoreCD
			tag := spec.beforeStoreTag
			beforeStore = []beforeStoreFunc[uint64, UniqueTestRec]{
				func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
					v.Email = email
					v.Code = code
					v.Tags = append(v.Tags, tag)
					return nil
				},
			}
		}
		return mustBuildSetAutoReq(tb, db, spec.id, cloneAutoBatchExtraUniqueRec(spec.set), beforeStore, beforeCommit, nil)
	case autoBatchPatch:
		var beforeProcess []beforeProcessFunc[uint64, UniqueTestRec]
		if spec.useBeforeProcess {
			email := spec.beforeProcessEM
			code := spec.beforeProcessCD
			beforeProcess = []beforeProcessFunc[uint64, UniqueTestRec]{
				func(_ uint64, v *UniqueTestRec) error {
					v.Email = email
					v.Code = code
					return nil
				},
			}
		}
		var beforeStore []beforeStoreFunc[uint64, UniqueTestRec]
		if spec.useBeforeStore {
			tag := spec.beforeStoreTag
			beforeStore = []beforeStoreFunc[uint64, UniqueTestRec]{
				func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
					v.Tags = append(v.Tags, tag)
					return nil
				},
			}
		}
		return mustBuildPatchAutoReq(tb, db, spec.id, cloneAutoBatchExtraFields(spec.patch), true, beforeProcess, beforeStore, beforeCommit)
	case autoBatchDelete:
		return mustBuildDeleteAutoReq(tb, db, spec.id, beforeCommit)
	default:
		tb.Fatalf("unknown unique spec op: %v", spec.op)
		return nil
	}
}

func TestAutoBatchExtra_MixedQueuedOps_MatchSequentialModel(t *testing.T) {
	dbBatch, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1})
	dbSeq, _ := openTempDBUint64(t, Options{AnalyzeInterval: -1, AutoBatchMax: 1})

	namePool := []string{"alpha", "beta", "gamma", "delta", "epsilon", "bp-a", "bp-b", "bp-c", "bp-d"}
	countryPool := []string{"NL", "DE", "PL", "ES", "US"}
	tagPool := []string{"tg-a", "tg-b", "tg-c", "tg-d", "bs-a", "bs-b", "bs-c"}

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
			if !sameAutoBatchExtraRec(vb, vs) {
				t.Fatalf("step=%d id=%d value mismatch\nbatch=%#v\nseq=%#v", step, id, vb, vs)
			}
		}

		for _, name := range namePool {
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
		for _, cc := range countryPool {
			ib, err := dbBatch.QueryKeys(qx.Query(qx.EQ("country", cc)))
			if err != nil {
				t.Fatalf("step=%d batch QueryKeys(country=%q): %v", step, cc, err)
			}
			is, err := dbSeq.QueryKeys(qx.Query(qx.EQ("country", cc)))
			if err != nil {
				t.Fatalf("step=%d seq QueryKeys(country=%q): %v", step, cc, err)
			}
			if !slices.Equal(ib, is) {
				t.Fatalf("step=%d country index mismatch for %q: batch=%v seq=%v", step, cc, ib, is)
			}
		}
		for _, tag := range tagPool {
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
		1: {Name: "alpha", Age: 21, Score: 1.5, Tags: []string{"tg-a"}, FullName: "seed-1", Meta: Meta{Country: "NL"}},
		2: {Name: "beta", Age: 22, Score: 2.5, Tags: []string{"tg-b"}, FullName: "seed-2", Meta: Meta{Country: "DE"}},
		3: {Name: "gamma", Age: 23, Score: 3.5, Tags: []string{"tg-c"}, FullName: "seed-3", Meta: Meta{Country: "PL"}},
	}
	for id, rec := range seed {
		if err := dbBatch.Set(id, cloneAutoBatchExtraRec(rec)); err != nil {
			t.Fatalf("seed batch Set(%d): %v", id, err)
		}
		if err := dbSeq.Set(id, cloneAutoBatchExtraRec(rec)); err != nil {
			t.Fatalf("seed seq Set(%d): %v", id, err)
		}
	}

	r := newRand(20260329)
	for step := 0; step < 140; step++ {
		n := 4 + r.IntN(4)
		failIdx := -1
		if r.IntN(3) == 0 {
			failIdx = r.IntN(n)
		}

		specs := make([]autoBatchExtraRecSpec, n)
		reqs := make([]*autoBatchRequest[uint64, Rec], n)
		reqIndex := make(map[*autoBatchRequest[uint64, Rec]]int, n)
		wantErrs := make([]error, n)

		for i := 0; i < n; i++ {
			spec := autoBatchExtraRecSpec{
				step:             step,
				pos:              i,
				id:               uint64(1 + r.IntN(4)),
				useBeforeProcess: r.IntN(2) == 0,
				beforeProcess:    namePool[5+r.IntN(4)],
				useBeforeStore:   r.IntN(2) == 0,
				beforeStoreTag:   tagPool[4+r.IntN(3)],
				beforeStoreCC:    countryPool[r.IntN(len(countryPool))],
				failCommit:       i == failIdx,
			}

			switch r.IntN(3) {
			case 0:
				spec.op = autoBatchSet
				spec.set = &Rec{
					Name:     namePool[r.IntN(5)],
					Age:      18 + r.IntN(50),
					Score:    float64(5*r.IntN(20)) / 10,
					Active:   r.IntN(2) == 0,
					Tags:     []string{tagPool[r.IntN(4)]},
					FullName: fmt.Sprintf("full-%d-%d", step, i),
					Meta:     Meta{Country: countryPool[r.IntN(len(countryPool))]},
				}
				if r.IntN(2) == 0 {
					spec.set.Tags = append(spec.set.Tags, tagPool[r.IntN(4)])
				}
			case 1:
				spec.op = autoBatchPatch
				switch r.IntN(6) {
				case 0:
					spec.patch = []Field{{Name: "age", Value: float64(18 + r.IntN(50))}}
				case 1:
					spec.patch = []Field{{Name: "score", Value: float64(5*r.IntN(20)) / 10}}
				case 2:
					spec.patch = []Field{{Name: "active", Value: r.IntN(2) == 0}}
				case 3:
					spec.patch = []Field{{Name: "country", Value: countryPool[r.IntN(len(countryPool))]}}
				case 4:
					spec.patch = []Field{{Name: "tags", Value: []string{tagPool[r.IntN(4)], tagPool[r.IntN(4)]}}}
				default:
					spec.patch = []Field{{Name: "full_name", Value: fmt.Sprintf("patch-full-%d-%d", step, i)}}
				}
			default:
				spec.op = autoBatchDelete
			}

			specs[i] = spec
			reqs[i] = buildAutoBatchExtraRecReq(t, dbBatch, spec)
			reqIndex[reqs[i]] = i
		}

		drainQueuedAutoBatchStep(t, dbBatch, reqs, func(batch []*autoBatchJob[uint64, Rec]) {
			for _, job := range batch {
				req := job.reqs.Get(0)
				if req.replacedBy != nil {
					continue
				}
				idx := reqIndex[req]
				wantErrs[idx] = applyAutoBatchExtraRecSpec(dbSeq, specs[idx])
			}
			for _, job := range batch {
				req := job.reqs.Get(0)
				if req.replacedBy == nil {
					continue
				}
				term := terminalAutoBatchReq(req)
				wantErrs[reqIndex[req]] = wantErrs[reqIndex[term]]
			}
		})

		for i, req := range reqs {
			if got := mustAutoBatchErr(t, req); !sameAutoBatchExtraErr(got, wantErrs[i]) {
				t.Fatalf("step=%d req=%d id=%d op=%v error mismatch: got=%v want=%v", step, i, specs[i].id, specs[i].op, got, wantErrs[i])
			}
		}

		compareState(step)
	}

	assertNoFutureSnapshotRefs(t, dbBatch)
}

func TestAutoBatchExtra_UniqueMixedQueuedOps_MatchSequentialModel(t *testing.T) {
	dbBatch, _ := openTempDBUint64Unique(t, Options{AnalyzeInterval: -1})
	dbSeq, _ := openTempDBUint64Unique(t, Options{AnalyzeInterval: -1, AutoBatchMax: 1})

	emailPool := []string{
		"seed-1@x",
		"seed-2@x",
		"seed-3@x",
		"seed-4@x",
		"u0@x",
		"u1@x",
		"u2@x",
		"u3@x",
		"h0@x",
		"h1@x",
		"h2@x",
	}
	tagPool := []string{"tg-a", "tg-b", "tg-c", "tg-d", "bs-a", "bs-b", "bs-c"}
	const codeMax = 8

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

		for id := uint64(1); id <= 6; id++ {
			vb, err := dbBatch.Get(id)
			if err != nil {
				t.Fatalf("step=%d batch Get(%d): %v", step, id, err)
			}
			vs, err := dbSeq.Get(id)
			if err != nil {
				t.Fatalf("step=%d seq Get(%d): %v", step, id, err)
			}
			if !sameAutoBatchExtraUniqueRec(vb, vs) {
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
		for _, tag := range tagPool {
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

	seed := map[uint64]*UniqueTestRec{
		1: {Email: "seed-1@x", Code: 1, Tags: []string{"tg-a"}},
		2: {Email: "seed-2@x", Code: 2, Tags: []string{"tg-b"}},
		3: {Email: "seed-3@x", Code: 3, Tags: []string{"tg-c"}},
		4: {Email: "seed-4@x", Code: 4, Tags: []string{"tg-d"}},
	}
	for id, rec := range seed {
		if err := dbBatch.Set(id, cloneAutoBatchExtraUniqueRec(rec)); err != nil {
			t.Fatalf("seed batch Set(%d): %v", id, err)
		}
		if err := dbSeq.Set(id, cloneAutoBatchExtraUniqueRec(rec)); err != nil {
			t.Fatalf("seed seq Set(%d): %v", id, err)
		}
	}

	r := newRand(20260330)
	for step := 0; step < 140; step++ {
		n := 4 + r.IntN(4)
		failIdx := -1
		if r.IntN(4) == 0 {
			failIdx = r.IntN(n)
		}

		specs := make([]autoBatchExtraUniqueSpec, n)
		reqs := make([]*autoBatchRequest[uint64, UniqueTestRec], n)
		reqIndex := make(map[*autoBatchRequest[uint64, UniqueTestRec]]int, n)
		wantErrs := make([]error, n)

		for i := 0; i < n; i++ {
			spec := autoBatchExtraUniqueSpec{
				step:             step,
				pos:              i,
				id:               uint64(1 + r.IntN(6)),
				useBeforeProcess: r.IntN(2) == 0,
				beforeProcessEM:  emailPool[r.IntN(len(emailPool))],
				beforeProcessCD:  1 + r.IntN(codeMax),
				useBeforeStore:   r.IntN(2) == 0,
				beforeStoreEM:    emailPool[r.IntN(len(emailPool))],
				beforeStoreCD:    1 + r.IntN(codeMax),
				beforeStoreTag:   tagPool[4+r.IntN(3)],
				failCommit:       i == failIdx,
			}

			switch r.IntN(3) {
			case 0:
				spec.op = autoBatchSet
				spec.set = &UniqueTestRec{
					Email: emailPool[r.IntN(len(emailPool))],
					Code:  1 + r.IntN(codeMax),
					Tags:  []string{tagPool[r.IntN(4)]},
				}
				if r.IntN(2) == 0 {
					spec.set.Tags = append(spec.set.Tags, tagPool[r.IntN(4)])
				}
			case 1:
				spec.op = autoBatchPatch
				switch r.IntN(3) {
				case 0:
					spec.patch = []Field{{Name: "email", Value: emailPool[r.IntN(len(emailPool))]}}
				case 1:
					spec.patch = []Field{{Name: "code", Value: float64(1 + r.IntN(codeMax))}}
				default:
					spec.patch = []Field{{Name: "tags", Value: []string{tagPool[r.IntN(4)], tagPool[r.IntN(4)]}}}
				}
			default:
				spec.op = autoBatchDelete
			}

			specs[i] = spec
			reqs[i] = buildAutoBatchExtraUniqueReq(t, dbBatch, spec)
			reqIndex[reqs[i]] = i
		}

		drainQueuedAutoBatchStep(t, dbBatch, reqs, func(batch []*autoBatchJob[uint64, UniqueTestRec]) {
			for _, job := range batch {
				req := job.reqs.Get(0)
				if req.replacedBy != nil {
					continue
				}
				idx := reqIndex[req]
				wantErrs[idx] = applyAutoBatchExtraUniqueSpec(dbSeq, specs[idx])
			}
			for _, job := range batch {
				req := job.reqs.Get(0)
				if req.replacedBy == nil {
					continue
				}
				term := terminalAutoBatchReq(req)
				wantErrs[reqIndex[req]] = wantErrs[reqIndex[term]]
			}
		})

		for i, req := range reqs {
			if got := mustAutoBatchErr(t, req); !sameAutoBatchExtraErr(got, wantErrs[i]) {
				t.Fatalf("step=%d req=%d id=%d op=%v error mismatch: got=%v want=%v", step, i, specs[i].id, specs[i].op, got, wantErrs[i])
			}
		}

		compareState(step)
	}

	assertNoFutureSnapshotRefs(t, dbBatch)
}

func TestAutoBatchExtra_GrowQueuePreservesRingOrderAndCoalesceAcrossWrap(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}

	req1 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "wrap-A", Age: 20}, nil, nil, nil)
	req2 := mustBuildDeleteAutoReq(t, db, 1, nil)
	req3 := mustBuildSetAutoReq(t, db, 2, &Rec{Name: "wrap-B", Age: 30}, nil, nil, nil)
	req4 := mustBuildSetAutoReq(t, db, 3, &Rec{Name: "wrap-C", Age: 40}, nil, nil, nil)
	req5 := mustBuildSetAutoReq(t, db, 1, &Rec{Name: "wrap-D", Age: 50}, nil, nil, nil)

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	db.autoBatcher.queue = make([]*autoBatchJob[uint64, Rec], 4)
	db.autoBatcher.queueHead = 3
	db.autoBatcher.queueSize = 4
	db.autoBatcher.queue[3] = queuedSingleJob(req1)
	db.autoBatcher.queue[0] = queuedSingleJob(req2)
	db.autoBatcher.queue[1] = queuedSingleJob(req3)
	db.autoBatcher.queue[2] = queuedSingleJob(req4)
	db.autoBatcher.enqueue(queuedSingleJob(req5))
	if len(db.autoBatcher.queue) <= 4 {
		got := len(db.autoBatcher.queue)
		db.autoBatcher.mu.Unlock()
		t.Fatalf("queue did not grow, len=%d", got)
	}
	gotOrder := []uint64{
		db.autoBatcher.queueAt(0).reqs.Get(0).id,
		db.autoBatcher.queueAt(1).reqs.Get(0).id,
		db.autoBatcher.queueAt(2).reqs.Get(0).id,
		db.autoBatcher.queueAt(3).reqs.Get(0).id,
		db.autoBatcher.queueAt(4).reqs.Get(0).id,
	}
	db.autoBatcher.mu.Unlock()

	if !slices.Equal(gotOrder, []uint64{1, 1, 2, 3, 1}) {
		t.Fatalf("queue order after grow = %v, want [1 1 2 3 1]", gotOrder)
	}

	batch := db.popAutoBatch()
	if len(batch) != 5 {
		t.Fatalf("popped batch size = %d, want 5", len(batch))
	}
	if req1.replacedBy != req2 || req2.replacedBy != req5 || req5.replacedBy != nil {
		t.Fatalf("unexpected coalesce chain after grow: req1->%p req2->%p req5->%p", req1.replacedBy, req2.replacedBy, req5.replacedBy)
	}

	db.executeAutoBatchJobs(batch)
	clear(batch)
	db.autoBatcher.mu.Lock()
	if cap(batch) > cap(db.autoBatcher.batchScratch) {
		db.autoBatcher.batchScratch = batch[:0]
	} else if db.autoBatcher.batchScratch == nil {
		db.autoBatcher.batchScratch = batch[:0]
	}
	db.autoBatcher.mu.Unlock()

	for i, req := range []*autoBatchRequest[uint64, Rec]{req1, req2, req3, req4, req5} {
		if err := mustAutoBatchErr(t, req); err != nil {
			t.Fatalf("req%d error = %v", i+1, err)
		}
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "wrap-D" || got.Age != 50 {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "wrap-B" || got.Age != 30 {
		t.Fatalf("unexpected id=2 value: %#v", got)
	}
	if got, err := db.Get(3); err != nil {
		t.Fatalf("Get(3): %v", err)
	} else if got == nil || got.Name != "wrap-C" || got.Age != 40 {
		t.Fatalf("unexpected id=3 value: %#v", got)
	}

	if ids, err := db.QueryKeys(qx.Query(qx.EQ("name", "wrap-A"))); err != nil {
		t.Fatalf("QueryKeys(name=wrap-A): %v", err)
	} else if len(ids) != 0 {
		t.Fatalf("stale name=wrap-A ids: %v", ids)
	}
}

func TestAutoBatchExtra_InterleavedIsolatedSameID_PreservesSequentialState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &Rec{Name: "seed", Age: 10, Score: 1.5}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}

	var seen []string
	req1 := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "age", Value: float64(20)}}, true, nil, nil, nil)
	req2 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "score", Value: 9.5}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
				seen = append(seen, fmt.Sprintf("iso:%s/%d/%.1f->%s/%d/%.1f", oldValue.Name, oldValue.Age, oldValue.Score, newValue.Name, newValue.Age, newValue.Score))
				return nil
			},
		},
	)
	req3 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "name", Value: "tail"}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
				seen = append(seen, fmt.Sprintf("tail:%s/%d/%.1f->%s/%d/%.1f", oldValue.Name, oldValue.Age, oldValue.Score, newValue.Name, newValue.Age, newValue.Score))
				return nil
			},
		},
	)

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(
		db,
		queuedSingleJob(req1),
		&autoBatchJob[uint64, Rec]{
			reqs:     testAutoBatchRequestBuf(req2),
			isolated: true,
			done:     req2.done,
		},
		queuedSingleJob(req3),
	)
	db.autoBatcher.mu.Unlock()

	var popped []int
	drainCurrentAutoBatchQueue(t, db, func(batch []*autoBatchJob[uint64, Rec]) {
		popped = append(popped, len(batch))
	})

	if !slices.Equal(popped, []int{1, 1, 1}) {
		t.Fatalf("popped batch sizes = %v, want [1 1 1]", popped)
	}
	for i, req := range []*autoBatchRequest[uint64, Rec]{req1, req2, req3} {
		if err := mustAutoBatchErr(t, req); err != nil {
			t.Fatalf("req%d error = %v", i+1, err)
		}
	}

	wantSeen := []string{
		"iso:seed/20/1.5->seed/20/9.5",
		"tail:seed/20/9.5->tail/20/9.5",
	}
	if !slices.Equal(seen, wantSeen) {
		t.Fatalf("BeforeCommit sequence = %v, want %v", seen, wantSeen)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "tail" || got.Age != 20 || got.Score != 9.5 {
		t.Fatalf("unexpected final value: %#v", got)
	}
}

func TestAutoBatchExtra_GroupedJobBetweenSharedRequests_StaysIsolatedAndOrdered(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	if err := db.Set(1, &Rec{Name: "seed-1", Age: 10, Score: 1.5}); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	if err := db.Set(2, &Rec{Name: "seed-2", Age: 20, Score: 2.5}); err != nil {
		t.Fatalf("seed Set(2): %v", err)
	}

	headReq := mustBuildPatchAutoReq(t, db, 1, []Field{{Name: "age", Value: float64(30)}}, true, nil, nil, nil)

	var seen []string
	groupReq1 := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "score", Value: 7.5}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
				seen = append(seen, fmt.Sprintf("group1:%s/%d/%.1f->%s/%d/%.1f", oldValue.Name, oldValue.Age, oldValue.Score, newValue.Name, newValue.Age, newValue.Score))
				return nil
			},
		},
	)
	groupReq2 := mustBuildPatchAutoReq(
		t,
		db,
		2,
		[]Field{{Name: "name", Value: "mid-2"}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
				seen = append(seen, fmt.Sprintf("group2:%s/%d/%.1f->%s/%d/%.1f", oldValue.Name, oldValue.Age, oldValue.Score, newValue.Name, newValue.Age, newValue.Score))
				return nil
			},
		},
	)
	tailReq := mustBuildPatchAutoReq(
		t,
		db,
		1,
		[]Field{{Name: "name", Value: "tail"}},
		true,
		nil,
		nil,
		[]beforeCommitFunc[uint64, Rec]{
			func(_ *bbolt.Tx, _ uint64, oldValue, newValue *Rec) error {
				seen = append(seen, fmt.Sprintf("tail:%s/%d/%.1f->%s/%d/%.1f", oldValue.Name, oldValue.Age, oldValue.Score, newValue.Name, newValue.Age, newValue.Score))
				return nil
			},
		},
	)

	groupDone := make(chan error, 1)

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(
		db,
		queuedSingleJob(headReq),
		&autoBatchJob[uint64, Rec]{
			reqs:     testAutoBatchRequestBuf(groupReq1, groupReq2),
			isolated: true,
			done:     groupDone,
		},
		queuedSingleJob(tailReq),
	)
	db.autoBatcher.mu.Unlock()

	var popped []string
	drainCurrentAutoBatchQueue(t, db, func(batch []*autoBatchJob[uint64, Rec]) {
		switch {
		case len(batch) != 1:
			popped = append(popped, fmt.Sprintf("size=%d", len(batch)))
		case batch[0].isolated && batch[0].reqs.Len() == 2:
			popped = append(popped, "group")
		case batch[0].isolated:
			popped = append(popped, "isolated")
		default:
			popped = append(popped, "shared")
		}
	})

	if !slices.Equal(popped, []string{"shared", "group", "shared"}) {
		t.Fatalf("popped batches = %v, want [shared group shared]", popped)
	}
	if err := mustAutoBatchErr(t, headReq); err != nil {
		t.Fatalf("headReq error = %v", err)
	}
	select {
	case err := <-groupDone:
		if err != nil {
			t.Fatalf("grouped job error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grouped job")
	}
	if err := mustAutoBatchErr(t, tailReq); err != nil {
		t.Fatalf("tailReq error = %v", err)
	}

	wantSeen := []string{
		"group1:seed-1/30/1.5->seed-1/30/7.5",
		"group2:seed-2/20/2.5->mid-2/20/2.5",
		"tail:seed-1/30/7.5->tail/30/7.5",
	}
	if !slices.Equal(seen, wantSeen) {
		t.Fatalf("BeforeCommit sequence = %v, want %v", seen, wantSeen)
	}

	if got, err := db.Get(1); err != nil {
		t.Fatalf("Get(1): %v", err)
	} else if got == nil || got.Name != "tail" || got.Age != 30 || got.Score != 7.5 {
		t.Fatalf("unexpected id=1 value: %#v", got)
	}
	if got, err := db.Get(2); err != nil {
		t.Fatalf("Get(2): %v", err)
	} else if got == nil || got.Name != "mid-2" || got.Age != 20 || got.Score != 2.5 {
		t.Fatalf("unexpected id=2 value: %#v", got)
	}
}

func TestAutoBatchExtra_GroupedJobBetweenSharedRequests_PopSequenceStable(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	iters := 20000
	if testRaceEnabled {
		iters = 4000
	}

	for i := 0; i < iters; i++ {
		configureGroupedBetweenSharedQueueForTest(db, 1, [2]uint64{2, 3}, 4)
		assertGroupedBetweenSharedPopSequenceForTest(t, db, i)
	}
}

func TestAutoBatchExtra_GroupedJobBetweenSharedRequests_PopSequenceStableAfterScratchReuse(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	iters := 12000
	if testRaceEnabled {
		iters = 2000
	}

	for i := 0; i < iters; i++ {
		warmAutoBatchScratchForTest(db)
		configureGroupedBetweenSharedQueueForTest(db, 1, [2]uint64{2, 3}, 4)
		assertGroupedBetweenSharedPopSequenceForTest(t, db, i)
	}
}

func TestAutoBatchExtra_ClosedSetRejectsBeforeAutobatcher(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	before := db.AutoBatchStats()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := db.Set(1, &Rec{Name: "closed", Age: 10})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}

	after := db.AutoBatchStats()
	if after.Submitted != before.Submitted || after.FallbackClosed != before.FallbackClosed {
		t.Fatalf("closed write must be rejected before autobatcher, before=%+v after=%+v", before, after)
	}
	if after.Enqueued != before.Enqueued || after.Dequeued != before.Dequeued {
		t.Fatalf("closed write must not enqueue/dequeue, before=%+v after=%+v", before, after)
	}
}

func TestAutoBatchExtra_CloseUnblocksQueueFullWaiter_WithFallbackClosed(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1,
	})

	blockedReq := mustBuildSetAutoReq(t, db, 777, &Rec{Name: "blocked", Age: 7}, nil, nil, nil)
	before := db.AutoBatchStats()

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(blockedReq))

	done := make(chan error, 1)
	go func() {
		done <- db.Set(1, &Rec{Name: "late", Age: 11})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for db.autoBatcher.submitted.Load() != before.Submitted+1 && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if db.autoBatcher.submitted.Load() != before.Submitted+1 {
		db.autoBatcher.mu.Unlock()
		t.Fatal("waiting writer did not reach autobatcher in time")
	}
	db.autoBatcher.mu.Unlock()

	select {
	case err := <-done:
		t.Fatalf("writer returned before Close, err=%v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("writer error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocked writer after Close")
	}

	after := db.AutoBatchStats()
	if after.Submitted != before.Submitted+1 {
		t.Fatalf("Submitted delta = %d, want 1; before=%+v after=%+v", after.Submitted-before.Submitted, before, after)
	}
	if after.FallbackClosed != before.FallbackClosed+1 {
		t.Fatalf("FallbackClosed delta = %d, want 1; before=%+v after=%+v", after.FallbackClosed-before.FallbackClosed, before, after)
	}
	if after.Enqueued != before.Enqueued || after.Dequeued != before.Dequeued {
		t.Fatalf("blocked waiter must not enqueue/dequeue on close, before=%+v after=%+v", before, after)
	}

	if got, err := db.Get(777); !errors.Is(err, ErrClosed) || got != nil {
		t.Fatalf("Get(777) after Close = (%#v, %v), want (nil, ErrClosed)", got, err)
	}
}

func TestAutoBatchExtra_CloseWhileWindowedCoalescedBurst_FailsAllAndPreservesState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   200 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	seed := &Rec{Name: "seed", Age: 10, Tags: []string{"base"}, Meta: Meta{Country: "NL"}}
	if err := db.Set(1, cloneAutoBatchExtraRec(seed)); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	baseline := db.AutoBatchStats()

	db.autoBatcher.mu.Lock()
	db.autoBatcher.hotUntil = time.Now().Add(2 * time.Second)
	db.autoBatcher.mu.Unlock()

	type opResult struct {
		name string
		err  error
	}

	start := make(chan struct{})
	done := make(chan opResult, 4)
	run := func(name string, fn func() error) {
		go func() {
			<-start
			done <- opResult{name: name, err: fn()}
		}()
	}

	run("set-mid", func() error {
		return db.Set(1, &Rec{Name: "mid", Age: 20, Tags: []string{"mid"}, Meta: Meta{Country: "DE"}})
	})
	run("delete-seed", func() error {
		return db.Delete(1)
	})
	run("set-tail", func() error {
		return db.Set(1, &Rec{Name: "tail", Age: 30, Tags: []string{"tail"}, Meta: Meta{Country: "PL"}})
	})
	run("set-other", func() error {
		return db.Set(2, &Rec{Name: "other", Age: 40, Tags: []string{"other"}, Meta: Meta{Country: "ES"}})
	})

	close(start)

	beforeClose := waitAutoBatchExtraStats(t, db, "queued coalesced burst", func(st AutoBatchStats) bool {
		return st.Submitted == baseline.Submitted+4 &&
			st.Enqueued == baseline.Enqueued+4 &&
			st.Dequeued == baseline.Dequeued &&
			st.QueueLen == 4
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 4; i++ {
		res := <-done
		if !errors.Is(res.err, ErrClosed) {
			t.Fatalf("%s error = %v, want ErrClosed", res.name, res.err)
		}
	}

	after := waitAutoBatchExtraStats(t, db, "drained closed coalesced burst", func(st AutoBatchStats) bool {
		return st.Dequeued == beforeClose.Dequeued+4 && st.QueueLen == 0 && !st.WorkerRunning
	})
	if after.ExecutedBatches != beforeClose.ExecutedBatches {
		t.Fatalf("closed queued burst must not execute batches, before=%+v after=%+v", beforeClose, after)
	}
	if after.FallbackClosed != beforeClose.FallbackClosed {
		t.Fatalf("already enqueued jobs must not count as fallback-closed, before=%+v after=%+v", beforeClose, after)
	}

	got1 := readAutoBatchExtraRawValue(t, db, 1)
	if !sameAutoBatchExtraRec(got1, seed) {
		t.Fatalf("seed record changed after closed burst: got=%#v want=%#v", got1, seed)
	}
	if got2 := readAutoBatchExtraRawValue(t, db, 2); got2 != nil {
		t.Fatalf("id=2 must stay absent after closed burst, got=%#v", got2)
	}
}

func TestAutoBatchExtra_CloseWhileWindowedGroupedAndSharedBurst_FailsAllAndPreservesState(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   200 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 64,
	})

	seed := &Rec{Name: "seed", Age: 10, Tags: []string{"base"}, Meta: Meta{Country: "NL"}}
	if err := db.Set(1, cloneAutoBatchExtraRec(seed)); err != nil {
		t.Fatalf("seed Set(1): %v", err)
	}
	baseline := db.AutoBatchStats()

	db.autoBatcher.mu.Lock()
	db.autoBatcher.hotUntil = time.Now().Add(2 * time.Second)
	db.autoBatcher.mu.Unlock()

	type opResult struct {
		name string
		err  error
	}

	start := make(chan struct{})
	done := make(chan opResult, 4)
	run := func(name string, fn func() error) {
		go func() {
			<-start
			done <- opResult{name: name, err: fn()}
		}()
	}

	run("batch-set", func() error {
		return db.BatchSet(
			[]uint64{2, 3},
			[]*Rec{
				{Name: "batch-2", Age: 22, Tags: []string{"b2"}, Meta: Meta{Country: "DE"}},
				{Name: "batch-3", Age: 33, Tags: []string{"b3"}, Meta: Meta{Country: "PL"}},
			},
		)
	})
	run("patch-seed", func() error {
		return db.Patch(1, []Field{{Name: "age", Value: float64(77)}})
	})
	run("delete-seed", func() error {
		return db.Delete(1)
	})
	run("set-solo", func() error {
		return db.Set(4, &Rec{Name: "solo", Age: 44, Tags: []string{"solo"}, Meta: Meta{Country: "ES"}})
	})

	close(start)

	beforeClose := waitAutoBatchExtraStats(t, db, "queued grouped/shared burst", func(st AutoBatchStats) bool {
		return st.Submitted == baseline.Submitted+4 &&
			st.Enqueued == baseline.Enqueued+4 &&
			st.Dequeued == baseline.Dequeued &&
			st.QueueLen == 4
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 4; i++ {
		res := <-done
		if !errors.Is(res.err, ErrClosed) {
			t.Fatalf("%s error = %v, want ErrClosed", res.name, res.err)
		}
	}

	after := waitAutoBatchExtraStats(t, db, "drained closed grouped/shared burst", func(st AutoBatchStats) bool {
		return st.Dequeued == beforeClose.Dequeued+4 && st.QueueLen == 0 && !st.WorkerRunning
	})
	if after.ExecutedBatches != beforeClose.ExecutedBatches {
		t.Fatalf("closed grouped/shared burst must not execute batches, before=%+v after=%+v", beforeClose, after)
	}

	got1 := readAutoBatchExtraRawValue(t, db, 1)
	if !sameAutoBatchExtraRec(got1, seed) {
		t.Fatalf("seed record changed after closed grouped/shared burst: got=%#v want=%#v", got1, seed)
	}
	for _, id := range []uint64{2, 3, 4} {
		if got := readAutoBatchExtraRawValue(t, db, id); got != nil {
			t.Fatalf("id=%d must stay absent after closed grouped/shared burst, got=%#v", id, got)
		}
	}
}

func TestAutoBatchExtra_CloseUnblocksMultipleQueueFullWaiters_MixedJobs(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 1,
	})

	blockedReq := mustBuildSetAutoReq(t, db, 777, &Rec{Name: "blocked", Age: 7}, nil, nil, nil)
	before := db.AutoBatchStats()

	db.autoBatcher.mu.Lock()
	db.autoBatcher.window = 0
	db.autoBatcher.maxOps = 16
	db.autoBatcher.running = true
	db.autoBatcher.hotUntil = time.Time{}
	setAutoBatchQueueJobsForTest(db, queuedSingleJob(blockedReq))
	db.autoBatcher.mu.Unlock()

	type opResult struct {
		name string
		err  error
	}
	done := make(chan opResult, 3)
	go func() {
		done <- opResult{
			name: "set",
			err:  db.Set(1, &Rec{Name: "late-set", Age: 11}),
		}
	}()
	go func() {
		done <- opResult{
			name: "batch-set",
			err: db.BatchSet(
				[]uint64{2, 3},
				[]*Rec{
					{Name: "late-batch-2", Age: 22},
					{Name: "late-batch-3", Age: 33},
				},
			),
		}
	}()
	go func() {
		done <- opResult{
			name: "batch-delete",
			err:  db.BatchDelete([]uint64{4, 5}),
		}
	}()

	waitAutoBatchExtraStats(t, db, "queue-full waiters submitted", func(st AutoBatchStats) bool {
		return st.Submitted == before.Submitted+3
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i := 0; i < 3; i++ {
		res := <-done
		if !errors.Is(res.err, ErrClosed) {
			t.Fatalf("%s error = %v, want ErrClosed", res.name, res.err)
		}
	}

	after := db.AutoBatchStats()
	if after.Submitted != before.Submitted+3 {
		t.Fatalf("Submitted delta = %d, want 3; before=%+v after=%+v", after.Submitted-before.Submitted, before, after)
	}
	if after.FallbackClosed != before.FallbackClosed+3 {
		t.Fatalf("FallbackClosed delta = %d, want 3; before=%+v after=%+v", after.FallbackClosed-before.FallbackClosed, before, after)
	}
	if after.Enqueued != before.Enqueued || after.Dequeued != before.Dequeued {
		t.Fatalf("queue-full waiters must not enqueue/dequeue on close, before=%+v after=%+v", before, after)
	}
}

func TestAutoBatchExtra_StringSharedCloseAfterPrepare_RollsBackCreatedStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("seed", &Product{SKU: "seed", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	before := captureAutoBatchExtraStrMapState(db)

	req1 := mustBuildSetAutoReq(t, db, "ghost-a", &Product{SKU: "ghost-a", Price: 11}, nil, nil, nil)
	req2 := mustBuildSetAutoReq(t, db, "ghost-b", &Product{SKU: "ghost-b", Price: 12}, nil, nil, nil)

	db.mu.Lock()
	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		db.executeAutoBatch([]*autoBatchRequest[string, Product]{req1, req2})
	}()

	during := waitAutoBatchExtraStrMapHasKeys(t, db, "ghost-a", "ghost-b")
	if during.keyCount != before.keyCount+2 || during.next != before.next+2 || !during.dirty {
		db.mu.Unlock()
		t.Fatalf("unexpected strmap state during prepared shared close path: before=%+v during=%+v", before, during)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		db.mu.Unlock()
		t.Fatal("Close did not mark db closed in time")
	}
	db.mu.Unlock()

	<-execDone
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := mustAutoBatchErr(t, req1); !errors.Is(err, ErrClosed) {
		t.Fatalf("req1 error = %v, want ErrClosed", err)
	}
	if err := mustAutoBatchErr(t, req2); !errors.Is(err, ErrClosed) {
		t.Fatalf("req2 error = %v, want ErrClosed", err)
	}

	after := captureAutoBatchExtraStrMapState(db)
	if after.keyCount != before.keyCount || after.next != before.next || after.dirty {
		t.Fatalf("strmap state mismatch after rollback on shared close: before=%+v after=%+v", before, after)
	}
	if after.snap != before.snap || after.published != before.published || after.pubSource != before.pubSource {
		t.Fatalf("published strmap pointers changed after shared close rollback: before=%+v after=%+v", before, after)
	}
	if after.committed != before.committed || after.committedPub != before.committedPub {
		t.Fatalf("committed strmap pointers changed after shared close rollback: before=%+v after=%+v", before, after)
	}

	if got := readAutoBatchExtraRawValue(t, db, "ghost-a"); got != nil {
		t.Fatalf("ghost-a must stay absent after shared close rollback, got=%#v", got)
	}
	if got := readAutoBatchExtraRawValue(t, db, "ghost-b"); got != nil {
		t.Fatalf("ghost-b must stay absent after shared close rollback, got=%#v", got)
	}
}

func TestAutoBatchExtra_StringAtomicCloseAfterPrepare_RollsBackCreatedStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("seed", &Product{SKU: "seed", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	before := captureAutoBatchExtraStrMapState(db)

	req1 := mustBuildSetAutoReq(t, db, "atomic-a", &Product{SKU: "atomic-a", Price: 21}, nil, nil, nil)
	req2 := mustBuildSetAutoReq(t, db, "atomic-b", &Product{SKU: "atomic-b", Price: 22}, nil, nil, nil)

	jobDone := make(chan error, 1)
	job := &autoBatchJob[string, Product]{
		reqs:     testAutoBatchRequestBuf(req1, req2),
		isolated: true,
		done:     jobDone,
	}

	db.mu.Lock()
	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		db.executeAutoBatchJobs([]*autoBatchJob[string, Product]{job})
	}()

	during := waitAutoBatchExtraStrMapHasKeys(t, db, "atomic-a", "atomic-b")
	if during.keyCount != before.keyCount+2 || during.next != before.next+2 || !during.dirty {
		db.mu.Unlock()
		t.Fatalf("unexpected strmap state during prepared atomic close path: before=%+v during=%+v", before, during)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !db.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if !db.closed.Load() {
		db.mu.Unlock()
		t.Fatal("Close did not mark db closed in time")
	}
	db.mu.Unlock()

	<-execDone
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-jobDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("grouped job error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grouped job error")
	}

	if !errors.Is(req1.err, ErrClosed) {
		t.Fatalf("req1 error = %v, want ErrClosed", req1.err)
	}
	if !errors.Is(req2.err, ErrClosed) {
		t.Fatalf("req2 error = %v, want ErrClosed", req2.err)
	}

	after := captureAutoBatchExtraStrMapState(db)
	if after.keyCount != before.keyCount || after.next != before.next || after.dirty {
		t.Fatalf("strmap state mismatch after rollback on atomic close: before=%+v after=%+v", before, after)
	}
	if after.snap != before.snap || after.published != before.published || after.pubSource != before.pubSource {
		t.Fatalf("published strmap pointers changed after atomic close rollback: before=%+v after=%+v", before, after)
	}
	if after.committed != before.committed || after.committedPub != before.committedPub {
		t.Fatalf("committed strmap pointers changed after atomic close rollback: before=%+v after=%+v", before, after)
	}

	if got := readAutoBatchExtraRawValue(t, db, "atomic-a"); got != nil {
		t.Fatalf("atomic-a must stay absent after atomic close rollback, got=%#v", got)
	}
	if got := readAutoBatchExtraRawValue(t, db, "atomic-b"); got != nil {
		t.Fatalf("atomic-b must stay absent after atomic close rollback, got=%#v", got)
	}
}

func TestAutoBatchExtra_CloseUnblocksWaitingWriterEvenWithInFlightCommitAndQueuedGroupedJob(t *testing.T) {
	db, _ := openTempDBUint64(t, Options{
		AutoBatchWindow:   5 * time.Millisecond,
		AutoBatchMax:      1,
		AutoBatchMaxQueue: 1,
	})

	before := db.AutoBatchStats()

	entered := make(chan struct{})
	release := make(chan struct{})
	db.testHooks.beforeCommit = func(op string) error {
		if op != "set" {
			return nil
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	}
	t.Cleanup(func() {
		db.testHooks.beforeCommit = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	inFlightDone := make(chan error, 1)
	go func() {
		inFlightDone <- db.Set(1, &Rec{Name: "persisted", Age: 11})
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight commit hook")
	}

	queuedDone := make(chan error, 1)
	go func() {
		queuedDone <- db.BatchSet(
			[]uint64{2, 3},
			[]*Rec{
				{Name: "queued-2", Age: 22},
				{Name: "queued-3", Age: 33},
			},
		)
	}()

	waitAutoBatchExtraStats(t, db, "in-flight+queued state", func(st AutoBatchStats) bool {
		return st.Submitted == before.Submitted+2 &&
			st.Enqueued == before.Enqueued+2 &&
			st.Dequeued == before.Dequeued+1 &&
			st.QueueLen == 1
	})

	waitingDone := make(chan error, 1)
	go func() {
		waitingDone <- db.BatchDelete([]uint64{4, 5})
	}()

	waitAutoBatchExtraStats(t, db, "in-flight+queued+waiting state", func(st AutoBatchStats) bool {
		return st.Submitted == before.Submitted+3 &&
			st.Enqueued == before.Enqueued+2 &&
			st.Dequeued == before.Dequeued+1 &&
			st.QueueLen == 1
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	releaseCommit := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}

	select {
	case err := <-waitingDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting writer error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		releaseCommit()

		var (
			waitingErr  error
			inFlightErr error
			queuedErr   error
			closeErr    error
		)

		select {
		case waitingErr = <-waitingDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("waiting writer stayed blocked even after releasing in-flight commit, stats=%+v", db.AutoBatchStats())
		}
		select {
		case inFlightErr = <-inFlightDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("in-flight writer did not finish after releasing commit, stats=%+v", db.AutoBatchStats())
		}
		select {
		case queuedErr = <-queuedDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("queued grouped job did not finish after releasing commit, stats=%+v", db.AutoBatchStats())
		}
		select {
		case closeErr = <-closeDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("Close did not finish after releasing commit, stats=%+v", db.AutoBatchStats())
		}

		t.Fatalf(
			"waiting writer did not unblock on Close while in-flight commit was blocked; after release waiting=%v inFlight=%v queued=%v close=%v stats=%+v",
			waitingErr,
			inFlightErr,
			queuedErr,
			closeErr,
			db.AutoBatchStats(),
		)
	}

	releaseCommit()

	select {
	case err := <-inFlightDone:
		if err != nil {
			t.Fatalf("in-flight writer error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for in-flight writer")
	}

	select {
	case err := <-queuedDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("queued grouped job error = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for queued grouped job")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Close")
	}

	after := waitAutoBatchExtraStats(t, db, "mixed close outcomes settled", func(st AutoBatchStats) bool {
		return st.Submitted == before.Submitted+3 &&
			st.Enqueued == before.Enqueued+2 &&
			st.Dequeued == before.Dequeued+2 &&
			st.ExecutedBatches == before.ExecutedBatches+1 &&
			st.FallbackClosed == before.FallbackClosed+1 &&
			st.QueueLen == 0 &&
			!st.WorkerRunning
	})
	if after.MultiRequestBatches != before.MultiRequestBatches {
		t.Fatalf("queued grouped job closed before execution must not count as executed multi-request batch, before=%+v after=%+v", before, after)
	}

	if got := readAutoBatchExtraRawValue(t, db, uint64(1)); got == nil || got.Name != "persisted" || got.Age != 11 {
		t.Fatalf("id=1 must persist from in-flight writer, got=%#v", got)
	}
	for _, id := range []uint64{2, 3, 4, 5} {
		if got := readAutoBatchExtraRawValue(t, db, id); got != nil {
			t.Fatalf("id=%d must stay absent after close, got=%#v", id, got)
		}
	}
}

func TestAutoBatchExtra_Race_UniqueHookMutations_NoInvariantBreak(t *testing.T) {
	db, _ := openTempDBUint64Unique(t, Options{
		AnalyzeInterval:   -1,
		AutoBatchWindow:   200 * time.Microsecond,
		AutoBatchMax:      16,
		AutoBatchMaxQueue: 256,
	})

	emails := []string{
		"seed-1@x",
		"seed-2@x",
		"seed-3@x",
		"seed-4@x",
		"u0@x",
		"u1@x",
		"u2@x",
		"u3@x",
		"h0@x",
		"h1@x",
	}
	tags := []string{"tg-a", "tg-b", "tg-c", "tg-d", "bs-a", "bs-b", "bs-c"}
	const codeMax = 8

	for i := 1; i <= 4; i++ {
		if err := db.Set(uint64(i), &UniqueTestRec{
			Email: fmt.Sprintf("seed-%d@x", i),
			Code:  i,
			Tags:  []string{tags[i-1]},
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
				var err error

				switch r.IntN(3) {
				case 0:
					rec := &UniqueTestRec{
						Email: emails[r.IntN(len(emails))],
						Code:  1 + r.IntN(codeMax),
						Tags:  []string{tags[r.IntN(4)]},
					}
					if r.IntN(2) == 0 {
						email := emails[r.IntN(len(emails))]
						code := 1 + r.IntN(codeMax)
						tag := tags[r.IntN(len(tags))]
						err = db.Set(id, rec, BeforeStore(func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
							v.Email = email
							v.Code = code
							v.Tags = append(v.Tags, tag)
							return nil
						}))
					} else {
						err = db.Set(id, rec)
					}
				case 1:
					var patch []Field
					if r.IntN(2) == 0 {
						patch = []Field{{Name: "tags", Value: []string{tags[r.IntN(len(tags))]}}}
					} else {
						patch = []Field{{Name: "code", Value: float64(1 + r.IntN(codeMax))}}
					}

					var opts []ExecOption[uint64, UniqueTestRec]
					if r.IntN(2) == 0 {
						email := emails[r.IntN(len(emails))]
						code := 1 + r.IntN(codeMax)
						opts = append(opts, BeforeProcess(func(_ uint64, v *UniqueTestRec) error {
							v.Email = email
							v.Code = code
							return nil
						}))
					}
					if r.IntN(2) == 0 {
						tag := tags[r.IntN(len(tags))]
						opts = append(opts, BeforeStore(func(_ uint64, _ *UniqueTestRec, v *UniqueTestRec) error {
							v.Tags = append(v.Tags, tag)
							return nil
						}))
					}
					err = db.Patch(id, patch, opts...)
				default:
					err = db.Delete(id)
				}

				if err != nil && !errors.Is(err, ErrUniqueViolation) {
					reportErr(fmt.Errorf("writer id=%d: %w", id, err))
					return
				}
			}
		}(int64(6200 + w))
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

				queryKind := r.IntN(3)
				queryEmail := ""
				queryCode := 0
				queryTag := ""

				var q *qx.QX
				switch queryKind {
				case 0:
					queryEmail = emails[r.IntN(len(emails))]
					q = qx.Query(qx.EQ("email", queryEmail))
				case 1:
					queryCode = 1 + r.IntN(codeMax)
					q = qx.Query(qx.EQ("code", queryCode))
				default:
					queryTag = tags[r.IntN(len(tags))]
					q = qx.Query(qx.HASANY("tags", []string{queryTag}))
				}

				items, err := db.Query(q)
				if err != nil {
					reportErr(fmt.Errorf("Query: %w", err))
					return
				}
				for _, item := range items {
					if item == nil {
						continue
					}
					switch queryKind {
					case 0:
						if item.Email != queryEmail {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for email=%q", item, queryEmail))
							return
						}
					case 1:
						if item.Code != queryCode {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for code=%d", item, queryCode))
							return
						}
					default:
						if !slices.Contains(item.Tags, queryTag) {
							reportErr(fmt.Errorf("query returned inconsistent item %#v for tag=%q", item, queryTag))
							return
						}
					}
				}
			}
		}(int64(7200 + rr))
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("race failure: %v", err)
	}

	seenEmail := make(map[string]uint64)
	seenCode := make(map[int]uint64)
	for id := uint64(1); id <= 4; id++ {
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
			t.Fatalf("QueryKeys(email=%q): %v", email, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("email=%q ids=%v want=[%d]", email, ids, id)
		}
	}
	for code, id := range seenCode {
		ids, err := db.QueryKeys(qx.Query(qx.EQ("code", code)))
		if err != nil {
			t.Fatalf("QueryKeys(code=%d): %v", code, err)
		}
		if !slices.Equal(ids, []uint64{id}) {
			t.Fatalf("code=%d ids=%v want=[%d]", code, ids, id)
		}
	}
}
