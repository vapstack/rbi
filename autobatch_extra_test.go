package rbi

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/vapstack/qx"
	"github.com/vapstack/rbi/internal/keycodec"
	"go.etcd.io/bbolt"
)

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
		var keyBuf [8]byte
		raw := bucket.Get(keycodec.UserKeyBytesWithBuf(id, db.strKey, &keyBuf))
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
	next     uint64
	keyCount int
}

func captureAutoBatchExtraStrMapState[V any](db *DB[string, V]) autoBatchExtraStrMapState {
	snap := db.strMap.Snapshot()
	return autoBatchExtraStrMapState{
		next:     snap.Next(),
		keyCount: testStrMapSnapshotCount(snap),
	}
}

func waitAutoBatchExtraStrMapHasKeys[V any](tb testing.TB, db *DB[string, V], keys ...string) autoBatchExtraStrMapState {
	tb.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap := db.strMap.Snapshot()
		ok := true
		for _, key := range keys {
			if _, exists := snap.Index(key); !exists {
				ok = false
				break
			}
		}
		state := autoBatchExtraStrMapState{
			next:     snap.Next(),
			keyCount: testStrMapSnapshotCount(snap),
		}

		if ok {
			return state
		}
		if time.Now().After(deadline) {
			tb.Fatalf("timeout waiting for strmap keys %v last=%+v", keys, state)
		}
		time.Sleep(1 * time.Millisecond)
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

func TestAutoBatchExtra_StringAtomicCloseAfterPrepare_RollsBackCreatedStrMap(t *testing.T) {
	db, _ := openTempDBStringProduct(t)

	if err := db.Set("seed", &Product{SKU: "seed", Price: 10}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	before := captureAutoBatchExtraStrMapState(db)

	db.mu.Lock()
	execDone := make(chan error, 1)
	go func() {
		execDone <- db.BatchSet(
			[]string{"atomic-a", "atomic-b"},
			[]*Product{
				{SKU: "atomic-a", Price: 21},
				{SKU: "atomic-b", Price: 22},
			},
		)
	}()

	during := waitAutoBatchExtraStrMapHasKeys(t, db, "atomic-a", "atomic-b")
	if during.keyCount != before.keyCount+2 || during.next != before.next+2 {
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

	if err := <-execDone; !errors.Is(err, ErrClosed) {
		t.Fatalf("BatchSet error = %v, want ErrClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}

	after := captureAutoBatchExtraStrMapState(db)
	if after.keyCount != before.keyCount || after.next != before.next {
		t.Fatalf("strmap state mismatch after rollback on atomic close: before=%+v after=%+v", before, after)
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
	db.testHooks = &testHooks{
		beforeCommit: func(op string) error {
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
		},
	}
	t.Cleanup(func() {
		db.testHooks = nil
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
